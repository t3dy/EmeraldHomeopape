"""
mark_target_sections.py — Score segments by keyword density and detect section types.

Stage 4 of pipeline. Updates corpus_segments with:
- relevance_score (keyword density)
- section_type (FRONT_MATTER, CHAPTER, BIBLIOGRAPHY, etc.)
- language (detected from content)
- has_emerald_tablet (1 if Emerald Tablet text detected)
- persons_mentioned (JSON array of person slugs found)
- concepts_mentioned (JSON array of concept slugs found)

All deterministic. No LLM.
Idempotent: overwrites scores on re-run.
Prerequisite: segment_texts.py must have run.
"""

import json
import re
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "emerald_tablet.db"

# Keyword lists for relevance scoring
KEYWORDS = {
    'primary': [
        r'hermes\s+trismegist', r'tabula\s+smaragdina', r'emerald\s+tablet',
        r'corpus\s+hermeticum', r'asclepius', r'poimandres', r'pimander',
        r'sirr\s+al.khali', r'kitab\s+sirr', r'balinas', r'apollonius\s+of\s+tyana',
    ],
    'hermetic_concepts': [
        r'\bnous\b', r'\blogos\b', r'\bgnosis\b', r'\btheurgy\b',
        r'macrocosm', r'microcosm', r'emanation', r'demiurge',
        r'transmutation', r'philosopher.s\s+stone', r'palingenesi',
    ],
    'persons': [
        r'\bficino\b', r'\bbruno\b', r'\bagrippa\b', r'\biamblichus\b',
        r'\bzosimos\b', r'\bjabir\b', r'\bhortulanus\b', r'\bkhunrath\b',
        r'\bmaier\b', r'\bnewton\b', r'\bpico\b', r'\bdee\b',
        r'\bcasaubon\b', r'\bcopenhaver\b', r'\bbull\b', r'\bhanegraaff\b',
        r'\bfowden\b', r'\blitwa\b', r'\bruska\b', r'\bholmyard\b',
        r'\bweisser\b', r'\bnemesius\b', r'\bplato\s+of\s+tivoli\b',
        r'hugo\s+of\s+santalla', r'philip\s+of\s+tripoli',
        r'\bdelp\b', r'\bburnett\b', r'\blucentini\b', r'\bporreca\b',
        r'\bdebus\b', r'\bmoreschini\b',
    ],
    'alchemical': [
        r'\bnigredo\b', r'\balbedo\b', r'\brubedo\b', r'\bcitrinitas\b',
        r'sulphur.mercury', r'tria\s+prima', r'\bcalcination\b',
        r'\bdistillation\b', r'\bcoagulation\b', r'magnum\s+opus',
        r'\btelesmi\b', r'\bdecknamen\b', r'\bvitriol\b',
    ],
    'texts': [
        r'de\s+mysteriis', r'picatrix', r'secretum\s+secretorum',
        r'book\s+of\s+24\s+philosopher', r'liber\s+hermetis',
        r'de\s+sex\s+rerum', r'three\s+books\s+of\s+occult',
        r'amphitheatrum', r'atalanta\s+fugiens',
        r'discourse\s+on\s+the\s+eighth', r'nag\s+hammadi',
    ],
}

# Weight per category
WEIGHTS = {
    'primary': 5,
    'hermetic_concepts': 3,
    'persons': 2,
    'alchemical': 2,
    'texts': 3,
}

# Emerald Tablet specific phrases
TABLET_PHRASES = [
    r'quod\s+est\s+(inferius|superius)',
    r'as\s+above.*so\s+below',
    r'father.*sun.*mother.*moon',
    r'pater.*sol.*mater.*luna',
    r'wind.*womb',
    r'tabula\s+smaragdina',
    r'emerald\s+tablet',
    r'الأعلى.*الأسفل',
]

# Section type detection
FRONT_MATTER_PATTERNS = [
    r'(?i)^(table\s+of\s+contents|contents|abbreviations|acknowledgment|preface|foreword)',
    r'(?i)^(list\s+of\s+illustrations|list\s+of\s+tables|dedication)',
    r'(?i)copyright.*\d{4}',
    r'(?i)^isbn\b',
]

BIBLIOGRAPHY_PATTERNS = [
    r'(?i)^(bibliography|references|works\s+cited|sources)',
    r'(?i)^\d+\.\s+[A-Z][a-z]+,\s+[A-Z]',  # numbered bibliography entries
]

INDEX_PATTERNS = [
    r'(?i)^(index|general\s+index|subject\s+index|name\s+index)',
]

APPARATUS_PATTERNS = [
    r'(?i)^(apparatus\s+criticus|critical\s+apparatus|textual\s+notes)',
    r'(?i)^(editorial\s+notes|sigla|manuscript\s+descriptions)',
]

# Person slug mapping for mentions — slugs MUST match persons.person_id in DB
PERSON_PATTERNS = {
    'hermes_trismegistus': r'hermes\s+trismegist',
    'balinas': r'\bbalinas\b|apollonius\s+of\s+tyana',
    'marsilio_ficino': r'\bficino\b',
    'giordano_bruno': r'giordano\s+bruno',
    'cornelius_agrippa': r'\bagrippa\b',
    'iamblichus': r'\biamblichus\b',
    'zosimos_of_panopolis': r'\bzosimos\b',
    'jabir_ibn_hayyan': r'\bjabir\b|ibn\s+hayyan',
    'hortulanus': r'\bhortulanus\b',
    'heinrich_khunrath': r'\bkhunrath\b',
    'michael_maier': r'michael\s+maier',
    'isaac_newton': r'isaac\s+newton',
    'giovanni_pico': r'\bpico\b',
    'nemesius_of_emesa': r'\bnemesius\b',
    'plato_of_tivoli': r'plato\s+of\s+tivoli',
    'hugo_of_santalla': r'hugo\s+of\s+santalla',
    'brian_copenhaver': r'\bcopenhaver\b',
    'christian_bull': r'christian\s+(h\.?\s+)?bull',
    'wouter_hanegraaff': r'\bhanegraaff\b',
    'julius_ruska': r'\bruska\b',
    'ursula_weisser': r'\bweisser\b',
    'isaac_casaubon': r'\bcasaubon\b',
    'augustine': r'\baugustine\b|augustinus',
    'plotinus': r'\bplotinus\b',
    'porphyry': r'\bporphyry\b',
    'lactantius': r'\blactantius\b',
    'thomas_aquinas': r'\baquinas\b|thomas\s+aquinas',
    'albertus_magnus': r'albertus\s+magnus|albert\s+the\s+great',
    'roger_bacon': r'roger\s+bacon',
    'john_dee': r'john\s+dee',
    'al_razi': r'\bal.razi\b|rhazes',
    'david_litwa': r'\blitwa\b',
    'garth_fowden': r'\bfowden\b',
    'kyle_fraser': r'kyle\s+fraser',
    'jean_pierre_mahe': r'\bmah[eé]\b',
    'paolo_lucentini': r'\blucentini\b',
    'allen_debus': r'\bdebus\b',
    'peter_forshaw': r'\bforshaw\b',
    'claudio_moreschini': r'\bmoreschini\b',
    'florian_ebeling': r'\bebeling\b',
    'lodovico_lazzarelli': r'\blazzarelli\b',
    'ibn_umayl': r'\bibn\s+umayl\b',
    'philip_of_tripoli': r'philip\s+of\s+tripoli',
    'john_of_seville': r'john\s+of\s+seville|johannes\s+hispalensis',
    'eric_holmyard': r'\bholmyard\b',
    'paul_kraus': r'\bkraus\b',
    'silvestre_de_sacy': r'silvestre\s+de\s+sacy',
    'sagijus_of_nablus': r'sagijus|sergius\s+of\s+nablus',
    'job_of_odessa': r'job\s+of\s+odessa|ayyub\s+al.ruhawi',
}

# Concept slug mapping — slugs MUST match concepts.slug in DB
CONCEPT_PATTERNS = {
    'macrocosm_microcosm': r'macrocosm|microcosm|as\s+above.*so\s+below',
    'emanation': r'\bemanation\b|\bprocession\b',
    'elemental_qualities': r'four\s+elements|hot.*cold.*moist.*dry|hylomorphism',
    'telesmi': r'\btelesmi\b|\btelesmum\b|\btelesmus\b',
    'sulfur_mercury_theory': r'sulphur.*mercury|sol.*luna|chemical\s+wedding',
    'decknamen': r'\bdecknamen\b|cover\s+names',
    'vitriol_acrostic': r'\bvitriol\b|visita\s+interiora',
    'discovery_topos': r'hidden\s+chamber|underground\s+vault|discovery\s+narrative',
    'gnosis': r'\bgnosis\b|\bgnostic\b',
    'theurgy': r'\btheurgy\b|\btheurgic\b',
    'nous': r'\bnous\b|\bnoetic\b',
    'logos': r'\blogos\b',
    'palingenesia': r'palingenesi|regeneration|rebirth',
    'sympatheia': r'\bsympatheia\b|\bsympathy\b|cosmic\s+sympathy',
    'anthropos': r'\banthropos\b|primal\s+human',
    'prisca_theologia': r'prisca\s+theologia|ancient\s+theology|perennial\s+philosophy',
    'barbarous_names': r'barbarous\s+names|onomata\s+barbar|ineffable\s+names',
    'demiurge': r'\bdemiurge\b|\bdemiurg',
    'magnum_opus': r'magnum\s+opus|great\s+work|philosopher.s\s+stone',
}


def detect_language(text):
    """Detect language from character analysis."""
    if not text:
        return None
    arabic = len(re.findall(r'[\u0600-\u06FF]', text))
    greek = len(re.findall(r'[\u0370-\u03FF]', text))
    total = max(len(text), 1)
    if arabic / total > 0.1:
        return 'ARABIC'
    if greek / total > 0.05:
        return 'GREEK'

    german = len(re.findall(r'\b(und|der|die|das|ist|ein|von|für|über)\b', text, re.IGNORECASE))
    french = len(re.findall(r'\b(les|des|une|est|dans|pour|avec)\b', text, re.IGNORECASE))
    italian = len(re.findall(r'\b(della|nella|alla|sono|questa|degli)\b', text, re.IGNORECASE))
    latin = len(re.findall(r'\b(est|sunt|quod|cum|enim|autem|vel)\b', text))

    if german > 15:
        return 'GERMAN'
    if french > 15:
        return 'FRENCH'
    if italian > 15:
        return 'ITALIAN'
    if latin > 20 and german < 5 and french < 5:
        return 'LATIN'
    return 'ENGLISH'


def detect_section_type(text):
    """Detect section type from content patterns."""
    first_500 = text[:500]
    for p in FRONT_MATTER_PATTERNS:
        if re.search(p, first_500):
            return 'FRONT_MATTER'
    for p in BIBLIOGRAPHY_PATTERNS:
        if re.search(p, first_500):
            return 'BIBLIOGRAPHY'
    for p in INDEX_PATTERNS:
        if re.search(p, first_500):
            return 'INDEX'
    for p in APPARATUS_PATTERNS:
        if re.search(p, first_500):
            return 'APPARATUS'
    return None


def score_relevance(text):
    """Score segment by keyword density."""
    score = 0
    text_lower = text.lower()
    for category, patterns in KEYWORDS.items():
        weight = WEIGHTS[category]
        for pattern in patterns:
            matches = len(re.findall(pattern, text_lower))
            score += matches * weight
    return score


def detect_tablet(text):
    """Check if segment contains Emerald Tablet text."""
    text_lower = text.lower()
    for pattern in TABLET_PHRASES:
        if re.search(pattern, text_lower):
            return 1
    return 0


def find_persons(text):
    """Find mentioned persons."""
    found = []
    text_lower = text.lower()
    for slug, pattern in PERSON_PATTERNS.items():
        if re.search(pattern, text_lower):
            found.append(slug)
    return found


def find_concepts(text):
    """Find mentioned concepts."""
    found = []
    text_lower = text.lower()
    for slug, pattern in CONCEPT_PATTERNS.items():
        if re.search(pattern, text_lower):
            found.append(slug)
    return found


def main():
    if not DB_PATH.exists():
        print("ERROR: Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)

    segments = conn.execute("""
        SELECT id, text_content FROM corpus_segments
    """).fetchall()

    if not segments:
        print("No segments found. Run segment_texts.py first.")
        conn.close()
        return

    updated = 0
    high_relevance = 0
    tablet_mentions = 0

    for seg_id, text in segments:
        score = score_relevance(text)
        language = detect_language(text)
        section_type = detect_section_type(text)
        has_tablet = detect_tablet(text)
        persons = find_persons(text)
        concepts = find_concepts(text)

        conn.execute("""
            UPDATE corpus_segments SET
                relevance_score = ?,
                language = ?,
                section_type = ?,
                has_emerald_tablet = ?,
                persons_mentioned = ?,
                concepts_mentioned = ?
            WHERE id = ?
        """, (
            score, language, section_type, has_tablet,
            json.dumps(persons) if persons else None,
            json.dumps(concepts) if concepts else None,
            seg_id
        ))

        updated += 1
        if score >= 10:
            high_relevance += 1
        if has_tablet:
            tablet_mentions += 1

    conn.commit()

    # Report
    print(f"Marked: {updated} segments")
    print(f"High relevance (score >= 10): {high_relevance}")
    print(f"Emerald Tablet mentions: {tablet_mentions}")

    # Score distribution
    for threshold, label in [(50, '50+'), (20, '20-49'), (10, '10-19'), (5, '5-9'), (1, '1-4'), (0, '0')]:
        if threshold == 0:
            count = conn.execute("SELECT COUNT(*) FROM corpus_segments WHERE relevance_score = 0").fetchone()[0]
        elif threshold == 50:
            count = conn.execute("SELECT COUNT(*) FROM corpus_segments WHERE relevance_score >= 50").fetchone()[0]
        else:
            upper = {20: 49, 10: 19, 5: 9, 1: 4}[threshold]
            count = conn.execute(
                "SELECT COUNT(*) FROM corpus_segments WHERE relevance_score BETWEEN ? AND ?",
                (threshold, upper)
            ).fetchone()[0]
        print(f"  Score {label}: {count} segments")

    # Top 10 segments
    top = conn.execute("""
        SELECT cs.segment_id, cs.relevance_score, cd.title
        FROM corpus_segments cs
        JOIN corpus_documents cd ON cs.doc_id = cd.id
        ORDER BY cs.relevance_score DESC
        LIMIT 10
    """).fetchall()
    print(f"\nTop 10 segments:")
    for seg_id, score, title in top:
        safe_title = (title or 'untitled')[:60]
        print(f"  [{score:3d}] {seg_id[:40]} — {safe_title}")

    conn.close()


if __name__ == "__main__":
    main()
