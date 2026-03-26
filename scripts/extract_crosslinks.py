"""
extract_crosslinks.py — Build person-text and concept-text links from segment co-occurrence.

Deterministic extraction. Uses the persons_mentioned and concepts_mentioned
fields from corpus_segments (populated by mark_target_sections.py) to find
which persons and concepts co-occur in which scholarly documents.

Also extracts timeline event candidates from date+person patterns.

Idempotent: uses INSERT OR IGNORE.
"""

import json
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "emerald_tablet.db"


def extract_person_text_roles(conn):
    """Find person-text relationships from segment co-occurrence with scholarly works."""
    # Get all segments with persons mentioned
    rows = conn.execute("""
        SELECT cs.persons_mentioned, cd.doc_id, cd.title, cd.doc_family
        FROM corpus_segments cs
        JOIN corpus_documents cd ON cs.doc_id = cd.id
        WHERE cs.persons_mentioned IS NOT NULL
        AND cd.doc_family IN ('SCHOLARLY_MONOGRAPH', 'SCHOLARLY_ARTICLE', 'CRITICAL_EDITION',
                              'HERMETIC_CORPUS', 'REFERENCE_WORK', 'CONFERENCE_PROCEEDINGS')
    """).fetchall()

    # Count person-document co-occurrences
    person_doc = defaultdict(lambda: defaultdict(int))
    for pm, doc_id, doc_title, doc_family in rows:
        try:
            persons = json.loads(pm)
            for p in persons:
                person_doc[p][doc_id] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    # For persons who appear in many segments of a document, they're likely the subject
    # We can create COMMENTATOR relationships for modern scholars writing about historical figures
    inserted = 0
    for person_slug, docs in person_doc.items():
        # Check person exists in DB
        person_row = conn.execute(
            "SELECT id, role_primary FROM persons WHERE person_id = ?", (person_slug,)
        ).fetchone()
        if not person_row:
            continue

        person_pk, role = person_row

        for doc_id, count in docs.items():
            if count < 3:  # Skip incidental mentions
                continue

            # Try to find a matching text in the texts table
            # Match by doc_id patterns to text slugs
            text_row = None
            # Direct slug match
            text_row = conn.execute(
                "SELECT id FROM texts WHERE text_id = ?", (doc_id,)
            ).fetchone()

            if text_row:
                # Don't duplicate existing roles
                existing = conn.execute("""
                    SELECT 1 FROM person_text_roles
                    WHERE person_id = ? AND text_id = ?
                """, (person_pk, text_row[0])).fetchone()
                if not existing:
                    # Determine role based on person's primary role
                    if role in ('SCHOLAR', 'EDITOR'):
                        link_role = 'COMMENTATOR'
                    elif role == 'TRANSLATOR':
                        link_role = 'TRANSLATOR'
                    elif role in ('AUTHOR', 'PHILOSOPHER', 'MYTHICAL_FIGURE'):
                        link_role = 'ATTRIBUTED_AUTHOR'
                    else:
                        link_role = 'COMMENTATOR'

                    conn.execute("""
                        INSERT OR IGNORE INTO person_text_roles
                            (person_id, text_id, role, notes, confidence)
                        VALUES (?, ?, ?, ?, 'MEDIUM')
                    """, (person_pk, text_row[0], link_role,
                          f"Co-occurs in {count} segments of {doc_id}"))
                    inserted += 1

    return inserted


def extract_concept_text_refs(conn):
    """Find concept-text links from segment co-occurrence."""
    rows = conn.execute("""
        SELECT cs.concepts_mentioned, cd.doc_id
        FROM corpus_segments cs
        JOIN corpus_documents cd ON cs.doc_id = cd.id
        WHERE cs.concepts_mentioned IS NOT NULL
    """).fetchall()

    concept_doc = defaultdict(lambda: defaultdict(int))
    for cm, doc_id in rows:
        try:
            concepts = json.loads(cm)
            for c in concepts:
                concept_doc[c][doc_id] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    inserted = 0
    for concept_slug, docs in concept_doc.items():
        concept_row = conn.execute(
            "SELECT id FROM concepts WHERE slug = ?", (concept_slug,)
        ).fetchone()
        if not concept_row:
            continue

        for doc_id, count in docs.items():
            if count < 2:
                continue

            text_row = conn.execute(
                "SELECT id FROM texts WHERE text_id = ?", (doc_id,)
            ).fetchone()
            if text_row:
                conn.execute("""
                    INSERT OR IGNORE INTO concept_text_refs
                        (concept_id, text_id, notes)
                    VALUES (?, ?, ?)
                """, (concept_row[0], text_row[0],
                      f"Mentioned in {count} segments"))
                inserted += 1

    return inserted


def extract_timeline_candidates(conn):
    """Extract date+person+event patterns from high-scoring segments."""
    segments = conn.execute("""
        SELECT cs.text_content, cs.persons_mentioned
        FROM corpus_segments cs
        WHERE cs.relevance_score >= 20
        AND cs.persons_mentioned IS NOT NULL
    """).fetchall()

    # Pattern: year (4 digits, 200-2025) near a person name and action verb
    year_pattern = re.compile(r'\b(1[0-9]{3}|20[0-2][0-9]|[2-9][0-9]{2})\b')
    action_verbs = re.compile(
        r'\b(published|wrote|translated|composed|compiled|printed|edited|discovered|'
        r'proposed|argued|demonstrated|founded|established|burned|died|born)\b',
        re.IGNORECASE
    )

    candidates = []
    seen_years = set()

    # Get existing timeline years to avoid duplicates
    existing = conn.execute("SELECT year, title FROM timeline_events").fetchall()
    existing_keys = {(y, t[:30]) for y, t in existing}

    for text, pm in segments:
        if not text or len(text) < 50:
            continue

        try:
            persons = json.loads(pm)
        except (json.JSONDecodeError, TypeError):
            continue

        # Find sentences with years
        sentences = re.split(r'[.!?]\s+', text)
        for sentence in sentences:
            years = year_pattern.findall(sentence)
            actions = action_verbs.findall(sentence)

            if years and actions:
                year = int(years[0])
                if year < 200 or year > 2025:
                    continue

                # Find which person is mentioned in this sentence
                for p in persons:
                    short_name = p.split('_')[-1]  # last part of slug
                    if re.search(re.escape(short_name), sentence, re.IGNORECASE):
                        action = actions[0].lower()
                        title = f"{short_name.title()} {action} ({year})"
                        key = (year, title[:30])
                        if key not in existing_keys and key not in seen_years:
                            seen_years.add(key)
                            # Resolve person FK
                            person_row = conn.execute(
                                "SELECT id FROM persons WHERE person_id = ?", (p,)
                            ).fetchone()
                            if person_row:
                                candidates.append({
                                    'year': year,
                                    'title': sentence[:120].strip(),
                                    'description': sentence[:300].strip(),
                                    'person_id': person_row[0],
                                    'event_type': 'SCHOLARSHIP' if year > 1800 else 'PUBLICATION',
                                })
                        break

    # Insert top candidates (limit to avoid noise)
    inserted = 0
    for c in candidates[:40]:
        conn.execute("""
            INSERT OR IGNORE INTO timeline_events
                (year, event_type, title, description, person_id, confidence)
            VALUES (?, ?, ?, ?, ?, 'MEDIUM')
        """, (c['year'], c['event_type'], c['title'], c['description'], c['person_id']))
        inserted += 1

    return inserted, len(candidates)


def report_person_coverage(conn):
    """Report which persons have the most segment mentions but fewest DB links."""
    rows = conn.execute("""
        SELECT cs.persons_mentioned
        FROM corpus_segments cs
        WHERE cs.persons_mentioned IS NOT NULL
    """).fetchall()

    mentions = Counter()
    for (pm,) in rows:
        try:
            for p in json.loads(pm):
                mentions[p] += 1
        except (json.JSONDecodeError, TypeError):
            pass

    print("\nPerson coverage (top 30):")
    print(f"  {'Person':<30} {'Segments':>8} {'Roles':>6} {'Status'}")
    for person_slug, seg_count in mentions.most_common(30):
        exists = conn.execute("SELECT 1 FROM persons WHERE person_id = ?", (person_slug,)).fetchone()
        roles = conn.execute("""
            SELECT COUNT(*) FROM person_text_roles ptr
            JOIN persons p ON ptr.person_id = p.id
            WHERE p.person_id = ?
        """, (person_slug,)).fetchone()[0] if exists else 0
        status = "OK" if exists else "MISSING"
        safe = person_slug[:28].encode('ascii', errors='replace').decode('ascii')
        print(f"  {safe:<30} {seg_count:>8} {roles:>6} {status}")


def main():
    if not DB_PATH.exists():
        print("ERROR: Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)

    # Extract cross-links
    ptr_count = extract_person_text_roles(conn)
    print(f"Person-text roles added: {ptr_count}")

    ctr_count = extract_concept_text_refs(conn)
    print(f"Concept-text refs added: {ctr_count}")

    timeline_inserted, timeline_candidates = extract_timeline_candidates(conn)
    print(f"Timeline events added: {timeline_inserted} (from {timeline_candidates} candidates)")

    conn.commit()

    # Report totals
    for table in ['person_text_roles', 'concept_text_refs', 'timeline_events']:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  Total {table}: {count}")

    report_person_coverage(conn)

    conn.close()


if __name__ == "__main__":
    main()
