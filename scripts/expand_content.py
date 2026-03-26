"""
expand_content.py — Gather evidence segments for LLM-assisted content expansion.

Produces staging/expansion_packets.json containing, for each entity,
the top relevant segments from the corpus as evidence for writing
expanded descriptions.

This script is DETERMINISTIC — it gathers evidence but does NOT generate content.
The LLM expansion happens in the main session reading these packets.

Usage:
    python scripts/expand_content.py              # All entities
    python scripts/expand_content.py --persons     # Persons only
    python scripts/expand_content.py --texts       # Texts only
    python scripts/expand_content.py --concepts    # Concepts only
"""

import json
import sqlite3
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "emerald_tablet.db"
OUTPUT_PATH = BASE_DIR / "staging" / "expansion_packets.json"

MAX_SEGMENTS_PER_ENTITY = 5
MAX_CHARS_PER_SEGMENT = 1500


def gather_person_evidence(conn):
    """For each person, find the most relevant corpus segments mentioning them."""
    persons = conn.execute("""
        SELECT person_id, name, era, role_primary, description
        FROM persons ORDER BY name
    """).fetchall()

    packets = []
    for pid, name, era, role, desc in persons:
        # Find segments mentioning this person, ordered by relevance
        segments = conn.execute("""
            SELECT cs.text_content, cs.relevance_score, cd.title
            FROM corpus_segments cs
            JOIN corpus_documents cd ON cs.doc_id = cd.id
            WHERE cs.persons_mentioned LIKE ?
            ORDER BY cs.relevance_score DESC
            LIMIT ?
        """, (f'%"{pid}"%', MAX_SEGMENTS_PER_ENTITY)).fetchall()

        evidence = []
        for text, score, doc_title in segments:
            # Truncate long segments
            if len(text) > MAX_CHARS_PER_SEGMENT:
                text = text[:MAX_CHARS_PER_SEGMENT] + "..."
            evidence.append({
                "source": doc_title,
                "score": score,
                "text": text
            })

        # Get associated texts
        roles = conn.execute("""
            SELECT t.title, ptr.role
            FROM person_text_roles ptr
            JOIN texts t ON ptr.text_id = t.id
            WHERE ptr.person_id = (SELECT id FROM persons WHERE person_id = ?)
        """, (pid,)).fetchall()

        packets.append({
            "entity_type": "person",
            "slug": pid,
            "name": name,
            "era": era,
            "role": role,
            "current_description": desc,
            "current_desc_length": len(desc) if desc else 0,
            "associated_texts": [{"title": t, "role": r} for t, r in roles],
            "evidence_segments": evidence,
            "segment_count": len(evidence),
        })

    return packets


def gather_text_evidence(conn):
    """For each text, find the most relevant corpus segments discussing it."""
    texts = conn.execute("""
        SELECT text_id, title, language, text_type, description, transmission_notes
        FROM texts ORDER BY title
    """).fetchall()

    packets = []
    for tid, title, lang, ttype, desc, trans in texts:
        # Build search terms from title words
        title_words = [w.lower() for w in title.split() if len(w) > 3]
        search_pattern = '|'.join(title_words[:3]) if title_words else tid

        segments = conn.execute("""
            SELECT cs.text_content, cs.relevance_score, cd.title
            FROM corpus_segments cs
            JOIN corpus_documents cd ON cs.doc_id = cd.id
            WHERE cs.relevance_score >= 10
            AND (cs.text_content LIKE ? OR cs.text_content LIKE ?)
            ORDER BY cs.relevance_score DESC
            LIMIT ?
        """, (f'%{tid.replace("_", " ")}%', f'%{title[:20]}%',
              MAX_SEGMENTS_PER_ENTITY)).fetchall()

        evidence = []
        for text, score, doc_title in segments:
            if len(text) > MAX_CHARS_PER_SEGMENT:
                text = text[:MAX_CHARS_PER_SEGMENT] + "..."
            evidence.append({
                "source": doc_title,
                "score": score,
                "text": text
            })

        packets.append({
            "entity_type": "text",
            "slug": tid,
            "title": title,
            "language": lang,
            "text_type": ttype,
            "current_description": desc,
            "current_desc_length": len(desc) if desc else 0,
            "transmission_notes": trans,
            "evidence_segments": evidence,
            "segment_count": len(evidence),
        })

    return packets


def gather_concept_evidence(conn):
    """For each concept, find relevant corpus segments."""
    concepts = conn.execute("""
        SELECT slug, label, category, definition_short, definition_long, significance
        FROM concepts ORDER BY label
    """).fetchall()

    packets = []
    for slug, label, cat, def_short, def_long, sig in concepts:
        segments = conn.execute("""
            SELECT cs.text_content, cs.relevance_score, cd.title
            FROM corpus_segments cs
            JOIN corpus_documents cd ON cs.doc_id = cd.id
            WHERE cs.concepts_mentioned LIKE ?
            ORDER BY cs.relevance_score DESC
            LIMIT ?
        """, (f'%"{slug}"%', MAX_SEGMENTS_PER_ENTITY)).fetchall()

        evidence = []
        for text, score, doc_title in segments:
            if len(text) > MAX_CHARS_PER_SEGMENT:
                text = text[:MAX_CHARS_PER_SEGMENT] + "..."
            evidence.append({
                "source": doc_title,
                "score": score,
                "text": text
            })

        packets.append({
            "entity_type": "concept",
            "slug": slug,
            "label": label,
            "category": cat,
            "current_def_short": def_short,
            "current_def_long": def_long,
            "current_significance": sig,
            "evidence_segments": evidence,
            "segment_count": len(evidence),
        })

    return packets


def main():
    if not DB_PATH.exists():
        print("ERROR: Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    mode = sys.argv[1] if len(sys.argv) > 1 else '--all'

    packets = []

    if mode in ('--all', '--persons'):
        person_packets = gather_person_evidence(conn)
        packets.extend(person_packets)
        with_evidence = sum(1 for p in person_packets if p['segment_count'] > 0)
        print(f"Persons: {len(person_packets)} total, {with_evidence} with evidence segments")

    if mode in ('--all', '--texts'):
        text_packets = gather_text_evidence(conn)
        packets.extend(text_packets)
        with_evidence = sum(1 for p in text_packets if p['segment_count'] > 0)
        print(f"Texts: {len(text_packets)} total, {with_evidence} with evidence segments")

    if mode in ('--all', '--concepts'):
        concept_packets = gather_concept_evidence(conn)
        packets.extend(concept_packets)
        with_evidence = sum(1 for p in concept_packets if p['segment_count'] > 0)
        print(f"Concepts: {len(concept_packets)} total, {with_evidence} with evidence segments")

    # Write packets
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(packets, f, indent=2, ensure_ascii=False)

    total_evidence = sum(p['segment_count'] for p in packets)
    print(f"\nTotal packets: {len(packets)}")
    print(f"Total evidence segments: {total_evidence}")
    print(f"Output: {OUTPUT_PATH}")

    conn.close()


if __name__ == "__main__":
    main()
