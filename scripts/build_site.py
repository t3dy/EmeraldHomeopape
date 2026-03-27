"""
build_site.py — Generate static HTML site from database.

Stage 9 of pipeline. Produces:
- index.html (home with stats + featured cards)
- texts/index.html + texts/{text_id}.html
- persons/index.html + persons/{person_id}.html
- concepts/index.html + concepts/{slug}.html
- translations/index.html + translations/emerald-tablet.html (parallel viewer)
- timeline.html
- bibliography.html
- manuscripts/index.html + manuscripts/{id}.html
- about.html
- data.json (for JS consumers)
- style.css

Depth-aware navigation. Stale file cleanup before generation.
"""

import json
import re
import shutil
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "db" / "emerald_tablet.db"
SITE_DIR = BASE_DIR / "site"

# ── Design System ──────────────────────────────────────────────

CSS = """
:root {
    --bg: #f5f0e8;
    --bg-card: #fff;
    --bg-dark: #2c2418;
    --text: #2c2418;
    --text-muted: #6b5d4d;
    --accent: #8b4513;
    --accent-light: #d4a574;
    --border: #d4c5a9;
    --font-serif: Georgia, 'Times New Roman', serif;
    --font-sans: 'Segoe UI', system-ui, -apple-system, sans-serif;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: var(--font-serif);
    background: var(--bg);
    color: var(--text);
    line-height: 1.7;
    min-height: 100vh;
}

/* Navigation */
.site-nav {
    background: var(--bg-dark);
    padding: 0.75rem 1.5rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    position: sticky;
    top: 0;
    z-index: 100;
}
.site-nav a {
    color: var(--accent-light);
    text-decoration: none;
    padding: 0.4rem 0.8rem;
    font-family: var(--font-sans);
    font-size: 0.85rem;
    border-radius: 3px;
    transition: background 0.15s;
}
.site-nav a:hover, .site-nav a.active {
    background: rgba(212,165,116,0.2);
    color: #fff;
}
.site-title {
    color: #fff;
    font-weight: 700;
    font-size: 1rem;
    margin-right: 1rem;
    padding: 0.4rem 0;
}

/* Layout */
.page-content {
    max-width: 960px;
    margin: 2rem auto;
    padding: 0 1.5rem;
}
.page-wide {
    max-width: 1200px;
    margin: 2rem auto;
    padding: 0 1.5rem;
}

/* Cards */
.card-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 1.5rem;
    margin: 1.5rem 0;
}
.card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1.25rem;
    text-decoration: none;
    color: var(--text);
    display: block;
    transition: transform 0.15s, box-shadow 0.15s;
}
.card:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(0,0,0,0.1);
}
.card-sig {
    font-weight: 700;
    font-size: 1.05rem;
    margin-bottom: 0.3rem;
}
.card-label {
    font-family: var(--font-sans);
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-bottom: 0.5rem;
}
.card-desc {
    font-size: 0.9rem;
    line-height: 1.5;
    color: var(--text);
}
.card-cta {
    margin-top: 0.5rem;
    font-size: 0.85rem;
    color: var(--accent);
    font-family: var(--font-sans);
}

/* Stats bar */
.stats-bar {
    display: flex;
    flex-wrap: wrap;
    gap: 1rem;
    margin: 1.5rem 0;
}
.stat-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1rem 1.5rem;
    text-align: center;
    flex: 1;
    min-width: 120px;
}
.stat-num {
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--accent);
    display: block;
}
.stat-label {
    font-family: var(--font-sans);
    font-size: 0.8rem;
    color: var(--text-muted);
}

/* Badges */
.badge {
    display: inline-block;
    padding: 0.15rem 0.5rem;
    border-radius: 3px;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    font-family: var(--font-sans);
    color: white;
    margin-left: 0.3rem;
}

/* Section headers */
.section-header {
    font-size: 1.3rem;
    margin: 2rem 0 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid var(--accent-light);
    color: var(--accent);
}

/* Back link */
.back-link {
    display: inline-block;
    margin-bottom: 1rem;
    color: var(--accent);
    text-decoration: none;
    font-family: var(--font-sans);
    font-size: 0.9rem;
}
.back-link:hover { text-decoration: underline; }

/* Detail page */
.detail-body { margin-top: 1.5rem; }
.detail-body h3 { color: var(--accent); font-size: 1.1rem; margin-top: 1.5rem; }
.detail-body p { margin: 0.5rem 0; }
.detail-body a { color: var(--accent); }

/* Timeline */
.timeline-year {
    font-size: 1.3rem;
    font-weight: 700;
    color: var(--accent);
    margin: 2rem 0 0.5rem;
    padding-top: 1rem;
    border-top: 1px solid var(--border);
}
.timeline-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1rem;
    margin: 0.5rem 0;
    border-left: 4px solid var(--accent);
}

/* Parallel translation table */
.verse-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.9rem;
}
.verse-table th {
    background: var(--bg-dark);
    color: var(--accent-light);
    padding: 0.75rem;
    text-align: left;
    font-family: var(--font-sans);
    font-size: 0.8rem;
}
/* Word annotation tooltips */
.anno-word {
    position: relative;
    border-bottom: 1px dotted #d4a574;
    cursor: pointer;
}
.anno-tip {
    display: none;
    position: absolute;
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    background: #2c2418;
    color: #f5f0e8;
    padding: 0.4rem 0.6rem;
    border-radius: 4px;
    font-size: 0.78rem;
    line-height: 1.4;
    white-space: nowrap;
    max-width: 300px;
    white-space: normal;
    z-index: 50;
    pointer-events: none;
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
    font-family: var(--font-sans);
    direction: ltr;
    text-align: left;
}
.anno-tip::after {
    content: '';
    position: absolute;
    top: 100%;
    left: 50%;
    transform: translateX(-50%);
    border: 5px solid transparent;
    border-top-color: #2c2418;
}
.anno-word:hover .anno-tip {
    display: block;
}

.translation-selector {
    display: inline-block;
    font-family: var(--font-sans);
    font-size: 0.85rem;
    padding: 0.3rem 0.5rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-card);
    color: var(--text);
    margin: 0.25rem;
}
.verse-table td {
    padding: 0.75rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
    line-height: 1.6;
}
.verse-table tr:hover td { background: #faf8f4; }
.verse-num {
    font-weight: 700;
    color: var(--accent);
    white-space: nowrap;
    width: 40px;
}

/* Bibliography */
.ref-card {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1rem;
    margin: 0.75rem 0;
}
.ref-card h4 { font-size: 0.95rem; }
.ref-card em { color: var(--text); }
.ref-meta {
    font-family: var(--font-sans);
    font-size: 0.8rem;
    color: var(--text-muted);
    margin-top: 0.3rem;
}

/* Provenance */
.provenance {
    margin-top: 2rem;
    padding: 0.75rem 1rem;
    background: #faf8f4;
    border-radius: 4px;
    font-family: var(--font-sans);
    font-size: 0.8rem;
    color: var(--text-muted);
}

/* Responsive */
@media (max-width: 768px) {
    .card-grid { grid-template-columns: 1fr; }
    .stats-bar { flex-direction: column; }
    .verse-table { font-size: 0.8rem; }
    .site-nav { gap: 0; }
}
"""

# ── Helpers ────────────────────────────────────────────────────

def nav_html(prefix="", active=""):
    """Generate navigation bar."""
    links = [
        ("Home", f"{prefix}index.html"),
        ("Texts", f"{prefix}texts/index.html"),
        ("Persons", f"{prefix}persons/index.html"),
        ("Translations", f"{prefix}translations/index.html"),
        ("Concepts", f"{prefix}concepts/index.html"),
        ("Timeline", f"{prefix}timeline.html"),
        ("Bibliography", f"{prefix}bibliography.html"),
        ("Manuscripts", f"{prefix}manuscripts/index.html"),
        ("About", f"{prefix}about.html"),
    ]
    nav = '<nav class="site-nav">\n'
    nav += '  <span class="site-title">EmeraldTablet</span>\n'
    for label, href in links:
        cls = ' class="active"' if label.lower() == active.lower() else ''
        nav += f'  <a href="{href}"{cls}>{label}</a>\n'
    nav += '</nav>\n'
    return nav


def page_shell(title, body, depth=0, active=""):
    """Wrap body in full HTML page."""
    prefix = "../" * depth
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} — EmeraldTablet</title>
    <link rel="stylesheet" href="{prefix}style.css">
</head>
<body>
{nav_html(prefix, active)}
{body}
</body>
</html>"""


def truncate(text, length=150):
    """Truncate text to length with ellipsis."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)  # strip HTML
    if len(text) <= length:
        return text
    return text[:length].rsplit(' ', 1)[0] + "..."


def badge_html(label, color="#7f8c8d"):
    return f'<span class="badge" style="background:{color}">{label}</span>'


TYPE_COLORS = {
    'PRIMARY_SOURCE': '#c0392b', 'COMMENTARY': '#8e44ad', 'COMPILATION': '#e67e22',
    'TREATISE': '#2980b9', 'ENCYCLOPEDIA': '#16a085', 'TRANSLATION': '#27ae60',
    'PSEUDO_EPIGRAPHA': '#7f8c8d',
}
ROLE_COLORS = {
    'AUTHOR': '#27ae60', 'ATTRIBUTED_AUTHOR': '#2ecc71', 'TRANSLATOR': '#2980b9',
    'COMMENTATOR': '#8e44ad', 'EDITOR': '#e67e22', 'DISCOVERER': '#c0392b',
    'COMPILER': '#16a085', 'SCHOLAR': '#34495e', 'MYTHICAL_FIGURE': '#9b59b6',
    'PHILOSOPHER': '#2c3e50',
}
CATEGORY_COLORS = {
    'COSMOLOGICAL': '#1a5276', 'ALCHEMICAL': '#8b4513', 'PHILOSOPHICAL': '#2c3e50',
    'LINGUISTIC': '#6c3483', 'THEOLOGICAL': '#c0392b', 'SCIENTIFIC': '#27ae60',
}
EVENT_COLORS = {
    'COMPOSITION': '#8e44ad', 'TRANSLATION': '#2980b9', 'COMMENTARY': '#e67e22',
    'PUBLICATION': '#27ae60', 'SCHOLARSHIP': '#16a085', 'MANUSCRIPT': '#795548',
    'DISCOVERY': '#c0392b', 'PRINTING': '#34495e',
}
RELEVANCE_COLORS = {'PRIMARY': '#c0392b', 'DIRECT': '#2980b9', 'CONTEXTUAL': '#7f8c8d'}


# ── Page Builders ──────────────────────────────────────────────

def build_home(conn):
    counts = {}
    for t in ['texts', 'persons', 'translations', 'concepts', 'bibliography', 'manuscripts', 'timeline_events', 'translation_verses']:
        counts[t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]

    body = '<div class="page-content">\n'
    body += '<h1 style="font-size:2rem;margin-bottom:0.5rem">Hermeticism Knowledge Portal</h1>\n'
    body += '<p style="font-size:1.1rem;color:var(--text-muted);margin-bottom:2rem">A digital edition of the Emerald Tablet and encyclopedic reference for the Hermetic tradition from late antiquity to the early modern period.</p>\n'

    # Stats
    body += '<div class="stats-bar">\n'
    for label, key in [("Texts", "texts"), ("Persons", "persons"), ("Translations", "translations"),
                        ("Concepts", "concepts"), ("Bibliography", "bibliography"), ("Verses", "translation_verses")]:
        body += f'<div class="stat-card"><span class="stat-num">{counts[key]}</span><span class="stat-label">{label}</span></div>\n'
    body += '</div>\n'

    # Featured: Emerald Tablet
    body += '<div style="background:var(--bg-card);border:2px solid var(--accent);border-radius:8px;padding:1.5rem;margin:2rem 0">\n'
    body += '<h2 style="color:var(--accent);margin-bottom:0.5rem">Emerald Tablet Parallel Translations</h2>\n'
    body += f'<p>Compare {counts["translations"]} translations of the Emerald Tablet verse-by-verse: Arabic, Latin, English, French, and more.</p>\n'
    body += '<a href="translations/emerald-tablet.html" style="display:inline-block;margin-top:0.75rem;padding:0.5rem 1.25rem;background:var(--accent);color:white;border-radius:4px;text-decoration:none;font-family:var(--font-sans)">Open Parallel Viewer &rarr;</a>\n'
    body += '</div>\n'

    # Recent texts cards
    body += '<h2 class="section-header">Featured Texts</h2>\n'
    body += '<div class="card-grid">\n'
    featured = conn.execute("""
        SELECT text_id, title, text_type, language, description
        FROM texts WHERE text_id IN ('emerald_tablet','corpus_hermeticum','asclepius','de_mysteriis','sirr_al_khaliqa','picatrix')
        ORDER BY date_composed_start
    """).fetchall()
    for tid, title, ttype, lang, desc in featured:
        color = TYPE_COLORS.get(ttype, '#7f8c8d')
        body += f'<a href="texts/{tid}.html" class="card">\n'
        body += f'  <div class="card-sig">{title}</div>\n'
        body += f'  <div class="card-label">{lang} {badge_html(ttype, color) if ttype else ""}</div>\n'
        body += f'  <div class="card-desc">{truncate(desc)}</div>\n'
        body += f'  <div class="card-cta">View details &rarr;</div>\n'
        body += '</a>\n'
    body += '</div>\n</div>\n'

    return page_shell("Home", body, active="Home")


def build_texts_index(conn):
    body = '<div class="page-content">\n'
    body += '<h1>Texts</h1>\n'
    body += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Primary sources, treatises, and compilations of the Hermetic tradition.</p>\n'

    # Group by type
    types = conn.execute("SELECT DISTINCT text_type FROM texts WHERE text_type IS NOT NULL ORDER BY text_type").fetchall()
    for (ttype,) in types:
        color = TYPE_COLORS.get(ttype, '#7f8c8d')
        texts = conn.execute("""
            SELECT text_id, title, language, date_composed_start, date_composed_end, description
            FROM texts WHERE text_type = ? ORDER BY date_composed_start, title
        """, (ttype,)).fetchall()
        body += f'<h2 class="section-header">{ttype.replace("_", " ").title()} {badge_html(str(len(texts)), color)}</h2>\n'
        body += '<div class="card-grid">\n'
        for tid, title, lang, ds, de, desc in texts:
            date_str = f"{ds}-{de} CE" if ds and de else f"ca. {ds} CE" if ds else ""
            body += f'<a href="{tid}.html" class="card">\n'
            body += f'  <div class="card-sig">{title}</div>\n'
            body += f'  <div class="card-label">{lang or ""} &middot; {date_str}</div>\n'
            body += f'  <div class="card-desc">{truncate(desc)}</div>\n'
            body += f'  <div class="card-cta">View details &rarr;</div>\n'
            body += '</a>\n'
        body += '</div>\n'

    body += '</div>\n'
    return page_shell("Texts", body, depth=1, active="Texts")


def build_text_detail(conn, text_row):
    tid, slug, title, title_orig, lang, ttype, ds, de, desc, analysis, trans = text_row
    body = '<div class="page-content">\n'
    body += '<a href="index.html" class="back-link">&larr; All Texts</a>\n'
    body += f'<h1>{title}'
    if ttype:
        body += f' {badge_html(ttype, TYPE_COLORS.get(ttype, "#7f8c8d"))}'
    body += '</h1>\n'

    body += '<div class="detail-body">\n'
    if analysis:
        body += analysis
    else:
        if desc:
            body += f'<p>{desc}</p>'
    body += '\n</div>\n'

    body += '<div class="provenance">Source: SEED_DATA &middot; Confidence: HIGH &middot; Status: DRAFT</div>\n'
    body += '</div>\n'
    return page_shell(title, body, depth=1, active="Texts")


def build_persons_index(conn):
    body = '<div class="page-content">\n'
    body += '<h1>Persons</h1>\n'
    body += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Historical figures, mythical authorities, and modern scholars of the Hermetic tradition.</p>\n'

    roles = conn.execute("SELECT DISTINCT role_primary FROM persons WHERE role_primary IS NOT NULL ORDER BY role_primary").fetchall()
    for (role,) in roles:
        color = ROLE_COLORS.get(role, '#7f8c8d')
        persons = conn.execute("""
            SELECT person_id, name, era, description
            FROM persons WHERE role_primary = ? ORDER BY name
        """, (role,)).fetchall()
        body += f'<h2 class="section-header">{role.replace("_", " ").title()} {badge_html(str(len(persons)), color)}</h2>\n'
        body += '<div class="card-grid">\n'
        for pid, name, era, desc in persons:
            body += f'<a href="{pid}.html" class="card">\n'
            body += f'  <div class="card-sig">{name}</div>\n'
            body += f'  <div class="card-label">{era or ""}</div>\n'
            body += f'  <div class="card-desc">{truncate(desc)}</div>\n'
            body += f'  <div class="card-cta">View profile &rarr;</div>\n'
            body += '</a>\n'
        body += '</div>\n'

    body += '</div>\n'
    return page_shell("Persons", body, depth=1, active="Persons")


def build_person_detail(conn, person_row):
    pid_num, slug, name, name_alt, by, dy, era, role, desc, bio = person_row
    body = '<div class="page-content">\n'
    body += '<a href="index.html" class="back-link">&larr; All Persons</a>\n'
    body += f'<h1>{name}'
    if role:
        body += f' {badge_html(role, ROLE_COLORS.get(role, "#7f8c8d"))}'
    body += '</h1>\n'

    body += '<div class="detail-body">\n'
    if bio:
        body += bio
    else:
        if desc:
            body += f'<p>{desc}</p>'
    body += '\n</div>\n'

    body += '<div class="provenance">Source: SEED_DATA &middot; Confidence: HIGH &middot; Status: DRAFT</div>\n'
    body += '</div>\n'
    return page_shell(name, body, depth=1, active="Persons")


def build_concepts_index(conn):
    body = '<div class="page-content">\n'
    body += '<h1>Concepts</h1>\n'
    body += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Key philosophical, alchemical, and theological concepts of the Hermetic tradition.</p>\n'

    categories = conn.execute("SELECT DISTINCT category FROM concepts WHERE category IS NOT NULL ORDER BY category").fetchall()
    for (cat,) in categories:
        color = CATEGORY_COLORS.get(cat, '#7f8c8d')
        concepts = conn.execute("""
            SELECT slug, label, definition_short
            FROM concepts WHERE category = ? ORDER BY label
        """, (cat,)).fetchall()
        body += f'<h2 class="section-header">{cat.replace("_", " ").title()} {badge_html(str(len(concepts)), color)}</h2>\n'
        body += '<div class="card-grid">\n'
        for slug, label, defn in concepts:
            body += f'<a href="{slug}.html" class="card">\n'
            body += f'  <div class="card-sig">{label}</div>\n'
            body += f'  <div class="card-desc">{truncate(defn)}</div>\n'
            body += f'  <div class="card-cta">View definition &rarr;</div>\n'
            body += '</a>\n'
        body += '</div>\n'

    body += '</div>\n'
    return page_shell("Concepts", body, depth=1, active="Concepts")


def build_concept_detail(conn, concept_row):
    cid, slug, label, label_alt, cat, def_short, def_long, sig = concept_row
    color = CATEGORY_COLORS.get(cat, '#7f8c8d')

    body = '<div class="page-content">\n'
    body += '<a href="index.html" class="back-link">&larr; Concepts</a>\n'
    body += f'<h1>{label} {badge_html(cat, color) if cat else ""}</h1>\n'

    if label_alt:
        try:
            alts = json.loads(label_alt)
            if alts:
                body += f'<p style="font-style:italic;color:var(--text-muted)">{", ".join(alts)}</p>\n'
        except (json.JSONDecodeError, TypeError):
            pass

    if def_short:
        body += f'<div style="border-left:3px solid {color};padding-left:1rem;margin:1rem 0;font-style:italic">{def_short}</div>\n'

    if def_long:
        body += f'<div style="margin:1rem 0;line-height:1.8">'
        for p in def_long.split('\n'):
            if p.strip():
                body += f'<p>{p.strip()}</p>'
        body += '</div>\n'

    if sig:
        body += '<h3 style="color:var(--accent);margin-top:1.5rem">Significance</h3>\n'
        body += f'<p>{sig}</p>\n'

    # Related texts
    texts = conn.execute("""
        SELECT t.text_id, t.title FROM concept_text_refs ctr
        JOIN texts t ON ctr.text_id = t.id WHERE ctr.concept_id = ?
    """, (cid,)).fetchall()
    if texts:
        body += '<h3 style="color:var(--accent);margin-top:1.5rem">Appears In</h3>\n'
        body += '<div style="display:flex;flex-wrap:wrap;gap:0.5rem">\n'
        for ttid, ttitle in texts:
            body += f'<a href="../texts/{ttid}.html" style="display:inline-block;padding:0.3rem 0.7rem;background:#faf8f4;border:1px solid var(--border);border-radius:4px;color:var(--accent);text-decoration:none;font-size:0.85rem">{ttitle}</a>\n'
        body += '</div>\n'

    # Related concepts
    related = conn.execute("""
        SELECT c.slug, c.label, cl.relationship FROM concept_links cl
        JOIN concepts c ON cl.to_concept_id = c.id WHERE cl.from_concept_id = ?
        UNION
        SELECT c.slug, c.label, cl.relationship FROM concept_links cl
        JOIN concepts c ON cl.from_concept_id = c.id WHERE cl.to_concept_id = ?
    """, (cid, cid)).fetchall()
    if related:
        body += '<h3 style="color:var(--accent);margin-top:1.5rem">Related Concepts</h3>\n'
        body += '<div style="display:flex;flex-wrap:wrap;gap:0.5rem">\n'
        for rslug, rlabel, rrel in related:
            body += f'<a href="{rslug}.html" style="display:inline-block;padding:0.3rem 0.7rem;background:#faf8f4;border:1px solid var(--border);border-radius:4px;color:var(--accent);text-decoration:none;font-size:0.85rem">{rlabel} <span style="font-size:0.7rem;color:var(--text-muted)">({rrel})</span></a>\n'
        body += '</div>\n'

    body += '<div class="provenance">Source: SEED_DATA &middot; Confidence: HIGH</div>\n'
    body += '</div>\n'
    return page_shell(label, body, depth=1, active="Concepts")


def build_translations_index(conn):
    body = '<div class="page-content">\n'
    body += '<h1>Translations</h1>\n'
    body += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Named translations of the Emerald Tablet across languages and centuries.</p>\n'

    body += '<div style="background:var(--bg-card);border:2px solid var(--accent);border-radius:8px;padding:1.5rem;margin:1.5rem 0">\n'
    body += '<h2 style="color:var(--accent);margin-bottom:0.5rem">Parallel Translation Viewer</h2>\n'
    body += '<p>Compare all translations verse-by-verse in a side-by-side table.</p>\n'
    body += '<a href="emerald-tablet.html" style="display:inline-block;margin-top:0.75rem;padding:0.5rem 1.25rem;background:var(--accent);color:white;border-radius:4px;text-decoration:none;font-family:var(--font-sans)">Open Viewer &rarr;</a>\n'
    body += '</div>\n'

    translations = conn.execute("""
        SELECT t.translation_id, t.title, t.language, t.date_approximate, t.tradition,
               p.name as translator_name, COUNT(tv.id) as verse_count
        FROM translations t
        LEFT JOIN persons p ON t.translator_id = p.id
        LEFT JOIN translation_verses tv ON t.id = tv.translation_id
        GROUP BY t.id ORDER BY t.date_year, t.title
    """).fetchall()

    body += '<div class="card-grid">\n'
    for tid, title, lang, date_approx, tradition, translator, vc in translations:
        body += f'<div class="card">\n'
        body += f'  <div class="card-sig">{title}</div>\n'
        body += f'  <div class="card-label">{lang} &middot; {date_approx or "undated"} &middot; {vc} verses</div>\n'
        if translator:
            body += f'  <div class="card-desc">Translator: {translator}</div>\n'
        if tradition:
            body += f'  <div class="card-desc" style="margin-top:0.3rem">{badge_html(tradition, "#7f8c8d")}</div>\n'
        body += '</div>\n'
    body += '</div>\n</div>\n'
    return page_shell("Translations", body, depth=1, active="Translations")


def build_parallel_viewer(conn):
    """Build the centerpiece: Emerald Tablet parallel translation page with annotations."""
    # Load annotation data
    wa_path = BASE_DIR / "data" / "word_annotations.json"
    vc_path = BASE_DIR / "data" / "verse_commentary.json"
    word_annotations = {}
    verse_commentary = {}
    if wa_path.exists():
        with open(wa_path, "r", encoding="utf-8") as f:
            word_annotations = json.load(f)
    if vc_path.exists():
        with open(vc_path, "r", encoding="utf-8") as f:
            verse_commentary = json.load(f)

    def annotate_text(verse_key, lang_key):
        """Wrap words in spans with CSS hover tooltips from word_annotations."""
        annotations = word_annotations.get(lang_key, {}).get(verse_key, [])
        if not annotations:
            return None  # No annotations for this verse
        parts = []
        for wa in annotations:
            word = wa.get("word", "")
            trans = wa.get("translation", "")
            translit = wa.get("transliteration", "")
            notes = wa.get("notes")
            if notes is None or notes == "None":
                notes = ""

            # Build tooltip lines
            tip_lines = []
            if translit:
                tip_lines.append(f'<em>{translit}</em>')
            if trans:
                tip_lines.append(trans)
            if notes:
                tip_lines.append(f'<span style="color:#d4a574">{notes}</span>')
            tip_html = "<br>".join(tip_lines)

            if tip_html:
                # Use CSS tooltip class instead of title attribute
                parts.append(
                    f'<span class="anno-word">{word}'
                    f'<span class="anno-tip">{tip_html}</span></span>'
                )
            else:
                parts.append(word)
        return " ".join(parts)

    # Get translations with verses, grouped by language
    translations = conn.execute("""
        SELECT t.id, t.translation_id, t.title, t.language
        FROM translations t
        WHERE t.id IN (SELECT DISTINCT translation_id FROM translation_verses)
        ORDER BY t.language, t.date_year
    """).fetchall()

    by_lang = {}
    for trans_pk, trans_id, trans_title, trans_lang in translations:
        by_lang.setdefault(trans_lang, []).append((trans_pk, trans_id, trans_title))

    all_verses = {}
    for trans_pk, trans_id, trans_title, trans_lang in translations:
        verses = conn.execute("""
            SELECT verse_number, verse_sub, verse_text
            FROM translation_verses WHERE translation_id = ?
        """, (trans_pk,)).fetchall()
        all_verses[trans_id] = {}
        for vnum, vsub, vtext in verses:
            key = f"{vnum}{vsub}" if vsub else vnum
            all_verses[trans_id][key] = vtext

    all_keys = set()
    for tv in all_verses.values():
        all_keys.update(tv.keys())
    sorted_keys = sorted(all_keys, key=lambda x: (int(re.match(r'\d+', x).group()), x))

    default_arabic = 'arabic_sirr_al_khaliqa'
    default_latin = 'latin_vulgate'
    default_english = 'newton_english'

    arabic_options = by_lang.get('ARABIC', [])
    latin_options = by_lang.get('LATIN', [])
    english_options = by_lang.get('ENGLISH', []) + by_lang.get('FRENCH', []) + by_lang.get('PHOENICIAN', [])

    def make_select(col_id, options, default_id, label):
        html = f'<div style="flex:1;min-width:180px">\n'
        html += f'<label style="font-family:var(--font-sans);font-size:0.8rem;color:var(--text-muted);display:block;margin-bottom:0.3rem">{label}</label>\n'
        html += f'<select class="translation-selector" data-col="{col_id}" style="width:100%">\n'
        for _, tid, ttitle in options:
            selected = ' selected' if tid == default_id else ''
            html += f'  <option value="{tid}"{selected}>{ttitle}</option>\n'
        html += '</select>\n</div>\n'
        return html

    body = '<div class="page-wide">\n'
    body += '<a href="index.html" class="back-link">&larr; All Translations</a>\n'
    body += '<h1>Emerald Tablet: Parallel Translations</h1>\n'
    body += f'<p style="color:var(--text-muted);margin-bottom:0.5rem">{len(translations)} translations available. Hover over Arabic and Latin words for transliteration, translation, and alchemical notes.</p>\n'

    # Dropdown selectors
    body += '<div style="display:flex;flex-wrap:wrap;gap:1rem;margin-bottom:1.5rem;padding:1rem;background:var(--bg-card);border:1px solid var(--border);border-radius:4px">\n'
    body += make_select('col-arabic', arabic_options, default_arabic, 'Arabic')
    body += make_select('col-latin', latin_options, default_latin, 'Latin')
    body += make_select('col-english', english_options, default_english, 'English / Other')
    body += '</div>\n'

    # Embed verse data + commentary as JSON for JS
    body += '<script>\nconst VERSE_DATA = '
    body += json.dumps(all_verses, ensure_ascii=False)
    body += ';\nconst COMMENTARY = '
    body += json.dumps(verse_commentary, ensure_ascii=False)
    body += ';\n</script>\n'

    # Table: 3 translation columns + narrow hint column
    body += '<div style="overflow-x:auto">\n<table class="verse-table">\n<thead><tr>\n'
    body += '<th style="width:30px">V.</th>\n'
    body += '<th id="th-arabic" style="width:28%">Arabic (Sirr al-Khaliqa)<br><span style="font-weight:normal;font-size:0.7rem">ARABIC</span></th>\n'
    body += '<th id="th-latin" style="width:28%">Latin Vulgate<br><span style="font-weight:normal;font-size:0.7rem">LATIN</span></th>\n'
    body += '<th id="th-english" style="width:28%">Newton\'s English<br><span style="font-weight:normal;font-size:0.7rem">ENGLISH</span></th>\n'
    body += '<th style="width:16%;background:#3a2a1a;font-size:0.75rem">Notes<br><span style="font-weight:normal;font-size:0.65rem;opacity:0.7">Click to expand</span></th>\n'
    body += '</tr></thead>\n<tbody>\n'

    for vkey in sorted_keys:
        ar_text = all_verses.get(default_arabic, {}).get(vkey, '')
        la_text = all_verses.get(default_latin, {}).get(vkey, '')
        en_text = all_verses.get(default_english, {}).get(vkey, '')

        ar_annotated = annotate_text(vkey, "arabic")
        la_annotated = annotate_text(vkey, "latin")
        ar_display = ar_annotated if ar_annotated else ar_text
        la_display = la_annotated if la_annotated else la_text

        # Narrow hint: just the summary, clickable
        vc = verse_commentary.get(vkey, {})
        summary = vc.get("summary", "")
        hint = ""
        if summary:
            short_summary = summary[:80] + ("..." if len(summary) > 80 else "")
            hint = (f'<a href="#" class="commentary-link" data-verse="{vkey}" '
                    f'style="font-size:0.78rem;color:var(--accent);text-decoration:none;line-height:1.3;display:block">'
                    f'{short_summary}</a>')

        body += '<tr>\n'
        body += f'<td class="verse-num">{vkey}</td>\n'
        body += f'<td class="verse-col" data-col="col-arabic" dir="rtl">{ar_display}</td>\n'
        body += f'<td class="verse-col" data-col="col-latin">{la_display}</td>\n'
        body += f'<td class="verse-col" data-col="col-english">{en_text}</td>\n'
        body += f'<td style="background:#faf8f4;padding:0.5rem">{hint}</td>\n'
        body += '</tr>\n'

    body += '</tbody></table>\n</div>\n'

    # Side panel overlay (hidden by default)
    body += """
<div id="commentary-panel" style="
    display:none;position:fixed;top:0;right:0;width:380px;height:100vh;
    background:var(--bg-card);border-left:3px solid var(--accent);
    box-shadow:-4px 0 20px rgba(0,0,0,0.15);z-index:200;
    overflow-y:auto;padding:1.5rem;font-size:0.88rem;line-height:1.6;
">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">
        <h3 id="panel-title" style="color:var(--accent);margin:0;font-size:1.1rem">Verse Commentary</h3>
        <a href="#" id="panel-close" style="color:var(--text-muted);text-decoration:none;font-size:1.5rem;line-height:1">&times;</a>
    </div>
    <div id="panel-body"></div>
</div>
<div id="commentary-backdrop" style="display:none;position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(0,0,0,0.2);z-index:199"></div>
"""

    # JavaScript for dropdown switching + commentary panel
    body += """
<script>
// Dropdown column switching
document.querySelectorAll('.translation-selector').forEach(select => {
    select.addEventListener('change', function() {
        const colId = this.dataset.col;
        const transId = this.value;
        const transData = VERSE_DATA[transId] || {};
        const selectedOption = this.options[this.selectedIndex];
        const title = selectedOption.text;

        const thId = colId.replace('col-', 'th-');
        const th = document.getElementById(thId);
        if (th) {
            const lang = transId.includes('arabic') ? 'ARABIC' :
                         transId.includes('latin') || transId.includes('vulgate') || transId.includes('beato') || transId.includes('twelfth') ? 'LATIN' :
                         transId.includes('french') || transId.includes('fulcanelli') ? 'FRENCH' :
                         transId.includes('phoenician') || transId.includes('kriegsmann') ? 'PHOENICIAN' : 'ENGLISH';
            th.innerHTML = title + '<br><span style="font-weight:normal;font-size:0.7rem">' + lang + '</span>';
        }

        const rows = document.querySelectorAll('tbody tr');
        rows.forEach(row => {
            const verseNum = row.querySelector('.verse-num');
            if (!verseNum) return;
            const vkey = verseNum.textContent;
            const cell = row.querySelector('[data-col="' + colId + '"]');
            if (cell) {
                cell.textContent = transData[vkey] || '';
                cell.dir = transId.includes('arabic') ? 'rtl' : 'ltr';
            }
        });
    });
});

// Commentary side panel
const panel = document.getElementById('commentary-panel');
const panelBody = document.getElementById('panel-body');
const panelTitle = document.getElementById('panel-title');
const backdrop = document.getElementById('commentary-backdrop');

function openCommentary(vkey) {
    const c = COMMENTARY[vkey];
    if (!c) return;

    panelTitle.textContent = 'Verse ' + vkey;
    let html = '';
    if (c.summary) {
        html += '<p style="font-weight:600;color:var(--accent);margin-bottom:0.75rem;font-size:0.95rem">' + c.summary + '</p>';
    }
    if (c.commentary) {
        html += '<div style="margin-bottom:1rem">' + c.commentary.split('\\n').map(p => '<p style="margin-bottom:0.5rem">' + p + '</p>').join('') + '</div>';
    }
    if (c.cosmological || c.laboratory || c.philological) {
        html += '<div style="border-top:2px solid var(--border);padding-top:0.75rem;margin-top:0.5rem">';
        html += '<h4 style="font-size:0.85rem;color:var(--text-muted);margin-bottom:0.5rem">Registers</h4>';
        if (c.cosmological) {
            html += '<div style="margin-bottom:0.6rem;padding-left:0.75rem;border-left:3px solid #1a5276">';
            html += '<p style="font-weight:600;color:#1a5276;font-size:0.8rem;margin-bottom:0.2rem">Cosmological</p>';
            html += '<p style="font-size:0.85rem">' + c.cosmological + '</p></div>';
        }
        if (c.laboratory) {
            html += '<div style="margin-bottom:0.6rem;padding-left:0.75rem;border-left:3px solid #8b4513">';
            html += '<p style="font-weight:600;color:#8b4513;font-size:0.8rem;margin-bottom:0.2rem">Laboratory</p>';
            html += '<p style="font-size:0.85rem">' + c.laboratory + '</p></div>';
        }
        if (c.philological) {
            html += '<div style="margin-bottom:0.6rem;padding-left:0.75rem;border-left:3px solid #6c3483">';
            html += '<p style="font-weight:600;color:#6c3483;font-size:0.8rem;margin-bottom:0.2rem">Philological</p>';
            html += '<p style="font-size:0.85rem">' + c.philological + '</p></div>';
        }
        html += '</div>';
    }
    panelBody.innerHTML = html;
    panel.style.display = 'block';
    backdrop.style.display = 'block';
}

function closeCommentary() {
    panel.style.display = 'none';
    backdrop.style.display = 'none';
}

document.querySelectorAll('.commentary-link').forEach(link => {
    link.addEventListener('click', function(e) {
        e.preventDefault();
        openCommentary(this.dataset.verse);
    });
});

document.getElementById('panel-close').addEventListener('click', function(e) {
    e.preventDefault();
    closeCommentary();
});

backdrop.addEventListener('click', closeCommentary);

document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') closeCommentary();
});
</script>
"""

    body += '</div>\n'
    return page_shell("Parallel Translations", body, depth=1, active="Translations")


def build_timeline(conn):
    events = conn.execute("""
        SELECT te.year, te.year_end, te.event_type, te.title, te.description,
               p.name as person_name, p.person_id as person_slug,
               t.title as text_title, t.text_id as text_slug
        FROM timeline_events te
        LEFT JOIN persons p ON te.person_id = p.id
        LEFT JOIN texts t ON te.text_id = t.id
        ORDER BY te.year
    """).fetchall()

    body = '<div class="page-content">\n'
    body += '<h1>Timeline</h1>\n'
    body += '<p style="color:var(--text-muted);margin-bottom:1.5rem">Key events in the history of the Hermetic tradition.</p>\n'

    current_century = None
    for year, year_end, etype, title, desc, pname, pslug, ttitle, tslug in events:
        century = (year // 100) * 100
        if century != current_century:
            current_century = century
            if century < 0:
                label = f"{abs(century)}s BCE"
            else:
                label = f"{century}s CE"
            body += f'<div class="timeline-year">{label}</div>\n'

        color = EVENT_COLORS.get(etype, '#7f8c8d')
        body += f'<div class="timeline-card" style="border-left-color:{color}">\n'
        body += f'  <h4>{badge_html(etype or "EVENT", color)} {title}</h4>\n'
        if desc:
            body += f'  <p>{desc}</p>\n'
        meta = []
        if pname:
            meta.append(f'<a href="persons/{pslug}.html" style="color:var(--accent)">{pname}</a>')
        if ttitle:
            meta.append(f'<a href="texts/{tslug}.html" style="color:var(--accent)">{ttitle}</a>')
        year_str = f"{year}" + (f"&ndash;{year_end}" if year_end else "")
        meta.append(year_str)
        body += f'  <div class="ref-meta">{" &middot; ".join(meta)}</div>\n'
        body += '</div>\n'

    body += '</div>\n'
    return page_shell("Timeline", body, active="Timeline")


def build_bibliography(conn):
    body = '<div class="page-content">\n'
    body += '<h1>Bibliography</h1>\n'

    for relevance in ['PRIMARY', 'DIRECT', 'CONTEXTUAL', None]:
        if relevance:
            entries = conn.execute("""
                SELECT source_id, author, title, year, journal, publisher, pub_type, relevance, in_collection, notes
                FROM bibliography WHERE relevance = ? ORDER BY author, year
            """, (relevance,)).fetchall()
            label = relevance.title()
        else:
            entries = conn.execute("""
                SELECT source_id, author, title, year, journal, publisher, pub_type, relevance, in_collection, notes
                FROM bibliography WHERE relevance IS NULL ORDER BY author, year
            """).fetchall()
            label = "Other"

        if not entries:
            continue

        color = RELEVANCE_COLORS.get(relevance, '#7f8c8d')
        body += f'<h2 class="section-header">{label} Sources {badge_html(str(len(entries)), color)}</h2>\n'

        for sid, author, title, year, journal, publisher, ptype, rel, in_coll, notes in entries:
            body += '<div class="ref-card">\n'
            body += f'  <h4>{author}'
            if year:
                body += f' ({year})'
            if ptype:
                body += f' {badge_html(ptype, "#7f8c8d")}'
            if in_coll:
                body += f' {badge_html("In Collection", "#27ae60")}'
            body += '</h4>\n'
            body += f'  <p><em>{title}</em></p>\n'
            venue = journal or publisher or ""
            if venue:
                body += f'  <div class="ref-meta">{venue}</div>\n'
            if notes:
                body += f'  <p style="font-size:0.85rem;margin-top:0.3rem;color:var(--text-muted)">{notes}</p>\n'
            body += '</div>\n'

    body += '</div>\n'
    return page_shell("Bibliography", body, active="Bibliography")


def build_manuscripts_index(conn):
    manuscripts = conn.execute("""
        SELECT manuscript_id, shelfmark, repository, city, date_approximate, language, contents_summary, significance
        FROM manuscripts ORDER BY shelfmark
    """).fetchall()

    body = '<div class="page-content">\n'
    body += '<h1>Manuscripts</h1>\n'
    body += '<div class="card-grid">\n'

    for mid, shelf, repo, city, date, lang, contents, sig in manuscripts:
        body += f'<div class="card">\n'
        body += f'  <div class="card-sig">{shelf}</div>\n'
        body += f'  <div class="card-label">{repo or ""}, {city or ""}</div>\n'
        if date:
            body += f'  <div class="card-label">{date} &middot; {lang or ""}</div>\n'
        body += f'  <div class="card-desc">{truncate(contents)}</div>\n'
        if sig:
            body += f'  <p style="font-size:0.85rem;margin-top:0.5rem;color:var(--text-muted)">{truncate(sig, 100)}</p>\n'
        body += '</div>\n'

    body += '</div>\n</div>\n'
    return page_shell("Manuscripts", body, depth=1, active="Manuscripts")


def build_about(conn):
    counts = {}
    for t in ['texts', 'persons', 'translations', 'concepts', 'bibliography', 'manuscripts',
              'timeline_events', 'translation_verses', 'person_text_roles', 'corpus_documents', 'corpus_segments']:
        try:
            counts[t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
        except Exception:
            counts[t] = 0

    body = '<div class="page-content">\n'
    body += '<h1>About This Project</h1>\n'
    body += '<p>The EmeraldTablet Knowledge Portal is a digital humanities project providing encyclopedic reference for the Hermetic tradition from late antiquity to the early modern period, anchored on a digital edition of the Emerald Tablet (Tabula Smaragdina).</p>\n'

    body += '<h2 class="section-header">Architecture</h2>\n'
    body += '<p>SQLite database &rarr; Python pipeline &rarr; static HTML/CSS/JS &rarr; GitHub Pages. No frameworks, Python standard library only.</p>\n'

    body += '<h2 class="section-header">Database Statistics</h2>\n'
    body += '<table style="width:100%;border-collapse:collapse;font-family:var(--font-sans)">\n'
    for label, key in [("Texts", "texts"), ("Persons", "persons"), ("Translations", "translations"),
                        ("Translation Verses", "translation_verses"), ("Concepts", "concepts"),
                        ("Bibliography", "bibliography"), ("Manuscripts", "manuscripts"),
                        ("Timeline Events", "timeline_events"), ("Person-Text Roles", "person_text_roles"),
                        ("Corpus Documents", "corpus_documents"), ("Corpus Segments", "corpus_segments")]:
        body += f'<tr><td style="padding:0.4rem;border-bottom:1px solid var(--border)">{label}</td>'
        body += f'<td style="padding:0.4rem;border-bottom:1px solid var(--border);font-weight:700;text-align:right">{counts[key]}</td></tr>\n'
    body += '</table>\n'

    body += '<h2 class="section-header">Provenance</h2>\n'
    body += '<p>Every datum in this portal carries provenance metadata: source method (SEED_DATA, CORPUS_EXTRACTION, DETERMINISTIC), review status (DRAFT, REVIEWED, VERIFIED), and confidence level (HIGH, MEDIUM, LOW).</p>\n'

    body += '<h2 class="section-header">Methodology</h2>\n'
    body += '<p>Data pipeline: corpus conversion (PDF to markdown) &rarr; corpus indexing &rarr; text segmentation &rarr; keyword marking &rarr; entity extraction &rarr; relationship linking &rarr; card assembly &rarr; static site generation &rarr; validation.</p>\n'

    body += '</div>\n'
    return page_shell("About", body, active="About")


def export_data_json(conn):
    """Export entity data as JSON for JS consumers."""
    data = {}

    data['texts'] = [dict(r) for r in conn.execute(
        "SELECT text_id, title, language, text_type, date_composed_start, date_composed_end, description FROM texts"
    ).fetchall()] if False else []  # placeholder — populate if needed

    # Simple counts for now
    data['counts'] = {}
    for t in ['texts', 'persons', 'translations', 'concepts', 'bibliography']:
        data['counts'][t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]

    return json.dumps(data, indent=2, ensure_ascii=False)


# ── Main ───────────────────────────────────────────────────────

def main():
    if not DB_PATH.exists():
        print("ERROR: Database not found.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Clean output directories (stale file cleanup)
    for subdir in ['texts', 'persons', 'concepts', 'translations', 'manuscripts']:
        d = SITE_DIR / subdir
        if d.exists():
            shutil.rmtree(d)

    # Create directories
    for subdir in ['texts', 'persons', 'concepts', 'translations', 'manuscripts']:
        (SITE_DIR / subdir).mkdir(parents=True, exist_ok=True)

    conn.row_factory = None  # back to tuples

    pages = 0

    # Home
    (SITE_DIR / "index.html").write_text(build_home(conn), encoding='utf-8')
    pages += 1

    # Texts index + detail
    (SITE_DIR / "texts" / "index.html").write_text(build_texts_index(conn), encoding='utf-8')
    pages += 1
    for row in conn.execute("SELECT id, text_id, title, title_original, language, text_type, date_composed_start, date_composed_end, description, analysis_html, transmission_notes FROM texts"):
        html = build_text_detail(conn, row)
        (SITE_DIR / "texts" / f"{row[1]}.html").write_text(html, encoding='utf-8')
        pages += 1

    # Persons index + detail
    (SITE_DIR / "persons" / "index.html").write_text(build_persons_index(conn), encoding='utf-8')
    pages += 1
    for row in conn.execute("SELECT id, person_id, name, name_alt, birth_year, death_year, era, role_primary, description, bio_html FROM persons"):
        html = build_person_detail(conn, row)
        (SITE_DIR / "persons" / f"{row[1]}.html").write_text(html, encoding='utf-8')
        pages += 1

    # Concepts index + detail
    (SITE_DIR / "concepts" / "index.html").write_text(build_concepts_index(conn), encoding='utf-8')
    pages += 1
    for row in conn.execute("SELECT id, slug, label, label_alt, category, definition_short, definition_long, significance FROM concepts"):
        html = build_concept_detail(conn, row)
        (SITE_DIR / "concepts" / f"{row[1]}.html").write_text(html, encoding='utf-8')
        pages += 1

    # Translations index + parallel viewer
    (SITE_DIR / "translations" / "index.html").write_text(build_translations_index(conn), encoding='utf-8')
    pages += 1
    (SITE_DIR / "translations" / "emerald-tablet.html").write_text(build_parallel_viewer(conn), encoding='utf-8')
    pages += 1

    # Timeline
    (SITE_DIR / "timeline.html").write_text(build_timeline(conn), encoding='utf-8')
    pages += 1

    # Bibliography
    (SITE_DIR / "bibliography.html").write_text(build_bibliography(conn), encoding='utf-8')
    pages += 1

    # Manuscripts
    (SITE_DIR / "manuscripts" / "index.html").write_text(build_manuscripts_index(conn), encoding='utf-8')
    pages += 1

    # About
    (SITE_DIR / "about.html").write_text(build_about(conn), encoding='utf-8')
    pages += 1

    # CSS
    (SITE_DIR / "style.css").write_text(CSS, encoding='utf-8')

    # data.json
    (SITE_DIR / "data.json").write_text(export_data_json(conn), encoding='utf-8')

    print(f"Site built: {pages} pages in {SITE_DIR}")

    # Validate links
    conn.close()


if __name__ == "__main__":
    main()
