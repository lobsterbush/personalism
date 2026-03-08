#!/usr/bin/env python3
"""
Text-mine Wikipedia articles for personality cult indicators.

For each leader in recovered_qids.csv, fetches their English Wikipedia article
and searches for textual evidence of:
  - cult_of_personality: mentions of "personality cult" / "cult of personality"
  - hagiography: authored ideological/political works (not memoirs)
  - birthday_holiday: birthday designated as national/public holiday
  - monuments: statues or monuments erected in the leader's honor
  - grandiose_titles: non-standard honorific titles

This supplements Wikidata-based indicators (scripts 03, 09) which suffer from
incomplete structured data. Text evidence is stored for manual auditing.

Uses the MediaWiki API (action=query, prop=extracts) for plaintext extraction.
Rate-limited to 1 request/second per Wikimedia policy.

Usage:
    python 16_wikipedia_text_indicators.py
"""

import csv
import re
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV = PROJECT_ROOT / "data" / "raw" / "wikipedia_text_indicators.csv"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = ("PersonalismProject/0.4 "
              "(academic research; mailto:charles.crabtree@monash.edu)")
RATE_LIMIT = 1.2  # seconds between requests

# =========================================================================
# Keyword patterns for each indicator
# =========================================================================

CULT_PATTERNS = [
    r"cult\s+of\s+personality",
    r"personality\s+cult",
    r"leader\s+cult",
    r"cult[\s-]+like\s+(?:status|following|devotion|worship)",
    r"(?:state|regime|official)\s+(?:worship|veneration|idolat)",
    r"deif(?:y|ied|ication)",
]

# Known ideological works by authoritarian leaders
IDEOLOGICAL_WORKS = [
    r"little\s+red\s+book",
    r"quotations\s+from\s+chairman",
    r"green\s+book",
    r"ruhnama",
    r"juche",
    r"white\s+book",
    r"third\s+universal\s+theory",
    r"manifesto",
    r"collected\s+works",
    r"selected\s+works",
    r"political\s+philosophy",
    r"official\s+ideology",
    r"state\s+ideology",
    r"mandatory\s+(?:reading|study|curriculum)",
    r"required\s+(?:reading|study)",
    r"(?:wrote|authored|published)\s+(?:a\s+)?(?:book|treatise|work)\s+(?:on|about)\s+(?:his|the)\s+(?:political|ideolog|revolution)",
]

HAGIOGRAPHY_PATTERNS = [
    r"official\s+biography",
    r"state[\s-]+(?:sponsored|published|mandated)\s+(?:biography|hagiography|book)",
    r"hagiograph",
    r"propaganda\s+(?:film|book|publication)",
]

BIRTHDAY_PATTERNS = [
    r"birthday\b.*(?:national|public|state)\s+holiday",
    r"(?:national|public|state)\s+holiday\b.*birthday",
    r"(?:day\s+of\s+the\s+(?:sun|shining\s+star|dear\s+leader))",
    r"birthday\b.*(?:celebrated|observed|commemorated)\s+(?:as\s+a\s+)?(?:national|annual|public)",
    r"(?:national|annual)\s+(?:celebration|holiday|day)\b.*(?:birthday|birth\s+anniversary)",
]

MONUMENT_PATTERNS = [
    r"(?:statue|monument|bust|memorial)\s+(?:of|to|honoring|depicting|erected|built|commissioned)\s+(?:him|the\s+leader|the\s+president)",
    r"(?:erected|built|commissioned|unveiled)\s+(?:a\s+)?(?:statue|monument|bust|memorial)",
    r"(?:gold[\s-]+plated|rotating|giant|massive|large[\s-]+scale)\s+statue",
    r"(?:his|the\s+leader'?s?)\s+(?:statue|monument|bust|image|portrait)",
    r"(?:statue|monument)\s+(?:was\s+)?(?:built|erected|placed|installed|commissioned)\s+(?:in\s+)?(?:his\s+honor|every|throughout|across)",
    r"arch\s+of\s+triumph",
    r"(?:mansol|mausol)eum",
]

# Grandiose title patterns — exclude standard titles like Commander-in-Chief
TITLE_PATTERNS = [
    r"father\s+of\s+(?:the\s+)?(?:nation|country|people|revolution|independence)",
    r"(?:eternal|supreme|great|dear|beloved|brilliant|respected|glorious)\s+(?:leader|president|chairman|comrade|guide|commander|marshal)",
    r"(?:leader|guide)\s+of\s+the\s+revolution",
    r"brotherly\s+leader",
    r"turkmenbashi",
    r"conducator",
    r"caudillo",
    r"generalissimo",
    r"(?:president|leader|chairman)\s+for\s+life",
    r"(?:el\s+)?(?:jefe|lider)\s+maximo",
    r"sun\s+of\s+the\s+nation",
    r"teacher\s+of\s+the\s+people",
    r"savior\s+of\s+the\s+(?:nation|people|fatherland)",
    r"hero\s+of\s+the\s+(?:nation|people|revolution)",
    r"(?:number\s+one|first)\s+(?:citizen|peasant|worker)",
    r"marshal\s+of\s+[A-Z]",
    r"king\s+of\s+kings",
]


def fetch_wikipedia_text(wiki_title: str) -> str:
    """Fetch plaintext extract of a Wikipedia article via MediaWiki API.

    Uses prop=extracts first; falls back to action=parse + HTML stripping
    for large articles where TextExtracts returns empty.
    """
    headers = {"User-Agent": USER_AGENT}

    # Attempt 1: prop=extracts (fast, works for most articles)
    params = {
        "action": "query",
        "titles": wiki_title,
        "prop": "extracts",
        "explaintext": True,
        "exlimit": 1,
        "format": "json",
    }
    for attempt in range(2):
        try:
            resp = requests.get(
                WIKIPEDIA_API, params=params, headers=headers, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            pages = data.get("query", {}).get("pages", {})
            for pid, page in pages.items():
                if int(pid) > 0:
                    text = page.get("extract", "")
                    if text:
                        return text
            break  # page exists but extract empty — fall through
        except Exception:
            if attempt < 1:
                time.sleep(3)

    # Attempt 2: action=parse with HTML stripping (handles large articles)
    params2 = {
        "action": "parse",
        "page": wiki_title,
        "prop": "text",
        "format": "json",
        "redirects": True,
        "disabletoc": True,
    }
    for attempt in range(2):
        try:
            resp = requests.get(
                WIKIPEDIA_API, params=params2, headers=headers, timeout=60
            )
            resp.raise_for_status()
            data = resp.json()
            html = data.get("parse", {}).get("text", {}).get("*", "")
            if html:
                text = re.sub(r"<[^>]+>", " ", html)
                text = re.sub(r"&[a-z]+;", " ", text)
                text = re.sub(r"\s+", " ", text).strip()
                return text
        except Exception:
            if attempt < 1:
                time.sleep(5)

    return ""


def resolve_qid_to_title(qid: str) -> str:
    """Resolve a Wikidata QID to its English Wikipedia article title."""
    params = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "sitelinks",
        "sitefilter": "enwiki",
        "format": "json",
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(
            WIKIDATA_API, params=params, headers=headers, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
        entity = data.get("entities", {}).get(qid, {})
        sitelinks = entity.get("sitelinks", {})
        return sitelinks.get("enwiki", {}).get("title", "")
    except Exception:
        return ""


def find_matches(text: str, patterns: list[str], context_chars: int = 150) -> list[str]:
    """Find all regex matches in text, returning context snippets."""
    snippets = []
    text_lower = text.lower()
    for pattern in patterns:
        for match in re.finditer(pattern, text_lower):
            start = max(0, match.start() - context_chars)
            end = min(len(text), match.end() + context_chars)
            snippet = text[start:end].replace("\n", " ").strip()
            snippets.append(f"...{snippet}...")
            break  # one match per pattern is enough
    return snippets


def analyze_article(text: str, leader_name: str) -> dict:
    """Analyze Wikipedia article text for all personality cult indicators."""
    results: dict[str, int | str] = {}

    # cult_of_personality
    cult_snippets = find_matches(text, CULT_PATTERNS)
    results["cult_text"] = 1 if cult_snippets else 0
    results["cult_evidence"] = " | ".join(cult_snippets[:3])[:500]

    # hagiography — authored ideological works
    hagio_snippets = find_matches(text, IDEOLOGICAL_WORKS + HAGIOGRAPHY_PATTERNS)
    results["hagiography_text"] = 1 if hagio_snippets else 0
    results["hagiography_evidence"] = " | ".join(hagio_snippets[:3])[:500]

    # birthday_holiday
    bday_snippets = find_matches(text, BIRTHDAY_PATTERNS)
    results["birthday_text"] = 1 if bday_snippets else 0
    results["birthday_evidence"] = " | ".join(bday_snippets[:3])[:500]

    # monuments
    monument_snippets = find_matches(text, MONUMENT_PATTERNS)
    results["monuments_text"] = 1 if monument_snippets else 0
    results["monuments_evidence"] = " | ".join(monument_snippets[:3])[:500]

    # grandiose_titles
    title_snippets = find_matches(text, TITLE_PATTERNS)
    results["titles_text"] = 1 if title_snippets else 0
    results["titles_evidence"] = " | ".join(title_snippets[:3])[:500]

    return results


def main() -> None:
    # Load leaders
    print("Loading leaders...")
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        leaders = list(csv.DictReader(f))
    print(f"  {len(leaders)} leader-spells")

    # Deduplicate by QID (process each leader once)
    seen_qids: dict[str, dict] = {}
    for row in leaders:
        qid = row.get("qid", "")
        if qid and qid not in seen_qids:
            seen_qids[qid] = row
    unique_leaders = list(seen_qids.values())
    print(f"  {len(unique_leaders)} unique QIDs")

    # Process each leader
    output_rows: list[dict] = []
    n_cult = n_hagio = n_bday = n_monument = n_title = 0
    n_fetched = n_failed = 0

    for i, row in enumerate(unique_leaders):
        qid = row["qid"]
        leader_name = row["leader"]
        wiki_title = row.get("wiki_title", "")

        if i % 100 == 0:
            print(f"\nProgress: {i}/{len(unique_leaders)} "
                  f"(cult={n_cult}, hagio={n_hagio}, bday={n_bday}, "
                  f"monument={n_monument}, title={n_title})")

        # Resolve title if not available
        if not wiki_title:
            wiki_title = resolve_qid_to_title(qid)
            time.sleep(0.5)

        if not wiki_title:
            n_failed += 1
            output_rows.append({
                "qid": qid, "leader": leader_name,
                "wiki_title": "",
                "cult_text": 0, "cult_evidence": "",
                "hagiography_text": 0, "hagiography_evidence": "",
                "birthday_text": 0, "birthday_evidence": "",
                "monuments_text": 0, "monuments_evidence": "",
                "titles_text": 0, "titles_evidence": "",
                "article_length": 0,
            })
            continue

        # Fetch article
        text = fetch_wikipedia_text(wiki_title)
        time.sleep(RATE_LIMIT)

        if not text:
            n_failed += 1
            output_rows.append({
                "qid": qid, "leader": leader_name,
                "wiki_title": wiki_title,
                "cult_text": 0, "cult_evidence": "",
                "hagiography_text": 0, "hagiography_evidence": "",
                "birthday_text": 0, "birthday_evidence": "",
                "monuments_text": 0, "monuments_evidence": "",
                "titles_text": 0, "titles_evidence": "",
                "article_length": 0,
            })
            continue

        n_fetched += 1

        # Analyze
        results = analyze_article(text, leader_name)
        results["qid"] = qid
        results["leader"] = leader_name
        results["wiki_title"] = wiki_title
        results["article_length"] = len(text)

        if results["cult_text"]:
            n_cult += 1
        if results["hagiography_text"]:
            n_hagio += 1
        if results["birthday_text"]:
            n_bday += 1
        if results["monuments_text"]:
            n_monument += 1
        if results["titles_text"]:
            n_title += 1

        output_rows.append(results)

    # Write output
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "qid", "leader", "wiki_title", "article_length",
        "cult_text", "cult_evidence",
        "hagiography_text", "hagiography_evidence",
        "birthday_text", "birthday_evidence",
        "monuments_text", "monuments_evidence",
        "titles_text", "titles_evidence",
    ]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    # Summary
    print(f"\n{'='*60}")
    print("WIKIPEDIA TEXT INDICATORS SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders processed:     {len(output_rows)}")
    print(f"Articles fetched:      {n_fetched}")
    print(f"Articles failed:       {n_failed}")
    print(f"  cult_of_personality: {n_cult}")
    print(f"  hagiography:         {n_hagio}")
    print(f"  birthday_holiday:    {n_bday}")
    print(f"  monuments:           {n_monument}")
    print(f"  grandiose_titles:    {n_title}")
    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
