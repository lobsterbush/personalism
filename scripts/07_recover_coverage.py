#!/usr/bin/env python3
"""
Recover Wikidata coverage for Archigos leaders who failed initial matching.

Two strategies:
1. For 282 leaders with Wikipedia titles but no QID: resolve Wikipedia
   redirects via the MediaWiki API, then retry SPARQL resolution.
2. For 1,229 leaders with no Wikipedia link: fuzzy-search Wikidata
   using name + country via the MediaWiki API search service.

Outputs data/raw/recovered_qids.csv with new QID mappings, then
re-runs the three existing indicator queries for newly matched leaders.

Usage:
  python 07_recover_coverage.py
"""

import csv
import json
import re
import time
import urllib.parse
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ARCHIGOS_CSV = PROJECT_ROOT / "data" / "raw" / "archigos.csv"
EXISTING_CSV = PROJECT_ROOT / "data" / "raw" / "wikidata_leaders.csv"
OUTPUT_CSV   = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIPEDIA_API   = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"
RATE_LIMIT = 2.0

# COW -> country name for fuzzy search context
COW_TO_COUNTRY = {
    2: "United States", 20: "Canada", 40: "Cuba", 41: "Haiti", 42: "Dominican Republic",
    51: "Jamaica", 70: "Mexico", 90: "Guatemala", 91: "Honduras", 92: "El Salvador",
    93: "Nicaragua", 94: "Costa Rica", 95: "Panama", 100: "Colombia", 101: "Venezuela",
    110: "Guyana", 130: "Ecuador", 135: "Peru", 140: "Brazil", 145: "Bolivia",
    150: "Paraguay", 155: "Chile", 160: "Argentina", 165: "Uruguay",
    200: "United Kingdom", 205: "Ireland", 210: "Netherlands", 211: "Belgium",
    212: "Luxembourg", 220: "France", 225: "Switzerland", 230: "Spain", 235: "Portugal",
    255: "Germany", 260: "Germany", 265: "Germany", 290: "Poland", 305: "Austria",
    310: "Hungary", 315: "Czechoslovakia", 316: "Czech Republic", 317: "Slovakia",
    325: "Italy", 338: "Malta", 339: "Albania", 341: "Montenegro", 343: "North Macedonia",
    344: "Croatia", 345: "Yugoslavia", 346: "Bosnia", 349: "Slovenia", 350: "Greece",
    352: "Cyprus", 355: "Bulgaria", 359: "Moldova", 360: "Romania",
    364: "Russia", 365: "Russia", 366: "Estonia", 367: "Latvia", 368: "Lithuania",
    369: "Ukraine", 370: "Belarus", 371: "Armenia", 372: "Georgia", 373: "Azerbaijan",
    375: "Finland", 380: "Sweden", 385: "Norway", 390: "Denmark", 395: "Iceland",
    402: "Cape Verde", 404: "Guinea-Bissau", 411: "Guinea", 420: "Gambia",
    432: "Mali", 433: "Senegal", 434: "Benin", 435: "Mauritania", 436: "Niger",
    437: "Ivory Coast", 438: "Guinea", 439: "Burkina Faso", 450: "Liberia",
    451: "Sierra Leone", 452: "Ghana", 461: "Togo", 471: "Cameroon", 475: "Nigeria",
    481: "Gabon", 482: "Central African Republic", 483: "Chad", 484: "Congo",
    490: "Congo", 500: "Uganda", 501: "Kenya", 510: "Tanzania", 516: "Burundi",
    517: "Rwanda", 520: "Somalia", 522: "Djibouti", 530: "Ethiopia", 531: "Eritrea",
    540: "Angola", 541: "Mozambique", 551: "Zambia", 552: "Zimbabwe", 553: "Malawi",
    560: "South Africa", 565: "Namibia", 570: "Lesotho", 571: "Botswana",
    572: "Eswatini", 580: "Madagascar", 590: "Mauritius", 600: "Morocco",
    615: "Algeria", 616: "Tunisia", 620: "Libya", 625: "Sudan", 626: "South Sudan",
    630: "Iran", 640: "Turkey", 645: "Iraq", 651: "Egypt", 652: "Syria",
    660: "Lebanon", 663: "Jordan", 666: "Israel", 670: "Saudi Arabia",
    678: "Yemen", 679: "Yemen", 680: "Yemen", 690: "Kuwait", 694: "Qatar",
    696: "UAE", 698: "Oman", 700: "Afghanistan", 701: "Turkmenistan",
    702: "Tajikistan", 703: "Kyrgyzstan", 704: "Uzbekistan", 705: "Kazakhstan",
    710: "China", 712: "Mongolia", 713: "Taiwan", 730: "Korea", 731: "North Korea",
    732: "South Korea", 740: "Japan", 750: "India", 770: "Pakistan",
    771: "Bangladesh", 775: "Myanmar", 780: "Sri Lanka", 790: "Nepal",
    800: "Thailand", 811: "Cambodia", 812: "Laos", 816: "Vietnam", 817: "Vietnam",
    820: "Malaysia", 830: "Singapore", 840: "Philippines", 850: "Indonesia",
    900: "Australia", 910: "Papua New Guinea", 920: "New Zealand", 950: "Fiji",
}


def api_get(url: str, params: dict, retries: int = 3) -> dict | None:
    """Generic API GET with retries."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 30)))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                return None
    return None


def sparql_query(query: str) -> list[dict]:
    """Execute Wikidata SPARQL query."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL, params={"query": query},
                headers=headers, timeout=90
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 30)))
                continue
            if resp.status_code == 504:
                time.sleep(15)
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except Exception as e:
            time.sleep(5 * (attempt + 1))
    return []


def val(b: dict, key: str) -> str:
    return b.get(key, {}).get("value", "")


# =========================================================================
# Strategy 1: Resolve Wikipedia redirects
# =========================================================================

def resolve_redirects(titles: list[str], batch_size: int = 50) -> dict[str, str]:
    """
    Use Wikipedia API to resolve redirects for titles.
    Returns dict: original_title -> resolved_title
    """
    print(f"\nResolving redirects for {len(titles)} titles...")
    resolved = {}
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "redirects": 1,
            "format": "json",
        }
        data = api_get(WIKIPEDIA_API, params)
        if not data:
            continue

        # Build redirect map
        redirects = {}
        for r in data.get("query", {}).get("redirects", []):
            redirects[r["from"]] = r["to"]
        # Normalizations
        for n in data.get("query", {}).get("normalized", []):
            if n["to"] in redirects:
                redirects[n["from"]] = redirects[n["to"]]
            else:
                redirects[n["from"]] = n["to"]

        # Check which pages exist
        pages = data.get("query", {}).get("pages", {})
        valid_titles = set()
        for pid, page in pages.items():
            if int(pid) > 0:  # negative IDs = missing pages
                valid_titles.add(page.get("title", ""))

        for title in batch:
            final = redirects.get(title, title)
            if final in valid_titles:
                resolved[title] = final

        time.sleep(0.5)

    print(f"  Resolved {len(resolved)} redirects")
    return resolved


def resolve_titles_to_qids(titles: list[str], batch_size: int = 40) -> dict[str, str]:
    """Resolve Wikipedia titles to Wikidata QIDs via SPARQL."""
    title_to_qid = {}
    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        values = " ".join(f'"{t}"@en' for t in batch)
        query = f"""
        SELECT ?title ?item WHERE {{
          VALUES ?title {{ {values} }}
          ?article schema:about ?item ;
                   schema:isPartOf <https://en.wikipedia.org/> ;
                   schema:name ?title .
        }}
        """
        results = sparql_query(query)
        for r in results:
            t = val(r, "title")
            qid = val(r, "item").split("/")[-1]
            if t and qid:
                title_to_qid[t] = qid
        time.sleep(RATE_LIMIT)
    return title_to_qid


# =========================================================================
# Strategy 2: Fuzzy Wikidata search for leaders without Wikipedia
# =========================================================================

def fuzzy_search_leader(name: str, country: str, start_year: int) -> str | None:
    """
    Search Wikidata for a leader using the MediaWiki API search.
    Returns QID if a plausible match found, else None.
    """
    # Clean name
    search_term = f"{name} {country} politician"

    params = {
        "action": "wbsearchentities",
        "search": name,
        "language": "en",
        "type": "item",
        "limit": 5,
        "format": "json",
    }
    data = api_get("https://www.wikidata.org/w/api.php", params)
    if not data or "search" not in data:
        return None

    for result in data["search"]:
        desc = result.get("description", "").lower()
        qid = result.get("id", "")

        # Basic filtering: should be a politician/leader/president/etc.
        political_terms = ["politician", "president", "prime minister", "leader",
                          "head of state", "minister", "dictator", "king", "queen",
                          "emperor", "sultan", "shah", "premier", "chancellor",
                          "governor", "general", "military"]
        if any(term in desc for term in political_terms):
            # Additional check: country name should appear in description
            country_lower = country.lower()
            if country_lower in desc or len(data["search"]) == 1:
                return qid

    return None


# =========================================================================
# Main
# =========================================================================

def main():
    # Load Archigos
    print("Loading Archigos data...")
    with open(ARCHIGOS_CSV, "r", encoding="utf-8") as f:
        archigos = list(csv.DictReader(f))
    print(f"  {len(archigos)} total leaders")

    # Load existing matches
    existing_qids = set()
    if EXISTING_CSV.exists():
        with open(EXISTING_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("qid"):
                    existing_qids.add(row["qid"])
    print(f"  {len(existing_qids)} already matched")

    # Parse DBpedia URIs
    def dbpedia_to_title(uri: str) -> str:
        if not uri:
            return ""
        return uri.replace("http://dbpedia.org/resource/", "").replace("_", " ")

    # Separate leaders by status
    with_wiki_no_qid = []
    no_wiki = []
    already_matched_titles = set()

    # First pass: identify already-matched Wikipedia titles
    for row in archigos:
        title = dbpedia_to_title(row.get("dbpedia_clean", ""))
        if title:
            row["_wiki_title"] = title
        else:
            row["_wiki_title"] = ""

    # We need to know which titles were already resolved
    # Re-resolve all titles to identify which ones succeeded
    wiki_titles = list(set(r["_wiki_title"] for r in archigos if r["_wiki_title"]))
    print(f"\n{len(wiki_titles)} unique Wikipedia titles in Archigos")

    print("Resolving existing titles to find gaps...")
    existing_title_qid = resolve_titles_to_qids(wiki_titles, batch_size=40)
    print(f"  {len(existing_title_qid)} resolve directly")

    failed_titles = [t for t in wiki_titles if t not in existing_title_qid]
    print(f"  {len(failed_titles)} failed — trying redirect resolution")

    # ---- Strategy 1: Redirect resolution ----
    redirect_map = resolve_redirects(failed_titles)
    redirected_titles = [redirect_map[t] for t in failed_titles if t in redirect_map and redirect_map[t] != t]
    unique_redirected = list(set(redirected_titles))

    if unique_redirected:
        print(f"\nResolving {len(unique_redirected)} redirected titles to QIDs...")
        redirect_qids = resolve_titles_to_qids(unique_redirected, batch_size=40)
        print(f"  {len(redirect_qids)} new QIDs via redirects")

        # Merge into existing mapping
        for orig_title in failed_titles:
            if orig_title in redirect_map:
                resolved = redirect_map[orig_title]
                if resolved in redirect_qids:
                    existing_title_qid[orig_title] = redirect_qids[resolved]

    still_failed = [t for t in wiki_titles if t not in existing_title_qid]
    print(f"  {len(still_failed)} still unresolved after redirects")

    # ---- Strategy 2: Fuzzy search for leaders without any Wikipedia ----
    leaders_no_wiki = [r for r in archigos if not r["_wiki_title"]]
    print(f"\n{len(leaders_no_wiki)} leaders without Wikipedia titles — running fuzzy search...")

    fuzzy_matches = {}
    for idx, row in enumerate(leaders_no_wiki):
        name = row.get("leader", "")
        cow = int(row.get("ccode", 0) or 0)
        country = COW_TO_COUNTRY.get(cow, "")
        start_year = int(row.get("start_year", 0) or 0)

        if not name or not country:
            continue

        if idx % 100 == 0:
            print(f"  Searching {idx}/{len(leaders_no_wiki)}...")

        qid = fuzzy_search_leader(name, country, start_year)
        if qid and qid not in existing_qids:
            fuzzy_matches[f"{name}|{cow}|{start_year}"] = qid

        time.sleep(1.0)  # conservative rate limit for Wikidata API

    print(f"  Found {len(fuzzy_matches)} new matches via fuzzy search")

    # ---- Compile all new QID mappings ----
    print("\nCompiling recovered QIDs...")
    output_rows = []

    for row in archigos:
        title = row["_wiki_title"]
        name = row.get("leader", "")
        cow = int(row.get("ccode", 0) or 0)
        start_year = int(row.get("start_year", 0) or 0)

        qid = ""
        source = ""

        if title and title in existing_title_qid:
            qid = existing_title_qid[title]
            if title in redirect_map and redirect_map[title] != title:
                source = "redirect"
            else:
                source = "direct"
        else:
            key = f"{name}|{cow}|{start_year}"
            if key in fuzzy_matches:
                qid = fuzzy_matches[key]
                source = "fuzzy"

        if qid:
            output_rows.append({
                "leader": name,
                "ccode": cow,
                "start_year": start_year,
                "end_year": int(row.get("end_year", 0) or 0),
                "idacr": row.get("idacr", ""),
                "qid": qid,
                "source": source,
                "wiki_title": title,
            })

    # Write output
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if output_rows:
        fieldnames = list(output_rows[0].keys())
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)

    # Summary
    direct = sum(1 for r in output_rows if r["source"] == "direct")
    redirect = sum(1 for r in output_rows if r["source"] == "redirect")
    fuzzy = sum(1 for r in output_rows if r["source"] == "fuzzy")
    total_qids = len(set(r["qid"] for r in output_rows))

    print(f"\n{'='*60}")
    print("COVERAGE RECOVERY SUMMARY")
    print(f"{'='*60}")
    print(f"Total Archigos leaders:     {len(archigos)}")
    print(f"Total with QIDs now:        {len(output_rows)} ({len(output_rows)/len(archigos)*100:.1f}%)")
    print(f"  - Direct resolution:      {direct}")
    print(f"  - Via redirect:           {redirect}")
    print(f"  - Via fuzzy search:       {fuzzy}")
    print(f"Unique QIDs:                {total_qids}")
    print(f"Previously matched:         {len(existing_qids)}")
    print(f"Net new QIDs:               {total_qids - len(existing_qids)}")
    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
