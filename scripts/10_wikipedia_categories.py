#!/usr/bin/env python3
"""
Scrape Wikipedia categories for personalism-related indicators.

Queries English Wikipedia's category system for:
  - Category:Presidents_for_life → A2 president_for_life
  - Category:Cult_of_personality → supplementary cult indicator
  - Subcategories of "Authoritarian rulers" and similar

Matches category members to Archigos leaders via Wikidata QID resolution.

Usage:
  python 10_wikipedia_categories.py
"""

import csv
import json
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV    = PROJECT_ROOT / "data" / "raw" / "wikipedia_categories.csv"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
WIKIDATA_API  = "https://www.wikidata.org/w/api.php"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"

# Categories to scrape (category name → indicator field)
CATEGORIES = {
    "Presidents for life": "president_for_life",
    "Cults of personality": "cult_of_personality",
    "Totalitarian rulers": "totalitarian_ruler",
    "Dictators": "dictator_category",
    "People who have been declared combatants of national liberation": "national_liberation",
}

# Also check subcategories of these for broader coverage
SUBCATEGORY_ROOTS = [
    "Cult of personality",
    "Presidents for life",
]


def api_get(url: str, params: dict) -> dict | None:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def get_category_members(category: str, cmtype: str = "page", max_pages: int = 20) -> list[str]:
    """Get all page titles in a Wikipedia category."""
    titles = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Category:{category}",
        "cmtype": cmtype,
        "cmlimit": 500,
        "format": "json",
    }

    for _ in range(max_pages):
        data = api_get(WIKIPEDIA_API, params)
        if not data:
            break

        for member in data.get("query", {}).get("categorymembers", []):
            titles.append(member["title"])

        if "continue" in data:
            params["cmcontinue"] = data["continue"]["cmcontinue"]
        else:
            break
        time.sleep(0.5)

    return titles


def get_subcategories(category: str) -> list[str]:
    """Get subcategory names for a category."""
    members = get_category_members(category, cmtype="subcat")
    # Strip "Category:" prefix
    return [m.replace("Category:", "") for m in members]


def titles_to_qids(titles: list[str], batch_size: int = 50) -> dict[str, str]:
    """Resolve Wikipedia page titles to Wikidata QIDs."""
    title_to_qid = {}

    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        params = {
            "action": "query",
            "titles": "|".join(batch),
            "prop": "pageprops",
            "ppprop": "wikibase_item",
            "format": "json",
        }
        data = api_get(WIKIPEDIA_API, params)
        if not data:
            continue

        for pid, page in data.get("query", {}).get("pages", {}).items():
            if int(pid) > 0:
                qid = page.get("pageprops", {}).get("wikibase_item", "")
                if qid:
                    title_to_qid[page.get("title", "")] = qid

        time.sleep(0.5)

    return title_to_qid


def main():
    # Load known QIDs from recovered data
    print("Loading recovered QIDs...")
    known_qids = {}
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("qid"):
                known_qids[row["qid"]] = row

    print(f"  {len(known_qids)} known leader QIDs")

    # Scrape categories
    all_category_qids: dict[str, set] = {}  # qid -> set of category labels

    for cat_name, field_name in CATEGORIES.items():
        print(f"\nScraping Category:{cat_name}...")
        titles = get_category_members(cat_name)
        print(f"  {len(titles)} members")

        # Also get subcategories
        subcats = get_subcategories(cat_name)
        for subcat in subcats[:10]:  # limit depth
            sub_titles = get_category_members(subcat)
            titles.extend(sub_titles)
            print(f"    Subcat '{subcat}': {len(sub_titles)} members")
            time.sleep(0.5)

        titles = list(set(titles))
        print(f"  Total unique titles: {len(titles)}")

        # Resolve to QIDs
        qid_map = titles_to_qids(titles)
        print(f"  Resolved to {len(qid_map)} QIDs")

        # Match against known leaders
        matched = 0
        for title, qid in qid_map.items():
            if qid in known_qids:
                if qid not in all_category_qids:
                    all_category_qids[qid] = set()
                all_category_qids[qid].add(field_name)
                matched += 1

        print(f"  Matched to {matched} Archigos leaders")

    # Compile output for ALL leaders (with 0/1 for each category)
    print("\nCompiling category indicators...")
    output_rows = []
    indicator_fields = list(CATEGORIES.values())

    for qid, row in known_qids.items():
        cats = all_category_qids.get(qid, set())
        out = {
            "qid": qid,
            "leader": row["leader"],
            "ccode": row["ccode"],
            "start_year": row["start_year"],
            "end_year": row["end_year"],
        }
        for field in indicator_fields:
            out[field] = 1 if field in cats else 0

        # Composite: any cult/personality indicator
        out["any_cult_category"] = 1 if cats else 0

        output_rows.append(out)

    # Write
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if output_rows:
        fieldnames = list(output_rows[0].keys())
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)

    # Summary
    print(f"\n{'='*60}")
    print("WIKIPEDIA CATEGORIES SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders processed: {len(output_rows)}")
    for field in indicator_fields:
        n = sum(1 for r in output_rows if r[field])
        print(f"  {field:30s}: {n}")
    n_any = sum(1 for r in output_rows if r["any_cult_category"])
    print(f"  {'any_cult_category':30s}: {n_any}")
    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
