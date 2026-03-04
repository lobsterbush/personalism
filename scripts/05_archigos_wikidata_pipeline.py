#!/usr/bin/env python3
"""
Batch Wikidata pipeline for the full Archigos leader set.

1. Reads Archigos CSV (data/raw/archigos.csv)
2. Resolves Wikipedia article titles -> Wikidata QIDs (batch SPARQL)
3. Queries personalism indicators for all leaders (batch SPARQL):
   - A3: Family members in government
   - B3: Places/institutions named after the leader
   - B4: Honorific titles
4. Outputs:
   - data/raw/wikidata_leaders.csv (full results)
   - dashboard/data/personalism.json (dashboard-ready)

Uses batch SPARQL queries (50 leaders per query) to stay within
Wikidata rate limits while completing in ~15-20 minutes.

Usage:
  python 05_archigos_wikidata_pipeline.py [--batch-size 50] [--limit N]
"""

import argparse
import csv
import json
import re
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ARCHIGOS_CSV = PROJECT_ROOT / "data" / "raw" / "archigos.csv"
OUTPUT_CSV = PROJECT_ROOT / "data" / "raw" / "wikidata_leaders.csv"
DASHBOARD_JSON = PROJECT_ROOT / "dashboard" / "data" / "personalism.json"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "PersonalismProject/0.2 (academic research; mailto:charles.crabtree@monash.edu)"
RATE_LIMIT = 2.0  # seconds between batch queries

# COW code -> ISO3 mapping (subset covering most cases)
COW_TO_ISO3 = {
    2: "USA", 20: "CAN", 31: "BHS", 40: "CUB", 41: "HTI", 42: "DOM",
    51: "JAM", 52: "TTO", 53: "BRB", 54: "DMA", 55: "GRD", 56: "LCA",
    57: "VCT", 58: "ATG", 60: "KNA", 70: "MEX", 80: "BLZ", 90: "GTM",
    91: "HND", 92: "SLV", 93: "NIC", 94: "CRI", 95: "PAN", 100: "COL",
    101: "VEN", 110: "GUY", 115: "SUR", 130: "ECU", 135: "PER", 140: "BRA",
    145: "BOL", 150: "PRY", 155: "CHL", 160: "ARG", 165: "URY",
    200: "GBR", 205: "IRL", 210: "NLD", 211: "BEL", 212: "LUX",
    220: "FRA", 225: "CHE", 230: "ESP", 235: "PRT", 255: "DEU",
    260: "DEU", 265: "DEU", 290: "POL", 305: "AUT", 310: "HUN",
    315: "CZE", 316: "CZE", 317: "SVK", 325: "ITA", 331: "SMR",
    338: "MLT", 339: "ALB", 341: "MNE", 343: "MKD", 344: "HRV",
    345: "YUG", 346: "BIH", 349: "SVN", 350: "GRC", 352: "CYP",
    355: "BGR", 359: "MDA", 360: "ROU", 365: "RUS", 366: "EST",
    367: "LVA", 368: "LTU", 369: "UKR", 370: "BLR", 371: "ARM",
    372: "GEO", 373: "AZE", 375: "FIN", 380: "SWE", 385: "NOR",
    390: "DNK", 395: "ISL",
    402: "CPV", 404: "GNB", 411: "GIN", 420: "GMB", 432: "MLI",
    433: "SEN", 434: "BEN", 435: "MRT", 436: "NER", 437: "CIV",
    438: "GIN", 439: "BFA", 450: "LBR", 451: "SLE", 452: "GHA",
    461: "TGO", 471: "CMR", 475: "NGA", 481: "GAB", 482: "CAF",
    483: "TCD", 484: "COG", 490: "COD", 500: "UGA", 501: "KEN",
    510: "TZA", 516: "BDI", 517: "RWA", 520: "SOM", 522: "DJI",
    530: "ETH", 531: "ERI", 540: "AGO", 541: "MOZ", 551: "ZMB",
    552: "ZWE", 553: "MWI", 560: "ZAF", 565: "NAM", 570: "LSO",
    571: "BWA", 572: "SWZ", 580: "MDG", 581: "COM", 590: "MUS",
    591: "SYC", 600: "MAR", 615: "DZA", 616: "TUN", 620: "LBY",
    625: "SDN", 626: "SSD", 630: "IRN", 640: "TUR", 645: "IRQ",
    651: "EGY", 652: "SYR", 660: "LBN", 663: "JOR", 666: "ISR",
    670: "SAU", 678: "YEM", 679: "YEM", 680: "YEM", 690: "KWT",
    692: "BHR", 694: "QAT", 696: "ARE", 698: "OMN",
    700: "AFG", 701: "TKM", 702: "TJK", 703: "KGZ", 704: "UZB",
    705: "KAZ", 710: "CHN", 712: "MNG", 713: "TWN", 730: "KOR",
    731: "PRK", 732: "KOR", 740: "JPN", 750: "IND", 770: "PAK",
    771: "BGD", 775: "MMR", 780: "LKA", 781: "MDV", 790: "NPL",
    800: "THA", 811: "KHM", 812: "LAO", 816: "VNM", 817: "VNM",
    820: "MYS", 830: "SGP", 835: "BRN", 840: "PHL", 850: "IDN",
    860: "TLS", 900: "AUS", 910: "PNG", 920: "NZL", 935: "VUT",
    940: "SLB", 946: "KIR", 947: "TUV", 950: "FJI", 955: "TON",
    970: "NRU", 983: "MHL", 986: "PLW", 987: "FSM", 990: "WSM",
    # Soviet Union and Yugoslavia
    364: "RUS", 345: "SRB", 347: "KOS",
    # GNQ = Equatorial Guinea
    411: "GNQ",
}


# ---------------------------------------------------------------------------
# SPARQL helpers
# ---------------------------------------------------------------------------

def sparql_query(query: str, retries: int = 3) -> list[dict]:
    """Execute a SPARQL query with retries."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/sparql-results+json",
    }
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query},
                headers=headers,
                timeout=60,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", {}).get("bindings", [])
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}")
            time.sleep(5 * (attempt + 1))
    return []


def val(binding: dict, key: str) -> str:
    """Extract value from SPARQL binding."""
    return binding.get(key, {}).get("value", "")


# ---------------------------------------------------------------------------
# Step 1: Resolve Wikipedia titles -> Wikidata QIDs
# ---------------------------------------------------------------------------

def resolve_qids(wiki_titles: list[str], batch_size: int = 40) -> dict[str, str]:
    """
    Resolve Wikipedia article titles to Wikidata QIDs in batches.

    Returns dict mapping wiki_title -> QID.
    """
    print(f"\nResolving {len(wiki_titles)} Wikipedia titles to Wikidata QIDs...")
    title_to_qid: dict[str, str] = {}

    for i in range(0, len(wiki_titles), batch_size):
        batch = wiki_titles[i:i + batch_size]
        print(f"  Batch {i//batch_size + 1}/{(len(wiki_titles)-1)//batch_size + 1} ({len(batch)} titles)")

        # Build VALUES clause
        values = " ".join(f'"{t}"@en' for t in batch)
        query = f"""
        SELECT ?title ?item ?itemLabel WHERE {{
          VALUES ?title {{ {values} }}
          ?article schema:about ?item ;
                   schema:isPartOf <https://en.wikipedia.org/> ;
                   schema:name ?title .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        """

        results = sparql_query(query)
        for r in results:
            title = val(r, "title")
            qid_uri = val(r, "item")
            qid = qid_uri.split("/")[-1] if qid_uri else ""
            if title and qid:
                title_to_qid[title] = qid

        time.sleep(RATE_LIMIT)

    print(f"  Resolved {len(title_to_qid)} of {len(wiki_titles)} titles")
    return title_to_qid


# ---------------------------------------------------------------------------
# Step 2: Batch indicator queries
# ---------------------------------------------------------------------------

def query_family_in_govt_batch(qids: list[str], batch_size: int = 30) -> dict[str, list[dict]]:
    """Query family members in government for a batch of leaders."""
    print(f"\nQuerying family-in-government for {len(qids)} leaders...")
    results: dict[str, list[dict]] = {q: [] for q in qids}

    for i in range(0, len(qids), batch_size):
        batch = qids[i:i + batch_size]
        print(f"  Batch {i//batch_size + 1}/{(len(qids)-1)//batch_size + 1}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?relative ?relativeLabel ?relationship ?position ?positionLabel WHERE {{
          VALUES ?leader {{ {values} }}
          {{
            ?leader wdt:P40 ?relative . BIND("child" AS ?relationship)
          }} UNION {{
            ?leader wdt:P26 ?relative . BIND("spouse" AS ?relationship)
          }} UNION {{
            ?leader wdt:P3373 ?relative . BIND("sibling" AS ?relationship)
          }} UNION {{
            ?relative wdt:P40 ?leader . BIND("parent" AS ?relationship)
          }}
          ?relative wdt:P39 ?position .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        """
        bindings = sparql_query(query)
        for r in bindings:
            leader_qid = val(r, "leader").split("/")[-1]
            if leader_qid in results:
                results[leader_qid].append({
                    "relative": val(r, "relativeLabel"),
                    "relationship": val(r, "relationship"),
                    "position": val(r, "positionLabel"),
                })
        time.sleep(RATE_LIMIT)

    return results


def query_places_named_batch(qids: list[str], batch_size: int = 30) -> dict[str, list[str]]:
    """Query places named after leaders in batch."""
    print(f"\nQuerying places-named-after for {len(qids)} leaders...")
    results: dict[str, list[str]] = {q: [] for q in qids}

    for i in range(0, len(qids), batch_size):
        batch = qids[i:i + batch_size]
        print(f"  Batch {i//batch_size + 1}/{(len(qids)-1)//batch_size + 1}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?place ?placeLabel WHERE {{
          VALUES ?leader {{ {values} }}
          ?place wdt:P138 ?leader .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 1000
        """
        bindings = sparql_query(query)
        for r in bindings:
            leader_qid = val(r, "leader").split("/")[-1]
            place = val(r, "placeLabel")
            if leader_qid in results and place:
                results[leader_qid].append(place)
        time.sleep(RATE_LIMIT)

    return results


def query_titles_batch(qids: list[str], batch_size: int = 30) -> dict[str, list[str]]:
    """Query honorific titles for leaders in batch."""
    print(f"\nQuerying titles/honors for {len(qids)} leaders...")
    results: dict[str, list[str]] = {q: [] for q in qids}

    for i in range(0, len(qids), batch_size):
        batch = qids[i:i + batch_size]
        print(f"  Batch {i//batch_size + 1}/{(len(qids)-1)//batch_size + 1}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?valueLabel ?propType WHERE {{
          VALUES ?leader {{ {values} }}
          {{
            ?leader wdt:P511 ?value . BIND("prefix" AS ?propType)
          }} UNION {{
            ?leader wdt:P1035 ?value . BIND("suffix" AS ?propType)
          }} UNION {{
            ?leader wdt:P97 ?value . BIND("noble_title" AS ?propType)
          }} UNION {{
            ?leader wdt:P1449 ?value . BIND("nickname" AS ?propType)
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        """
        bindings = sparql_query(query)
        for r in bindings:
            leader_qid = val(r, "leader").split("/")[-1]
            title_val = val(r, "valueLabel")
            if leader_qid in results and title_val and not title_val.startswith("http"):
                results[leader_qid].append(title_val)
        time.sleep(RATE_LIMIT)

    return results


# ---------------------------------------------------------------------------
# Step 3: Build output
# ---------------------------------------------------------------------------

def build_dashboard_json(leaders: list[dict]) -> dict:
    """Build dashboard JSON from leader results."""
    from collections import defaultdict

    country_map: dict[str, dict] = defaultdict(lambda: {"leaders": [], "name": ""})
    for l in leaders:
        iso3 = l["iso3"]
        if not country_map[iso3]["name"]:
            country_map[iso3]["name"] = l.get("country_name", iso3)
        country_map[iso3]["leaders"].append({
            "name": l["leader"],
            "start_year": l["start_year"],
            "end_year": l["end_year"],
            "indicators": {
                "family_in_govt": l["family_in_govt_binary"],
                "places_named": l["places_named_binary"],
                "grandiose_titles": l["grandiose_titles_binary"],
            },
        })

    countries = []
    for iso3, data in sorted(country_map.items()):
        countries.append({
            "iso3": iso3,
            "name": data["name"],
            "leaders": sorted(data["leaders"], key=lambda x: x["start_year"]),
        })

    return {
        "metadata": {
            "version": "0.2",
            "description": "Personalism indicators from Wikidata for all Archigos leaders (post-1945)",
            "last_updated": time.strftime("%Y-%m-%d"),
            "source": "Wikidata SPARQL + Archigos 4.1",
            "n_leaders": len(leaders),
            "indicators": [
                {"key": "family_in_govt", "label": "Family in Government", "dimension": "power",
                 "source": "Wikidata", "description": "≥2 immediate family members held political office"},
                {"key": "places_named", "label": "Places Named After", "dimension": "cult",
                 "source": "Wikidata", "description": "≥2 places or institutions named after the leader"},
                {"key": "grandiose_titles", "label": "Grandiose Titles", "dimension": "cult",
                 "source": "Wikidata", "description": "Leader holds honorific prefixes, suffixes, or noble titles"},
            ],
        },
        "countries": countries,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Archigos -> Wikidata pipeline")
    parser.add_argument("--batch-size", type=int, default=40, help="Leaders per SPARQL query")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of leaders (0=all)")
    args = parser.parse_args()

    # Load Archigos
    print("Loading Archigos data...")
    with open(ARCHIGOS_CSV, "r", encoding="utf-8") as f:
        archigos = list(csv.DictReader(f))
    print(f"  {len(archigos)} leaders loaded")

    # Extract Wikipedia titles
    def dbpedia_to_title(uri: str) -> str:
        if not uri:
            return ""
        return uri.replace("http://dbpedia.org/resource/", "").replace("_", " ")

    leaders_with_wiki = []
    for row in archigos:
        title = dbpedia_to_title(row.get("dbpedia_clean", ""))
        if title:
            leaders_with_wiki.append({**row, "wiki_title": title})

    print(f"  {len(leaders_with_wiki)} have Wikipedia titles")

    if args.limit > 0:
        leaders_with_wiki = leaders_with_wiki[:args.limit]
        print(f"  Limited to {args.limit}")

    # Step 1: Resolve QIDs
    unique_titles = list(set(l["wiki_title"] for l in leaders_with_wiki))
    title_to_qid = resolve_qids(unique_titles, batch_size=args.batch_size)

    # Attach QIDs to leaders
    for l in leaders_with_wiki:
        l["qid"] = title_to_qid.get(l["wiki_title"], "")

    leaders_with_qid = [l for l in leaders_with_wiki if l["qid"]]
    print(f"\n{len(leaders_with_qid)} leaders with Wikidata QIDs")

    all_qids = list(set(l["qid"] for l in leaders_with_qid))

    # Step 2: Query indicators
    family_data = query_family_in_govt_batch(all_qids, batch_size=args.batch_size)
    places_data = query_places_named_batch(all_qids, batch_size=args.batch_size)
    titles_data = query_titles_batch(all_qids, batch_size=args.batch_size)

    # Step 3: Compile results
    print("\nCompiling results...")
    output_rows = []
    for l in leaders_with_qid:
        qid = l["qid"]
        cow = int(l.get("ccode", 0) or 0)
        iso3 = COW_TO_ISO3.get(cow, f"COW{cow}")

        family = family_data.get(qid, [])
        family_members = set(f["relative"] for f in family)
        places = places_data.get(qid, [])
        titles = titles_data.get(qid, [])

        start_year = int(l.get("start_year") or 0)
        end_year = int(l.get("end_year") or start_year)

        output_rows.append({
            "qid": qid,
            "leader": l["leader"],
            "iso3": iso3,
            "country_name": l.get("idacr", iso3),
            "start_year": start_year,
            "end_year": end_year if end_year else start_year,
            "entry": l.get("entry", ""),
            "exit": l.get("exit", ""),
            "family_in_govt_count": len(family_members),
            "family_in_govt_binary": 1 if len(family_members) >= 2 else 0,
            "family_in_govt_details": "; ".join(
                f"{f['relative']} ({f['relationship']}): {f['position']}"
                for f in family[:10]
            ),
            "places_named_count": len(places),
            "places_named_binary": 1 if len(places) >= 2 else 0,
            "places_named_details": "; ".join(places[:20]),
            "grandiose_titles_count": len(titles),
            "grandiose_titles_binary": 1 if len(titles) >= 1 else 0,
            "grandiose_titles_details": "; ".join(titles[:10]),
        })

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    if output_rows:
        fieldnames = list(output_rows[0].keys())
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(output_rows)
        print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")

    # Write dashboard JSON
    DASHBOARD_JSON.parent.mkdir(parents=True, exist_ok=True)
    dashboard = build_dashboard_json(output_rows)
    with open(DASHBOARD_JSON, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)
    print(f"Wrote dashboard JSON to {DASHBOARD_JSON}")

    # Summary stats
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders processed: {len(output_rows)}")
    print(f"With family in government (≥2): {sum(1 for r in output_rows if r['family_in_govt_binary'])}")
    print(f"With places named after (≥2):   {sum(1 for r in output_rows if r['places_named_binary'])}")
    print(f"With grandiose titles (≥1):      {sum(1 for r in output_rows if r['grandiose_titles_binary'])}")


if __name__ == "__main__":
    main()
