#!/usr/bin/env python3
"""
Query additional Wikidata indicators for all matched leaders.

Reads recovered_qids.csv (from 07_recover_coverage.py) and queries:
  B8  monuments_and_statues — statues/monuments named after (P138) the leader
  B5  national_holiday_birthday — public holidays matching leader's birthday
  B10 state_hagiography — books/works authored by or named after the leader
  B7  state_media_named — media orgs named after the leader

Uses the same small-batch SPARQL pattern as 06_fix_family_indicator.py
to avoid Wikidata timeouts.

Usage:
  python 09_wikidata_extra.py
"""

import csv
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV    = PROJECT_ROOT / "data" / "raw" / "wikidata_extra.csv"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"
RATE_LIMIT = 3.0
BATCH_SIZE = 8


def sparql_query(query: str, retries: int = 3) -> list[dict]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL, params={"query": query},
                headers=headers, timeout=90
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 60)))
                continue
            if resp.status_code == 504:
                time.sleep(15 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except requests.exceptions.ReadTimeout:
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            print(f"    Error (attempt {attempt+1}): {e}")
            time.sleep(5 * (attempt + 1))
    return []


def val(b: dict, key: str) -> str:
    return b.get(key, {}).get("value", "")


# =========================================================================
# B8: Monuments and statues named after leader
# =========================================================================

def query_monuments(qids: list[str]) -> dict[str, list[str]]:
    """Query statues/monuments/sculptures named after leaders."""
    print(f"\nQuerying monuments/statues (B8) for {len(qids)} leaders...")
    results = {q: [] for q in qids}
    total_batches = (len(qids) - 1) // BATCH_SIZE + 1

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        if bn % 20 == 1 or bn == total_batches:
            print(f"  Batch {bn}/{total_batches}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?monumentLabel WHERE {{
          VALUES ?leader {{ {values} }}
          ?monument wdt:P138 ?leader .
          {{ ?monument wdt:P31/wdt:P279* wd:Q179700 . }}   # statue
          UNION {{ ?monument wdt:P31/wdt:P279* wd:Q4989906 . }}  # monument
          UNION {{ ?monument wdt:P31/wdt:P279* wd:Q860861 . }}   # sculpture
          UNION {{ ?monument wdt:P31/wdt:P279* wd:Q575 . }}      # human settlement (cities named after)
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 500
        """
        bindings = sparql_query(query)
        for r in bindings:
            qid = val(r, "leader").split("/")[-1]
            name = val(r, "monumentLabel")
            if qid in results and name and not name.startswith("http"):
                results[qid].append(name)
        time.sleep(RATE_LIMIT)

    found = sum(1 for q in qids if results[q])
    print(f"  {found} leaders with monuments/statues")
    return results


# =========================================================================
# B5: National holiday on leader's birthday
# =========================================================================

def query_birthday_holidays(qids: list[str]) -> dict[str, list[str]]:
    """Check if any public holiday falls on the leader's birthday."""
    print(f"\nQuerying birthday holidays (B5) for {len(qids)} leaders...")
    results = {q: [] for q in qids}
    total_batches = (len(qids) - 1) // BATCH_SIZE + 1

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        if bn % 20 == 1 or bn == total_batches:
            print(f"  Batch {bn}/{total_batches}")

        values = " ".join(f"wd:{q}" for q in batch)
        # Query: find holidays named after the leader, or holidays that
        # are "day of celebration" with the leader as honoree
        query = f"""
        SELECT ?leader ?holidayLabel WHERE {{
          VALUES ?leader {{ {values} }}
          {{
            ?holiday wdt:P138 ?leader .
            ?holiday wdt:P31/wdt:P279* wd:Q1197685 .
          }} UNION {{
            ?holiday wdt:P547 ?leader .
            ?holiday wdt:P31/wdt:P279* wd:Q1197685 .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 200
        """
        bindings = sparql_query(query)
        for r in bindings:
            qid = val(r, "leader").split("/")[-1]
            name = val(r, "holidayLabel")
            if qid in results and name and not name.startswith("http"):
                results[qid].append(name)
        time.sleep(RATE_LIMIT)

    found = sum(1 for q in qids if results[q])
    print(f"  {found} leaders with birthday/named holidays")
    return results


# =========================================================================
# B10: State hagiography (books/works by or about the leader)
# =========================================================================

def query_hagiography(qids: list[str]) -> dict[str, list[str]]:
    """Query for books/works authored by or named after the leader."""
    print(f"\nQuerying hagiography/works (B10) for {len(qids)} leaders...")
    results = {q: [] for q in qids}
    total_batches = (len(qids) - 1) // BATCH_SIZE + 1

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        if bn % 20 == 1 or bn == total_batches:
            print(f"  Batch {bn}/{total_batches}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?workLabel WHERE {{
          VALUES ?leader {{ {values} }}
          {{
            ?work wdt:P50 ?leader .
            ?work wdt:P31/wdt:P279* wd:Q571 .
          }} UNION {{
            ?work wdt:P138 ?leader .
            ?work wdt:P31/wdt:P279* wd:Q571 .
          }} UNION {{
            ?work wdt:P50 ?leader .
            ?work wdt:P31/wdt:P279* wd:Q47461344 .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 500
        """
        bindings = sparql_query(query)
        for r in bindings:
            qid = val(r, "leader").split("/")[-1]
            name = val(r, "workLabel")
            if qid in results and name and not name.startswith("http"):
                results[qid].append(name)
        time.sleep(RATE_LIMIT)

    found = sum(1 for q in qids if results[q])
    print(f"  {found} leaders with authored works/hagiography")
    return results


# =========================================================================
# B7 supplementary: Media organizations named after leader
# =========================================================================

def query_media_named(qids: list[str]) -> dict[str, list[str]]:
    """Query media organizations named after leaders."""
    print(f"\nQuerying media named after (B7) for {len(qids)} leaders...")
    results = {q: [] for q in qids}
    total_batches = (len(qids) - 1) // BATCH_SIZE + 1

    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        if bn % 20 == 1 or bn == total_batches:
            print(f"  Batch {bn}/{total_batches}")

        values = " ".join(f"wd:{q}" for q in batch)
        query = f"""
        SELECT ?leader ?orgLabel WHERE {{
          VALUES ?leader {{ {values} }}
          ?org wdt:P138 ?leader .
          {{
            ?org wdt:P31/wdt:P279* wd:Q11032 .
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q1616075 .
          }} UNION {{
            ?org wdt:P31/wdt:P279* wd:Q15265344 .
          }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        LIMIT 200
        """
        bindings = sparql_query(query)
        for r in bindings:
            qid = val(r, "leader").split("/")[-1]
            name = val(r, "orgLabel")
            if qid in results and name and not name.startswith("http"):
                results[qid].append(name)
        time.sleep(RATE_LIMIT)

    found = sum(1 for q in qids if results[q])
    print(f"  {found} leaders with media orgs named after")
    return results


# =========================================================================
# Main
# =========================================================================

def main():
    # Load recovered QIDs
    print("Loading recovered QIDs...")
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        leaders = list(csv.DictReader(f))
    print(f"  {len(leaders)} leader rows")

    qids = list(set(row["qid"] for row in leaders if row.get("qid")))
    print(f"  {len(qids)} unique QIDs")

    # Query all indicators
    monuments_data = query_monuments(qids)
    holidays_data  = query_birthday_holidays(qids)
    hagiography_data = query_hagiography(qids)
    media_data     = query_media_named(qids)

    # Compile output
    print("\nCompiling extra indicators...")
    output_rows = []
    for row in leaders:
        qid = row.get("qid", "")
        if not qid:
            continue

        monuments = monuments_data.get(qid, [])
        holidays  = holidays_data.get(qid, [])
        works     = hagiography_data.get(qid, [])
        media     = media_data.get(qid, [])

        output_rows.append({
            "qid": qid,
            "leader": row["leader"],
            "ccode": row["ccode"],
            "start_year": row["start_year"],
            "end_year": row["end_year"],
            "monuments_count": len(set(monuments)),
            "monuments_binary": 1 if len(set(monuments)) >= 1 else 0,
            "monuments_details": "; ".join(set(monuments))[:500],
            "holiday_count": len(set(holidays)),
            "holiday_binary": 1 if len(set(holidays)) >= 1 else 0,
            "holiday_details": "; ".join(set(holidays))[:500],
            "hagiography_count": len(set(works)),
            "hagiography_binary": 1 if len(set(works)) >= 2 else 0,
            "hagiography_details": "; ".join(set(works))[:500],
            "media_named_count": len(set(media)),
            "media_named_binary": 1 if len(set(media)) >= 1 else 0,
            "media_named_details": "; ".join(set(media))[:500],
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
    n_monuments = sum(1 for r in output_rows if r["monuments_binary"])
    n_holidays  = sum(1 for r in output_rows if r["holiday_binary"])
    n_works     = sum(1 for r in output_rows if r["hagiography_binary"])
    n_media     = sum(1 for r in output_rows if r["media_named_binary"])

    print(f"\n{'='*60}")
    print("EXTRA WIKIDATA INDICATORS SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders processed:     {len(output_rows)}")
    print(f"B8  Monuments (≥1):    {n_monuments}")
    print(f"B5  Holiday named:     {n_holidays}")
    print(f"B10 Hagiography (≥2):  {n_works}")
    print(f"B7  Media named:       {n_media}")
    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
