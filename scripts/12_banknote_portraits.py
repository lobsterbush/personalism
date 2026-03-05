#!/usr/bin/env python3
"""
Identify leaders depicted on banknotes via Wikidata.

Queries Wikidata for banknote items (P180 depicts) that reference
Archigos leaders, coding indicator B1 (currency_portrait).

Strategy:
  1. For each batch of leader QIDs, SPARQL-query banknotes that
     depict (P180) or have main subject (P921) matching the leader.
  2. Also checks postage stamps and coins for broader coverage.

Usage:
  python 12_banknote_portraits.py
"""

import csv
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV    = PROJECT_ROOT / "data" / "raw" / "banknote_indicators.csv"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"


def sparql_query(query: str) -> list[dict]:
    """Execute SPARQL query against Wikidata."""
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL, params={"query": query},
                headers=headers, timeout=90,
            )
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 30)))
                continue
            if resp.status_code == 504:
                time.sleep(15)
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except Exception:
            time.sleep(5 * (attempt + 1))
    return []


def val(b: dict, key: str) -> str:
    return b.get(key, {}).get("value", "")


def query_banknote_leaders(qids: list[str]) -> set[str]:
    """Find which QIDs appear on banknotes/coins/stamps."""
    values = " ".join(f"wd:{q}" for q in qids)

    # Query: items that are banknotes (Q11396789), coins (Q41207),
    # or stamps (Q37930) AND depict (P180) or have main subject (P921)
    # matching one of our leaders
    query = f"""
    SELECT DISTINCT ?leader WHERE {{
      VALUES ?leader {{ {values} }}
      ?item (wdt:P180|wdt:P921) ?leader .
      ?item wdt:P31/wdt:P279* ?type .
      VALUES ?type {{
        wd:Q11396789   # banknote
        wd:Q41207      # coin
        wd:Q37930      # postage stamp
        wd:Q4917288    # banknote series
        wd:Q131344     # denomination of currency
      }}
    }}
    """
    results = sparql_query(query)
    return {val(r, "leader").split("/")[-1] for r in results}


def main():
    # Load leaders
    print("Loading leaders...")
    leaders = []
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("qid"):
                leaders.append(row)
    print(f"  {len(leaders)} leaders with QIDs")

    # Query in batches
    all_qids = list(set(r["qid"] for r in leaders))
    depicted_qids: set[str] = set()
    batch_size = 80

    for i in range(0, len(all_qids), batch_size):
        batch = all_qids[i:i + batch_size]
        n = min(i + batch_size, len(all_qids))
        print(f"  [{n}/{len(all_qids)}] Querying banknotes/coins/stamps...")
        found = query_banknote_leaders(batch)
        depicted_qids |= found
        time.sleep(3)

    print(f"\n  Found {len(depicted_qids)} leaders depicted on currency/stamps")

    # Also try a broader query: leaders who have P18 (image) on items
    # that are currency-related — skip if the above already gave results
    if len(depicted_qids) < 5:
        print("  Trying broader SPARQL for currency depictions...")
        # Fallback: check if leader's Wikidata item has "depicted on" (P1299)
        for i in range(0, len(all_qids), batch_size):
            batch = all_qids[i:i + batch_size]
            values = " ".join(f"wd:{q}" for q in batch)
            query = f"""
            SELECT DISTINCT ?leader WHERE {{
              VALUES ?leader {{ {values} }}
              ?leader wdt:P1299 ?item .
              ?item wdt:P31/wdt:P279* ?type .
              VALUES ?type {{
                wd:Q11396789 wd:Q41207 wd:Q37930
                wd:Q4917288 wd:Q131344
              }}
            }}
            """
            results = sparql_query(query)
            for r in results:
                depicted_qids.add(val(r, "leader").split("/")[-1])
            time.sleep(3)
        print(f"  After broader query: {len(depicted_qids)} depicted")

    # Write output
    output_rows = []
    for ldr in leaders:
        output_rows.append({
            "qid": ldr["qid"],
            "leader": ldr["leader"],
            "ccode": ldr["ccode"],
            "start_year": ldr["start_year"],
            "end_year": ldr["end_year"],
            "currency_portrait": 1 if ldr["qid"] in depicted_qids else 0,
        })

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(output_rows[0].keys())
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    n_portrait = sum(1 for r in output_rows if r["currency_portrait"])
    print(f"\n{'='*60}")
    print("BANKNOTE PORTRAIT SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders with currency/stamp depiction: {n_portrait} / {len(output_rows)}")
    print(f"Wrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
