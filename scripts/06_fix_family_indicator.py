#!/usr/bin/env python3
"""
Re-query the family-in-government indicator with smaller batches
and simplified SPARQL to avoid Wikidata timeouts.

Reads the existing wikidata_leaders.csv, queries family data for each
leader's QID in small batches (5 at a time) with separate queries per
relationship type, then patches the CSV and dashboard JSON.
"""

import csv
import json
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEADERS_CSV = PROJECT_ROOT / "data" / "raw" / "wikidata_leaders.csv"
DASHBOARD_JSON = PROJECT_ROOT / "dashboard" / "data" / "personalism.json"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "PersonalismProject/0.2 (academic research; mailto:charles.crabtree@monash.edu)"
RATE_LIMIT = 3.0  # be more conservative


def sparql_query(query: str, retries: int = 3, timeout: int = 90) -> list[dict]:
    """Execute a SPARQL query with retries and longer timeout."""
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
                timeout=timeout,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 504:
                print(f"    504 timeout (attempt {attempt+1})")
                time.sleep(10 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except requests.exceptions.ReadTimeout:
            print(f"    Read timeout (attempt {attempt+1})")
            time.sleep(10 * (attempt + 1))
        except Exception as e:
            print(f"    Error (attempt {attempt+1}): {e}")
            time.sleep(5 * (attempt + 1))
    return []


def val(binding: dict, key: str) -> str:
    return binding.get(key, {}).get("value", "")


# Relationship properties to check
RELATIONSHIPS = [
    ("P40", "child"),     # child
    ("P26", "spouse"),    # spouse
    ("P3373", "sibling"), # sibling
]


def query_relatives_with_positions(qids: list[str], prop: str, rel_label: str) -> dict[str, list[dict]]:
    """
    For a small batch of QIDs, find relatives via one property
    who held a political position (P39).
    """
    results: dict[str, list[dict]] = {q: [] for q in qids}
    values = " ".join(f"wd:{q}" for q in qids)

    query = f"""
    SELECT ?leader ?relativeLabel ?posLabel WHERE {{
      VALUES ?leader {{ {values} }}
      ?leader wdt:{prop} ?relative .
      ?relative wdt:P39 ?pos .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    LIMIT 500
    """
    bindings = sparql_query(query)
    for r in bindings:
        leader_qid = val(r, "leader").split("/")[-1]
        if leader_qid in results:
            results[leader_qid].append({
                "relative": val(r, "relativeLabel"),
                "relationship": rel_label,
                "position": val(r, "posLabel"),
            })
    return results


def query_parents_with_positions(qids: list[str]) -> dict[str, list[dict]]:
    """Find parents (reverse P40) who held political positions."""
    results: dict[str, list[dict]] = {q: [] for q in qids}
    values = " ".join(f"wd:{q}" for q in qids)

    query = f"""
    SELECT ?leader ?relativeLabel ?posLabel WHERE {{
      VALUES ?leader {{ {values} }}
      ?relative wdt:P40 ?leader .
      ?relative wdt:P39 ?pos .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    LIMIT 500
    """
    bindings = sparql_query(query)
    for r in bindings:
        leader_qid = val(r, "leader").split("/")[-1]
        if leader_qid in results:
            results[leader_qid].append({
                "relative": val(r, "relativeLabel"),
                "relationship": "parent",
                "position": val(r, "posLabel"),
            })
    return results


def main():
    # Load existing data
    print("Loading existing leader data...")
    with open(LEADERS_CSV, "r", encoding="utf-8") as f:
        leaders = list(csv.DictReader(f))
    print(f"  {len(leaders)} leaders")

    qids = list(set(row["qid"] for row in leaders if row["qid"]))
    print(f"  {len(qids)} unique QIDs")

    # Query each relationship type separately in small batches
    BATCH_SIZE = 5
    family_data: dict[str, list[dict]] = {q: [] for q in qids}

    for prop, rel_label in RELATIONSHIPS:
        print(f"\nQuerying {rel_label} (wdt:{prop})...")
        for i in range(0, len(qids), BATCH_SIZE):
            batch = qids[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(qids) - 1) // BATCH_SIZE + 1
            if batch_num % 20 == 1 or batch_num == total_batches:
                print(f"  Batch {batch_num}/{total_batches}")

            results = query_relatives_with_positions(batch, prop, rel_label)
            for qid, entries in results.items():
                family_data[qid].extend(entries)
            time.sleep(RATE_LIMIT)

    # Parents (reverse P40)
    print(f"\nQuerying parents (reverse P40)...")
    for i in range(0, len(qids), BATCH_SIZE):
        batch = qids[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(qids) - 1) // BATCH_SIZE + 1
        if batch_num % 20 == 1 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches}")

        results = query_parents_with_positions(batch)
        for qid, entries in results.items():
            family_data[qid].extend(entries)
        time.sleep(RATE_LIMIT)

    # Summarise findings
    leaders_with_family = sum(1 for q in qids if family_data[q])
    print(f"\n{'='*60}")
    print(f"Leaders with any family in govt: {leaders_with_family}")

    # Show details
    for qid in qids:
        if family_data[qid]:
            names = set(f"{f['relative']} ({f['relationship']})" for f in family_data[qid])
            matching_leader = next((l["leader"] for l in leaders if l["qid"] == qid), qid)
            print(f"  {matching_leader}: {'; '.join(names)}")

    # Patch the CSV
    print(f"\nPatching {LEADERS_CSV}...")
    for row in leaders:
        qid = row["qid"]
        family = family_data.get(qid, [])
        family_members = set(f["relative"] for f in family)
        row["family_in_govt_count"] = str(len(family_members))
        row["family_in_govt_binary"] = "1" if len(family_members) >= 2 else "0"
        row["family_in_govt_details"] = "; ".join(
            f"{f['relative']} ({f['relationship']}): {f['position']}"
            for f in family[:10]
        )

    fieldnames = list(leaders[0].keys())
    with open(LEADERS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leaders)
    print(f"  Updated {len(leaders)} rows")

    # Patch dashboard JSON
    print(f"Patching {DASHBOARD_JSON}...")
    with open(DASHBOARD_JSON, "r", encoding="utf-8") as f:
        dashboard = json.load(f)

    # Build QID->family lookup for binary
    qid_binary = {}
    for row in leaders:
        qid_binary[row["qid"]] = int(row["family_in_govt_binary"])

    # Map leader name+country to QID
    name_country_qid = {}
    for row in leaders:
        key = (row["leader"], row["country_name"])
        name_country_qid[key] = row["qid"]

    patched = 0
    for country in dashboard["countries"]:
        for leader in country["leaders"]:
            key = (leader["name"], country.get("name", ""))
            qid = name_country_qid.get(key, "")
            if qid:
                leader["indicators"]["family_in_govt"] = qid_binary.get(qid, 0)
                patched += 1

    with open(DASHBOARD_JSON, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)
    print(f"  Patched {patched} leaders in dashboard JSON")

    # Final summary
    fam_count = sum(1 for row in leaders if row["family_in_govt_binary"] == "1")
    places_count = sum(1 for row in leaders if row["places_named_binary"] == "1")
    titles_count = sum(1 for row in leaders if row["grandiose_titles_binary"] == "1")
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders: {len(leaders)}")
    print(f"Family in govt (≥2): {fam_count}")
    print(f"Places named (≥2):   {places_count}")
    print(f"Grandiose titles:    {titles_count}")


if __name__ == "__main__":
    main()
