#!/usr/bin/env python3
"""
Backfill Wikidata indicators for recovered leaders missing from wikidata_leaders.csv.

Queries family_in_govt, places_named, and grandiose_titles for ~433 QIDs
that were recovered in Phase 1 but never had original indicators queried.
Appends results to wikidata_leaders.csv.

Usage:
  python 14_backfill_wikidata.py
"""

import csv
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
LEADERS_CSV   = PROJECT_ROOT / "data" / "raw" / "wikidata_leaders.csv"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"
RATE_LIMIT = 3.0
BATCH_SIZE = 8


def sparql_query(query: str) -> list[dict]:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/sparql-results+json"}
    for attempt in range(3):
        try:
            resp = requests.get(WIKIDATA_SPARQL, params={"query": query},
                                headers=headers, timeout=90)
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


# --- Family in government ---
RELATIONSHIPS = [("P40", "child"), ("P26", "spouse"), ("P3373", "sibling")]


def query_family(qids: list[str]) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {q: [] for q in qids}
    values = " ".join(f"wd:{q}" for q in qids)

    for prop, label in RELATIONSHIPS:
        query = f"""
        SELECT ?leader ?relativeLabel ?posLabel WHERE {{
          VALUES ?leader {{ {values} }}
          ?leader wdt:{prop} ?relative .
          ?relative wdt:P39 ?pos .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }} LIMIT 500
        """
        for r in sparql_query(query):
            qid = val(r, "leader").split("/")[-1]
            if qid in results:
                results[qid].append(f"{val(r, 'relativeLabel')} ({label}): {val(r, 'posLabel')}")
        time.sleep(RATE_LIMIT)

    # Parents (reverse P40)
    query = f"""
    SELECT ?leader ?relativeLabel ?posLabel WHERE {{
      VALUES ?leader {{ {values} }}
      ?relative wdt:P40 ?leader .
      ?relative wdt:P39 ?pos .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }} LIMIT 500
    """
    for r in sparql_query(query):
        qid = val(r, "leader").split("/")[-1]
        if qid in results:
            results[qid].append(f"{val(r, 'relativeLabel')} (parent): {val(r, 'posLabel')}")
    time.sleep(RATE_LIMIT)

    return results


# --- Places named after ---
def query_places(qids: list[str]) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {q: [] for q in qids}
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?leader ?placeLabel WHERE {{
      VALUES ?leader {{ {values} }}
      ?place wdt:P138 ?leader .
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }} LIMIT 500
    """
    for r in sparql_query(query):
        qid = val(r, "leader").split("/")[-1]
        if qid in results:
            results[qid].append(val(r, "placeLabel"))
    return results


# --- Grandiose titles ---
def query_titles(qids: list[str]) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {q: [] for q in qids}
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?leader ?titleLabel WHERE {{
      VALUES ?leader {{ {values} }}
      {{ ?leader wdt:P511 ?title . }}
      UNION
      {{ ?leader wdt:P97 ?title . }}
      UNION
      {{ ?leader wdt:P1035 ?title . }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }} LIMIT 500
    """
    for r in sparql_query(query):
        qid = val(r, "leader").split("/")[-1]
        if qid in results:
            results[qid].append(val(r, "titleLabel"))
    return results


def main():
    print("Loading data...")
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        recovered = list(csv.DictReader(f))
    with open(LEADERS_CSV, "r", encoding="utf-8") as f:
        existing = list(csv.DictReader(f))

    existing_keys = {(r["qid"], r["start_year"]) for r in existing}
    missing = [r for r in recovered if (r["qid"], r["start_year"]) not in existing_keys and r["qid"]]
    unique_qids = list(set(r["qid"] for r in missing))

    print(f"  {len(missing)} missing leader-spells, {len(unique_qids)} unique QIDs to query")

    # Query all 3 indicators in batches
    family_data: dict[str, list[str]] = {}
    places_data: dict[str, list[str]] = {}
    titles_data: dict[str, list[str]] = {}

    total_batches = (len(unique_qids) - 1) // BATCH_SIZE + 1
    for i in range(0, len(unique_qids), BATCH_SIZE):
        batch = unique_qids[i:i + BATCH_SIZE]
        bn = i // BATCH_SIZE + 1
        if bn % 10 == 1 or bn == total_batches:
            print(f"  Batch {bn}/{total_batches}...")

        fd = query_family(batch)
        family_data.update(fd)
        time.sleep(1)

        pd = query_places(batch)
        places_data.update(pd)
        time.sleep(RATE_LIMIT)

        td = query_titles(batch)
        titles_data.update(td)
        time.sleep(RATE_LIMIT)

    # Build new rows for wikidata_leaders.csv
    new_rows = []
    for r in missing:
        qid = r["qid"]
        fam = family_data.get(qid, [])
        fam_unique = set(e.split(" (")[0] for e in fam)
        plc = places_data.get(qid, [])
        ttl = titles_data.get(qid, [])

        new_rows.append({
            "qid": qid,
            "leader": r["leader"],
            "iso3": "",
            "country_name": "",
            "start_year": r["start_year"],
            "end_year": r["end_year"],
            "entry": "",
            "exit": "",
            "family_in_govt_count": str(len(fam_unique)),
            "family_in_govt_binary": "1" if len(fam_unique) >= 2 else "0",
            "family_in_govt_details": "; ".join(fam[:10]),
            "places_named_count": str(len(plc)),
            "places_named_binary": "1" if len(plc) >= 2 else "0",
            "places_named_details": "; ".join(plc[:10]),
            "grandiose_titles_count": str(len(ttl)),
            "grandiose_titles_binary": "1" if ttl else "0",
            "grandiose_titles_details": "; ".join(ttl[:10]),
        })

    # Append to wikidata_leaders.csv
    fieldnames = list(existing[0].keys())
    with open(LEADERS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writerows(new_rows)

    # Summary
    n_fam = sum(1 for r in new_rows if r["family_in_govt_binary"] == "1")
    n_plc = sum(1 for r in new_rows if r["places_named_binary"] == "1")
    n_ttl = sum(1 for r in new_rows if r["grandiose_titles_binary"] == "1")
    print(f"\n{'='*60}")
    print(f"BACKFILL SUMMARY")
    print(f"{'='*60}")
    print(f"New rows added: {len(new_rows)}")
    print(f"  family_in_govt:   {n_fam} positive")
    print(f"  places_named:     {n_plc} positive")
    print(f"  grandiose_titles: {n_ttl} positive")
    print(f"wikidata_leaders.csv now has {len(existing) + len(new_rows)} rows")


if __name__ == "__main__":
    main()
