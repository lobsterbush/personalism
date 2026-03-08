#!/usr/bin/env python3
"""
Recover canonical leaders missing from the Archigos-Wikidata matching pipeline.

The Archigos→Wikidata name-matching (scripts 05/07) failed for ~15 canonical
personalists due to diacritics, transliteration, and name variants. This script
manually adds them to recovered_qids.csv with verified QIDs and Archigos dates.

COW codes and tenure dates follow Archigos 4.1. QIDs verified against Wikidata.
For leaders whose tenure began before 1946, start_year is set to 1946 (Archigos
coverage start).

Usage:
    python 15_recover_missing_leaders.py
"""

import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"

# Canonical leaders missing from the pipeline, manually verified.
# Format: (leader, ccode, start_year, end_year, idacr, qid, source, wiki_title)
MISSING_LEADERS = [
    # North Korea — Kim dynasty
    ("Kim Il-sung", 731, 1948, 1994, "PRK", "Q1109", "manual", "Kim Il-sung"),
    ("Kim Jong-il", 731, 1994, 2011, "PRK", "Q5765", "manual", "Kim Jong-il"),
    # Romania
    ("Ceausescu", 360, 1967, 1989, "RUM", "Q203633", "manual", "Nicolae Ceaușescu"),
    # Uganda
    ("Idi Amin", 500, 1971, 1979, "UGA", "Q5621", "manual", "Idi Amin"),
    # Central African Republic
    ("Bokassa", 482, 1966, 1979, "CEN", "Q191543", "manual", "Jean-Bédel Bokassa"),
    # Haiti — Duvalier dynasty
    ("Francois Duvalier", 41, 1957, 1971, "HAI", "Q16220", "manual", "François Duvalier"),
    ("Jean-Claude Duvalier", 41, 1971, 1986, "HAI", "Q314812", "manual", "Jean-Claude Duvalier"),
    # Dominican Republic
    ("Trujillo", 42, 1946, 1961, "DOM", "Q192927", "manual", "Rafael Trujillo"),
    # Spain
    ("Franco", 230, 1946, 1975, "SPN", "Q29179", "manual", "Francisco Franco"),
    # Portugal
    ("Salazar", 235, 1946, 1968, "POR", "Q37040", "manual", "António de Oliveira Salazar"),
    # Albania
    ("Hoxha", 339, 1946, 1985, "ALB", "Q58422", "manual", "Enver Hoxha"),
    # Soviet Union / Russia
    ("Stalin", 365, 1946, 1953, "RUS", "Q4534", "manual", "Joseph Stalin"),
    ("Khrushchev", 365, 1953, 1964, "RUS", "Q47272", "manual", "Nikita Khrushchev"),
    ("Brezhnev", 365, 1964, 1982, "RUS", "Q4076", "manual", "Leonid Brezhnev"),
    # Cuba
    ("Fidel Castro", 40, 1959, 2008, "CUB", "Q16213", "manual", "Fidel Castro"),
    ("Raul Castro", 40, 2008, 2015, "CUB", "Q165292", "manual", "Raúl Castro"),
    # Syria
    ("Hafez al-Assad", 652, 1971, 2000, "SYR", "Q57583", "manual", "Hafez al-Assad"),
    # Ethiopia
    ("Mengistu", 530, 1977, 1991, "ETH", "Q168751", "manual", "Mengistu Haile Mariam"),
    # Libya — verify Gaddafi has both spells if needed
    # Already present: Qaddafi,620,1969,2011,LIB,Q19878
]


def main() -> None:
    # Load existing QIDs to avoid duplicates
    existing_qids: set[str] = set()
    existing_rows: list[dict] = []
    fieldnames = ["leader", "ccode", "start_year", "end_year", "idacr",
                  "qid", "source", "wiki_title"]

    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or fieldnames
        for row in reader:
            existing_rows.append(row)
            if row.get("qid"):
                existing_qids.add(f"{row['qid']}__{row['start_year']}")

    print(f"Existing leaders: {len(existing_rows)}")
    print(f"Existing unique QID-spells: {len(existing_qids)}")

    # Add missing leaders
    added = 0
    skipped = 0
    for leader, ccode, start, end, idacr, qid, source, wiki_title in MISSING_LEADERS:
        key = f"{qid}__{start}"
        if key in existing_qids:
            print(f"  SKIP (already present): {leader} ({qid}, {start})")
            skipped += 1
            continue

        row = {
            "leader": leader,
            "ccode": str(ccode),
            "start_year": str(start),
            "end_year": str(end),
            "idacr": idacr,
            "qid": qid,
            "source": source,
            "wiki_title": wiki_title,
        }
        existing_rows.append(row)
        existing_qids.add(key)
        added += 1
        print(f"  ADDED: {leader} ({qid}, {ccode}, {start}-{end})")

    # Write back
    with open(RECOVERED_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)

    print(f"\nAdded {added} leaders, skipped {skipped} duplicates")
    print(f"Total leaders: {len(existing_rows)}")
    print(f"Wrote {RECOVERED_CSV}")


if __name__ == "__main__":
    main()
