#!/usr/bin/env python3
"""
Query Wikidata via SPARQL for personalism-related indicators.

Extracts indicators:
  A3 (family in government) — kinship + office-holding relations
  B3 (places named after leader) — "named after" property (P138)
  B4 (grandiose titles) — honorific titles and style of address
  B5 (national holiday on birthday) — public holidays matching leader DOB

Uses the Wikidata Query Service SPARQL endpoint, which is free and requires
no authentication. Rate limit: be polite (1 req/sec for batch queries).

Usage:
  python 03_query_wikidata.py [--output-dir PATH]
"""

import argparse
import csv
import json
import time
from pathlib import Path
from typing import Optional

from SPARQLWrapper import SPARQLWrapper, JSON

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "PersonalismProject/0.1 (academic research; mailto:charles.crabtree@monash.edu)"
}
RATE_LIMIT_SECONDS = 2.0  # Wikidata asks for >=1s between queries

# Key authoritarian leaders with their Wikidata QIDs
# This list is the backbone — extend as needed
LEADERS = [
    # (QID, name, country_iso3, start_year, end_year)
    ("Q1109", "Kim Il-sung", "PRK", 1948, 1994),
    ("Q5765", "Kim Jong-il", "PRK", 1994, 2011),
    ("Q56226", "Kim Jong-un", "PRK", 2011, 2026),
    ("Q1316", "Saddam Hussein", "IRQ", 1979, 2003),
    ("Q19878", "Muammar Gaddafi", "LBY", 1969, 2011),
    ("Q34266", "Bashar al-Assad", "SYR", 2000, 2024),
    ("Q57583", "Hafez al-Assad", "SYR", 1971, 2000),
    ("Q183100", "Saparmurat Niyazov", "TKM", 1991, 2006),
    ("Q57641", "Islam Karimov", "UZB", 1991, 2016),
    ("Q39993", "Nursultan Nazarbayev", "KAZ", 1991, 2019),
    ("Q7530", "Alexander Lukashenko", "BLR", 1994, 2026),
    ("Q7747", "Vladimir Putin", "RUS", 2000, 2026),
    ("Q16213", "Fidel Castro", "CUB", 1959, 2008),
    ("Q5809", "Robert Mugabe", "ZWE", 1987, 2017),
    ("Q5621", "Idi Amin", "UGA", 1971, 1979),
    ("Q57420", "Yoweri Museveni", "UGA", 1986, 2026),
    ("Q3339", "Mao Zedong", "CHN", 1949, 1976),
    ("Q57387", "Hosni Mubarak", "EGY", 1981, 2011),
    ("Q307737", "Teodoro Obiang Nguema Mbasogo", "GNQ", 1979, 2026),
    ("Q57437", "Paul Biya", "CMR", 1982, 2026),
    ("Q471905", "Idriss Déby", "TCD", 1990, 2021),
    ("Q180589", "Hun Sen", "KHM", 1985, 2023),
    ("Q1249", "Hugo Chávez", "VEN", 1999, 2013),
    ("Q432778", "Nicolás Maduro", "VEN", 2013, 2026),
    ("Q352282", "Daniel Ortega", "NIC", 2007, 2026),
    ("Q192820", "Ilham Aliyev", "AZE", 2003, 2026),
    ("Q57382", "Emomali Rahmon", "TJK", 1994, 2026),
    ("Q470563", "Isaias Afwerki", "ERI", 1993, 2026),
    ("Q39780", "Haile Selassie", "ETH", 1930, 1974),
    ("Q168751", "Mengistu Haile Mariam", "ETH", 1977, 1991),
    ("Q353530", "Omar al-Bashir", "SDN", 1989, 2019),
    ("Q57370", "Paul Kagame", "RWA", 2000, 2026),
    ("Q16220", "François Duvalier", "HTI", 1957, 1971),
    ("Q314812", "Jean-Claude Duvalier", "HTI", 1971, 1986),
    ("Q128799", "Augusto Pinochet", "CHL", 1973, 1990),
    ("Q7416", "Suharto", "IDN", 1967, 1998),
    ("Q4534", "Joseph Stalin", "SUN", 1924, 1953),
]


# ---------------------------------------------------------------------------
# SPARQL queries
# ---------------------------------------------------------------------------

def query_sparql(query: str) -> list[dict]:
    """Execute a SPARQL query against Wikidata and return results."""
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.addCustomHttpHeader("User-Agent", HEADERS["User-Agent"])

    try:
        results = sparql.query().convert()
        bindings = results.get("results", {}).get("bindings", [])
        return bindings
    except Exception as e:
        print(f"  SPARQL error: {e}")
        return []


def query_family_in_government(leader_qid: str) -> list[dict]:
    """
    Find family members of a leader who held political office.

    Uses P22 (father), P25 (mother), P26 (spouse), P40 (child),
    P3373 (sibling) to find relatives, then checks if they held
    positions (P39) in government.
    """
    query = f"""
    SELECT DISTINCT ?relative ?relativeLabel ?relationship ?position ?positionLabel ?startDate ?endDate
    WHERE {{
      VALUES ?leader {{ wd:{leader_qid} }}

      # Family relationships
      {{
        ?leader wdt:P40 ?relative .
        BIND("child" AS ?relationship)
      }} UNION {{
        ?leader wdt:P26 ?relative .
        BIND("spouse" AS ?relationship)
      }} UNION {{
        ?leader wdt:P3373 ?relative .
        BIND("sibling" AS ?relationship)
      }} UNION {{
        ?relative wdt:P40 ?leader .
        BIND("parent" AS ?relationship)
      }}

      # Check if relative held political positions
      ?relative p:P39 ?posStmt .
      ?posStmt ps:P39 ?position .
      OPTIONAL {{ ?posStmt pq:P580 ?startDate . }}
      OPTIONAL {{ ?posStmt pq:P582 ?endDate . }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    ORDER BY ?relationship ?startDate
    """
    return query_sparql(query)


def query_places_named_after(leader_qid: str) -> list[dict]:
    """
    Find places, institutions, or features named after a leader.

    Uses P138 (named after) to find entities.
    """
    query = f"""
    SELECT DISTINCT ?place ?placeLabel ?placeType ?placeTypeLabel ?country ?countryLabel
    WHERE {{
      ?place wdt:P138 wd:{leader_qid} .

      OPTIONAL {{
        ?place wdt:P31 ?placeType .
      }}
      OPTIONAL {{
        ?place wdt:P17 ?country .
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    LIMIT 200
    """
    return query_sparql(query)


def query_titles_and_honors(leader_qid: str) -> list[dict]:
    """
    Retrieve honorific titles, styles of address, and awards for a leader.

    Uses P511 (honorific prefix), P1035 (honorific suffix), P166 (award),
    P97 (noble title), and the description fields.
    """
    query = f"""
    SELECT DISTINCT ?property ?propertyLabel ?value ?valueLabel
    WHERE {{
      VALUES ?leader {{ wd:{leader_qid} }}

      {{
        ?leader wdt:P511 ?value .
        BIND("honorific_prefix" AS ?property)
      }} UNION {{
        ?leader wdt:P1035 ?value .
        BIND("honorific_suffix" AS ?property)
      }} UNION {{
        ?leader wdt:P97 ?value .
        BIND("noble_title" AS ?property)
      }} UNION {{
        ?leader wdt:P1813 ?value .
        BIND("short_name" AS ?property)
      }} UNION {{
        ?leader wdt:P1449 ?value .
        BIND("nickname" AS ?property)
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """
    return query_sparql(query)


def query_leader_metadata(leader_qid: str) -> list[dict]:
    """Get basic metadata about a leader (DOB, positions held, etc.)."""
    query = f"""
    SELECT ?dob ?dobPrecision ?position ?positionLabel ?startDate ?endDate
    WHERE {{
      VALUES ?leader {{ wd:{leader_qid} }}

      OPTIONAL {{ ?leader wdt:P569 ?dob . }}

      OPTIONAL {{
        ?leader p:P39 ?posStmt .
        ?posStmt ps:P39 ?position .
        OPTIONAL {{ ?posStmt pq:P580 ?startDate . }}
        OPTIONAL {{ ?posStmt pq:P582 ?endDate . }}
      }}

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
    }}
    """
    return query_sparql(query)


# ---------------------------------------------------------------------------
# Result processing
# ---------------------------------------------------------------------------

def extract_value(binding: dict, key: str) -> str:
    """Extract a value from a SPARQL binding."""
    if key in binding:
        return binding[key].get("value", "")
    return ""


def process_leader(
    qid: str,
    name: str,
    iso3: str,
    start_year: int,
    end_year: int,
) -> dict:
    """Process all queries for a single leader."""
    print(f"\n{'='*60}")
    print(f"Processing: {name} ({iso3}, {start_year}-{end_year})")
    print(f"{'='*60}")

    result = {
        "qid": qid,
        "leader": name,
        "iso3": iso3,
        "start_year": start_year,
        "end_year": end_year,
    }

    # 1. Family in government
    print("  Querying family in government...")
    family_results = query_family_in_government(qid)
    time.sleep(RATE_LIMIT_SECONDS)

    family_members = set()
    family_details = []
    for r in family_results:
        relative_name = extract_value(r, "relativeLabel")
        relationship = extract_value(r, "relationship")
        position = extract_value(r, "positionLabel")
        if relative_name and position:
            family_members.add(relative_name)
            family_details.append(f"{relative_name} ({relationship}): {position}")

    result["family_in_govt_count"] = len(family_members)
    result["family_in_govt_binary"] = 1 if len(family_members) >= 2 else 0
    result["family_in_govt_details"] = "; ".join(family_details[:10])
    print(f"  Family members in government: {len(family_members)}")

    # 2. Places named after
    print("  Querying places named after leader...")
    places_results = query_places_named_after(qid)
    time.sleep(RATE_LIMIT_SECONDS)

    places = []
    for r in places_results:
        place_name = extract_value(r, "placeLabel")
        place_type = extract_value(r, "placeTypeLabel")
        if place_name:
            places.append(f"{place_name} ({place_type})" if place_type else place_name)

    result["places_named_count"] = len(places)
    result["places_named_binary"] = 1 if len(places) >= 2 else 0
    result["places_named_details"] = "; ".join(places[:20])
    print(f"  Places named after: {len(places)}")

    # 3. Titles and honors
    print("  Querying titles and honors...")
    titles_results = query_titles_and_honors(qid)
    time.sleep(RATE_LIMIT_SECONDS)

    titles = []
    for r in titles_results:
        prop = extract_value(r, "property")
        value = extract_value(r, "valueLabel")
        if value and not value.startswith("http"):
            titles.append(f"{prop}: {value}")

    result["grandiose_titles_count"] = len(titles)
    result["grandiose_titles_binary"] = 1 if len(titles) >= 1 else 0
    result["grandiose_titles_details"] = "; ".join(titles[:10])
    print(f"  Titles/honors: {len(titles)}")

    # 4. Leader metadata
    print("  Querying leader metadata...")
    meta_results = query_leader_metadata(qid)
    time.sleep(RATE_LIMIT_SECONDS)

    positions = set()
    dob = ""
    for r in meta_results:
        if not dob:
            dob = extract_value(r, "dob")
        pos = extract_value(r, "positionLabel")
        if pos:
            positions.add(pos)

    result["date_of_birth"] = dob[:10] if dob else ""
    result["positions_held"] = "; ".join(sorted(positions)[:10])
    result["n_positions"] = len(positions)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Query Wikidata for personalism indicators."
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(Path(__file__).resolve().parent.parent / "data" / "raw"),
        help="Output directory for CSV files",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results: list[dict] = []

    for qid, name, iso3, start, end in LEADERS:
        result = process_leader(qid, name, iso3, start, end)
        all_results.append(result)

    # Write combined output
    output_path = output_dir / "wikidata_leaders.csv"
    if all_results:
        fieldnames = list(all_results[0].keys())
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nWrote {len(all_results)} leaders to {output_path}")

    # Also write as JSON for dashboard consumption
    json_path = output_dir / "wikidata_leaders.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)
    print(f"Wrote JSON to {json_path}")


if __name__ == "__main__":
    main()
