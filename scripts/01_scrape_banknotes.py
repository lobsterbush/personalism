#!/usr/bin/env python3
"""
Scrape banknote portrait data from Numista catalog.

Identifies whether living rulers appear on national currency — a manifest
indicator of personalism / personality cult (indicator B1 in the codebook).

Approach:
  1. Query Numista's public catalog pages for banknotes by country.
  2. Extract portrait/obverse descriptions to identify depicted persons.
  3. Cross-reference with Archigos head-of-state data to determine whether
     the depicted person was the *living* ruler at time of issue.

Usage:
  python 01_scrape_banknotes.py [--country ISO3] [--output PATH]

Requires:
  - requests, beautifulsoup4, pandas
  - Optional: NUMISTA_API_KEY env var for higher rate limits via their API

Note: Numista offers a REST API (https://en.numista.com/api/doc/) that
provides structured data. Register at https://en.numista.com/api/ for a
free API key. This script supports both the API (preferred) and catalog
page scraping (fallback).
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

NUMISTA_API_BASE = "https://api.numista.com/api/v3"
NUMISTA_CATALOG_BASE = "https://en.numista.com/catalogue"
HEADERS = {
    "User-Agent": "PersonalismProject/0.1 (academic research; mailto:charles.crabtree@monash.edu)"
}
RATE_LIMIT_SECONDS = 1.0  # Be polite

# ISO3 codes for countries with authoritarian regimes (post-1946 sample)
# This is a starter list — extend as needed
TARGET_COUNTRIES = {
    "PRK": "North Korea",
    "IRQ": "Iraq",
    "LBY": "Libya",
    "SYR": "Syria",
    "TKM": "Turkmenistan",
    "UZB": "Uzbekistan",
    "TJK": "Tajikistan",
    "KAZ": "Kazakhstan",
    "BLR": "Belarus",
    "ERI": "Eritrea",
    "GNQ": "Equatorial Guinea",
    "CMR": "Cameroon",
    "TCD": "Chad",
    "COG": "Congo (Brazzaville)",
    "COD": "Congo (Kinshasa)",
    "ZWE": "Zimbabwe",
    "SDN": "Sudan",
    "MMR": "Myanmar",
    "KHM": "Cambodia",
    "LAO": "Laos",
    "VNM": "Vietnam",
    "CHN": "China",
    "CUB": "Cuba",
    "VEN": "Venezuela",
    "NIC": "Nicaragua",
    "AZE": "Azerbaijan",
    "RUS": "Russia",
    "EGY": "Egypt",
    "SAU": "Saudi Arabia",
    "IRN": "Iran",
    "AFG": "Afghanistan",
    "HTI": "Haiti",
    "UGA": "Uganda",
    "RWA": "Rwanda",
    "ETH": "Ethiopia",
    "DJI": "Djibouti",
    "GAB": "Gabon",
    "TGO": "Togo",
    "GIN": "Guinea",
    "MWI": "Malawi",
}

# Numista uses its own country IDs — this maps ISO3 to Numista search terms
# (the catalog URL uses country names in the URL path)
NUMISTA_COUNTRY_SLUGS = {
    "PRK": "north-korea",
    "IRQ": "iraq",
    "LBY": "libya",
    "SYR": "syria",
    "TKM": "turkmenistan",
    "UZB": "uzbekistan",
    "TJK": "tajikistan",
    "KAZ": "kazakhstan",
    "BLR": "belarus",
    "ZWE": "zimbabwe",
    "CMR": "cameroon",
    "CHN": "china",
    "CUB": "cuba",
    "EGY": "egypt",
    "SAU": "saudi-arabia",
    "IRN": "iran",
    "RUS": "russia",
    "KHM": "cambodia",
    "MMR": "myanmar",
    "UGA": "uganda",
    "RWA": "rwanda",
    "ETH": "ethiopia",
}


# ---------------------------------------------------------------------------
# Numista API client (preferred if API key available)
# ---------------------------------------------------------------------------

def get_api_key() -> Optional[str]:
    """Retrieve Numista API key from environment."""
    return os.environ.get("NUMISTA_API_KEY")


def search_banknotes_api(country_name: str, api_key: str) -> list[dict]:
    """
    Search Numista API for banknotes from a given country.

    Returns list of dicts with keys: title, year_from, year_to, description.
    """
    results = []
    page = 1
    per_page = 50

    while True:
        params = {
            "q": country_name,
            "type": "banknote",
            "page": page,
            "count": per_page,
            "lang": "en",
        }
        headers = {**HEADERS, "Numista-API-Key": api_key}

        try:
            resp = requests.get(
                f"{NUMISTA_API_BASE}/coins",
                params=params,
                headers=headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  API error for {country_name}: {e}")
            break

        items = data.get("coins", [])
        if not items:
            break

        for item in items:
            results.append({
                "numista_id": item.get("id"),
                "title": item.get("title", ""),
                "min_year": item.get("minYear"),
                "max_year": item.get("maxYear"),
                "obverse": item.get("obverse", {}).get("description", ""),
                "reverse": item.get("reverse", {}).get("description", ""),
            })

        # Check if there are more pages
        total = data.get("count", 0)
        if page * per_page >= total:
            break
        page += 1
        time.sleep(RATE_LIMIT_SECONDS)

    return results


# ---------------------------------------------------------------------------
# Catalog page scraper (fallback)
# ---------------------------------------------------------------------------

def scrape_banknotes_catalog(country_slug: str) -> list[dict]:
    """
    Scrape Numista catalog page for banknotes from a country.

    This is a fallback when no API key is available. It extracts basic info
    from the catalog listing pages.
    """
    results = []
    page = 1
    base_url = f"{NUMISTA_CATALOG_BASE}/billets-{country_slug}.html"

    while True:
        url = f"{base_url}?page={page}" if page > 1 else base_url
        print(f"  Fetching: {url}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"  Fetch error: {e}")
            break

        soup = BeautifulSoup(resp.text, "lxml")

        # Find banknote listing items
        items = soup.select("div.catalogue-item, div.piece-billet")
        if not items:
            # Try alternative selectors
            items = soup.select("a.catalogue-link")

        if not items:
            break

        for item in items:
            title_el = item.select_one("h2, .titre, .title")
            title = title_el.get_text(strip=True) if title_el else item.get_text(strip=True)[:100]

            # Extract year from title (common pattern: "100 Won (2008)")
            year_match = re.search(r"\((\d{4})\)", title)
            year = int(year_match.group(1)) if year_match else None

            # Extract description/portrait info if available
            desc_el = item.select_one(".description, .desc, p")
            description = desc_el.get_text(strip=True) if desc_el else ""

            results.append({
                "title": title,
                "year": year,
                "description": description,
                "source_url": url,
            })

        # Check for next page
        next_link = soup.select_one("a.next, a[rel='next']")
        if not next_link:
            break

        page += 1
        time.sleep(RATE_LIMIT_SECONDS)

    return results


# ---------------------------------------------------------------------------
# Portrait detection heuristics
# ---------------------------------------------------------------------------

# Known leader names by country for cross-referencing
# Format: ISO3 -> list of (leader_name_pattern, start_year, end_year)
KNOWN_LEADERS = {
    "PRK": [
        (r"Kim Il.?sung", 1948, 1994),
        (r"Kim Jong.?il", 1994, 2011),
        (r"Kim Jong.?un", 2011, 2026),
    ],
    "IRQ": [
        (r"Saddam|Hussein", 1979, 2003),
    ],
    "LBY": [
        (r"Gaddafi|Qadhafi|Qaddafi", 1969, 2011),
    ],
    "TKM": [
        (r"Niyazov|Turkmenbashi", 1991, 2006),
        (r"Berdimuhamedov", 2006, 2022),
        (r"Serdar", 2022, 2026),
    ],
    "ZWE": [
        (r"Mugabe", 1987, 2017),
        (r"Mnangagwa", 2017, 2026),
    ],
    "KHM": [
        (r"Hun Sen", 1985, 2023),
        (r"Hun Manet", 2023, 2026),
    ],
    "CHN": [
        (r"Mao|Zedong|Tse.?tung", 1949, 1976),
    ],
    "CUB": [
        (r"Fidel|Castro", 1959, 2008),
    ],
    "SYR": [
        (r"Hafez.*Assad", 1971, 2000),
        (r"Bashar.*Assad", 2000, 2024),
    ],
    "EGY": [
        (r"Nasser", 1956, 1970),
        (r"Sadat", 1970, 1981),
        (r"Mubarak", 1981, 2011),
        (r"Sisi", 2014, 2026),
    ],
    "UGA": [
        (r"Idi Amin", 1971, 1979),
        (r"Museveni", 1986, 2026),
    ],
    "BLR": [
        (r"Lukashenko", 1994, 2026),
    ],
    "RUS": [
        (r"Putin", 2000, 2026),
    ],
    "KAZ": [
        (r"Nazarbayev", 1991, 2019),
        (r"Tokayev", 2019, 2026),
    ],
    "ETH": [
        (r"Haile Selassie", 1930, 1974),
        (r"Mengistu", 1977, 1991),
    ],
    "GNQ": [
        (r"Obiang|Nguema", 1979, 2026),
    ],
    "CMR": [
        (r"Biya", 1982, 2026),
    ],
    "TCD": [
        (r"D[eé]by", 1990, 2021),
    ],
}


def detect_leader_portrait(
    text: str,
    iso3: str,
    year: Optional[int],
) -> Optional[str]:
    """
    Check whether a banknote description/title mentions a known leader.

    Returns the matched leader name pattern, or None.
    """
    if not text or iso3 not in KNOWN_LEADERS:
        return None

    for pattern, start, end in KNOWN_LEADERS[iso3]:
        if year and not (start <= year <= end):
            continue
        if re.search(pattern, text, re.IGNORECASE):
            return pattern

    return None


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_country(iso3: str, country_name: str, api_key: Optional[str]) -> list[dict]:
    """Process a single country: fetch banknotes and detect leader portraits."""
    print(f"\n{'='*60}")
    print(f"Processing: {country_name} ({iso3})")
    print(f"{'='*60}")

    # Fetch banknote data
    if api_key:
        print("  Using Numista API...")
        raw_notes = search_banknotes_api(country_name, api_key)
    elif iso3 in NUMISTA_COUNTRY_SLUGS:
        print("  Using catalog scraper (no API key)...")
        raw_notes = scrape_banknotes_catalog(NUMISTA_COUNTRY_SLUGS[iso3])
    else:
        print(f"  Skipping {iso3}: no API key and no catalog slug configured.")
        return []

    print(f"  Found {len(raw_notes)} banknote entries")

    # Detect leader portraits
    coded = []
    for note in raw_notes:
        # Combine all text fields for matching
        search_text = " ".join([
            note.get("title", ""),
            note.get("obverse", ""),
            note.get("reverse", ""),
            note.get("description", ""),
        ])

        year = note.get("year") or note.get("min_year")
        leader_match = detect_leader_portrait(search_text, iso3, year)

        coded.append({
            "iso3": iso3,
            "country": country_name,
            "banknote_title": note.get("title", "")[:200],
            "year": year,
            "leader_detected": leader_match or "",
            "currency_portrait": 1 if leader_match else 0,
            "source_text": search_text[:300],
        })

    portrait_count = sum(1 for c in coded if c["currency_portrait"] == 1)
    print(f"  Leader portraits detected: {portrait_count}/{len(coded)}")

    return coded


def main():
    parser = argparse.ArgumentParser(
        description="Scrape banknote portrait data for personalism indicators."
    )
    parser.add_argument(
        "--country",
        type=str,
        help="ISO3 code of a single country to process (default: all)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).resolve().parent.parent / "data" / "raw" / "banknotes.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    api_key = get_api_key()
    if api_key:
        print("Numista API key found — using structured API.")
    else:
        print("No NUMISTA_API_KEY set — falling back to catalog scraping.")
        print("For better results, register at https://en.numista.com/api/")

    # Select countries
    if args.country:
        countries = {args.country: TARGET_COUNTRIES.get(args.country, args.country)}
    else:
        countries = TARGET_COUNTRIES

    # Process
    all_results: list[dict] = []
    for iso3, name in countries.items():
        results = process_country(iso3, name, api_key)
        all_results.extend(results)
        time.sleep(RATE_LIMIT_SECONDS)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if all_results:
        fieldnames = list(all_results[0].keys())
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nWrote {len(all_results)} rows to {output_path}")
    else:
        print("\nNo results to write.")

    # Also write a summary at country-year level
    summary = aggregate_country_year(all_results)
    summary_path = output_path.with_name("banknotes_summary.csv")
    if summary:
        fieldnames = list(summary[0].keys())
        with open(summary_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary)
        print(f"Wrote {len(summary)} country-year rows to {summary_path}")


def aggregate_country_year(results: list[dict]) -> list[dict]:
    """Aggregate banknote-level data to country-year level."""
    from collections import defaultdict

    groups: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for r in results:
        if r.get("year"):
            key = (r["iso3"], r["year"])
            groups[key].append(r)

    summary = []
    for (iso3, year), notes in sorted(groups.items()):
        has_portrait = any(n["currency_portrait"] == 1 for n in notes)
        summary.append({
            "iso3": iso3,
            "country": notes[0]["country"],
            "year": year,
            "n_banknotes": len(notes),
            "n_with_portrait": sum(n["currency_portrait"] for n in notes),
            "currency_portrait": 1 if has_portrait else 0,
        })

    return summary


if __name__ == "__main__":
    main()
