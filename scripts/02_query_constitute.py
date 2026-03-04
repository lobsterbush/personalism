#!/usr/bin/env python3
"""
Query the Constitute Project API for constitutional provisions related to personalism.

Extracts indicators A1 (term limits), A2 (president for life), A4 (appointment
monopoly), and B9 (loyalty oaths) from constitutional texts.

The Constitute Project (constituteproject.org) provides full-text constitutions
for ~200 countries with topic-level annotations. Their API enables searching by
topic tags and retrieving relevant constitutional sections.

Usage:
  python 02_query_constitute.py [--country ISO3] [--output PATH]

API docs: https://www.constituteproject.org/content/api
"""

import argparse
import csv
import json
import re
import time
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONSTITUTE_API = "https://www.constituteproject.org/service"
HEADERS = {
    "User-Agent": "PersonalismProject/0.1 (academic research; mailto:charles.crabtree@monash.edu)",
    "Accept": "application/json",
}
RATE_LIMIT_SECONDS = 0.5

# Constitute Project topic tags relevant to personalism indicators
# See: https://www.constituteproject.org/ontology
TOPIC_TAGS = {
    "term_limits": [
        "head_of_state_term_length",
        "head_of_state_term_number",
        "head_of_government_term_length",
        "head_of_government_term_number",
    ],
    "head_of_state_powers": [
        "head_of_state_powers",
        "head_of_state_decree_power",
        "head_of_state_selection",
        "head_of_state_immunity",
    ],
    "appointment_power": [
        "head_of_state_powers",
        "cabinet_selection",
        "supreme_court_selection",
        "military_appointment",
    ],
    "oaths": [
        "oaths_to_abide_by_constitution",
    ],
}

# Countries of interest (ISO2 codes — Constitute uses ISO2)
TARGET_COUNTRIES_ISO2 = {
    "KP": ("PRK", "North Korea"),
    "IQ": ("IRQ", "Iraq"),
    "LY": ("LBY", "Libya"),
    "SY": ("SYR", "Syria"),
    "TM": ("TKM", "Turkmenistan"),
    "UZ": ("UZB", "Uzbekistan"),
    "TJ": ("TJK", "Tajikistan"),
    "KZ": ("KAZ", "Kazakhstan"),
    "BY": ("BLR", "Belarus"),
    "ER": ("ERI", "Eritrea"),
    "GQ": ("GNQ", "Equatorial Guinea"),
    "CM": ("CMR", "Cameroon"),
    "TD": ("TCD", "Chad"),
    "CG": ("COG", "Congo (Brazzaville)"),
    "CD": ("COD", "Congo (Kinshasa)"),
    "ZW": ("ZWE", "Zimbabwe"),
    "SD": ("SDN", "Sudan"),
    "MM": ("MMR", "Myanmar"),
    "KH": ("KHM", "Cambodia"),
    "LA": ("LAO", "Laos"),
    "VN": ("VNM", "Vietnam"),
    "CN": ("CHN", "China"),
    "CU": ("CUB", "Cuba"),
    "VE": ("VEN", "Venezuela"),
    "NI": ("NIC", "Nicaragua"),
    "AZ": ("AZE", "Azerbaijan"),
    "RU": ("RUS", "Russia"),
    "EG": ("EGY", "Egypt"),
    "SA": ("SAU", "Saudi Arabia"),
    "IR": ("IRN", "Iran"),
    "AF": ("AFG", "Afghanistan"),
    "HT": ("HTI", "Haiti"),
    "UG": ("UGA", "Uganda"),
    "RW": ("RWA", "Rwanda"),
    "ET": ("ETH", "Ethiopia"),
    "DJ": ("DJI", "Djibouti"),
    "GA": ("GAB", "Gabon"),
    "TG": ("TGO", "Togo"),
    "GN": ("GIN", "Guinea"),
    "MW": ("MWI", "Malawi"),
}


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

def get_constitutions_for_country(iso2: str) -> list[dict]:
    """
    Fetch list of constitutions available for a country.

    Returns list of constitution metadata dicts.
    """
    url = f"{CONSTITUTE_API}/constitutions"
    params = {"country": iso2}

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else data.get("constitutions", [])
    except Exception as e:
        print(f"  Error fetching constitutions for {iso2}: {e}")
        return []


def get_constitution_text(constitution_id: str) -> Optional[str]:
    """Fetch the full text of a constitution by ID."""
    url = f"{CONSTITUTE_API}/constitutions/{constitution_id}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("content", "") or data.get("text", "")
    except Exception as e:
        print(f"  Error fetching constitution {constitution_id}: {e}")
        return None


def search_by_topic(topic: str, country: str = "") -> list[dict]:
    """
    Search Constitute API for constitutional sections matching a topic.

    Returns list of section dicts with text and metadata.
    """
    url = f"{CONSTITUTE_API}/search"
    params = {"q": topic, "country": country} if country else {"q": topic}

    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = data if isinstance(data, list) else data.get("results", [])
        return results
    except Exception as e:
        print(f"  Error searching topic '{topic}' for {country}: {e}")
        return []


# ---------------------------------------------------------------------------
# Indicator extraction
# ---------------------------------------------------------------------------

# Regex patterns for extracting personalism-relevant provisions
TERM_LIMIT_PATTERNS = [
    (r"(?:president|head of state).*?(?:shall )?(?:not )?(?:serve|hold office|be elected).*?(?:more than|exceed(?:ing)?)\s+(\w+)\s+(?:consecutive\s+)?terms?",
     "term_limit_found"),
    (r"(?:no|not).*?(?:person|one).*?(?:shall|may).*?(?:serve|hold|be elected).*?(?:president|head of state).*?(?:more than|exceed)\s+(\w+)\s+terms?",
     "term_limit_found"),
    (r"(?:president|head of state).*?(?:for life|life tenure|indefinite(?:ly)?|without limit)",
     "president_for_life"),
    (r"(?:term|mandate).*?(?:renewable|may be renewed)\s*(?:indefinitely|without limit)?",
     "renewable_term"),
]

OATH_PATTERNS = [
    (r"(?:swear|oath|pledge|allegiance).*?(?:to the (?:president|leader|head of state|chairman))",
     "oath_to_person"),
    (r"(?:swear|oath|pledge).*?(?:to the (?:constitution|state|nation|people|republic))",
     "oath_to_state"),
    (r"(?:loyalty|faithful|devotion).*?(?:to the (?:president|leader|supreme))",
     "loyalty_to_person"),
]

APPOINTMENT_PATTERNS = [
    (r"(?:president|head of state).*?(?:shall |may )?(?:appoint|nominate|designate).*?(?:ministers?|cabinet|judges?|governors?|commanders?|generals?)",
     "president_appoints"),
    (r"(?:president|head of state).*?(?:sole|exclusive|absolute|unilateral).*?(?:authority|power|discretion).*?(?:appoint|dismiss)",
     "sole_appointment_power"),
    (r"(?:appoint|dismiss).*?(?:without|no).*?(?:approval|consent|confirmation|consultation)",
     "unchecked_appointments"),
]


def analyze_text_for_indicators(
    text: str,
    iso3: str,
    constitution_year: Optional[int] = None,
) -> dict:
    """
    Analyze constitutional text and extract personalism indicators.

    Returns dict of indicator codings.
    """
    text_lower = text.lower()
    result = {
        "iso3": iso3,
        "constitution_year": constitution_year,
        "term_limits_mentioned": 0,
        "term_limits_removed_or_absent": 0,
        "president_for_life_provision": 0,
        "oath_to_person": 0,
        "oath_to_state": 0,
        "president_appoints_unilateral": 0,
        "n_appointment_provisions": 0,
        "raw_term_limit_text": "",
        "raw_oath_text": "",
        "raw_appointment_text": "",
    }

    # Term limits
    for pattern, label in TERM_LIMIT_PATTERNS:
        matches = re.findall(pattern, text_lower, re.DOTALL)
        if matches:
            if label == "term_limit_found":
                result["term_limits_mentioned"] = 1
            elif label == "president_for_life":
                result["president_for_life_provision"] = 1
                result["term_limits_removed_or_absent"] = 1
            elif label == "renewable_term":
                result["term_limits_mentioned"] = 1

            # Capture context
            for m in re.finditer(pattern, text_lower, re.DOTALL):
                start = max(0, m.start() - 50)
                end = min(len(text_lower), m.end() + 50)
                result["raw_term_limit_text"] += text_lower[start:end] + " | "

    # Oaths
    for pattern, label in OATH_PATTERNS:
        matches = re.findall(pattern, text_lower, re.DOTALL)
        if matches:
            if "person" in label:
                result["oath_to_person"] = 1
            else:
                result["oath_to_state"] = 1

            for m in re.finditer(pattern, text_lower, re.DOTALL):
                start = max(0, m.start() - 50)
                end = min(len(text_lower), m.end() + 50)
                result["raw_oath_text"] += text_lower[start:end] + " | "

    # Appointment powers
    appt_count = 0
    for pattern, label in APPOINTMENT_PATTERNS:
        matches = re.findall(pattern, text_lower, re.DOTALL)
        if matches:
            appt_count += len(matches)
            if "sole" in label or "unchecked" in label:
                result["president_appoints_unilateral"] = 1

            for m in re.finditer(pattern, text_lower, re.DOTALL):
                start = max(0, m.start() - 50)
                end = min(len(text_lower), m.end() + 50)
                result["raw_appointment_text"] += text_lower[start:end] + " | "

    result["n_appointment_provisions"] = appt_count

    # Truncate raw text fields
    for key in ["raw_term_limit_text", "raw_oath_text", "raw_appointment_text"]:
        result[key] = result[key][:500]

    return result


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def process_country(iso2: str, iso3: str, country_name: str) -> list[dict]:
    """Process a single country through the Constitute API."""
    print(f"\n{'='*60}")
    print(f"Processing: {country_name} ({iso2} / {iso3})")
    print(f"{'='*60}")

    results = []

    # Strategy 1: Get constitutions for this country
    constitutions = get_constitutions_for_country(iso2)
    time.sleep(RATE_LIMIT_SECONDS)

    if constitutions:
        print(f"  Found {len(constitutions)} constitution(s)")
        for const in constitutions:
            const_id = const.get("id", const.get("constitutionId", ""))
            year = const.get("year", const.get("date", ""))
            if isinstance(year, str) and year:
                year_match = re.search(r"(\d{4})", str(year))
                year = int(year_match.group(1)) if year_match else None

            if const_id:
                print(f"  Fetching constitution: {const_id} ({year})")
                text = get_constitution_text(const_id)
                time.sleep(RATE_LIMIT_SECONDS)

                if text:
                    indicators = analyze_text_for_indicators(text, iso3, year)
                    indicators["country"] = country_name
                    indicators["constitution_id"] = const_id
                    results.append(indicators)
    else:
        print("  No constitutions found via direct lookup.")

    # Strategy 2: Topic-based search as supplement
    for indicator_group, topics in TOPIC_TAGS.items():
        for topic in topics:
            sections = search_by_topic(topic, iso2)
            time.sleep(RATE_LIMIT_SECONDS)

            if sections:
                print(f"  Found {len(sections)} sections for topic: {topic}")
                for section in sections[:5]:
                    text = section.get("text", section.get("content", ""))
                    if text and not results:
                        # Only use topic search if direct lookup failed
                        indicators = analyze_text_for_indicators(text, iso3)
                        indicators["country"] = country_name
                        indicators["source"] = f"topic:{topic}"
                        results.append(indicators)

    if not results:
        print("  No constitutional data found.")
        results.append({
            "iso3": iso3,
            "country": country_name,
            "constitution_year": None,
            "term_limits_mentioned": None,
            "term_limits_removed_or_absent": None,
            "president_for_life_provision": None,
            "oath_to_person": None,
            "oath_to_state": None,
            "president_appoints_unilateral": None,
            "n_appointment_provisions": None,
            "raw_term_limit_text": "",
            "raw_oath_text": "",
            "raw_appointment_text": "",
        })

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Query Constitute Project for personalism-related provisions."
    )
    parser.add_argument(
        "--country",
        type=str,
        help="ISO2 code of a single country (default: all target countries)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(Path(__file__).resolve().parent.parent / "data" / "raw" / "constitutions.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    # Select countries
    if args.country:
        iso2 = args.country.upper()
        if iso2 in TARGET_COUNTRIES_ISO2:
            countries = {iso2: TARGET_COUNTRIES_ISO2[iso2]}
        else:
            print(f"Unknown country code: {iso2}")
            return
    else:
        countries = TARGET_COUNTRIES_ISO2

    # Process
    all_results: list[dict] = []
    for iso2, (iso3, name) in countries.items():
        results = process_country(iso2, iso3, name)
        all_results.extend(results)

    # Write output
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if all_results:
        # Normalize fieldnames across all results
        all_keys: set[str] = set()
        for r in all_results:
            all_keys.update(r.keys())
        fieldnames = sorted(all_keys)

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nWrote {len(all_results)} rows to {output_path}")
    else:
        print("\nNo results to write.")


if __name__ == "__main__":
    main()
