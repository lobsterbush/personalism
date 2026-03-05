#!/usr/bin/env python3
"""
Query Constitute Project API for constitutional personalism indicators.

Expands the original 02_query_constitute.py to cover all Archigos countries.
Extracts:
  A1 - term_limits_absent: no constitutional term limits for executive
  A2 - president_for_life: constitution allows life presidency
  B9 - oath_to_person:     loyalty oath directed at leader (not state)

For each leader, the most recent constitution in effect during their
tenure is matched. Country-level provisions are mapped to leader-spells.

Usage:
  python 11_constitute_indicators.py
"""

import csv
import json
import re
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV    = PROJECT_ROOT / "data" / "raw" / "constitute_indicators.csv"

CONSTITUTE_API = "https://www.constituteproject.org/service"
CONSTITUTE_WEB = "https://www.constituteproject.org/constitution"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"
HEADERS_API = {"User-Agent": USER_AGENT, "Accept": "application/json"}
HEADERS_WEB = {"User-Agent": "Mozilla/5.0 (Macintosh; academic research)", "Accept": "text/html"}
RATE_LIMIT = 0.8  # polite to website

# COW code → ISO2 code for Constitute API
COW_TO_ISO2: dict[int, str] = {
    2: "US", 20: "CA", 31: "BS", 40: "CU", 41: "HT", 42: "DO",
    51: "JM", 52: "TT", 53: "BB", 70: "MX", 80: "BZ", 90: "GT",
    91: "HN", 92: "SV", 93: "NI", 94: "CR", 95: "PA", 100: "CO",
    101: "VE", 110: "GY", 115: "SR", 130: "EC", 135: "PE", 140: "BR",
    145: "BO", 150: "PY", 155: "CL", 160: "AR", 165: "UY",
    200: "GB", 205: "IE", 210: "NL", 211: "BE", 212: "LU",
    220: "FR", 225: "CH", 230: "ES", 235: "PT", 255: "DE", 260: "DE",
    265: "DE", 290: "PL", 305: "AT", 310: "HU", 315: "CZ",
    316: "CZ", 317: "SK", 325: "IT", 338: "MT", 339: "AL",
    341: "ME", 343: "MK", 344: "HR", 345: "RS", 346: "BA",
    349: "SI", 350: "GR", 352: "CY", 355: "BG", 359: "MD",
    360: "RO", 364: "RU", 365: "RU", 366: "EE", 367: "LV",
    368: "LT", 369: "UA", 370: "BY", 371: "AM", 372: "GE",
    373: "AZ", 375: "FI", 380: "SE", 385: "NO", 390: "DK",
    395: "IS", 402: "CV", 404: "GW", 411: "GN", 420: "GM",
    432: "ML", 433: "SN", 434: "BJ", 435: "MR", 436: "NE",
    437: "CI", 438: "GN", 439: "BF", 450: "LR", 451: "SL",
    452: "GH", 461: "TG", 471: "CM", 475: "NG", 481: "GA",
    482: "CF", 483: "TD", 484: "CG", 490: "CD", 500: "UG",
    501: "KE", 510: "TZ", 516: "BI", 517: "RW", 520: "SO",
    522: "DJ", 530: "ET", 531: "ER", 540: "AO", 541: "MZ",
    551: "ZM", 552: "ZW", 553: "MW", 560: "ZA", 565: "NA",
    570: "LS", 571: "BW", 572: "SZ", 580: "MG", 590: "MU",
    600: "MA", 615: "DZ", 616: "TN", 620: "LY", 625: "SD",
    626: "SS", 630: "IR", 640: "TR", 645: "IQ", 651: "EG",
    652: "SY", 660: "LB", 663: "JO", 666: "IL", 670: "SA",
    678: "YE", 679: "YE", 680: "YE", 690: "KW", 694: "QA",
    696: "AE", 698: "OM", 700: "AF", 701: "TM", 702: "TJ",
    703: "KG", 704: "UZ", 705: "KZ", 710: "CN", 712: "MN",
    713: "TW", 730: "KR", 731: "KP", 732: "KR", 740: "JP",
    750: "IN", 770: "PK", 771: "BD", 775: "MM", 780: "LK",
    790: "NP", 800: "TH", 811: "KH", 812: "LA", 816: "VN",
    817: "VN", 820: "MY", 830: "SG", 840: "PH", 850: "ID",
    900: "AU", 910: "PG", 920: "NZ", 950: "FJ",
}

# --- Keyword sets for sentence-level matching (avoids regex backtracking) ---
TERM_LIMIT_KEYWORDS = [
    "term limit", "two terms", "two consecutive terms", "more than two",
    "not be re-elected", "shall not serve", "re-election",
    "shall not be eligible", "renewable once", "one time only",
    "consecutive terms", "exceeding two",
]
LIFE_PRESIDENCY_KEYWORDS = [
    "president for life", "life president", "elected for life",
    "head of state for life", "unlimited number of terms",
    "renewable without limit", "serve for life",
    "hold office for life", "presidency for life",
]
OATH_TO_PERSON_KEYWORDS = [
    "oath to the president", "allegiance to the president",
    "loyalty to the leader", "faithful to the president",
    "devotion to the leader", "oath to the head of state",
    "pledge to the chairman", "allegiance to the leader",
]


def fetch_all_constitutions() -> list[dict]:
    """Fetch full list of constitutions from API."""
    for attempt in range(3):
        try:
            resp = requests.get(
                f"{CONSTITUTE_API}/constitutions",
                headers=HEADERS_API, timeout=30,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return []


def get_constitution_text(const_id: str) -> str | None:
    """Scrape constitution text from website HTML."""
    url = f"{CONSTITUTE_WEB}/{const_id}"
    for attempt in range(2):
        try:
            resp = requests.get(url, headers=HEADERS_WEB, timeout=20)
            if resp.status_code != 200:
                return None
            # Strip HTML to plain text
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', resp.text, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        except Exception:
            if attempt < 1:
                time.sleep(5)
    return None


def analyze_constitution(text: str) -> dict:
    """Extract binary indicators via fast keyword matching on sentences."""
    text_lower = text.lower()
    # Split into sentences (rough but fast)
    sentences = re.split(r'[.;\n]', text_lower)

    result = {
        "has_term_limit": 0,
        "life_presidency": 0,
        "oath_to_person": 0,
    }

    for sent in sentences:
        for kw in TERM_LIMIT_KEYWORDS:
            if kw in sent:
                result["has_term_limit"] = 1
                break
        for kw in LIFE_PRESIDENCY_KEYWORDS:
            if kw in sent:
                result["life_presidency"] = 1
                break
        for kw in OATH_TO_PERSON_KEYWORDS:
            if kw in sent:
                result["oath_to_person"] = 1
                break

    return result


def main():
    # Load leaders
    print("Loading leaders...")
    leaders = []
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            leaders.append(row)
    print(f"  {len(leaders)} leader spells")

    # Get unique country codes
    unique_ccodes = sorted(set(int(r["ccode"]) for r in leaders if r["ccode"].isdigit()))
    iso2_set = set()
    for cc in unique_ccodes:
        iso2 = COW_TO_ISO2.get(cc)
        if iso2:
            iso2_set.add(iso2)
    print(f"  {len(iso2_set)} unique ISO2 codes needed")

    # Fetch all constitutions at once from API
    print("\nFetching constitution catalog from Constitute API...")
    all_consts = fetch_all_constitutions()
    print(f"  {len(all_consts)} constitutions across {len(set(c['country_id'] for c in all_consts))} countries")

    # Build ISO2 → Constitute country_id mapping
    # Constitute uses country names like "United_States_of_America"
    ISO2_TO_CONSTITUTE: dict[str, str] = {}
    for c in all_consts:
        cid = c.get("country_id", "")
        # Try to infer ISO2 from the constitution data
        # We'll match by building a reverse lookup
        if cid not in [v for v in ISO2_TO_CONSTITUTE.values()]:
            ISO2_TO_CONSTITUTE[cid] = cid  # store country_id

    # Build country_id -> list of constitutions
    consts_by_country: dict[str, list[dict]] = {}
    for c in all_consts:
        cid = c.get("country_id", "")
        if cid not in consts_by_country:
            consts_by_country[cid] = []
        consts_by_country[cid].append(c)

    # Map ISO2 to Constitute country_id
    ISO2_TO_CID = {
        "US": "United_States_of_America", "CA": "Canada", "BS": "Bahamas",
        "CU": "Cuba", "HT": "Haiti", "DO": "Dominican_Republic",
        "JM": "Jamaica", "TT": "Trinidad_and_Tobago", "BB": "Barbados",
        "MX": "Mexico", "BZ": "Belize", "GT": "Guatemala",
        "HN": "Honduras", "SV": "El_Salvador", "NI": "Nicaragua",
        "CR": "Costa_Rica", "PA": "Panama", "CO": "Colombia",
        "VE": "Venezuela", "GY": "Guyana", "SR": "Suriname",
        "EC": "Ecuador", "PE": "Peru", "BR": "Brazil",
        "BO": "Bolivia", "PY": "Paraguay", "CL": "Chile",
        "AR": "Argentina", "UY": "Uruguay",
        "GB": "United_Kingdom", "IE": "Ireland", "NL": "Netherlands",
        "BE": "Belgium", "LU": "Luxembourg", "FR": "France",
        "CH": "Switzerland", "ES": "Spain", "PT": "Portugal",
        "DE": "Germany", "PL": "Poland", "AT": "Austria",
        "HU": "Hungary", "CZ": "Czech_Republic", "SK": "Slovakia",
        "IT": "Italy", "MT": "Malta", "AL": "Albania",
        "ME": "Montenegro", "MK": "Macedonia", "HR": "Croatia",
        "RS": "Serbia", "BA": "Bosnia_and_Herzegovina", "SI": "Slovenia",
        "GR": "Greece", "CY": "Cyprus", "BG": "Bulgaria",
        "MD": "Moldova", "RO": "Romania", "RU": "Russia",
        "EE": "Estonia", "LV": "Latvia", "LT": "Lithuania",
        "UA": "Ukraine", "BY": "Belarus", "AM": "Armenia",
        "GE": "Georgia", "AZ": "Azerbaijan", "FI": "Finland",
        "SE": "Sweden", "NO": "Norway", "DK": "Denmark", "IS": "Iceland",
        "CV": "Cape_Verde", "GW": "Guinea-Bissau", "GN": "Guinea",
        "GM": "Gambia", "ML": "Mali", "SN": "Senegal", "BJ": "Benin",
        "MR": "Mauritania", "NE": "Niger", "CI": "Cote_d'Ivoire",
        "BF": "Burkina_Faso", "LR": "Liberia", "SL": "Sierra_Leone",
        "GH": "Ghana", "TG": "Togo", "CM": "Cameroon", "NG": "Nigeria",
        "GA": "Gabon", "CF": "Central_African_Republic", "TD": "Chad",
        "CG": "Congo", "CD": "Democratic_Republic_of_the_Congo",
        "UG": "Uganda", "KE": "Kenya", "TZ": "Tanzania",
        "BI": "Burundi", "RW": "Rwanda", "SO": "Somalia",
        "DJ": "Djibouti", "ET": "Ethiopia", "ER": "Eritrea",
        "AO": "Angola", "MZ": "Mozambique", "ZM": "Zambia",
        "ZW": "Zimbabwe", "MW": "Malawi", "ZA": "South_Africa",
        "NA": "Namibia", "LS": "Lesotho", "BW": "Botswana",
        "SZ": "Eswatini", "MG": "Madagascar", "MU": "Mauritius",
        "MA": "Morocco", "DZ": "Algeria", "TN": "Tunisia",
        "LY": "Libya", "SD": "Sudan", "SS": "South_Sudan",
        "IR": "Iran", "TR": "Turkey", "IQ": "Iraq", "EG": "Egypt",
        "SY": "Syria", "LB": "Lebanon", "JO": "Jordan", "IL": "Israel",
        "SA": "Saudi_Arabia", "YE": "Yemen", "KW": "Kuwait",
        "QA": "Qatar", "AE": "United_Arab_Emirates", "OM": "Oman",
        "AF": "Afghanistan", "TM": "Turkmenistan", "TJ": "Tajikistan",
        "KG": "Kyrgyzstan", "UZ": "Uzbekistan", "KZ": "Kazakhstan",
        "CN": "China", "MN": "Mongolia", "TW": "Taiwan",
        "KP": "North_Korea", "KR": "South_Korea", "JP": "Japan",
        "IN": "India", "PK": "Pakistan", "BD": "Bangladesh",
        "MM": "Myanmar", "LK": "Sri_Lanka", "NP": "Nepal",
        "TH": "Thailand", "KH": "Cambodia", "LA": "Laos",
        "VN": "Vietnam", "MY": "Malaysia", "SG": "Singapore",
        "PH": "Philippines", "ID": "Indonesia",
        "AU": "Australia", "PG": "Papua_New_Guinea", "NZ": "New_Zealand",
        "FJ": "Fiji",
    }

    # For each needed country, pick only the most recent constitution
    # (reduces fetches from ~184 to ~152)
    country_constitutions: dict[str, list[dict]] = {}
    iso2_to_const_id: dict[str, str] = {}  # iso2 -> most recent const_id
    for iso2 in iso2_set:
        cid = ISO2_TO_CID.get(iso2)
        matched_consts = None
        if cid and cid in consts_by_country:
            matched_consts = consts_by_country[cid]
        else:
            # Try fuzzy match
            for cid_key in consts_by_country:
                if cid and cid_key.lower().startswith(cid.lower()[:5]):
                    matched_consts = consts_by_country[cid_key]
                    break
        if matched_consts:
            # Sort by year in id, take most recent
            def extract_year(c: dict) -> int:
                m = re.search(r"(\d{4})", c.get("id", ""))
                return int(m.group(1)) if m else 0
            most_recent = max(matched_consts, key=extract_year)
            iso2_to_const_id[iso2] = most_recent.get("id", "")

    print(f"  Matched {len(iso2_to_const_id)} countries (1 constitution each)")

    fetched = 0
    total = len(iso2_to_const_id)
    for iso2, const_id in iso2_to_const_id.items():
        fetched += 1
        year_match = re.search(r"(\d{4})", const_id)
        year = int(year_match.group(1)) if year_match else None

        if fetched % 25 == 1:
            print(f"  [{fetched}/{total}] Fetching {const_id}...")

        text = get_constitution_text(const_id)
        time.sleep(RATE_LIMIT)

        if text and len(text) > 500:
            indicators = analyze_constitution(text)
            indicators["year"] = year
            indicators["const_id"] = const_id
            country_constitutions[iso2] = [indicators]

    # Match constitutions to leader spells
    print(f"\nMatching constitutions to {len(leaders)} leader spells...")
    output_rows = []

    for ldr in leaders:
        ccode = int(ldr["ccode"]) if ldr["ccode"].isdigit() else None
        iso2 = COW_TO_ISO2.get(ccode) if ccode else None
        start = int(ldr["start_year"]) if ldr["start_year"].isdigit() else None
        end = int(ldr["end_year"]) if ldr["end_year"].isdigit() else None

        out = {
            "qid": ldr.get("qid", ""),
            "leader": ldr["leader"],
            "ccode": ldr["ccode"],
            "start_year": ldr["start_year"],
            "end_year": ldr["end_year"],
            "term_limits_absent": None,
            "president_for_life": None,
            "oath_to_person": None,
        }

        if iso2 and iso2 in country_constitutions and start:
            # Find most recent constitution in effect during leader's tenure
            candidates = country_constitutions[iso2]
            # Sort by year descending, pick first with year <= leader start
            candidates_sorted = sorted(
                [c for c in candidates if c["year"] and c["year"] <= start],
                key=lambda c: c["year"],
                reverse=True,
            )
            if not candidates_sorted:
                # Fall back to earliest available
                candidates_sorted = sorted(
                    [c for c in candidates if c["year"]],
                    key=lambda c: c["year"],
                )

            if candidates_sorted:
                best = candidates_sorted[0]
                # A1: term_limits_absent = 1 if no term limit found
                out["term_limits_absent"] = 0 if best["has_term_limit"] else 1
                if best["life_presidency"]:
                    out["term_limits_absent"] = 1
                # A2: president_for_life
                out["president_for_life"] = best["life_presidency"]
                # B9: oath_to_person
                out["oath_to_person"] = best["oath_to_person"]

        output_rows.append(out)

    # Write
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(output_rows[0].keys())
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    # Summary
    print(f"\n{'='*60}")
    print("CONSTITUTE INDICATORS SUMMARY")
    print(f"{'='*60}")
    print(f"Countries with constitution data: {len(country_constitutions)}")
    n_coded = sum(1 for r in output_rows if r["term_limits_absent"] is not None)
    print(f"Leaders with matched constitution: {n_coded} / {len(output_rows)}")
    for col in ["term_limits_absent", "president_for_life", "oath_to_person"]:
        n = sum(1 for r in output_rows if r[col] == 1)
        print(f"  {col:25s}: {n}")
    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
