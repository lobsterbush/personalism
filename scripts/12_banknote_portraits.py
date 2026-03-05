#!/usr/bin/env python3
"""
Identify leaders depicted on banknotes via Wikipedia currency article scraping.

Strategy:
  1. For each country, find the Wikipedia article for its currency
     (via search API).
  2. Fetch the article's plain text.
  3. Search for leader surnames in banknote/portrait context.
  4. Code currency_portrait = 1 if the leader's name appears near
     portrait/obverse/banknote keywords.

Usage:
  python 12_banknote_portraits.py
"""

import csv
import re
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV    = PROJECT_ROOT / "data" / "raw" / "banknote_indicators.csv"

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "PersonalismProject/0.3 (academic research; mailto:charles.crabtree@monash.edu)"
HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json"}

# Context window: how many characters around a leader name to check
# for banknote-related keywords
CONTEXT_WINDOW = 300
BANKNOTE_KEYWORDS = [
    "banknote", "bank note", "portrait", "depict", "obverse",
    "front", "denomination", "note", "bill", "currency",
    "serie", "issued", "printing", "featured", "appear",
]

# COW code -> country name for Wikipedia search
COW_TO_COUNTRY: dict[int, str] = {
    2: "United States", 20: "Canada", 31: "Bahamas", 40: "Cuba",
    41: "Haiti", 42: "Dominican Republic", 51: "Jamaica",
    52: "Trinidad and Tobago", 53: "Barbados", 70: "Mexico",
    80: "Belize", 90: "Guatemala", 91: "Honduras", 92: "El Salvador",
    93: "Nicaragua", 94: "Costa Rica", 95: "Panama", 100: "Colombia",
    101: "Venezuela", 110: "Guyana", 115: "Suriname", 130: "Ecuador",
    135: "Peru", 140: "Brazil", 145: "Bolivia", 150: "Paraguay",
    155: "Chile", 160: "Argentina", 165: "Uruguay",
    200: "United Kingdom", 205: "Ireland", 210: "Netherlands",
    211: "Belgium", 212: "Luxembourg", 220: "France",
    225: "Switzerland", 230: "Spain", 235: "Portugal",
    255: "Germany", 260: "Germany", 265: "Germany",
    290: "Poland", 305: "Austria", 310: "Hungary",
    315: "Czechoslovakia", 316: "Czech Republic", 317: "Slovakia",
    325: "Italy", 338: "Malta", 339: "Albania", 341: "Montenegro",
    343: "North Macedonia", 344: "Croatia", 345: "Serbia",
    346: "Bosnia and Herzegovina", 349: "Slovenia", 350: "Greece",
    352: "Cyprus", 355: "Bulgaria", 359: "Moldova", 360: "Romania",
    364: "Russia", 365: "Russia", 366: "Estonia", 367: "Latvia",
    368: "Lithuania", 369: "Ukraine", 370: "Belarus", 371: "Armenia",
    372: "Georgia", 373: "Azerbaijan", 375: "Finland", 380: "Sweden",
    385: "Norway", 390: "Denmark", 395: "Iceland",
    402: "Cape Verde", 404: "Guinea-Bissau", 411: "Guinea",
    420: "Gambia", 432: "Mali", 433: "Senegal", 434: "Benin",
    435: "Mauritania", 436: "Niger", 437: "Ivory Coast",
    438: "Guinea", 439: "Burkina Faso", 450: "Liberia",
    451: "Sierra Leone", 452: "Ghana", 461: "Togo", 471: "Cameroon",
    475: "Nigeria", 481: "Gabon", 482: "Central African Republic",
    483: "Chad", 484: "Republic of the Congo",
    490: "Democratic Republic of the Congo",
    500: "Uganda", 501: "Kenya", 510: "Tanzania", 516: "Burundi",
    517: "Rwanda", 520: "Somalia", 522: "Djibouti", 530: "Ethiopia",
    531: "Eritrea", 540: "Angola", 541: "Mozambique", 551: "Zambia",
    552: "Zimbabwe", 553: "Malawi", 560: "South Africa",
    565: "Namibia", 570: "Lesotho", 571: "Botswana",
    572: "Eswatini", 580: "Madagascar", 590: "Mauritius",
    600: "Morocco", 615: "Algeria", 616: "Tunisia", 620: "Libya",
    625: "Sudan", 626: "South Sudan", 630: "Iran", 640: "Turkey",
    645: "Iraq", 651: "Egypt", 652: "Syria", 660: "Lebanon",
    663: "Jordan", 666: "Israel", 670: "Saudi Arabia",
    678: "Yemen", 679: "Yemen", 680: "Yemen", 690: "Kuwait",
    694: "Qatar", 696: "United Arab Emirates", 698: "Oman",
    700: "Afghanistan", 701: "Turkmenistan", 702: "Tajikistan",
    703: "Kyrgyzstan", 704: "Uzbekistan", 705: "Kazakhstan",
    710: "China", 712: "Mongolia", 713: "Taiwan",
    730: "Korea", 731: "North Korea", 732: "South Korea",
    740: "Japan", 750: "India", 770: "Pakistan", 771: "Bangladesh",
    775: "Myanmar", 780: "Sri Lanka", 790: "Nepal", 800: "Thailand",
    811: "Cambodia", 812: "Laos", 816: "Vietnam", 817: "Vietnam",
    820: "Malaysia", 830: "Singapore", 840: "Philippines",
    850: "Indonesia", 900: "Australia", 910: "Papua New Guinea",
    920: "New Zealand", 950: "Fiji",
}


def get_currency_articles(country: str) -> list[str]:
    """Search Wikipedia for currency/banknote articles for a country."""
    titles = set()
    for query in [f"{country} currency", f"{country} banknotes"]:
        params = {
            "action": "query", "list": "search",
            "srsearch": query, "srlimit": 5, "format": "json",
        }
        try:
            r = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=10)
            for hit in r.json().get("query", {}).get("search", []):
                t = hit["title"]
                t_lower = t.lower()
                if any(kw in t_lower for kw in [
                    "dollar", "peso", "franc", "pound", "euro", "yen", "yuan",
                    "won", "ruble", "rupee", "dinar", "dirham", "real", "lira",
                    "mark", "krona", "krone", "zloty", "forint", "lei", "koruna",
                    "shilling", "rand", "baht", "dong", "ringgit", "taka",
                    "rial", "riyal", "manat", "som", "tenge", "lari", "dram",
                    "hryvnia", "birr", "nakfa", "kwanza", "kwacha", "cedi",
                    "naira", "leone", "dalasi", "ouguiya", "banknote",
                    "currency", country.lower().split()[0],
                ]):
                    titles.add(t)
        except Exception:
            pass
    return list(titles)


def get_article_text(title: str) -> str:
    """Fetch plain text of a Wikipedia article."""
    params = {
        "action": "query", "titles": title,
        "prop": "extracts", "explaintext": True,
        "format": "json",
    }
    try:
        r = requests.get(WIKIPEDIA_API, params=params, headers=HEADERS, timeout=15)
        pages = r.json().get("query", {}).get("pages", {})
        for pid, page in pages.items():
            if int(pid) > 0:
                return page.get("extract", "")
    except Exception:
        pass
    return ""


def normalize_name(name: str) -> str:
    """Extract the most distinctive part of a leader's name for matching."""
    name = re.sub(r"^(General|President|King|Prince|Sheikh|Dr\.?|Sir)\s+",
                  "", name, flags=re.IGNORECASE)
    parts = name.strip().split()
    if len(parts) >= 2:
        return parts[-1]
    return parts[0] if parts else name


def check_leader_in_text(leader_name: str, text: str) -> bool:
    """Check if leader name appears in banknote-relevant context."""
    text_lower = text.lower()
    name = normalize_name(leader_name)

    if len(name) < 3:
        return False

    pattern = re.compile(re.escape(name.lower()))
    for match in pattern.finditer(text_lower):
        start = max(0, match.start() - CONTEXT_WINDOW)
        end = min(len(text_lower), match.end() + CONTEXT_WINDOW)
        context = text_lower[start:end]

        if any(kw in context for kw in BANKNOTE_KEYWORDS):
            return True

    return False


def main():
    print("Loading leaders...")
    leaders = []
    with open(RECOVERED_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            leaders.append(row)
    print(f"  {len(leaders)} leader spells")

    # Group leaders by country code
    leaders_by_ccode: dict[int, list[dict]] = {}
    for ldr in leaders:
        cc = int(ldr["ccode"]) if ldr["ccode"].isdigit() else None
        if cc:
            leaders_by_ccode.setdefault(cc, []).append(ldr)

    unique_countries = sorted(leaders_by_ccode.keys())
    print(f"  {len(unique_countries)} unique countries")

    # For each country, find and scrape currency articles
    depicted: set[str] = set()
    country_articles: dict[int, list[str]] = {}

    for i, ccode in enumerate(unique_countries):
        country_name = COW_TO_COUNTRY.get(ccode, f"COW-{ccode}")
        if i % 20 == 0:
            print(f"  [{i+1}/{len(unique_countries)}] {country_name}...")

        articles = get_currency_articles(country_name)
        time.sleep(0.5)

        if not articles:
            continue

        country_articles[ccode] = articles

        # Fetch article texts (limit to 3 most relevant)
        combined_text = ""
        for title in articles[:3]:
            text = get_article_text(title)
            combined_text += f" {text} "
            time.sleep(0.3)

        if len(combined_text) < 200:
            continue

        # Check each leader from this country
        for ldr in leaders_by_ccode[ccode]:
            name = ldr["leader"]
            if check_leader_in_text(name, combined_text):
                key = f"{name}__{ccode}__{ldr['start_year']}"
                depicted.add(key)

    # Write output
    print(f"\nFound {len(depicted)} leader-spells depicted on currency")

    output_rows = []
    for ldr in leaders:
        key = f"{ldr['leader']}__{ldr.get('ccode','')}__{ldr.get('start_year','')}"
        output_rows.append({
            "qid": ldr.get("qid", ""),
            "leader": ldr["leader"],
            "ccode": ldr.get("ccode", ""),
            "start_year": ldr.get("start_year", ""),
            "end_year": ldr.get("end_year", ""),
            "currency_portrait": 1 if key in depicted else 0,
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
    print(f"Countries with articles scraped: {len(country_articles)}")
    print(f"Leaders with currency depiction: {n_portrait} / {len(output_rows)}")

    if depicted:
        print("\nDepicted leaders:")
        for ldr in output_rows:
            if ldr["currency_portrait"]:
                print(f"  {ldr['leader']} ({COW_TO_COUNTRY.get(int(ldr['ccode']), '?')}, "
                      f"{ldr['start_year']}-{ldr['end_year']})")

    print(f"\nWrote {len(output_rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
