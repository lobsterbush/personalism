#!/usr/bin/env python3
"""
Compile all personalism indicators into unified dataset and dashboard JSON.

Merges:
  - recovered_qids.csv (base leader list, 1057 spells)
  - wikidata_leaders.csv (family_in_govt, places_named, grandiose_titles)
  - wikidata_extra.csv (monuments, holiday, hagiography)
  - vdem_indicators.csv (political_killings, military_executive, etc.)
  - wikipedia_categories.csv (president_for_life, cult_of_personality)
  - constitute_indicators.csv (term_limits_absent, president_for_life, oath)
  - banknote_indicators.csv (currency_portrait)

Outputs:
  - data/compiled/personalism_full.csv (full merged dataset)
  - dashboard/data/personalism.json (dashboard JSON)

Usage:
  python 13_compile_all_indicators.py
"""

import csv
import json
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW = PROJECT_ROOT / "data" / "raw"
COMPILED_DIR = PROJECT_ROOT / "data" / "compiled"
DASHBOARD_JSON = PROJECT_ROOT / "dashboard" / "data" / "personalism.json"

# COW code -> (ISO3, country name)
COW_TO_META: dict[int, tuple[str, str]] = {
    2: ("USA", "United States"), 20: ("CAN", "Canada"), 31: ("BHS", "Bahamas"),
    40: ("CUB", "Cuba"), 41: ("HTI", "Haiti"), 42: ("DOM", "Dominican Republic"),
    51: ("JAM", "Jamaica"), 52: ("TTO", "Trinidad and Tobago"),
    53: ("BRB", "Barbados"), 70: ("MEX", "Mexico"), 80: ("BLZ", "Belize"),
    90: ("GTM", "Guatemala"), 91: ("HND", "Honduras"), 92: ("SLV", "El Salvador"),
    93: ("NIC", "Nicaragua"), 94: ("CRI", "Costa Rica"), 95: ("PAN", "Panama"),
    100: ("COL", "Colombia"), 101: ("VEN", "Venezuela"), 110: ("GUY", "Guyana"),
    115: ("SUR", "Suriname"), 130: ("ECU", "Ecuador"), 135: ("PER", "Peru"),
    140: ("BRA", "Brazil"), 145: ("BOL", "Bolivia"), 150: ("PRY", "Paraguay"),
    155: ("CHL", "Chile"), 160: ("ARG", "Argentina"), 165: ("URY", "Uruguay"),
    200: ("GBR", "United Kingdom"), 205: ("IRL", "Ireland"),
    210: ("NLD", "Netherlands"), 211: ("BEL", "Belgium"),
    212: ("LUX", "Luxembourg"), 220: ("FRA", "France"),
    225: ("CHE", "Switzerland"), 230: ("ESP", "Spain"), 235: ("PRT", "Portugal"),
    255: ("DEU", "Germany"), 260: ("DEU", "Germany"), 265: ("DEU", "Germany"),
    290: ("POL", "Poland"), 305: ("AUT", "Austria"), 310: ("HUN", "Hungary"),
    315: ("CZE", "Czechoslovakia"), 316: ("CZE", "Czech Republic"),
    317: ("SVK", "Slovakia"), 325: ("ITA", "Italy"), 338: ("MLT", "Malta"),
    339: ("ALB", "Albania"), 341: ("MNE", "Montenegro"),
    343: ("MKD", "North Macedonia"), 344: ("HRV", "Croatia"),
    345: ("SRB", "Yugoslavia/Serbia"), 346: ("BIH", "Bosnia"),
    349: ("SVN", "Slovenia"), 350: ("GRC", "Greece"), 352: ("CYP", "Cyprus"),
    355: ("BGR", "Bulgaria"), 359: ("MDA", "Moldova"), 360: ("ROU", "Romania"),
    364: ("RUS", "Russia"), 365: ("RUS", "Russia"),
    366: ("EST", "Estonia"), 367: ("LVA", "Latvia"), 368: ("LTU", "Lithuania"),
    369: ("UKR", "Ukraine"), 370: ("BLR", "Belarus"), 371: ("ARM", "Armenia"),
    372: ("GEO", "Georgia"), 373: ("AZE", "Azerbaijan"),
    375: ("FIN", "Finland"), 380: ("SWE", "Sweden"), 385: ("NOR", "Norway"),
    390: ("DNK", "Denmark"), 395: ("ISL", "Iceland"),
    402: ("CPV", "Cape Verde"), 404: ("GNB", "Guinea-Bissau"),
    411: ("GIN", "Guinea"), 420: ("GMB", "Gambia"), 432: ("MLI", "Mali"),
    433: ("SEN", "Senegal"), 434: ("BEN", "Benin"),
    435: ("MRT", "Mauritania"), 436: ("NER", "Niger"),
    437: ("CIV", "Ivory Coast"), 438: ("GIN", "Guinea"),
    439: ("BFA", "Burkina Faso"), 450: ("LBR", "Liberia"),
    451: ("SLE", "Sierra Leone"), 452: ("GHA", "Ghana"), 461: ("TGO", "Togo"),
    471: ("CMR", "Cameroon"), 475: ("NGA", "Nigeria"), 481: ("GAB", "Gabon"),
    482: ("CAF", "Central African Republic"), 483: ("TCD", "Chad"),
    484: ("COG", "Congo-Brazzaville"), 490: ("COD", "Congo-Kinshasa"),
    500: ("UGA", "Uganda"), 501: ("KEN", "Kenya"), 510: ("TZA", "Tanzania"),
    516: ("BDI", "Burundi"), 517: ("RWA", "Rwanda"), 520: ("SOM", "Somalia"),
    522: ("DJI", "Djibouti"), 530: ("ETH", "Ethiopia"), 531: ("ERI", "Eritrea"),
    540: ("AGO", "Angola"), 541: ("MOZ", "Mozambique"), 551: ("ZMB", "Zambia"),
    552: ("ZWE", "Zimbabwe"), 553: ("MWI", "Malawi"),
    560: ("ZAF", "South Africa"), 565: ("NAM", "Namibia"),
    570: ("LSO", "Lesotho"), 571: ("BWA", "Botswana"),
    572: ("SWZ", "Eswatini"), 580: ("MDG", "Madagascar"),
    590: ("MUS", "Mauritius"), 600: ("MAR", "Morocco"),
    615: ("DZA", "Algeria"), 616: ("TUN", "Tunisia"), 620: ("LBY", "Libya"),
    625: ("SDN", "Sudan"), 626: ("SSD", "South Sudan"),
    630: ("IRN", "Iran"), 640: ("TUR", "Turkey"), 645: ("IRQ", "Iraq"),
    651: ("EGY", "Egypt"), 652: ("SYR", "Syria"), 660: ("LBN", "Lebanon"),
    663: ("JOR", "Jordan"), 666: ("ISR", "Israel"),
    670: ("SAU", "Saudi Arabia"), 678: ("YEM", "Yemen"),
    679: ("YEM", "Yemen"), 680: ("YEM", "Yemen"),
    690: ("KWT", "Kuwait"), 694: ("QAT", "Qatar"), 696: ("ARE", "UAE"),
    698: ("OMN", "Oman"), 700: ("AFG", "Afghanistan"),
    701: ("TKM", "Turkmenistan"), 702: ("TJK", "Tajikistan"),
    703: ("KGZ", "Kyrgyzstan"), 704: ("UZB", "Uzbekistan"),
    705: ("KAZ", "Kazakhstan"), 710: ("CHN", "China"),
    712: ("MNG", "Mongolia"), 713: ("TWN", "Taiwan"),
    730: ("KOR", "Korea"), 731: ("PRK", "North Korea"),
    732: ("KOR", "South Korea"), 740: ("JPN", "Japan"),
    750: ("IND", "India"), 770: ("PAK", "Pakistan"),
    771: ("BGD", "Bangladesh"), 775: ("MMR", "Myanmar"),
    780: ("LKA", "Sri Lanka"), 790: ("NPL", "Nepal"),
    800: ("THA", "Thailand"), 811: ("KHM", "Cambodia"),
    812: ("LAO", "Laos"), 816: ("VNM", "Vietnam"), 817: ("VNM", "Vietnam"),
    820: ("MYS", "Malaysia"), 830: ("SGP", "Singapore"),
    840: ("PHL", "Philippines"), 850: ("IDN", "Indonesia"),
    900: ("AUS", "Australia"), 910: ("PNG", "Papua New Guinea"),
    920: ("NZL", "New Zealand"), 950: ("FJI", "Fiji"),
}

# Final indicator set for the dashboard
INDICATORS = [
    # Power concentration (A-dimension)
    {"key": "term_limits_absent",  "label": "Term Limits Absent",       "dimension": "power", "source": "Constitute Project"},
    {"key": "president_for_life",  "label": "President for Life",       "dimension": "power", "source": "Constitute + Wikipedia"},
    {"key": "family_in_govt",      "label": "Family in Government",     "dimension": "power", "source": "Wikidata"},
    {"key": "political_killings",  "label": "Political Killings",       "dimension": "power", "source": "V-Dem (v2clkill)"},
    {"key": "military_executive",  "label": "Military Executive",       "dimension": "power", "source": "V-Dem (v2x_ex_military)"},
    {"key": "judicial_purges",     "label": "Judicial Purges",          "dimension": "power", "source": "V-Dem (v2jupurge)"},
    {"key": "const_disregard",     "label": "Constitutional Disregard", "dimension": "power", "source": "V-Dem (v2exrescon)"},
    {"key": "no_leg_constraint",   "label": "No Legislative Constraint","dimension": "power", "source": "V-Dem (v2xlg_legcon)"},
    # Personality cult (B-dimension)
    {"key": "places_named",        "label": "Places Named After",       "dimension": "cult",  "source": "Wikidata"},
    {"key": "grandiose_titles",    "label": "Grandiose Titles",         "dimension": "cult",  "source": "Wikidata"},
    {"key": "monuments",           "label": "Monuments/Statues",        "dimension": "cult",  "source": "Wikidata"},
    {"key": "birthday_holiday",    "label": "Birthday as Holiday",      "dimension": "cult",  "source": "Wikidata"},
    {"key": "hagiography",         "label": "State Hagiography",        "dimension": "cult",  "source": "Wikidata"},
    {"key": "cult_of_personality", "label": "Cult of Personality (cat)","dimension": "cult",  "source": "Wikipedia categories"},
    {"key": "currency_portrait",   "label": "Currency Portrait",        "dimension": "cult",  "source": "Wikidata"},
    {"key": "oath_to_person",      "label": "Loyalty Oath to Person",   "dimension": "cult",  "source": "Constitute Project"},
]


def load_csv(path: Path) -> list[dict]:
    """Load CSV, return list of row dicts."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def make_key(row: dict) -> str:
    """Create a join key from leader + ccode + start_year."""
    return f"{row.get('leader','')}__{row.get('ccode','')}__{row.get('start_year','')}"


def safe_int(val) -> int | None:
    """Parse to int or None."""
    if val is None or val == "" or val == "None":
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def main():
    print("Loading all data sources...")

    # Base: recovered QIDs (1057 leaders)
    base = load_csv(RAW / "recovered_qids.csv")
    print(f"  Base leaders: {len(base)}")

    # Index all supplementary sources by join key
    def index_by_key(rows: list[dict]) -> dict[str, dict]:
        idx = {}
        for r in rows:
            k = make_key(r)
            idx[k] = r
        return idx

    # Also index original wikidata_leaders by QID (it uses iso3 not ccode)
    wiki_orig = load_csv(RAW / "wikidata_leaders.csv")
    wiki_by_qid: dict[str, dict] = {}
    for r in wiki_orig:
        qid = r.get("qid", "")
        if qid:
            # May have multiple spells; use qid+start_year
            wiki_by_qid[f"{qid}__{r.get('start_year','')}"] = r

    wiki_extra_idx = index_by_key(load_csv(RAW / "wikidata_extra.csv"))
    vdem_idx = index_by_key(load_csv(RAW / "vdem_indicators.csv"))
    wikicat_idx = {r.get("qid", ""): r for r in load_csv(RAW / "wikipedia_categories.csv")}
    const_idx = index_by_key(load_csv(RAW / "constitute_indicators.csv"))
    bank_idx = index_by_key(load_csv(RAW / "banknote_indicators.csv"))

    print(f"  Wikidata original: {len(wiki_orig)}")
    print(f"  Wikidata extra: {len(wiki_extra_idx)}")
    print(f"  V-Dem: {len(vdem_idx)}")
    print(f"  Wikipedia categories: {len(wikicat_idx)}")
    print(f"  Constitute: {len(const_idx)}")
    print(f"  Banknotes: {len(bank_idx)}")

    # Merge into unified records
    print("\nMerging indicators...")
    compiled = []
    indicator_keys = [ind["key"] for ind in INDICATORS]

    for row in base:
        key = make_key(row)
        qid = row.get("qid", "")
        ccode = safe_int(row.get("ccode"))
        iso3, country_name = COW_TO_META.get(ccode, ("UNK", f"COW-{ccode}"))

        record = {
            "qid": qid,
            "leader": row["leader"],
            "ccode": row.get("ccode", ""),
            "iso3": iso3,
            "country": country_name,
            "start_year": row.get("start_year", ""),
            "end_year": row.get("end_year", ""),
        }

        # Initialize all indicators as None
        indicators: dict[str, int | None] = {k: None for k in indicator_keys}

        # --- Wikidata original (family, places, titles) ---
        wk = f"{qid}__{row.get('start_year','')}"
        wo = wiki_by_qid.get(wk, {})
        if wo:
            indicators["family_in_govt"] = safe_int(wo.get("family_in_govt_binary"))
            indicators["places_named"] = safe_int(wo.get("places_named_binary"))
            indicators["grandiose_titles"] = safe_int(wo.get("grandiose_titles_binary"))

        # --- Wikidata extra (monuments, holiday, hagiography) ---
        we = wiki_extra_idx.get(key, {})
        if we:
            indicators["monuments"] = safe_int(we.get("monuments_binary"))
            indicators["birthday_holiday"] = safe_int(we.get("holiday_binary"))
            indicators["hagiography"] = safe_int(we.get("hagiography_binary"))

        # --- V-Dem ---
        vd = vdem_idx.get(key, {})
        if vd:
            indicators["political_killings"] = safe_int(vd.get("political_killings"))
            indicators["military_executive"] = safe_int(vd.get("military_executive"))
            indicators["judicial_purges"] = safe_int(vd.get("judicial_purges"))
            indicators["const_disregard"] = safe_int(vd.get("const_disregard"))
            indicators["no_leg_constraint"] = safe_int(vd.get("no_leg_constraint"))

        # --- Wikipedia categories ---
        wc = wikicat_idx.get(qid, {})
        if wc:
            indicators["cult_of_personality"] = safe_int(wc.get("cult_of_personality"))
            # Merge president_for_life from wiki categories (OR with constitute)
            wp_life = safe_int(wc.get("president_for_life"))
            if wp_life == 1:
                indicators["president_for_life"] = 1

        # --- Constitute ---
        cn = const_idx.get(key, {})
        if cn:
            indicators["term_limits_absent"] = safe_int(cn.get("term_limits_absent"))
            # OR with Wikipedia categories for president_for_life
            cp_life = safe_int(cn.get("president_for_life"))
            if cp_life == 1:
                indicators["president_for_life"] = 1
            elif indicators["president_for_life"] is None:
                indicators["president_for_life"] = cp_life
            indicators["oath_to_person"] = safe_int(cn.get("oath_to_person"))

        # --- Banknotes ---
        bn = bank_idx.get(key, {})
        if bn:
            indicators["currency_portrait"] = safe_int(bn.get("currency_portrait"))

        record["indicators"] = indicators
        compiled.append(record)

    # Write compiled CSV
    COMPILED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = COMPILED_DIR / "personalism_full.csv"
    csv_fields = ["qid", "leader", "ccode", "iso3", "country", "start_year", "end_year"] + indicator_keys
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
        writer.writeheader()
        for rec in compiled:
            flat = {k: rec[k] for k in ["qid", "leader", "ccode", "iso3", "country", "start_year", "end_year"]}
            flat.update(rec["indicators"])
            writer.writerow(flat)
    print(f"  Wrote {len(compiled)} rows to {csv_path}")

    # Build dashboard JSON
    print("\nBuilding dashboard JSON...")
    countries_dict: dict[str, list] = {}
    for rec in compiled:
        iso3 = rec["iso3"]
        if iso3 not in countries_dict:
            countries_dict[iso3] = {"iso3": iso3, "name": rec["country"], "leaders": []}
        countries_dict[iso3]["leaders"].append({
            "name": rec["leader"],
            "start_year": safe_int(rec["start_year"]),
            "end_year": safe_int(rec["end_year"]),
            "indicators": rec["indicators"],
        })

    dashboard = {
        "metadata": {
            "version": "0.3",
            "description": "Personalism indicators for Archigos leaders (post-1945)",
            "last_updated": str(date.today()),
            "source": "Wikidata + V-Dem + Constitute Project + Wikipedia + Archigos 4.1",
            "n_leaders": len(compiled),
            "indicators": INDICATORS,
            "universe": {
                "total_archigos": 2090,
                "with_wikipedia": 861,
                "with_wikidata": 918,
                "with_any_indicator": len(compiled),
            },
        },
        "countries": sorted(countries_dict.values(), key=lambda c: c["iso3"]),
    }

    DASHBOARD_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(DASHBOARD_JSON, "w", encoding="utf-8") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  Wrote dashboard JSON to {DASHBOARD_JSON}")

    # Summary
    print(f"\n{'='*60}")
    print("COMPILATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total leader-spells: {len(compiled)}")
    print(f"Countries: {len(countries_dict)}")
    print(f"Indicators: {len(INDICATORS)}")
    print()
    for ind in INDICATORS:
        k = ind["key"]
        coded = sum(1 for r in compiled if r["indicators"][k] is not None)
        positive = sum(1 for r in compiled if r["indicators"][k] == 1)
        missing = len(compiled) - coded
        print(f"  {ind['label']:30s}: {positive:4d} positive, {coded:4d} coded, {missing:4d} missing")


if __name__ == "__main__":
    main()
