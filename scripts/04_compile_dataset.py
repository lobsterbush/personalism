#!/usr/bin/env python3
"""
Compile all scraped data sources into a unified country-leader-year panel.

Merges output from:
  - 01_scrape_banknotes.py   (B1: currency_portrait)
  - 02_query_constitute.py   (A1, A2, A4, B9)
  - 03_query_wikidata.py      (A3, B3, B4)

Outputs:
  - data/processed/personalism_panel.csv  (analysis-ready panel)
  - dashboard/data/personalism.json       (dashboard-ready JSON)

Usage:
  python 04_compile_dataset.py
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard" / "data"

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list[dict]:
    """Load a CSV file into a list of dicts."""
    if not path.exists():
        print(f"  Warning: {path} not found, skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_json(path: Path) -> list[dict]:
    """Load a JSON file."""
    if not path.exists():
        print(f"  Warning: {path} not found, skipping.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Merge logic
# ---------------------------------------------------------------------------

def build_panel(
    banknotes: list[dict],
    constitutions: list[dict],
    wikidata: list[dict],
) -> list[dict]:
    """
    Merge all data sources into a country-leader-year panel.

    The panel is keyed on (iso3, leader, year).
    """
    # Index banknote data by (iso3, year)
    banknote_idx: dict[tuple[str, int], dict] = {}
    for row in banknotes:
        try:
            key = (row["iso3"], int(row["year"]))
            banknote_idx[key] = row
        except (KeyError, ValueError, TypeError):
            continue

    # Index constitution data by iso3 (take most recent)
    constitution_idx: dict[str, dict] = {}
    for row in constitutions:
        iso3 = row.get("iso3", "")
        if iso3:
            # Keep the row with the latest constitution_year
            existing = constitution_idx.get(iso3)
            if not existing:
                constitution_idx[iso3] = row
            else:
                try:
                    if int(row.get("constitution_year") or 0) > int(existing.get("constitution_year") or 0):
                        constitution_idx[iso3] = row
                except (ValueError, TypeError):
                    pass

    # Build panel from Wikidata leaders (the backbone)
    panel = []
    for leader in wikidata:
        iso3 = leader.get("iso3", "")
        name = leader.get("leader", "")
        start = int(leader.get("start_year", 0))
        end = int(leader.get("end_year", 0))

        if not iso3 or not start or not end:
            continue

        # Create one row per year of rule
        for year in range(start, min(end, 2026) + 1):
            row = {
                "iso3": iso3,
                "leader": name,
                "year": year,
                # Wikidata indicators
                "family_in_govt_count": leader.get("family_in_govt_count", ""),
                "family_in_govt_binary": leader.get("family_in_govt_binary", ""),
                "places_named_count": leader.get("places_named_count", ""),
                "places_named_binary": leader.get("places_named_binary", ""),
                "grandiose_titles_count": leader.get("grandiose_titles_count", ""),
                "grandiose_titles_binary": leader.get("grandiose_titles_binary", ""),
            }

            # Merge banknote data
            bn = banknote_idx.get((iso3, year))
            if bn:
                row["currency_portrait"] = bn.get("currency_portrait", "")
            else:
                row["currency_portrait"] = ""

            # Merge constitution data
            const = constitution_idx.get(iso3)
            if const:
                row["term_limits_mentioned"] = const.get("term_limits_mentioned", "")
                row["term_limits_removed"] = const.get("term_limits_removed_or_absent", "")
                row["president_for_life"] = const.get("president_for_life_provision", "")
                row["oath_to_person"] = const.get("oath_to_person", "")
                row["appointment_monopoly"] = const.get("president_appoints_unilateral", "")
            else:
                row["term_limits_mentioned"] = ""
                row["term_limits_removed"] = ""
                row["president_for_life"] = ""
                row["oath_to_person"] = ""
                row["appointment_monopoly"] = ""

            panel.append(row)

    return panel


def panel_to_dashboard_json(panel: list[dict]) -> dict:
    """
    Convert the panel into a JSON structure optimized for the dashboard.

    Structure:
    {
      "metadata": {...},
      "countries": [
        {
          "iso3": "PRK",
          "name": "North Korea",
          "leaders": [
            {
              "name": "Kim Jong-un",
              "start_year": 2011,
              "end_year": 2026,
              "observations": [
                {"year": 2020, "indicators": {...}},
                ...
              ]
            }
          ]
        }
      ],
      "indicators": [...]
    }
    """
    # Group by country and leader
    country_leaders: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for row in panel:
        country_leaders[row["iso3"]][row["leader"]].append(row)

    indicator_keys = [
        "currency_portrait", "term_limits_removed", "president_for_life",
        "oath_to_person", "appointment_monopoly",
        "family_in_govt_binary", "places_named_binary", "grandiose_titles_binary",
    ]

    countries = []
    for iso3, leaders in sorted(country_leaders.items()):
        leader_list = []
        for leader_name, rows in leaders.items():
            years = sorted(rows, key=lambda r: r["year"])
            observations = []
            for r in years:
                indicators = {}
                for k in indicator_keys:
                    val = r.get(k, "")
                    if val != "" and val is not None:
                        try:
                            indicators[k] = int(val)
                        except (ValueError, TypeError):
                            indicators[k] = None
                    else:
                        indicators[k] = None
                observations.append({
                    "year": int(r["year"]),
                    "indicators": indicators,
                })

            leader_list.append({
                "name": leader_name,
                "start_year": int(years[0]["year"]),
                "end_year": int(years[-1]["year"]),
                "observations": observations,
            })

        countries.append({
            "iso3": iso3,
            "leaders": leader_list,
        })

    return {
        "metadata": {
            "version": "0.1",
            "description": "Personalism in Dictatorships dataset",
            "sources": ["numista", "constitute_project", "wikidata"],
            "indicators": [
                {"key": "currency_portrait", "label": "Currency Portrait", "dimension": "cult", "source": "Numista"},
                {"key": "term_limits_removed", "label": "Term Limits Removed", "dimension": "power", "source": "Constitute"},
                {"key": "president_for_life", "label": "President for Life", "dimension": "power", "source": "Constitute"},
                {"key": "oath_to_person", "label": "Loyalty Oath to Person", "dimension": "cult", "source": "Constitute"},
                {"key": "appointment_monopoly", "label": "Appointment Monopoly", "dimension": "power", "source": "Constitute"},
                {"key": "family_in_govt_binary", "label": "Family in Government", "dimension": "power", "source": "Wikidata"},
                {"key": "places_named_binary", "label": "Places Named After", "dimension": "cult", "source": "Wikidata"},
                {"key": "grandiose_titles_binary", "label": "Grandiose Titles", "dimension": "cult", "source": "Wikidata"},
            ],
        },
        "countries": countries,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading scraped data...")

    banknotes = load_csv(RAW_DIR / "banknotes_summary.csv")
    constitutions = load_csv(RAW_DIR / "constitutions.csv")
    wikidata = load_json(RAW_DIR / "wikidata_leaders.json")

    print(f"  Banknotes: {len(banknotes)} rows")
    print(f"  Constitutions: {len(constitutions)} rows")
    print(f"  Wikidata leaders: {len(wikidata)} entries")

    print("\nBuilding panel...")
    panel = build_panel(banknotes, constitutions, wikidata)
    print(f"  Panel rows: {len(panel)}")

    # Write CSV
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = PROCESSED_DIR / "personalism_panel.csv"
    if panel:
        fieldnames = list(panel[0].keys())
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(panel)
        print(f"  Wrote {csv_path}")

    # Write dashboard JSON
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    dashboard_data = panel_to_dashboard_json(panel)
    json_path = DASHBOARD_DIR / "personalism.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
    print(f"  Wrote {json_path}")

    print("\nDone.")


if __name__ == "__main__":
    main()
