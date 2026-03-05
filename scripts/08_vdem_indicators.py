#!/usr/bin/env python3
"""
Extract V-Dem indicators for personalism measurement.

Uses the R `vdemdata` package (must be installed) to extract:
  - v2clkilev  → proxy for A5 (political purges)
  - v2x_civmil → proxy for A6 (personal security control)
  - v2psbars   → supplementary (barriers to parties)
  - v2csreprss → supplementary (civil society repression)

For each Archigos leader-spell, computes the mean V-Dem score over their
tenure years and codes binary indicators using thresholds.

Usage:
  python 08_vdem_indicators.py
"""

import csv
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RECOVERED_CSV = PROJECT_ROOT / "data" / "raw" / "recovered_qids.csv"
OUTPUT_CSV = PROJECT_ROOT / "data" / "raw" / "vdem_indicators.csv"

# V-Dem variables to extract (names verified against vdemdata v15)
VDEM_VARS = [
    "country_name",
    "COWcode",
    "year",
    "v2clkill",        # Political killings (higher = fewer killings)
    "v2x_ex_military", # Military dimension of executive (higher = more military)
    "v2jupurge",       # Government purges of judiciary
    "v2psbars",        # Barriers to parties (higher = more barriers)
    "v2csreprss",      # Civil society repression (higher = more repression)
    "v2exrescon",      # Executive respects constitution (higher = more respect)
    "v2xlg_legcon",    # Legislative constraints on executive (higher = more constrained)
]

R_EXTRACT_SCRIPT = """
library(vdemdata)
vars <- c({var_list})
# Keep only post-1945 data
df <- vdem[vdem$year >= 1945, vars]
# Remove rows where COWcode is missing
df <- df[!is.na(df$COWcode), ]
write.csv(df, "{outpath}", row.names = FALSE, na = "")
cat(sprintf("Extracted %d rows, %d countries\\n", nrow(df), length(unique(df$COWcode))))
"""


def extract_vdem_via_r() -> pd.DataFrame:
    """Call Rscript to extract V-Dem data, return as DataFrame."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    var_list = ", ".join(f'"{v}"' for v in VDEM_VARS)
    script = R_EXTRACT_SCRIPT.format(var_list=var_list, outpath=tmp_path)

    with tempfile.NamedTemporaryFile(suffix=".R", mode="w", delete=False) as r_file:
        r_file.write(script)
        r_path = r_file.name

    print("Running Rscript to extract V-Dem data...")
    result = subprocess.run(
        ["Rscript", r_path],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        print(f"R stderr: {result.stderr}")
        raise RuntimeError("Rscript extraction failed")

    print(f"R output: {result.stdout.strip()}")
    df = pd.read_csv(tmp_path)

    # Clean up
    Path(tmp_path).unlink(missing_ok=True)
    Path(r_path).unlink(missing_ok=True)

    return df


def load_leaders() -> pd.DataFrame:
    """Load Archigos leaders from recovered QIDs."""
    leaders = pd.read_csv(RECOVERED_CSV)
    leaders["ccode"] = pd.to_numeric(leaders["ccode"], errors="coerce")
    leaders["start_year"] = pd.to_numeric(leaders["start_year"], errors="coerce")
    leaders["end_year"] = pd.to_numeric(leaders["end_year"], errors="coerce")
    return leaders


def merge_leader_vdem(leaders: pd.DataFrame, vdem: pd.DataFrame) -> pd.DataFrame:
    """Merge V-Dem country-year data onto leader spells via COW code + year overlap."""
    vdem["COWcode"] = pd.to_numeric(vdem["COWcode"], errors="coerce")

    results = []
    numeric_cols = ["v2clkill", "v2x_ex_military", "v2jupurge", "v2psbars",
                    "v2csreprss", "v2exrescon", "v2xlg_legcon"]

    for _, ldr in leaders.iterrows():
        ccode = ldr["ccode"]
        start = ldr["start_year"]
        end = ldr["end_year"]

        if pd.isna(ccode) or pd.isna(start) or pd.isna(end):
            results.append({col: None for col in numeric_cols})
            continue

        # Match V-Dem rows for this country during leader's tenure
        mask = (
            (vdem["COWcode"] == ccode) &
            (vdem["year"] >= start) &
            (vdem["year"] <= end)
        )
        matched = vdem.loc[mask, numeric_cols]

        if matched.empty:
            results.append({col: None for col in numeric_cols})
        else:
            # Mean over tenure years
            results.append(matched.mean().to_dict())

    scores = pd.DataFrame(results)

    # Binary coding — thresholds calibrated to BFM scale distributions
    # Continuous scores are also kept for flexible re-analysis.

    # v2clkill: BFM; higher = fewer killings. Score ≤ 0 → political_killings = 1
    scores["political_killings"] = (scores["v2clkill"] <= 0).astype(int)
    scores.loc[scores["v2clkill"].isna(), "political_killings"] = None

    # v2x_ex_military: 0-1 interval. Higher = more military character of exec.
    # Score ≥ 0.5 → military_executive = 1
    scores["military_executive"] = (scores["v2x_ex_military"] >= 0.5).astype(int)
    scores.loc[scores["v2x_ex_military"].isna(), "military_executive"] = None

    # v2jupurge: BFM; higher = more purges. Score ≥ 2 → judicial_purges = 1
    scores["judicial_purges"] = (scores["v2jupurge"] >= 2).astype(int)
    scores.loc[scores["v2jupurge"].isna(), "judicial_purges"] = None

    # v2psbars: BFM; higher = more barriers. max=2.94. Score ≥ 2 → party_barriers = 1
    scores["party_barriers"] = (scores["v2psbars"] >= 2).astype(int)
    scores.loc[scores["v2psbars"].isna(), "party_barriers"] = None

    # v2csreprss: BFM; higher = more repression. Score ≥ 2 → civil_repression = 1
    scores["civil_repression"] = (scores["v2csreprss"] >= 2).astype(int)
    scores.loc[scores["v2csreprss"].isna(), "civil_repression"] = None

    # v2exrescon: BFM; lower = less respect for constitution. Score ≤ 0 → const_disregard = 1
    scores["const_disregard"] = (scores["v2exrescon"] <= 0).astype(int)
    scores.loc[scores["v2exrescon"].isna(), "const_disregard"] = None

    # v2xlg_legcon: 0-1 interval; lower = less constrained. Score ≤ 0.25 → no_leg_constraint = 1
    scores["no_leg_constraint"] = (scores["v2xlg_legcon"] <= 0.25).astype(int)
    scores.loc[scores["v2xlg_legcon"].isna(), "no_leg_constraint"] = None

    return scores


def main():
    # Step 1: Extract V-Dem via R
    vdem = extract_vdem_via_r()
    print(f"V-Dem data: {len(vdem)} country-year rows, "
          f"{vdem['COWcode'].nunique()} countries, "
          f"years {int(vdem['year'].min())}-{int(vdem['year'].max())}")

    # Step 2: Load leaders
    leaders = load_leaders()
    print(f"Leaders: {len(leaders)} spells")

    # Step 3: Merge
    print("Merging leader spells with V-Dem scores...")
    scores = merge_leader_vdem(leaders, vdem)

    # Step 4: Combine and write
    output = leaders[["qid", "leader", "ccode", "start_year", "end_year"]].copy()
    for col in scores.columns:
        output[col] = scores[col].values

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(OUTPUT_CSV, index=False)

    # Summary
    print(f"\n{'='*60}")
    print("V-DEM INDICATORS SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders with V-Dem match: {scores['v2clkill'].notna().sum()} / {len(leaders)}")
    binary_cols = ["political_killings", "military_executive", "judicial_purges",
                   "party_barriers", "civil_repression", "const_disregard",
                   "no_leg_constraint"]
    for col in binary_cols:
        n = int(scores[col].sum()) if scores[col].notna().any() else 0
        total = int(scores[col].notna().sum())
        print(f"  {col:25s}: {n:4d} / {total} coded 1")
    print(f"\nWrote {len(output)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
