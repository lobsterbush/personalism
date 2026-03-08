#!/usr/bin/env python3
"""
Download GWF Autocratic Regimes data and match to personalism leaders.

Downloads the Geddes, Wright, and Frantz (2014) autocratic regimes dataset,
extracts regime type classifications, and matches to our leader-spells by
COW code and year overlap. Codes gwf_personal = 1 if the regime type
contains 'personal' (personal, party-personal, military-personal, etc.).

Usage:
    python 17_gwf_comparison.py
"""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
THETA_CSV = PROJECT_ROOT / "data" / "compiled" / "personalism_theta.csv"
OUTPUT_CSV = PROJECT_ROOT / "data" / "raw" / "gwf_comparison.csv"

GWF_ZIP = PROJECT_ROOT / "data" / "raw" / "gwf.zip"


def download_gwf() -> pd.DataFrame:
    """Extract GWF TSCS data from the downloaded zip file."""
    import tempfile
    import zipfile

    if not GWF_ZIP.exists():
        raise FileNotFoundError(
            f"GWF zip not found at {GWF_ZIP}. "
            "Download from http://sites.psu.edu/dictators/ first."
        )

    print(f"Extracting GWF data from {GWF_ZIP}...")
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(GWF_ZIP) as zf:
            names = zf.namelist()
            print(f"  Zip contents: {names}")
            # Find the TSCS Stata file
            tscs_file = [n for n in names if "tscs" in n.lower() and n.endswith(".dta")]
            if not tscs_file:
                tscs_file = [n for n in names if n.endswith(".dta")]
            if not tscs_file:
                raise FileNotFoundError(f"No .dta file found in zip: {names}")

            target = tscs_file[0]
            print(f"  Extracting: {target}")
            zf.extract(target, tmp)
            df = pd.read_stata(Path(tmp) / target)

    print(f"  GWF data: {len(df)} rows, {df.columns.tolist()[:10]}...")
    return df


def load_theta() -> pd.DataFrame:
    """Load our R-estimated theta scores."""
    df = pd.read_csv(THETA_CSV)
    df["ccode"] = pd.to_numeric(df["ccode"], errors="coerce")
    df["start_year"] = pd.to_numeric(df["start_year"], errors="coerce")
    df["end_year"] = pd.to_numeric(df["end_year"], errors="coerce")
    return df


def match_gwf(leaders: pd.DataFrame, gwf: pd.DataFrame) -> pd.DataFrame:
    """Match GWF regime type to each leader by COW code + year overlap."""
    # Identify COW code column in GWF
    cow_col = None
    for candidate in ["cowcode", "COWcode", "ccode", "cow"]:
        if candidate in gwf.columns:
            cow_col = candidate
            break
    if cow_col is None:
        raise KeyError(f"No COW code column found. Columns: {gwf.columns.tolist()}")

    gwf[cow_col] = pd.to_numeric(gwf[cow_col], errors="coerce")

    # Identify year column
    year_col = "year" if "year" in gwf.columns else None
    if year_col is None:
        raise KeyError(f"No year column found. Columns: {gwf.columns.tolist()}")

    # Identify regime type column
    regime_col = None
    for candidate in ["gwf_regimetype", "regimetype", "gwf_full_regimetype"]:
        if candidate in gwf.columns:
            regime_col = candidate
            break
    if regime_col is None:
        # Try any column with 'regime' in name
        regime_cols = [c for c in gwf.columns if "regime" in c.lower()]
        if regime_cols:
            regime_col = regime_cols[0]
        else:
            raise KeyError(f"No regime type column found. Columns: {gwf.columns.tolist()}")

    print(f"  Using columns: cow={cow_col}, year={year_col}, regime={regime_col}")
    print(f"  Regime types: {gwf[regime_col].value_counts().to_dict()}")

    results = []
    for _, ldr in leaders.iterrows():
        ccode = ldr["ccode"]
        start = ldr["start_year"]
        end = ldr["end_year"]

        if pd.isna(ccode) or pd.isna(start) or pd.isna(end):
            results.append({
                "gwf_regimetype": None,
                "gwf_personal": None,
                "gwf_years_matched": 0,
            })
            continue

        # Match GWF rows for this country during leader's tenure
        mask = (
            (gwf[cow_col] == ccode) &
            (gwf[year_col] >= start) &
            (gwf[year_col] <= end)
        )
        matched = gwf.loc[mask, regime_col].dropna()

        if matched.empty:
            results.append({
                "gwf_regimetype": None,
                "gwf_personal": None,
                "gwf_years_matched": 0,
            })
        else:
            # Modal regime type during tenure
            modal_type = matched.mode().iloc[0] if len(matched.mode()) > 0 else matched.iloc[0]
            # Personal = regime type contains 'personal' (case-insensitive)
            is_personal = 1 if "personal" in str(modal_type).lower() else 0
            results.append({
                "gwf_regimetype": str(modal_type),
                "gwf_personal": is_personal,
                "gwf_years_matched": len(matched),
            })

    return pd.DataFrame(results)


def main():
    # Step 1: Download GWF
    gwf = download_gwf()

    # Step 2: Load our theta scores
    leaders = load_theta()
    print(f"\nOur leaders: {len(leaders)} (with theta scores)")

    # Step 3: Match
    print("\nMatching GWF regime types...")
    gwf_matches = match_gwf(leaders, gwf)

    # Step 4: Combine and write
    output = leaders[["qid", "leader", "ccode", "iso3", "country",
                       "start_year", "end_year", "theta", "theta_se",
                       "theta_inst", "theta_cult"]].copy()
    for col in gwf_matches.columns:
        output[col] = gwf_matches[col].values

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(OUTPUT_CSV, index=False)

    # Summary
    matched = output["gwf_personal"].notna()
    n_matched = matched.sum()
    n_personal = (output["gwf_personal"] == 1).sum()
    n_nonpersonal = (output["gwf_personal"] == 0).sum()
    n_unmatched = len(output) - n_matched

    print(f"\n{'='*60}")
    print("GWF MATCHING SUMMARY")
    print(f"{'='*60}")
    print(f"Leaders with GWF match:  {n_matched} / {len(output)}")
    print(f"  GWF personal:          {n_personal}")
    print(f"  GWF non-personal:      {n_nonpersonal}")
    print(f"  Unmatched:             {n_unmatched}")

    if n_matched > 0:
        # Quick correlation preview
        valid = output[output["gwf_personal"].notna()].copy()
        personal = valid[valid["gwf_personal"] == 1]["theta"]
        nonpersonal = valid[valid["gwf_personal"] == 0]["theta"]
        print(f"\nTheta by GWF classification:")
        print(f"  Personal (n={len(personal)}):     mean = {personal.mean():.3f}, sd = {personal.std():.3f}")
        print(f"  Non-personal (n={len(nonpersonal)}): mean = {nonpersonal.mean():.3f}, sd = {nonpersonal.std():.3f}")
        print(f"  Difference:                 {personal.mean() - nonpersonal.mean():.3f}")

        # Regime type breakdown
        print(f"\nGWF regime type distribution:")
        for rtype, count in output["gwf_regimetype"].value_counts().items():
            pct = count / n_matched * 100
            print(f"  {rtype:30s}: {count:4d} ({pct:.1f}%)")

    print(f"\nWrote {len(output)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
