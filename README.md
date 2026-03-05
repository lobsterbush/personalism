# Personalism in Dictatorships: Measurement and Data

## Authors
- Lee Morgenbesser
- Charles Crabtree, Senior Lecturer, School of Social Sciences, Monash University and K-Club Professor, University College, Korea University

## Overview

This project builds a new cross-national dataset of personalism in dictatorships. We collect 16 observable "manifest" indicators of a latent personalism trait from publicly available data sources, covering 1,057 authoritarian leader-spells across 160 countries (1946--present). We estimate continuous personalism scores using a two-parameter Item Response Theory (IRT) model. Leader identification follows Archigos 4.1.

### Indicators (16 total)

**Power concentration** (8 indicators): Term limits absent, President for life, Family in government, Political killings, Military executive, Judicial purges, Constitutional disregard, No legislative constraint.

**Personality cult** (8 indicators): Places named after leader, Grandiose titles, Monuments/statues, Birthday as holiday, State hagiography, Cult of personality, Currency portrait, Loyalty oath to person.

### Data Sources

- **Archigos 4.1** — Leader identification and tenure dates
- **V-Dem v15** — Political killings, military executive, judicial purges, constitutional disregard, legislative constraints
- **Constitute Project** — Term limits, president-for-life provisions, loyalty oaths
- **Wikidata SPARQL** — Family in government, places named, grandiose titles, monuments, birthday holidays, state hagiography
- **Wikipedia** — Cult of personality categories
- **National currency records** — Leader portraits on banknotes

### Interactive Dashboard

An interactive web dashboard in `dashboard/` includes world map, country profiles, indicator explorer, and data table. Open `dashboard/index.html` in any browser.

## Requirements

### Python (data collection)
- Python 3.10+
- See `scripts/requirements.txt`

### R (analysis)
- R 4.0+
- Packages: `mirt`, `ggplot2`, `ggthemes`, `here`, `readr`, `dplyr`, `tidyr`, `patchwork`, `scales`

## Replication Instructions

1. Install Python dependencies:
   ```bash
   pip install -r scripts/requirements.txt
   ```

2. Run data collection pipeline (14 scripts):
   ```bash
   python scripts/01_scrape_banknotes.py       # Numista banknote catalog
   python scripts/02_query_constitute.py       # Constitute Project API
   python scripts/03_query_wikidata.py         # Wikidata SPARQL (3 indicators)
   python scripts/04_compile_dataset.py        # Initial compilation
   python scripts/05_archigos_wikidata_pipeline.py  # Archigos-Wikidata QID matching
   python scripts/06_fix_family_indicator.py   # Fix family indicator logic
   python scripts/07_recover_coverage.py       # Recover leaders via QID matching
   python scripts/08_vdem_indicators.py        # V-Dem indicators (5 variables)
   python scripts/09_wikidata_extra.py         # Wikidata extra (monuments, holiday, hagiography)
   python scripts/10_wikipedia_categories.py   # Wikipedia categories (cult, president-for-life)
   python scripts/11_constitute_indicators.py  # Constitute (term limits, oath, PFL)
   python scripts/12_banknote_portraits.py     # Wikipedia currency portrait matching
   python scripts/13_compile_all_indicators.py # Final compilation (16 indicators)
   python scripts/14_backfill_wikidata.py      # Backfill Wikidata for recovered leaders
   ```

3. Run R analysis scripts:
   ```bash
   Rscript analysis/01_clean_data.R   # Clean and prepare binary matrix
   Rscript analysis/02_irt_model.R    # Fit 2PL IRT, export thetas + stats
   Rscript analysis/03_figures.R      # Publication figures (PDF)
   ```

4. Compile paper:
   ```bash
   cd paper && pdflatex main && bibtex main && pdflatex main && pdflatex main
   ```

## Data Availability

We will make all data and code used to generate our results available at a figshare repository at the time of publication.
