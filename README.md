# Personalism in Dictatorships: Measurement and Data

## Authors
- Lee Morgenbesser
- Charles Crabtree, Senior Lecturer, School of Social Sciences, Monash University and K-Club Professor, University College, Korea University

## Overview

This project builds a new cross-national, time-series dataset of personalism in dictatorships. Existing authoritarian regime datasets suffer from weak measurement validity when classifying personalist regimes (see Morgenbesser's critique in the `paper/` directory). We address this by collecting observable "manifest" indicators of a latent personalism trait from publicly available and scrapable data sources, then estimating a continuous personalism score using Bayesian Item Response Theory (IRT).

### Data Sources

| Source | Indicators | Script |
|--------|-----------|--------|
| Numista banknote catalog | Living ruler's portrait on currency | `scripts/01_scrape_banknotes.py` |
| Constitute Project API | Term limits, president-for-life, loyalty oaths | `scripts/02_query_constitute.py` |
| Wikidata SPARQL | Official titles, family in government, named places | `scripts/03_query_wikidata.py` |
| Country reports (manual) | Mandatory portraits, personality cult features | Manual coding |

### Interactive Dashboard

An interactive web dashboard for exploring the data is in `dashboard/`. It includes:
- World map with personalism scores by country-year
- Country profiles with indicator timelines
- Indicator explorer with coverage statistics
- Filterable data table

## Requirements

### Python (data collection)
- Python 3.10+
- See `scripts/requirements.txt`

### R (analysis)
- R 4.0+
- Packages: `tidyverse`, `ggplot2`, `ggthemes`, `brms` or `MCMCpack` (for IRT)

### Dashboard
- Any modern web browser (no build step required)

## Replication Instructions

1. Install Python dependencies:
   ```bash
   pip install -r scripts/requirements.txt
   ```

2. Run scrapers in order:
   ```bash
   python scripts/01_scrape_banknotes.py
   python scripts/02_query_constitute.py
   python scripts/03_query_wikidata.py
   python scripts/04_compile_dataset.py
   ```

3. Run R analysis scripts in order:
   ```bash
   Rscript analysis/01_clean_data.R
   Rscript analysis/02_irt_model.R
   Rscript analysis/03_figures.R
   ```

4. Open `dashboard/index.html` in a browser to explore the data.

## Data Availability

We will make all data and code used to generate our results available at a figshare repository at the time of publication.
