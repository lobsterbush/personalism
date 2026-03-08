# ============================================================================
# 01_clean_data.R — Clean and prepare personalism indicators for IRT analysis
# ============================================================================

library(here)
library(readr)
library(dplyr)

# --- Load compiled dataset ------------------------------------------------
dat <- read_csv(here("data", "compiled", "personalism_full.csv"),
                show_col_types = FALSE)

# --- Define indicator columns ---------------------------------------------
indicators <- c(
  "term_limits_absent", "president_for_life", "family_in_govt",
  "places_named",
  "grandiose_titles", "monuments", "birthday_holiday",
  "hagiography", "cult_of_personality", "currency_portrait",
  "oath_to_person"
)

# --- Filter to autocracies ------------------------------------------------
# V-Dem Regimes of the World: 0 = closed autocracy, 1 = electoral autocracy,
# 2 = electoral democracy, 3 = liberal democracy
n_before <- nrow(dat)
dat <- dat |> filter(!is.na(regime_type) & regime_type <= 1)
cat(sprintf("  Filtered to autocracies: %d -> %d spells\n", n_before, nrow(dat)))

# --- Collapse multi-spell leaders to one row per leader -------------------
# Archigos records non-consecutive terms as separate spells. For IRT the unit
# should be the *leader*, not the spell, because Wikidata indicators are
# time-invariant and V-Dem indicators vary only slightly across terms.
# Strategy: per leader (QID), take max of each indicator across spells.

cat(sprintf("  Raw spells:   %d\n", nrow(dat)))
cat(sprintf("  Unique QIDs:  %d\n", n_distinct(dat$qid)))

# Keep metadata for the longest spell per leader
dat <- dat |>
  mutate(spell_length = end_year - start_year) |>
  arrange(qid, desc(spell_length))

meta_cols <- c("qid", "leader", "ccode", "iso3", "country",
               "start_year", "end_year")
meta_first <- dat |>
  group_by(qid) |>
  slice(1) |>
  ungroup() |>
  select(all_of(meta_cols))

# Collapse indicators: max per leader (1 if EVER present, 0 if always 0, NA only if always NA)
collapsed <- dat |>
  group_by(qid) |>
  summarise(across(all_of(indicators), ~ {
    vals <- na.omit(.x)
    if (length(vals) == 0) NA_real_ else max(vals)
  }), .groups = "drop")

dat <- left_join(meta_first, collapsed, by = "qid")

# --- Construct binary response matrix -------------------------------------
resp <- dat[, indicators]
resp <- as.data.frame(lapply(resp, function(x) {
  x <- as.integer(x)
  x[!x %in% c(0L, 1L)] <- NA_integer_
  x
}))
rownames(resp) <- paste(dat$leader, dat$country, sep = "_")
rownames(resp) <- make.unique(rownames(resp), sep = "_")

# --- Descriptive statistics -----------------------------------------------
cat("\n========================================\n")
cat("DATA SUMMARY (after collapsing spells)\n")
cat("========================================\n")
cat(sprintf("  Leaders:    %d\n", nrow(resp)))
cat(sprintf("  Indicators: %d\n", ncol(resp)))
cat(sprintf("  Countries:  %d\n", n_distinct(dat$country)))

cat("\nIndicator prevalence:\n")
for (ind in indicators) {
  n_pos <- sum(resp[[ind]] == 1, na.rm = TRUE)
  n_obs <- sum(!is.na(resp[[ind]]))
  pct   <- round(100 * n_pos / n_obs, 1)
  cat(sprintf("  %-30s %4d / %4d (%5.1f%%)\n", ind, n_pos, n_obs, pct))
}

n_complete <- sum(complete.cases(resp))
cat(sprintf("\n  Complete cases: %d / %d (%.1f%%)\n",
            n_complete, nrow(resp), 100 * n_complete / nrow(resp)))

# Flag low-prevalence items
low_prev <- sapply(indicators, function(ind) {
  sum(resp[[ind]] == 1, na.rm = TRUE)
})
if (any(low_prev < 10)) {
  cat("\n  WARNING — low prevalence (<10 positives):\n")
  for (nm in names(low_prev[low_prev < 10])) {
    cat(sprintf("    %s: %d positives\n", nm, low_prev[nm]))
  }
  cat("  These items may have poorly estimated parameters.\n")
}

# --- Save cleaned objects -------------------------------------------------
saveRDS(list(
  meta       = dat[, c("qid", "leader", "ccode", "iso3", "country",
                        "start_year", "end_year")],
  resp       = resp,
  indicators = indicators
), file = here("analysis", "cleaned_data.rds"))

cat("\nSaved: analysis/cleaned_data.rds\n")
