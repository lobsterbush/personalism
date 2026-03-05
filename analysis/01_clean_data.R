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
  "political_killings", "military_executive", "judicial_purges",
  "const_disregard", "no_leg_constraint", "places_named",
  "grandiose_titles", "monuments", "birthday_holiday",
  "hagiography", "cult_of_personality", "currency_portrait",
  "oath_to_person"
)

# --- Construct binary response matrix -------------------------------------
resp <- dat[, indicators]
resp <- as.data.frame(lapply(resp, function(x) {
  x <- as.integer(x)
  x[!x %in% c(0L, 1L)] <- NA_integer_
  x
}))
rn <- paste(dat$leader, dat$country, dat$start_year, sep = "_")
rn <- make.unique(rn, sep = "_")
rownames(resp) <- rn

# --- Descriptive statistics -----------------------------------------------
cat("\n========================================\n")
cat("DATA SUMMARY\n")
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
