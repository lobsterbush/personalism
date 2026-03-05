# ============================================================================
# 02_irt_model.R — Fit 2PL IRT model to personalism indicators (mirt)
# ============================================================================

library(here)
library(mirt)
library(readr)
library(dplyr)

# --- Load cleaned data ----------------------------------------------------
d <- readRDS(here("analysis", "cleaned_data.rds"))
meta       <- d$meta
resp       <- d$resp
indicators <- d$indicators

# ===========================================================================
# STAGE 1: Fit all items to identify negatively-loading indicators
# ===========================================================================
cat(sprintf("STAGE 1 — Fitting 2PL with all %d items...\n", ncol(resp)))

mod_full <- mirt(resp, model = 1, itemtype = "2PL", SE = TRUE,
                 verbose = TRUE, technical = list(NCYCLES = 3000))

full_pars <- coef(mod_full, simplify = TRUE, IRTpars = TRUE)$items
full_df   <- data.frame(indicator = rownames(full_pars),
                        a = full_pars[, "a"], b = full_pars[, "b"],
                        stringsAsFactors = FALSE)

cat("\n========================================\n")
cat("STAGE 1 ITEM PARAMETERS (all items)\n")
cat("========================================\n")
for (i in seq_len(nrow(full_df))) {
  flag <- ifelse(full_df$a[i] < 0.1, "  ** DROP", "")
  cat(sprintf("  %-30s a = %6.3f  b = %6.3f%s\n",
              full_df$indicator[i], full_df$a[i], full_df$b[i], flag))
}

# Drop items with discrimination < 0.1 (negative or near-zero loading)
keep <- full_df$indicator[full_df$a >= 0.1]
drop <- full_df$indicator[full_df$a < 0.1]
cat(sprintf("\n  Keeping %d items, dropping %d: %s\n",
            length(keep), length(drop), paste(drop, collapse = ", ")))

# ===========================================================================
# STAGE 2: Refit with positively-loading items only
# ===========================================================================
resp2      <- resp[, keep, drop = FALSE]
indicators <- keep  # update indicator list for downstream

cat(sprintf("\nSTAGE 2 — Refitting 2PL with %d items...\n", ncol(resp2)))
mod <- mirt(resp2, model = 1, itemtype = "2PL", SE = TRUE,
            verbose = TRUE, technical = list(NCYCLES = 3000))

# --- Extract item parameters (traditional IRT parameterisation) -----------
item_pars <- coef(mod, simplify = TRUE, IRTpars = TRUE)$items
item_df   <- as.data.frame(item_pars)
item_df$indicator <- rownames(item_df)
rownames(item_df) <- NULL
item_df <- item_df[, c("indicator", "a", "b", "g", "u")]

cat("\n========================================\n")
cat("STAGE 2 ITEM PARAMETERS (refined model)\n")
cat("========================================\n")
for (i in seq_len(nrow(item_df))) {
  cat(sprintf("  %-30s a = %6.3f  b = %6.3f\n",
              item_df$indicator[i], item_df$a[i], item_df$b[i]))
}

# --- Person parameters (theta) -------------------------------------------
theta_raw <- fscores(mod, method = "EAP", full.scores.SE = TRUE)
theta_df  <- data.frame(
  meta,
  theta    = theta_raw[, 1],
  theta_se = theta_raw[, 2]
)

# --- Model fit ------------------------------------------------------------
cat("\n========================================\n")
cat("MODEL FIT (refined model)\n")
cat("========================================\n")

fit <- tryCatch({
  M2(mod)
}, error = function(e) {
  cat("  M2 could not be computed: ", conditionMessage(e), "\n")
  NULL
})

if (!is.null(fit)) {
  cat(sprintf("  M2     = %.2f (df = %d, p = %.4f)\n", fit$M2, fit$df, fit$p))
  cat(sprintf("  RMSEA  = %.4f [%.4f, %.4f]\n",
              fit$RMSEA, fit$RMSEA_5, fit$RMSEA_95))
  cat(sprintf("  SRMSR  = %.4f\n", fit$SRMSR))
  cat(sprintf("  TLI    = %.4f\n", fit$TLI))
  cat(sprintf("  CFI    = %.4f\n", fit$CFI))
}

aic_val <- extract.mirt(mod, "AIC")
bic_val <- extract.mirt(mod, "BIC")
loglik  <- extract.mirt(mod, "logLik")
cat(sprintf("  AIC    = %.2f\n", aic_val))
cat(sprintf("  BIC    = %.2f\n", bic_val))
cat(sprintf("  logLik = %.2f\n", loglik))

# --- Item fit -------------------------------------------------------------
cat("\n========================================\n")
cat("ITEM FIT (S-X2)\n")
cat("========================================\n")
ifit <- tryCatch({
  itemfit(mod, fit_stats = "S_X2", na.rm = TRUE)
}, error = function(e) {
  cat("  itemfit error: ", conditionMessage(e), "\n")
  tryCatch(itemfit(mod, na.rm = TRUE), error = function(e2) NULL)
})
if (!is.null(ifit)) print(ifit)

# --- Face validity: known personalists ------------------------------------
cat("\n========================================\n")
cat("FACE VALIDITY — KNOWN PERSONALISTS\n")
cat("========================================\n")

face_patterns <- c(
  "Saddam|Hussein"       = "Saddam Hussein",
  "Kim Il"               = "Kim Il-sung",
  "Kim Jong"             = "Kim Jong-il / Jong-un",
  "Gaddafi|Qadhafi|Qaddafi" = "Gaddafi",
  "^Mao"                 = "Mao Zedong",
  "Stroessner"           = "Stroessner",
  "Mobutu"               = "Mobutu",
  "Niyazov"              = "Niyazov",
  "Mugabe"               = "Mugabe",
  "Assad"                = "Assad",
  "Lukash"               = "Lukashenko",
  "^Putin"               = "Putin",
  "Xi Jinping"           = "Xi Jinping",
  "Duvalier"             = "Duvalier",
  "Trujillo"             = "Trujillo",
  "Ceausescu"            = "Ceausescu"
)

for (i in seq_along(face_patterns)) {
  pat <- names(face_patterns)[i]
  matches <- theta_df[grepl(pat, theta_df$leader, ignore.case = TRUE), ]
  if (nrow(matches) > 0) {
    for (j in seq_len(nrow(matches))) {
      rank_pct <- round(100 * mean(theta_df$theta <= matches$theta[j]), 1)
      cat(sprintf("  %-35s theta = %6.3f (SE = %.3f)  %5.1f%%ile\n",
                  paste0(matches$leader[j], " (", matches$country[j], ", ",
                         matches$start_year[j], ")"),
                  matches$theta[j], matches$theta_se[j], rank_pct))
    }
  }
}

# --- Top and bottom 20 ----------------------------------------------------
theta_df <- theta_df[order(-theta_df$theta), ]

cat("\n========================================\n")
cat("TOP 20 MOST PERSONALIST LEADERS\n")
cat("========================================\n")
for (i in 1:min(20, nrow(theta_df))) {
  r <- theta_df[i, ]
  cat(sprintf("  %2d. %-25s %-20s %d-%d  theta = %6.3f\n",
              i, r$leader, r$country, r$start_year, r$end_year, r$theta))
}

cat("\n========================================\n")
cat("BOTTOM 20 LEAST PERSONALIST LEADERS\n")
cat("========================================\n")
n <- nrow(theta_df)
for (i in seq(n, max(n - 19, 1))) {
  r <- theta_df[i, ]
  rank <- n - i + 1
  cat(sprintf("  %2d. %-25s %-20s %d-%d  theta = %6.3f\n",
              rank, r$leader, r$country, r$start_year, r$end_year, r$theta))
}

# --- Export results -------------------------------------------------------
write_csv(theta_df, here("data", "compiled", "personalism_theta.csv"))
write_csv(item_df, here("data", "compiled", "item_parameters.csv"))
saveRDS(mod, here("analysis", "irt_model.rds"))

# --- Export LaTeX macros --------------------------------------------------
clean_name <- function(x) {
  words <- strsplit(x, "_")[[1]]
  paste0(toupper(substring(words, 1, 1)), substring(words, 2), collapse = "")
}

tex_lines <- c("% Auto-generated by 02_irt_model.R — do not edit by hand", "")

# Counts
tex_lines <- c(tex_lines,
  sprintf("\\newcommand{\\nLeaders}{%d}", nrow(theta_df)),
  sprintf("\\newcommand{\\nCountries}{%d}", n_distinct(theta_df$country)),
  sprintf("\\newcommand{\\nIndicators}{%d}", length(indicators)),
  "")

# Model fit
if (!is.null(fit)) {
  tex_lines <- c(tex_lines,
    sprintf("\\newcommand{\\fitMtwo}{%.2f}", fit$M2),
    sprintf("\\newcommand{\\fitDf}{%d}", fit$df),
    sprintf("\\newcommand{\\fitPval}{%.4f}", fit$p),
    sprintf("\\newcommand{\\fitRMSEA}{%.4f}", fit$RMSEA),
    sprintf("\\newcommand{\\fitRMSEAlow}{%.4f}", fit$RMSEA_5),
    sprintf("\\newcommand{\\fitRMSEAhigh}{%.4f}", fit$RMSEA_95),
    sprintf("\\newcommand{\\fitSRMSR}{%.4f}", fit$SRMSR),
    sprintf("\\newcommand{\\fitTLI}{%.4f}", fit$TLI),
    sprintf("\\newcommand{\\fitCFI}{%.4f}", fit$CFI))
}
tex_lines <- c(tex_lines,
  sprintf("\\newcommand{\\fitAIC}{%.2f}", aic_val),
  sprintf("\\newcommand{\\fitBIC}{%.2f}", bic_val),
  sprintf("\\newcommand{\\fitLogLik}{%.2f}", loglik),
  "")

# Theta summary
tex_lines <- c(tex_lines,
  sprintf("\\newcommand{\\thetaMean}{%.3f}", mean(theta_df$theta)),
  sprintf("\\newcommand{\\thetaSD}{%.3f}", sd(theta_df$theta)),
  sprintf("\\newcommand{\\thetaMin}{%.3f}", min(theta_df$theta)),
  sprintf("\\newcommand{\\thetaMax}{%.3f}", max(theta_df$theta)),
  "")

# Item parameters
for (i in seq_len(nrow(item_df))) {
  nm <- clean_name(item_df$indicator[i])
  tex_lines <- c(tex_lines,
    sprintf("\\newcommand{\\discrim%s}{%.3f}", nm, item_df$a[i]),
    sprintf("\\newcommand{\\diff%s}{%.3f}", nm, item_df$b[i]))
}
tex_lines <- c(tex_lines, "")

# Top personalist
tex_lines <- c(tex_lines,
  sprintf("\\newcommand{\\topLeader}{%s}", theta_df$leader[1]),
  sprintf("\\newcommand{\\topCountry}{%s}", theta_df$country[1]),
  sprintf("\\newcommand{\\topTheta}{%.3f}", theta_df$theta[1]))

writeLines(tex_lines, here("paper", "statistics.tex"))

cat("\nExported: paper/statistics.tex\n")
cat("Saved:    data/compiled/personalism_theta.csv\n")
cat("Saved:    data/compiled/item_parameters.csv\n")
cat("Saved:    analysis/irt_model.rds\n")
