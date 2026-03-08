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
# STAGE 1: Unidimensional 2PL (diagnostic baseline)
# ===========================================================================
cat(sprintf("STAGE 1 — Unidimensional 2PL baseline (%d items)...\n", ncol(resp)))

mod_uni <- mirt(resp, model = 1, itemtype = "2PL", SE = TRUE,
                verbose = TRUE, technical = list(NCYCLES = 3000))

uni_pars <- coef(mod_uni, simplify = TRUE, IRTpars = TRUE)$items
uni_df   <- data.frame(indicator = rownames(uni_pars),
                       a = uni_pars[, "a"], b = uni_pars[, "b"],
                       stringsAsFactors = FALSE)

cat("\n========================================\n")
cat("UNIDIMENSIONAL BASELINE\n")
cat("========================================\n")
for (i in seq_len(nrow(uni_df))) {
  cat(sprintf("  %-30s a = %6.3f  b = %6.3f\n",
              uni_df$indicator[i], uni_df$a[i], uni_df$b[i]))
}

uni_fit <- tryCatch(M2(mod_uni), error = function(e) NULL)
if (!is.null(uni_fit)) {
  cat(sprintf("\n  Unidimensional fit: CFI = %.4f, RMSEA = %.4f, TLI = %.4f\n",
              uni_fit$CFI, uni_fit$RMSEA, uni_fit$TLI))
}

# ===========================================================================
# STAGE 2: Bifactor model — General + 2 specific factors
# ===========================================================================
# Specific factor assignments:
#   1 = Institutional (term_limits_absent, president_for_life, family_in_govt, oath_to_person)
#   2 = Cult/symbolic (places_named, grandiose_titles, monuments, birthday_holiday,
#                      hagiography, cult_of_personality, currency_portrait)
spec_labels <- c("INST", "CULT")
specific <- c(
  1,  # term_limits_absent
  1,  # president_for_life
  1,  # family_in_govt
  2,  # places_named
  2,  # grandiose_titles
  2,  # monuments
  2,  # birthday_holiday
  2,  # hagiography
  2,  # cult_of_personality
  2,  # currency_portrait
  1   # oath_to_person
)

cat(sprintf("\nSTAGE 2 — Bifactor model: General + %d specific factors (%d items)...\n",
            length(spec_labels), ncol(resp)))

mod <- bfactor(resp, specific, itemtype = "2PL", SE = TRUE,
               verbose = TRUE, technical = list(NCYCLES = 3000))
indicators <- colnames(resp)

# --- Extract item parameters (slope-intercept form) -----------------------
pars_raw <- coef(mod, simplify = TRUE)$items
# Columns: a1 (General), a2 (Specific-1), a3 (Specific-2), d

item_df <- data.frame(
  indicator = rownames(pars_raw),
  a_general = pars_raw[, 1],          # General factor loading
  a_specific = ifelse(specific == 1, pars_raw[, 2], pars_raw[, 3]),
  d = pars_raw[, ncol(pars_raw)],     # Intercept
  specific_factor = spec_labels[specific],
  stringsAsFactors = FALSE
)
rownames(item_df) <- NULL

# Explained common variance (ECV) — proportion of variance from general factor
total_var <- sum(item_df$a_general^2) + sum(item_df$a_specific^2)
ecv <- sum(item_df$a_general^2) / total_var

cat("\n========================================\n")
cat("BIFACTOR ITEM PARAMETERS\n")
cat("========================================\n")
cat(sprintf("  %-30s %8s %8s %8s  %s\n",
            "Item", "a_G", "a_S", "d", "Factor"))
cat(paste(rep("-", 75), collapse = ""), "\n")
for (i in seq_len(nrow(item_df))) {
  cat(sprintf("  %-30s %8.3f %8.3f %8.3f  %s\n",
              item_df$indicator[i], item_df$a_general[i],
              item_df$a_specific[i], item_df$d[i],
              item_df$specific_factor[i]))
}
cat(sprintf("\n  Explained Common Variance (ECV): %.1f%%\n", 100 * ecv))

# --- Person parameters (general factor theta) -----------------------------
theta_all <- fscores(mod, method = "EAP", full.scores.SE = TRUE)
# Columns: G, S1, S2, SE_G, SE_S1, SE_S2
theta_df <- data.frame(
  meta,
  theta        = theta_all[, 1],   # General factor
  theta_se     = theta_all[, 4],   # SE of general factor
  theta_inst   = theta_all[, 2],   # Institutional specific
  theta_cult   = theta_all[, 3]    # Cult specific
)

# --- Model fit ------------------------------------------------------------
cat("\n========================================\n")
cat("MODEL FIT (bifactor)\n")
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

# Compare with unidimensional
if (!is.null(uni_fit) && !is.null(fit)) {
  cat(sprintf("\n  vs. Unidimensional: dCFI = %+.4f, dRMSEA = %+.4f\n",
              fit$CFI - uni_fit$CFI, fit$RMSEA - uni_fit$RMSEA))
}

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

tex_lines <- c("% Auto-generated by 02_irt_model.R (bifactor) — do not edit by hand", "")

# Counts
tex_lines <- c(tex_lines,
  sprintf("\\newcommand{\\nLeaders}{%d}", nrow(theta_df)),
  sprintf("\\newcommand{\\nCountries}{%d}", n_distinct(theta_df$country)),
  sprintf("\\newcommand{\\nIndicators}{%d}", length(indicators)),
  sprintf("\\newcommand{\\ecv}{%.1f}", 100 * ecv),
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

# Item parameters (general + specific loadings)
for (i in seq_len(nrow(item_df))) {
  nm <- clean_name(item_df$indicator[i])
  tex_lines <- c(tex_lines,
    sprintf("\\newcommand{\\discrimG%s}{%.3f}", nm, item_df$a_general[i]),
    sprintf("\\newcommand{\\discrimS%s}{%.3f}", nm, item_df$a_specific[i]))
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
