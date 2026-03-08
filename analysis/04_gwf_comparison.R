# ============================================================================
# 04_gwf_comparison.R — Validate theta against GWF regime classifications
# ============================================================================

library(here)
library(readr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(ggthemes)

fig_dir <- here("figures")
dir.create(fig_dir, showWarnings = FALSE)

# --- Load data --------------------------------------------------------------
gwf <- read_csv(here("data", "raw", "gwf_comparison.csv"), show_col_types = FALSE)
cat(sprintf("Loaded %d leaders, %d with GWF match\n",
            nrow(gwf), sum(!is.na(gwf$gwf_personal))))

# Restrict to matched leaders
matched <- gwf |> filter(!is.na(gwf_personal))

# --- Recode regime types into categories ------------------------------------
matched <- matched |>
  mutate(
    gwf_cat = case_when(
      gwf_regimetype == "personal" ~ "Pure Personal",
      grepl("personal", gwf_regimetype) ~ "Hybrid Personal",
      TRUE ~ "Non-Personal"
    ),
    gwf_cat = factor(gwf_cat,
                     levels = c("Non-Personal", "Hybrid Personal", "Pure Personal")),
    gwf_personal_f = factor(gwf_personal,
                            levels = c(0, 1),
                            labels = c("Non-Personal", "Personal"))
  )

# --- Summary statistics -----------------------------------------------------
cat("\n--- Theta by GWF binary classification ---\n")
summ_binary <- matched |>
  group_by(gwf_personal_f) |>
  summarise(
    n     = n(),
    mean  = mean(theta),
    sd    = sd(theta),
    med   = median(theta),
    .groups = "drop"
  )
print(summ_binary)

cat("\n--- Theta by GWF three-way classification ---\n")
summ_three <- matched |>
  group_by(gwf_cat) |>
  summarise(
    n     = n(),
    mean  = mean(theta),
    sd    = sd(theta),
    med   = median(theta),
    .groups = "drop"
  )
print(summ_three)

# --- Statistical tests ------------------------------------------------------

# Point-biserial correlation (= Pearson on binary)
r_pb <- cor.test(matched$theta, matched$gwf_personal)
cat(sprintf("\nPoint-biserial r = %.3f, p = %.4f, 95%% CI [%.3f, %.3f]\n",
            r_pb$estimate, r_pb$p.value,
            r_pb$conf.int[1], r_pb$conf.int[2]))

# Welch's t-test
tt <- t.test(theta ~ gwf_personal_f, data = matched)
cat(sprintf("Welch t = %.3f, df = %.1f, p = %.4f\n",
            tt$statistic, tt$parameter, tt$p.value))

# Cohen's d
personal    <- matched$theta[matched$gwf_personal == 1]
nonpersonal <- matched$theta[matched$gwf_personal == 0]
pooled_sd   <- sqrt(((length(personal) - 1) * sd(personal)^2 +
                      (length(nonpersonal) - 1) * sd(nonpersonal)^2) /
                     (length(personal) + length(nonpersonal) - 2))
cohens_d    <- (mean(personal) - mean(nonpersonal)) / pooled_sd
cat(sprintf("Cohen's d = %.3f\n", cohens_d))

# One-way ANOVA on three-way classification
aov_three <- aov(theta ~ gwf_cat, data = matched)
f_stat    <- summary(aov_three)[[1]][["F value"]][1]
f_pval    <- summary(aov_three)[[1]][["Pr(>F)"]][1]
cat(sprintf("ANOVA (3-way): F = %.3f, p = %.4f\n", f_stat, f_pval))

# --- Common theme -----------------------------------------------------------
base_theme <- theme_tufte(base_size = 13) +
  theme(
    plot.title    = element_text(size = 14, face = "bold"),
    axis.title    = element_text(size = 12),
    axis.text     = element_text(size = 11),
    strip.text    = element_text(size = 10)
  )

# =========================================================================
# Figure: Violin + Box plot — Binary classification
# =========================================================================
p_binary <- ggplot(matched, aes(x = gwf_personal_f, y = theta)) +
  geom_violin(fill = "#BDC3C7", alpha = 0.5, colour = NA) +
  geom_boxplot(width = 0.15, outlier.shape = NA, fill = "white") +
  geom_jitter(width = 0.08, alpha = 0.2, size = 0.8, colour = "#2C3E50") +
  labs(
    x = "GWF Regime Classification",
    y = expression(paste("Personalism (", theta, ")")),
    title = "Personalism Scores by GWF Classification"
  ) +
  annotate("text", x = 1.5, y = max(matched$theta) + 0.15,
           label = sprintf("r = %.2f, p = %.3f", r_pb$estimate, r_pb$p.value),
           size = 4, hjust = 0.5) +
  base_theme

ggsave(file.path(fig_dir, "fig_gwf_binary.pdf"), p_binary,
       device = cairo_pdf, width = 6.5, height = 5, dpi = 300)
cat("Saved: figures/fig_gwf_binary.pdf\n")

# =========================================================================
# Figure: Violin + Box plot — Three-way classification
# =========================================================================
p_three <- ggplot(matched, aes(x = gwf_cat, y = theta)) +
  geom_violin(fill = "#BDC3C7", alpha = 0.5, colour = NA) +
  geom_boxplot(width = 0.15, outlier.shape = NA, fill = "white") +
  geom_jitter(width = 0.08, alpha = 0.2, size = 0.8, colour = "#2C3E50") +
  labs(
    x = "GWF Regime Classification",
    y = expression(paste("Personalism (", theta, ")")),
    title = "Personalism Scores by GWF Regime Type"
  ) +
  annotate("text", x = 2, y = max(matched$theta) + 0.15,
           label = sprintf("F = %.2f, p = %.3f", f_stat, f_pval),
           size = 4, hjust = 0.5) +
  base_theme

ggsave(file.path(fig_dir, "fig_gwf_threeway.pdf"), p_three,
       device = cairo_pdf, width = 6.5, height = 5, dpi = 300)
cat("Saved: figures/fig_gwf_threeway.pdf\n")

# =========================================================================
# Figure: Theta distribution by regime type (faceted density)
# =========================================================================
regime_counts <- matched |>
  count(gwf_regimetype, sort = TRUE) |>
  filter(n >= 5, gwf_regimetype != "")

regime_df <- matched |>
  filter(gwf_regimetype %in% regime_counts$gwf_regimetype) |>
  mutate(gwf_regimetype = factor(gwf_regimetype,
                                  levels = regime_counts$gwf_regimetype))

p_regime <- ggplot(regime_df, aes(x = theta)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 20, fill = "#BDC3C7", colour = "white") +
  geom_density(linewidth = 0.7, colour = "#2C3E50") +
  facet_wrap(~gwf_regimetype, ncol = 3, scales = "free_y") +
  labs(
    x = expression(paste("Personalism (", theta, ")")),
    y = "Density",
    title = "Theta Distributions by GWF Regime Type"
  ) +
  base_theme +
  theme(strip.text = element_text(size = 9))

ggsave(file.path(fig_dir, "fig_gwf_regimetype.pdf"), p_regime,
       device = cairo_pdf, width = 10, height = 7, dpi = 300)
cat("Saved: figures/fig_gwf_regimetype.pdf\n")

# =========================================================================
# Write LaTeX macros
# =========================================================================
tex_path <- here("paper", "gwf_statistics.tex")
sink(tex_path)
cat("% Auto-generated by 04_gwf_comparison.R — do not edit by hand\n\n")

cat(sprintf("\\newcommand{\\gwfNmatched}{%d}\n", nrow(matched)))
cat(sprintf("\\newcommand{\\gwfNpersonal}{%d}\n", sum(matched$gwf_personal == 1)))
cat(sprintf("\\newcommand{\\gwfNnonpersonal}{%d}\n", sum(matched$gwf_personal == 0)))
cat(sprintf("\\newcommand{\\gwfNunmatched}{%d}\n",
            nrow(gwf) - nrow(matched)))

# Binary means
cat(sprintf("\\newcommand{\\gwfMeanPersonal}{%.3f}\n",
            mean(personal)))
cat(sprintf("\\newcommand{\\gwfSDPersonal}{%.3f}\n",
            sd(personal)))
cat(sprintf("\\newcommand{\\gwfMeanNonpersonal}{%.3f}\n",
            mean(nonpersonal)))
cat(sprintf("\\newcommand{\\gwfSDNonpersonal}{%.3f}\n",
            sd(nonpersonal)))
cat(sprintf("\\newcommand{\\gwfDiffMeans}{%.3f}\n",
            mean(personal) - mean(nonpersonal)))

# Point-biserial
cat(sprintf("\\newcommand{\\gwfPBr}{%.3f}\n", r_pb$estimate))
cat(sprintf("\\newcommand{\\gwfPBp}{%.4f}\n", r_pb$p.value))
cat(sprintf("\\newcommand{\\gwfPBciLow}{%.3f}\n", r_pb$conf.int[1]))
cat(sprintf("\\newcommand{\\gwfPBciHigh}{%.3f}\n", r_pb$conf.int[2]))

# t-test
cat(sprintf("\\newcommand{\\gwfTstat}{%.3f}\n", tt$statistic))
cat(sprintf("\\newcommand{\\gwfTdf}{%.1f}\n", tt$parameter))
cat(sprintf("\\newcommand{\\gwfTp}{%.4f}\n", tt$p.value))

# Cohen's d
cat(sprintf("\\newcommand{\\gwfCohensD}{%.3f}\n", cohens_d))

# Three-way means
for (i in seq_len(nrow(summ_three))) {
  cat_label <- gsub("[- ]", "", summ_three$gwf_cat[i])
  cat(sprintf("\\newcommand{\\gwfMean%s}{%.3f}\n",
              cat_label, summ_three$mean[i]))
  cat(sprintf("\\newcommand{\\gwfN%s}{%d}\n",
              cat_label, summ_three$n[i]))
}

# ANOVA
cat(sprintf("\\newcommand{\\gwfFstat}{%.3f}\n", f_stat))
cat(sprintf("\\newcommand{\\gwfFp}{%.4f}\n", f_pval))

sink()
cat(sprintf("Wrote LaTeX macros to %s\n", tex_path))
cat("\nDone.\n")
