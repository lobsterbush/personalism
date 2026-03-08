# ============================================================================
# 03_figures.R — Publication-quality figures for personalism IRT analysis
# ============================================================================

library(here)
library(mirt)
library(readr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(ggthemes)
library(patchwork)
library(scales)

fig_dir <- here("figures")
dir.create(fig_dir, showWarnings = FALSE)

# --- Load data ------------------------------------------------------------
mod      <- readRDS(here("analysis", "irt_model.rds"))
theta_df <- read_csv(here("data", "compiled", "personalism_theta.csv"),
                     show_col_types = FALSE)
item_df  <- read_csv(here("data", "compiled", "item_parameters.csv"),
                     show_col_types = FALSE)
d        <- readRDS(here("analysis", "cleaned_data.rds"))
resp           <- d$resp
all_indicators <- d$indicators            # all 16 (for coverage plot)
irt_indicators <- item_df$indicator        # items retained in IRT model

# --- Clean indicator labels -----------------------------------------------
label_map <- c(
  term_limits_absent  = "Term Limits Absent",
  president_for_life  = "President for Life",
  family_in_govt      = "Family in Government",
  political_killings  = "Political Killings",
  military_executive  = "Military Executive",
  judicial_purges     = "Judicial Purges",
  const_disregard     = "Constitutional Disregard",
  no_leg_constraint   = "No Legislative Constraint",
  places_named        = "Places Named After",
  grandiose_titles    = "Grandiose Titles",
  monuments           = "Monuments / Statues",
  birthday_holiday    = "Birthday as Holiday",
  hagiography         = "State Hagiography",
  cult_of_personality = "Cult of Personality",
  currency_portrait   = "Currency Portrait",
  oath_to_person      = "Loyalty Oath to Person"
)
item_df$label <- label_map[item_df$indicator]

# Common theme
base_theme <- theme_tufte(base_size = 13) +
  theme(
    plot.title    = element_text(size = 14, face = "bold"),
    axis.title    = element_text(size = 12),
    axis.text     = element_text(size = 11),
    strip.text    = element_text(size = 10)
  )

# =========================================================================
# Figure 1: Bifactor Loadings — General vs Specific
# =========================================================================
load_long <- item_df |>
  select(indicator, label, a_general, a_specific, specific_factor) |>
  pivot_longer(cols = c(a_general, a_specific),
               names_to = "type", values_to = "loading") |>
  mutate(
    type = ifelse(type == "a_general", "General", paste0("Specific (", specific_factor, ")")),
    label = factor(label, levels = rev(item_df$label))
  )

p1 <- ggplot(load_long, aes(x = loading, y = label, shape = type, colour = type)) +
  geom_point(size = 3) +
  geom_vline(xintercept = 0, linetype = "dashed", colour = "grey60") +
  scale_colour_manual(values = c("General" = "#2C3E50",
                                  "Specific (INST)" = "#E74C3C",
                                  "Specific (CULT)" = "#3498DB")) +
  scale_shape_manual(values = c("General" = 16,
                                 "Specific (INST)" = 17,
                                 "Specific (CULT)" = 15)) +
  labs(x = "Factor Loading", y = NULL,
       title = "Bifactor Item Loadings",
       colour = "Factor", shape = "Factor") +
  base_theme +
  theme(legend.position = "bottom")

ggsave(file.path(fig_dir, "fig_item_parameters.pdf"), p1,
       device = cairo_pdf, width = 6.5, height = 5.5, dpi = 300)
cat("Saved: figures/fig_item_parameters.pdf\n")

# =========================================================================
# Figure 2: Theta Distribution
# =========================================================================
p2 <- ggplot(theta_df, aes(x = theta)) +
  geom_histogram(aes(y = after_stat(density)),
                 bins = 40, fill = "#BDC3C7", colour = "white") +
  geom_density(linewidth = 0.8, colour = "#2C3E50") +
  geom_vline(xintercept = mean(theta_df$theta),
             linetype = "dashed", colour = "#E74C3C") +
  labs(x = expression(paste("Personalism (", theta, ")")),
       y = "Density",
       title = "Distribution of Personalism Scores") +
  base_theme

ggsave(file.path(fig_dir, "fig_theta_distribution.pdf"), p2,
       device = cairo_pdf, width = 6.5, height = 4.5, dpi = 300)
cat("Saved: figures/fig_theta_distribution.pdf\n")

# =========================================================================
# Figure 3: Top and Bottom Leaders (coefficient plot)
# =========================================================================
n_show <- 25
top_df <- theta_df |>
  slice_max(theta, n = n_show) |>
  mutate(rank_label = paste0(leader, " (", country, ", ", start_year, ")"))
bot_df <- theta_df |>
  slice_min(theta, n = n_show) |>
  mutate(rank_label = paste0(leader, " (", country, ", ", start_year, ")"))

plot_rank <- function(df, title_text) {
  df$rank_label <- factor(df$rank_label, levels = rev(df$rank_label))
  ggplot(df, aes(x = theta, y = rank_label)) +
    geom_point(size = 2, colour = "#2C3E50") +
    geom_errorbarh(aes(xmin = theta - 1.96 * theta_se,
                       xmax = theta + 1.96 * theta_se),
                   height = 0.3, colour = "#7F8C8D") +
    labs(x = expression(theta), y = NULL, title = title_text) +
    base_theme +
    theme(axis.text.y = element_text(size = 9))
}

p3a <- plot_rank(top_df, paste("Top", n_show, "Most Personalist Leaders"))
p3b <- plot_rank(bot_df, paste("Bottom", n_show, "Least Personalist Leaders"))

ggsave(file.path(fig_dir, "fig_top_leaders.pdf"), p3a,
       device = cairo_pdf, width = 6.5, height = 7, dpi = 300)
ggsave(file.path(fig_dir, "fig_bottom_leaders.pdf"), p3b,
       device = cairo_pdf, width = 6.5, height = 7, dpi = 300)
cat("Saved: figures/fig_top_leaders.pdf\n")
cat("Saved: figures/fig_bottom_leaders.pdf\n")

# =========================================================================
# Figure 4: Test Information Function (along general factor)
# =========================================================================
theta_grid <- seq(-4, 4, length.out = 201)
Theta_mat  <- matrix(0, nrow = 201, ncol = 3)  # G, S1, S2
Theta_mat[, 1] <- theta_grid                     # vary general, hold specific at 0
# Information along general factor direction (0° from G, 90° from S1 and S2)
tinfo      <- testinfo(mod, Theta_mat, degrees = c(0, 90, 90))
tinfo_df   <- data.frame(theta = theta_grid, information = tinfo)

p4 <- ggplot(tinfo_df, aes(x = theta, y = information)) +
  geom_line(linewidth = 0.9, colour = "#2C3E50") +
  geom_ribbon(aes(ymin = 0, ymax = information),
              alpha = 0.15, fill = "#2C3E50") +
  labs(x = expression(theta), y = "Test Information",
       title = "Test Information Function") +
  base_theme

ggsave(file.path(fig_dir, "fig_test_information.pdf"), p4,
       device = cairo_pdf, width = 6.5, height = 4.5, dpi = 300)
cat("Saved: figures/fig_test_information.pdf\n")

# =========================================================================
# Figure 5: ICC Curves along general factor (all items, faceted)
# =========================================================================
icc_data <- expand.grid(
  theta     = theta_grid,
  indicator = irt_indicators,
  stringsAsFactors = FALSE
)
icc_data$prob <- NA_real_

# Compute P(Y=1) along general factor, holding specifics at 0
for (ind in irt_indicators) {
  item_idx <- which(colnames(resp) == ind)
  item_obj <- extract.item(mod, item_idx)
  probs    <- probtrace(item_obj, Theta_mat)
  idx      <- icc_data$indicator == ind
  icc_data$prob[idx] <- probs[, 2]  # P(Y=1)
}

icc_data$label <- label_map[icc_data$indicator]
icc_data$label <- factor(icc_data$label,
                         levels = label_map[label_map %in% icc_data$label])

p5 <- ggplot(icc_data, aes(x = theta, y = prob)) +
  geom_line(linewidth = 0.7, colour = "#2C3E50") +
  facet_wrap(~label, ncol = 4) +
  labs(x = expression(theta), y = "P(Y = 1)",
       title = "Item Characteristic Curves") +
  base_theme +
  theme(strip.text = element_text(size = 9))

ggsave(file.path(fig_dir, "fig_icc_curves.pdf"), p5,
       device = cairo_pdf, width = 10, height = 8, dpi = 300)
cat("Saved: figures/fig_icc_curves.pdf\n")

# =========================================================================
# Figure 6: Coverage Summary
# =========================================================================
cov_df <- data.frame(
  indicator = all_indicators,
  label     = label_map[all_indicators],
  n_coded   = sapply(all_indicators, function(x) sum(!is.na(resp[[x]]))),
  n_pos     = sapply(all_indicators, function(x) sum(resp[[x]] == 1, na.rm = TRUE))
)
cov_df$n_missing <- nrow(resp) - cov_df$n_coded
cov_df$pct_pos   <- round(100 * cov_df$n_pos / cov_df$n_coded, 1)
cov_df$label     <- factor(cov_df$label, levels = rev(label_map))

p6 <- ggplot(cov_df, aes(x = n_coded, y = label)) +
  geom_col(fill = "#BDC3C7", width = 0.6) +
  geom_col(aes(x = n_pos), fill = "#2C3E50", width = 0.6) +
  geom_text(aes(x = n_pos + 15, label = paste0(pct_pos, "%")),
            size = 3.2, hjust = 0) +
  labs(x = "Leader-spells", y = NULL,
       title = "Indicator Coverage and Prevalence",
       subtitle = "Dark = positive cases; light = total coded") +
  base_theme

ggsave(file.path(fig_dir, "fig_coverage.pdf"), p6,
       device = cairo_pdf, width = 6.5, height = 5.5, dpi = 300)
cat("Saved: figures/fig_coverage.pdf\n")

cat("\nAll figures saved to figures/\n")
