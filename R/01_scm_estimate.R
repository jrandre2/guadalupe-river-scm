## =============================================================================
## 01_scm_estimate.R
## Guadalupe River SCM Recovery — Phase 5, Step 3
##
## Synthetic Control Method estimation for the long-term economic impact of
## the October 1998 Guadalupe River flood (DR-1257-TX) on Comal County, TX.
##
## Inputs:
##   data/processed/panels/scm_panel.csv
##   data/processed/panels/scm_fips_list.csv   (screened donor pool + Comal)
##
## Outputs (saved to data/results/):
##   scm_weights.csv          — Donor county weights (W*)
##   scm_synthetic_series.csv — Comal actual vs. synthetic, 1990–2022
##   scm_gap.csv              — Treatment effect (actual − synthetic) by year
##   scm_balance.csv          — Pre-treatment predictor fit
##   scm_pretrend_rmspe.txt   — Pre-treatment RMSPE (fit quality)
##   figures/                 — Trends + gap plots
##
## R packages required:
##   tidysynth, tidyverse, readr, ggplot2
##
## Install: install.packages(c("tidysynth", "tidyverse"))
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(tidysynth)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(tidyr)
  library(stringr)
  library(scales)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()   # or set manually:
# PROJECT_ROOT <- "/Volumes/T9/Projects/ Guadalupe River Synthetic Controls Recovery"

PANEL_PATH       <- file.path(PROJECT_ROOT, "data/processed/panels/scm_panel.csv")
FIPS_LIST_PATH   <- file.path(PROJECT_ROOT, "data/processed/panels/scm_fips_list.csv")
RESULTS_DIR      <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR          <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ───────────────────────────────────────────────────────────────
TREATED_FIPS    <- "48091"          # Comal County, TX
TREATMENT_YEAR  <- 1999             # First post-treatment year
PRE_START       <- 1990             # Pre-treatment window start
PRE_END         <- 1998             # Pre-treatment window end
POST_END        <- 2022             # Post-treatment window end (most sources end here)
OUTCOME         <- "per_capita_income_real"   # Primary outcome

# ── 1. Load & Filter Data ────────────────────────────────────────────────────
message("Loading panel...")
panel <- read_csv(PANEL_PATH, show_col_types = FALSE, col_types = cols(fips = col_character()))
fips_list <- read_csv(FIPS_LIST_PATH, show_col_types = FALSE, col_types = cols(fips = col_character()))

# Keep only screened donor pool + treated unit, within study window
df <- panel |>
  filter(
    fips %in% fips_list$fips,
    year >= PRE_START,
    year <= POST_END
  ) |>
  # tidysynth needs a numeric unit ID
  mutate(unit_id = as.integer(factor(fips))) |>
  arrange(fips, year)

comal_id <- df |> filter(fips == TREATED_FIPS) |> pull(unit_id) |> unique()

message(sprintf(
  "Panel ready: %d counties, %d years (%d–%d)",
  n_distinct(df$fips), n_distinct(df$year), min(df$year), max(df$year)
))
message(sprintf("Comal County unit_id: %d", comal_id))

# ── 2. Verify Comal Data Completeness ─────────────────────────────────────────
comal_pre <- df |>
  filter(fips == TREATED_FIPS, year >= PRE_START, year <= PRE_END)

# NOTE: poverty_rate and college_share excluded — ACS back-fill yields invalid
# pre-treatment values (0 / ~1.0) for all counties before 2012. No variation.
key_vars <- c(
  "per_capita_income_real", "laus_unemployment_rate", "emp_pop_ratio",
  "qcew_avg_annual_pay_real"
)
miss <- sapply(key_vars, function(v) sum(is.na(comal_pre[[v]])))
if (any(miss > 0)) {
  warning("Missing values in Comal pre-treatment predictors:")
  print(miss[miss > 0])
} else {
  message("Comal pre-treatment data: complete for all predictors.")
}

# ── 3. Fit Synthetic Control ─────────────────────────────────────────────────
message("Fitting synthetic control...")

# tidysynth::synthetic_control() workflow:
#  1. synthetic_control() — define the panel
#  2. generate_predictor()  — define predictors (pre-treatment averages)
#  3. generate_weights()    — solve for V and W simultaneously

scm <- df |>
  # Step 1: define panel
  synthetic_control(
    outcome    = !!sym(OUTCOME),
    unit       = fips,
    time       = year,
    i_unit     = TREATED_FIPS,
    i_time     = TREATMENT_YEAR,
    generate_placebos = FALSE   # we run placebos separately in 02_placebo_tests.R
  ) |>

  # Step 2: predictors
  # Two sub-period averages of primary outcome
  generate_predictor(
    time_window = PRE_START:1993,
    pci_early   = mean(per_capita_income_real, na.rm = TRUE)
  ) |>
  generate_predictor(
    time_window = 1994:PRE_END,
    pci_late    = mean(per_capita_income_real, na.rm = TRUE)
  ) |>
  # Unemployment rate (pre-period average)
  generate_predictor(
    time_window = PRE_START:PRE_END,
    unemp_avg   = mean(laus_unemployment_rate, na.rm = TRUE)
  ) |>
  # Employment-to-population ratio
  generate_predictor(
    time_window = PRE_START:PRE_END,
    emp_pop_avg = mean(emp_pop_ratio, na.rm = TRUE)
  ) |>
  # Average annual pay (wage level proxy)
  generate_predictor(
    time_window = PRE_START:PRE_END,
    wage_avg    = mean(qcew_avg_annual_pay_real, na.rm = TRUE)
  ) |>
  # Population level (early vs. late to capture growth trajectory)
  generate_predictor(
    time_window = PRE_START:1993,
    pop_early   = mean(population, na.rm = TRUE)
  ) |>
  generate_predictor(
    time_window = 1994:PRE_END,
    pop_late    = mean(population, na.rm = TRUE)
  ) |>
  # NOTE: ACS covariates (poverty_rate, college_share) excluded — back-fill from
  # 2000/2009 ACS produces degenerate values (0 / ~1.0) for pre-2012 years.

  # Step 3: optimize weights
  generate_weights(
    optimization_window = PRE_START:PRE_END,
    margin_ipop   = 0.02,
    sigf_ipop     = 7,
    bound_ipop    = 6
  ) |>

  # Step 4: construct synthetic control series
  generate_control()

message("Synthetic control fit complete.")

# ── 4. Inspect Fit Quality ────────────────────────────────────────────────────
# Grab balance table (predictor match quality)
balance <- grab_balance_table(scm)
message("\nPredictor balance (Comal | Synthetic | Donor pool):")
print(balance)

# Pre-treatment RMSPE
rmspe_pre <- scm |>
  grab_synthetic_control() |>
  filter(time_unit < TREATMENT_YEAR) |>
  mutate(sq_err = (real_y - synth_y)^2) |>
  summarise(rmspe = sqrt(mean(sq_err, na.rm = TRUE))) |>
  pull(rmspe)

pci_mean_pre <- df |>
  filter(fips == TREATED_FIPS, year < TREATMENT_YEAR) |>
  pull(!!sym(OUTCOME)) |>
  mean(na.rm = TRUE)

message(sprintf("\nPre-treatment RMSPE : $%.0f", rmspe_pre))
message(sprintf("As %% of pre-treatment mean: %.1f%%", 100 * rmspe_pre / pci_mean_pre))

# ── 5. Extract & Save Results ─────────────────────────────────────────────────
# 5a. Donor weights
weights <- grab_unit_weights(scm) |>
  rename(donor_fips = unit, weight = weight) |>
  arrange(desc(weight))

write_csv(weights, file.path(RESULTS_DIR, "scm_weights.csv"))
message(sprintf("\nTop donor weights:"))
print(head(weights, 10))

# 5b. Actual vs. synthetic time series
synth_series <- grab_synthetic_control(scm) |>
  rename(year = time_unit, actual = real_y, synthetic = synth_y) |>
  mutate(
    gap         = actual - synthetic,
    post        = year >= TREATMENT_YEAR
  )

write_csv(synth_series, file.path(RESULTS_DIR, "scm_synthetic_series.csv"))

# 5c. Gap series
write_csv(
  synth_series |> select(year, actual, synthetic, gap, post),
  file.path(RESULTS_DIR, "scm_gap.csv")
)

# 5d. Balance table
write_csv(balance, file.path(RESULTS_DIR, "scm_balance.csv"))

# 5e. RMSPE summary
writeLines(
  c(
    paste("Pre-treatment RMSPE:", round(rmspe_pre, 2)),
    paste("Pre-treatment mean outcome:", round(pci_mean_pre, 2)),
    paste("RMSPE as pct of mean:", round(100 * rmspe_pre / pci_mean_pre, 2), "%")
  ),
  file.path(RESULTS_DIR, "scm_pretrend_rmspe.txt")
)

message(sprintf("\nResults saved to: %s", RESULTS_DIR))

# ── 6. Figures ────────────────────────────────────────────────────────────────

# Figure 1: Trends plot (actual vs. synthetic)
p_trends <- ggplot(synth_series, aes(x = year)) +
  geom_line(aes(y = actual,    color = "Comal County"),    linewidth = 1.2) +
  geom_line(aes(y = synthetic, color = "Synthetic Comal"), linewidth = 1.2, linetype = "dashed") +
  geom_vline(xintercept = TREATMENT_YEAR - 0.5,
             linetype = "dotted", color = "black", linewidth = 0.8) +
  annotate("text", x = TREATMENT_YEAR, y = Inf, label = "1998 flood",
           hjust = -0.1, vjust = 1.5, size = 3.5) +
  scale_color_manual(values = c("Comal County" = "#d62728", "Synthetic Comal" = "#1f77b4")) +
  scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
  scale_x_continuous(breaks = seq(1990, 2022, 4)) +
  labs(
    title    = "Comal County vs. Synthetic Comal: Real Per Capita Income",
    subtitle = "Synthetic Control Method, 1990–2022 (2020 dollars)",
    x        = "Year",
    y        = "Real per capita income ($2020)",
    color    = NULL,
    caption  = "Source: BEA, BLS, Census. Donor pool: 69 Texas counties."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position   = "bottom",
    plot.title        = element_text(face = "bold"),
    panel.grid.minor  = element_blank()
  )

ggsave(file.path(FIG_DIR, "01_trends.png"), p_trends, width = 10, height = 5.5, dpi = 150)
message("Saved: figures/01_trends.png")

# Figure 2: Gap plot (treatment effect)
p_gap <- ggplot(synth_series, aes(x = year, y = gap)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.7) +
  geom_vline(xintercept = TREATMENT_YEAR - 0.5,
             linetype = "dotted", color = "black", linewidth = 0.8) +
  geom_ribbon(
    data = synth_series |> filter(year >= TREATMENT_YEAR),
    aes(ymin = pmin(gap, 0), ymax = pmax(gap, 0)),
    fill = "#d62728", alpha = 0.2
  ) +
  geom_line(color = "#d62728", linewidth = 1.3) +
  geom_point(
    data = synth_series |> filter(year >= TREATMENT_YEAR),
    color = "#d62728", size = 1.5
  ) +
  scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
  scale_x_continuous(breaks = seq(1990, 2022, 4)) +
  labs(
    title    = "Treatment Effect: Comal County minus Synthetic Comal",
    subtitle = "Positive values = Comal outperformed its synthetic counterfactual",
    x        = "Year",
    y        = "Gap in real per capita income ($2020)",
    caption  = "Dashed vertical line = 1998 Guadalupe River flood."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(FIG_DIR, "02_gap.png"), p_gap, width = 10, height = 5, dpi = 150)
message("Saved: figures/02_gap.png")

# Figure 3: Donor weights
top_weights <- weights |> filter(weight > 0.01) |> arrange(desc(weight))

p_weights <- ggplot(top_weights, aes(x = reorder(donor_fips, weight), y = weight)) +
  geom_col(fill = "#1f77b4", alpha = 0.85) +
  coord_flip() +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  labs(
    title = "Synthetic Control Donor Weights",
    subtitle = "Counties with weight > 1%",
    x = "FIPS (donor county)",
    y = "Weight (W*)"
  ) +
  theme_minimal(base_size = 11) +
  theme(plot.title = element_text(face = "bold"))

ggsave(file.path(FIG_DIR, "03_weights.png"), p_weights,
       width = 7, height = max(3, nrow(top_weights) * 0.35 + 1.5), dpi = 150)
message("Saved: figures/03_weights.png")

message("\n=== SCM estimation complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
message("Next: run R/02_placebo_tests.R for inference")
