## =============================================================================
## 02_placebo_tests.R
## Guadalupe River SCM Recovery — Phase 5, Step 4
##
## Inference via permutation (placebo) tests for the SCM treatment effect.
##
## Three standard validity checks:
##   1. In-space placebo  — Re-assign treatment to each donor county in turn;
##                          compare Comal's post/pre RMSPE ratio to the distribution.
##   2. Leave-one-out     — Drop the highest-weight donor and re-estimate;
##                          verify the main result is not driven by a single county.
##   3. In-time placebo   — Assign a false treatment at 1994 (5 years before true
##                          treatment) using only pre-1999 data; should show no effect.
##
## Inputs:
##   data/processed/panels/scm_panel.csv
##   data/processed/panels/scm_fips_list.csv
##   data/results/scm_weights.csv         (from 01_scm_estimate.R)
##
## Outputs (data/results/):
##   placebo_gaps.csv           — Gap series for every donor placebo
##   placebo_rmspe_ratios.csv   — Post/pre RMSPE ratio per unit (for p-value)
##   loo_synthetic_series.csv   — Leave-one-out synthetic series
##   intime_gap.csv             — In-time placebo gap series
##   figures/
##     04_placebo_gaps.png
##     05_rmspe_ratio.png
##     06_leave_one_out.png
##     07_intime_placebo.png
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(tidysynth)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(purrr)
  library(tidyr)
  library(scales)
})

# ── Paths & parameters ────────────────────────────────────────────────────────
PROJECT_ROOT    <- here::here()
PANEL_PATH      <- file.path(PROJECT_ROOT, "data/processed/panels/scm_panel.csv")
FIPS_LIST_PATH  <- file.path(PROJECT_ROOT, "data/processed/panels/scm_fips_list.csv")
WEIGHTS_PATH    <- file.path(PROJECT_ROOT, "data/results/scm_weights.csv")
RESULTS_DIR     <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR         <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

TREATED_FIPS   <- "48091"
TREATMENT_YEAR <- 1999
PRE_START      <- 1990
PRE_END        <- 1998
POST_END       <- 2022
OUTCOME        <- "per_capita_income_real"

# ── Load data ─────────────────────────────────────────────────────────────────
panel     <- read_csv(PANEL_PATH,     show_col_types = FALSE, col_types = cols(fips = col_character()))
fips_list <- read_csv(FIPS_LIST_PATH, show_col_types = FALSE, col_types = cols(fips = col_character()))
weights   <- read_csv(WEIGHTS_PATH,   show_col_types = FALSE, col_types = cols(donor_fips = col_character()))

df <- panel |>
  filter(fips %in% fips_list$fips, year >= PRE_START, year <= POST_END) |>
  arrange(fips, year)

all_fips   <- unique(df$fips)
donor_fips <- setdiff(all_fips, TREATED_FIPS)

# Helper: fit SCM for a given treated unit (returns synthetic control tibble)
fit_scm <- function(i_unit, df) {
  tryCatch({
    df |>
      synthetic_control(
        outcome  = !!sym(OUTCOME),
        unit     = fips,
        time     = year,
        i_unit   = i_unit,
        i_time   = TREATMENT_YEAR,
        generate_placebos = FALSE
      ) |>
      generate_predictor(time_window = PRE_START:1993,
                         pci_early = mean(per_capita_income_real, na.rm = TRUE)) |>
      generate_predictor(time_window = 1994:PRE_END,
                         pci_late  = mean(per_capita_income_real, na.rm = TRUE)) |>
      generate_predictor(time_window = PRE_START:PRE_END,
                         unemp_avg   = mean(laus_unemployment_rate, na.rm = TRUE),
                         emp_pop_avg = mean(emp_pop_ratio, na.rm = TRUE),
                         wage_avg    = mean(qcew_avg_annual_pay_real, na.rm = TRUE)) |>
      generate_predictor(time_window = PRE_START:1993,
                         pop_early   = mean(population, na.rm = TRUE)) |>
      generate_predictor(time_window = 1994:PRE_END,
                         pop_late    = mean(population, na.rm = TRUE)) |>
      generate_weights(optimization_window = PRE_START:PRE_END,
                       margin_ipop = 0.02, sigf_ipop = 7, bound_ipop = 6) |>
      generate_control() |>
      grab_synthetic_control() |>
      rename(year = time_unit, actual = real_y, synthetic = synth_y) |>
      mutate(fips = i_unit)
  }, error = function(e) {
    message(sprintf("  SCM failed for %s: %s", i_unit, conditionMessage(e)))
    NULL
  })
}

# RMSPE helper
rmspe <- function(actual, synthetic) sqrt(mean((actual - synthetic)^2, na.rm = TRUE))

# ── 1. In-Space Placebo Test ──────────────────────────────────────────────────
message("\n=== 1. In-Space Placebo Test ===")
message(sprintf("Running %d placebo SCMs (one per donor county)...", length(donor_fips)))

# Fit placebo for every donor county
placebo_results <- map(donor_fips, function(f) {
  message(sprintf("  Placebo: %s", f))
  fit_scm(f, df)
}, .progress = FALSE)

# Filter successful fits
placebo_results <- compact(placebo_results)

# Combine with Comal's main result (re-fit to get gap)
comal_scm_series <- fit_scm(TREATED_FIPS, df)

all_gaps <- bind_rows(
  list(comal_scm_series),
  placebo_results
) |>
  mutate(gap = actual - synthetic, is_treated = fips == TREATED_FIPS)

write_csv(all_gaps, file.path(RESULTS_DIR, "placebo_gaps.csv"))
message(sprintf("Saved: placebo_gaps.csv (%d county-year rows)", nrow(all_gaps)))

# ── RMSPE ratio: post/pre ──────────────────────────────────────────────────────
rmspe_ratios <- all_gaps |>
  group_by(fips) |>
  summarise(
    rmspe_pre  = rmspe(actual[year < TREATMENT_YEAR],  synthetic[year < TREATMENT_YEAR]),
    rmspe_post = rmspe(actual[year >= TREATMENT_YEAR], synthetic[year >= TREATMENT_YEAR]),
    ratio      = rmspe_post / rmspe_pre,
    is_treated = any(is_treated),
    .groups = "drop"
  ) |>
  arrange(desc(ratio))

write_csv(rmspe_ratios, file.path(RESULTS_DIR, "placebo_rmspe_ratios.csv"))

comal_ratio <- rmspe_ratios |> filter(fips == TREATED_FIPS) |> pull(ratio)
n_higher    <- rmspe_ratios |> filter(!is_treated, ratio >= comal_ratio) |> nrow()
p_value     <- (n_higher + 1) / (nrow(rmspe_ratios))   # including Comal

message(sprintf("\nComal post/pre RMSPE ratio: %.2f", comal_ratio))
message(sprintf("Donor counties with ratio >= Comal: %d / %d", n_higher, length(donor_fips)))
message(sprintf("Permutation p-value: %.3f", p_value))

# ── Figure 4: Placebo gap plot ─────────────────────────────────────────────────
# Exclude placebo units with very poor pre-treatment fit (rmspe_pre > 5× Comal)
comal_rmspe_pre <- rmspe_ratios |> filter(fips == TREATED_FIPS) |> pull(rmspe_pre)
good_placebos <- rmspe_ratios |>
  filter(!is_treated, rmspe_pre <= 5 * comal_rmspe_pre) |>
  pull(fips)

gap_plot_data <- all_gaps |>
  filter(fips %in% c(TREATED_FIPS, good_placebos))

p_placebo <- ggplot() +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
  geom_vline(xintercept = TREATMENT_YEAR - 0.5,
             linetype = "dotted", color = "black", linewidth = 0.8) +
  # Donor placebo gaps
  geom_line(
    data = gap_plot_data |> filter(!is_treated),
    aes(x = year, y = gap, group = fips),
    color = "gray70", linewidth = 0.5, alpha = 0.6
  ) +
  # Comal gap
  geom_line(
    data = gap_plot_data |> filter(is_treated),
    aes(x = year, y = gap),
    color = "#d62728", linewidth = 1.5
  ) +
  scale_x_continuous(breaks = seq(1990, 2022, 4)) +
  scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
  labs(
    title    = "In-Space Placebo Test: Gap in Real Per Capita Income",
    subtitle = sprintf(
      "Comal County (red) vs. %d donor county placebos | p = %.3f",
      length(good_placebos), p_value
    ),
    x       = "Year",
    y       = "Gap (actual − synthetic) in real PCI ($2020)",
    caption = "Gray lines: placebos with pre-treatment RMSPE ≤ 5× Comal's RMSPE"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(FIG_DIR, "04_placebo_gaps.png"), p_placebo, width = 10, height = 5.5, dpi = 150)
message("Saved: figures/04_placebo_gaps.png")

# ── Figure 5: RMSPE ratio distribution ───────────────────────────────────────
p_rmspe <- ggplot(rmspe_ratios, aes(x = ratio, fill = is_treated)) +
  geom_histogram(bins = 30, color = "white", alpha = 0.85) +
  geom_vline(xintercept = comal_ratio, color = "#d62728", linewidth = 1.5,
             linetype = "dashed") +
  scale_fill_manual(
    values = c("FALSE" = "#aec7e8", "TRUE" = "#d62728"),
    labels = c("Donor counties", "Comal County"),
    name   = NULL
  ) +
  annotate("text", x = comal_ratio, y = Inf,
           label = sprintf("Comal\n(ratio=%.1f)", comal_ratio),
           hjust = -0.1, vjust = 1.4, color = "#d62728", size = 3.5) +
  labs(
    title    = "Post/Pre RMSPE Ratio Distribution",
    subtitle = sprintf("p-value = %.3f (%d/%d donors have higher ratio than Comal)",
                       p_value, n_higher, length(donor_fips)),
    x        = "Post-treatment RMSPE / Pre-treatment RMSPE",
    y        = "Count"
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"), legend.position = "bottom")

ggsave(file.path(FIG_DIR, "05_rmspe_ratio.png"), p_rmspe, width = 9, height = 5, dpi = 150)
message("Saved: figures/05_rmspe_ratio.png")

# ── 2. Leave-One-Out Test ─────────────────────────────────────────────────────
message("\n=== 2. Leave-One-Out Test ===")

# Identify top donor county (highest weight)
top_donor <- weights |> arrange(desc(weight)) |> slice(1) |> pull(donor_fips)
message(sprintf("Top donor county (excluded in LOO): %s (weight = %.3f)",
                top_donor,
                weights |> filter(donor_fips == top_donor) |> pull(weight)))

# Re-fit without top donor
df_loo <- df |> filter(fips != top_donor)
loo_series <- fit_scm(TREATED_FIPS, df_loo)

if (!is.null(loo_series)) {
  loo_series <- loo_series |>
    mutate(gap_loo = actual - synthetic, is_treated = TRUE)
  write_csv(loo_series, file.path(RESULTS_DIR, "loo_synthetic_series.csv"))

  # Figure 6: LOO comparison
  loo_compare <- bind_rows(
    comal_scm_series |> mutate(gap = actual - synthetic, series = "Main estimate"),
    loo_series       |> mutate(gap = gap_loo,            series = sprintf("LOO (excl. %s)", top_donor))
  )

  loo_label <- sprintf("LOO (excl. %s)", top_donor)
  p_loo <- ggplot(loo_compare, aes(x = year, y = gap, color = series, linetype = series)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_vline(xintercept = TREATMENT_YEAR - 0.5,
               linetype = "dotted", color = "black", linewidth = 0.8) +
    geom_line(linewidth = 1.3) +
    scale_color_manual(
      values = setNames(c("#d62728", "#1f77b4"), c("Main estimate", loo_label)),
      name = NULL
    ) +
    scale_linetype_manual(
      values = setNames(c("solid", "dashed"), c("Main estimate", loo_label)),
      name = NULL
    ) +
    scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
    scale_x_continuous(breaks = seq(1990, 2022, 4)) +
    labs(
      title    = "Leave-One-Out Robustness Check",
      subtitle = sprintf("Main SCM vs. SCM excluding top donor county (%s)", top_donor),
      x        = "Year", y = "Gap in real PCI ($2020)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"), legend.position = "bottom",
          panel.grid.minor = element_blank())

  ggsave(file.path(FIG_DIR, "06_leave_one_out.png"), p_loo, width = 10, height = 5, dpi = 150)
  message("Saved: figures/06_leave_one_out.png")
} else {
  message("LOO estimation failed; skipping.")
}

# ── 3. In-Time Placebo Test ───────────────────────────────────────────────────
message("\n=== 3. In-Time Placebo Test ===")
FALSE_TREATMENT_YEAR <- 1994

# Use only pre-actual-treatment data (1990–1998)
df_intime <- df |> filter(year <= PRE_END)

intime_scm <- tryCatch({
  df_intime |>
    synthetic_control(
      outcome  = !!sym(OUTCOME),
      unit     = fips,
      time     = year,
      i_unit   = TREATED_FIPS,
      i_time   = FALSE_TREATMENT_YEAR,
      generate_placebos = FALSE
    ) |>
    generate_predictor(
      time_window = PRE_START:(FALSE_TREATMENT_YEAR - 1),
      pci_early   = mean(per_capita_income_real, na.rm = TRUE)
    ) |>
    generate_predictor(
      time_window = PRE_START:(FALSE_TREATMENT_YEAR - 1),
      unemp_avg   = mean(laus_unemployment_rate, na.rm = TRUE),
      emp_pop_avg = mean(emp_pop_ratio, na.rm = TRUE),
      wage_avg    = mean(qcew_avg_annual_pay_real, na.rm = TRUE)
    ) |>
    generate_weights(
      optimization_window = PRE_START:(FALSE_TREATMENT_YEAR - 1),
      margin_ipop = 0.02, sigf_ipop = 7, bound_ipop = 6
    ) |>
    generate_control() |>
    grab_synthetic_control() |>
    rename(year = time_unit, actual = real_y, synthetic = synth_y) |>
    mutate(gap = actual - synthetic)
}, error = function(e) {
  message(sprintf("In-time placebo failed: %s", conditionMessage(e)))
  NULL
})

if (!is.null(intime_scm)) {
  write_csv(intime_scm, file.path(RESULTS_DIR, "intime_gap.csv"))

  p_intime <- ggplot(intime_scm, aes(x = year, y = gap)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_vline(xintercept = FALSE_TREATMENT_YEAR - 0.5,
               linetype = "dotted", color = "#1f77b4", linewidth = 1) +
    geom_line(color = "#1f77b4", linewidth = 1.3) +
    scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
    labs(
      title    = "In-Time Placebo Test: False Treatment in 1994",
      subtitle = "Using pre-treatment data only (1990–1998); should show no persistent gap",
      x        = "Year", y = "Gap in real PCI ($2020)",
      caption  = "Blue dashed line = false treatment date (1994)"
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

  ggsave(file.path(FIG_DIR, "07_intime_placebo.png"), p_intime,
         width = 9, height = 4.5, dpi = 150)
  message("Saved: figures/07_intime_placebo.png")
}

# ── Summary ───────────────────────────────────────────────────────────────────
message("\n=== PLACEBO TEST SUMMARY ===")
message(sprintf("In-space p-value    : %.3f", p_value))
message(sprintf("Comal RMSPE ratio   : %.2f", comal_ratio))
message(sprintf("Leave-one-out: check figures/06_leave_one_out.png"))
message(sprintf("In-time placebo:     check figures/07_intime_placebo.png"))
message(sprintf("\nAll results saved to: %s", RESULTS_DIR))
