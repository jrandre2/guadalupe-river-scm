## =============================================================================
## 08_scm_sector.R
## Guadalupe River SCM Recovery — Extension: Sector-Level SCM
##
## Runs separate Synthetic Control analyses for three sectors:
##   - Construction (NAICS 23)
##   - Retail Trade (NAICS 44-45)
##   - Food Service & Accommodation (NAICS 72)
##
## Answers: "Which sectors recovered fastest after the 1998 flood?"
##
## Inputs:
##   data/processed/panels/scm_sector_panel.csv   (built by panel_builder.py)
##   data/processed/panels/scm_fips_list.csv      (31 growth-matched donors)
##
## Outputs (saved to data/results/):
##   scm_sector_summary.csv      — ATT, RMSPE, p-value per sector
##   scm_sector_gaps.csv         — Gap series per sector per year
##   scm_sector_weights.csv      — Donor weights per sector
##   figures/12_sector_gaps.png  — 3-panel gap plot
##   figures/13_sector_recovery.png — Recovery timeline comparison
##
## Note on pre-treatment window:
##   NAICS-based QCEW starts ~1990, giving 9 pre-treatment years (1990–1998).
##   This is borderline acceptable for SCM; interpret with caution.
##
## Note on multiple testing:
##   3 sector tests. Both raw and Holm-adjusted p-values are reported.
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(tidysynth)
  library(magrittr)   # for %>% (R 4.1 doesn't support {blocks} in native |> pipe)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(tidyr)
  library(purrr)
  library(stringr)
  library(scales)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
# PROJECT_ROOT <- "/Volumes/T9/Projects/ Guadalupe River Synthetic Controls Recovery"

SECTOR_PANEL_PATH <- file.path(PROJECT_ROOT, "data/processed/panels/scm_sector_panel.csv")
FIPS_LIST_PATH    <- file.path(PROJECT_ROOT, "data/processed/panels/scm_fips_list.csv")
RESULTS_DIR       <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR           <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ───────────────────────────────────────────────────────────────
TREATED_FIPS   <- "48091"
TREATMENT_YEAR <- 1999
PRE_START      <- 1990
PRE_END        <- 1998
POST_END       <- 2022

# Sectors: outcome column (employment), label
SECTORS <- list(
  construction = list(
    emp_col   = "construction_emp",
    wage_col  = "construction_wages_real",
    label     = "Construction (NAICS 23)"
  ),
  retail = list(
    emp_col   = "retail_emp",
    wage_col  = "retail_wages_real",
    label     = "Retail Trade (NAICS 44-45)"
  ),
  foodservice = list(
    emp_col   = "foodservice_emp",
    wage_col  = "foodservice_wages_real",
    label     = "Food Service & Accommodation (NAICS 72)"
  )
)

# ── 1. Load Data ─────────────────────────────────────────────────────────────
message("Loading sector panel...")

if (!file.exists(SECTOR_PANEL_PATH)) {
  stop(paste(
    "Sector panel not found:", SECTOR_PANEL_PATH,
    "\nRun: python -c \"from src.process.panel_builder import build_sector_panel; build_sector_panel()\""
  ))
}

panel     <- read_csv(SECTOR_PANEL_PATH, show_col_types = FALSE,
                      col_types = cols(fips = col_character()))
fips_list <- read_csv(FIPS_LIST_PATH,   show_col_types = FALSE,
                      col_types = cols(fips = col_character()))

message(sprintf("Sector panel: %d rows, %d cols", nrow(panel), ncol(panel)))
message(sprintf("Donor pool: %d counties", nrow(fips_list)))

# ── 2. Fit SCM for Each Sector ───────────────────────────────────────────────

fit_sector_scm <- function(sector_name, sector_info, panel, fips_list) {
  emp_col  <- sector_info$emp_col
  label    <- sector_info$label

  message(sprintf("\n--- Sector: %s ---", label))

  if (!emp_col %in% names(panel)) {
    warning(sprintf("Column '%s' not found in panel. Skipping.", emp_col))
    return(NULL)
  }

  # Filter to donor pool + treated, study window
  df <- panel |>
    filter(
      fips %in% fips_list$fips,
      year >= PRE_START,
      year <= POST_END
    ) |>
    mutate(unit_id = as.integer(factor(fips))) |>
    arrange(fips, year)

  # Check data availability for this sector
  pre_data <- df |> filter(fips == TREATED_FIPS, year >= PRE_START, year <= PRE_END)
  n_missing_comal <- sum(is.na(pre_data[[emp_col]]))
  if (n_missing_comal > 2) {
    warning(sprintf("Comal has %d missing pre-treatment values for %s. Skipping.",
                    n_missing_comal, emp_col))
    return(NULL)
  }

  # Drop donors with >20% missing in this sector over pre-treatment period
  pre_all <- df |> filter(year >= PRE_START, year <= PRE_END)
  suppression <- pre_all |>
    group_by(fips) |>
    summarise(pct_missing = mean(is.na(.data[[emp_col]])), .groups = "drop")

  heavy_suppression <- suppression |> filter(pct_missing > 0.20) |> pull(fips)
  if (length(heavy_suppression) > 0) {
    message(sprintf("  Dropping %d counties with >20%% missing in %s",
                    length(heavy_suppression), emp_col))
    df <- df |> filter(!fips %in% heavy_suppression)
  }

  n_donors_avail <- n_distinct(df$fips) - 1  # subtract treated
  if (n_donors_avail < 5) {
    warning(sprintf("Only %d donors available for %s after suppression filter. Skipping.",
                    n_donors_avail, sector_name))
    return(NULL)
  }
  message(sprintf("  Available donors: %d", n_donors_avail))

  # Log-transform employment (more symmetric, reduces scale sensitivity)
  df <- df |>
    mutate(outcome = log(.data[[emp_col]] + 1))

  comal_id <- df |> filter(fips == TREATED_FIPS) |> pull(unit_id) |> unique()

  # Fit SCM using magrittr %>% (required for R 4.1 — native |> doesn't support
  # multi-step tidysynth chains reliably on this version).
  # Covariates (per_capita_income_real, population) are always present because
  # build_sector_panel() merges them from the main SCM panel.
  scm <- tryCatch({
    df %>%
      synthetic_control(
        outcome  = outcome,
        unit     = fips,
        time     = year,
        i_unit   = TREATED_FIPS,
        i_time   = TREATMENT_YEAR,
        generate_placebos = TRUE
      ) %>%
      generate_predictor(
        time_window = PRE_START:1993,
        emp_early   = mean(outcome, na.rm = TRUE)
      ) %>%
      generate_predictor(
        time_window = 1994:PRE_END,
        emp_late    = mean(outcome, na.rm = TRUE)
      ) %>%
      generate_predictor(
        time_window = PRE_START:PRE_END,
        pci_avg     = mean(per_capita_income_real, na.rm = TRUE)
      ) %>%
      generate_predictor(
        time_window = PRE_START:1993,
        pop_early   = mean(population, na.rm = TRUE)
      ) %>%
      generate_predictor(
        time_window = 1994:PRE_END,
        pop_late    = mean(population, na.rm = TRUE)
      ) %>%
      generate_weights(
        optimization_window = PRE_START:PRE_END,
        margin_ipop   = 0.02,
        sigf_ipop     = 7,
        bound_ipop    = 6
      ) %>%
      generate_control()
  }, error = function(e) {
    warning(sprintf("SCM fitting failed for %s: %s", sector_name, conditionMessage(e)))
    NULL
  })

  if (is.null(scm)) return(NULL)

  # Extract results
  synth_series <- grab_synthetic_control(scm) |>
    rename(year = time_unit, actual = real_y, synthetic = synth_y) |>
    mutate(
      gap      = actual - synthetic,
      post     = year >= TREATMENT_YEAR,
      sector   = sector_name,
      emp_col  = emp_col
    )

  # Pre-treatment RMSPE
  rmspe_pre <- synth_series |>
    filter(year < TREATMENT_YEAR) |>
    mutate(sq_err = (actual - synthetic)^2) |>
    summarise(rmspe = sqrt(mean(sq_err, na.rm = TRUE))) |>
    pull(rmspe)

  outcome_mean_pre <- synth_series |>
    filter(year < TREATMENT_YEAR) |>
    summarise(m = mean(actual, na.rm = TRUE)) |>
    pull(m)

  message(sprintf("  Pre-treatment RMSPE: %.4f (%.1f%% of mean)",
                  rmspe_pre, 100 * rmspe_pre / abs(outcome_mean_pre)))

  # Donor weights
  weights <- grab_unit_weights(scm) |>
    rename(donor_fips = unit, weight = weight) |>
    mutate(sector = sector_name) |>
    arrange(desc(weight))
  message("  Top donors:")
  print(head(weights, 5))

  # In-space permutation p-value via post/pre RMSPE ratio (same as R/02)
  rmspe_fn <- function(actual, synth) sqrt(mean((actual - synth)^2, na.rm = TRUE))

  p_value <- tryCatch({
    all_gaps <- grab_synthetic_control(scm, placebo = TRUE) %>%
      rename(year = time_unit, actual = real_y, synthetic = synth_y) %>%
      mutate(gap = actual - synthetic)

    rmspe_ratios <- all_gaps %>%
      group_by(.id) %>%
      summarise(
        rmspe_pre  = rmspe_fn(actual[year <  TREATMENT_YEAR], synthetic[year <  TREATMENT_YEAR]),
        rmspe_post = rmspe_fn(actual[year >= TREATMENT_YEAR], synthetic[year >= TREATMENT_YEAR]),
        ratio      = rmspe_post / rmspe_pre,
        .groups    = "drop"
      )

    comal_ratio <- rmspe_ratios %>% filter(.id == TREATED_FIPS) %>% pull(ratio)
    n_higher    <- rmspe_ratios %>% filter(.id != TREATED_FIPS, ratio >= comal_ratio) %>% nrow()
    (n_higher + 1) / nrow(rmspe_ratios)
  }, error = function(e) {
    warning(sprintf("p-value calculation failed for %s: %s", sector_name, conditionMessage(e)))
    NA_real_
  })

  message(sprintf("  Permutation p-value: %.3f", ifelse(is.na(p_value), -1, p_value)))

  # Post-treatment ATT (average gap in log-employment)
  att_post <- synth_series |>
    filter(post) |>
    summarise(att = mean(gap, na.rm = TRUE)) |>
    pull(att)

  list(
    sector      = sector_name,
    label       = label,
    rmspe_pre   = rmspe_pre,
    att_post    = att_post,
    p_value     = p_value,
    synth_series = synth_series,
    weights     = weights,
    scm_obj     = scm
  )
}

# Run for all sectors
results <- map(names(SECTORS), function(s) {
  fit_sector_scm(s, SECTORS[[s]], panel, fips_list)
})
names(results) <- names(SECTORS)

# Remove failed sectors
results <- Filter(Negate(is.null), results)
message(sprintf("\n\n=== Sector SCM complete: %d of %d sectors succeeded ===",
                length(results), length(SECTORS)))

# ── 3. Multiple Testing Correction ──────────────────────────────────────────
# Apply Holm-Bonferroni correction across the 3 sector tests

if (length(results) > 0) {
  raw_p <- sapply(results, function(r) ifelse(is.null(r$p_value), NA, r$p_value))
  holm_p <- p.adjust(raw_p, method = "holm")

  for (i in seq_along(results)) {
    results[[i]]$p_holm <- holm_p[i]
  }
}

# ── 4. Save Results ──────────────────────────────────────────────────────────

# 4a. Summary table
summary_df <- map_dfr(results, function(r) {
  tibble(
    sector      = r$sector,
    label       = r$label,
    rmspe_pre   = round(r$rmspe_pre, 4),
    att_post    = round(r$att_post, 4),
    p_raw       = round(r$p_value, 3),
    p_holm      = round(r$p_holm, 3),
    significant = ifelse(!is.na(r$p_holm), r$p_holm < 0.05, FALSE)
  )
})
write_csv(summary_df, file.path(RESULTS_DIR, "scm_sector_summary.csv"))
message("\nSector SCM Summary:")
print(summary_df)

# 4b. Gap series (all sectors combined)
gaps_df <- map_dfr(results, function(r) r$synth_series)
write_csv(gaps_df, file.path(RESULTS_DIR, "scm_sector_gaps.csv"))

# 4c. Donor weights (all sectors combined)
weights_df <- map_dfr(results, function(r) r$weights)
write_csv(weights_df, file.path(RESULTS_DIR, "scm_sector_weights.csv"))

# ── 5. Figures ────────────────────────────────────────────────────────────────

# Build sector label lookup from results (avoids R-4.1 pipe/lambda issues)
sector_label_map <- setNames(
  sapply(names(SECTORS), function(s) SECTORS[[s]]$label),
  names(SECTORS)
)

# Figure 12: 3-panel gap plot (one panel per sector)
if (length(results) > 0) {
  gaps_plot <- gaps_df
  gaps_plot$sector_label <- sector_label_map[gaps_plot$sector]
  gaps_plot$sector_label[is.na(gaps_plot$sector_label)] <- gaps_plot$sector[is.na(gaps_plot$sector_label)]

  # Add significance annotation
  sig_labels <- map_dfr(results, function(r) {
    tibble(
      sector       = r$sector,
      sector_label = sector_label_map[[r$sector]],
      p_holm       = r$p_holm,
      ann          = sprintf("p = %.3f (Holm)", r$p_holm)
    )
  })

  p12 <- ggplot(gaps_plot, aes(x = year, y = gap)) +
    geom_hline(yintercept = 0, color = "gray60", linewidth = 0.6) +
    geom_vline(xintercept = TREATMENT_YEAR - 0.5,
               linetype = "dotted", color = "black", linewidth = 0.8) +
    geom_ribbon(
      data = gaps_plot[gaps_plot$year >= TREATMENT_YEAR, ],
      aes(ymin = pmin(gap, 0), ymax = pmax(gap, 0)),
      fill = "#d62728", alpha = 0.15
    ) +
    geom_line(color = "#d62728", linewidth = 1.1) +
    geom_point(
      data = gaps_plot[gaps_plot$year >= TREATMENT_YEAR, ],
      color = "#d62728", size = 1.3
    ) +
    geom_text(
      data = sig_labels,
      aes(x = POST_END - 1, y = Inf, label = ann),
      hjust = 1, vjust = 1.8, size = 3, color = "gray30"
    ) +
    facet_wrap(~sector_label, ncol = 1, scales = "free_y") +
    scale_x_continuous(breaks = seq(1990, POST_END, 4)) +
    labs(
      title   = "Sector-Level Treatment Effects: Comal County vs. Synthetic",
      subtitle = "Log-employment gap (actual − synthetic), 1990–2022",
      x = "Year",
      y = "Gap in log employment",
      caption = "Vertical dotted line = 1998 Guadalupe River flood. Holm-adjusted p-values shown."
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.title       = element_text(face = "bold"),
      panel.grid.minor = element_blank(),
      strip.text       = element_text(face = "bold")
    )

  ggsave(file.path(FIG_DIR, "12_sector_gaps.png"), p12,
         width = 10, height = 4 * length(results), dpi = 150)
  message("Saved: figures/12_sector_gaps.png")

  # Figure 13: Post-treatment ATT comparison across sectors
  p13 <- ggplot(summary_df, aes(x = reorder(label, att_post), y = att_post,
                                 fill = significant)) +
    geom_col(alpha = 0.85) +
    geom_hline(yintercept = 0, color = "gray40") +
    geom_text(aes(
      label = sprintf("p=%.3f", p_holm),
      y = ifelse(att_post >= 0, att_post + 0.003, att_post - 0.003),
      vjust = ifelse(att_post >= 0, 0, 1)
    ), size = 3.5) +
    coord_flip() +
    scale_fill_manual(values = c("FALSE" = "#aec7e8", "TRUE" = "#1f77b4"),
                      labels = c("Not significant", "Significant (Holm p < 0.05)")) +
    labs(
      title   = "Average Post-Treatment Effect by Sector",
      subtitle = "Log-employment: Comal County vs. Synthetic Comal (1999–2022 avg)",
      x = NULL,
      y = "Avg gap in log employment",
      fill = NULL,
      caption = "Holm-adjusted p-values. Error bars not shown (see gap plots for full distribution)."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title      = element_text(face = "bold"),
      legend.position = "bottom"
    )

  ggsave(file.path(FIG_DIR, "13_sector_recovery.png"), p13,
         width = 9, height = max(3.5, nrow(summary_df) * 0.7 + 2), dpi = 150)
  message("Saved: figures/13_sector_recovery.png")
}

message("\n=== R/08_scm_sector.R complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
