## =============================================================================
## 10_did_zbp_sector.R
## Guadalupe River SCM Recovery — ZIP-Level Sector DiD (ZBP Detail)
##
## Tests whether flood-exposed ZIPs show different sector employment trajectories
## compared to control ZIPs, using ZBP detail (NAICS breakdown) data.
##
## Sectors: Construction (NAICS 23), Retail (NAICS 44-45), Food Service (NAICS 72)
## Same 4 treated / 9 control ZIPs as R/03_did_zbp.R.
## Treatment: NFIP payouts Oct-Nov 1998 > $500K.
##
## Inputs:
##   data/processed/panels/did_zbp_sector_panel.csv  (built by 06_zbp_sector_panel.ipynb)
##
## Outputs (saved to data/results/):
##   did_zbp_sector_results.txt       — Regression tables
##   did_zbp_sector_event_study.csv   — Event study coefficients + CIs
##   figures/17_zbp_sector_trajectories.png  (built by notebook)
##   figures/18_zbp_sector_event_study.png   — Event study plot
##   figures/19_zbp_sector_shares.png        — Sector share comparison
##
## R packages required: fixest, tidyverse, ggplot2, scales
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(fixest)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(tidyr)
  library(stringr)
  library(scales)
})

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
PANEL_PATH   <- file.path(PROJECT_ROOT, "data/processed/panels/did_zbp_sector_panel.csv")
RESULTS_DIR  <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR      <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ────────────────────────────────────────────────────────────────
REF_YEAR <- 1998   # Reference year for event study (last pre-treatment year)
SECTORS  <- c("construction", "retail", "foodservice")
SECTOR_LABELS <- c(
  construction = "Construction (NAICS 23)",
  retail       = "Retail (NAICS 44-45)",
  foodservice  = "Food Service (NAICS 72)"
)

# ── 1. Load Data ──────────────────────────────────────────────────────────────
message("Loading ZBP sector panel...")

if (!file.exists(PANEL_PATH)) {
  stop(paste(
    "ZBP sector panel not found:", PANEL_PATH,
    "\nRun: notebooks/06_zbp_sector_panel.ipynb first"
  ))
}

df <- read_csv(PANEL_PATH, show_col_types = FALSE,
               col_types = cols(zip = col_character()))

df <- df |>
  mutate(
    year    = as.integer(year),
    treated = as.integer(treated),
    post    = as.integer(post),
    across(starts_with("ln_"),         as.numeric),
    across(ends_with("_emp"),          as.numeric),
    across(ends_with("_share"),        as.numeric),
    intensity = as.numeric(intensity)
  )

message(sprintf(
  "Panel: %d ZIPs (%d treated, %d control), %d years (%d–%d)",
  n_distinct(df$zip),
  n_distinct(df$zip[df$treated == 1]),
  n_distinct(df$zip[df$treated == 0]),
  n_distinct(df$year), min(df$year), max(df$year)
))

# ── 2. Data Quality: Suppression Summary ──────────────────────────────────────
cat("\n=== Employment Suppression (% zero cells by sector) ===\n")
for (sec in SECTORS) {
  col <- paste0(sec, "_emp")
  zero_pct <- mean(df[[col]] == 0, na.rm = TRUE) * 100
  cat(sprintf("  %-15s: %.1f%% zero\n", sec, zero_pct))
}

# Flag heavily-suppressed sectors for this analysis
suppression_threshold <- 0.40  # drop sector from binary DiD if >40% zero
active_sectors <- c()
for (sec in SECTORS) {
  col <- paste0(sec, "_emp")
  zero_pct <- mean(df[[col]] == 0, na.rm = TRUE)
  if (zero_pct <= suppression_threshold) {
    active_sectors <- c(active_sectors, sec)
  } else {
    message(sprintf("WARNING: %s has %.0f%% zero cells — dropping from DiD", sec, zero_pct * 100))
  }
}
message(sprintf("Active sectors for DiD: %s", paste(active_sectors, collapse = ", ")))

# ── 3. Binary DiD per Sector ──────────────────────────────────────────────────
message("\n--- Binary DiD: sector employment (log) ---")

did_results <- list()
for (sec in active_sectors) {
  outcome <- paste0("ln_", sec)
  message(sprintf("  Sector: %s ~ %s", sec, outcome))

  # Drop rows where outcome is effectively missing (floor of log(1) = 0 means suppressed)
  df_sec <- df |> filter(.data[[paste0(sec, "_emp")]] > 0 | year < 1999)

  fit <- feols(
    as.formula(sprintf("%s ~ treated:post | zip + year", outcome)),
    data  = df_sec,
    vcov  = ~zip
  )
  did_results[[sec]] <- fit
  print(summary(fit))
}

# ── 4. Event Study per Sector ─────────────────────────────────────────────────
message("\n--- Event study: sector employment (log) ---")

es_results <- list()
for (sec in active_sectors) {
  outcome <- paste0("ln_", sec)
  df_sec  <- df |> filter(.data[[paste0(sec, "_emp")]] > 0 | year < 1999)

  fit <- feols(
    as.formula(sprintf(
      "%s ~ i(year, treated, ref = %d) | zip + year",
      outcome, REF_YEAR
    )),
    data = df_sec,
    vcov = ~zip
  )
  es_results[[sec]] <- fit
  print(summary(fit))
}

# ── 5. Intensity DiD ──────────────────────────────────────────────────────────
message("\n--- Intensity DiD (NFIP payout dose-response) ---")

intensity_results <- list()
for (sec in active_sectors) {
  outcome <- paste0("ln_", sec)
  df_sec  <- df |> filter(.data[[paste0(sec, "_emp")]] > 0 | year < 1999)

  fit <- feols(
    as.formula(sprintf("%s ~ intensity:post | zip + year", outcome)),
    data = df_sec,
    vcov = ~zip
  )
  intensity_results[[sec]] <- fit
}

# ── 6. Sector Share DiD ───────────────────────────────────────────────────────
message("\n--- Sector share DiD ---")

share_results <- list()
for (sec in active_sectors) {
  outcome <- paste0(sec, "_share")
  fit <- feols(
    as.formula(sprintf("%s ~ treated:post | zip + year", outcome)),
    data = df,
    vcov = ~zip
  )
  share_results[[sec]] <- fit
}

# ── 7. Save Regression Output ─────────────────────────────────────────────────
sink(file.path(RESULTS_DIR, "did_zbp_sector_results.txt"))
cat("=== ZIP-Level Sector DiD (ZBP Detail, NAICS Breakdown) ===\n")
cat(sprintf("Date: %s\n", Sys.time()))
cat(sprintf("Panel: %d ZIPs (%d treated, %d control), %d years (%d–%d)\n",
            n_distinct(df$zip),
            n_distinct(df$zip[df$treated == 1]),
            n_distinct(df$zip[df$treated == 0]),
            n_distinct(df$year), min(df$year), max(df$year)))
cat("Treatment: NFIP payouts > $500K (Oct-Nov 1998 flood)\n")
cat("SEs: clustered by ZIP (13 clusters)\n\n")

for (sec in active_sectors) {
  cat(rep("-", 60), "\n")
  cat(sprintf("Sector: %s\n", SECTOR_LABELS[sec]))
  cat("Binary DiD (ln_emp ~ treated:post | zip + year):\n")
  print(summary(did_results[[sec]]))
  cat("\nEvent Study:\n")
  print(summary(es_results[[sec]]))
  cat("\nIntensity DiD:\n")
  print(summary(intensity_results[[sec]]))
  cat("\nSector share DiD:\n")
  print(summary(share_results[[sec]]))
  cat("\n")
}
sink()
message("Saved: did_zbp_sector_results.txt")

# ── 8. Event Study CSV ────────────────────────────────────────────────────────
es_all <- purrr::map_dfr(active_sectors, function(sec) {
  fit <- es_results[[sec]]
  coefs <- as.data.frame(coef(fit)) |>
    tibble::rownames_to_column("term") |>
    rename(estimate = 2) |>
    mutate(
      se    = sqrt(diag(vcov(fit))),
      ci_lo = estimate - 1.96 * se,
      ci_hi = estimate + 1.96 * se,
      year  = as.integer(str_extract(term, "\\d{4}")),
      sector = sec
    ) |>
    filter(!is.na(year)) |>
    select(year, sector, estimate, se, ci_lo, ci_hi)

  # Add reference year (0 by construction)
  ref_row <- tibble(year = REF_YEAR, sector = sec,
                    estimate = 0, se = 0, ci_lo = 0, ci_hi = 0)
  bind_rows(coefs, ref_row)
})

write_csv(es_all, file.path(RESULTS_DIR, "did_zbp_sector_event_study.csv"))
message("Saved: did_zbp_sector_event_study.csv")

# ── 9. Figure 18: Event Study Plots ───────────────────────────────────────────
es_plot_df <- es_all |>
  filter(sector %in% active_sectors) |>
  mutate(sector_label = SECTOR_LABELS[sector])

p18 <- ggplot(es_plot_df, aes(x = year, y = estimate, ymin = ci_lo, ymax = ci_hi)) +
  geom_hline(yintercept = 0, color = "gray60", linewidth = 0.7) +
  geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dashed", color = "gray40") +
  geom_ribbon(alpha = 0.15, fill = "#d62728") +
  geom_line(color = "#d62728", linewidth = 1.1) +
  geom_point(color = "#d62728", size = 1.8) +
  annotate("text", x = REF_YEAR + 0.5, y = Inf,
           label = "1998 flood", hjust = -0.1, vjust = 1.5,
           size = 2.8, color = "gray40") +
  facet_wrap(~sector_label, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(1994, 2020, 2)) +
  labs(
    title    = "ZIP-Level Sector DiD Event Study (ZBP Detail)",
    subtitle = "Coef on (year × treated), ref = 1998. Clustered SEs (13 ZIPs).",
    x        = "Year",
    y        = "Estimated coefficient (log employment)",
    caption  = paste(
      "95% CI, clustered by ZIP. Treated = 4 ZIPs with NFIP payouts > $500K.",
      "\nControl = 9 ZIPs in Comal/Kendall/Hays with < $130K damage."
    )
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold")
  )

ggsave(file.path(FIG_DIR, "18_zbp_sector_event_study.png"), p18,
       width = 10, height = 3 * length(active_sectors) + 1, dpi = 150)
message("Saved: figures/18_zbp_sector_event_study.png")

# ── 10. Figure 19: Sector Share Comparison (Treated vs Control) ───────────────
share_traj <- df |>
  filter(year %in% c(1994, 1998, 2002, 2010, 2020)) |>
  group_by(year, treated) |>
  summarise(
    construction = mean(construction_share, na.rm = TRUE),
    retail       = mean(retail_share,       na.rm = TRUE),
    foodservice  = mean(foodservice_share,  na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_longer(c(construction, retail, foodservice),
               names_to = "sector", values_to = "share") |>
  mutate(
    group        = ifelse(treated == 1, "Treated ZIPs\n(NFIP > $500K)", "Control ZIPs"),
    sector_label = SECTOR_LABELS[sector],
    year_label   = as.character(year)
  )

p19 <- ggplot(share_traj, aes(x = year_label, y = share, fill = group)) +
  geom_col(position = "dodge", alpha = 0.85) +
  facet_wrap(~sector_label, nrow = 1) +
  scale_y_continuous(labels = percent_format(accuracy = 1)) +
  scale_fill_manual(values = c("Treated ZIPs\n(NFIP > $500K)" = "#d62728",
                               "Control ZIPs" = "#1f77b4")) +
  labs(
    title    = "Sector Employment Shares: Treated vs Control ZIPs (ZBP Detail)",
    subtitle = "Share of total ZIP employment in each sector, selected years",
    x        = NULL,
    y        = "Employment share",
    fill     = NULL,
    caption  = "Source: Census ZBP Detail (NAICS). Descriptive — vertical line = flood year."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title  = element_text(face = "bold"),
    strip.text  = element_text(face = "bold"),
    axis.text.x = element_text(angle = 20, hjust = 1),
    legend.position = "top"
  )

ggsave(file.path(FIG_DIR, "19_zbp_sector_shares.png"), p19,
       width = 12, height = 5, dpi = 150)
message("Saved: figures/19_zbp_sector_shares.png")

# ── 11. Summary Table ─────────────────────────────────────────────────────────
cat("\n")
cat(rep("=", 70), "\n")
cat("SECTOR DiD SUMMARY\n")
cat(rep("=", 70), "\n")
cat(sprintf("%-18s  %10s  %10s  %8s\n", "Sector", "ATT (log emp)", "Cluster SE", "p-value"))
cat(rep("-", 55), "\n")
for (sec in active_sectors) {
  fit <- did_results[[sec]]
  coef_name <- grep("treated:post|post:treated", names(coef(fit)), value = TRUE)[1]
  if (!is.na(coef_name)) {
    est <- coef(fit)[coef_name]
    se  <- sqrt(vcov(fit)[coef_name, coef_name])
    pv  <- 2 * pt(abs(est / se), df = n_distinct(df$zip) - 2, lower.tail = FALSE)
    cat(sprintf("%-18s  %+10.3f  %10.3f  %8.3f\n",
                SECTOR_LABELS[sec], est, se, pv))
  }
}
cat(rep("=", 70), "\n\n")

message("=== R/10_did_zbp_sector.R complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
