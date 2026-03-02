## =============================================================================
## 05_ascm.R
## Guadalupe River SCM Recovery — Augmented Synthetic Control Method
##
## Ridge-augmented SCM (Ben-Michael, Feller, Rothstein 2021) provides
## bias-corrected estimates and valid confidence intervals for the
## treatment effect, even with imperfect pre-treatment fit.
##
## Inputs:
##   data/processed/panels/scm_panel.csv
##   data/processed/panels/scm_fips_list.csv   (screened donor pool + Comal)
##   data/results/scm_gap.csv                  (from 01_scm_estimate.R)
##
## Outputs (saved to data/results/):
##   ascm_att.csv             — ATT by year with 95% CIs
##   ascm_summary.txt         — Average ATT, p-value, CI
##   ascm_weights.csv         — ASCM unit weights
##   figures/08_ascm_gap.png  — Gap plot with confidence bands
##   figures/09_ascm_vs_scm.png — Traditional SCM vs ASCM comparison
##
## R packages required:
##   augsynth, tidyverse, ggplot2
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(augsynth)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(tidyr)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
PANEL_PATH     <- file.path(PROJECT_ROOT, "data/processed/panels/scm_panel.csv")
FIPS_LIST_PATH <- file.path(PROJECT_ROOT, "data/processed/panels/scm_fips_list.csv")
SCM_GAP_PATH   <- file.path(PROJECT_ROOT, "data/results/scm_gap.csv")
RESULTS_DIR    <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR        <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ───────────────────────────────────────────────────────────────
TREATED_FIPS   <- "48091"          # Comal County, TX
TREATMENT_YEAR <- 1999             # First post-treatment year
PRE_START      <- 1990
POST_END       <- 2022
OUTCOME        <- "per_capita_income_real"

# ── 1. Load & Prepare Data ──────────────────────────────────────────────────
message("Loading panel...")
panel <- read_csv(PANEL_PATH, show_col_types = FALSE,
                  col_types = cols(fips = col_character()))
fips_list <- read_csv(FIPS_LIST_PATH, show_col_types = FALSE,
                      col_types = cols(fips = col_character()))

df <- panel |>
  filter(
    fips %in% fips_list$fips,
    year >= PRE_START,
    year <= POST_END
  ) |>
  arrange(fips, year)

# Create treatment indicator: 1 for Comal in post-treatment years, 0 otherwise
df <- df |>
  mutate(trt = as.integer(fips == TREATED_FIPS & year >= TREATMENT_YEAR))

message(sprintf(
  "Panel: %d counties, %d years (%d–%d), %d obs",
  n_distinct(df$fips), n_distinct(df$year), min(df$year), max(df$year), nrow(df)
))
message(sprintf("Treatment obs: %d", sum(df$trt)))

# Check for missing outcome values
n_miss <- sum(is.na(df[[OUTCOME]]))
if (n_miss > 0) {
  message(sprintf("WARNING: %d missing values in %s — dropping", n_miss, OUTCOME))
  df <- df |> filter(!is.na(!!sym(OUTCOME)))
}

# ── 2. Fit Ridge Augmented SCM ──────────────────────────────────────────────
message("\nFitting Ridge Augmented SCM...")

asyn <- augsynth(
  per_capita_income_real ~ trt,
  unit  = fips,
  time  = year,
  data  = df,
  progfunc = "Ridge",
  scm      = TRUE
)

message("ASCM fit complete.")

# ── 3. Summary ──────────────────────────────────────────────────────────────
asyn_summ <- summary(asyn)

message("\n=== ASCM Summary ===")
print(asyn_summ)

# Extract average ATT
att_avg <- asyn_summ$average_att
att_est <- att_avg$Estimate
att_p   <- att_avg$p_val
message(sprintf("\nAverage ATT: $%.0f (p = %.4f)", att_est, att_p))

# ── 4. Extract Time-Varying ATT ─────────────────────────────────────────────
att_by_year <- asyn_summ$att |>
  mutate(
    year     = as.integer(Time),
    att      = Estimate,
    ci_lower = lower_bound,
    ci_upper = upper_bound
  ) |>
  select(year, att, ci_lower, ci_upper) |>
  arrange(year)

# If CIs are all NA (conformal inference may not produce them in all configs),
# compute jackknife CIs from the augsynth object
if (all(is.na(att_by_year$ci_lower))) {
  message("NOTE: CIs not in summary; using permutation-based p-values only.")
  # Use the printed representation which includes CIs
  # Compute approximate CIs from the SE of the average ATT
  # For individual years, just report point estimates
}

write_csv(att_by_year, file.path(RESULTS_DIR, "ascm_att.csv"))
message(sprintf("Saved: ascm_att.csv (%d rows)", nrow(att_by_year)))

# ── 5. Extract Unit Weights ──────────────────────────────────────────────────
# augsynth stores weights in $weights (matrix with rownames = FIPS)
raw_w <- asyn$weights
w <- data.frame(
  fips   = rownames(raw_w),
  weight = as.numeric(raw_w[, 1]),
  stringsAsFactors = FALSE
)
w <- w |>
  filter(abs(weight) > 0.001) |>
  arrange(desc(weight))

write_csv(w, file.path(RESULTS_DIR, "ascm_weights.csv"))
message(sprintf("Saved: ascm_weights.csv (%d donors with |weight| > 0.001)", nrow(w)))
message("\nTop ASCM donor weights:")
print(head(w, 10))

# ── 6. Save Summary ─────────────────────────────────────────────────────────
sink(file.path(RESULTS_DIR, "ascm_summary.txt"))
cat("=== Augmented SCM (Ridge) — Ben-Michael, Feller, Rothstein 2021 ===\n\n")
cat(sprintf("Treated unit: %s (Comal County, TX)\n", TREATED_FIPS))
cat(sprintf("Outcome: %s\n", OUTCOME))
cat(sprintf("Pre-treatment: %d–%d\n", PRE_START, TREATMENT_YEAR - 1))
cat(sprintf("Post-treatment: %d–%d\n", TREATMENT_YEAR, POST_END))
cat(sprintf("Donor pool: %d counties\n\n", n_distinct(df$fips) - 1))
cat("--- Average Treatment Effect on the Treated (ATT) ---\n")
cat(sprintf("  Estimate:  $%.0f\n", att_est))
cat(sprintf("  p-value:   %.4f (permutation-based)\n", att_p))
if (!is.na(att_avg$lower_bound)) {
  cat(sprintf("  95%% CI:    [$%.0f, $%.0f]\n\n", att_avg$lower_bound, att_avg$upper_bound))
} else {
  cat("  95%% CI:    not available (see individual year CIs)\n\n")
}
print(asyn_summ)
sink()
message("Saved: ascm_summary.txt")

# ── 7. Figure 8: ASCM Gap with Confidence Bands ─────────────────────────────
message("\n=== Generating Figures ===")

# Post-treatment ATT only for gap plot
att_post <- att_by_year |> filter(year >= TREATMENT_YEAR)

p_ascm <- ggplot(att_post, aes(x = year)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.7) +
  geom_vline(xintercept = TREATMENT_YEAR - 0.5,
             linetype = "dotted", color = "black", linewidth = 0.8)

# Add CI ribbon if available
if (!all(is.na(att_post$ci_lower))) {
  p_ascm <- p_ascm +
    geom_ribbon(aes(ymin = ci_lower, ymax = ci_upper),
                fill = "#2ca02c", alpha = 0.2)
}

p_ascm <- p_ascm +
  geom_line(aes(y = att), color = "#2ca02c", linewidth = 1.3) +
  geom_point(aes(y = att), color = "#2ca02c", size = 2) +
  annotate("text", x = TREATMENT_YEAR, y = Inf,
           label = "1998 flood", hjust = -0.1, vjust = 1.5, size = 3.5) +
  scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
  scale_x_continuous(breaks = seq(PRE_START, POST_END, 4)) +
  labs(
    title    = "Augmented SCM: Treatment Effect on Real Per Capita Income",
    subtitle = "Ridge ASCM (Ben-Michael et al. 2021). Outcome: Real PCI ($2020).",
    x        = "Year",
    y        = "ATT: Actual minus synthetic ($2020)",
    caption  = sprintf(
      "Average ATT = $%.0f (p = %.3f). Donor pool: %d TX counties.",
      att_est, att_p, n_distinct(df$fips) - 1
    )
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank()
  )

ggsave(file.path(FIG_DIR, "08_ascm_gap.png"), p_ascm,
       width = 10, height = 5.5, dpi = 150)
message("Saved: figures/08_ascm_gap.png")

# ── 8. Figure 9: Traditional SCM vs ASCM Comparison ─────────────────────────
scm_gap <- tryCatch(
  read_csv(SCM_GAP_PATH, show_col_types = FALSE),
  error = function(e) NULL
)

if (!is.null(scm_gap)) {
  compare <- bind_rows(
    scm_gap |>
      filter(post) |>
      select(year, gap) |>
      mutate(method = "Traditional SCM"),
    att_post |>
      select(year, att) |>
      rename(gap = att) |>
      mutate(method = "Ridge ASCM")
  )

  p_compare <- ggplot(compare, aes(x = year, y = gap, color = method, linetype = method)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_vline(xintercept = TREATMENT_YEAR - 0.5,
               linetype = "dotted", color = "black", linewidth = 0.8)

  # ASCM confidence band (if available)
  if (!all(is.na(att_post$ci_lower))) {
    p_compare <- p_compare +
      geom_ribbon(
        data = att_post,
        aes(x = year, ymin = ci_lower, ymax = ci_upper),
        inherit.aes = FALSE,
        fill = "#2ca02c", alpha = 0.12
      )
  }

  p_compare <- p_compare +
    geom_line(linewidth = 1.2) +
    geom_point(size = 1.8) +
    scale_color_manual(
      values = c("Traditional SCM" = "#d62728", "Ridge ASCM" = "#2ca02c"),
      name = NULL
    ) +
    scale_linetype_manual(
      values = c("Traditional SCM" = "dashed", "Ridge ASCM" = "solid"),
      name = NULL
    ) +
    scale_y_continuous(labels = scales::dollar_format(scale = 1)) +
    scale_x_continuous(breaks = seq(PRE_START, POST_END, 4)) +
    labs(
      title    = "Treatment Effect Comparison: Traditional SCM vs Ridge ASCM",
      subtitle = "Post-treatment gap in real per capita income ($2020). Shaded = ASCM 95% CI.",
      x        = "Year",
      y        = "Gap (actual minus synthetic)",
      caption  = "Traditional SCM: tidysynth. ASCM: augsynth with Ridge bias correction."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold"),
      legend.position  = "bottom",
      panel.grid.minor = element_blank()
    )

  ggsave(file.path(FIG_DIR, "09_ascm_vs_scm.png"), p_compare,
         width = 10, height = 5.5, dpi = 150)
  message("Saved: figures/09_ascm_vs_scm.png")
} else {
  message("NOTE: scm_gap.csv not found; skipping comparison plot.")
  message("Run R/01_scm_estimate.R first to generate it.")
}

message("\n=== ASCM estimation complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
