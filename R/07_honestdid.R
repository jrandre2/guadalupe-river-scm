## =============================================================================
## 07_honestdid.R
## Guadalupe River SCM Recovery — HonestDiD Pre-Trend Sensitivity
##
## The ZBP DiD event study (R/03) shows divergent pre-trends in 1994-1997,
## violating the parallel trends assumption. Rambachan & Roth (2023) provide
## methods to construct robust confidence sets under violations of parallel
## trends, bounded by the maximum pre-treatment violation.
##
## Two approaches implemented:
##   1. Smoothness restriction (Delta^{SD}): post-treatment trend deviation
##      bounded by M × max pre-treatment trend deviation
##   2. Relative magnitudes (Delta^{RM}): post-treatment deviation relative
##      to pre-treatment magnitude
##
## Inputs:
##   data/processed/panels/did_zbp_panel.csv
##
## Outputs (saved to data/results/):
##   honestdid_sensitivity.csv          — Sensitivity results across M-bar
##   honestdid_summary.txt              — Interpretation
##   figures/10_honestdid_estab.png     — Sensitivity plot (establishments)
##   figures/11_honestdid_rm_estab.png  — Relative magnitudes plot
##
## R packages required:
##   fixest, HonestDiD, tidyverse, ggplot2
##
## Reference:
##   Rambachan, A. & Roth, J. (2023). "A More Credible Approach to Parallel
##   Trends." Review of Economic Studies, 90(5), 2555-2591.
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(fixest)
  library(HonestDiD)
  library(dplyr)
  library(readr)
  library(ggplot2)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
PANEL_PATH   <- file.path(PROJECT_ROOT, "data/processed/panels/did_zbp_panel.csv")
RESULTS_DIR  <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR      <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ───────────────────────────────────────────────────────────────
REF_YEAR  <- 1998   # Reference year for event study
ALPHA     <- 0.05   # Significance level for confidence sets

# ── 1. Load Data & Fit Event Study ──────────────────────────────────────────
message("Loading ZBP panel...")
df <- read_csv(PANEL_PATH, show_col_types = FALSE) |>
  mutate(
    zip      = as.character(zip),
    year     = as.integer(year),
    treated  = as.integer(treated),
    post     = as.integer(post),
    ln_estab = as.numeric(ln_estab),
    ln_emp   = as.numeric(ln_emp),
    ln_payann_real = as.numeric(ln_payann_real)
  )

message(sprintf("Panel: %d ZIPs, %d years, %d obs", n_distinct(df$zip),
                n_distinct(df$year), nrow(df)))

# Fit event study (same specification as R/03)
message("\nFitting event study: ln(establishments)...")
es_estab <- feols(
  ln_estab ~ i(year, treated, ref = REF_YEAR) | zip + year,
  data    = df,
  cluster = ~zip
)

# ── 2. Extract Coefficients for HonestDiD ───────────────────────────────────
# HonestDiD requires:
#   betahat: vector of event study coefficients (pre then post, chronological)
#   sigma:   variance-covariance matrix
#   numPrePeriods:  number of pre-treatment period coefficients
#   numPostPeriods: number of post-treatment period coefficients

# Get coefficient names and sort by year
ct <- as.data.frame(coeftable(es_estab))
ct$term <- rownames(ct)
ct$year_num <- as.integer(gsub(".*year::(-?\\d+):.*", "\\1", ct$term))
ct <- ct |> arrange(year_num)

# Separate pre and post
pre_idx  <- which(ct$year_num < REF_YEAR + 1)  # years < 1999 (pre-treatment)
post_idx <- which(ct$year_num >= REF_YEAR + 1)  # years >= 1999 (post-treatment)

# Note: ref year (1998) is excluded by construction
n_pre  <- length(pre_idx)
n_post <- length(post_idx)

message(sprintf("Pre-treatment periods: %d (%s)",
                n_pre, paste(ct$year_num[pre_idx], collapse = ", ")))
message(sprintf("Post-treatment periods: %d (%d–%d)",
                n_post, min(ct$year_num[post_idx]), max(ct$year_num[post_idx])))

# Extract ordered coefficients and vcov
ordered_idx <- c(pre_idx, post_idx)
betahat <- coef(es_estab)[ct$term[ordered_idx]]
sigma   <- vcov(es_estab)[ct$term[ordered_idx], ct$term[ordered_idx]]

message(sprintf("\nCoefficient vector length: %d (pre=%d, post=%d)",
                length(betahat), n_pre, n_post))

# ── 3. Smoothness Restriction (Delta^SD) ────────────────────────────────────
message("\n=== Smoothness Restriction Sensitivity (Delta^SD) ===")
message("M controls max deviation from linear extrapolation of pre-trends.")
message("M=0: exact parallel trends. Larger M: more deviation allowed.\n")

# Choose M values based on the observed pre-trend violations
# Max absolute second difference in pre-period betas gives a natural scale
if (n_pre >= 2) {
  pre_betas <- unname(betahat[1:n_pre])
  if (n_pre >= 3) {
    second_diffs <- abs(diff(diff(pre_betas)))
    max_pre_violation <- max(second_diffs)
  } else {
    max_pre_violation <- abs(diff(pre_betas))
  }
  message(sprintf("Max pre-treatment 2nd difference: %.4f", max_pre_violation))
} else {
  max_pre_violation <- 0.1
}

Mvec <- c(0, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5,
          round(max_pre_violation, 3),
          round(max_pre_violation * 2, 3))
Mvec <- sort(unique(Mvec))

message(sprintf("Testing M values: %s", paste(Mvec, collapse = ", ")))

# Run smoothness restriction sensitivity
sd_results <- tryCatch({
  createSensitivityResults(
    betahat        = betahat,
    sigma          = sigma,
    numPrePeriods  = n_pre,
    numPostPeriods = n_post,
    Mvec           = Mvec,
    alpha          = ALPHA
  )
}, error = function(e) {
  message(sprintf("Delta^SD failed: %s", conditionMessage(e)))
  NULL
})

if (!is.null(sd_results)) {
  message("\nSmoothness restriction results:")
  print(sd_results)
}

# ── 4. Relative Magnitudes (Delta^RM) ───────────────────────────────────────
message("\n=== Relative Magnitudes Sensitivity (Delta^RM) ===")
message("Mbar bounds the ratio of post-treatment trend violation to max pre-trend violation.")
message("Mbar=1: post violations same magnitude as pre. Mbar=2: up to 2x pre-violations.\n")

Mbarvec <- seq(0, 2, by = 0.25)

rm_results <- tryCatch({
  createSensitivityResults_relativeMagnitudes(
    betahat        = betahat,
    sigma          = sigma,
    numPrePeriods  = n_pre,
    numPostPeriods = n_post,
    Mbarvec        = Mbarvec,
    alpha          = ALPHA
  )
}, error = function(e) {
  message(sprintf("Delta^RM failed: %s", conditionMessage(e)))
  NULL
})

if (!is.null(rm_results)) {
  message("\nRelative magnitudes results:")
  print(rm_results)
}

# ── 5. Save Results ─────────────────────────────────────────────────────────
# Combine into a single CSV
all_sensitivity <- data.frame()

if (!is.null(sd_results)) {
  sd_df <- as.data.frame(sd_results)
  sd_df$method <- "Smoothness (Delta^SD)"
  sd_df$outcome <- "ln_estab"
  all_sensitivity <- bind_rows(all_sensitivity, sd_df)
}

if (!is.null(rm_results)) {
  rm_df <- as.data.frame(rm_results)
  rm_df$method <- "Relative Magnitudes (Delta^RM)"
  rm_df$outcome <- "ln_estab"
  all_sensitivity <- bind_rows(all_sensitivity, rm_df)
}

if (nrow(all_sensitivity) > 0) {
  write_csv(all_sensitivity, file.path(RESULTS_DIR, "honestdid_sensitivity.csv"))
  message(sprintf("\nSaved: honestdid_sensitivity.csv (%d rows)", nrow(all_sensitivity)))
}

# ── 6. Summary Text ─────────────────────────────────────────────────────────
sink(file.path(RESULTS_DIR, "honestdid_summary.txt"))
cat("=== HonestDiD Sensitivity Analysis (Rambachan & Roth 2023) ===\n\n")
cat(sprintf("Outcome: ln(establishments)\n"))
cat(sprintf("Event study: ln_estab ~ i(year, treated, ref=%d) | zip + year\n", REF_YEAR))
cat(sprintf("Pre-treatment periods: %d (%s)\n",
            n_pre, paste(ct$year_num[pre_idx], collapse = ", ")))
cat(sprintf("Post-treatment periods: %d (%d–%d)\n",
            n_post, min(ct$year_num[post_idx]), max(ct$year_num[post_idx])))
cat(sprintf("Significance level: %.2f\n\n", ALPHA))

cat("Pre-treatment coefficients (year: estimate):\n")
for (i in pre_idx) {
  cat(sprintf("  %d: %+.4f (SE=%.4f)\n", ct$year_num[i], ct$Estimate[i], ct$`Std. Error`[i]))
}
cat(sprintf("\nMax pre-treatment 2nd difference: %.4f\n\n", max_pre_violation))

if (!is.null(sd_results)) {
  cat("--- Smoothness Restriction (Delta^SD) ---\n")
  cat("M controls how much post-treatment trend can deviate from\n")
  cat("linear extrapolation of pre-trends.\n\n")
  print(sd_results)
}

if (!is.null(rm_results)) {
  cat("\n--- Relative Magnitudes (Delta^RM) ---\n")
  cat("Mbar bounds the ratio of post-treatment trend violation\n")
  cat("to max pre-treatment trend violation.\n\n")
  print(rm_results)
}

cat("\n\n--- Interpretation ---\n")
cat("If confidence sets include 0 even at M/Mbar > 0, the null of no\n")
cat("treatment effect cannot be rejected even allowing for some degree\n")
cat("of parallel trends violation. This strengthens the null finding.\n\n")
cat("If confidence sets exclude 0 only at M/Mbar = 0 (exact parallel trends),\n")
cat("any significance is fragile to even minor trends violations.\n")
sink()
message("Saved: honestdid_summary.txt")

# ── 7. Figures ──────────────────────────────────────────────────────────────
message("\n=== Generating Figures ===")

# Figure 10: Smoothness restriction sensitivity plot
if (!is.null(sd_results)) {
  sd_plot_data <- as.data.frame(sd_results)

  p_sd <- ggplot(sd_plot_data, aes(x = M)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_ribbon(aes(ymin = lb, ymax = ub),
                fill = "#9467bd", alpha = 0.2) +
    geom_line(aes(y = lb), color = "#9467bd", linewidth = 0.8) +
    geom_line(aes(y = ub), color = "#9467bd", linewidth = 0.8) +
    labs(
      title    = expression(paste("HonestDiD Sensitivity: Smoothness Restriction (", Delta^SD, ")")),
      subtitle = "Robust 95% CI for treatment effect on ln(establishments).\nCI widens as M increases (more parallel trends violation allowed).",
      x        = expression(paste("M (max post-treatment deviation from pre-trend extrapolation)")),
      y        = "Treatment effect on ln(establishments)",
      caption  = "Rambachan & Roth (2023). M=0: exact parallel trends."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold"),
      panel.grid.minor = element_blank()
    )

  ggsave(file.path(FIG_DIR, "10_honestdid_estab.png"), p_sd,
         width = 10, height = 5.5, dpi = 150)
  message("Saved: figures/10_honestdid_estab.png")
}

# Figure 11: Relative magnitudes sensitivity plot
if (!is.null(rm_results)) {
  rm_plot_data <- as.data.frame(rm_results)

  p_rm <- ggplot(rm_plot_data, aes(x = Mbar)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_ribbon(aes(ymin = lb, ymax = ub),
                fill = "#e377c2", alpha = 0.2) +
    geom_line(aes(y = lb), color = "#e377c2", linewidth = 0.8) +
    geom_line(aes(y = ub), color = "#e377c2", linewidth = 0.8) +
    geom_vline(xintercept = 1, linetype = "dashed", color = "gray30", linewidth = 0.5) +
    annotate("text", x = 1.05, y = Inf,
             label = "Mbar=1\n(violations = pre-trend)",
             hjust = 0, vjust = 1.3, size = 3, color = "gray30") +
    labs(
      title    = expression(paste("HonestDiD Sensitivity: Relative Magnitudes (", Delta^RM, ")")),
      subtitle = "Robust 95% CI for treatment effect on ln(establishments).\nMbar=1: post-treatment violations can equal pre-treatment violations.",
      x        = expression(paste(bar(M), " (ratio of post to pre-treatment trend violation)")),
      y        = "Treatment effect on ln(establishments)",
      caption  = "Rambachan & Roth (2023). Dashed line: Mbar=1 (equal magnitude violations)."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold"),
      panel.grid.minor = element_blank()
    )

  ggsave(file.path(FIG_DIR, "11_honestdid_rm_estab.png"), p_rm,
         width = 10, height = 5.5, dpi = 150)
  message("Saved: figures/11_honestdid_rm_estab.png")
}

# ── Summary ──────────────────────────────────────────────────────────────────
message("\n=== HonestDiD Sensitivity Analysis Complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
if (!is.null(sd_results)) {
  sd_df_final <- as.data.frame(sd_results)
  sd_at_0 <- sd_df_final[sd_df_final$M == 0, ]
  message(sprintf("\nAt M=0 (exact parallel trends): CI [%.4f, %.4f]",
                  sd_at_0$lb, sd_at_0$ub))
  includes_zero <- any(sd_df_final$lb <= 0 & sd_df_final$ub >= 0)
  message(sprintf("Zero in CI across all M values: %s", includes_zero))
}
message("\nInterpretation: If zero remains in CI even at larger M/Mbar values,")
message("the null finding is robust to parallel trends violations.")
