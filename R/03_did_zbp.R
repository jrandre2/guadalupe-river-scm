## =============================================================================
## 03_did_zbp.R
## Guadalupe River SCM Recovery — Within-County DiD at ZIP Level
##
## Difference-in-differences using ZIP Code Business Patterns (ZBP) data.
## Compares heavily-flooded ZIPs (78130, 78132, 78163, 78131) against
## minimally-affected ZIPs within Comal County and adjacent counties.
##
## Treatment assignment based on NFIP flood insurance payouts from Oct–Nov 1998:
##   Treated (>$500K): 78130, 78131, 78132, 78163
##   Control (<$130K): 78133, 78266, 78070, 78006, 78013, 78015, 78610, 78620, 78640
##
## Inputs:
##   data/processed/panels/did_zbp_panel.csv
##
## Outputs (saved to data/results/):
##   did_binary_results.txt     — Binary DiD regression tables
##   did_event_study.csv        — Event study coefficients + CIs
##   did_intensity_results.txt  — Continuous intensity DiD
##   figures/did_event_study_*.png — Event study plots
##
## R packages required:
##   fixest, tidyverse, ggplot2
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(fixest)
  library(dplyr)
  library(readr)
  library(ggplot2)
  library(tidyr)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
PANEL_PATH   <- file.path(PROJECT_ROOT, "data/processed/panels/did_zbp_panel.csv")
RESULTS_DIR  <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR      <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ───────────────────────────────────────────────────────────────
TREATMENT_YEAR <- 1999   # First post-treatment year (flood: Oct 1998)
REF_YEAR       <- 1998   # Reference year for event study

# ── 1. Load Data ─────────────────────────────────────────────────────────────
message("Loading DiD panel...")
df <- read_csv(PANEL_PATH, show_col_types = FALSE)

# Ensure proper types
df <- df |>
  mutate(
    zip       = as.character(zip),
    year      = as.integer(year),
    treated   = as.integer(treated),
    post      = as.integer(post),
    comal     = as.integer(comal),
    estab     = as.numeric(estab),
    emp       = as.numeric(emp),
    payann_real = as.numeric(payann_real),
    ln_estab  = as.numeric(ln_estab),
    ln_emp    = as.numeric(ln_emp),
    ln_payann_real = as.numeric(ln_payann_real),
    intensity = as.numeric(intensity)
  )

message(sprintf(
  "Panel: %d ZIPs (%d treated, %d control), %d years (%d–%d), %d obs",
  n_distinct(df$zip),
  n_distinct(df$zip[df$treated == 1]),
  n_distinct(df$zip[df$treated == 0]),
  n_distinct(df$year), min(df$year), max(df$year),
  nrow(df)
))

# ── 2. Binary DiD (TWFE) ────────────────────────────────────────────────────
message("\n=== Binary DiD (Two-Way Fixed Effects) ===")

# Main specifications: log outcomes ~ treated × post | ZIP + year FE
did_estab <- feols(ln_estab ~ treated:post | zip + year, data = df)
did_emp   <- feols(ln_emp   ~ treated:post | zip + year, data = df)
did_pay   <- feols(ln_payann_real ~ treated:post | zip + year, data = df)

message("\n--- Establishments ---")
summary(did_estab)

message("\n--- Employment ---")
summary(did_emp)

message("\n--- Payroll (real 2020$) ---")
summary(did_pay)

# Save results
sink(file.path(RESULTS_DIR, "did_binary_results.txt"))
cat("=== Binary DiD: Treated × Post | ZIP + Year FE ===\n\n")
cat("--- log(Establishments) ---\n")
print(summary(did_estab))
cat("\n--- log(Employment) ---\n")
print(summary(did_emp))
cat("\n--- log(Payroll, real 2020$) ---\n")
print(summary(did_pay))
sink()

message("Saved: did_binary_results.txt")

# ── 3. Event Study ──────────────────────────────────────────────────────────
message("\n=== Event Study (Dynamic Treatment Effects) ===")

# Event study: interact year × treated, ref = 1998
es_estab <- feols(ln_estab ~ i(year, treated, ref = REF_YEAR) | zip + year, data = df)
es_emp   <- feols(ln_emp   ~ i(year, treated, ref = REF_YEAR) | zip + year, data = df)
es_pay   <- feols(ln_payann_real ~ i(year, treated, ref = REF_YEAR) | zip + year, data = df)

# Extract event study coefficients
extract_es <- function(model, outcome_name) {
  ct <- as.data.frame(coeftable(model))
  ct$term <- rownames(ct)
  ct$year <- as.integer(gsub(".*year::(-?\\d+):.*", "\\1", ct$term))
  ct$outcome <- outcome_name
  ct |>
    rename(estimate = Estimate, se = `Std. Error`, t_val = `t value`, p_val = `Pr(>|t|)`) |>
    mutate(
      ci_lo = estimate - 1.96 * se,
      ci_hi = estimate + 1.96 * se
    ) |>
    select(outcome, year, estimate, se, ci_lo, ci_hi, t_val, p_val)
}

es_df <- bind_rows(
  extract_es(es_estab, "ln_estab"),
  extract_es(es_emp,   "ln_emp"),
  extract_es(es_pay,   "ln_payann_real")
)

# Add reference year row (coefficient = 0 by construction)
ref_rows <- data.frame(
  outcome = c("ln_estab", "ln_emp", "ln_payann_real"),
  year = REF_YEAR, estimate = 0, se = 0, ci_lo = 0, ci_hi = 0, t_val = NA, p_val = NA
)
es_df <- bind_rows(es_df, ref_rows) |> arrange(outcome, year)

write_csv(es_df, file.path(RESULTS_DIR, "did_event_study.csv"))
message("Saved: did_event_study.csv")

# Joint test of pre-treatment coefficients = 0 (parallel trends diagnostic)
pre_coefs_estab <- grep("year::(199[4-7]|1998):", names(coef(es_estab)), value = TRUE)
pre_coefs_estab <- setdiff(pre_coefs_estab, grep("1998", pre_coefs_estab, value = TRUE))
if (length(pre_coefs_estab) > 0) {
  pt_test <- wald(es_estab, pre_coefs_estab)
  message(sprintf("\nJoint pre-trend test (H0: all pre-treatment coefficients = 0):"))
  message(sprintf("  F(%d, %.0f) = %.3f, p = %.4f",
                  pt_test$df1, pt_test$df2, pt_test$stat, pt_test$p))
  if (pt_test$p < 0.10) {
    message("  NOTE: Pre-trends may be violated. See R/07_honestdid.R for robust inference.")
  }
}

# Print key coefficients
message("\n--- Event Study: log(Establishments) ---")
es_estab_sub <- es_df |> filter(outcome == "ln_estab") |> arrange(year)
for (i in seq_len(nrow(es_estab_sub))) {
  r <- es_estab_sub[i, ]
  sig <- ifelse(!is.na(r$p_val) & r$p_val < 0.05, " *", "")
  message(sprintf("  %d: %+.3f (%.3f) [%.3f, %.3f]%s",
                  r$year, r$estimate, r$se, r$ci_lo, r$ci_hi, sig))
}

# ── 4. Event Study Plots ────────────────────────────────────────────────────
message("\n=== Generating Event Study Plots ===")

plot_event_study <- function(data, outcome_name, title, filename) {
  pdata <- data |> filter(outcome == outcome_name)

  p <- ggplot(pdata, aes(x = year, y = estimate)) +
    geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
    geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dotted", color = "black", linewidth = 0.7) +
    geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), fill = "#d62728", alpha = 0.15) +
    geom_point(color = "#d62728", size = 2.5) +
    geom_line(color = "#d62728", linewidth = 0.8) +
    annotate("text", x = REF_YEAR + 0.5, y = Inf,
             label = "1998 flood", hjust = -0.1, vjust = 1.5, size = 3.5) +
    scale_x_continuous(breaks = seq(1994, 2020, 2)) +
    labs(
      title    = title,
      subtitle = sprintf("Ref. year = %d. Shaded region = 95%% CI. ZIP + year FE.", REF_YEAR),
      x        = "Year",
      y        = "Coefficient (treated × year)",
      caption  = "Source: Census ZBP. Treatment: NFIP payouts > $500K in Oct–Nov 1998."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold"),
      panel.grid.minor = element_blank()
    )

  ggsave(file.path(FIG_DIR, filename), p, width = 10, height = 5.5, dpi = 150)
  message(sprintf("Saved: figures/%s", filename))
}

plot_event_study(es_df, "ln_estab",
                 "Event Study: Flood Impact on Establishments",
                 "did_event_study_estab.png")

plot_event_study(es_df, "ln_emp",
                 "Event Study: Flood Impact on Employment",
                 "did_event_study_emp.png")

plot_event_study(es_df, "ln_payann_real",
                 "Event Study: Flood Impact on Payroll (2020$)",
                 "did_event_study_payroll.png")

# ── 5. Continuous Intensity ──────────────────────────────────────────────────
message("\n=== Continuous Intensity DiD ===")

int_estab <- feols(ln_estab ~ intensity:post | zip + year, data = df)
int_emp   <- feols(ln_emp   ~ intensity:post | zip + year, data = df)
int_pay   <- feols(ln_payann_real ~ intensity:post | zip + year, data = df)

message("\n--- Intensity: log(Establishments) ---")
summary(int_estab)

message("\n--- Intensity: log(Employment) ---")
summary(int_emp)

message("\n--- Intensity: log(Payroll) ---")
summary(int_pay)

sink(file.path(RESULTS_DIR, "did_intensity_results.txt"))
cat("=== Continuous Intensity DiD: ln(NFIP_paid+1) × Post | ZIP + Year FE ===\n\n")
cat("--- log(Establishments) ---\n")
print(summary(int_estab))
cat("\n--- log(Employment) ---\n")
print(summary(int_emp))
cat("\n--- log(Payroll, real 2020$) ---\n")
print(summary(int_pay))
sink()

message("Saved: did_intensity_results.txt")

# ── 6. Robustness: Comal-Only Panel ─────────────────────────────────────────
message("\n=== Robustness: Comal County ZIPs Only (7 ZIPs) ===")

df_comal <- df |> filter(comal == 1)
message(sprintf("Comal-only panel: %d ZIPs, %d obs", n_distinct(df_comal$zip), nrow(df_comal)))

rob_estab <- feols(ln_estab ~ treated:post | zip + year, data = df_comal)
rob_emp   <- feols(ln_emp   ~ treated:post | zip + year, data = df_comal)
rob_pay   <- feols(ln_payann_real ~ treated:post | zip + year, data = df_comal)

message("\nComal-only Binary DiD:")
message(sprintf("  Establishments: coef = %.4f (se = %.4f, p = %.4f)",
                coef(rob_estab), se(rob_estab), pvalue(rob_estab)))
message(sprintf("  Employment:     coef = %.4f (se = %.4f, p = %.4f)",
                coef(rob_emp), se(rob_emp), pvalue(rob_emp)))
message(sprintf("  Payroll:        coef = %.4f (se = %.4f, p = %.4f)",
                coef(rob_pay), se(rob_pay), pvalue(rob_pay)))

# Comal-only event study for establishments
es_comal <- feols(ln_estab ~ i(year, treated, ref = REF_YEAR) | zip + year, data = df_comal)
es_comal_df <- extract_es(es_comal, "ln_estab")
es_comal_df <- bind_rows(es_comal_df,
  data.frame(outcome = "ln_estab", year = REF_YEAR, estimate = 0, se = 0,
             ci_lo = 0, ci_hi = 0, t_val = NA, p_val = NA)
) |> arrange(year)

# Combined event study plot: full panel vs Comal-only
es_full <- es_df |> filter(outcome == "ln_estab") |> mutate(panel = "Full (13 ZIPs)")
es_rob  <- es_comal_df |> mutate(panel = "Comal only (7 ZIPs)")
es_compare <- bind_rows(es_full, es_rob)

p_compare <- ggplot(es_compare, aes(x = year, y = estimate, color = panel, fill = panel)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
  geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dotted", color = "black", linewidth = 0.7) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.1, color = NA) +
  geom_point(size = 2, position = position_dodge(width = 0.4)) +
  geom_line(linewidth = 0.7, position = position_dodge(width = 0.4)) +
  scale_color_manual(values = c("Full (13 ZIPs)" = "#d62728", "Comal only (7 ZIPs)" = "#1f77b4")) +
  scale_fill_manual(values = c("Full (13 ZIPs)" = "#d62728", "Comal only (7 ZIPs)" = "#1f77b4")) +
  scale_x_continuous(breaks = seq(1994, 2020, 2)) +
  labs(
    title    = "Event Study Robustness: Full Panel vs Comal County Only",
    subtitle = "Outcome: log(Establishments). Ref. year = 1998.",
    x = "Year", y = "Coefficient (treated × year)",
    color = NULL, fill = NULL,
    caption = "Source: Census ZBP. Treatment: NFIP payouts > $500K in Oct–Nov 1998."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

ggsave(file.path(FIG_DIR, "did_event_study_robustness.png"), p_compare,
       width = 10, height = 5.5, dpi = 150)
message("Saved: figures/did_event_study_robustness.png")

# ── 7. Summary ───────────────────────────────────────────────────────────────
message("\n=== DiD Estimation Complete ===")
message(sprintf("Results saved to: %s", RESULTS_DIR))
message("\nKey results:")
message(sprintf("  Binary DiD (establishments): coef = %.4f, se = %.4f, p = %.4f",
                coef(did_estab), se(did_estab), pvalue(did_estab)))
message(sprintf("  Binary DiD (employment):     coef = %.4f, se = %.4f, p = %.4f",
                coef(did_emp), se(did_emp), pvalue(did_emp)))
message(sprintf("  Binary DiD (payroll):        coef = %.4f, se = %.4f, p = %.4f",
                coef(did_pay), se(did_pay), pvalue(did_pay)))
message("\nInterpretation: coefficient on treated×post gives the average treatment")
message("effect of flood exposure on log(outcome), controlling for ZIP and year FE.")
message("Positive = flooded ZIPs grew faster; Negative = flooded ZIPs grew slower.")
