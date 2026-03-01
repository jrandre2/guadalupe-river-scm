## =============================================================================
## 04_did_hpi.R
## Guadalupe River SCM Recovery — Housing Value DiD at ZIP & Tract Level
##
## Difference-in-differences on FHFA House Price Index (repeat-sales).
## Adapts the donor-based counterfactual methodology from the Longitudinal
## Housing Recovery project (Hurricane Sandy) to measure flood impact on
## housing markets at the ZIP code and census tract level.
##
## Inputs:
##   data/processed/panels/did_hpi_zip_panel.csv
##   data/processed/panels/did_hpi_tract_panel.csv
##
## Outputs:
##   data/results/did_hpi_binary_results.txt
##   data/results/did_hpi_event_study.csv
##   data/results/figures/did_hpi_event_study.png
##   data/results/figures/did_hpi_event_study_tract.png
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
RESULTS_DIR  <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR      <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

REF_YEAR <- 1998

# ══════════════════════════════════════════════════════════════════════════════
# PART A: ZIP-LEVEL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

message("=== PART A: ZIP-Level HPI DiD ===")

df <- read_csv(
  file.path(PROJECT_ROOT, "data/processed/panels/did_hpi_zip_panel.csv"),
  show_col_types = FALSE
) |>
  mutate(
    zip     = as.character(zip),
    year    = as.integer(year),
    treated = as.integer(treated),
    post    = as.integer(post),
    ln_hpi  = as.numeric(ln_hpi),
    intensity = as.numeric(intensity)
  )

message(sprintf(
  "ZIP panel: %d ZIPs (%d treated, %d control), %d years, %d obs",
  n_distinct(df$zip),
  n_distinct(df$zip[df$treated == 1]),
  n_distinct(df$zip[df$treated == 0]),
  n_distinct(df$year),
  nrow(df)
))

# ── Binary DiD ───────────────────────────────────────────────────────────────
message("\n--- Binary DiD: log(HPI) ~ treated:post | zip + year ---")
did_hpi <- feols(ln_hpi ~ treated:post | zip + year, data = df)
summary(did_hpi)

# ── Continuous Intensity ─────────────────────────────────────────────────────
message("\n--- Intensity DiD: log(HPI) ~ intensity:post | zip + year ---")
int_hpi <- feols(ln_hpi ~ intensity:post | zip + year, data = df)
summary(int_hpi)

# Save results
sink(file.path(RESULTS_DIR, "did_hpi_binary_results.txt"))
cat("=== ZIP-Level HPI DiD Results ===\n\n")
cat("--- Binary DiD: log(HPI) ~ treated:post | zip + year FE ---\n")
print(summary(did_hpi))
cat("\n--- Intensity DiD: log(HPI) ~ intensity:post | zip + year FE ---\n")
print(summary(int_hpi))
sink()
message("Saved: did_hpi_binary_results.txt")

# ── Event Study ──────────────────────────────────────────────────────────────
message("\n--- Event Study: log(HPI) ~ i(year, treated, ref=1998) | zip + year ---")
es_hpi <- feols(ln_hpi ~ i(year, treated, ref = REF_YEAR) | zip + year, data = df)

# Extract coefficients
ct <- as.data.frame(coeftable(es_hpi))
ct$term <- rownames(ct)
ct$year <- as.integer(gsub(".*year::(-?\\d+):.*", "\\1", ct$term))
ct$outcome <- "ln_hpi"
ct <- ct |>
  rename(estimate = Estimate, se = `Std. Error`, t_val = `t value`, p_val = `Pr(>|t|)`) |>
  mutate(ci_lo = estimate - 1.96 * se, ci_hi = estimate + 1.96 * se) |>
  select(outcome, year, estimate, se, ci_lo, ci_hi, t_val, p_val)

# Add reference year
ct <- bind_rows(ct, data.frame(
  outcome = "ln_hpi", year = REF_YEAR, estimate = 0, se = 0,
  ci_lo = 0, ci_hi = 0, t_val = NA, p_val = NA
)) |> arrange(year)

ct$level <- "ZIP"
write_csv(ct, file.path(RESULTS_DIR, "did_hpi_event_study.csv"))
message("Saved: did_hpi_event_study.csv")

# Print key years
message("\nEvent study coefficients (ZIP-level):")
for (i in seq_len(nrow(ct))) {
  r <- ct[i, ]
  sig <- ifelse(!is.na(r$p_val) & r$p_val < 0.05, " *", "")
  if (!is.na(r$p_val) & r$p_val < 0.01) sig <- " **"
  message(sprintf("  %d: %+.4f (%.4f) [%.4f, %.4f]%s",
                  r$year, r$estimate, r$se, r$ci_lo, r$ci_hi, sig))
}

# ── ZIP Event Study Plot ─────────────────────────────────────────────────────
p_zip <- ggplot(ct, aes(x = year, y = estimate)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
  geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dotted", color = "black", linewidth = 0.7) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), fill = "#d62728", alpha = 0.15) +
  geom_point(color = "#d62728", size = 2.5) +
  geom_line(color = "#d62728", linewidth = 0.8) +
  annotate("text", x = REF_YEAR + 0.5, y = Inf,
           label = "1998 flood", hjust = -0.1, vjust = 1.5, size = 3.5) +
  scale_x_continuous(breaks = seq(1990, 2024, 2)) +
  labs(
    title    = "Event Study: Flood Impact on Housing Values (ZIP-Level HPI)",
    subtitle = sprintf("Ref. year = %d. 3 treated ZIPs, 9 controls. Shaded = 95%% CI.", REF_YEAR),
    x        = "Year",
    y        = "Coefficient (treated × year)",
    caption  = "Source: FHFA All-Transactions HPI. Treatment: NFIP payouts > $500K in Oct–Nov 1998."
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(FIG_DIR, "did_hpi_event_study.png"), p_zip, width = 11, height = 5.5, dpi = 150)
message("Saved: figures/did_hpi_event_study.png")


# ══════════════════════════════════════════════════════════════════════════════
# PART B: CENSUS TRACT-LEVEL ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════

message("\n=== PART B: Census Tract-Level HPI DiD ===")

dt <- read_csv(
  file.path(PROJECT_ROOT, "data/processed/panels/did_hpi_tract_panel.csv"),
  show_col_types = FALSE
) |>
  mutate(
    tract   = as.character(tract),
    year    = as.integer(year),
    treated = as.integer(treated),
    post    = as.integer(post),
    ln_hpi  = as.numeric(ln_hpi),
    intensity = as.numeric(intensity)
  )

message(sprintf(
  "Tract panel: %d tracts (%d treated, %d control), %d obs",
  n_distinct(dt$tract),
  n_distinct(dt$tract[dt$treated == 1]),
  n_distinct(dt$tract[dt$treated == 0]),
  nrow(dt)
))

# ── Binary DiD (Tract) ──────────────────────────────────────────────────────
message("\n--- Binary DiD (tract): log(HPI) ~ treated:post | tract + year ---")
did_tract <- feols(ln_hpi ~ treated:post | tract + year, data = dt)
summary(did_tract)

# ── Intensity DiD (Tract) ───────────────────────────────────────────────────
message("\n--- Intensity DiD (tract): log(HPI) ~ intensity:post | tract + year ---")
int_tract <- feols(ln_hpi ~ intensity:post | tract + year, data = dt)
summary(int_tract)

# Save tract results
sink(file.path(RESULTS_DIR, "did_hpi_binary_results.txt"), append = TRUE)
cat("\n\n=== Census Tract-Level HPI DiD Results ===\n\n")
cat("--- Binary DiD: log(HPI) ~ treated:post | tract + year FE ---\n")
print(summary(did_tract))
cat("\n--- Intensity DiD: log(HPI) ~ intensity:post | tract + year FE ---\n")
print(summary(int_tract))
sink()

# ── Event Study (Tract) ─────────────────────────────────────────────────────
message("\n--- Event Study (tract): log(HPI) ~ i(year, treated, ref=1998) | tract + year ---")
es_tract <- feols(ln_hpi ~ i(year, treated, ref = REF_YEAR) | tract + year, data = dt)

ct_tract <- as.data.frame(coeftable(es_tract))
ct_tract$term <- rownames(ct_tract)
ct_tract$year <- as.integer(gsub(".*year::(-?\\d+):.*", "\\1", ct_tract$term))
ct_tract$outcome <- "ln_hpi"
ct_tract <- ct_tract |>
  rename(estimate = Estimate, se = `Std. Error`, t_val = `t value`, p_val = `Pr(>|t|)`) |>
  mutate(ci_lo = estimate - 1.96 * se, ci_hi = estimate + 1.96 * se) |>
  select(outcome, year, estimate, se, ci_lo, ci_hi, t_val, p_val)

ct_tract <- bind_rows(ct_tract, data.frame(
  outcome = "ln_hpi", year = REF_YEAR, estimate = 0, se = 0,
  ci_lo = 0, ci_hi = 0, t_val = NA, p_val = NA
)) |> arrange(year)
ct_tract$level <- "Tract"

# Append to ZIP event study CSV
es_both <- bind_rows(ct |> mutate(level = "ZIP"), ct_tract)
write_csv(es_both, file.path(RESULTS_DIR, "did_hpi_event_study.csv"))

# Print key years
message("\nEvent study coefficients (tract-level):")
for (i in seq_len(nrow(ct_tract))) {
  r <- ct_tract[i, ]
  sig <- ifelse(!is.na(r$p_val) & r$p_val < 0.05, " *", "")
  if (!is.na(r$p_val) & r$p_val < 0.01) sig <- " **"
  message(sprintf("  %d: %+.4f (%.4f) [%.4f, %.4f]%s",
                  r$year, r$estimate, r$se, r$ci_lo, r$ci_hi, sig))
}

# ── Tract Event Study Plot ───────────────────────────────────────────────────
p_tract <- ggplot(ct_tract, aes(x = year, y = estimate)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
  geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dotted", color = "black", linewidth = 0.7) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), fill = "#1f77b4", alpha = 0.15) +
  geom_point(color = "#1f77b4", size = 2.5) +
  geom_line(color = "#1f77b4", linewidth = 0.8) +
  annotate("text", x = REF_YEAR + 0.5, y = Inf,
           label = "1998 flood", hjust = -0.1, vjust = 1.5, size = 3.5) +
  scale_x_continuous(breaks = seq(1992, 2024, 2)) +
  labs(
    title    = "Event Study: Flood Impact on Housing Values (Census Tract-Level)",
    subtitle = sprintf("Ref. year = %d. 2 treated tracts, 30 controls (Comal+Hays+Kendall).", REF_YEAR),
    x        = "Year",
    y        = "Coefficient (treated × year)",
    caption  = "Source: FHFA Census Tract HPI. Treatment: NFIP payouts > $200K in Oct–Nov 1998."
  ) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"), panel.grid.minor = element_blank())

ggsave(file.path(FIG_DIR, "did_hpi_event_study_tract.png"), p_tract, width = 11, height = 5.5, dpi = 150)
message("Saved: figures/did_hpi_event_study_tract.png")

# ── Combined ZIP + Tract Plot ────────────────────────────────────────────────
p_both <- ggplot(es_both, aes(x = year, y = estimate, color = level, fill = level)) +
  geom_hline(yintercept = 0, color = "gray50", linewidth = 0.5) +
  geom_vline(xintercept = REF_YEAR + 0.5, linetype = "dotted", color = "black", linewidth = 0.7) +
  geom_ribbon(aes(ymin = ci_lo, ymax = ci_hi), alpha = 0.1, color = NA) +
  geom_point(size = 2, position = position_dodge(width = 0.4)) +
  geom_line(linewidth = 0.7, position = position_dodge(width = 0.4)) +
  scale_color_manual(values = c("ZIP" = "#d62728", "Tract" = "#1f77b4")) +
  scale_fill_manual(values = c("ZIP" = "#d62728", "Tract" = "#1f77b4")) +
  scale_x_continuous(breaks = seq(1990, 2024, 2)) +
  labs(
    title    = "Housing Value Event Study: ZIP vs Census Tract Resolution",
    subtitle = "Outcome: log(HPI). Ref. year = 1998.",
    x = "Year", y = "Coefficient (treated × year)",
    color = "Level", fill = "Level",
    caption = "Source: FHFA All-Transactions HPI. Treatment based on NFIP payouts."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "bottom",
    panel.grid.minor = element_blank()
  )

ggsave(file.path(FIG_DIR, "did_hpi_event_study_combined.png"), p_both,
       width = 11, height = 5.5, dpi = 150)
message("Saved: figures/did_hpi_event_study_combined.png")

# ── Summary ──────────────────────────────────────────────────────────────────
message("\n=== HPI DiD Estimation Complete ===")
message(sprintf("Results saved to: %s", RESULTS_DIR))
message("\nKey results:")
message(sprintf("  ZIP DiD (binary):      coef = %.4f, se = %.4f, p = %.4f",
                coef(did_hpi), se(did_hpi), pvalue(did_hpi)))
message(sprintf("  ZIP DiD (intensity):   coef = %.6f, se = %.6f, p = %.4f",
                coef(int_hpi), se(int_hpi), pvalue(int_hpi)))
message(sprintf("  Tract DiD (binary):    coef = %.4f, se = %.4f, p = %.4f",
                coef(did_tract), se(did_tract), pvalue(did_tract)))
message(sprintf("  Tract DiD (intensity): coef = %.6f, se = %.6f, p = %.4f",
                coef(int_tract), se(int_tract), pvalue(int_tract)))
