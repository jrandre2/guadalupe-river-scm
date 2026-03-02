## =============================================================================
## 06_wild_bootstrap.R
## Guadalupe River SCM Recovery — Wild Cluster Bootstrap Inference
##
## Standard cluster-robust SEs are unreliable with few clusters (<10).
## The ZIP-level DiD has only 13 clusters (4 treated, 9 control).
## Wild cluster bootstrap (Roodman et al. 2019, Webb 6-point weights)
## provides more reliable p-values in this setting.
##
## Note: fwildclusterboot has a compatibility issue with fixest's feols()
## in this R version (4.1.0), so we use lm() with explicit factor FEs for
## the bootstrap while reporting standard CR SEs from feols() for comparison.
##
## Covers both:
##   - ZBP DiD (R/03): Establishments, employment, payroll
##   - HPI DiD (R/04): Housing values at ZIP and tract level
##
## Inputs:
##   data/processed/panels/did_zbp_panel.csv
##   data/processed/panels/did_hpi_zip_panel.csv
##   data/processed/panels/did_hpi_tract_panel.csv
##
## Outputs (saved to data/results/):
##   wild_bootstrap_results.csv   — All models: standard vs bootstrap p-values
##   wild_bootstrap_details.txt   — Full output
##
## R packages required:
##   fixest, fwildclusterboot, tidyverse
## =============================================================================

.libPaths(c("~/R/library", .libPaths()))

suppressPackageStartupMessages({
  library(fixest)
  library(fwildclusterboot)
  library(dplyr)
  library(readr)
})

# ── Paths ────────────────────────────────────────────────────────────────────
PROJECT_ROOT <- here::here()
RESULTS_DIR  <- file.path(PROJECT_ROOT, "data/results")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)

set.seed(20241998)  # reproducibility
B <- 99999          # bootstrap iterations

# Helper: run boottest via lm() with factor FEs (workaround for feols compat)
run_boot <- function(formula_str, data, cluster_var, param_name, B, label) {
  message(sprintf("  Bootstrapping %s (B=%d)...", label, B))
  # Fit lm with explicit factor FEs
  fml <- as.formula(formula_str)
  fit <- lm(fml, data = data)
  boot <- boottest(fit, param = param_name, clustid = cluster_var,
                   B = B, type = "webb")
  boot
}

# ══════════════════════════════════════════════════════════════════════════════
# PART A: ZBP DiD (Business Activity)
# ══════════════════════════════════════════════════════════════════════════════
message("=== PART A: Wild Cluster Bootstrap — ZBP DiD ===")

df <- read_csv(
  file.path(PROJECT_ROOT, "data/processed/panels/did_zbp_panel.csv"),
  show_col_types = FALSE
) |>
  mutate(
    zip_f     = as.factor(zip),
    year_f    = as.factor(year),
    treated   = as.integer(treated),
    post      = as.integer(post),
    ln_estab  = as.numeric(ln_estab),
    ln_emp    = as.numeric(ln_emp),
    ln_payann_real = as.numeric(ln_payann_real),
    intensity = as.numeric(intensity)
  )

message(sprintf("ZBP panel: %d ZIPs (%d treated, %d control), %d obs",
                n_distinct(df$zip_f),
                n_distinct(df$zip_f[df$treated == 1]),
                n_distinct(df$zip_f[df$treated == 0]),
                nrow(df)))

# Standard feols for comparison (CR SEs)
did_estab <- feols(ln_estab ~ treated:post | zip_f + year_f, data = df)
did_emp   <- feols(ln_emp   ~ treated:post | zip_f + year_f, data = df)
did_pay   <- feols(ln_payann_real ~ treated:post | zip_f + year_f, data = df)

# Wild cluster bootstrap via lm()
boot_estab <- run_boot("ln_estab ~ treated:post + zip_f + year_f", df,
                       c("zip_f"), "treated:post", B, "establishments")
boot_emp   <- run_boot("ln_emp ~ treated:post + zip_f + year_f", df,
                       c("zip_f"), "treated:post", B, "employment")
boot_pay   <- run_boot("ln_payann_real ~ treated:post + zip_f + year_f", df,
                       c("zip_f"), "treated:post", B, "payroll")

zbp_results <- data.frame(
  analysis = "ZBP DiD",
  outcome  = c("ln_estab", "ln_emp", "ln_payann_real"),
  coef     = c(coef(did_estab), coef(did_emp), coef(did_pay)),
  se_cr    = c(se(did_estab), se(did_emp), se(did_pay)),
  p_cr     = c(pvalue(did_estab), pvalue(did_emp), pvalue(did_pay)),
  p_boot   = c(boot_estab$p_val, boot_emp$p_val, boot_pay$p_val),
  ci_boot_lo = c(boot_estab$conf_int[1], boot_emp$conf_int[1], boot_pay$conf_int[1]),
  ci_boot_hi = c(boot_estab$conf_int[2], boot_emp$conf_int[2], boot_pay$conf_int[2]),
  n_clusters = n_distinct(df$zip_f),
  n_obs      = nrow(df),
  stringsAsFactors = FALSE
)

message("\nZBP results:")
for (i in seq_len(nrow(zbp_results))) {
  r <- zbp_results[i, ]
  message(sprintf("  %s: coef = %.4f, p(CR) = %.4f, p(boot) = %.4f, 95%% CI_boot [%.4f, %.4f]",
                  r$outcome, r$coef, r$p_cr, r$p_boot, r$ci_boot_lo, r$ci_boot_hi))
}

# ══════════════════════════════════════════════════════════════════════════════
# PART B: HPI DiD — ZIP Level
# ══════════════════════════════════════════════════════════════════════════════
message("\n=== PART B: Wild Cluster Bootstrap — HPI DiD (ZIP) ===")

df_hpi <- read_csv(
  file.path(PROJECT_ROOT, "data/processed/panels/did_hpi_zip_panel.csv"),
  show_col_types = FALSE
) |>
  mutate(
    zip_f     = as.factor(zip),
    year_f    = as.factor(year),
    treated   = as.integer(treated),
    post      = as.integer(post),
    ln_hpi    = as.numeric(ln_hpi),
    intensity = as.numeric(intensity)
  )

message(sprintf("HPI ZIP panel: %d ZIPs (%d treated, %d control), %d obs",
                n_distinct(df_hpi$zip_f),
                n_distinct(df_hpi$zip_f[df_hpi$treated == 1]),
                n_distinct(df_hpi$zip_f[df_hpi$treated == 0]),
                nrow(df_hpi)))

# feols for CR SEs
did_hpi_zip <- feols(ln_hpi ~ treated:post | zip_f + year_f, data = df_hpi)
int_hpi_zip <- feols(ln_hpi ~ intensity:post | zip_f + year_f, data = df_hpi)

# Wild cluster bootstrap
boot_hpi_zip <- run_boot("ln_hpi ~ treated:post + zip_f + year_f", df_hpi,
                         c("zip_f"), "treated:post", B, "HPI ZIP binary")
boot_int_zip <- run_boot("ln_hpi ~ intensity:post + zip_f + year_f", df_hpi,
                         c("zip_f"), "intensity:post", B, "HPI ZIP intensity")

hpi_zip_results <- data.frame(
  analysis = "HPI DiD (ZIP)",
  outcome  = c("ln_hpi (binary)", "ln_hpi (intensity)"),
  coef     = c(coef(did_hpi_zip), coef(int_hpi_zip)),
  se_cr    = c(se(did_hpi_zip), se(int_hpi_zip)),
  p_cr     = c(pvalue(did_hpi_zip), pvalue(int_hpi_zip)),
  p_boot   = c(boot_hpi_zip$p_val, boot_int_zip$p_val),
  ci_boot_lo = c(boot_hpi_zip$conf_int[1], boot_int_zip$conf_int[1]),
  ci_boot_hi = c(boot_hpi_zip$conf_int[2], boot_int_zip$conf_int[2]),
  n_clusters = n_distinct(df_hpi$zip_f),
  n_obs      = nrow(df_hpi),
  stringsAsFactors = FALSE
)

message("\nHPI ZIP results:")
for (i in seq_len(nrow(hpi_zip_results))) {
  r <- hpi_zip_results[i, ]
  message(sprintf("  %s: coef = %.6f, p(CR) = %.4f, p(boot) = %.4f",
                  r$outcome, r$coef, r$p_cr, r$p_boot))
}

# ══════════════════════════════════════════════════════════════════════════════
# PART C: HPI DiD — Census Tract Level
# ══════════════════════════════════════════════════════════════════════════════
message("\n=== PART C: Wild Cluster Bootstrap — HPI DiD (Tract) ===")

dt <- read_csv(
  file.path(PROJECT_ROOT, "data/processed/panels/did_hpi_tract_panel.csv"),
  show_col_types = FALSE
) |>
  mutate(
    tract_f   = as.factor(tract),
    year_f    = as.factor(year),
    treated   = as.integer(treated),
    post      = as.integer(post),
    ln_hpi    = as.numeric(ln_hpi),
    intensity = as.numeric(intensity)
  )

message(sprintf("HPI tract panel: %d tracts (%d treated, %d control), %d obs",
                n_distinct(dt$tract_f),
                n_distinct(dt$tract_f[dt$treated == 1]),
                n_distinct(dt$tract_f[dt$treated == 0]),
                nrow(dt)))

# feols for CR SEs
did_hpi_tract <- feols(ln_hpi ~ treated:post | tract_f + year_f, data = dt)
int_hpi_tract <- feols(ln_hpi ~ intensity:post | tract_f + year_f, data = dt)

# Wild cluster bootstrap
boot_hpi_tract <- run_boot("ln_hpi ~ treated:post + tract_f + year_f", dt,
                           c("tract_f"), "treated:post", B, "HPI tract binary")
boot_int_tract <- run_boot("ln_hpi ~ intensity:post + tract_f + year_f", dt,
                           c("tract_f"), "intensity:post", B, "HPI tract intensity")

hpi_tract_results <- data.frame(
  analysis = "HPI DiD (Tract)",
  outcome  = c("ln_hpi (binary)", "ln_hpi (intensity)"),
  coef     = c(coef(did_hpi_tract), coef(int_hpi_tract)),
  se_cr    = c(se(did_hpi_tract), se(int_hpi_tract)),
  p_cr     = c(pvalue(did_hpi_tract), pvalue(int_hpi_tract)),
  p_boot   = c(boot_hpi_tract$p_val, boot_int_tract$p_val),
  ci_boot_lo = c(boot_hpi_tract$conf_int[1], boot_int_tract$conf_int[1]),
  ci_boot_hi = c(boot_hpi_tract$conf_int[2], boot_int_tract$conf_int[2]),
  n_clusters = n_distinct(dt$tract_f),
  n_obs      = nrow(dt),
  stringsAsFactors = FALSE
)

message("\nHPI tract results:")
for (i in seq_len(nrow(hpi_tract_results))) {
  r <- hpi_tract_results[i, ]
  message(sprintf("  %s: coef = %.6f, p(CR) = %.4f, p(boot) = %.4f",
                  r$outcome, r$coef, r$p_cr, r$p_boot))
}

# ══════════════════════════════════════════════════════════════════════════════
# SAVE COMBINED RESULTS
# ══════════════════════════════════════════════════════════════════════════════
all_results <- bind_rows(zbp_results, hpi_zip_results, hpi_tract_results)
write_csv(all_results, file.path(RESULTS_DIR, "wild_bootstrap_results.csv"))
message(sprintf("\nSaved: wild_bootstrap_results.csv (%d rows)", nrow(all_results)))

# Detailed text output
sink(file.path(RESULTS_DIR, "wild_bootstrap_details.txt"))
cat("=== Wild Cluster Bootstrap Inference (Webb 6-point weights) ===\n")
cat(sprintf("B = %d bootstrap replications\n", B))
cat("Method: Roodman, Nielsen, MacKinnon & Webb (2019)\n\n")

cat("--- ZBP DiD: Establishments ---\n")
print(summary(boot_estab))
cat("\n--- ZBP DiD: Employment ---\n")
print(summary(boot_emp))
cat("\n--- ZBP DiD: Payroll ---\n")
print(summary(boot_pay))

cat("\n--- HPI DiD (ZIP): Binary ---\n")
print(summary(boot_hpi_zip))
cat("\n--- HPI DiD (ZIP): Intensity ---\n")
print(summary(boot_int_zip))

cat("\n--- HPI DiD (Tract): Binary ---\n")
print(summary(boot_hpi_tract))
cat("\n--- HPI DiD (Tract): Intensity ---\n")
print(summary(boot_int_tract))

cat("\n\n=== Summary: Standard CR vs Wild Bootstrap P-Values ===\n\n")
cat(sprintf("%-35s %10s %10s %10s %10s\n",
            "Model", "Coef", "p(CR)", "p(Boot)", "Clusters"))
cat(paste(rep("-", 80), collapse = ""), "\n")
for (i in seq_len(nrow(all_results))) {
  r <- all_results[i, ]
  cat(sprintf("%-35s %10.4f %10.4f %10.4f %10d\n",
              paste(r$analysis, r$outcome), r$coef, r$p_cr, r$p_boot, r$n_clusters))
}
cat("\n\nInterpretation:\n")
cat("Wild cluster bootstrap p-values are more reliable than standard CR p-values\n")
cat("when the number of clusters is small (<20). Webb 6-point weights are\n")
cat("recommended when fewer than ~10 clusters exist in the treated group.\n")
cat("If p(Boot) > 0.05, the null of no treatment effect cannot be rejected\n")
cat("even with proper few-cluster inference.\n")
sink()

message("Saved: wild_bootstrap_details.txt")

# ── Summary ──────────────────────────────────────────────────────────────────
message("\n=== Wild Cluster Bootstrap Complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
message("\nComparison of standard (CR) vs bootstrap p-values:")
message(sprintf("  %-35s  p(CR)   p(Boot)", ""))
for (i in seq_len(nrow(all_results))) {
  r <- all_results[i, ]
  label <- paste(r$analysis, r$outcome, sep = ": ")
  message(sprintf("  %-35s  %.4f  %.4f", label, r$p_cr, r$p_boot))
}
