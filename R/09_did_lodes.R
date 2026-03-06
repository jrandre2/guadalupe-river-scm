## =============================================================================
## 09_did_lodes.R
## Guadalupe River SCM Recovery — Extension: LEHD LODES Persistent Effects
##
## Tests whether flood-exposed census tracts show lasting workplace differences
## compared to nearby control tracts, using LODES WAC data from 2002-2021.
##
## IMPORTANT LIMITATION: LODES starts in 2002, four years after the 1998 flood.
## There is no pre-treatment period for a standard DiD. Results show trajectory
## of treated vs. control tracts from a 2002 baseline — they can detect
## persistent or growing differences but NOT the initial flood impact.
## Inference is further limited by only 2 treated tracts (HC1 SEs used).
##
## Inputs:
##   data/processed/panels/did_lodes_panel.csv  (built by 05_lodes_panel.ipynb)
##
## Outputs (saved to data/results/):
##   did_lodes_results.txt        — Regression tables
##   did_lodes_event_study.csv    — Event study coefficients + CIs
##   figures/15_lodes_event_study.png
##   figures/16_lodes_sector_mix.png
##
## R packages required: fixest, tidyverse, readr, ggplot2
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
# PROJECT_ROOT <- "/Volumes/T9/Projects/ Guadalupe River Synthetic Controls Recovery"

LODES_PANEL_PATH <- file.path(PROJECT_ROOT, "data/processed/panels/did_lodes_panel.csv")
RESULTS_DIR      <- file.path(PROJECT_ROOT, "data/results")
FIG_DIR          <- file.path(RESULTS_DIR, "figures")
dir.create(RESULTS_DIR, showWarnings = FALSE, recursive = TRUE)
dir.create(FIG_DIR,     showWarnings = FALSE, recursive = TRUE)

# ── Parameters ────────────────────────────────────────────────────────────────
REF_YEAR     <- 2002   # First available LODES year; used as event study reference
POST_CUTOFF  <- 2010   # Split window into "early" (2002-2009) vs "later" (2010-2021)
                       # to test whether differences persisted into the 2010s
NFIP_THRESH  <- 200000 # NFIP payout threshold for treated tracts (same as R/04)

# ── 1. Load Data ──────────────────────────────────────────────────────────────
message("Loading LODES panel...")

if (!file.exists(LODES_PANEL_PATH)) {
  stop(paste(
    "LODES panel not found:", LODES_PANEL_PATH,
    "\nRun: notebooks/05_lodes_panel.ipynb first"
  ))
}

df <- read_csv(LODES_PANEL_PATH, show_col_types = FALSE,
               col_types = cols(tract = col_character()))

message(sprintf("LODES panel: %d rows, %d cols", nrow(df), ncol(df)))
message(sprintf("Tracts: %d total (%d treated, %d control)",
                n_distinct(df$tract),
                n_distinct(df$tract[df$treated == 1]),
                n_distinct(df$tract[df$treated == 0])))
message(sprintf("Years: %d–%d", min(df$year), max(df$year)))

# Reminder about the design limitation
cat("\n")
cat(rep("=", 70), "\n")
cat("DESIGN LIMITATION: No pre-treatment data\n")
cat("LODES starts 2002; flood was October 1998.\n")
cat("Cannot estimate flood impact or test parallel trends.\n")
cat("Results show 2002-onward trajectory, not causal effect.\n")
cat(rep("=", 70), "\n\n")

# ── 2. Prepare Variables ──────────────────────────────────────────────────────
df <- df |>
  mutate(
    ln_jobs    = log(C000 + 1),
    ln_hi_wage = log(CE03 + 1),      # high-wage jobs (>$3,333/mo)
    ln_lo_wage = log(CE01 + 1),      # low-wage jobs (<$1,250/mo)
    post_2010  = as.integer(year >= POST_CUTOFF),
    # Sector shares (construction + retail as flood-relevant proxies)
    constr_share = CNS04 / (C000 + 1),   # NAICS 23 Construction in LODES = CNS04
    retail_share = CNS07 / (C000 + 1),   # NAICS 44-45 Retail = CNS07
    food_share   = CNS18 / (C000 + 1)    # NAICS 72 Food Service = CNS18
  )

# ── 3. Binary DiD ─────────────────────────────────────────────────────────────
# Tests: are flood-exposed tracts on a different trajectory from 2010 onward?
# (Relative to the 2002-2009 baseline, conditioning on tract + year FEs)
# SEs: HC1 heteroskedasticity-robust (cluster-robust infeasible with 2 treated tracts)

message("--- Binary DiD: total jobs ---")

did_jobs <- feols(
  ln_jobs ~ treated:post_2010 | tract + year,
  data = df,
  vcov = "hetero"
)
summary(did_jobs)

message("--- Binary DiD: high-wage jobs ---")
did_hiwage <- feols(
  ln_hi_wage ~ treated:post_2010 | tract + year,
  data = df,
  vcov = "hetero"
)
summary(did_hiwage)

# Intensity DiD (if intensity column exists)
if ("intensity" %in% names(df)) {
  message("--- Intensity DiD: total jobs ---")
  did_intensity <- feols(
    ln_jobs ~ intensity:post_2010 | tract + year,
    data = df,
    vcov = "hetero"
  )
  summary(did_intensity)
}

# ── 4. Event Study ────────────────────────────────────────────────────────────
# Reference year = 2002 (first LODES year). No pre-treatment dummies available.
# Coefficients show cumulative trajectory of treated tracts vs. controls
# relative to the 2002 baseline, conditional on two-way FEs.

message("--- Event study: total jobs ---")

es_jobs <- feols(
  ln_jobs ~ i(year, treated, ref = REF_YEAR) | tract + year,
  data = df,
  vcov = "hetero"
)
summary(es_jobs)

message("--- Event study: high-wage jobs ---")
es_hiwage <- feols(
  ln_hi_wage ~ i(year, treated, ref = REF_YEAR) | tract + year,
  data = df,
  vcov = "hetero"
)
summary(es_hiwage)

# ── 5. Save Regression Output ─────────────────────────────────────────────────

sink(file.path(RESULTS_DIR, "did_lodes_results.txt"))
cat("=== LEHD LODES Persistent Effects Analysis ===\n")
cat(sprintf("Date: %s\n", Sys.time()))
cat(sprintf("Panel: %d tracts, %d years (%d–%d)\n",
            n_distinct(df$tract), n_distinct(df$year), min(df$year), max(df$year)))
cat(sprintf("Treated tracts: %d (NFIP > $%s)\n",
            n_distinct(df$tract[df$treated == 1]),
            format(NFIP_THRESH, big.mark = ",")))
cat(sprintf("Control tracts: %d\n", n_distinct(df$tract[df$treated == 0])))
cat("\nDESIGN LIMITATION: No pre-treatment period. LODES starts 2002 (flood was 1998).\n")
cat("HC1 heteroskedasticity-robust SEs (cluster SE infeasible with 2 treated tracts).\n")
cat("Reference year = 2002 in event study (not pre-treatment).\n\n")
cat(rep("-", 60), "\n")
cat("Binary DiD: ln(total jobs)\n")
print(summary(did_jobs))
cat(rep("-", 60), "\n")
cat("Binary DiD: ln(high-wage jobs, >$3,333/mo)\n")
print(summary(did_hiwage))
if (exists("did_intensity")) {
  cat(rep("-", 60), "\n")
  cat("Intensity DiD: ln(total jobs)\n")
  print(summary(did_intensity))
}
cat(rep("-", 60), "\n")
cat("Event study: ln(total jobs)\n")
print(summary(es_jobs))
cat(rep("-", 60), "\n")
cat("Event study: ln(high-wage jobs)\n")
print(summary(es_hiwage))
sink()
message("Saved: did_lodes_results.txt")

# ── 6. Event Study CSV ────────────────────────────────────────────────────────
es_coefs <- as.data.frame(coef(es_jobs)) |>
  tibble::rownames_to_column("term") |>
  rename(estimate = 2) |>
  mutate(
    se   = sqrt(diag(vcov(es_jobs))),
    ci_lo = estimate - 1.96 * se,
    ci_hi = estimate + 1.96 * se,
    year  = as.integer(str_extract(term, "\\d{4}")),
    outcome = "ln_jobs"
  ) |>
  filter(!is.na(year)) |>
  select(year, estimate, se, ci_lo, ci_hi, outcome)

# Add high-wage event study
es_coefs_hw <- as.data.frame(coef(es_hiwage)) |>
  tibble::rownames_to_column("term") |>
  rename(estimate = 2) |>
  mutate(
    se   = sqrt(diag(vcov(es_hiwage))),
    ci_lo = estimate - 1.96 * se,
    ci_hi = estimate + 1.96 * se,
    year  = as.integer(str_extract(term, "\\d{4}")),
    outcome = "ln_hi_wage"
  ) |>
  filter(!is.na(year)) |>
  select(year, estimate, se, ci_lo, ci_hi, outcome)

# Add reference year (coefficient = 0 by construction)
ref_row <- tibble(
  year = REF_YEAR, estimate = 0, se = 0, ci_lo = 0, ci_hi = 0, outcome = "ln_jobs"
)
ref_row_hw <- ref_row |> mutate(outcome = "ln_hi_wage")

es_all <- bind_rows(es_coefs, ref_row, es_coefs_hw, ref_row_hw)
write_csv(es_all, file.path(RESULTS_DIR, "did_lodes_event_study.csv"))
message("Saved: did_lodes_event_study.csv")

# ── 7. Figures ────────────────────────────────────────────────────────────────

# Figure 15: Event study plot (total jobs + high-wage jobs)
outcome_labels <- c(
  "ln_jobs"    = "Total jobs (log)",
  "ln_hi_wage" = "High-wage jobs >$3,333/mo (log)"
)

p15 <- ggplot(
  es_all |> mutate(outcome_label = outcome_labels[outcome]),
  aes(x = year, y = estimate, ymin = ci_lo, ymax = ci_hi)
) +
  geom_hline(yintercept = 0, color = "gray60", linewidth = 0.7) +
  geom_vline(xintercept = REF_YEAR - 0.5, linetype = "dashed", color = "gray40") +
  geom_vline(xintercept = POST_CUTOFF - 0.5,
             linetype = "dotted", color = "steelblue", linewidth = 0.8) +
  geom_ribbon(alpha = 0.15, fill = "#d62728") +
  geom_line(color = "#d62728", linewidth = 1.1) +
  geom_point(color = "#d62728", size = 1.8) +
  annotate("text", x = REF_YEAR, y = Inf,
           label = "LODES\nbaseline", hjust = -0.1, vjust = 1.5, size = 2.8, color = "gray40") +
  annotate("text", x = POST_CUTOFF, y = Inf,
           label = "Post-2010\ncutoff", hjust = -0.1, vjust = 1.5, size = 2.8, color = "steelblue") +
  facet_wrap(~outcome_label, ncol = 1, scales = "free_y") +
  scale_x_continuous(breaks = seq(2002, 2021, 2)) +
  labs(
    title    = "LODES Event Study: Flood-Exposed Tracts vs. Control Tracts",
    subtitle = "Coef on (year × treated), ref = 2002. No pre-treatment data available.",
    x = "Year",
    y = "Estimated coefficient (log jobs)",
    caption = paste(
      "95% CI with HC1 robust SEs. Only 2 treated tracts — underpowered.",
      "\nCoefficients relative to 2002 baseline (NOT relative to pre-flood period)."
    )
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title       = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    strip.text       = element_text(face = "bold")
  )

ggsave(file.path(FIG_DIR, "15_lodes_event_study.png"), p15,
       width = 10, height = 7, dpi = 150)
message("Saved: figures/15_lodes_event_study.png")

# Figure 16: Descriptive sector mix — treated vs. control tracts at 2002 and 2021
sector_mix <- df |>
  filter(year %in% c(2002, 2021)) |>
  group_by(year, treated) |>
  summarise(
    construction = mean(constr_share, na.rm = TRUE),
    retail       = mean(retail_share, na.rm = TRUE),
    food_service = mean(food_share, na.rm = TRUE),
    .groups = "drop"
  ) |>
  pivot_longer(c(construction, retail, food_service),
               names_to = "sector", values_to = "share") |>
  mutate(
    group = ifelse(treated == 1, "Treated tracts", "Control tracts"),
    year_label = as.character(year)
  )

p16 <- ggplot(sector_mix, aes(x = sector, y = share, fill = year_label)) +
  geom_col(position = "dodge", alpha = 0.85) +
  facet_wrap(~group) +
  scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
  scale_fill_manual(values = c("2002" = "#aec7e8", "2021" = "#1f77b4")) +
  labs(
    title   = "Sector Mix: Flood-Exposed vs. Control Tracts (2002 vs. 2021)",
    subtitle = "Share of workplace jobs in construction, retail, food service",
    x = NULL,
    y = "Job share",
    fill = "Year",
    caption = "Source: LEHD LODES8. Descriptive — not causal."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title  = element_text(face = "bold"),
    strip.text  = element_text(face = "bold"),
    axis.text.x = element_text(angle = 20, hjust = 1)
  )

ggsave(file.path(FIG_DIR, "16_lodes_sector_mix.png"), p16,
       width = 9, height = 5, dpi = 150)
message("Saved: figures/16_lodes_sector_mix.png")

message("\n=== R/09_did_lodes.R complete ===")
message(sprintf("Results: %s", RESULTS_DIR))
