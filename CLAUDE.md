# Guadalupe River SCM Recovery — AI Session Context

## Project

October 1998 Guadalupe River flood (DR-1257-TX) economic impact study.

- **Method:** Synthetic Control Method (county) + Difference-in-Differences (ZIP/tract)
- **Treated unit:** Comal County, TX (FIPS 48091)
- **Pre-period:** 1978–1998 | **Post-period:** 1999–2025
- **Division of labor:** Python = ETL pipeline; R = estimation

## Analysis Status: COMPLETE

All 7 analyses are finished. Results in `data/results/` (14 CSV files, 25 figures).

```
R/01_scm_estimate.R      → county SCM (RMSPE $660, p = 0.471)
R/02_placebo_tests.R     → in-space, leave-one-out, in-time (1994) placebos
R/03_did_zbp.R           → ZIP DiD on business activity (p = 0.202)
R/04_did_hpi.R           → housing HPI DiD at ZIP and tract level
R/05_ascm.R              → Ridge Augmented SCM (ATT +$6,248, p = 0.107)
R/06_wild_bootstrap.R    → wild cluster bootstrap (few-clusters correction)
R/07_honestdid.R         → HonestDiD pre-trend sensitivity (fragile at M > 0.02)
```

**Overall finding:** No statistically significant long-run effect. Three robustness checks confirm.

## Key Commands

```bash
# Python ETL pipeline
make acquire              # download all 22 raw data sources
make acquire-phase1       # anchor datasets only (BEA, BDS, QCEW, LAUS, FEMA donors)
make acquire-bea_income   # single source
make harmonize            # standardize raw → county-year panels
make panel                # full pipeline: harmonize + merge + deflate + export
make list-tasks           # show all 22 acquisition tasks

# R estimation (run scripts individually)
Rscript R/01_scm_estimate.R
Rscript R/02_placebo_tests.R
# ... etc.
```

## R Environment

- **Library path:** `~/R/library` (non-standard; required for augsynth, fwildclusterboot)
- All R scripts include `.libPaths(c("~/R/library", .libPaths()))` at the top
- **R version:** 4.1.0

## Common Gotchas

| Issue | Fix |
|-------|-----|
| FIPS reads as numeric, losing leading zero | `col_types = cols(fips = col_character())` |
| `augsynth` weights access | `asyn$weights` (not `coef(asyn)`) |
| `fwildclusterboot` + `feols()` incompatible | Use `lm()` with `factor(fips) + factor(year)` FEs |
| pandas/pyarrow incompatibility | Use `src/utils/file_io.py` wrappers, not `pd.read_parquet()` directly |
| ACS covariates sparse pre-2009 | Forward/back-filled in panel_builder.py — use with caution for pre-2000 years |
| ZBP establishment pre-trend (visual decline) | Mean reversion in firm counts, not confounding; employment/payroll show no trend (see METHODS.md §3.4) |

## Key Files

```
config/project.yaml              # treated unit, study period, disaster declaration
config/sources.yaml              # per-source API endpoints and rate limits
src/pipeline.py                  # DAG orchestrator (22 sources, 3 phases)
src/process/panel_builder.py     # merges harmonized data → scm_panel.parquet
src/process/harmonize_county.py  # standardizes raw → county-year panels
src/utils/file_io.py             # parquet I/O (use instead of pandas directly)
data/processed/panels/scm_panel.parquet  # 11,938 rows × 63 cols, 254 TX counties
data/results/                    # all CSVs and figures from R/01–07
```

## Panel Details

- **SCM panel:** 11,938 rows × 63 columns (254 TX counties × 47 years, 1978–2024)
- **Donor pool:** 171 eligible → 69 screened → 31 growth-matched (see `notebooks/02_donor_screening.ipynb`)
- **CPI deflator:** CPI-U, 2020 base year; applied to 14 nominal dollar columns
- **ZBP DiD panel:** 351 rows (13 ZIPs × 27 years, 1994–2020)
- **HPI panels:** ZIP (391 rows, 12 ZIPs) + tract (1,037 rows, 32 tracts)

## Next Steps

- **Primary:** Quarto manuscript integrating all 7 analyses
- **Optional:** Sector-level SCM (construction, retail, services)
- **Optional:** LEHD LODES commuting analysis (tract-level, 2002+)
- **Blocked:** TTU does not have CoreLogic/ATTOM/WRDS subscriptions
