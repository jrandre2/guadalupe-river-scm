# Guadalupe River Synthetic Controls Recovery

On October 17, 1998, severe flooding along the Guadalupe River devastated communities in
Central Texas, prompting federal disaster declaration DR-1257-TX and causing widespread
damage to homes, businesses, and infrastructure in Comal County. More than 25 years later,
this project answers the question: did the local economy fully recover — and was there a
lasting scar?

**Short answer:** recovery was swift, and no analysis finds a statistically significant
long-run economic effect.

---

**Manuscript:** [manuscript.pdf](manuscript.pdf) | [Quarto source](manuscript.qmd)

---

## Research Question and Approach

This study applies three complementary causal inference methods to measure the long-run
economic impact of the 1998 flood on Comal County, TX.

**Synthetic Control Method (county level):** A "synthetic" Comal County is constructed by
combining data from other Texas counties unaffected by the flood. Comparing actual Comal to
its synthetic counterpart isolates the flood's effect from broader regional trends. The donor
pool excludes any Texas county with its own major flood event between 1995 and 2001.

**Difference-in-differences (ZIP and tract level):** Within Comal County, ZIP codes with
heavy flood insurance claims are compared to lightly-damaged ZIP codes nearby. This
geographic contrast detects effects on business activity (establishment counts, employment,
payroll) and housing values that might be averaged away at the county level.

**Robustness checks:** Augmented SCM with formal confidence intervals, wild cluster bootstrap
for small-sample inference, and HonestDiD pre-trend sensitivity stress-test the main results.

## Main Findings

- **County SCM:** Comal's per capita income dipped briefly below its synthetic counterpart
  in 1999 and recovered by 2001, outperforming thereafter. Permutation p-value = 0.471 —
  not statistically significant.
- **ZIP-level DiD (business activity):** No significant effect on establishment counts,
  employment, or payroll. Pre-trend diagnostic attributed to mean reversion, not a
  confounding shock.
- **Housing values:** Recovery within 1–6 years at affected ZIP codes (median 2 years).
  No statistically significant long-run effect.
- **All three robustness checks** (augmented SCM, wild cluster bootstrap, HonestDiD)
  uniformly confirm the null finding.

The flood caused clear short-term damage ($22.5M in NFIP payouts) but no detectable
long-run economic scar. This is consistent with disaster economics literature: localized
floods in growing economies produce transitory effects, especially when underlying demand
drivers (here, I-35 corridor growth) remain intact.

Full statistical details — coefficients, confidence intervals, placebo distributions,
and pre-trend diagnostics — are in the [manuscript](manuscript.pdf) and
[Methods Supplement](METHODS.md).

## Study Period

- **Pre-flood (1978–1998):** Calibration window for the synthetic control.
- **Post-flood (1999–2025):** Tracks whether Comal's economic trajectory converges back
  to the synthetic control's prediction.

## Data

The project draws on publicly available federal and state datasets:

| Source | Coverage |
|--------|----------|
| BEA Personal Income | 1969+, all 254 TX counties |
| BLS QCEW (employment and wages) | 1990+ |
| Census Business Dynamics Statistics | 1978+ |
| Census ZIP Code Business Patterns | 1994–2020 |
| BLS Local Area Unemployment Statistics | 1990+ |
| Census QWI (workforce dynamics) | ~1995+ |
| Census County Business Patterns | 1986+ |
| American Community Survey + Decennial | demographics, housing, poverty |
| IRS Statistics of Income | 2011+ |
| FEMA (PA, IA, HMA, NFIP) | disaster aid and flood insurance |
| SBA disaster loans | |
| FHFA House Price Index | ZIP and tract level |
| NOAA Storm Events | |
| USGS stream gauges | Guadalupe and Comal Rivers |

Raw and processed data are not committed (gitignored). See [DATA_DICTIONARY.md](DATA_DICTIONARY.md)
for variable definitions and panel dimensions. API keys are optional — all sources have
public bulk endpoints; see `.env.example` for key names (no secrets committed).

## Repository Map

```
manuscript.qmd              # Quarto manuscript source
manuscript.pdf              # Compiled manuscript
references.bib              # Bibliography

R/                          # Estimation scripts (R)
  01_scm_estimate.R         # Synthetic control (tidysynth)
  02_placebo_tests.R        # In-space, leave-one-out, in-time placebos
  03_did_zbp.R              # ZIP-level DiD — establishments, employment, payroll
  04_did_hpi.R              # Housing HPI DiD at ZIP and census tract level
  05_ascm.R                 # Augmented SCM (Ben-Michael et al. 2021)
  06_wild_bootstrap.R       # Wild cluster bootstrap (few-cluster correction)
  07_honestdid.R            # HonestDiD pre-trend sensitivity
  08_scm_sector.R           # Sector-level SCM extensions
  09_did_lodes.R / 10_did_zbp_sector.R  # LODES and sector DiD

notebooks/                  # Exploratory and diagnostic notebooks (Python)
  01_panel_diagnostics.ipynb    # Coverage heatmaps, balance table
  02_donor_screening.ipynb      # Donor pool screening (171 → 31 growth-matched)
  03_did_zbp_analysis.ipynb     # ZIP DiD panel construction
  04_housing_hpi_analysis.ipynb # Donor-counterfactual housing recovery timing

src/                        # Python ETL pipeline
  pipeline.py               # DAG orchestrator
  acquire/                  # 22 data-source modules
  process/                  # Harmonization, deflation, panel build
  utils/                    # Shared HTTP, API, file-I/O helpers

data/
  raw/                      # Downloaded data (gitignored)
  processed/                # Harmonized panels and merged SCM panel (gitignored)
  results/                  # SCM outputs, DiD results, figures (committed)
  metadata/source_log.json  # Acquisition log

config/
  project.yaml              # Treated unit, study period, disaster declaration
  sources.yaml              # Per-source endpoint configuration
```

## Limitations

- **ZBP noise infusion (post-2007):** Census adds statistical noise to ZIP-level business
  patterns to protect confidentiality; wild bootstrap p-values are reported alongside
  cluster-robust SEs.
- **ACS covariate imputation (pre-2000):** ACS data starts in 2009; pre-2000 values are
  forward/back-filled from nearest observations and should be interpreted with caution.
- **FHFA HPI sparse coverage:** Tracts or ZIPs with few repeat sales may have noisy index
  values early in the panel.
- **Geographic aggregation:** County-level SCM captures a net effect; ZIP/tract DiD
  partially addresses this, constrained by data availability and control-unit requirements.
- **Donor pool size:** Growth matching reduces 171 eligible donors to 31; minimum achievable
  in-space placebo p-value is 1/32 ≈ 0.031.

Full discussion in [METHODS.md](METHODS.md).

## Reproduction

See [REPRODUCTION.md](REPRODUCTION.md) for environment setup, the full `make` pipeline,
and step-by-step commands for replicating every analysis script.
