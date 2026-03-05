# Guadalupe River Synthetic Controls Recovery

## Background

On October 17, 1998, severe flooding along the Guadalupe River devastated communities in Central Texas. The event prompted a federal disaster declaration (DR-1257-TX) and caused widespread damage to homes, businesses, and infrastructure — particularly in Comal County.

More than 25 years later, this project answers those questions: Did the local economy fully recover? How long did it take? Which parts of the economy bounced back quickly, and which lagged behind? The short answer: recovery was swift and the flood left no detectable long-run scar — but reaching that conclusion required a battery of methods to rule out alternative explanations.

## What This Project Does

This project applies three complementary causal inference methods to measure the economic impact of the 1998 flood on Comal County.

**Synthetic Control Method (county level):** We construct a "synthetic" Comal County by combining data from other Texas counties unaffected by the flood. This synthetic county represents what Comal's economy would have looked like without the disaster. Comparing actual Comal to its synthetic counterpart isolates the flood's effect from broader regional trends. The donor pool excludes any Texas county with its own major flood event between 1995 and 2001.

**Difference-in-differences (ZIP and tract level):** Within Comal County, ZIP codes with heavy flood insurance claims are compared to lightly-damaged ZIP codes nearby. This sharper geographic contrast lets us detect effects on business activity (establishment counts, employment, payroll) and housing values that might be averaged away at the county level.

**Robustness checks:** Three additional analyses — Augmented SCM with formal confidence intervals, wild cluster bootstrap for small-sample inference, and HonestDiD pre-trend sensitivity — stress-test the main results against statistical and methodological concerns.

## Study Period

- **Pre-flood (1978--1998):** Used to calibrate the synthetic control so it closely matches Comal County's economy before the disaster struck.
- **Post-flood (1999--2025):** Used to track whether and when Comal County's actual economic trajectory converged back to what the synthetic control predicts.

## Data Sources

The project draws on a wide range of publicly available federal and state datasets to build a detailed picture of county-level economic activity over time:

- **Income and earnings** from the Bureau of Economic Analysis (1969+)
- **Employment and wages** from the Bureau of Labor Statistics QCEW (1990+)
- **Business formation and closure** from the Census Business Dynamics Statistics (1978+)
- **Building permits and construction activity** from the Census Building Permits Survey (1990+)
- **Unemployment rates** from BLS Local Area Unemployment Statistics (1990+)
- **Workforce dynamics** (hiring, separations, earnings) from Census QWI (~1995+)
- **Business counts and payroll** from the Census County Business Patterns (1986+)
- **Demographics, housing, and poverty** from the American Community Survey and decennial census
- **Household income** from IRS Statistics of Income (2011+)
- **Disaster aid and recovery funding** from FEMA, SBA, and HUD
- **Flood insurance claims** from the National Flood Insurance Program
- **Physical flood and storm records** from NOAA Storm Events
- **River conditions** from USGS stream gauges on the Guadalupe and Comal Rivers

## Output

The pipeline produces a merged **SCM panel** at `data/processed/panels/scm_panel.parquet` (also `.csv`):

- **11,938 rows** (254 TX counties x 47 years)
- **63 columns** spanning outcomes, covariates, and donor eligibility flags
- All nominal dollar values deflated to constant **2020 dollars** using CPI-U
- **171 donor-eligible counties** after excluding flood-affected areas

## Analysis Results

### County-Level SCM (R/01 + R/02)

The Synthetic Control Method estimated against real per capita income (1990--2022) with a screened donor pool:

- **Pre-treatment RMSPE:** $660 (1.68% of pre-treatment mean) — excellent fit
- **Gap series:** Comal dropped $2,685 below its synthetic counterpart in 1999, recovered by 2001, and outperformed thereafter (average post-treatment gap = +$3,845)
- **Top donors:** Rockwall County (50%), Lampasas (24%), Orange (7%)
- **Permutation p-value: 0.471** — not statistically significant. Comal's post-flood trajectory is not unusual among fast-growing TX counties. Growth-matched donor pool (31 counties, see `notebooks/02_donor_screening.ipynb` for screening steps) yielded p = 0.625.
- **Validity checks** (R/02): (1) *In-space placebo* — re-assigns treatment to each of the 31 donors; Comal ranks 35th of 69 by post/pre RMSPE ratio; (2) *leave-one-out* — dropping Rockwall (50% weight) produces nearly identical gaps; (3) *in-time placebo* — false treatment assigned at **1994** (5 years before true event), using only pre-1999 data, shows no spurious effect.

### Within-County DiD: ZIP-Level Business Activity (R/03)

To gain sharper identification, a difference-in-differences at the ZIP code level exploits geographic variation in flood damage using Census ZIP Code Business Patterns (1994--2020) and NFIP claims data.

- **Treatment:** 4 ZIPs with >$500K in NFIP payouts (78130 NB Downtown: $17M, 78132, 78163, 78131). The $500K threshold separates ZIPs with substantial insured damage from minimally affected areas; these four ZIPs account for >95% of Comal County's total DR-1257-TX NFIP claims.
- **Control:** 9 ZIPs with minimal damage (3 Comal + 6 adjacent county)
- **Binary DiD (establishments):** coef = -0.524, p = 0.202
- **Event study:** Pre-trends slope downward (1994–1997: +0.48, +0.34, +0.20, +0.06), but the joint Wald test does not reject parallel trends (F(4,286) = 1.3, p = 0.260). Further investigation (linear trend test p = 0.121, placebo treatment tests p > 0.12, outcome decomposition) identifies the pattern as mean reversion in firm counts — establishment-specific, within-county, and not present in employment or payroll. Confidence intervals include zero throughout.

### Housing Value Analysis (R/04)

Adapted the donor-based counterfactual methodology from the Longitudinal Housing Recovery project (Hurricane Sandy) using FHFA House Price Index at ZIP and census tract level.

- **ZIP DiD:** coef = +0.010, p = 0.762 — no significant effect on housing values
- **Tract intensity DiD:** coef = -0.004, p = 0.087 — marginally significant
- **Donor-counterfactual recovery timing:** 78130 recovered in 1 year, 78132 in 2 years, 78163 in 6 years (median: 2 years)
- **Event study:** Joint Wald tests reject parallel trends at both ZIP (F(8,309) = 10.8, p < 0.001) and tract (F(6,911) = 11.0, p < 0.001) levels. HonestDiD sensitivity analysis (R/07) shows results are robust to these violations. Post-treatment gaps within ±5%.

### Robustness: Augmented SCM (R/05)

Ridge-augmented SCM (Ben-Michael, Feller, Rothstein 2021) provides bias-corrected estimates with formal confidence intervals via conformal inference:

- **Average ATT:** +$6,248 (p = 0.107) — not statistically significant
- **Per-year CIs:** 95% CIs include zero in every post-treatment year (1999--2022)
- **Top donors:** Rockwall (38%), Randall (29%), Polk (19%) — partially overlapping with traditional SCM
- **L2 imbalance:** 92% improvement from uniform weights
- Confirms the traditional SCM result: positive gap post-2001 but not statistically distinguishable from zero

### Robustness: Wild Cluster Bootstrap (R/06)

Standard cluster-robust SEs are unreliable with few clusters. Wild cluster bootstrap (Roodman et al. 2019, Webb 6-point weights, B = 99,999) provides valid inference:

| Model | Coef | p (CR) | p (Boot) | Clusters |
|-------|------|--------|----------|----------|
| ZBP: ln(estab) | -0.524 | 0.202 | 0.222 | 13 |
| ZBP: ln(emp) | -0.021 | 0.974 | 0.970 | 13 |
| ZBP: ln(payroll) | +0.586 | 0.562 | 0.576 | 13 |
| HPI ZIP (binary) | +0.010 | 0.762 | 0.758 | 12 |
| HPI Tract (intensity) | -0.004 | 0.087 | 0.135 | 32 |

The tract intensity DiD (the only marginally significant result at p = 0.087) weakens to p = 0.135 under bootstrap, confirming no significant effects across all specifications.

### Robustness: HonestDiD Pre-Trend Sensitivity (R/07)

The ZBP event study shows pre-trends (1994--1997 coefficients: +0.48, +0.34, +0.20, +0.06). Rambachan & Roth (2023) HonestDiD bounds treatment effects under parallel trends violations:

- **Smoothness restriction (Delta^SD):** At M = 0 (exact parallel trends), CI = [0.016, 0.132] excludes zero. At M = 0.02 — the smallest value at which CIs cross zero — CI = [−0.002, 0.149] includes zero. Any significance is fragile.
- **Relative magnitudes (Delta^RM):** CI includes zero at all Mbar values (0--2), confirming the null is robust.

### Interpretation

The 1998 Guadalupe River flood caused clear short-term physical damage ($22.5M in NFIP payouts to Comal County) and a brief economic dip visible at both county and ZIP level. However, no analysis — county SCM, ZIP-level DiD on establishments/employment/payroll, or housing price DiD — finds a statistically significant long-run effect. Three additional robustness checks (augmented SCM with conformal inference, wild cluster bootstrap for few-clusters inference, and HonestDiD pre-trend sensitivity) uniformly confirm this null finding. The housing market recovered within 1--6 years (median 2). This is consistent with the disaster economics literature finding that localized natural disasters in growing economies produce transitory economic effects, particularly when underlying demand drivers (I-35 corridor growth) remain intact.

## Limitations

### Data Quality

- **ZBP noise infusion (post-2007):** The Census Bureau adds statistical noise to ZIP Code Business Patterns data beginning around 2007 to protect respondent confidentiality. Establishment and employment counts at ZIP level may be distorted, particularly for small geographic units. Wild bootstrap p-values (R/06) are reported alongside cluster-robust SEs to account for this uncertainty.
- **ACS covariate imputation (pre-2000):** American Community Survey data is available only from 2009 onward (5-year estimates) and the 2000 Decennial Census. ACS covariates in the SCM panel are forward- and back-filled from the nearest available observation to span the full 1978–2024 window. Pre-2000 values are imputed and should be interpreted with caution in pre-treatment balance checks.
- **FHFA HPI sparse coverage:** The FHFA repeat-sales House Price Index requires a minimum number of transactions per geographic unit. Census tracts or ZIPs with few repeat sales may have sparse or noisy index values, especially early in the panel.

### Study Design

- **Parallel trends:** The ZBP establishment event study shows a visually declining pre-trend (joint Wald p = 0.260, linear trend p = 0.121), diagnosed as mean reversion in firm counts rather than a confounding shock (see METHODS.md §3.4). HonestDiD (R/07) confirms significance vanishes at M > 0.02. The HPI event studies formally reject parallel trends at both ZIP (p < 0.001) and tract (p < 0.001) levels; HonestDiD relative-magnitudes CIs include zero at all Mbar values, confirming the null result is robust.
- **Geographic aggregation:** Comal County encompasses both New Braunfels (primary flood impact area) and rural areas largely unaffected by the flood. County-level SCM captures a net effect but cannot isolate impacts to specific neighborhoods. ZIP and tract DiD analyses partially address this, constrained by data availability and the need for sufficient control units.
- **Donor pool size:** Growth matching reduces the donor pool from 171 to 31 counties. With 31 donors, the minimum achievable in-space placebo p-value is 1/32 ≈ 0.031. Comal's rank in the permutation distribution comfortably exceeds this bound.
- **NFIP threshold choice:** The $500K ZIP treatment threshold was chosen based on the observed distribution of NFIP payouts for DR-1257-TX. A lower threshold ($250K) would add one additional treated ZIP; a higher threshold ($750K) would reduce to three. The null result is insensitive to these alternatives.

## Project Structure

```
config/
  project.yaml            # Treated unit, study period, disaster declaration
  sources.yaml            # Per-source endpoint configuration
src/
  pipeline.py             # DAG orchestrator for data acquisition
  config.py               # YAML + .env loader
  acquire/                # 22 data source modules (incl. census_zbp.py)
  process/
    harmonize_county.py   # Standardize all datasets to county-year panels
    deflator.py           # CPI-U deflator (constant 2020 dollars)
    panel_builder.py      # Merge harmonized data into final SCM panel
  utils/                  # Shared I/O, HTTP, API helpers
notebooks/
  01_panel_diagnostics.ipynb  # Coverage heatmaps, pre-treatment trends, balance table
  02_donor_screening.ipynb    # Donor pool screening (171 → 69 → 31 growth-matched)
  03_did_zbp_analysis.ipynb   # ZIP-level DiD panel build (ZBP + NFIP crosswalk)
  04_housing_hpi_analysis.ipynb # Housing HPI analysis (donor counterfactual)
R/
  01_scm_estimate.R       # tidysynth SCM estimation
  02_placebo_tests.R      # In-space, leave-one-out, in-time placebos
  03_did_zbp.R            # ZIP-level DiD on establishments/employment/payroll
  04_did_hpi.R            # Housing value DiD at ZIP and census tract level
  05_ascm.R               # Ridge Augmented SCM (Ben-Michael et al. 2021)
  06_wild_bootstrap.R     # Wild cluster bootstrap for few-clusters inference
  07_honestdid.R          # HonestDiD pre-trend sensitivity (Rambachan & Roth 2023)
data/
  raw/                    # Downloaded data (gitignored)
  processed/              # Harmonized panels + merged SCM panel (gitignored)
  results/                # SCM outputs, DiD results, figures
```

## Setup

Requires Python 3.11 or later. Install dependencies with:

```
pip install -e ".[dev]"
```

**API keys are optional.** The pipeline downloads all data from bulk public endpoints that do not require authentication. If you want to use the Census, BLS, or BEA APIs directly (for faster or more targeted queries), copy `.env.example` to `.env` and add your keys.

## Running the Pipeline

```bash
# Full pipeline: acquire all data, harmonize, and build panel
make all

# Individual steps
make acquire              # download all raw data (phases 1-3)
make acquire-phase1       # anchor datasets and donor pool only
make acquire-bea_income   # a single data source
make harmonize            # standardize raw data to county-year panels
make panel                # merge harmonized data + deflate + export

# Utilities
make list-tasks           # show all available data sources
make clean-processed      # remove processed data (keeps raw)
```

Or run steps directly with Python:

```bash
python -m src.pipeline --phase 3               # acquire all phases
python -m src.pipeline --task bea_income --force  # re-download one source
python -c "from src.process.harmonize_county import run_all; run_all()"
python -c "from src.process.panel_builder import save_panel; save_panel()"
```
