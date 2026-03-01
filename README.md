# Guadalupe River Synthetic Controls Recovery

## Background

On October 17, 1998, severe flooding along the Guadalupe River devastated communities in Central Texas. The event prompted a federal disaster declaration (DR-1257-TX) and caused widespread damage to homes, businesses, and infrastructure — particularly in Comal County.

More than 25 years later, the long-term economic consequences of that flood remain an open question. Did the local economy fully recover? How long did it take? Which parts of the economy bounced back quickly, and which lagged behind?

## What This Project Does

This project uses a statistical technique called the **Synthetic Control Method** to measure the causal economic impact of the 1998 flood on Comal County, Texas.

The idea is straightforward: we construct a "synthetic" version of Comal County by combining data from other Texas counties that were not affected by the flood. This synthetic county serves as an estimate of what Comal County's economy would have looked like if the flood had never happened. By comparing the real Comal County to its synthetic counterpart over time, we can isolate the effect of the disaster from other trends affecting the region.

The donor pool — the set of comparison counties — excludes any Texas county that experienced its own major flood event between 1995 and 2001, so the comparison is not contaminated by similar shocks elsewhere.

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
- **Permutation p-value: 0.471** — not statistically significant. Comal's post-flood trajectory is not unusual among fast-growing TX counties. Growth-matched donor pool (31 counties) yielded p = 0.625.

### Within-County DiD: ZIP-Level Business Activity (R/03)

To gain sharper identification, a difference-in-differences at the ZIP code level exploits geographic variation in flood damage using Census ZIP Code Business Patterns (1994--2020) and NFIP claims data.

- **Treatment:** 4 ZIPs with >$500K in NFIP payouts (78130 NB Downtown: $17M, 78132, 78163, 78131)
- **Control:** 9 ZIPs with minimal damage (3 Comal + 6 adjacent county)
- **Binary DiD (establishments):** coef = -0.524, p = 0.202
- **Event study:** Pre-trends slope downward, suggesting convergence rather than flood effect. Confidence intervals include zero throughout.

### Housing Value Analysis (R/04)

Adapted the donor-based counterfactual methodology from the Longitudinal Housing Recovery project (Hurricane Sandy) using FHFA House Price Index at ZIP and census tract level.

- **ZIP DiD:** coef = +0.010, p = 0.762 — no significant effect on housing values
- **Tract intensity DiD:** coef = -0.004, p = 0.087 — marginally significant
- **Donor-counterfactual recovery timing:** 78130 recovered in 1 year, 78132 in 2 years, 78163 in 6 years (median: 2 years)
- **Event study:** Pre-trends flat (validating parallel trends). Post-treatment gaps within ±5%.

### Interpretation

The 1998 Guadalupe River flood caused clear short-term physical damage ($22.5M in NFIP payouts to Comal County) and a brief economic dip visible at both county and ZIP level. However, no analysis — county SCM, ZIP-level DiD on establishments/employment/payroll, or housing price DiD — finds a statistically significant long-run effect. The housing market recovered within 1--6 years (median 2). This is consistent with the disaster economics literature finding that localized natural disasters in growing economies produce transitory economic effects, particularly when underlying demand drivers (I-35 corridor growth) remain intact.

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
