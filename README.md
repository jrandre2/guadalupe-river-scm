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

## Project Structure

```
config/
  project.yaml          # Treated unit, study period, disaster declaration
  sources.yaml          # Per-source endpoint configuration
src/
  pipeline.py           # DAG orchestrator for data acquisition
  config.py             # YAML + .env loader
  acquire/              # 21 data source modules
  process/
    harmonize_county.py # Standardize all datasets to county-year panels
    deflator.py         # CPI-U deflator (constant 2020 dollars)
    panel_builder.py    # Merge harmonized data into final SCM panel
  utils/                # Shared I/O, HTTP, API helpers
data/
  raw/                  # Downloaded data (gitignored)
  processed/            # Harmonized panels + merged SCM panel (gitignored)
R/                      # SCM estimation scripts (forthcoming)
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
