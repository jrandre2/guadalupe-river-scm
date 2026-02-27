# Guadalupe River Synthetic Controls Recovery

## Background

On October 17, 1998, severe flooding along the Guadalupe River devastated communities in Central Texas. The event prompted a federal disaster declaration (DR-1257-TX) and caused widespread damage to homes, businesses, and infrastructure — particularly in Comal County.

More than 25 years later, the long-term economic consequences of that flood remain an open question. Did the local economy fully recover? How long did it take? Which parts of the economy bounced back quickly, and which lagged behind?

## What This Project Does

This project uses a statistical technique called the **Synthetic Control Method** to measure the causal economic impact of the 1998 flood on Comal County, Texas.

The idea is straightforward: we construct a "synthetic" version of Comal County by combining data from other Texas counties that were not affected by the flood. This synthetic county serves as an estimate of what Comal County's economy would have looked like if the flood had never happened. By comparing the real Comal County to its synthetic counterpart over time, we can isolate the effect of the disaster from other trends affecting the region.

The donor pool — the set of comparison counties — excludes any Texas county that experienced its own major flood event between 1995 and 2001, so the comparison is not contaminated by similar shocks elsewhere.

## Study Period

- **Pre-flood (1978 -- 1998):** Used to calibrate the synthetic control so it closely matches Comal County's economy before the disaster struck.
- **Post-flood (1999 -- 2025):** Used to track whether and when Comal County's actual economic trajectory converged back to what the synthetic control predicts.

## Data Sources

The project draws on a wide range of publicly available federal and state datasets to build a detailed picture of county-level economic activity over time:

- **Income and earnings** from the Bureau of Economic Analysis
- **Employment and wages** from the Bureau of Labor Statistics
- **Business formation and closure** from the Census Bureau
- **Building permits and construction activity** from the Census Bureau
- **Unemployment rates** from local area statistics
- **Workforce dynamics** (hiring, separations, earnings) from the Census Bureau
- **Demographics, housing, and poverty** from the American Community Survey and decennial census
- **Household income** from IRS tax return data
- **Sales tax revenue** from the Texas Comptroller
- **Disaster aid and recovery funding** from FEMA, SBA, HUD, and USAspending.gov
- **Flood insurance claims** from the National Flood Insurance Program
- **Physical flood and storm records** from NOAA
- **River conditions** from USGS stream gauges on the Guadalupe and Comal Rivers

## How It Works

The Python code in this repository handles data acquisition — downloading, cleaning, and organizing all of the source datasets into a consistent county-level panel. A pipeline orchestrator runs each data source in the correct order, respecting dependencies between steps.

The actual synthetic control estimation is performed in R.

## Setup

Requires Python 3.11 or later. Install dependencies with:

```
pip install -e ".[dev]"
```

Some data sources require free API keys. Copy `.env.example` to `.env` and fill in keys for the Bureau of Labor Statistics, Census Bureau, and Bureau of Economic Analysis (registration links are in the file).

## Running the Pipeline

```
make acquire          # download everything
make acquire-phase1   # anchor datasets and donor pool only
make acquire-bea_income  # a single source
make list-tasks       # show all available data sources
```
