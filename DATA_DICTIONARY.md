# Data Dictionary

## SCM Panel

**File:** `data/processed/panels/scm_panel.parquet` (also `.csv`)

**Unit of observation:** County-year (one row per Texas county per year)

**Dimensions:** 11,938 rows (254 counties x 47 years), 63 columns

---

### Identifiers

| Column | Type | Description |
|--------|------|-------------|
| `fips` | string | 5-digit county FIPS code (e.g., "48091" for Comal County) |
| `year` | integer | Calendar year (1978--2024) |

### Donor Pool Flags

| Column | Type | Description |
|--------|------|-------------|
| `is_treated` | boolean | True for Comal County (FIPS 48091) only |
| `in_dr1257` | boolean | True if the county was designated in disaster declaration DR-1257-TX |
| `donor_eligible` | boolean | True if the county is eligible for the donor pool (not affected by major flood 1995--2001). 171 of 254 counties are eligible. |

---

### BEA Personal Income (1978--2024, 100% coverage)

Source: Bureau of Economic Analysis, Regional Economic Accounts (CAINC1 + CAINC4).

| Column | Units | Description |
|--------|-------|-------------|
| `personal_income` | thousands of $ (nominal) | Total personal income |
| `population` | persons | Mid-year population estimate |
| `per_capita_income` | $ (nominal) | Personal income per capita |
| `net_earnings` | thousands of $ (nominal) | Net earnings by place of residence |
| `transfer_receipts` | thousands of $ (nominal) | Personal current transfer receipts |
| `personal_income_real` | thousands of $ (2020) | Deflated total personal income |
| `per_capita_income_real` | $ (2020) | Deflated per capita income |
| `net_earnings_real` | thousands of $ (2020) | Deflated net earnings |
| `transfer_receipts_real` | thousands of $ (2020) | Deflated transfer receipts |

### Census Business Dynamics Statistics (1978--2023, 97% coverage)

Source: Census Bureau, Business Dynamics Statistics (BDS). Covers employer firms only.

| Column | Units | Description |
|--------|-------|-------------|
| `firms` | count | Number of firms |
| `estabs` | count | Number of establishments |
| `emp` | count | Employment (number of employees, March payroll reference) |
| `estabs_entry` | count | Number of establishment entries (births) |
| `estabs_exit` | count | Number of establishment exits (deaths) |
| `job_creation` | count | Gross job creation |
| `job_destruction` | count | Gross job destruction |
| `net_job_creation` | count | Net job creation (creation minus destruction) |
| `net_job_creation_rate` | ratio | Net job creation divided by total employment |

### BLS QCEW -- Private Sector (1990--2024, 74% coverage)

Source: Bureau of Labor Statistics, Quarterly Census of Employment and Wages. Filtered to private sector (own_code=5), total industry (industry_code=10).

| Column | Units | Description |
|--------|-------|-------------|
| `qcew_establishments` | count | Number of private-sector establishments |
| `qcew_employment` | count | Average annual employment (private sector) |
| `qcew_total_wages` | $ (nominal) | Total annual wages (private sector) |
| `qcew_avg_weekly_wage` | $ (nominal) | Average weekly wage (private sector) |
| `qcew_avg_annual_pay` | $ (nominal) | Average annual pay (private sector) |
| `qcew_total_wages_real` | $ (2020) | Deflated total wages |
| `qcew_avg_weekly_wage_real` | $ (2020) | Deflated average weekly wage |
| `qcew_avg_annual_pay_real` | $ (2020) | Deflated average annual pay |

### BLS LAUS (1990--2024, 74% coverage)

Source: Bureau of Labor Statistics, Local Area Unemployment Statistics. Annual averages from monthly data (period M13 or computed).

| Column | Units | Description |
|--------|-------|-------------|
| `laus_employment` | persons | Annual average employment |
| `laus_labor_force` | persons | Annual average labor force |
| `laus_unemployment` | persons | Annual average unemployment |
| `laus_unemployment_rate` | percent | Annual average unemployment rate |
| `emp_pop_ratio` | ratio | LAUS employment divided by BEA population |

### Census County Business Patterns (1986--2022, 79% coverage)

Source: Census Bureau, County Business Patterns. Covers establishments with paid employees.

| Column | Units | Description |
|--------|-------|-------------|
| `cbp_establishments` | count | Number of establishments |
| `cbp_employment` | count | Mid-March employment |
| `cbp_annual_payroll` | thousands of $ (nominal) | Total annual payroll |
| `cbp_annual_payroll_real` | thousands of $ (2020) | Deflated annual payroll |

### Census Building Permits Survey (1990--2024, 66% coverage)

Source: Census Bureau, Building Permits Survey. Single-family (1-unit) residential construction permits. Not all rural counties report.

| Column | Units | Description |
|--------|-------|-------------|
| `bps_buildings` | count | Number of 1-unit buildings authorized |
| `bps_units` | count | Number of 1-unit housing units authorized (equals buildings for 1-unit) |
| `bps_value` | $ (nominal) | Total valuation of 1-unit permits |
| `bps_value_real` | $ (2020) | Deflated permit valuation |

### Census Quarterly Workforce Indicators (~1995--2024, 64% coverage)

Source: Census Bureau, LEHD Quarterly Workforce Indicators. Annual aggregates from quarterly data. Subject to suppression in small counties.

| Column | Units | Description |
|--------|-------|-------------|
| `qwi_emp` | count (avg) | Average beginning-of-quarter employment |
| `qwi_hires` | count | New hires during the year |
| `qwi_separations` | count | Separations during the year |
| `qwi_earnings` | $ (nominal) | Average monthly earnings |
| `qwi_earnings_real` | $ (2020) | Deflated average monthly earnings |

### IRS Statistics of Income (2011--2022, 23% coverage)

Source: IRS, Statistics of Income Division, County-Level Data. Limited to years with available bulk downloads.

| Column | Units | Description |
|--------|-------|-------------|
| `irs_returns` | count | Number of individual income tax returns filed |
| `irs_agi` | thousands of $ (nominal) | Adjusted gross income |
| `irs_wages` | thousands of $ (nominal) | Wages and salaries |
| `irs_agi_real` | thousands of $ (2020) | Deflated adjusted gross income |
| `irs_wages_real` | thousands of $ (2020) | Deflated wages and salaries |

### Census ACS / Decennial Covariates (2000--2023, forward/back-filled)

Source: Census Bureau, American Community Survey (5-year estimates, 2009+) and Decennial Census (2000, 2010, 2020). Values are forward-filled and back-filled within each county to cover the full panel period.

| Column | Units | Description |
|--------|-------|-------------|
| `total_population` | persons | Total population (ACS/Decennial) |
| `median_household_income` | $ (nominal) | Median household income |
| `poverty_rate` | proportion (0--1) | Poverty count / total population |
| `college_share` | proportion (0--1) | Population with bachelor's degree or higher / total population |
| `housing_units` | count | Total housing units |
| `median_home_value` | $ (nominal) | Median value of owner-occupied housing units |
| `vacant_housing_units` | count | Vacant housing units |
| `median_household_income_real` | $ (2020) | Deflated median household income |
| `median_home_value_real` | $ (2020) | Deflated median home value |

---

### Notes

- **Coverage percentages** reflect the fraction of the 11,938 county-year observations with non-null values. Coverage below 100% reflects years outside the source's temporal range, not missing counties within covered years.
- **Deflated columns** (suffix `_real`) are in constant 2020 dollars, computed using the BLS CPI-U All Items index (series CUUR0000SA0). The deflator is at `data/processed/deflator/cpi_deflator.parquet`.
- **ACS covariates** are forward/back-filled from their original availability (2000 Decennial, ACS 2009+) to cover the full 1978--2024 panel. Use with caution for pre-2000 years: values are imputed from the earliest observation.
- **BDS coverage** ends in 2023 because the 2024 vintage has not yet been released.
- **CBP coverage** ends in 2022 for the same reason.
- **IRS SOI** has low panel coverage (23%) because only 2011--2022 county-level files were downloadable. This source is supplementary.

---

## DiD Panel: ZIP Code Business Patterns

**File:** `data/processed/panels/did_zbp_panel.csv`

**Unit of observation:** ZIP code-year (one row per ZIP per year)

**Dimensions:** 351 rows (13 ZIPs x 27 years, 1994--2020), 15 columns

**Built by:** `notebooks/03_did_zbp_analysis.ipynb`

**Estimated by:** `R/03_did_zbp.R`

| Column | Type | Description |
|--------|------|-------------|
| `zip` | string | 5-digit ZIP code |
| `year` | integer | Calendar year (1994--2020) |
| `zip_label` | string | ZIP code with place name (e.g., "78130 NB Downtown") |
| `comal` | integer (0/1) | 1 if ZIP is in Comal County |
| `estab` | integer | Number of business establishments (Census ZBP) |
| `emp` | integer | Total employment (Census ZBP, subject to noise-infusion post-2007) |
| `payann_real` | float | Annual payroll in constant 2020 dollars (thousands) |
| `ln_estab` | float | log(establishments) |
| `ln_emp` | float | log(employment) |
| `ln_payann_real` | float | log(payroll, real 2020$) |
| `treated` | integer (0/1) | 1 if NFIP payouts > $500K in Oct--Nov 1998 (ZIPs: 78130, 78131, 78132, 78163) |
| `intensity` | float | ln(total NFIP payouts + 1), continuous treatment intensity |
| `post` | integer (0/1) | 1 if year >= 1999 (post-flood) |
| `nfip_total_paid` | float | Total NFIP insurance payouts for Oct--Nov 1998 flood ($) |
| `nfip_claims` | integer | Number of NFIP claims for Oct--Nov 1998 flood |

**Treatment assignment:** 4 treated ZIPs (78130, 78131, 78132, 78163) with >$500K in NFIP payouts. 9 control ZIPs: 3 low-damage Comal County (78133, 78266, 78070) + 6 adjacent county (78006 Boerne, 78013 Comfort, 78015 Fair Oaks Ranch, 78610 Buda, 78620 Dripping Springs, 78640 Kyle).

---

## DiD Panel: Housing Price Index (ZIP Level)

**File:** `data/processed/panels/did_hpi_zip_panel.csv`

**Unit of observation:** ZIP code-year

**Dimensions:** 391 rows (12 ZIPs x ~33 years, 1990--2024), 12 columns

**Built by:** `notebooks/04_housing_hpi_analysis.ipynb`

**Estimated by:** `R/04_did_hpi.R`

| Column | Type | Description |
|--------|------|-------------|
| `zip` | string | 5-digit ZIP code |
| `year` | integer | Calendar year |
| `zip_label` | string | ZIP code with place name |
| `comal` | integer (0/1) | 1 if ZIP is in Comal County |
| `hpi_val` | float | FHFA All-Transactions House Price Index (base = 100 at first observation) |
| `ln_hpi` | float | log(HPI) |
| `annual_change` | float | Year-over-year percent change in HPI |
| `treated` | integer (0/1) | 1 if NFIP payouts > $500K in Oct--Nov 1998 |
| `intensity` | float | ln(total NFIP payouts + 1) |
| `post` | integer (0/1) | 1 if year >= 1999 |
| `nfip_total_paid` | float | Total NFIP payouts ($) |
| `nfip_claims` | integer | Number of NFIP claims |

**Note:** ZIP 78131 excluded (insufficient repeat-sales transactions for FHFA to compute index). 3 treated ZIPs, 9 controls.

---

## DiD Panel: Housing Price Index (Census Tract Level)

**File:** `data/processed/panels/did_hpi_tract_panel.csv`

**Unit of observation:** Census tract-year

**Dimensions:** 1,037 rows (32 tracts x ~33 years), 11 columns

**Built by:** `notebooks/04_housing_hpi_analysis.ipynb`

**Estimated by:** `R/04_did_hpi.R`

| Column | Type | Description |
|--------|------|-------------|
| `tract` | string | 11-digit census tract FIPS code |
| `year` | integer | Calendar year |
| `comal` | integer (0/1) | 1 if tract is in Comal County |
| `hpi` | float | FHFA Census Tract House Price Index |
| `ln_hpi` | float | log(HPI) |
| `annual_change` | float | Year-over-year percent change in HPI |
| `treated` | integer (0/1) | 1 if tract is in a treated ZIP (NFIP payouts > $200K) |
| `intensity` | float | ln(total NFIP payouts + 1), aggregated from ZIP-level damage |
| `post` | integer (0/1) | 1 if year >= 1999 |
| `nfip_total_paid` | float | Total NFIP payouts ($) |
| `nfip_claims` | integer | Number of NFIP claims |

**Note:** 2 treated tracts, 30 control tracts (15 Comal + 17 Hays/Kendall). Only tracts with HPI observations starting <= 1996 included.

---

## Results Files

### SCM Results (`data/results/`)

| File | Description |
|------|-------------|
| `scm_balance.csv` | Predictor balance: treated vs synthetic (covariate means) |
| `scm_gap.csv` | Gap series: actual minus synthetic per-capita income by year |
| `scm_synthetic_series.csv` | Treated and synthetic PCI series (1990--2022) |
| `scm_weights.csv` | Donor county weights (FIPS, weight) |
| `scm_pretrend_rmspe.txt` | Pre-treatment RMSPE statistic |
| `placebo_gaps.csv` | In-space placebo gap series for all donors |
| `placebo_rmspe_ratios.csv` | Post/pre RMSPE ratios for permutation inference |
| `loo_synthetic_series.csv` | Leave-one-out synthetic series |
| `intime_gap.csv` | In-time placebo gap series |

### ZBP DiD Results (`data/results/`)

| File | Description |
|------|-------------|
| `did_binary_results.txt` | Binary DiD regression tables (establishments, employment, payroll) |
| `did_event_study.csv` | Event study coefficients + 95% CIs by outcome and year |
| `did_intensity_results.txt` | Continuous intensity DiD regression tables |

### HPI DiD Results (`data/results/`)

| File | Description |
|------|-------------|
| `did_hpi_binary_results.txt` | ZIP + tract DiD regression tables |
| `did_hpi_event_study.csv` | Event study coefficients for ZIP and tract levels |
| `hpi_recovery_timing.csv` | Recovery timing by treated ZIP (years to HPI >= counterfactual) |

### Figures (`data/results/figures/`)

| File | Description |
|------|-------------|
| `01_trends.png` | Treated vs synthetic PCI trends |
| `02_gap.png` | Gap series (actual - synthetic) |
| `03_weights.png` | Donor county weights bar chart |
| `04_placebo_gaps.png` | In-space placebo gap series |
| `05_rmspe_ratio.png` | RMSPE ratio distribution |
| `06_leave_one_out.png` | Leave-one-out robustness |
| `07_intime_placebo.png` | In-time placebo (shifted treatment year) |
| `did_raw_trends.png` | Raw establishment trends (treated vs control ZIPs) |
| `did_indexed_trends.png` | Indexed trends (1998 = 100) |
| `did_zip_trends.png` | Individual ZIP trend lines |
| `did_event_study_estab.png` | Event study: establishments |
| `did_event_study_emp.png` | Event study: employment |
| `did_event_study_payroll.png` | Event study: payroll |
| `did_event_study_robustness.png` | Event study: full panel vs Comal-only |
| `hpi_all_zips.png` | Raw HPI trends for all study ZIPs |
| `hpi_normalized_trends.png` | Normalized HPI trends (1998 = 100) |
| `hpi_counterfactual_trajectories.png` | Donor-based counterfactual vs actual HPI |
| `hpi_recovery_gap.png` | Recovery gap ratio (actual / expected) over time |
| `did_hpi_event_study.png` | HPI event study (ZIP level) |
| `did_hpi_event_study_tract.png` | HPI event study (tract level) |
| `did_hpi_event_study_combined.png` | HPI event study (ZIP + tract overlay) |

---

## Raw Data Inventory

Raw data files are stored in `data/raw/` (gitignored). Each subdirectory corresponds to one acquisition module:

| Directory | Source | Key Files |
|-----------|--------|-----------|
| `bea_income/` | BEA Regional Income | `bea_income_tx_counties.parquet` |
| `bls_laus/` | BLS Local Area Unemployment | `laus_tx_counties.parquet` |
| `bls_qcew/` | BLS QCEW | `qcew_tx_counties.parquet` |
| `census_acs/` | Census ACS + Decennial | `census_covariates_tx.parquet` |
| `census_bds/` | Census Business Dynamics | `bds_tx_counties.parquet` |
| `census_bps/` | Census Building Permits | `bps_tx_counties.parquet` |
| `census_cbp/` | Census County Business Patterns | `cbp_tx_counties.parquet` |
| `census_qwi/` | Census QWI | `qwi_tx_counties.parquet` |
| `fema_declarations/` | FEMA Disaster Declarations | `tx_declarations.parquet`, `donor_pool.parquet` |
| `fema_hma/` | FEMA Hazard Mitigation | `hma_texas.parquet` |
| `fema_ia/` | FEMA Individual Assistance | `ia_dr1257.parquet` |
| `fema_nfip/` | FEMA Flood Insurance Claims | `nfip_tx_1998.parquet`, `nfip_tx_1990_2005.parquet` |
| `fema_pa/` | FEMA Public Assistance | `pa_dr1257.parquet`, `pa_tx_floods.parquet` |
| `irs_soi/` | IRS Statistics of Income | `irs_soi_tx_counties.parquet` |
| `noaa_storms/` | NOAA Storm Events | `storm_events_tx.parquet` |
| `sba_loans/` | SBA Disaster Loans | `sba_loans_tx.parquet` |
| `usgs_nwis/` | USGS Stream Gauges | `guadalupe_daily_discharge.parquet` |
| `census_zbp/` | Census ZIP Business Patterns | `zbp_tx_zips.parquet` (64,502 rows, 2,628 TX ZIPs, 1994--2020) |
| `fhfa_hpi/` | FHFA House Price Index | `hpi_at_zip5.xlsx` (ZIP5 level), `hpi_at_tract.csv` (census tract level) |

## Processed Data

Processed data files are in `data/processed/` (gitignored):

| Directory | Contents |
|-----------|----------|
| `panels/` | Harmonized per-source panels, merged `scm_panel`, DiD panels, screening outputs |
| `deflator/` | `cpi_deflator.parquet` (CPI-U annual index + deflator, 1913--2025) |
