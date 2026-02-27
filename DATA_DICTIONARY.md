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

## Processed Data

Processed data files are in `data/processed/` (gitignored):

| Directory | Contents |
|-----------|----------|
| `panels/` | Harmonized per-source panels + merged `scm_panel.parquet` + `availability_matrix.csv` |
| `deflator/` | `cpi_deflator.parquet` (CPI-U annual index + deflator, 1913--2025) |
