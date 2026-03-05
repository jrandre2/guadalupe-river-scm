# Methods Supplement

Detailed documentation of data pipeline decisions, donor pool screening, DiD design choices, and robustness check rationale for the Guadalupe River SCM Recovery project.

---

## 1. Data Pipeline

### 1.1 Overview

The pipeline runs in three stages, each producing intermediate outputs:

```
Stage 1: Acquisition
  22 source modules → data/raw/<source>/*.parquet

Stage 2: Harmonization
  src/process/harmonize_county.py → data/processed/panels/<source>.parquet
  (standardized: county × year long format, 5-digit FIPS, integer year)

Stage 3: Panel Build
  src/process/panel_builder.py → data/processed/panels/scm_panel.parquet
  (merged, deflated, derived variables added)
```

### 1.2 Acquisition Phases

The pipeline organizes 22 sources into three dependency phases:

**Phase 1 — Anchor datasets** (run first; others depend on them)
- `bea_income`: BEA per capita income, 1969–2024, all 254 TX counties — the backbone of the SCM panel
- `donor_pool`: Pre-computed donor eligibility flags from notebook 02
- `census_bds`: Census Business Dynamics Statistics, 1978+
- `bls_qcew`: BLS Quarterly Census of Employment and Wages, 1990+
- `bls_laus`: BLS Local Area Unemployment Statistics, 1990+

**Phase 2 — Extended indicators**
- `census_cbp`, `census_zbp`, `census_bps`, `census_qwi`, `census_acs`
- `irs_soi`: IRS Statistics of Income, 2011+
- `fema_pa`, `fema_ia`, `fema_hma`, `fema_nfip`: FEMA disaster funding and flood insurance
- `sba_loans`, `noaa_storms`, `usgs_nwis`: SBA loans, storm records, stream gauges

**Phase 3 — Manual/semi-automated sources**
- `hud_cdbgdr`: HUD CDBG-DR grants (requires manual download — DRGR portal)
- `tx_comptroller`, `usa_spending`: Texas state and federal spending data

### 1.3 BEA as the Panel Skeleton

BEA personal income data was chosen as the skeleton for the merged panel because it:
1. Covers all 254 Texas counties continuously from 1969 to 2024
2. Is the primary SCM outcome variable (per capita income)
3. Provides consistent county FIPS codes across the full study period

All other datasets are left-joined to the BEA skeleton on `(fips, year)`. Missing values for a given source-year-county combination are retained as `NaN` rather than dropped, preserving the balanced panel structure.

### 1.4 ACS Covariate Imputation

American Community Survey covariates (poverty rate, educational attainment, median household income, median home value) are only available from:
- 2000 Decennial Census (single cross-section)
- ACS 5-year estimates, 2009–2024

For SCM pre-treatment predictor matching (1990–1998), these values would otherwise be missing. The panel builder applies:
1. **Forward-fill** within each county: fills post-2009 values backward from the earliest observation to 2000
2. **Back-fill** within each county: fills the 2000 Decennial value backward to 1978

This imputation treats socioeconomic structure as slowly-changing — reasonable for decade-scale characteristics like educational attainment but less so for cyclically sensitive variables like poverty rate. The ACS covariates are included in SCM predictor matching with awareness of this limitation.

**Code location:** `src/process/panel_builder.py`, lines 98–107

### 1.5 CPI Deflation

All nominal dollar values are deflated to constant **2020 dollars** using the CPI-U All Items series (FRED: `CPIAUCSL`).

The deflator is downloaded and computed in `src/process/deflator.py`:
- Source: BLS CPI-U monthly averages, converted to annual
- Base year: 2020 (deflator = 1.0 in 2020)
- Applied to 14 nominal columns: personal income, per capita income, net earnings, transfer receipts, QCEW wages, CBP payroll, BPS building value, QWI earnings, IRS AGI/wages, median household income, median home value

**Code location:** `src/process/panel_builder.py`, lines 119–134

---

## 2. Donor Pool Screening

**Notebook:** `notebooks/02_donor_screening.ipynb`

### 2.1 Starting Pool

All 254 Texas counties are candidate donors.

### 2.2 Exclusions — Structural Incomparability

Excluded immediately because their economies are structurally different from a mid-size Texas county:
- **Travis** (Austin), **Harris** (Houston), **Bexar** (San Antonio), **Dallas**, **Tarrant** (Fort Worth): Major metro cores with large-city dynamics
- **Comal** itself (the treated unit)

Remaining: ~248 counties

### 2.3 Exclusions — Data Coverage

Require complete BEA per capita income data for the full pre-treatment window (1978–1998). Counties with missing BEA coverage for more than 2 years in the pre-period are dropped.

Remaining: ~171 counties → recorded in `data/results/scm_weights.csv` as `donor_eligible`

### 2.4 Exclusions — Concurrent Flood Events

Counties with their own major flood disasters between 1995–2001 (identified via FEMA declarations) are excluded to avoid contaminating the donor pool with treated units.

This filter is applied during SCM estimation (R/01), not at the panel build stage.

### 2.5 Growth Rate Matching

To improve SCM pre-treatment fit and reduce the risk of extrapolation, donors are further screened to counties with similar pre-treatment per-capita income growth trajectories to Comal.

**Criterion:** Annual PCI growth rate for 1978–1998 within ±1.5 standard deviations of Comal County's rate.

This reduces the pool to ~69 counties.

### 2.6 Final Review

A manual inspection of the remaining ~69 donors removes counties with:
- Large one-time structural breaks (e.g., oil boom/bust counties with extreme PCI volatility)
- Obvious data quality anomalies in the BEA series

**Final donor pool:** 31 counties

These 31 donors are used for all primary SCM inference (R/01, R/02, R/05). The full 69-county growth-matched pool is used only for the supplementary p-value comparison.

---

## 3. Difference-in-Differences Design

### 3.1 ZIP-Level Business Activity DiD (R/03)

**Data source:** Census ZIP Code Business Patterns (ZBP), 1994–2020

**Treatment assignment:**
- **Treated ZIPs (4):** 78130 (New Braunfels Downtown/Gruene, $17M NFIP), 78131, 78132 ($2.6M), 78163 ($1.1M)
  - Criterion: >$500K in NFIP payouts for the 1998 Texas event
  - These four ZIPs account for >95% of Comal County's total NFIP claims from DR-1257-TX
- **Control ZIPs (9):** ZIPs within Comal County and adjacent counties (Guadalupe, Hays, Kendall) with <$130K NFIP payouts
  - Selection: within the same regional labor market, similar pre-flood economic profile, minimal flood exposure

**$500K threshold rationale:** Chosen to identify ZIPs where flood damage was economically material (large enough to affect business behavior) and clearly separable from incidental minor claims. The distribution of NFIP payouts across Comal ZIPs shows a natural break between the four high-damage ZIPs (>$1M) and the rest (<$200K), making the $500K threshold conservative and robust.

**Panel:** 13 ZIPs × 27 years = 351 observations (1994–2020)

**Specification:**
```
ln(Y_it) = α_i + λ_t + β(Treated_i × Post_t) + ε_it
```
- Outcomes: log establishments (`ln_estab`), log employment (`ln_emp`), log payroll (`ln_payroll`)
- FEs: ZIP (`α_i`) + year (`λ_t`)
- Treatment dummy: 1 if ZIP in treated set AND year ≥ 1999
- Event study: year-by-year interaction with 1998 as reference year
- SEs: clustered by ZIP

**Panel construction:** `notebooks/03_did_zbp_analysis.ipynb` (crosswalks NFIP claims to ZIPs, filters to Comal + adjacent, assigns treatment)

### 3.2 Housing Value DiD — ZIP Level (R/04)

**Data source:** FHFA House Price Index, repeat-sales, ZIP5-level

**Treatment assignment:**
- **Treated ZIPs (3):** 78130, 78132, 78163 (NFIP >$500K, same as ZBP treated)
- **Control ZIPs (9):** Same control ZIPs as ZBP DiD

**Panel:** 12 ZIPs × varying years (1987–2024) = 391 observations

### 3.3 Housing Value DiD — Tract Level (R/04)

**Data source:** FHFA House Price Index, repeat-sales, census tract-level

**Treatment assignment:**
- **Treated tracts (2):** Census tracts overlapping 78130 and 78132 boundaries, with NFIP payout intensity above $200K
  - Lower threshold than ZIP ($200K vs. $500K) because tract-level NFIP payouts are smaller in aggregate
  - Chosen to retain at least 2 treated tracts (minimum for DiD identification)
- **Control tracts (30):** Tracts in Comal and adjacent counties with minimal flood exposure

**Panel:** 32 tracts × varying years (1992–2024) = 1,037 observations

**Intensity specification (tract):**
```
ln(HPI_it) = α_i + λ_t + γ(ln(NFIP_i) × Post_t) + ε_it
```
where `ln(NFIP_i)` is the log of total NFIP payouts for the tract during DR-1257-TX.

**Reference year for event studies:** 1998 (treatment year) for both ZIP and tract models.

### 3.4 Pre-Trend Diagnostics

Formal pre-trend tests were added to all DiD specifications (R/03, R/04) to assess the parallel trends assumption. Additional investigations decomposed the ZBP pre-trend pattern by outcome and sample.

#### 3.4.1 Joint Wald Tests

Joint Wald tests of the null hypothesis that all pre-treatment event-study coefficients equal zero:

| Model | F-statistic | p-value | Interpretation |
|-------|-------------|---------|----------------|
| ZBP establishments | F(4, 286) = 1.326 | 0.260 | Not rejected |
| ZBP employment | F(4, 286) = 1.301 | 0.270 | Not rejected |
| ZBP payroll | F(4, 286) = 1.888 | 0.113 | Not rejected |
| HPI ZIP | F(8, 309) = 10.841 | <0.001 | **Rejected** |
| HPI Tract | F(6, 911) = 11.032 | <0.001 | **Rejected** |

The ZBP panel does not reject parallel trends at conventional levels despite the visual downward pattern in establishment coefficients. The HPI panels reject parallel trends at both geographic levels, motivating the HonestDiD sensitivity analysis in §4.3.

**Code location:** R/03 lines 147–158 (ZBP), R/04 lines 108–119 and 222–233 (HPI)

#### 3.4.2 ZBP Establishment Pre-Trend Investigation

The ZBP establishment event study shows a visually monotone decline in pre-treatment coefficients (1994: +0.48, 1995: +0.34, 1996: +0.20, 1997: +0.06), yet the joint Wald test does not reject (p = 0.260). This apparent contradiction was investigated through four additional tests.

**Linear pre-trend test.** A regression of log establishments on `treated × relative_year` in the pre-period (1994–1998) estimates the slope of the pre-trend:

| Outcome | Slope | SE | p-value |
|---------|-------|----|---------|
| Establishments | -0.125 | 0.075 | 0.121 |
| Employment | -0.566 | 0.462 | 0.243 |
| Payroll | -1.112 | 0.960 | 0.263 |

No outcome shows a statistically significant linear pre-trend, though the establishment slope (−0.125 per year) is suggestive. The test is underpowered given 13 clusters and only 4 pre-treatment periods.

**Placebo treatment tests.** Assigning a false treatment at 1996 (excluding post-1998 data) yields a placebo coefficient of −0.325 (p = 0.122); a false treatment at 1997 yields −0.312 (p = 0.133). Neither is significant, consistent with the absence of a discrete pre-treatment structural break.

**Outcome decomposition.** The monotone declining pattern is specific to establishments. Employment and payroll pre-treatment coefficients show flat level shifts (employment: ~+2.5, payroll: ~+5.1 across all pre-years) rather than trends. This indicates the pattern reflects firm-count dynamics (entry/exit convergence) rather than a broad economic shock affecting treated ZIPs.

**Comal-only subsample.** Restricting the panel to the 7 Comal County ZIPs (excluding the 6 adjacent-county controls) produces a similar pattern (1994: +0.326, 1995: +0.253, 1996: +0.179, 1997: +0.031) with Wald p = 0.342 and linear trend p = 0.235. The pre-trend is not an artifact of cross-county composition.

**Interpretation.** The establishment pre-trend is best characterized as mean reversion in firm counts: treated ZIPs had relatively more establishments in the early 1990s and were converging toward the control group before the flood. This is a common pattern in business dynamics data where firm entry/exit rates exhibit regression to the mean. Because the trend is establishment-specific (not present in employment or payroll) and within-county (not driven by cross-county differences), it does not indicate a confounding pre-treatment shock.

---

## 4. Robustness Check Rationale

### 4.1 Augmented SCM (R/05)

**Why:** Traditional SCM can produce biased estimates when pre-treatment fit is imperfect. Even with RMSPE $660 (1.68% of mean), the SCM weights may over-fit to pre-treatment noise, and the standard permutation-based p-value has limited power in small donor pools.

**Method:** Ridge-augmented SCM (Ben-Michael, Feller & Rothstein 2021, *JASA*). Augments the SCM estimate with a ridge regression bias correction term. Provides:
- Bias-corrected ATT estimates that are more robust to model misspecification
- Asymptotically valid confidence intervals via conformal inference (jackknife+)
- L2 imbalance metric as a companion to RMSPE

**Implementation:** `augsynth` R package (GitHub), loaded from `~/R/library`

**Key result:** Average ATT = +$6,248 (p = 0.107); CIs include zero in every post-treatment year. Consistent with main SCM result.

### 4.2 Wild Cluster Bootstrap (R/06)

**Why:** Standard cluster-robust standard errors (CR1/CR2) are unreliable when the number of clusters is small (< 20, and especially < 10). The ZBP DiD panel has only 13 clusters (ZIPs); the HPI ZIP panel has 12; only the tract panel has 32.

**Method:** Wild cluster bootstrap with Webb 6-point weights (Roodman, Nielsen, MacKinnon & Webb 2019, *Stata Journal*), B = 99,999 replications. More reliable p-values and CIs under small-cluster asymptotics.

**Implementation note:** `fwildclusterboot` R package (GitHub) is incompatible with `feols()` in the version installed on R 4.1.0. Workaround: re-estimate models using `lm()` with explicit `factor(fips) + factor(year)` fixed effects, then pass to `boottest()`. Results should be numerically identical to `feols()`.

**Key results:** All p-values similar to cluster-robust. The one marginally significant result (tract intensity DiD, p(CR) = 0.087) weakens to p(boot) = 0.135, confirming no significant effects.

### 4.3 HonestDiD Pre-Trend Sensitivity (R/07)

**Why:** Joint Wald tests of pre-treatment event-study coefficients reject parallel trends for HPI at both ZIP (F(8,309) = 10.8, p < 0.001) and tract (F(6,911) = 11.0, p < 0.001) levels. The ZBP pre-trend test is not significant (F(4,286) = 1.3, p = 0.260), but the visual pattern of declining coefficients (1994–1997: +0.48, +0.34, +0.20, +0.06) still warrants sensitivity analysis. HonestDiD provides confidence intervals that remain valid even if pre-trends are extrapolated into the post-period.

**Method:** Rambachan & Roth (2023, *Review of Economic Studies*). Two restrictions:

1. **Smoothness restriction (Δ^SD):** The post-period trend deviation is bounded by M times the maximum absolute pre-period deviation. M = 0 recovers the standard parallel trends assumption. As M increases, the CI widens.
   - M = 0.02 is the smallest value at which CIs include zero (i.e., where significance first disappears)

2. **Relative magnitudes restriction (Δ^RM):** The post-period deviation is bounded by Mbar times the maximum pre-period deviation. Mbar = 0 requires exact parallel trends; Mbar = 1 allows post-deviations as large as the largest pre-deviation.

**Implementation:** `HonestDiD` R package (GitHub), loaded from `~/R/library`

**Key result:** CIs include zero at M = 0.02 (Δ^SD) and at all Mbar values (Δ^RM). Significance is highly fragile to even minimal pre-trend violations, consistent with the null finding.

---

## 5. Reference List

- Ben-Michael, E., Feller, A., & Rothstein, J. (2021). The augmented synthetic control method. *Journal of the American Statistical Association*, 116(536), 1789–1803.
- Rambachan, A., & Roth, J. (2023). A more credible approach to parallel trends. *Review of Economic Studies*, 90(5), 2555–2591.
- Roodman, D., Nielsen, M. Ø., MacKinnon, J. G., & Webb, M. D. (2019). Fast and wild: Bootstrap inference in Stata using boottest. *Stata Journal*, 19(1), 4–60.
- Abadie, A., Diamond, A., & Hainmueller, J. (2010). Synthetic control methods for comparative case studies. *Journal of the American Statistical Association*, 105(490), 493–505.
