"""Harmonize raw datasets into standardized county-year panels.

Each harmonization function:
- Loads raw parquet data
- Standardizes to 5-digit zero-padded FIPS + integer year
- Removes state-level entries
- Selects and renames key variables
- Saves to data/processed/
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import DATA_DIR, RAW_DIR, PROCESSED_DIR
from src.utils.file_io import load_parquet, save_parquet
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def _standardize_fips(df: pd.DataFrame, fips_col: str = "fips") -> pd.DataFrame:
    """Ensure FIPS is a 5-digit zero-padded string, remove non-county entries."""
    df = df.copy()
    df[fips_col] = df[fips_col].astype(str).str.strip().str.zfill(5)
    # Keep only TX county FIPS (48xxx, not 48000 state-level)
    mask = (
        df[fips_col].str.startswith("48")
        & (df[fips_col].str.len() == 5)
        & (df[fips_col] != "48000")
    )
    dropped = (~mask).sum()
    if dropped > 0:
        log.debug("fips_filtered", dropped=dropped)
    return df[mask].copy()


def _ensure_year_int(df: pd.DataFrame, year_col: str = "year") -> pd.DataFrame:
    """Convert year column to integer."""
    df = df.copy()
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")
    df = df.dropna(subset=[year_col])
    df[year_col] = df[year_col].astype(int)
    return df


def _load_raw(source_dir: str, filename: str) -> pd.DataFrame | None:
    """Load a raw parquet file, returning None if missing."""
    path = RAW_DIR / source_dir / filename
    if not path.exists():
        log.warning("raw_file_missing", path=str(path))
        return None
    return load_parquet(path)


def _save_processed(df: pd.DataFrame, name: str) -> Path:
    """Save harmonized data to processed directory."""
    out_dir = PROCESSED_DIR / "panels"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.parquet"
    save_parquet(df, path, source=name)
    return path


# ── BEA Personal Income ──────────────────────────────────────────────

def harmonize_bea() -> pd.DataFrame | None:
    """BEA CAINC1+CAINC4 merged income data. Annual, 1969+."""
    df = _load_raw("bea_income", "bea_income_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    # Convert string columns to numeric
    for col in ["personal_income", "population", "per_capita_income",
                "net_earnings", "dividends_interest_rent", "transfer_receipts"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "personal_income", "population", "per_capita_income",
            "net_earnings", "transfer_receipts"]
    df = df[[c for c in keep if c in df.columns]]

    _save_processed(df, "bea_income")
    log.info("harmonized_bea", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── Census BDS ────────────────────────────────────────────────────────

def harmonize_bds() -> pd.DataFrame | None:
    """Census Business Dynamics Statistics. Annual, 1978+."""
    df = _load_raw("census_bds", "bds_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["firms", "estabs", "emp", "estabs_entry", "estabs_exit",
                "job_creation", "job_destruction", "net_job_creation"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "firms", "estabs", "emp",
            "estabs_entry", "estabs_exit", "job_creation", "job_destruction",
            "net_job_creation"]
    df = df[[c for c in keep if c in df.columns]]

    _save_processed(df, "bds")
    log.info("harmonized_bds", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── BLS QCEW ─────────────────────────────────────────────────────────

def harmonize_qcew() -> pd.DataFrame | None:
    """BLS Quarterly Census of Employment & Wages. Annual, 1990+.

    Uses own_code=5 (private sector) and industry_code=10 (total private).
    """
    df = _load_raw("bls_qcew", "qcew_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    # Filter to private-sector total (own_code=5, industry_code=10)
    # own_code 0 = all, 5 = private
    mask = (df["own_code"].astype(str) == "5") & (df["industry_code"].astype(str) == "10")
    df_private = df[mask].copy()

    if df_private.empty:
        log.warning("qcew_no_private_data")
        # Fall back to own_code=0 (total covered)
        df_private = df[
            (df["own_code"].astype(str) == "0") & (df["industry_code"].astype(str) == "10")
        ].copy()

    for col in ["establishments", "employment", "total_wages", "avg_weekly_wage", "avg_annual_pay"]:
        if col in df_private.columns:
            df_private[col] = pd.to_numeric(df_private[col], errors="coerce")

    keep = ["fips", "year", "establishments", "employment", "total_wages",
            "avg_weekly_wage", "avg_annual_pay"]
    df_private = df_private[[c for c in keep if c in df_private.columns]]

    # Rename to distinguish from other employment sources
    df_private = df_private.rename(columns={
        "establishments": "qcew_establishments",
        "employment": "qcew_employment",
        "total_wages": "qcew_total_wages",
        "avg_weekly_wage": "qcew_avg_weekly_wage",
        "avg_annual_pay": "qcew_avg_annual_pay",
    })

    _save_processed(df_private, "qcew")
    log.info("harmonized_qcew", rows=len(df_private), counties=df_private["fips"].nunique(),
             years=f"{df_private['year'].min()}-{df_private['year'].max()}")
    return df_private


# ── BLS LAUS ──────────────────────────────────────────────────────────

def harmonize_laus() -> pd.DataFrame | None:
    """BLS Local Area Unemployment Statistics. Annual, 1990+."""
    df = _load_raw("bls_laus", "laus_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["employment", "labor_force", "unemployment", "unemployment_rate"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "employment", "labor_force",
            "unemployment", "unemployment_rate"]
    df = df[[c for c in keep if c in df.columns]]

    # Rename to distinguish
    df = df.rename(columns={
        "employment": "laus_employment",
        "labor_force": "laus_labor_force",
        "unemployment": "laus_unemployment",
        "unemployment_rate": "laus_unemployment_rate",
    })

    _save_processed(df, "laus")
    log.info("harmonized_laus", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── Census CBP ────────────────────────────────────────────────────────

def harmonize_cbp() -> pd.DataFrame | None:
    """Census County Business Patterns. Annual, 1986+."""
    df = _load_raw("census_cbp", "cbp_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["establishments", "employment", "annual_payroll"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "establishments", "employment", "annual_payroll"]
    df = df[[c for c in keep if c in df.columns]]

    df = df.rename(columns={
        "establishments": "cbp_establishments",
        "employment": "cbp_employment",
        "annual_payroll": "cbp_annual_payroll",
    })

    _save_processed(df, "cbp")
    log.info("harmonized_cbp", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── Census BPS ────────────────────────────────────────────────────────

def harmonize_bps() -> pd.DataFrame | None:
    """Census Building Permits Survey. Annual, 1990+."""
    df = _load_raw("census_bps", "bps_tx_counties.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["buildings", "units", "value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "buildings", "units", "value"]
    df = df[[c for c in keep if c in df.columns]]

    df = df.rename(columns={
        "buildings": "bps_buildings",
        "units": "bps_units",
        "value": "bps_value",
    })

    _save_processed(df, "bps")
    log.info("harmonized_bps", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── Census QWI ────────────────────────────────────────────────────────

def harmonize_qwi() -> pd.DataFrame | None:
    """Census Quarterly Workforce Indicators. Annual, ~1995+."""
    df = _load_raw("census_qwi", "qwi_tx_counties.parquet")
    if df is None:
        return None

    # QWI has '00048' as state-level entry
    df["fips"] = df["fips"].astype(str).str.strip()
    df = df[~df["fips"].isin(["00048", "48000"])].copy()
    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["emp", "empend", "emps", "hira", "sep", "earns"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "emp", "hira", "sep", "earns"]
    df = df[[c for c in keep if c in df.columns]]

    df = df.rename(columns={
        "emp": "qwi_emp",
        "hira": "qwi_hires",
        "sep": "qwi_separations",
        "earns": "qwi_earnings",
    })

    _save_processed(df, "qwi")
    log.info("harmonized_qwi", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── IRS SOI ───────────────────────────────────────────────────────────

def harmonize_irs() -> pd.DataFrame | None:
    """IRS Statistics of Income. Annual, 2011+."""
    df = _load_raw("irs_soi", "irs_soi_tx_counties.parquet")
    if df is None:
        return None

    # IRS uses 'tax_year' instead of 'year'
    if "tax_year" in df.columns:
        df = df.rename(columns={"tax_year": "year"})

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["num_returns", "num_exemptions", "agi", "wages_salaries"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["fips", "year", "num_returns", "agi", "wages_salaries"]
    df = df[[c for c in keep if c in df.columns]]

    df = df.rename(columns={
        "num_returns": "irs_returns",
        "agi": "irs_agi",
        "wages_salaries": "irs_wages",
    })

    _save_processed(df, "irs_soi")
    log.info("harmonized_irs", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── Census ACS ────────────────────────────────────────────────────────

def harmonize_acs() -> pd.DataFrame | None:
    """Census ACS/Decennial covariates. 2000+."""
    df = _load_raw("census_acs", "census_covariates_tx.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)
    df = _ensure_year_int(df)

    for col in ["total_population", "median_household_income", "poverty_count",
                "housing_units", "median_home_value", "college_plus",
                "vacant_housing_units", "unemployed_pop"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute derived rates
    if "poverty_count" in df.columns and "total_population" in df.columns:
        df["poverty_rate"] = df["poverty_count"] / df["total_population"]
    if "college_plus" in df.columns and "total_population" in df.columns:
        df["college_share"] = df["college_plus"] / df["total_population"]

    keep = ["fips", "year", "total_population", "median_household_income",
            "poverty_rate", "college_share", "housing_units",
            "median_home_value", "vacant_housing_units"]
    df = df[[c for c in keep if c in df.columns]]

    _save_processed(df, "acs_covariates")
    log.info("harmonized_acs", rows=len(df), counties=df["fips"].nunique(),
             years=f"{df['year'].min()}-{df['year'].max()}")
    return df


# ── FEMA Disaster Funding (PA) ────────────────────────────────────────

def harmonize_fema_pa() -> pd.DataFrame | None:
    """FEMA Public Assistance for DR-1257. County-level obligations."""
    df = _load_raw("fema_pa", "pa_dr1257.parquet")
    if df is None:
        return None

    # PA data uses county names, need to map to FIPS
    # Check for FIPS-like columns
    if "county" in df.columns and "fips" not in df.columns:
        # Load crosswalk
        crosswalk_path = DATA_DIR.parent / "config" / "fips_crosswalk.csv"
        if crosswalk_path.exists():
            xwalk = pd.read_csv(crosswalk_path, dtype=str)
            # Try to merge
            df["county_clean"] = df["county"].str.strip().str.lower()
            xwalk["county_clean"] = xwalk["county_name"].str.strip().str.lower()
            df = df.merge(xwalk[["county_clean", "fips"]], on="county_clean", how="left")
        else:
            log.warning("fema_pa_no_crosswalk")
            return None

    # Aggregate federal share by county
    if "federalShareObligated" in df.columns:
        for col in ["federalShareObligated", "totalObligated"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        agg = df.groupby("fips").agg(
            pa_federal_obligated=("federalShareObligated", "sum"),
            pa_total_obligated=("totalObligated", "sum"),
            pa_project_count=("fips", "size"),
        ).reset_index()

        if "fips" in agg.columns:
            agg = _standardize_fips(agg)
            _save_processed(agg, "fema_pa_dr1257")
            log.info("harmonized_fema_pa", rows=len(agg))
            return agg

    log.warning("fema_pa_no_funding_cols", cols=list(df.columns)[:15])
    return None


# ── Donor Pool ────────────────────────────────────────────────────────

def harmonize_donor_pool() -> pd.DataFrame | None:
    """Load donor pool eligibility flags."""
    df = _load_raw("fema_declarations", "donor_pool.parquet")
    if df is None:
        return None

    df = _standardize_fips(df)

    # Convert boolean columns
    for col in ["is_treated", "in_dr1257", "flood_1995_2001", "donor_eligible"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().isin(["true", "1", "yes"])

    keep = ["fips", "is_treated", "in_dr1257", "donor_eligible"]
    df = df[[c for c in keep if c in df.columns]]

    _save_processed(df, "donor_pool")
    log.info("harmonized_donor_pool", rows=len(df),
             donors=df["donor_eligible"].sum())
    return df


# ── Run all harmonization ─────────────────────────────────────────────

def run_all() -> dict[str, pd.DataFrame | None]:
    """Harmonize all raw datasets."""
    log.info("harmonization_start")

    results = {
        "bea": harmonize_bea(),
        "bds": harmonize_bds(),
        "qcew": harmonize_qcew(),
        "laus": harmonize_laus(),
        "cbp": harmonize_cbp(),
        "bps": harmonize_bps(),
        "qwi": harmonize_qwi(),
        "irs": harmonize_irs(),
        "acs": harmonize_acs(),
        "donor_pool": harmonize_donor_pool(),
    }

    succeeded = sum(1 for v in results.values() if v is not None)
    log.info("harmonization_complete", succeeded=succeeded, total=len(results))
    return results
