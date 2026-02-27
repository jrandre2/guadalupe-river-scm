"""Fetch Census ACS and Decennial data for SCM covariates.

Demographics, housing, income, education — used for pre-treatment matching.
Census API works without a key (500 req/day limit); only ~17 requests needed.
"""

from __future__ import annotations

import pandas as pd

from src.config import get_raw_dir
from src.utils.census_api import census_get
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "census_acs"

# ACS 5-year variables (table codes)
ACS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B17001_002E": "poverty_count",
    "B25001_001E": "housing_units",
    "B25077_001E": "median_home_value",
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_024E": "professional_degree",
    "B15003_025E": "doctorate_degree",
    "B25002_003E": "vacant_housing_units",
    "B23025_005E": "unemployed_pop",
    "B23025_002E": "labor_force_pop",
}


def _fetch_acs5(year: int) -> pd.DataFrame | None:
    """Fetch ACS 5-year estimates for a given end year."""
    url = f"https://api.census.gov/data/{year}/acs/acs5"
    var_list = list(ACS_VARIABLES.keys())

    try:
        df = census_get(url, get_vars=var_list, geo_for="county:*", geo_in="state:48")
    except Exception as e:
        log.warning("acs_fetch_failed", year=year, error=str(e))
        return None

    if df.empty:
        return None

    if "state" in df.columns and "county" in df.columns:
        df["fips"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)
    df["year"] = year

    # Rename to human-readable names
    df = df.rename(columns=ACS_VARIABLES)

    for col in ACS_VARIABLES.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _fetch_decennial_2000() -> pd.DataFrame | None:
    """Fetch key variables from 2000 Decennial Census SF3."""
    url = "https://api.census.gov/data/2000/dec/sf3"
    # SF3 variables for 2000
    vars_2000 = {
        "P001001": "total_population",
        "P053001": "median_household_income",
        "P087001": "poverty_count",
        "H001001": "housing_units",
    }

    try:
        df = census_get(url, get_vars=list(vars_2000.keys()), geo_for="county:*", geo_in="state:48")
    except Exception as e:
        log.warning("decennial_2000_failed", error=str(e))
        return None

    if df.empty:
        return None

    if "state" in df.columns and "county" in df.columns:
        df["fips"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)
    df["year"] = 2000
    df = df.rename(columns=vars_2000)
    for col in vars_2000.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def run(force: bool = False) -> None:
    """Download ACS and Decennial data for Texas counties."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "census_covariates_tx.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []

    # Decennial 2000
    log.info("fetching_decennial_2000")
    df = _fetch_decennial_2000()
    if df is not None:
        frames.append(df)

    # ACS 5-year: available from 2009 onward
    for year in range(2009, 2024):
        log.info("fetching_acs5", year=year)
        df = _fetch_acs5(year)
        if df is not None:
            frames.append(df)

    if not frames:
        log.error("census_acs_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Compute derived variables
    if "bachelors_degree" in combined.columns:
        edu_cols = ["bachelors_degree", "masters_degree", "professional_degree", "doctorate_degree"]
        existing = [c for c in edu_cols if c in combined.columns]
        combined["college_plus"] = combined[existing].sum(axis=1)

    keep_cols = ["fips", "year", "total_population", "median_household_income",
                 "poverty_count", "housing_units", "median_home_value",
                 "college_plus", "vacant_housing_units",
                 "unemployed_pop", "labor_force_pop"]
    combined = combined[[c for c in keep_cols if c in combined.columns]].copy()
    combined = combined.dropna(subset=["fips", "year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info("census_acs_complete", rows=len(combined), counties=combined["fips"].nunique())
