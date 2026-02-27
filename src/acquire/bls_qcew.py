"""Fetch BLS Quarterly Census of Employment and Wages for all Texas counties.

Primary outcome variable for the SCM: county-level employment, wages,
and establishment counts. Uses bulk CSV downloads (1990-2024) since the
Open Data API only covers recent years.
"""

from __future__ import annotations

import io
import zipfile

import pandas as pd

from src.config import get_raw_dir, get_study_period
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "bls_qcew"
_rate_limiter = RateLimiter(min_delay=1.0)

# Bulk annual single-file URL pattern
BULK_URL = "https://data.bls.gov/cew/data/files/{year}/csv/{year}_annual_singlefile.zip"


def _download_year(year: int) -> pd.DataFrame | None:
    """Download and parse QCEW bulk data for a single year."""
    url = BULK_URL.format(year=year)
    log.info("qcew_downloading", year=year, url=url)

    try:
        content = download_file(url, rate_limiter=_rate_limiter, timeout=180)
    except Exception as e:
        log.warning("qcew_download_failed", year=year, error=str(e))
        return None

    # Extract CSV from zip
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            log.warning("qcew_no_csv_in_zip", year=year)
            return None

        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f, dtype=str, low_memory=False)

    # Filter to Texas counties (area_fips starts with 48, length 5)
    if "area_fips" not in df.columns:
        log.warning("qcew_missing_area_fips", year=year, columns=list(df.columns)[:10])
        return None

    df = df[
        df["area_fips"].str.startswith("48")
        & (df["area_fips"].str.len() == 5)
    ].copy()

    # Filter to total and private ownership (own_code 0 and 5)
    # and total industry (industry_code 10 for total all industries)
    if "own_code" in df.columns:
        df = df[df["own_code"].isin(["0", "5"])].copy()
    if "industry_code" in df.columns:
        df = df[df["industry_code"] == "10"].copy()

    log.info("qcew_year_parsed", year=year, rows=len(df))
    return df


def run(force: bool = False) -> None:
    """Download QCEW bulk data for all years and all Texas counties."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "qcew_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    period = get_study_period()
    # QCEW with NAICS starts ~1990
    start_year = max(1990, period["pre_start"])
    end_year = period["post_end"]

    frames = []
    for year in range(start_year, end_year + 1):
        df = _download_year(year)
        if df is not None and not df.empty:
            df["year_downloaded"] = year
            frames.append(df)

    if not frames:
        log.error("qcew_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Standardize column names and types
    col_renames = {
        "area_fips": "fips",
        "own_code": "own_code",
        "industry_code": "industry_code",
        "year": "year",
        "qtr": "quarter",
        "annual_avg_estabs": "establishments",
        "annual_avg_emplvl": "employment",
        "total_annual_wages": "total_wages",
        "annual_avg_wkly_wage": "avg_weekly_wage",
        "avg_annual_pay": "avg_annual_pay",
    }

    # Rename columns that exist
    rename_actual = {k: v for k, v in col_renames.items() if k in combined.columns}
    combined = combined.rename(columns=rename_actual)

    # Convert numeric columns
    numeric = ["establishments", "employment", "total_wages", "avg_weekly_wage", "avg_annual_pay"]
    for col in numeric:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce")

    if "year" in combined.columns:
        combined["year"] = pd.to_numeric(combined["year"], errors="coerce").astype("Int64")

    # Keep the annual summary rows (qtr=A or quarter=0 depending on format)
    # In annual singlefile format, these are the annual averages
    keep_cols = ["fips", "year", "own_code", "industry_code"] + [
        c for c in numeric if c in combined.columns
    ]
    combined = combined[[c for c in keep_cols if c in combined.columns]].copy()
    combined = combined.dropna(subset=["fips", "year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info(
        "qcew_complete",
        rows=len(combined),
        counties=combined["fips"].nunique(),
        year_range=f"{combined['year'].min()}-{combined['year'].max()}",
    )
