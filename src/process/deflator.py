"""CPI-U deflator for converting nominal dollar values to constant dollars.

Downloads annual CPI-U from BLS bulk files and provides deflation utilities.
Base year: 2020 (default).
"""

from __future__ import annotations

import io
from pathlib import Path

import pandas as pd

from src.config import PROCESSED_DIR
from src.utils.file_io import save_parquet, load_parquet
from src.utils.http_client import RateLimiter, fetch_csv_text
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

_rate_limiter = RateLimiter(min_delay=1.0)

# BLS CPI-U All Urban Consumers, All Items, US City Average
# Series ID: CUUR0000SA0 (seasonally adjusted)
CPI_SERIES_ID = "CUUR0000SA0"
CPI_BULK_URL = "https://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"


def download_cpi(base_year: int = 2020) -> pd.DataFrame:
    """Download CPI-U data and compute deflators relative to base year."""
    log.info("downloading_cpi")
    text = fetch_csv_text(CPI_BULK_URL, rate_limiter=_rate_limiter)
    df = pd.read_csv(io.StringIO(text), sep="\t", dtype=str)
    df.columns = df.columns.str.strip()

    # Filter to our series
    df["series_id"] = df["series_id"].str.strip()
    cpi = df[df["series_id"] == CPI_SERIES_ID].copy()

    if cpi.empty:
        log.warning("cpi_series_not_found", trying_alt=True)
        # Try not-seasonally-adjusted
        alt_id = "CUUR0000SA0"
        cpi = df[df["series_id"].str.startswith("CUUR0000SA0")].copy()

    cpi["year"] = pd.to_numeric(cpi["year"].str.strip(), errors="coerce")
    cpi["value"] = pd.to_numeric(cpi["value"].str.strip(), errors="coerce")
    cpi["period"] = cpi["period"].str.strip()

    # Use annual average (M13) or compute from monthly
    annual = cpi[cpi["period"] == "M13"].copy()
    if annual.empty:
        monthly = cpi[cpi["period"].str.startswith("M") & (cpi["period"] != "M13")]
        annual = monthly.groupby("year")["value"].mean().reset_index()
    else:
        annual = annual[["year", "value"]].copy()

    annual = annual.rename(columns={"value": "cpi"})
    annual["year"] = annual["year"].astype(int)
    annual = annual.sort_values("year").reset_index(drop=True)

    # Compute deflator (base_year CPI = 1.0)
    base_cpi = annual.loc[annual["year"] == base_year, "cpi"]
    if base_cpi.empty:
        log.warning("cpi_base_year_missing", base_year=base_year, using_latest=True)
        base_cpi = annual["cpi"].iloc[-1]
    else:
        base_cpi = base_cpi.iloc[0]

    annual["deflator"] = base_cpi / annual["cpi"]
    annual["base_year"] = base_year

    log.info("cpi_downloaded", years=f"{annual['year'].min()}-{annual['year'].max()}",
             rows=len(annual))
    return annual


def save_cpi(base_year: int = 2020) -> Path:
    """Download CPI and save to processed directory."""
    cpi = download_cpi(base_year=base_year)
    out_dir = PROCESSED_DIR / "deflator"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "cpi_deflator.parquet"
    save_parquet(cpi, path, source="cpi_deflator")
    return path


def load_deflator() -> pd.DataFrame:
    """Load previously saved CPI deflator."""
    path = PROCESSED_DIR / "deflator" / "cpi_deflator.parquet"
    if not path.exists():
        log.info("cpi_not_cached_downloading")
        save_cpi()
    return load_parquet(path)


def deflate_column(
    df: pd.DataFrame,
    col: str,
    year_col: str = "year",
    new_col: str | None = None,
) -> pd.DataFrame:
    """Deflate a nominal dollar column to constant dollars using CPI.

    Args:
        df: DataFrame with a year column and a nominal dollar column.
        col: Name of the column to deflate.
        year_col: Name of the year column.
        new_col: Name for the deflated column. Defaults to '{col}_real'.

    Returns:
        DataFrame with the new deflated column added.
    """
    if new_col is None:
        new_col = f"{col}_real"

    cpi = load_deflator()
    deflator_map = dict(zip(cpi["year"], cpi["deflator"]))

    df = df.copy()
    df[new_col] = df[col] * df[year_col].map(deflator_map)
    return df
