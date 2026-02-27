"""Fetch Census LEHD Quarterly Workforce Indicators for Texas counties.

Rich labor market dynamics: hires, separations, earnings flows.
Heavy cell suppression expected for small counties.

The Census API works without a key (500 req/day limit). We use bulk
CSV download from the LED extractor when possible.
"""

from __future__ import annotations

import io
import zipfile

import pandas as pd

from src.config import get_raw_dir, get_study_period
from src.utils.census_api import census_get
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "census_qwi"
_rate_limiter = RateLimiter(min_delay=1.5)

# LEHD bulk CSV files (no key needed)
# https://lehd.ces.census.gov/data/
LEHD_BULK_BASE = "https://lehd.ces.census.gov/data/qwi/latest_release/tx/"

QWI_VARS = ["Emp", "EmpEnd", "EmpS", "HirA", "Sep", "EarnS"]


def _try_bulk_download() -> pd.DataFrame | None:
    """Try downloading QWI bulk CSV from LEHD servers."""
    # LEHD serves state-level QWI files as CSV.gz
    # Filename pattern: qwi_tx_sa_f_gc_ns_op_u.csv.gz
    # (sa=seasonally adjusted, f=firm characteristics, gc=geography county, etc.)
    url = f"{LEHD_BULK_BASE}qwi_tx_sa_f_gc_ns_op_u.csv.gz"

    log.info("qwi_trying_bulk", url=url)
    try:
        content = download_file(url, rate_limiter=_rate_limiter, timeout=300)

        # Try gzip decompression
        import gzip
        try:
            decompressed = gzip.decompress(content)
            df = pd.read_csv(io.BytesIO(decompressed), dtype=str, low_memory=False)
        except Exception:
            df = pd.read_csv(io.BytesIO(content), dtype=str, low_memory=False)

        if df is not None and not df.empty:
            log.info("qwi_bulk_downloaded", rows=len(df))
            return df
    except Exception as e:
        log.warning("qwi_bulk_failed", error=str(e))

    return None


def _try_api_download() -> pd.DataFrame | None:
    """Fall back to Census API (works without key, just slower)."""
    period = get_study_period()
    start_year = max(1993, period["pre_start"])
    end_year = period["post_end"]

    frames = []
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            log.info("qwi_api_fetching", year=year, quarter=quarter)
            try:
                df = census_get(
                    "https://api.census.gov/data/timeseries/qwi/sa",
                    get_vars=QWI_VARS,
                    geo_for="county:*",
                    geo_in="state:48",
                    extra_params={
                        "year": str(year),
                        "quarter": str(quarter),
                        "ownercode": "A05",
                        "firmsize": "0",
                        "seasonadj": "U",
                        "industry": "00",
                    },
                )
                if not df.empty:
                    if "state" in df.columns and "county" in df.columns:
                        df["fips"] = df["state"].str.zfill(2) + df["county"].str.zfill(3)
                    df["year"] = year
                    df["quarter"] = quarter
                    frames.append(df)
            except Exception as e:
                log.warning("qwi_api_failed", year=year, quarter=quarter, error=str(e))

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def run(force: bool = False) -> None:
    """Download QWI data for all Texas counties."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "qwi_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    period = get_study_period()

    # Try bulk download first (skip if previous attempt timed out — files are huge)
    combined = None
    try:
        combined = _try_bulk_download()
    except Exception as e:
        log.warning("qwi_bulk_exception", error=str(e))

    if combined is not None and not combined.empty:
        combined.columns = combined.columns.str.strip().str.lower()

        # Build FIPS from geography column
        geo_col = next((c for c in combined.columns if c in ["geography", "geo", "county"]), None)
        state_col = next((c for c in combined.columns if c in ["state", "geo_level_state"]), None)
        county_col = next((c for c in combined.columns if c in ["county", "geo_level_county"]), None)

        if state_col and county_col:
            combined["fips"] = combined[state_col].str.zfill(2) + combined[county_col].str.zfill(3)
        elif geo_col:
            combined["fips"] = combined[geo_col].str.zfill(5)

        year_col = next((c for c in combined.columns if c in ["year"]), None)
        if year_col:
            combined["year"] = pd.to_numeric(combined[year_col], errors="coerce").astype("Int64")
    else:
        # Fall back to API
        log.info("qwi_falling_back_to_api")
        combined = _try_api_download()
        if combined is None or combined.empty:
            log.error("qwi_no_data")
            return

    # Convert numeric columns
    for var in [v.lower() for v in QWI_VARS]:
        if var in combined.columns:
            combined[var] = pd.to_numeric(combined[var], errors="coerce")

    # Aggregate to annual if we have quarterly data
    numeric_cols = [v.lower() for v in QWI_VARS if v.lower() in combined.columns]
    if "quarter" in combined.columns:
        annual = combined.groupby(["fips", "year"])[numeric_cols].mean().reset_index()
    else:
        annual = combined

    # Filter to study period
    annual = annual[
        (annual["year"] >= period["pre_start"])
        & (annual["year"] <= period["post_end"])
    ]

    keep = ["fips", "year"] + numeric_cols
    annual = annual[[c for c in keep if c in annual.columns]].copy()
    annual = annual.dropna(subset=["fips", "year"])

    save_parquet(annual, output_path, source=SOURCE)
    log.info("qwi_complete", rows=len(annual), counties=annual["fips"].nunique())
