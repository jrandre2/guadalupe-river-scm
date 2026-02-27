"""Fetch Census Business Dynamics Statistics for all Texas counties.

BDS uniquely starts in 1978, covering the full pre-treatment period.
Provides firm/establishment counts, entry/exit, job creation/destruction.

Uses bulk CSV download from Census (no API key required).
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

SOURCE = "census_bds"
_rate_limiter = RateLimiter(min_delay=1.0)

# BDS bulk data download URLs (no API key required)
# Files are at: https://www2.census.gov/programs-surveys/bds/tables/time-series/{year}/
# County-level file: bds{year}_st_cty.csv (state x county cross-tab, all years in one file)
BDS_BULK_URLS = [
    "https://www2.census.gov/programs-surveys/bds/tables/time-series/2023/bds2023_st_cty.csv",
    "https://www2.census.gov/programs-surveys/bds/tables/time-series/2022/bds2022_st_cty.csv",
    "https://www2.census.gov/programs-surveys/bds/tables/time-series/2021/bds2021_st_cty.csv",
]


def run(force: bool = False) -> None:
    """Download BDS data for all Texas counties via bulk CSV."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "bds_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    period = get_study_period()
    df = None

    for url in BDS_BULK_URLS:
        log.info("bds_trying_url", url=url)
        try:
            content = download_file(url, rate_limiter=_rate_limiter, timeout=300)

            # Check if it's a zip
            if content[:4] == b'PK\x03\x04':
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                    if csv_names:
                        with zf.open(csv_names[0]) as f:
                            df = pd.read_csv(f, dtype=str, low_memory=False)
            else:
                df = pd.read_csv(io.BytesIO(content), dtype=str, low_memory=False)

            if df is not None and not df.empty:
                log.info("bds_downloaded", url=url, rows=len(df))
                break
        except Exception as e:
            log.warning("bds_url_failed", url=url, error=str(e))
            df = None

    if df is None or df.empty:
        log.error("bds_no_data", hint="All bulk download URLs failed")
        return

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()
    log.info("bds_columns", columns=list(df.columns))

    # Build FIPS from state + county columns
    state_col = next((c for c in df.columns if c in ["st", "state", "fipstate", "fips_state", "stcode"]), None)
    county_col = next((c for c in df.columns if c in ["cty", "county", "fipscty", "fips_county", "ctycode"]), None)
    year_col = next((c for c in df.columns if c in ["year", "year2", "time"]), None)

    if state_col and county_col:
        df["fips"] = df[state_col].str.zfill(2) + df[county_col].str.zfill(3)
    else:
        fips_col = next((c for c in df.columns if "fips" in c or "geo" in c), None)
        if fips_col:
            df["fips"] = df[fips_col].str.zfill(5)
        else:
            log.error("bds_no_fips_columns", columns=list(df.columns))
            return

    # Filter to Texas
    df = df[df["fips"].str.startswith("48") & (df["fips"].str.len() == 5)].copy()
    log.info("bds_filtered_texas", rows=len(df))

    if year_col:
        df["year"] = pd.to_numeric(df[year_col], errors="coerce").astype("Int64")
    else:
        log.error("bds_no_year_column", columns=list(df.columns))
        return

    # Map known BDS variable names to standard names
    var_candidates = {
        "firms": ["firms", "firm"],
        "estabs": ["estabs", "estab", "establishments"],
        "emp": ["emp", "employment"],
        "estabs_entry": ["estabs_entry"],
        "estabs_exit": ["estabs_exit"],
        "job_creation": ["job_creation"],
        "job_destruction": ["job_destruction"],
        "net_job_creation": ["net_job_creation"],
    }

    for target, candidates in var_candidates.items():
        for c in candidates:
            if c in df.columns and target != c:
                df[target] = pd.to_numeric(df[c], errors="coerce")
                break
            elif c in df.columns:
                df[target] = pd.to_numeric(df[c], errors="coerce")
                break

    keep = ["fips", "year"] + [k for k in var_candidates if k in df.columns]
    df = df[[c for c in keep if c in df.columns]].copy()

    df = df[
        (df["year"] >= period["pre_start"])
        & (df["year"] <= period["post_end"])
    ]
    df = df.dropna(subset=["fips", "year"])

    save_parquet(df, output_path, source=SOURCE)
    log.info(
        "bds_complete",
        rows=len(df),
        counties=df["fips"].nunique(),
        year_range=f"{df['year'].min()}-{df['year'].max()}",
    )
