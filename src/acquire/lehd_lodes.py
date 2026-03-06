"""Fetch LEHD LODES Workplace Area Characteristics (WAC) for Texas tracts.

LODES8 covers 2002-2021. We download annual WAC files (workplace jobs by census
block), aggregate to census tract, and filter to Comal + Hays + Kendall counties.

These data support the LODES persistent-effects analysis (R/09_did_lodes.R),
which tests whether flood-exposed tracts show lasting workplace differences
from 2002 onward. Note: LODES starts 4 years after the 1998 flood, so no
pre-treatment period is available for a standard DiD.
"""

from __future__ import annotations

import gzip
import io

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "lehd_lodes"
_rate_limiter = RateLimiter(min_delay=1.0)

# LODES8 WAC files: all jobs, all job types, by census block
# Filename: tx_wac_S000_JT00_{year}.csv.gz
LODES_BASE = "https://lehd.ces.census.gov/data/lodes/LODES8/tx/wac/"

# Texas county FIPS prefixes to retain (Comal=48091, Hays=48209, Kendall=48259)
TARGET_COUNTY_PREFIXES = ("48091", "48209", "48259")

# WAC columns to keep (total + wage tiers + NAICS sector job counts)
WAC_COLS = [
    "w_geocode",  # workplace census block (15-digit FIPS)
    "C000",       # total jobs
    "CE01",       # jobs, monthly earnings < $1,250
    "CE02",       # jobs, monthly earnings $1,250–$3,333
    "CE03",       # jobs, monthly earnings > $3,333
    "CNS01", "CNS02", "CNS03", "CNS04", "CNS05", "CNS06", "CNS07",
    "CNS08", "CNS09", "CNS10", "CNS11", "CNS12", "CNS13", "CNS14",
    "CNS15", "CNS16", "CNS17", "CNS18", "CNS19", "CNS20",
]

LODES_START = 2002
LODES_END   = 2021


def _download_year(year: int) -> pd.DataFrame | None:
    """Download and parse one year of WAC data, aggregate to tract level."""
    url = f"{LODES_BASE}tx_wac_S000_JT00_{year}.csv.gz"
    log.info("lodes_downloading", year=year, url=url)

    try:
        content = download_file(url, rate_limiter=_rate_limiter, timeout=120)
    except Exception as e:
        log.warning("lodes_download_failed", year=year, error=str(e))
        return None

    try:
        decompressed = gzip.decompress(content)
        df = pd.read_csv(io.BytesIO(decompressed), dtype=str, low_memory=False)
    except Exception as e:
        log.warning("lodes_parse_failed", year=year, error=str(e))
        return None

    if "w_geocode" not in df.columns:
        log.warning("lodes_missing_geocode", year=year, columns=list(df.columns)[:10])
        return None

    # Filter to target counties: first 5 digits of the 15-digit block code
    df["county_fips"] = df["w_geocode"].str[:5]
    df = df[df["county_fips"].isin(TARGET_COUNTY_PREFIXES)].copy()

    if df.empty:
        log.warning("lodes_no_target_counties", year=year)
        return None

    # Derive census tract (first 11 digits of block code)
    df["tract"] = df["w_geocode"].str[:11]

    # Convert numeric columns
    keep = [c for c in WAC_COLS if c in df.columns and c != "w_geocode"]
    for col in keep:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Aggregate from block to tract
    tract_df = df.groupby("tract")[keep].sum(min_count=1).reset_index()
    tract_df["year"] = year

    log.info("lodes_year_parsed", year=year, tracts=len(tract_df))
    return tract_df


def run(force: bool = False) -> None:
    """Download and aggregate LODES WAC data for target counties, 2002-2021."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "lodes_wac_tracts.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    for year in range(LODES_START, LODES_END + 1):
        df = _download_year(year)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        log.error("lodes_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values(["tract", "year"]).reset_index(drop=True)

    save_parquet(combined, output_path, source=SOURCE)
    log.info(
        "lodes_complete",
        rows=len(combined),
        tracts=combined["tract"].nunique(),
        year_range=f"{combined['year'].min()}-{combined['year'].max()}",
    )
