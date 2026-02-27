"""Fetch NOAA Storm Events Database records for Texas.

Characterizes the October 1998 flood — severity, geographic extent, fatalities.
"""

from __future__ import annotations

import gzip
import io
import re

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file, fetch_csv_text
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "noaa_storms"
BULK_BASE = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
_rate_limiter = RateLimiter(min_delay=1.0)


def _find_file_for_year(year: int, file_type: str = "details") -> str | None:
    """Find the CSV filename for a given year from the NOAA bulk listing."""
    # Files named like: StormEvents_details-ftp_v1.0_d{year}_c{date}.csv.gz
    try:
        listing = fetch_csv_text(BULK_BASE, rate_limiter=_rate_limiter)
        pattern = rf'StormEvents_{file_type}-ftp_v1\.0_d{year}_c\d+\.csv\.gz'
        matches = re.findall(pattern, listing)
        if matches:
            return matches[-1]  # Take the latest version
    except Exception as e:
        log.warning("noaa_listing_failed", error=str(e))
    return None


def _download_storm_file(filename: str) -> pd.DataFrame | None:
    """Download and decompress a NOAA storm events CSV.gz file."""
    url = BULK_BASE + filename
    try:
        content = download_file(url, rate_limiter=_rate_limiter)
        decompressed = gzip.decompress(content)
        df = pd.read_csv(io.BytesIO(decompressed), dtype=str, low_memory=False)
        return df
    except Exception as e:
        log.warning("noaa_download_failed", filename=filename, error=str(e))
        return None


def run(force: bool = False) -> None:
    """Download NOAA Storm Events for Texas, 1996-2002."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "storm_events_tx.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    for year in range(1996, 2003):
        log.info("noaa_fetching", year=year)
        filename = _find_file_for_year(year, "details")
        if filename:
            df = _download_storm_file(filename)
            if df is not None and not df.empty:
                # Filter to Texas (STATE_FIPS = 48)
                fips_col = next((c for c in df.columns if c.upper() in ["STATE_FIPS", "STATE_FIPS_CODE"]), None)
                state_col = next((c for c in df.columns if c.upper() == "STATE"), None)
                if fips_col:
                    df = df[df[fips_col].astype(str).str.strip() == "48"].copy()
                elif state_col:
                    df = df[df[state_col].str.strip().str.upper() == "TEXAS"].copy()
                frames.append(df)
                log.info("noaa_year_fetched", year=year, rows=len(df))
        else:
            log.warning("noaa_file_not_found", year=year)

    if not frames:
        log.error("noaa_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)

    # Filter to flood-related events
    event_col = next((c for c in combined.columns if c.upper() == "EVENT_TYPE"), None)
    if event_col:
        flood_types = ["Flash Flood", "Flood", "Heavy Rain", "Coastal Flood"]
        combined = combined[combined[event_col].str.strip().isin(flood_types)].copy()

    save_parquet(combined, output_path, source=SOURCE)
    log.info("noaa_complete", rows=len(combined))
