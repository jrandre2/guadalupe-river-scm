"""Fetch federal spending data from USAspending.gov API.

Cross-checks FEMA, SBA, and HUD disaster-related awards by county.
Data quality improves significantly after 2007 (DATA Act).
"""

from __future__ import annotations

import pandas as pd
import requests

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "usaspending"
API_BASE = "https://api.usaspending.gov/api/v2"
_rate_limiter = RateLimiter(min_delay=1.0)

# FEMA=070, SBA=073, HUD=086
AGENCY_CODES = {"FEMA": "070", "SBA": "073", "HUD": "086"}

# Comal County + neighboring counties
TARGET_COUNTIES = ["48091", "48029", "48187", "48259", "48171"]


def _search_awards(county_fips: str, fiscal_year: int) -> pd.DataFrame | None:
    """Search USAspending for awards in a specific county and fiscal year."""
    _rate_limiter.wait()

    url = f"{API_BASE}/search/spending_by_award/"
    payload = {
        "filters": {
            "time_period": [{"start_date": f"{fiscal_year}-10-01", "end_date": f"{fiscal_year + 1}-09-30"}],
            "place_of_performance_locations": [{"country": "USA", "state": "TX", "county": county_fips[2:]}],
            "award_type_codes": ["02", "03", "04", "05", "06", "07", "08", "09", "10", "11"],
        },
        "fields": [
            "Award ID", "Recipient Name", "Award Amount",
            "Awarding Agency", "Awarding Sub Agency",
            "Start Date", "End Date",
            "Award Type", "Description",
        ],
        "limit": 100,
        "page": 1,
    }

    try:
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results:
            return pd.DataFrame(results)
    except Exception as e:
        log.warning("usaspending_query_failed", county=county_fips, fy=fiscal_year, error=str(e))

    return None


def run(force: bool = False) -> None:
    """Download federal spending data for target counties around DR-1257."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "usaspending_tx_disaster.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    # Focus on FY1999-FY2005 (the period after the 1998 flood)
    for fy in range(1999, 2006):
        for county in TARGET_COUNTIES:
            log.info("usaspending_querying", county=county, fy=fy)
            df = _search_awards(county, fy)
            if df is not None and not df.empty:
                df["county_fips"] = county
                df["fiscal_year"] = fy
                frames.append(df)

    if not frames:
        log.warning("usaspending_no_results", hint="Pre-2001 data is sparse in USAspending")
        return

    combined = pd.concat(frames, ignore_index=True)
    save_parquet(combined, output_path, source=SOURCE)
    log.info("usaspending_complete", rows=len(combined))
