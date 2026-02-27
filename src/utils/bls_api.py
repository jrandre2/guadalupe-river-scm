"""BLS API v2 helper functions."""

from __future__ import annotations

from typing import Any

import pandas as pd
import requests

from src.config import get_api_key
from src.utils.http_client import RateLimiter, get_session
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
_rate_limiter = RateLimiter(min_delay=0.5)


def bls_series_get(
    series_ids: list[str],
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """Fetch one or more BLS time series via the API v2.

    The API accepts up to 50 series per request and a 20-year span.
    This function handles chunking across both dimensions.
    """
    key = get_api_key("bls")
    all_rows: list[dict[str, Any]] = []

    # Chunk series into groups of 50
    for i in range(0, len(series_ids), 50):
        chunk = series_ids[i : i + 50]

        # Chunk years into 20-year windows
        y = start_year
        while y <= end_year:
            y_end = min(y + 19, end_year)
            _rate_limiter.wait()

            payload: dict[str, Any] = {
                "seriesid": chunk,
                "startyear": str(y),
                "endyear": str(y_end),
            }
            if key:
                payload["registrationkey"] = key

            session = get_session()
            resp = session.post(BLS_API_URL, json=payload, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "REQUEST_SUCCEEDED":
                log.warning("bls_api_error", message=data.get("message", []))
                y = y_end + 1
                continue

            for series_result in data.get("Results", {}).get("series", []):
                sid = series_result["seriesID"]
                for obs in series_result.get("data", []):
                    all_rows.append({
                        "series_id": sid,
                        "year": int(obs["year"]),
                        "period": obs["period"],
                        "value": obs["value"],
                    })

            log.info(
                "bls_fetched",
                n_series=len(chunk),
                year_range=f"{y}-{y_end}",
                rows=len(all_rows),
            )
            y = y_end + 1

    return pd.DataFrame(all_rows)


def build_laus_series_id(
    state_fips: str, county_fips: str, measure_code: str
) -> str:
    """Build a LAUS series ID for a county.

    Measure codes: 3=unemp rate, 4=unemp count, 5=employment, 6=labor force.
    """
    return f"LAUCN{state_fips}{county_fips}0000000{measure_code}"
