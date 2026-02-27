"""Census Bureau API helper functions."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.config import get_api_key
from src.utils.http_client import RateLimiter, fetch_json
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

# Census API works without a key (500 req/day limit vs higher with key).
# Use a conservative rate limit since we have no key.
_rate_limiter = RateLimiter(min_delay=1.5)


def census_get(
    base_url: str,
    get_vars: list[str],
    geo_for: str,
    geo_in: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Query a Census API endpoint and return a DataFrame.

    Args:
        base_url: Full Census API URL (e.g. https://api.census.gov/data/2020/cbp)
        get_vars: Variable names to request (e.g. ["ESTAB", "EMP", "PAYANN"])
        geo_for: Geography specification (e.g. "county:*")
        geo_in: Parent geography (e.g. "state:48")
        extra_params: Additional query parameters

    Returns:
        DataFrame with columns = get_vars + geography columns.
    """
    key = get_api_key("census")
    params: dict[str, str] = {
        "get": ",".join(get_vars),
        "for": geo_for,
    }
    if geo_in:
        params["in"] = geo_in
    if key:
        params["key"] = key
    if extra_params:
        params.update(extra_params)

    data = fetch_json(base_url, params=params, rate_limiter=_rate_limiter)

    if not data or len(data) < 2:
        log.warning("census_empty_response", url=base_url, params=params)
        return pd.DataFrame()

    # First row is headers, rest are data
    df = pd.DataFrame(data[1:], columns=data[0])
    log.info("census_fetched", url=base_url, rows=len(df))
    return df


def census_timeseries_get(
    base_url: str,
    get_vars: list[str],
    geo_for: str,
    geo_in: str | None = None,
    time_range: str | None = None,
    extra_params: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Query a Census timeseries API endpoint (e.g., BDS, QWI).

    Args:
        time_range: e.g. "from+1978+to+2023" or "2020"
    """
    params = extra_params or {}
    if time_range:
        params["time"] = time_range

    return census_get(base_url, get_vars, geo_for, geo_in, extra_params=params)
