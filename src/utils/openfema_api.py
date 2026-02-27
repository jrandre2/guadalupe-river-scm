"""OpenFEMA API paginated fetcher.

OpenFEMA uses OData query parameters ($filter, $select, $skip, $top).
The requests library URL-encodes $ as %24, which OpenFEMA rejects.
We build the query string manually to preserve literal $ characters.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

from src.utils.http_client import RateLimiter, get_session
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

_rate_limiter = RateLimiter(min_delay=0.3)


def _build_url(base_url: str, odata_params: dict[str, str]) -> str:
    """Build a URL with OData $ parameters without encoding the $."""
    parts = []
    for key, value in odata_params.items():
        # Quote the value but preserve single quotes for OData strings
        encoded_value = quote(str(value), safe="' ")
        parts.append(f"{key}={encoded_value}")
    query_string = "&".join(parts)
    return f"{base_url}?{query_string}"


def openfema_fetch_all(
    endpoint_url: str,
    filter_expr: str | None = None,
    select_fields: list[str] | None = None,
    page_size: int = 1000,
    max_records: int | None = None,
) -> pd.DataFrame:
    """Fetch all records from an OpenFEMA v2 endpoint with pagination.

    Args:
        endpoint_url: Full URL (e.g. https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries)
        filter_expr: OData $filter expression (e.g. "state eq 'Texas'")
        select_fields: List of field names for $select
        page_size: Records per page (max 1000)
        max_records: Safety cap on total records fetched

    Returns:
        DataFrame of all fetched records.
    """
    all_records: list[dict[str, Any]] = []
    skip = 0
    session = get_session()

    while True:
        odata_params: dict[str, str] = {
            "$skip": str(skip),
            "$top": str(page_size),
            "$inlinecount": "allpages",
        }
        if filter_expr:
            odata_params["$filter"] = filter_expr
        if select_fields:
            odata_params["$select"] = ",".join(select_fields)

        url = _build_url(endpoint_url, odata_params)
        _rate_limiter.wait()

        log.debug("openfema_request", url=url[:200])
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            log.error("openfema_request_failed", url=url[:200], error=str(e))
            break

        # OpenFEMA returns data in a key matching the dataset name
        # Find the data key (it's the one that's a list)
        records = []
        for key, val in data.items():
            if isinstance(val, list):
                records = val
                break

        if not records:
            break

        all_records.extend(records)
        log.info(
            "openfema_page",
            endpoint=endpoint_url.split("/")[-1],
            skip=skip,
            page_records=len(records),
            total_so_far=len(all_records),
        )

        if len(records) < page_size:
            break  # Last page

        if max_records and len(all_records) >= max_records:
            log.warning("openfema_max_records", max_records=max_records)
            break

        skip += page_size

    if not all_records:
        log.warning("openfema_no_records", endpoint=endpoint_url)
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    log.info(
        "openfema_complete",
        endpoint=endpoint_url.split("/")[-1],
        total_records=len(df),
    )
    return df
