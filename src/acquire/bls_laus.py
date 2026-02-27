"""Fetch BLS Local Area Unemployment Statistics for all Texas counties.

Uses bulk flat file download (more efficient than API for 254 counties x 4 measures).
Monthly data aggregated to annual averages.
"""

from __future__ import annotations

import io

import pandas as pd

from src.config import get_raw_dir, get_study_period
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, fetch_csv_text
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "bls_laus"
BULK_BASE = "https://download.bls.gov/pub/time.series/la/"
_rate_limiter = RateLimiter(min_delay=1.0)


def run(force: bool = False) -> None:
    """Download LAUS bulk data and extract Texas county series."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "laus_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    # Download series file to identify Texas county series
    log.info("downloading_laus_series_file")
    series_text = fetch_csv_text(BULK_BASE + "la.series", rate_limiter=_rate_limiter)
    series_df = pd.read_csv(io.StringIO(series_text), sep="\t", dtype=str)
    series_df.columns = series_df.columns.str.strip()

    # Filter to county-level (area_type_code = F for counties) in Texas
    # Series ID format: LAUCN{state_fips}{county_fips}0000000{measure}
    tx_series = series_df[
        series_df["series_id"].str.strip().str.startswith("LAUCN48")
    ].copy()
    tx_series_ids = set(tx_series["series_id"].str.strip())
    log.info("tx_county_series", n_series=len(tx_series_ids))

    # Download Texas-specific data file (la.data.51.Texas)
    log.info("downloading_laus_data")
    data_text = fetch_csv_text(
        BULK_BASE + "la.data.51.Texas",
        rate_limiter=_rate_limiter,
    )
    data_df = pd.read_csv(io.StringIO(data_text), sep="\t", dtype=str)
    data_df.columns = data_df.columns.str.strip()

    # Filter to Texas series
    data_df["series_id"] = data_df["series_id"].str.strip()
    tx_data = data_df[data_df["series_id"].isin(tx_series_ids)].copy()
    log.info("tx_data_filtered", rows=len(tx_data))

    if tx_data.empty:
        log.error("no_tx_laus_data")
        return

    # Parse series IDs to extract FIPS and measure
    # Series ID format: LAUCN48001000000003
    #   Position 0-4: "LAUCN", 5-9: state+county FIPS, 10-16: zeros, 17+: measure
    tx_data["fips"] = tx_data["series_id"].str[5:10]
    tx_data["measure"] = tx_data["series_id"].str[-1]

    # Measure codes: 3=unemp_rate, 4=unemp_count, 5=employment, 6=labor_force
    measure_names = {"3": "unemployment_rate", "4": "unemployment", "5": "employment", "6": "labor_force"}
    tx_data["measure_name"] = tx_data["measure"].map(measure_names)

    tx_data["year"] = pd.to_numeric(tx_data["year"].str.strip(), errors="coerce").astype("Int64")
    tx_data["value"] = pd.to_numeric(tx_data["value"].str.strip().str.replace(",", ""), errors="coerce")

    # Filter to annual averages (period M13) or compute from monthly
    annual = tx_data[tx_data["period"].str.strip() == "M13"].copy()

    if annual.empty:
        # Compute annual averages from monthly data
        monthly = tx_data[tx_data["period"].str.strip().str.startswith("M")].copy()
        monthly = monthly[monthly["period"].str.strip() != "M13"]
        annual = monthly.groupby(["fips", "year", "measure_name"])["value"].mean().reset_index()

    # Pivot measures into columns
    annual_wide = annual.pivot_table(
        index=["fips", "year"],
        columns="measure_name",
        values="value",
        aggfunc="first",
    ).reset_index()
    annual_wide.columns.name = None

    # Filter to study period
    period = get_study_period()
    annual_wide = annual_wide[
        (annual_wide["year"] >= period["pre_start"])
        & (annual_wide["year"] <= period["post_end"])
    ]

    save_parquet(annual_wide, output_path, source=SOURCE)
    log.info(
        "laus_complete",
        rows=len(annual_wide),
        counties=annual_wide["fips"].nunique(),
        year_range=f"{annual_wide['year'].min()}-{annual_wide['year'].max()}",
    )
