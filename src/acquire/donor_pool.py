"""Construct the donor pool for the Synthetic Control Method.

Reads FEMA declarations output and produces a table of eligible donor counties.
"""

from __future__ import annotations

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import load_parquet, save_parquet
from src.utils.fips import COMAL_COUNTY_FIPS, TX_COUNTY_FIPS
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "donor_pool"


def run(force: bool = False) -> None:
    """Build donor pool membership table."""
    out_dir = get_raw_dir("fema_declarations")
    pool_path = out_dir / "donor_pool.parquet"

    if not force and pool_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    # Load flood exclusion list from FEMA declarations step
    exclusion_path = out_dir / "flood_exclusion_counties.parquet"
    if not exclusion_path.exists():
        log.error("missing_dependency", file=str(exclusion_path), hint="Run fema_declarations first")
        return

    exclusion_df = load_parquet(exclusion_path)
    excluded_fips = set(exclusion_df["fips"].tolist())
    dr1257_mask = exclusion_df["in_dr1257"].astype(bool)
    dr1257_fips = set(exclusion_df.loc[dr1257_mask, "fips"].tolist())

    # Build pool from all TX counties
    rows = []
    for fips in TX_COUNTY_FIPS:
        rows.append({
            "fips": fips,
            "is_treated": fips == COMAL_COUNTY_FIPS,
            "in_dr1257": fips in dr1257_fips,
            "flood_1995_2001": fips in excluded_fips,
            "donor_eligible": fips not in excluded_fips and fips != COMAL_COUNTY_FIPS,
        })

    pool_df = pd.DataFrame(rows)
    n_eligible = pool_df["donor_eligible"].sum()
    n_excluded = len(pool_df) - n_eligible - 1  # minus treated unit

    log.info(
        "donor_pool_built",
        total_counties=len(pool_df),
        eligible=n_eligible,
        excluded=n_excluded,
    )

    save_parquet(pool_df, pool_path, source=SOURCE)
