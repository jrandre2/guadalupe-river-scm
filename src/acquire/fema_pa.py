"""Fetch FEMA Public Assistance funded projects for DR-1257-TX.

Project-level federal obligations for infrastructure repair.
"""

from __future__ import annotations

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger
from src.utils.openfema_api import openfema_fetch_all

log = get_logger(__name__)

SOURCE = "fema_pa"
ENDPOINT = "https://www.fema.gov/api/open/v2/PublicAssistanceGrantAwardActivities"


def run(force: bool = False) -> None:
    """Download PA project data for DR-1257 and other major TX floods."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "pa_dr1257.parquet"
    tx_path = out_dir / "pa_tx_floods.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    # Fetch DR-1257 projects
    log.info("fetching_pa_dr1257")
    dr1257 = openfema_fetch_all(
        ENDPOINT,
        filter_expr="disasterNumber eq 1257",
    )
    if not dr1257.empty:
        save_parquet(dr1257, output_path, source=SOURCE)

    # Also fetch all Texas PA data for broader context
    log.info("fetching_pa_texas")
    tx_all = openfema_fetch_all(
        ENDPOINT,
        filter_expr="state eq 'Texas'",
        max_records=50000,
    )
    if not tx_all.empty:
        save_parquet(tx_all, tx_path, source=SOURCE)

    log.info("pa_complete", dr1257_rows=len(dr1257), tx_rows=len(tx_all))
