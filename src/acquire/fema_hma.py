"""Fetch FEMA Hazard Mitigation Assistance (HMA) project data for Texas.

Hazard Mitigation Assistance funds local projects that reduce future disaster
risk — home elevations, buyouts, storm drainage improvements, etc. For this
study, HMA data serves as a supplemental indicator of recovery investment and
mitigation activity in the years following the 1998 Guadalupe River flood.

Data source: OpenFEMA v4, HazardMitigationAssistanceProjects
Filtered to Texas statewide (all disasters, all years).

Key variables: project type, award amount, county, disaster number,
project status, fiscal year. Filter to disasterNumber == 1257 after download
to isolate DR-1257-TX mitigation grants.

Coverage: All HMA programs (HMGP, BRIC, FMA) statewide; coverage back to
early 1990s but most complete from 2000 onward.
"""

from __future__ import annotations

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger
from src.utils.openfema_api import openfema_fetch_all

log = get_logger(__name__)

SOURCE = "fema_hma"
ENDPOINT = "https://www.fema.gov/api/open/v4/HazardMitigationAssistanceProjects"


def run(force: bool = False) -> None:
    """Download HMA project data for Texas."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "hma_texas.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    log.info("fetching_hma_texas")
    df = openfema_fetch_all(
        ENDPOINT,
        filter_expr="state eq 'Texas'",
        max_records=50000,
    )

    if df.empty:
        log.warning("hma_no_data")
    else:
        save_parquet(df, output_path, source=SOURCE)

    log.info("hma_complete", rows=len(df))
