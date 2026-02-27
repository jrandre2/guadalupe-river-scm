"""Fetch FEMA Individual Assistance data for DR-1257-TX.

Note: IA data for pre-2004 disasters may be incomplete in OpenFEMA.
"""

from __future__ import annotations

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger
from src.utils.openfema_api import openfema_fetch_all

log = get_logger(__name__)

SOURCE = "fema_ia"
ENDPOINT = "https://www.fema.gov/api/open/v2/IndividualAssistanceHousingRegistrantsLargeDisasters"


def run(force: bool = False) -> None:
    """Download IA registrant data for DR-1257."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "ia_dr1257.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    log.info("fetching_ia_dr1257")
    df = openfema_fetch_all(
        ENDPOINT,
        filter_expr="disasterNumber eq 1257",
        max_records=100000,
    )

    if df.empty:
        log.warning("ia_no_data_dr1257", hint="Pre-2004 IA data may not be in OpenFEMA. Check FEMA disaster page for aggregate totals.")
        # Create empty placeholder
        import pandas as pd
        df = pd.DataFrame({"note": ["No IA data found for DR-1257 in OpenFEMA"]})

    save_parquet(df, output_path, source=SOURCE)
    log.info("ia_complete", rows=len(df))
