"""Fetch FEMA Individual Assistance (IA) registrant data for DR-1257-TX.

Individual Assistance provides direct aid to households — housing assistance,
other needs assistance, and crisis counseling — following a presidentially
declared disaster. This dataset captures the scale of household-level impact
from the 1998 Guadalupe River flood in Comal County.

Data source: OpenFEMA v2, IndividualAssistanceHousingRegistrantsLargeDisasters
Endpoint filtered to disasterNumber == 1257 (DR-1257-TX, October 1998 flood).

Key variables (when available): applicant count, damage amount, zip code,
county, program type (housing vs. other needs assistance).

Coverage note: OpenFEMA's IA registrant dataset has limited pre-2004 coverage.
DR-1257 data may be absent or incomplete. If empty, a placeholder is saved
and a warning is logged. Fallback: FEMA disaster summary pages provide
aggregate totals not captured here.
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
