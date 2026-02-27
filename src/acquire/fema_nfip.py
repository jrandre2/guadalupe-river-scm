"""Fetch FEMA NFIP (National Flood Insurance Program) claims for Texas."""

from __future__ import annotations

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger
from src.utils.openfema_api import openfema_fetch_all

log = get_logger(__name__)

SOURCE = "fema_nfip"
ENDPOINT = "https://www.fema.gov/api/open/v2/FimaNfipClaims"


def run(force: bool = False) -> None:
    """Download NFIP claims for Texas, focused on 1998 event."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "nfip_tx_1998.parquet"
    broad_path = out_dir / "nfip_tx_1990_2005.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    # Fetch 1998 claims for Texas
    log.info("fetching_nfip_tx_1998")
    df_1998 = openfema_fetch_all(
        ENDPOINT,
        filter_expr="state eq 'TX' and yearOfLoss eq 1998",
        max_records=100000,
    )
    if not df_1998.empty:
        save_parquet(df_1998, output_path, source=SOURCE)

    # Fetch broader window for context (1995-2005)
    log.info("fetching_nfip_tx_broad")
    df_broad = openfema_fetch_all(
        ENDPOINT,
        filter_expr="state eq 'TX' and yearOfLoss ge 1995 and yearOfLoss le 2005",
        max_records=200000,
    )
    if not df_broad.empty:
        save_parquet(df_broad, broad_path, source=SOURCE)

    log.info("nfip_complete", claims_1998=len(df_1998), claims_broad=len(df_broad))
