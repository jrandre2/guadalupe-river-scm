"""Fetch USGS stream gauge data for the Guadalupe River basin.

Uses the `dataretrieval` Python package for NWIS access.
Key gauges near New Braunfels for characterizing the 1998 flood.
"""

from __future__ import annotations

import pandas as pd

from src.config import get_raw_dir, load_sources_config
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "usgs_nwis"


def run(force: bool = False) -> None:
    """Download daily discharge data for Guadalupe River gauges."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "guadalupe_daily_discharge.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    try:
        import dataretrieval.nwis as nwis
    except ImportError:
        log.error("dataretrieval_not_installed", hint="pip install dataretrieval")
        return

    sources_cfg = load_sources_config()
    gauges = sources_cfg["usgs_nwis"]["gauges"]

    frames = []
    for gauge_id in gauges:
        log.info("usgs_fetching", gauge=gauge_id)
        try:
            # Daily values for discharge (parameter 00060)
            df, metadata = nwis.get_dv(
                sites=gauge_id,
                parameterCd="00060",
                start="1978-01-01",
                end="2025-12-31",
            )
            if df is not None and not df.empty:
                df = df.reset_index()
                df["site_no"] = gauge_id
                frames.append(df)
                log.info("usgs_gauge_fetched", gauge=gauge_id, rows=len(df))
        except Exception as e:
            log.warning("usgs_gauge_failed", gauge=gauge_id, error=str(e))

    if not frames:
        log.error("usgs_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    save_parquet(combined, output_path, source=SOURCE)
    log.info("usgs_complete", rows=len(combined), gauges=combined["site_no"].nunique())
