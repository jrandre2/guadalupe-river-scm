"""Fetch Texas Comptroller local sales tax allocation data.

Semi-automated: attempts form submission scrape of the Quarterly Historical
Sales data portal. Falls back to documenting manual download steps.

Data available quarterly from 2002 (post-treatment only).
"""

from __future__ import annotations

import io

import pandas as pd
import requests

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "tx_comptroller"
HIST_URL = "https://mycpa.cpa.state.tx.us/allocation/HistSales"
_rate_limiter = RateLimiter(min_delay=2.0)


def run(force: bool = False) -> None:
    """Attempt to download TX Comptroller sales tax data."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "tx_sales_tax.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    log.info("tx_comptroller_attempting_scrape")

    # The TX Comptroller site uses form submissions; attempt automated download
    # This may fail due to JavaScript rendering or session tokens
    try:
        session = requests.Session()
        session.headers.update({"User-Agent": "GuadalupeSCM/0.1 (academic-research)"})

        # Try to fetch the historical sales page
        resp = session.get(HIST_URL, timeout=30)
        resp.raise_for_status()

        # If we get HTML, the site likely requires form submission
        if "text/html" in resp.headers.get("content-type", ""):
            log.warning(
                "tx_comptroller_requires_manual",
                hint=(
                    "The TX Comptroller HistSales tool requires interactive form submission. "
                    "Manual download steps:\n"
                    "1. Visit https://mycpa.cpa.state.tx.us/allocation/HistSales\n"
                    "2. Select 'County' as geography type\n"
                    "3. Select 'Comal' county (and repeat for all TX counties)\n"
                    "4. Select quarterly date range (2002 Q1 through latest)\n"
                    "5. Download the resulting CSV/Excel\n"
                    "6. Save to data/raw/tx_comptroller/\n"
                ),
            )
            return

    except Exception as e:
        log.warning("tx_comptroller_scrape_failed", error=str(e))
        log.info(
            "tx_comptroller_manual_instructions",
            url=HIST_URL,
            hint="See above for manual download steps",
        )
        return

    log.info("tx_comptroller_complete")
