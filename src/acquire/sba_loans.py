"""Fetch SBA Disaster Loan data.

Downloads Excel files from SBA Open Data portal for FY2000-FY2002.
FY99 data not available on the current portal.
"""

from __future__ import annotations

import io

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "sba_loans"
_rate_limiter = RateLimiter(min_delay=2.0)

DATASET_ID = "e243640a-ed1c-4941-850e-b2c6aa15cad3"
_BASE = f"https://data.sba.gov/dataset/{DATASET_ID}/resource"

# SBA Open Data Excel download URLs (fiscal year files)
SBA_URLS = {
    "FY00": f"{_BASE}/781acddc-783b-41f1-a1e6-5e9f4fe0fac2/download/sba_disaster_loan_data_fy00.xlsx",
    "FY01": f"{_BASE}/2d9fbacc-a998-4881-ae1b-593a50a8265a/download/sba_disaster_loan_data_fy01.xls",
    "FY02": f"{_BASE}/9d22f22e-9f87-4a6d-8d8a-9a98d771168d/download/sba_disaster_loan_data_fy02.xls",
}


def run(force: bool = False) -> None:
    """Download SBA disaster loan data for relevant fiscal years."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "sba_loans_tx.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    for fy_label, url in SBA_URLS.items():
        log.info("sba_downloading", fiscal_year=fy_label)
        try:
            content = download_file(url, rate_limiter=_rate_limiter, timeout=120)

            # Try reading as Excel (xlsx/xls)
            try:
                df = pd.read_excel(io.BytesIO(content), dtype=str)
            except Exception:
                # Fallback to CSV
                df = pd.read_csv(io.BytesIO(content), dtype=str, low_memory=False, encoding="latin-1")

            df.columns = df.columns.str.strip().str.lower()

            # Filter to Texas
            state_col = next((c for c in df.columns if "state" in c), None)
            if state_col:
                df = df[df[state_col].str.strip().str.upper().isin(["TX", "TEXAS", "48"])].copy()

            df["fiscal_year"] = fy_label
            frames.append(df)
            log.info("sba_year_downloaded", fy=fy_label, rows=len(df))
        except Exception as e:
            log.warning("sba_download_failed", fy=fy_label, error=str(e))

    if not frames:
        log.warning("sba_no_data", hint="SBA URLs may have changed. Check https://data.sba.gov/dataset/disaster-loan-data")
        return

    combined = pd.concat(frames, ignore_index=True)
    save_parquet(combined, output_path, source=SOURCE)
    log.info("sba_complete", rows=len(combined))
