"""Fetch IRS Statistics of Income county-level data.

Tax-return-based AGI, wages, number of returns — 1989-2022.
Files downloaded directly from IRS website (no API).
"""

from __future__ import annotations

import io

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "irs_soi"
_rate_limiter = RateLimiter(min_delay=2.0)

# IRS SOI county data URL patterns (these change across years)
# Recent years use a consistent pattern; older years vary
IRS_BASE = "https://www.irs.gov/pub/irs-soi"


def _build_url(tax_year: int) -> str | None:
    """Build the download URL for a given tax year. Returns None if unknown."""
    # Recent years (2017+) use a consistent pattern
    yy = str(tax_year)[-2:]
    if tax_year >= 2017:
        return f"{IRS_BASE}/{yy}incyallnoagi.csv"
    elif tax_year >= 2012:
        return f"{IRS_BASE}/{yy}incyallnoagi.csv"
    elif tax_year >= 2005:
        return f"{IRS_BASE}/{yy}incyallnoagi.csv"
    elif tax_year >= 1998:
        return f"{IRS_BASE}/{yy}incyallnoagi.csv"
    elif tax_year >= 1989:
        # Older years may use different naming; try common patterns
        return f"{IRS_BASE}/{yy}incyallnoagi.csv"
    return None


def _download_year(tax_year: int) -> pd.DataFrame | None:
    """Download and parse IRS SOI data for a single tax year."""
    url = _build_url(tax_year)
    if not url:
        log.warning("irs_no_url", tax_year=tax_year)
        return None

    try:
        content = download_file(url, rate_limiter=_rate_limiter)
        df = pd.read_csv(io.BytesIO(content), dtype=str, low_memory=False, encoding="latin-1")
    except Exception as e:
        log.warning("irs_download_failed", tax_year=tax_year, url=url, error=str(e))
        # Try Excel format as fallback
        try:
            xls_url = url.replace(".csv", ".xls")
            content = download_file(xls_url, rate_limiter=_rate_limiter)
            df = pd.read_excel(io.BytesIO(content), dtype=str)
        except Exception:
            log.warning("irs_excel_fallback_failed", tax_year=tax_year)
            return None

    if df.empty:
        return None

    # Standardize column names (they vary across years)
    df.columns = df.columns.str.strip().str.lower()

    # Try to identify state and county FIPS columns
    state_col = next((c for c in df.columns if c in ["statefips", "state_fips", "fips_state", "state"]), None)
    county_col = next((c for c in df.columns if c in ["countyfips", "county_fips", "fips_county", "county"]), None)

    if state_col and county_col:
        df["fips"] = df[state_col].str.strip().str.zfill(2) + df[county_col].str.strip().str.zfill(3)
    else:
        log.warning("irs_no_fips_cols", tax_year=tax_year, columns=list(df.columns)[:15])
        return None

    # Filter to Texas
    df = df[df["fips"].str.startswith("48")].copy()

    # Filter to "all" AGI class (total, not broken by income bracket)
    agi_col = next((c for c in df.columns if c in ["agi_stub", "agi_class", "incsize"]), None)
    if agi_col:
        df = df[df[agi_col].astype(str).str.strip().isin(["0", "1", ""])].copy()

    df["tax_year"] = tax_year

    # Try to extract key variables (column names vary)
    var_mapping = {
        "n1": "num_returns",
        "n2": "num_exemptions",
        "a00100": "agi",
        "a00200": "wages_salaries",
        "mars1": "single_returns",
        "mars2": "joint_returns",
    }
    for old, new in var_mapping.items():
        if old in df.columns:
            df[new] = pd.to_numeric(df[old].astype(str).str.replace(",", ""), errors="coerce")

    log.info("irs_year_parsed", tax_year=tax_year, rows=len(df))
    return df


def run(force: bool = False) -> None:
    """Download IRS SOI county data for all available tax years."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "irs_soi_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    for tax_year in range(1989, 2023):
        log.info("irs_fetching", tax_year=tax_year)
        df = _download_year(tax_year)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        log.error("irs_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)

    keep_cols = ["fips", "tax_year", "num_returns", "num_exemptions", "agi",
                 "wages_salaries", "single_returns", "joint_returns"]
    combined = combined[[c for c in keep_cols if c in combined.columns]].copy()
    combined = combined.dropna(subset=["fips", "tax_year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info("irs_complete", rows=len(combined), counties=combined["fips"].nunique())
