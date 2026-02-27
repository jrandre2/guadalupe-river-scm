"""Fetch Census County Business Patterns for all Texas counties.

Annual establishments, employment, and payroll by industry. Covers 1986-2023.
Uses bulk CSV downloads from Census (no API key required).
"""

from __future__ import annotations

import io
import zipfile

import pandas as pd

from src.config import get_raw_dir, get_study_period
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "census_cbp"
_rate_limiter = RateLimiter(min_delay=1.0)

# CBP bulk download URLs from Census FTP
# Pattern: https://www2.census.gov/programs-surveys/cbp/datasets/{year}/cbp{yy}co.zip
# The file inside is cbp{yy}co.txt (pipe-delimited or CSV depending on year)
CBP_BASE = "https://www2.census.gov/programs-surveys/cbp/datasets"


def _download_year(year: int) -> pd.DataFrame | None:
    """Download and parse CBP data for a single year from bulk files."""
    yy = str(year)[-2:]

    # Try multiple URL patterns (Census has changed these over the years)
    urls = [
        f"{CBP_BASE}/{year}/cbp{yy}co.zip",
        f"{CBP_BASE}/{year}/cbp{yy}co.txt",
        f"{CBP_BASE}/{year}/CB{year}.CB1200A11-Data.csv",
    ]

    for url in urls:
        try:
            log.info("cbp_trying_url", year=year, url=url)
            content = download_file(url, rate_limiter=_rate_limiter, timeout=120)

            if url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    names = zf.namelist()
                    csv_name = next((n for n in names if n.endswith((".txt", ".csv"))), names[0])
                    with zf.open(csv_name) as f:
                        raw = f.read()
                        # Detect delimiter
                        first_line = raw[:500].decode("latin-1").split("\n")[0]
                        sep = "|" if "|" in first_line else ","
                        df = pd.read_csv(io.BytesIO(raw), sep=sep, dtype=str,
                                         low_memory=False, encoding="latin-1")
            else:
                first_line = content[:500].decode("latin-1").split("\n")[0]
                sep = "|" if "|" in first_line else ","
                df = pd.read_csv(io.BytesIO(content), sep=sep, dtype=str,
                                 low_memory=False, encoding="latin-1")

            if df is not None and not df.empty:
                log.info("cbp_downloaded", year=year, rows=len(df))
                return df
        except Exception as e:
            log.debug("cbp_url_failed", year=year, url=url, error=str(e))

    log.warning("cbp_all_urls_failed", year=year)
    return None


def run(force: bool = False) -> None:
    """Download CBP data for all Texas counties across all available years."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "cbp_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    period = get_study_period()
    start_year = max(1986, period["pre_start"])
    end_year = min(2022, period["post_end"])  # CBP bulk files lag ~1 year

    frames = []
    for year in range(start_year, end_year + 1):
        df = _download_year(year)
        if df is None or df.empty:
            continue

        df.columns = df.columns.str.strip().str.lower()

        # Build FIPS
        fips_col = next((c for c in df.columns if c in ["fipstate", "fipscty", "fips"]), None)
        state_col = next((c for c in df.columns if c in ["fipstate", "fips_state"]), None)
        county_col = next((c for c in df.columns if c in ["fipscty", "fips_county"]), None)

        if state_col and county_col:
            df["fips"] = df[state_col].str.strip().str.zfill(2) + df[county_col].str.strip().str.zfill(3)
        elif fips_col:
            df["fips"] = df[fips_col].str.strip().str.zfill(5)
        else:
            log.warning("cbp_no_fips", year=year, cols=list(df.columns)[:15])
            continue

        # Filter to Texas counties
        df = df[df["fips"].str.startswith("48") & (df["fips"].str.len() == 5)].copy()

        # Filter to total industry
        naics_col = next((c for c in df.columns if "naics" in c or "sic" in c), None)
        if naics_col:
            df = df[df[naics_col].astype(str).str.strip().isin(["------", "----", "00", "0", "10", ""])].copy()

        df["year"] = year

        # Extract numeric columns
        for col_name, targets in [
            ("establishments", ["est", "estab", "establishments"]),
            ("employment", ["emp", "employment"]),
            ("annual_payroll", ["ap", "payann", "annual_payroll", "pay"]),
        ]:
            for t in targets:
                if t in df.columns:
                    df[col_name] = pd.to_numeric(df[t], errors="coerce")
                    break

        frames.append(df)

    if not frames:
        log.error("cbp_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    keep_cols = ["fips", "year", "establishments", "employment", "annual_payroll"]
    combined = combined[[c for c in keep_cols if c in combined.columns]].copy()
    combined = combined.dropna(subset=["fips", "year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info(
        "cbp_complete",
        rows=len(combined),
        counties=combined["fips"].nunique(),
        year_range=f"{combined['year'].min()}-{combined['year'].max()}",
    )
