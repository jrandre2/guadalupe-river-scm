"""Fetch Census ZIP Code Business Patterns for Texas ZIP codes.

Annual establishments, employment, and payroll at ZIP code level. Covers 1994-2020.
Uses bulk CSV downloads from Census (no API key required).
ZBP totals files are distributed alongside CBP at:
  https://www2.census.gov/programs-surveys/cbp/datasets/{year}/zbp{yy}totals.zip
"""

from __future__ import annotations

import io
import zipfile

import pandas as pd

from src.config import get_raw_dir
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "census_zbp"
_rate_limiter = RateLimiter(min_delay=1.5)

ZBP_BASE = "https://www2.census.gov/programs-surveys/cbp/datasets"

# Texas ZIP code prefixes (750xx-799xx cover all of TX)
TX_ZIP_PREFIXES = tuple(str(i) for i in range(750, 800))


def _download_year(year: int) -> pd.DataFrame | None:
    """Download and parse ZBP totals data for a single year."""
    yy = str(year)[-2:]

    urls = [
        f"{ZBP_BASE}/{year}/zbp{yy}totals.zip",
        f"{ZBP_BASE}/{year}/zbp{yy}totals.txt",
    ]

    for url in urls:
        try:
            log.info("zbp_trying_url", year=year, url=url)
            content = download_file(url, rate_limiter=_rate_limiter, timeout=120)

            if url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    names = zf.namelist()
                    csv_name = next(
                        (n for n in names if n.endswith((".txt", ".csv"))), names[0]
                    )
                    with zf.open(csv_name) as f:
                        raw = f.read()
                        first_line = raw[:500].decode("latin-1").split("\n")[0]
                        sep = "|" if "|" in first_line else ","
                        df = pd.read_csv(
                            io.BytesIO(raw),
                            sep=sep,
                            dtype=str,
                            low_memory=False,
                            encoding="latin-1",
                        )
            else:
                first_line = content[:500].decode("latin-1").split("\n")[0]
                sep = "|" if "|" in first_line else ","
                df = pd.read_csv(
                    io.BytesIO(content),
                    sep=sep,
                    dtype=str,
                    low_memory=False,
                    encoding="latin-1",
                )

            if df is not None and not df.empty:
                log.info("zbp_downloaded", year=year, rows=len(df))
                return df
        except Exception as e:
            log.debug("zbp_url_failed", year=year, url=url, error=str(e))

    log.warning("zbp_all_urls_failed", year=year)
    return None


def _parse_zbp(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Parse raw ZBP DataFrame: standardize columns, filter to TX."""
    df.columns = df.columns.str.strip().str.lower()

    # Find ZIP column
    zip_col = next((c for c in df.columns if c in ["zip", "zipcode"]), None)
    if zip_col is None:
        log.warning("zbp_no_zip_col", year=year, cols=list(df.columns)[:15])
        return pd.DataFrame()

    df["zip"] = df[zip_col].astype(str).str.strip().str.zfill(5)

    # Filter to Texas ZIPs
    df = df[df["zip"].str[:3].isin(TX_ZIP_PREFIXES)].copy()

    df["year"] = year

    # Extract numeric columns (names vary across years)
    for col_name, targets in [
        ("estab", ["est", "estab", "establishments"]),
        ("emp", ["emp", "employment"]),
        ("payann", ["ap", "payann", "annual_payroll", "pay"]),
    ]:
        for t in targets:
            if t in df.columns:
                df[col_name] = pd.to_numeric(df[t], errors="coerce")
                break

    keep = ["zip", "year", "estab", "emp", "payann"]
    return df[[c for c in keep if c in df.columns]].copy()


def run(force: bool = False) -> None:
    """Download ZBP totals for all Texas ZIP codes, 1994-2020."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "zbp_tx_zips.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    frames = []
    for year in range(1994, 2021):
        df = _download_year(year)
        if df is None or df.empty:
            continue

        parsed = _parse_zbp(df, year)
        if parsed.empty:
            continue

        frames.append(parsed)
        log.info("zbp_year_parsed", year=year, tx_zips=len(parsed))

    if not frames:
        log.error("zbp_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["zip", "year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info(
        "zbp_complete",
        rows=len(combined),
        zips=combined["zip"].nunique(),
        year_range=f"{combined['year'].min()}-{combined['year'].max()}",
    )
