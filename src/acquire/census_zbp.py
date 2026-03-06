"""Fetch Census ZIP Code Business Patterns for Texas ZIP codes.

Annual establishments, employment, and payroll at ZIP code level. Covers 1994-2020.
Uses bulk CSV downloads from Census (no API key required).
ZBP totals files are distributed alongside CBP at:
  https://www2.census.gov/programs-surveys/cbp/datasets/{year}/zbp{yy}totals.zip

ZBP detail files (NAICS breakdown) at:
  https://www2.census.gov/programs-surveys/cbp/datasets/{year}/zbp{yy}detail.zip
NAICS available from 1998 onward; 1994-1997 use SIC codes.
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


# ── Sector detail download ─────────────────────────────────────────────────────

# NAICS 2-digit codes for target sectors (pattern: code + 4 dashes/slashes)
_NAICS_SECTOR_PAT = r"^(23|44|45|72)[-/]{4}$"

# SIC major-group prefixes for the same sectors (pre-1998 files use SIC)
# Construction: SIC 15xx–17xx; Retail: SIC 52xx–59xx; Food: SIC 58xx
_SIC_CONSTRUCTION = tuple(str(i) for i in range(15, 18))   # 15, 16, 17
_SIC_RETAIL       = tuple(str(i) for i in range(52, 60))   # 52–59
_SIC_FOOD         = ("58",)                                 # eating/drinking


def _naics_to_sector(naics: pd.Series) -> pd.Series:
    """Map NAICS 2-digit prefix to sector label; NaN for unrecognised."""
    prefix = naics.str[:2]
    s = pd.Series("", index=naics.index, dtype=str)
    s[prefix == "23"] = "construction"
    s[prefix.isin(["44", "45"])] = "retail"
    s[prefix == "72"] = "foodservice"
    s[s == ""] = pd.NA
    return s


def _sic_to_sector(sic: pd.Series) -> pd.Series:
    """Map SIC 2-digit prefix to sector label; NaN for unrecognised."""
    prefix = sic.astype(str).str.zfill(4).str[:2]
    s = pd.Series("", index=sic.index, dtype=str)
    s[prefix.isin(_SIC_CONSTRUCTION)] = "construction"
    s[prefix.isin(_SIC_RETAIL)] = "retail"
    s[prefix.isin(_SIC_FOOD)] = "foodservice"
    s[s == ""] = pd.NA
    return s


def _download_year_detail(year: int) -> pd.DataFrame | None:
    """Download and parse ZBP detail (NAICS/SIC breakdown) for a single year."""
    yy = str(year)[-2:]
    urls = [
        f"{ZBP_BASE}/{year}/zbp{yy}detail.zip",
        f"{ZBP_BASE}/{year}/zbp{yy}detail.txt",
    ]

    for url in urls:
        try:
            log.info("zbp_detail_trying", year=year, url=url)
            content = download_file(url, rate_limiter=_rate_limiter, timeout=180)

            if url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    names = zf.namelist()
                    csv_name = next(
                        (n for n in names if n.endswith((".txt", ".csv"))), names[0]
                    )
                    with zf.open(csv_name) as f:
                        raw = f.read()
            else:
                raw = content

            first_line = raw[:500].decode("latin-1").split("\n")[0]
            sep = "|" if "|" in first_line else ","
            df = pd.read_csv(
                io.BytesIO(raw),
                sep=sep,
                dtype=str,
                low_memory=False,
                encoding="latin-1",
            )
            if df is not None and not df.empty:
                log.info("zbp_detail_downloaded", year=year, rows=len(df))
                return df
        except Exception as e:
            log.debug("zbp_detail_url_failed", year=year, url=url, error=str(e))

    log.warning("zbp_detail_all_failed", year=year)
    return None


def _parse_zbp_detail(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """Parse ZBP detail file: filter to TX + target sectors, aggregate to ZIP × sector."""
    df.columns = df.columns.str.strip().str.lower()

    zip_col = next((c for c in df.columns if c in ["zip", "zipcode"]), None)
    if zip_col is None:
        log.warning("zbp_detail_no_zip", year=year, cols=list(df.columns)[:15])
        return pd.DataFrame()

    df["zip"] = df[zip_col].astype(str).str.strip().str.zfill(5)
    df = df[df["zip"].str[:3].isin(TX_ZIP_PREFIXES)].copy()

    # Detect industry column and assign sector labels
    if "naics" in df.columns:
        naics_col = df["naics"].astype(str).str.strip()
        # Keep only 2-digit level: code + 4 dashes/slashes
        mask_2digit = naics_col.str.match(_NAICS_SECTOR_PAT)
        df = df[mask_2digit].copy()
        df["sector"] = _naics_to_sector(naics_col[mask_2digit])
    elif "sic" in df.columns:
        sic_col = df["sic"].astype(str).str.strip()
        # SIC 2-digit level: 4-char code where last 2 are "//", "00", or similar
        # Filter for our target sector prefixes
        df["sector"] = _sic_to_sector(sic_col)
    else:
        log.warning("zbp_detail_no_industry_col", year=year, cols=list(df.columns)[:15])
        return pd.DataFrame()

    df = df[df["sector"].notna()].copy()
    if df.empty:
        return pd.DataFrame()

    # Establishment count
    for t in ["est", "estab"]:
        if t in df.columns:
            df["estab"] = pd.to_numeric(df[t], errors="coerce").fillna(0)
            break

    # ZBP detail files carry establishment SIZE BANDS, not total employment.
    # Estimate employment using midpoints of each size class.
    _SIZE_MIDPOINTS = {
        "n1_4": 2.5, "n5_9": 7.0, "n10_19": 14.5, "n20_49": 34.5,
        "n50_99": 74.5, "n100_249": 174.5, "n250_499": 374.5,
        "n500_999": 749.5, "n1000": 1250.0,
    }
    size_cols = [c for c in _SIZE_MIDPOINTS if c in df.columns]
    if size_cols:
        df["emp_est"] = sum(
            pd.to_numeric(df[c], errors="coerce").fillna(0) * mp
            for c, mp in _SIZE_MIDPOINTS.items()
            if c in df.columns
        )

    df["year"] = year

    keep = ["zip", "year", "sector", "estab", "emp_est"]
    df = df[[c for c in keep if c in df.columns]].copy()

    # Aggregate: some years list sub-sectors; sum up to the 2-digit level
    agg_cols = [c for c in ["estab", "emp_est"] if c in df.columns]
    return df.groupby(["zip", "year", "sector"], as_index=False)[agg_cols].sum()


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


def run_detail(force: bool = False) -> None:
    """Download ZBP detail (sector breakdown) for Texas ZIPs, 1994-2020.

    Saves data/raw/census_zbp/zbp_tx_sectors.parquet with columns:
      zip, year, sector (construction/retail/foodservice), emp, estab
    """
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "zbp_tx_sectors.parquet"

    if not force and output_path.exists():
        log.info("skip_existing_detail", source=SOURCE)
        return

    frames = []
    for year in range(1994, 2021):
        df = _download_year_detail(year)
        if df is None or df.empty:
            continue
        parsed = _parse_zbp_detail(df, year)
        if parsed.empty:
            log.warning("zbp_detail_empty_year", year=year)
            continue
        frames.append(parsed)
        sectors = parsed["sector"].value_counts().to_dict()
        log.info("zbp_detail_year_parsed", year=year, rows=len(parsed), sectors=sectors)

    if not frames:
        log.error("zbp_detail_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["zip", "year", "sector"])
    combined["year"] = combined["year"].astype(int)

    save_parquet(combined, output_path, source=SOURCE)
    log.info(
        "zbp_detail_complete",
        rows=len(combined),
        zips=combined["zip"].nunique(),
        sectors=combined["sector"].unique().tolist(),
        year_range=f"{combined['year'].min()}-{combined['year'].max()}",
    )
