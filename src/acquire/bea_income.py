"""Fetch BEA Regional Personal Income data for all Texas counties.

This is the anchor dataset — continuous from 1969 to present, covering the
entire 1978-2025 study period.

Uses direct CSV bulk download from BEA (no API key required).
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

SOURCE = "bea_income"
_rate_limiter = RateLimiter(min_delay=1.0)

# BEA bulk download URLs (CSV zip files, no API key needed)
# Interactive Data Tables → Download → Full county-level tables
CAINC1_URL = "https://apps.bea.gov/regional/zip/CAINC1.zip"
CAINC4_URL = "https://apps.bea.gov/regional/zip/CAINC4.zip"
CAINC30_URL = "https://apps.bea.gov/regional/zip/CAINC30.zip"


def _download_and_parse_bea_zip(url: str, table_name: str) -> pd.DataFrame:
    """Download a BEA regional data zip file and parse the CSV inside."""
    log.info("bea_downloading", table=table_name, url=url)
    content = download_file(url, rate_limiter=_rate_limiter, timeout=300)

    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            log.error("bea_no_csv_in_zip", table=table_name, files=zf.namelist())
            return pd.DataFrame()

        log.info("bea_parsing_csv", table=table_name, file=csv_names[0])
        with zf.open(csv_names[0]) as f:
            # BEA CSVs have a few header/footer lines and use wide format
            # with years as columns. Read with latin-1 encoding.
            df = pd.read_csv(
                f, encoding="latin-1", dtype=str, low_memory=False,
                na_values=["(NA)", "(D)", "(L)", "(T)", "..."],
            )

    log.info("bea_raw_parsed", table=table_name, rows=len(df), cols=len(df.columns))
    return df


def _process_bea_wide(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Convert BEA wide-format data to long format, filtered to Texas counties."""
    if df.empty:
        return df

    # Standardize column names
    df.columns = df.columns.str.strip()

    # Identify the GeoFIPS column
    fips_col = next((c for c in df.columns if "GeoFIPS" in c or "GeoFips" in c), None)
    name_col = next((c for c in df.columns if "GeoName" in c), None)
    line_col = next((c for c in df.columns if "LineCode" in c), None)
    desc_col = next((c for c in df.columns if "Description" in c), None)

    if not fips_col:
        log.error("bea_no_fips_column", table=table_name, columns=list(df.columns)[:10])
        return pd.DataFrame()

    # Clean FIPS: remove quotes and whitespace
    df[fips_col] = df[fips_col].astype(str).str.strip().str.replace('"', '').str.replace("'", "")

    # Filter to Texas counties (5-digit FIPS starting with 48, not state total "48000")
    mask = (
        df[fips_col].str.startswith("48")
        & (df[fips_col].str.len() >= 5)
        & (~df[fips_col].str.endswith("000"))  # Exclude state/MSA totals
    )
    df = df[mask].copy()
    df["fips"] = df[fips_col].str[:5]

    if line_col:
        df["line_code"] = pd.to_numeric(df[line_col], errors="coerce")

    if desc_col:
        df["description"] = df[desc_col].str.strip()

    # Find year columns (columns that are 4-digit numbers)
    year_cols = [c for c in df.columns if c.strip().isdigit() and len(c.strip()) == 4]
    if not year_cols:
        log.error("bea_no_year_columns", table=table_name)
        return pd.DataFrame()

    # Melt to long format
    id_cols = ["fips"]
    if line_col:
        id_cols.append("line_code")
    if desc_col:
        id_cols.append("description")

    melted = df.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )
    melted["year"] = pd.to_numeric(melted["year"].str.strip(), errors="coerce").astype("Int64")
    melted["value"] = pd.to_numeric(
        melted["value"].astype(str).str.replace(",", "").str.strip(),
        errors="coerce",
    )
    melted = melted.dropna(subset=["year"])

    log.info("bea_melted", table=table_name, rows=len(melted), counties=melted["fips"].nunique())
    return melted


def _build_cainc1(df_long: pd.DataFrame) -> pd.DataFrame:
    """Extract CAINC1 variables from long-format data.

    CAINC1 line codes:
      1 = Personal income (thousands $)
      2 = Population (persons)
      3 = Per capita personal income ($)
    """
    line_map = {
        1: "personal_income",
        2: "population",
        3: "per_capita_income",
    }

    if "line_code" not in df_long.columns:
        log.warning("cainc1_no_line_code")
        return pd.DataFrame()

    frames = []
    for code, var_name in line_map.items():
        subset = df_long[df_long["line_code"] == code][["fips", "year", "value"]].copy()
        subset = subset.rename(columns={"value": var_name})
        frames.append(subset)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.merge(f, on=["fips", "year"], how="outer")
    return result


def _build_cainc4(df_long: pd.DataFrame) -> pd.DataFrame:
    """Extract CAINC4 variables from long-format data.

    CAINC4 line codes:
      10 = Total personal income
      35 = Net earnings by place of residence
      46 = Dividends, interest, and rent
      47 = Personal current transfer receipts
      7010 = Total employment (full-time and part-time)
    """
    line_map = {
        35: "net_earnings",
        46: "dividends_interest_rent",
        47: "transfer_receipts",
        7010: "total_employment",
    }

    if "line_code" not in df_long.columns:
        return pd.DataFrame()

    frames = []
    for code, var_name in line_map.items():
        subset = df_long[df_long["line_code"] == code][["fips", "year", "value"]].copy()
        subset = subset.rename(columns={"value": var_name})
        if not subset.empty:
            frames.append(subset)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.merge(f, on=["fips", "year"], how="outer")
    return result


def run(force: bool = False) -> None:
    """Download BEA personal income data for all Texas counties via bulk CSV."""
    out_dir = get_raw_dir(SOURCE)
    cainc1_path = out_dir / "cainc1_tx_counties.parquet"
    cainc4_path = out_dir / "cainc4_tx_counties.parquet"
    merged_path = out_dir / "bea_income_tx_counties.parquet"

    if not force and merged_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    # Download and parse CAINC1
    log.info("fetching_cainc1_bulk")
    raw1 = _download_and_parse_bea_zip(CAINC1_URL, "CAINC1")
    long1 = _process_bea_wide(raw1, "CAINC1")
    cainc1 = _build_cainc1(long1)
    # Cast numeric columns to float64 to avoid pyarrow mixed-type errors
    numeric_cols_1 = ["personal_income", "population", "per_capita_income"]
    for col in numeric_cols_1:
        if col in cainc1.columns:
            cainc1[col] = cainc1[col].astype("float64")
    if not cainc1.empty:
        save_parquet(cainc1, cainc1_path, source=SOURCE)

    # Download and parse CAINC4
    log.info("fetching_cainc4_bulk")
    raw4 = _download_and_parse_bea_zip(CAINC4_URL, "CAINC4")
    long4 = _process_bea_wide(raw4, "CAINC4")
    cainc4 = _build_cainc4(long4)
    numeric_cols_4 = ["net_earnings", "dividends_interest_rent", "transfer_receipts", "total_employment"]
    for col in numeric_cols_4:
        if col in cainc4.columns:
            cainc4[col] = cainc4[col].astype("float64")
    if not cainc4.empty:
        save_parquet(cainc4, cainc4_path, source=SOURCE)

    # Merge
    if not cainc1.empty and not cainc4.empty:
        merged = cainc1.merge(cainc4, on=["fips", "year"], how="outer")
    elif not cainc1.empty:
        merged = cainc1
    elif not cainc4.empty:
        merged = cainc4
    else:
        log.error("bea_no_data_at_all")
        return

    # Ensure all numeric columns are float64
    for col in merged.columns:
        if col not in ("fips", "year"):
            merged[col] = pd.to_numeric(merged[col], errors="coerce").astype("float64")

    # Filter to study period range (with some buffer)
    period = get_study_period()
    merged = merged[
        (merged["year"] >= period["pre_start"] - 5)
        & (merged["year"] <= period["post_end"])
    ]

    save_parquet(merged, merged_path, source=SOURCE)
    log.info(
        "bea_income_complete",
        rows=len(merged),
        counties=merged["fips"].nunique(),
        year_range=f"{merged['year'].min()}-{merged['year'].max()}",
    )
