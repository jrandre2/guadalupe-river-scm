"""Fetch Census Building Permits Survey for Texas counties.

New residential construction permits — proxy for construction activity.
Uses bulk CSV downloads from Census (no API key required).
"""

from __future__ import annotations

import io

import pandas as pd

from src.config import get_raw_dir, get_study_period
from src.utils.file_io import save_parquet
from src.utils.http_client import RateLimiter, download_file
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "census_bps"
_rate_limiter = RateLimiter(min_delay=1.0)


def _download_year(year: int) -> pd.DataFrame | None:
    """Download building permits data for a single year."""
    yy = str(year)[-2:]

    urls = [
        f"https://www2.census.gov/econ/bps/County/co{year}a.txt",
        f"https://www2.census.gov/econ/bps/County/co{yy}a.txt",
        f"https://www2.census.gov/econ/bps/County/{year}/co{year}a.txt",
    ]

    for url in urls:
        try:
            content = download_file(url, rate_limiter=_rate_limiter, timeout=60)
            text = content.decode("latin-1")

            # BPS CSVs have a header row (29 fields) and data rows (30 fields).
            # Without index_col=False, pandas consumes field 0 as the row index,
            # shifting all columns by 1 and breaking FIPS extraction.
            df = pd.read_csv(
                io.StringIO(text), dtype=str, low_memory=False, index_col=False,
            )

            if df is not None and not df.empty:
                log.info("bps_downloaded", year=year, rows=len(df))
                return df
        except Exception as e:
            log.debug("bps_url_failed", year=year, url=url, error=str(e))

    log.warning("bps_all_urls_failed", year=year)
    return None


def run(force: bool = False) -> None:
    """Download building permits data for all Texas counties."""
    out_dir = get_raw_dir(SOURCE)
    output_path = out_dir / "bps_tx_counties.parquet"

    if not force and output_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    period = get_study_period()
    start_year = max(1990, period["pre_start"])
    end_year = min(2024, period["post_end"])

    frames = []
    for year in range(start_year, end_year + 1):
        log.info("bps_fetching", year=year)
        df = _download_year(year)
        if df is None or df.empty:
            continue

        df.columns = df.columns.str.strip().str.lower()

        # Drop sub-header row (has text like 'State', 'County', 'Date', 'Bldgs')
        # and empty rows. The sub-header is always the first data row.
        if len(df) > 0:
            first_fips = df.iloc[0, 1] if df.shape[1] > 1 else ""
            if isinstance(first_fips, str) and not first_fips.strip().isdigit():
                df = df.iloc[1:]
        df = df.dropna(how="all").reset_index(drop=True)

        # Build FIPS from state and county columns.
        # Census BPS CSV has two "FIPS" columns (state, county); pandas renames
        # the second to "fips .1" or "fips.1".
        state_col = next((c for c in df.columns if c in [
            "state", "fips st", "statefips", "fips state",
        ]), None)
        county_col = next((c for c in df.columns if c in [
            "fips cty", "countyfips", "cnty", "fips .1", "fips.1",
            "fips county",
        ]), None)

        # If 'fips' exists and we haven't matched state_col yet, it's the state FIPS
        if state_col is None and "fips" in df.columns:
            state_col = "fips"

        if state_col and county_col:
            # Convert to int then format to strip erroneous leading zeros
            state_num = pd.to_numeric(df[state_col].str.strip(), errors="coerce")
            county_num = pd.to_numeric(df[county_col].str.strip(), errors="coerce")
            fips_combined = (
                state_num.apply(lambda x: f"{int(x):02d}" if pd.notna(x) else None)
                + county_num.apply(lambda x: f"{int(x):03d}" if pd.notna(x) else None)
            )
            df = df.drop(columns=[c for c in [state_col, county_col] if c in df.columns], errors="ignore")
            df["fips"] = fips_combined
        else:
            log.warning("bps_no_fips", year=year, cols=list(df.columns)[:15])
            continue

        # Filter to Texas (FIPS starting with '48', exactly 5 digits)
        df = df[df["fips"].str.startswith("48", na=False) & (df["fips"].str.len() == 5)].copy()
        if df.empty:
            log.warning("bps_no_tx_rows", year=year)
            continue
        df["year"] = year

        # Extract 1-unit permit data using column positions.
        # BPS CSV has sub-header structure: after geographic cols, groups of 3
        # (Bldgs, Units, Value) for 1-unit, 2-unit, 3-4 unit, 5+ unit.
        # The '1-unit' label sits at the Units position; Bldgs is one column before.
        col_list = list(df.columns)
        one_unit_idx = None
        for i, c in enumerate(col_list):
            if "1-unit" in str(c).lower() and "rep" not in str(c).lower():
                one_unit_idx = i
                break

        if one_unit_idx is not None and one_unit_idx >= 1 and one_unit_idx + 1 < len(col_list):
            # Use .iloc for positional access — column names may be duplicated
            df["buildings"] = pd.to_numeric(
                df.iloc[:, one_unit_idx - 1].astype(str).str.replace(",", ""), errors="coerce"
            )
            df["units"] = pd.to_numeric(
                df.iloc[:, one_unit_idx].astype(str).str.replace(",", ""), errors="coerce"
            )
            df["value"] = pd.to_numeric(
                df.iloc[:, one_unit_idx + 1].astype(str).str.replace(",", ""), errors="coerce"
            )

        # Select only needed columns before appending (raw CSV has duplicate column names)
        keep = ["fips", "year", "buildings", "units", "value"]
        df = df[[c for c in keep if c in df.columns]].copy()
        df = df.dropna(subset=["buildings"], how="all")
        frames.append(df)

    if not frames:
        log.error("bps_no_data")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["fips", "year"])

    save_parquet(combined, output_path, source=SOURCE)
    log.info("bps_complete", rows=len(combined), counties=combined["fips"].nunique())
