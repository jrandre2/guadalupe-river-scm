"""Build merged SCM panel from harmonized datasets.

Produces a balanced county-year panel with:
- Outcome variables (employment, income, establishments, etc.)
- Covariates (population, education, poverty, urbanization)
- Donor eligibility flags
- All nominal dollar values deflated to constant 2020 dollars

Exports to Parquet (Python) and CSV (R).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import PROCESSED_DIR, get_study_period, get_treated_fips
from src.process.deflator import deflate_column, load_deflator
from src.utils.file_io import load_parquet, save_parquet
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

PANELS_DIR = PROCESSED_DIR / "panels"


def _load_panel(name: str) -> pd.DataFrame | None:
    """Load a harmonized panel from processed/panels/."""
    path = PANELS_DIR / f"{name}.parquet"
    if not path.exists():
        log.warning("panel_missing", name=name)
        return None
    df = load_parquet(path)
    df["fips"] = df["fips"].astype(str).str.zfill(5)
    if "year" in df.columns:
        df["year"] = df["year"].astype(int)
    return df


def build_panel() -> pd.DataFrame:
    """Merge all harmonized datasets into a single SCM panel."""
    period = get_study_period()
    treated_fips = get_treated_fips()

    # ── Load all panels ──────────────────────────────────────────────
    bea = _load_panel("bea_income")
    bds = _load_panel("bds")
    qcew = _load_panel("qcew")
    laus = _load_panel("laus")
    cbp = _load_panel("cbp")
    bps = _load_panel("bps")
    qwi = _load_panel("qwi")
    irs = _load_panel("irs_soi")
    acs = _load_panel("acs_covariates")
    donors = _load_panel("donor_pool")

    # ── Build skeleton from BEA (widest coverage: 1969-2024) ─────────
    # BEA covers all 254 TX counties for the full study period
    if bea is None:
        log.error("bea_required_for_panel")
        raise RuntimeError("BEA income data is required to build the panel")

    # Filter to study period
    skeleton = bea[["fips", "year"]].copy()
    skeleton = skeleton[
        (skeleton["year"] >= period["pre_start"])
        & (skeleton["year"] <= period["post_end"])
    ]

    log.info("panel_skeleton",
             counties=skeleton["fips"].nunique(),
             years=f"{skeleton['year'].min()}-{skeleton['year'].max()}",
             rows=len(skeleton))

    # ── Merge datasets ───────────────────────────────────────────────
    panel = skeleton.copy()

    # Merge BEA income
    panel = panel.merge(bea, on=["fips", "year"], how="left")

    # Merge other panels
    datasets = [
        ("bds", bds),
        ("qcew", qcew),
        ("laus", laus),
        ("cbp", cbp),
        ("bps", bps),
        ("qwi", qwi),
        ("irs_soi", irs),
    ]

    for name, df in datasets:
        if df is not None:
            panel = panel.merge(df, on=["fips", "year"], how="left")
            log.debug("panel_merged", source=name, cols_added=len(df.columns) - 2)

    # ── Merge covariates (ACS, many years missing pre-2009) ──────────
    if acs is not None:
        # For pre-ACS years, forward-fill from nearest available year
        panel = panel.merge(acs, on=["fips", "year"], how="left")
        # Forward-fill covariates within each county
        covariate_cols = [c for c in acs.columns if c not in ["fips", "year"]]
        panel = panel.sort_values(["fips", "year"])
        panel[covariate_cols] = panel.groupby("fips")[covariate_cols].ffill()
        # Also back-fill for years before first ACS observation
        panel[covariate_cols] = panel.groupby("fips")[covariate_cols].bfill()

    # ── Merge donor eligibility ──────────────────────────────────────
    if donors is not None:
        panel = panel.merge(donors, on="fips", how="left")
        # Counties not in donor table are not Texas counties
        panel["donor_eligible"] = panel["donor_eligible"].fillna(False)
        panel["is_treated"] = panel["fips"] == treated_fips
    else:
        panel["donor_eligible"] = True
        panel["is_treated"] = panel["fips"] == treated_fips

    # ── Deflate nominal dollar values ────────────────────────────────
    nominal_cols = [
        "personal_income", "per_capita_income", "net_earnings",
        "transfer_receipts", "qcew_total_wages", "qcew_avg_weekly_wage",
        "qcew_avg_annual_pay", "cbp_annual_payroll", "bps_value",
        "qwi_earnings", "irs_agi", "irs_wages",
        "median_household_income", "median_home_value",
    ]

    cpi = load_deflator()
    deflator_map = dict(zip(cpi["year"].astype(int), cpi["deflator"]))

    for col in nominal_cols:
        if col in panel.columns:
            real_col = f"{col}_real"
            panel[real_col] = panel[col] * panel["year"].map(deflator_map)

    # ── Compute derived variables ────────────────────────────────────
    # Net job creation rate (BDS)
    if "net_job_creation" in panel.columns and "emp" in panel.columns:
        panel["net_job_creation_rate"] = panel["net_job_creation"] / panel["emp"]

    # Employment-to-population ratio
    if "laus_employment" in panel.columns and "population" in panel.columns:
        panel["emp_pop_ratio"] = panel["laus_employment"] / panel["population"]

    # ── Sort and finalize ────────────────────────────────────────────
    panel = panel.sort_values(["fips", "year"]).reset_index(drop=True)

    log.info("panel_built",
             rows=len(panel),
             cols=len(panel.columns),
             counties=panel["fips"].nunique(),
             years=f"{panel['year'].min()}-{panel['year'].max()}",
             treated=treated_fips,
             donors=panel["donor_eligible"].sum() // len(panel["year"].unique()))

    return panel


def save_panel(panel: pd.DataFrame | None = None) -> dict[str, Path]:
    """Build (if needed) and save the panel in multiple formats."""
    if panel is None:
        panel = build_panel()

    out_dir = PROCESSED_DIR / "panels"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save Parquet (primary format for Python)
    parquet_path = out_dir / "scm_panel.parquet"
    save_parquet(panel, parquet_path, source="scm_panel")

    # Save CSV (for R and inspection)
    csv_path = out_dir / "scm_panel.csv"
    panel.to_csv(csv_path, index=False)
    log.info("panel_csv_saved", path=str(csv_path), rows=len(panel))

    # Save availability matrix
    _save_availability_matrix(panel)

    return {"parquet": parquet_path, "csv": csv_path}


def _save_availability_matrix(panel: pd.DataFrame) -> None:
    """Generate source x year coverage heatmap data."""
    # Group columns by source
    source_cols = {
        "BEA": ["personal_income", "population", "per_capita_income"],
        "BDS": ["firms", "estabs", "emp", "net_job_creation"],
        "QCEW": ["qcew_employment", "qcew_establishments"],
        "LAUS": ["laus_employment", "laus_unemployment_rate"],
        "CBP": ["cbp_employment", "cbp_establishments"],
        "BPS": ["bps_buildings", "bps_value"],
        "QWI": ["qwi_emp", "qwi_earnings"],
        "IRS": ["irs_returns", "irs_agi"],
        "ACS": ["total_population", "poverty_rate"],
    }

    years = sorted(panel["year"].unique())
    records = []

    for source, cols in source_cols.items():
        existing = [c for c in cols if c in panel.columns]
        if not existing:
            continue
        for year in years:
            year_data = panel[panel["year"] == year]
            n_counties = year_data[existing[0]].notna().sum()
            total = len(year_data)
            records.append({
                "source": source,
                "year": year,
                "counties_with_data": n_counties,
                "total_counties": total,
                "coverage": round(n_counties / total, 3) if total > 0 else 0,
            })

    matrix = pd.DataFrame(records)
    out_path = PROCESSED_DIR / "panels" / "availability_matrix.csv"
    matrix.to_csv(out_path, index=False)
    log.info("availability_matrix_saved", path=str(out_path))
