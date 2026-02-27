"""Fetch FEMA Disaster Declarations for Texas.

Identifies DR-1257-TX (October 1998 flood) designated counties and builds
the donor pool exclusion list.
"""

from __future__ import annotations

import pandas as pd

from src.config import get_raw_dir, load_project_config
from src.utils.file_io import save_parquet
from src.utils.logging_setup import get_logger
from src.utils.openfema_api import openfema_fetch_all

log = get_logger(__name__)

SOURCE = "fema_declarations"
ENDPOINT = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"


def run(force: bool = False) -> None:
    """Download all Texas disaster declarations from OpenFEMA."""
    out_dir = get_raw_dir(SOURCE)
    declarations_path = out_dir / "tx_declarations.parquet"
    dr1257_path = out_dir / "dr1257_counties.parquet"
    exclusion_path = out_dir / "flood_exclusion_counties.parquet"

    if not force and declarations_path.exists():
        log.info("skip_existing", source=SOURCE)
        return

    cfg = load_project_config()
    disaster_number = int(cfg["project"]["disaster_declaration"].split("-")[1])

    # Fetch all Texas declarations (state field uses 2-letter abbreviation)
    log.info("fetching_declarations", state="TX")
    df = openfema_fetch_all(
        ENDPOINT,
        filter_expr="state eq 'TX'",
        select_fields=[
            "disasterNumber", "declarationDate", "fyDeclared",
            "incidentType", "declarationType", "declarationTitle",
            "incidentBeginDate", "incidentEndDate",
            "fipsStateCode", "fipsCountyCode", "designatedArea",
            "placeCode",
            "ihProgramDeclared", "iaProgramDeclared",
            "paProgramDeclared", "hmProgramDeclared",
        ],
    )

    if df.empty:
        log.error("no_declarations_returned")
        return

    # Build 5-digit FIPS
    df["fips"] = df["fipsStateCode"].astype(str).str.zfill(2) + df["fipsCountyCode"].astype(str).str.zfill(3)

    # Save all Texas declarations
    save_parquet(df, declarations_path, source=SOURCE)

    # Extract DR-1257 counties
    dr1257 = df[df["disasterNumber"] == disaster_number].copy()
    dr1257_fips = dr1257["fips"].unique().tolist()
    log.info("dr1257_counties", n_counties=len(dr1257_fips), fips_sample=dr1257_fips[:5])
    save_parquet(dr1257, dr1257_path, source=SOURCE)

    # Build broader flood exclusion list: counties with flood declarations 1995-2001
    flood_types = ["Flood", "Coastal Storm", "Hurricane", "Severe Storm(s)"]
    df["declarationDate"] = pd.to_datetime(df["declarationDate"], errors="coerce")
    flood_window = df[
        (df["incidentType"].isin(flood_types))
        & (df["declarationDate"].dt.year >= 1995)
        & (df["declarationDate"].dt.year <= 2001)
    ]
    flood_exclusion_fips = flood_window["fips"].unique().tolist()
    log.info("flood_exclusion_counties", n_counties=len(flood_exclusion_fips))

    exclusion_df = pd.DataFrame({
        "fips": flood_exclusion_fips,
        "in_dr1257": [f in dr1257_fips for f in flood_exclusion_fips],
    })
    save_parquet(exclusion_df, exclusion_path, source=SOURCE)

    log.info("declarations_complete", total_rows=len(df), dr1257_counties=len(dr1257_fips))
