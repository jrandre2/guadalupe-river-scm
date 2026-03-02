"""HUD Community Development Block Grant – Disaster Recovery (CDBG-DR) data.

CDBG-DR provides flexible federal grants to help communities recover from
presidentially declared disasters, covering housing rehabilitation, economic
revitalization, and infrastructure repair. For the 1998 Guadalupe River flood,
any CDBG-DR allocation to Texas would represent supplemental recovery funding
beyond FEMA's direct assistance programs.

Data source: HUD Disaster Recovery Grants Reporting (DRGR) public portal
  URL: https://drgr.hud.gov/public/

Status: MANUAL DOWNLOAD REQUIRED. The DRGR system requires interactive
navigation and was not fully operational for pre-2004 disasters. DR-1257-TX
(1998) may predate DRGR records entirely.

Fallback options (in order of preference):
  1. HUD Exchange CDBG-DR reports: https://www.hudexchange.info/programs/cdbg-dr/reports/
  2. USAspending.gov — search for HUD disaster grants to Texas, 1998–2002
  3. Texas GLO (General Land Office) — administered CDBG-DR for Texas disasters

This `run()` function logs instructions and exits; it does not download data.
Place any manually acquired files in data/raw/hud_cdbgdr/.
"""

from __future__ import annotations

from src.config import get_raw_dir
from src.utils.logging_setup import get_logger

log = get_logger(__name__)

SOURCE = "hud_cdbgdr"


def run(force: bool = False) -> None:
    """Document manual download steps for HUD CDBG-DR data."""
    out_dir = get_raw_dir(SOURCE)

    log.info(
        "hud_cdbgdr_manual_source",
        instructions=(
            "CDBG-DR data requires manual acquisition:\n"
            "1. Visit DRGR Public Portal: https://drgr.hud.gov/public/\n"
            "2. Search for Texas grants related to DR-1257 or 1998 flood\n"
            "3. Download activity/expenditure reports\n"
            "4. Save to data/raw/hud_cdbgdr/\n"
            "\n"
            "Alternative: Search HUD Exchange for CDBG-DR reports:\n"
            "https://www.hudexchange.info/programs/cdbg-dr/reports/\n"
            "\n"
            "Note: CDBG-DR was less formalized in the late 1990s.\n"
            "The DRGR system launched later and may not contain 1998-era data.\n"
            "Fallback: Use USAspending.gov to find HUD disaster grants to Texas."
        ),
    )
