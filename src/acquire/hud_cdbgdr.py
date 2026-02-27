"""HUD CDBG-DR data acquisition.

This is a manual download source — the DRGR portal requires interactive
navigation. The 1998 flood may predate the DRGR system.
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
