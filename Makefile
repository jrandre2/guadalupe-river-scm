.PHONY: install acquire acquire-phase1 acquire-phase2 acquire-phase3 harmonize panel all clean test

PYTHON ?= python3

install:
	$(PYTHON) -m pip install -e ".[dev]"

# ── Full pipeline ──────────────────────────────────────────────────────

all: acquire harmonize panel

# ── Data acquisition ───────────────────────────────────────────────────

# Run all acquisition phases (1-3)
acquire:
	$(PYTHON) -m src.pipeline --phase 1
	$(PYTHON) -m src.pipeline --phase 2
	$(PYTHON) -m src.pipeline --phase 3

# Run only anchor datasets + donor pool
acquire-phase1:
	$(PYTHON) -m src.pipeline --phase 1

# Run extended indicators + disaster funding
acquire-phase2:
	$(PYTHON) -m src.pipeline --phase 2

# Run manual/semi-auto sources
acquire-phase3:
	$(PYTHON) -m src.pipeline --phase 3

# Run a specific task
acquire-%:
	$(PYTHON) -m src.pipeline --task $*

# List all tasks
list-tasks:
	$(PYTHON) -m src.pipeline --list-tasks

# ── Processing ─────────────────────────────────────────────────────────

# Harmonize raw data into standardized county-year panels
harmonize:
	$(PYTHON) -c "from src.process.harmonize_county import run_all; run_all()"

# Build merged SCM panel (runs harmonize + deflator + merge)
panel:
	$(PYTHON) -c "from src.process.harmonize_county import run_all; run_all()"
	$(PYTHON) -c "from src.process.panel_builder import save_panel; save_panel()"

# ── Cleanup ────────────────────────────────────────────────────────────

# Clean raw data (use with caution)
clean-raw:
	find data/raw -type f ! -name '.gitkeep' -delete

# Clean processed data
clean-processed:
	find data/processed -type f ! -name '.gitkeep' -delete

# Clean all data
clean: clean-raw clean-processed

# ── Tests ──────────────────────────────────────────────────────────────

test:
	$(PYTHON) -m pytest tests/ -v
