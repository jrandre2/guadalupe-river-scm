.PHONY: install acquire acquire-phase1 acquire-phase2 acquire-phase3 clean test

PYTHON ?= python3

install:
	$(PYTHON) -m pip install -e ".[dev]"

# Run full acquisition pipeline
acquire:
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

# Clean raw data (use with caution)
clean-raw:
	find data/raw -type f ! -name '.gitkeep' -delete

# Clean processed data
clean-processed:
	find data/processed -type f ! -name '.gitkeep' -delete

# Run tests
test:
	$(PYTHON) -m pytest tests/ -v
