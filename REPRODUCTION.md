# Reproduction Guide

Step-by-step instructions for replicating the data pipeline and all analysis scripts for
the Guadalupe River SCM Recovery project.

## Requirements

- **Python 3.11+** for the ETL pipeline (`src/`)
- **R 4.1+** with packages: `tidysynth`, `fixest`, `fwildclusterboot`, `HonestDiD`,
  `augsynth`, `dplyr`, `ggplot2`
- **Quarto** to recompile `manuscript.qmd`
- Internet access for data acquisition (all sources are public bulk endpoints)

## Python Environment

```bash
pip install -e ".[dev]"
```

**API keys are optional.** All data sources have public bulk download endpoints that do
not require authentication. For faster or more targeted API queries (Census, BLS, BEA),
copy `.env.example` to `.env` and add your keys. No secrets are committed to this repo.

## Data Pipeline (Python)

The pipeline runs in three stages using `make` or direct Python invocation.

### Using make

```bash
# Full pipeline: acquire all data, harmonize, and build panel
make all

# Individual stages
make acquire              # download all raw data (phases 1-3)
make acquire-phase1       # anchor datasets and donor pool only
make acquire-bea_income   # a single data source
make harmonize            # standardize raw data to county-year panels
make panel                # merge harmonized data + deflate + export

# Utilities
make list-tasks           # show all available data sources
make clean-processed      # remove processed data (keeps raw)
```

### Direct Python invocation

```bash
python -m src.pipeline --phase 3               # acquire all phases
python -m src.pipeline --task bea_income --force  # re-download one source
python -c "from src.process.harmonize_county import run_all; run_all()"
python -c "from src.process.panel_builder import save_panel; save_panel()"
```

### Pipeline phases

| Phase | Sources | Notes |
|-------|---------|-------|
| 1 — Anchor | `bea_income`, `donor_pool`, `census_bds`, `bls_qcew`, `bls_laus` | Run first; others depend on BEA skeleton |
| 2 — Extended | `census_cbp/zbp/bps/qwi/acs`, `irs_soi`, FEMA suite, `sba_loans`, `noaa_storms`, `usgs_nwis` | Automated bulk downloads |
| 3 — Manual | `hud_cdbgdr`, `tx_comptroller`, `usaspending` | Require manual portal download or semi-automation |

Outputs land in `data/raw/<source>/` (gitignored). See `config/sources.yaml` for
per-source endpoint configuration.

## Analysis Scripts (R)

Run scripts in order; each reads from `data/processed/panels/` and writes to
`data/results/`.

```bash
Rscript R/01_scm_estimate.R       # county-level SCM
Rscript R/02_placebo_tests.R      # in-space, leave-one-out, in-time placebos
Rscript R/03_did_zbp.R            # ZIP DiD on business activity
Rscript R/04_did_hpi.R            # housing HPI DiD
Rscript R/05_ascm.R               # augmented SCM
Rscript R/06_wild_bootstrap.R     # wild cluster bootstrap
Rscript R/07_honestdid.R          # HonestDiD pre-trend sensitivity
Rscript R/08_scm_sector.R         # sector SCM extensions
Rscript R/09_did_lodes.R          # LODES workforce DiD
Rscript R/10_did_zbp_sector.R     # sector-level ZBP DiD
```

## Diagnostic Notebooks (Python/Jupyter)

```bash
jupyter notebook notebooks/01_panel_diagnostics.ipynb
jupyter notebook notebooks/02_donor_screening.ipynb
jupyter notebook notebooks/03_did_zbp_analysis.ipynb
jupyter notebook notebooks/04_housing_hpi_analysis.ipynb
```

Notebooks 01–02 should be run before the R scripts (they produce `donor_pool` inputs).

## Recompiling the Manuscript

```bash
quarto render manuscript.qmd --to pdf
```

The compiled PDF is committed as `manuscript.pdf`. The HTML render (`manuscript.html`)
is gitignored.

## Notes on fwildclusterboot Compatibility

`fwildclusterboot` is incompatible with `feols()` on R 4.1.0. Workaround in `R/06`:
models are re-estimated using `lm()` with explicit `factor(fips) + factor(year)` fixed
effects, then passed to `boottest()`. Results are numerically identical to `feols()`.
