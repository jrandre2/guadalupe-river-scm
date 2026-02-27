"""Load project configuration from YAML files and environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

# Project root is one level up from src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
METADATA_DIR = DATA_DIR / "metadata"


def _load_yaml(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return yaml.safe_load(f)


def load_project_config() -> dict[str, Any]:
    """Load config/project.yaml."""
    return _load_yaml(CONFIG_DIR / "project.yaml")


def load_sources_config() -> dict[str, Any]:
    """Load config/sources.yaml."""
    return _load_yaml(CONFIG_DIR / "sources.yaml")


def load_env() -> None:
    """Load .env file from project root."""
    load_dotenv(PROJECT_ROOT / ".env")


def get_api_key(service: str) -> str | None:
    """Get an API key from environment. Service should be 'bls', 'census', or 'bea'."""
    load_env()
    key_map = {
        "bls": "BLS_API_KEY",
        "census": "CENSUS_API_KEY",
        "bea": "BEA_API_KEY",
    }
    env_var = key_map.get(service.lower())
    if env_var is None:
        raise ValueError(f"Unknown service: {service}. Expected one of: {list(key_map)}")
    return os.environ.get(env_var) or None


def get_treated_fips() -> str:
    """Return the 5-digit FIPS for the treated unit."""
    cfg = load_project_config()
    return cfg["treated_unit"]["fips"]


def get_state_fips() -> str:
    """Return the 2-digit state FIPS."""
    cfg = load_project_config()
    return cfg["treated_unit"]["state_fips"]


def get_study_period() -> dict[str, int]:
    """Return study period dict with pre_start, pre_end, post_start, post_end."""
    cfg = load_project_config()
    return cfg["study_period"]


def get_raw_dir(source: str) -> Path:
    """Return the raw data directory for a given source, creating it if needed."""
    sources_cfg = load_sources_config()
    if source in sources_cfg:
        subdir = sources_cfg[source].get("output_dir", source)
    else:
        subdir = source
    path = RAW_DIR / subdir
    path.mkdir(parents=True, exist_ok=True)
    return path
