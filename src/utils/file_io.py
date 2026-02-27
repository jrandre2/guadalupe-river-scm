"""File I/O utilities for reading/writing Parquet and CSV with consistent naming."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import METADATA_DIR
from src.utils.logging_setup import get_logger

log = get_logger(__name__)


def save_parquet(df: pd.DataFrame, path: Path, source: str | None = None) -> None:
    """Save a DataFrame as Parquet and log to source_log.json.

    Uses pa.Table.from_pydict to bypass pandas→pyarrow conversion issues
    (pandas 2.3 + pyarrow 16 are fundamentally incompatible via df.to_parquet).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pydict: dict[str, list] = {}
    for col in df.columns:
        raw = df[col].to_list()
        # Replace pd.NA / pd.NaT with None so pyarrow sees proper nulls
        pydict[col] = [None if v is pd.NA or v is pd.NaT else v for v in raw]
    # Build table column-by-column to handle mixed-type columns gracefully
    arrays = []
    names = []
    for col_name, values in pydict.items():
        try:
            arrays.append(pa.array(values))
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Mixed types — coerce to strings
            arrays.append(pa.array([str(v) if v is not None else None for v in values], type=pa.string()))
        names.append(col_name)
    table = pa.table(dict(zip(names, arrays)))
    pq.write_table(table, path)
    log.info("saved_parquet", path=str(path), rows=len(df), cols=len(df.columns))
    if source:
        _log_source(source, path, len(df))


def load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet file into a DataFrame.

    Reads via pyarrow then converts column-by-column to avoid
    pandas 2.3 / pyarrow 16 interop bugs.
    """
    table = pq.read_table(path)
    data = {}
    for col_name in table.column_names:
        col = table.column(col_name)
        if pa.types.is_boolean(col.type):
            data[col_name] = col.to_pylist()
        elif pa.types.is_integer(col.type):
            arr = col.to_pylist()
            data[col_name] = pd.array(arr, dtype="Int64")
        elif pa.types.is_floating(col.type):
            data[col_name] = np.array(col.to_pylist(), dtype="float64")
        else:
            data[col_name] = col.to_pylist()
    return pd.DataFrame(data)


def save_csv(df: pd.DataFrame, path: Path, source: str | None = None) -> None:
    """Save a DataFrame as CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    log.info("saved_csv", path=str(path), rows=len(df))
    if source:
        _log_source(source, path, len(df))


def load_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Load a CSV file into a DataFrame."""
    return pd.read_csv(path, **kwargs)


def _log_source(source: str, path: Path, row_count: int) -> None:
    """Append to data/metadata/source_log.json."""
    log_path = METADATA_DIR / "source_log.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    entries: list[dict] = []
    if log_path.exists():
        with open(log_path) as f:
            try:
                entries = json.load(f)
            except json.JSONDecodeError:
                entries = []

    entries.append({
        "source": source,
        "file": str(path),
        "rows": row_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "complete",
    })

    with open(log_path, "w") as f:
        json.dump(entries, f, indent=2)
