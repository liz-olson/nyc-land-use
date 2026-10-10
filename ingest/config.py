"""Shared paths and settings for ingestion.

Keeping paths in one place means every source module writes bronze data the same way,
and tests can point everything at a temporary folder.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", REPO_ROOT / "data"))
BRONZE_DIR = DATA_DIR / "bronze"


def bronze_partition_path(source: str, ingest_date: date, base: Path | None = None) -> Path:
    """Folder where one day's raw pull of a source is stored.

    Uses Hive-style partitioning (``key=value`` folder names), which DuckDB and most
    query engines understand natively, e.g.::

        data/bronze/zap_projects/ingest_date=2026-10-10/

    Writing each run to its own dated folder keeps raw history and makes reruns
    idempotent: rerunning on the same day replaces that day's folder only.
    """
    if not source or not source.replace("_", "").isalnum():
        raise ValueError(f"source must be a simple snake_case name, got {source!r}")
    root = base if base is not None else BRONZE_DIR
    return root / source / f"ingest_date={ingest_date.isoformat()}"
