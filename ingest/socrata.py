"""Shared helpers for pulling a Socrata (NYC Open Data) dataset into the bronze layer.

ingest/sources/zap.py (Phase 1) has its own copy of this logic, written out in full as
a learning exercise. Every source added since then (Phase 3 on) shares this one module
instead of re-copying it -- same pattern, written once.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingest.config import bronze_partition_path

log = logging.getLogger(__name__)

PAGE_SIZE = 10_000
METADATA_COLUMNS = ["_ingested_at", "_source_url", "_batch_id"]


def build_session(app_token: str | None) -> requests.Session:
    """A requests Session that sends the app token and retries temporary failures."""
    session = requests.Session()
    if app_token:
        session.headers["X-App-Token"] = app_token
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_page(
    session: requests.Session, url: str, *, limit: int, offset: int, order: str
) -> list[dict]:
    """Fetch one page of rows from a Socrata dataset."""
    params = {"$limit": limit, "$offset": offset, "$order": order}
    response = session.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_all(
    session: requests.Session, url: str, *, order: str, page_size: int = PAGE_SIZE
) -> list[dict]:
    """Fetch every row by requesting pages until one comes back short."""
    rows: list[dict] = []
    offset = 0
    while True:
        page = fetch_page(session, url, limit=page_size, offset=offset, order=order)
        rows.extend(page)
        log.info("fetched %s rows at offset %s", f"{len(page):,}", f"{offset:,}")
        if len(page) < page_size:
            return rows
        offset += page_size


def fetch_count(session: requests.Session, url: str) -> int:
    """Ask the API how many rows the dataset has."""
    response = session.get(url, params={"$select": "count(*)"}, timeout=60)
    response.raise_for_status()
    return int(response.json()[0]["count"])


def _as_text(value: object) -> str | None:
    """Stringify one field for bronze storage. str() is fine for scalars; a dict/list
    (not expected for the sources we're using it for, but cheap insurance for later)
    goes through json.dumps() instead, so it stays valid JSON rather than becoming
    Python's repr (single quotes, True/None) if str() were used on it."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def to_arrow(rows: list[dict]) -> pa.Table:
    """Convert API rows to an Arrow table with every column stored as text.

    The API leaves a field out of a row entirely when it's empty, so different rows have
    different keys. We collect every key seen in any row, sort them for a stable column
    order, and build the table with that schema: missing values become nulls.
    """
    columns = sorted({key for row in rows for key in row})
    schema = pa.schema([(name, pa.string()) for name in columns])
    as_text = [{k: _as_text(v) for k, v in row.items()} for row in rows]
    return pa.Table.from_pylist(as_text, schema=schema)


def add_metadata(
    table: pa.Table, *, source_url: str, ingested_at: datetime, batch_id: str
) -> pa.Table:
    """Add load metadata columns, the same value on every row."""
    n = table.num_rows
    table = table.append_column(
        "_ingested_at", pa.array([ingested_at] * n, type=pa.timestamp("us", tz="UTC"))
    )
    table = table.append_column("_source_url", pa.array([source_url] * n, type=pa.string()))
    table = table.append_column("_batch_id", pa.array([batch_id] * n, type=pa.string()))
    return table


def write_bronze(
    table: pa.Table, *, source_name: str, ingest_date: date, base: Path | None = None
) -> Path:
    """Write the table to that day's bronze partition, replacing anything already there."""
    folder = bronze_partition_path(source_name, ingest_date, base=base)
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    path = folder / "part-0.parquet"
    pq.write_table(table, path)
    return path


def run_source(
    source_name: str,
    base_url: str,
    *,
    order_by: str,
    session: requests.Session | None = None,
    base: Path | None = None,
    today: date | None = None,
    page_size: int = PAGE_SIZE,
) -> Path:
    """Fetch one whole Socrata dataset, check it's complete, and write it to bronze.

    Same orchestration as zap.py's run(), generalized to take the source name, URL and
    sort column as arguments -- so each new source's own file can be a few lines that
    just supply those three things.
    """
    session = session or build_session(os.environ.get("SOCRATA_APP_TOKEN"))
    ingested_at = datetime.now(UTC)
    today = today or ingested_at.date()
    batch_id = f"{source_name}-{ingested_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

    expected = fetch_count(session, base_url)
    rows = fetch_all(session, base_url, order=order_by, page_size=page_size)
    if len(rows) != expected:
        raise ValueError(f"fetched {len(rows):,} rows but the API reports {expected:,}")

    table = add_metadata(
        to_arrow(rows), source_url=base_url, ingested_at=ingested_at, batch_id=batch_id
    )
    path = write_bronze(table, source_name=source_name, ingest_date=today, base=base)
    log.info("wrote %s rows to %s (batch %s)", f"{table.num_rows:,}", path, batch_id)
    return path
