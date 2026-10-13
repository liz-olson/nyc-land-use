"""Ingest ZAP project data from NYC Open Data into the bronze layer.

Run it with::

    make ingest-zap
    # or: uv run python -m ingest.sources.zap

One run = one full snapshot of the dataset, saved as Parquet in
``data/bronze/zap_projects/ingest_date=YYYY-MM-DD/``. Values are kept exactly as the API
sends them (all text); typing and cleaning happen later, in the dbt silver models.
"""

from __future__ import annotations

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

from ingest.config import REPO_ROOT, bronze_partition_path

log = logging.getLogger(__name__)

SOURCE_NAME = "zap_projects"
DATASET_ID = "hgx4-8ukb"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
ORDER_BY = "project_id"  # a unique column, so pages never overlap or skip rows
PAGE_SIZE = 10_000

METADATA_COLUMNS = ["_ingested_at", "_source_url", "_batch_id"]


# --------------------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------------------


def build_session(app_token: str | None) -> requests.Session:
    """A requests Session that sends the app token and retries temporary failures.

    A Session reuses one connection for every page (faster) and lets us set headers
    and retry rules once instead of on every call. Retries cover rate limiting (429)
    and server errors (5xx), waiting 1s, 2s, 4s... between attempts.
    """
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


# --------------------------------------------------------------------------------------
# Shaping and writing
# --------------------------------------------------------------------------------------


def to_arrow(rows: list[dict]) -> pa.Table:
    """Convert API rows to an Arrow table with every column stored as text.

    The API leaves a field out of a row entirely when it's empty, so different rows have
    different keys. We collect every key seen in any row, sort them for a stable column
    order, and build the table with that schema: missing values become nulls.
    Keeping everything as strings is deliberate: bronze stores data exactly as received.
    """
    columns = sorted({key for row in rows for key in row})
    schema = pa.schema([(name, pa.string()) for name in columns])
    as_text = [{k: None if v is None else str(v) for k, v in row.items()} for row in rows]
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


def write_bronze(table: pa.Table, *, ingest_date: date, base: Path | None = None) -> Path:
    """Write the table to that day's bronze partition, replacing anything already there."""
    folder = bronze_partition_path(SOURCE_NAME, ingest_date, base=base)
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    path = folder / "part-0.parquet"
    pq.write_table(table, path)
    return path


# --------------------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------------------


def run(
    session: requests.Session | None = None,
    *,
    base: Path | None = None,
    today: date | None = None,
    page_size: int = PAGE_SIZE,
) -> Path:
    """Fetch the whole dataset, check it's complete, and write it to bronze.

    Arguments default to the real thing; tests pass in a fake session, a temp folder
    and a fixed date so they run offline and give the same result every time.
    """
    session = session or build_session(os.environ.get("SOCRATA_APP_TOKEN"))
    ingested_at = datetime.now(UTC)
    today = today or ingested_at.date()
    batch_id = f"{SOURCE_NAME}-{ingested_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

    expected = fetch_count(session, BASE_URL)
    rows = fetch_all(session, BASE_URL, order=ORDER_BY, page_size=page_size)
    if len(rows) != expected:
        raise ValueError(f"fetched {len(rows):,} rows but the API reports {expected:,}")

    table = add_metadata(
        to_arrow(rows), source_url=BASE_URL, ingested_at=ingested_at, batch_id=batch_id
    )
    path = write_bronze(table, ingest_date=today, base=base)
    log.info("wrote %s rows to %s (batch %s)", f"{table.num_rows:,}", path, batch_id)
    return path


def load_dotenv(path: Path = REPO_ROOT / ".env") -> None:
    """Read KEY=value lines from .env into the environment (existing values win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run()


if __name__ == "__main__":
    main()
