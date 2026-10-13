"""Tests for the ZAP ingestion. They run offline against a fake API, so CI never depends
on NYC Open Data being up, and every run gives the same result."""

from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from ingest.sources import zap

# --------------------------------------------------------------------------------------
# A fake Socrata API
# --------------------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class FakeSession:
    """Stands in for requests.Session: serves `rows` the way the real API would.

    `reported_count` lets a test make the API's count disagree with its rows.
    Every call's params are saved in `calls` so tests can check what was requested.
    """

    def __init__(self, rows, reported_count=None):
        self.rows = rows
        self.reported_count = len(rows) if reported_count is None else reported_count
        self.calls = []

    def get(self, url, params=None, timeout=None):
        params = params or {}
        self.calls.append(params)
        if params.get("$select") == "count(*)":
            return FakeResponse([{"count": str(self.reported_count)}])
        ordered = sorted(self.rows, key=lambda r: r[params["$order"]])
        start = params["$offset"]
        return FakeResponse(ordered[start : start + params["$limit"]])


def make_rows(n):
    return [{"project_id": f"P{i:04d}", "borough": "Brooklyn"} for i in range(n)]


def page_calls(session):
    return [c for c in session.calls if "$offset" in c]


# --------------------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------------------


def test_fetch_page_sends_limit_offset_and_order():
    session = FakeSession(make_rows(5))
    page = zap.fetch_page(session, zap.BASE_URL, limit=2, offset=2, order="project_id")
    assert [r["project_id"] for r in page] == ["P0002", "P0003"]
    assert session.calls == [{"$limit": 2, "$offset": 2, "$order": "project_id"}]


def test_fetch_all_pages_until_a_short_page():
    session = FakeSession(make_rows(25))
    rows = zap.fetch_all(session, zap.BASE_URL, order="project_id", page_size=10)
    assert len(rows) == 25
    assert [c["$offset"] for c in page_calls(session)] == [0, 10, 20]


def test_fetch_all_when_rows_divide_evenly_into_pages():
    # 20 rows, pages of 10: the third request returns an empty page, which ends the loop.
    session = FakeSession(make_rows(20))
    rows = zap.fetch_all(session, zap.BASE_URL, order="project_id", page_size=10)
    assert len(rows) == 20
    assert [c["$offset"] for c in page_calls(session)] == [0, 10, 20]


def test_fetch_all_returns_no_duplicates():
    session = FakeSession(make_rows(25))
    rows = zap.fetch_all(session, zap.BASE_URL, order="project_id", page_size=10)
    ids = [r["project_id"] for r in rows]
    assert len(ids) == len(set(ids))


def test_fetch_count_reads_the_count_field():
    session = FakeSession(make_rows(7))
    assert zap.fetch_count(session, zap.BASE_URL) == 7


# --------------------------------------------------------------------------------------
# Shaping and writing
# --------------------------------------------------------------------------------------


def test_to_arrow_keeps_columns_missing_from_some_rows():
    rows = [{"project_id": "A", "borough": "Bronx"}, {"project_id": "B", "cc_district": 11}]
    table = zap.to_arrow(rows)
    assert table.column_names == ["borough", "cc_district", "project_id"]
    assert table.column("cc_district").to_pylist() == [None, "11"]  # stored as text


def test_add_metadata_adds_three_columns_on_every_row():
    table = zap.to_arrow(make_rows(3))
    when = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
    result = zap.add_metadata(
        table, source_url="https://example.test", ingested_at=when, batch_id="batch-1"
    )
    for column in zap.METADATA_COLUMNS:
        assert column in result.column_names
    assert result.column("_source_url").to_pylist() == ["https://example.test"] * 3
    assert result.column("_batch_id").to_pylist() == ["batch-1"] * 3
    assert result.column("_ingested_at").to_pylist() == [when] * 3


def test_write_bronze_uses_the_dated_partition(tmp_path):
    path = zap.write_bronze(
        zap.to_arrow(make_rows(3)), ingest_date=date(2026, 10, 10), base=tmp_path
    )
    assert path.parent == tmp_path / "zap_projects" / "ingest_date=2026-10-10"
    assert pq.read_table(path).num_rows == 3


def test_write_bronze_twice_on_the_same_day_replaces_not_duplicates(tmp_path):
    day = date(2026, 10, 10)
    zap.write_bronze(zap.to_arrow(make_rows(5)), ingest_date=day, base=tmp_path)
    path = zap.write_bronze(zap.to_arrow(make_rows(3)), ingest_date=day, base=tmp_path)
    files = list(path.parent.glob("*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).num_rows == 3


# --------------------------------------------------------------------------------------
# The whole run
# --------------------------------------------------------------------------------------


def test_run_end_to_end(tmp_path):
    session = FakeSession(make_rows(25))
    path = zap.run(session, base=tmp_path, today=date(2026, 10, 10), page_size=10)
    table = pq.read_table(path)
    assert table.num_rows == 25
    assert set(zap.METADATA_COLUMNS) <= set(table.column_names)


def test_run_fails_loudly_when_row_count_disagrees_with_api(tmp_path):
    session = FakeSession(make_rows(25), reported_count=30)
    with pytest.raises(ValueError, match="30"):
        zap.run(session, base=tmp_path, today=date(2026, 10, 10), page_size=10)
    assert not (tmp_path / "zap_projects").exists()  # nothing written on failure
