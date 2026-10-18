"""Tests for the shared Socrata ingestion helpers. Same offline-fake approach as
test_zap.py -- see that file for how FakeSession/FakeResponse work."""

from datetime import UTC, date, datetime

import pyarrow.parquet as pq
import pytest

from ingest import socrata


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        pass


class FakeSession:
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
    return [{"bbl": f"{i:010d}", "borough": "Brooklyn"} for i in range(n)]


def test_fetch_page_sends_limit_offset_and_order():
    session = FakeSession(make_rows(5))
    page = socrata.fetch_page(session, "https://example.test", limit=2, offset=2, order="bbl")
    assert len(page) == 2
    assert session.calls == [{"$limit": 2, "$offset": 2, "$order": "bbl"}]


def test_fetch_all_pages_until_a_short_page():
    session = FakeSession(make_rows(25))
    rows = socrata.fetch_all(session, "https://example.test", order="bbl", page_size=10)
    assert len(rows) == 25


def test_fetch_count_reads_the_count_field():
    session = FakeSession(make_rows(7))
    assert socrata.fetch_count(session, "https://example.test") == 7


def test_to_arrow_keeps_columns_missing_from_some_rows():
    rows = [{"bbl": "A", "borough": "Bronx"}, {"bbl": "B", "cd": 11}]
    table = socrata.to_arrow(rows)
    assert table.column_names == ["bbl", "borough", "cd"]
    assert table.column("cd").to_pylist() == [None, "11"]


def test_to_arrow_stores_a_geometry_dict_as_valid_json_not_python_repr():
    # A geometry field (the_geom) comes back as a nested dict. json.dumps gives valid
    # JSON (double quotes, true/null); Python's str() would give single quotes and
    # True/None, which downstream JSON/GeoJSON parsers can't read.
    rows = [{"coundist": "1", "the_geom": {"type": "Point", "coordinates": [1.5, 2.5]}}]
    table = socrata.to_arrow(rows)
    stored = table.column("the_geom").to_pylist()[0]
    assert stored == '{"type": "Point", "coordinates": [1.5, 2.5]}'


def test_add_metadata_adds_three_columns_on_every_row():
    table = socrata.to_arrow(make_rows(3))
    when = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
    result = socrata.add_metadata(
        table, source_url="https://example.test", ingested_at=when, batch_id="batch-1"
    )
    for column in socrata.METADATA_COLUMNS:
        assert column in result.column_names
    assert result.column("_batch_id").to_pylist() == ["batch-1"] * 3


def test_write_bronze_uses_the_source_name_and_dated_partition(tmp_path):
    path = socrata.write_bronze(
        socrata.to_arrow(make_rows(3)),
        source_name="pluto",
        ingest_date=date(2026, 10, 10),
        base=tmp_path,
    )
    assert path.parent == tmp_path / "pluto" / "ingest_date=2026-10-10"
    assert pq.read_table(path).num_rows == 3


def test_write_bronze_twice_on_the_same_day_replaces_not_duplicates(tmp_path):
    day = date(2026, 10, 10)
    socrata.write_bronze(
        socrata.to_arrow(make_rows(5)), source_name="pluto", ingest_date=day, base=tmp_path
    )
    path = socrata.write_bronze(
        socrata.to_arrow(make_rows(3)), source_name="pluto", ingest_date=day, base=tmp_path
    )
    files = list(path.parent.glob("*.parquet"))
    assert len(files) == 1
    assert pq.read_table(files[0]).num_rows == 3


def test_run_source_end_to_end(tmp_path):
    session = FakeSession(make_rows(25))
    path = socrata.run_source(
        "pluto",
        "https://example.test",
        order_by="bbl",
        session=session,
        base=tmp_path,
        today=date(2026, 10, 10),
        page_size=10,
    )
    table = pq.read_table(path)
    assert table.num_rows == 25
    assert set(socrata.METADATA_COLUMNS) <= set(table.column_names)


def test_run_source_fails_loudly_when_row_count_disagrees_with_api(tmp_path):
    session = FakeSession(make_rows(25), reported_count=30)
    with pytest.raises(ValueError, match="30"):
        socrata.run_source(
            "pluto",
            "https://example.test",
            order_by="bbl",
            session=session,
            base=tmp_path,
            today=date(2026, 10, 10),
            page_size=10,
        )
    assert not (tmp_path / "pluto").exists()
