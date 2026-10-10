"""Phase 0 tests: prove the toolchain works before any real data arrives."""

from datetime import date

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingest.config import bronze_partition_path


def test_bronze_partition_path_uses_hive_style_folders(tmp_path):
    path = bronze_partition_path("zap_projects", date(2026, 10, 10), base=tmp_path)
    assert path == tmp_path / "zap_projects" / "ingest_date=2026-10-10"


@pytest.mark.parametrize("bad_name", ["", "zap projects", "../etc", "zap-projects"])
def test_bronze_partition_path_rejects_unsafe_source_names(tmp_path, bad_name):
    with pytest.raises(ValueError):
        bronze_partition_path(bad_name, date(2026, 10, 10), base=tmp_path)


def test_parquet_round_trip_through_duckdb(tmp_path):
    """Write Parquet with pyarrow, read it back with DuckDB: the core bronze -> silver hop."""
    folder = bronze_partition_path("smoke_test", date(2026, 10, 10), base=tmp_path)
    folder.mkdir(parents=True)
    table = pa.table({"project_id": ["P2026M0001", "P2026K0002"], "borough": ["MN", "BK"]})
    pq.write_table(table, folder / "part-0.parquet")

    rows = duckdb.sql(
        f"""
        select project_id, borough, ingest_date
        from read_parquet('{tmp_path}/smoke_test/*/*.parquet', hive_partitioning = true)
        order by project_id
        """
    ).fetchall()

    assert [r[:2] for r in rows] == [("P2026K0002", "BK"), ("P2026M0001", "MN")]
    # DuckDB turned the folder name into a column: this is what hive partitioning buys you.
    assert str(rows[0][2]) == "2026-10-10"
