"""The rest of the test suite exercises the full pipeline (nested types, stats, partitioning,
column mapping, deletion vectors, sync, real-DuckDB round trips) against SQLite only. This file
runs a representative slice of the same scenarios against a real local Postgres catalog too, via
the `pg_catalog_config` fixture (skipped automatically without `DELTA2DUCKLAKE_TEST_PG_DSN`) --
confirming the `CatalogBackend` abstraction (parameterized SQL, `?` -> `%s` translation) actually
holds up against a different SQL engine, not just SQLite.
"""

from pathlib import Path

import duckdb

from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.stats_refresh import refresh_stats

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"
DELTA_RS_FIXTURES = Path(__file__).parent / "fixtures" / "delta-rs"


def test_copy_table_nested_types_and_stats(pg_catalog_config, tmp_path):
    data_path = str(tmp_path / "data") + "/"
    bootstrap_catalog(pg_catalog_config, data_path)
    table_root = str(FIXTURES / "parquet-all-types")
    table_id = copy_table(table_root, pg_catalog_config, "all_types")

    backend = pg_catalog_config.connect()
    try:
        columns = backend.fetchall(
            "SELECT column_name, column_type FROM ducklake_column WHERE table_id = ?", (table_id,)
        )
        assert dict(columns)["nested_struct"] == "struct"
        assert dict(columns)["decimal"] == "decimal(10,2)"

        (record_count,) = backend.fetchone(
            "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?", (table_id,)
        )
        assert record_count == 200

        byte_stats = backend.fetchone(
            "SELECT s.contains_null, s.min_value, s.max_value "
            "FROM ducklake_table_column_stats s JOIN ducklake_column c USING (table_id, column_id) "
            "WHERE s.table_id = ? AND c.column_name = 'ByteType'",
            (table_id,),
        )
        assert byte_stats == (True, "-128", "127")
    finally:
        backend.close()


def test_copy_table_partitioning(pg_catalog_config, tmp_path):
    bootstrap_catalog(pg_catalog_config, str(tmp_path / "data") + "/")
    table_id = copy_table(str(FIXTURES / "data-reader-partition-values"), pg_catalog_config, "t")

    backend = pg_catalog_config.connect()
    try:
        (partition_count,) = backend.fetchone(
            "SELECT count(*) FROM ducklake_partition_column WHERE table_id = ?", (table_id,)
        )
        assert partition_count == 12
        null_values = backend.fetchall(
            "SELECT DISTINCT partition_value FROM ducklake_file_partition_value "
            "WHERE table_id = ? AND data_file_id = ("
            "  SELECT data_file_id FROM ducklake_data_file WHERE table_id = ? "
            "  AND path LIKE '%%__HIVE_DEFAULT_PARTITION%%')",
            (table_id, table_id),
        )
        assert null_values == [(None,)]
    finally:
        backend.close()


def test_copy_table_column_mapping(pg_catalog_config, tmp_path):
    bootstrap_catalog(pg_catalog_config, str(tmp_path / "data") + "/")
    table_id = copy_table(
        str(DELTA_RS_FIXTURES / "table_with_column_mapping"), pg_catalog_config, "cm"
    )

    backend = pg_catalog_config.connect()
    try:
        mapping_id = backend.fetchone(
            "SELECT mapping_id FROM ducklake_column_mapping WHERE table_id = ?", (table_id,)
        )[0]
        source_names = {
            r[0]
            for r in backend.fetchall(
                "SELECT source_name FROM ducklake_name_mapping WHERE mapping_id = ?", (mapping_id,)
            )
        }
        assert "col-173b4db9-b5ad-427f-9e75-516aae37fbbb" in source_names
        assert "col-3877fd94-0973-4941-ac6b-646849a1ff65" in source_names
    finally:
        backend.close()


def test_copy_table_deletion_vector(pg_catalog_config, tmp_path):
    bootstrap_catalog(pg_catalog_config, str(tmp_path / "data") + "/")
    table_id = copy_table(str(DELTA_RS_FIXTURES / "table-with-dv-small"), pg_catalog_config, "dv")

    backend = pg_catalog_config.connect()
    try:
        path, delete_count = backend.fetchone(
            "SELECT path, delete_count FROM ducklake_delete_file WHERE table_id = ?", (table_id,)
        )
        assert delete_count == 2
        con = duckdb.connect()
        positions = [r[0] for r in con.sql(f"SELECT pos FROM read_parquet('{path}')").fetchall()]
        assert sorted(positions) == [0, 9]
    finally:
        backend.close()


def test_sync_table_incremental(pg_catalog_config, tmp_path):
    bootstrap_catalog(pg_catalog_config, str(tmp_path / "data") + "/")
    table_root = str(FIXTURES / "snapshot-data3")
    table_id = copy_table(table_root, pg_catalog_config, "evolving", end_version=1)

    backend = pg_catalog_config.connect()
    active = backend.fetchone(
        "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
        (table_id,),
    )[0]
    backend.close()
    assert active == 4

    sync_table(table_root, pg_catalog_config, "evolving", end_version=3)

    backend = pg_catalog_config.connect()
    try:
        active = backend.fetchone(
            "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
            (table_id,),
        )[0]
        (record_count,) = backend.fetchone(
            "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?", (table_id,)
        )
        assert active == 4
        assert record_count == 30
    finally:
        backend.close()


def test_stats_refresh(pg_catalog_config, tmp_path):
    bootstrap_catalog(pg_catalog_config, str(tmp_path / "data") + "/")
    table_id = copy_table(str(FIXTURES / "parquet-all-types"), pg_catalog_config, "all_types")

    refresh_stats(pg_catalog_config, "all_types", columns=["BooleanType"])

    backend = pg_catalog_config.connect()
    try:
        stats = backend.fetchone(
            "SELECT s.min_value, s.max_value "
            "FROM ducklake_table_column_stats s JOIN ducklake_column c USING (table_id, column_id) "
            "WHERE s.table_id = ? AND c.column_name = 'BooleanType'",
            (table_id,),
        )
        assert stats == ("0", "1")
    finally:
        backend.close()


def test_copy_table_matches_delta_scan_via_real_ducklake_extension(pg_catalog_config, tmp_path):
    """The strongest signal: read the Postgres-backed catalog back through DuckDB's real
    `ducklake` extension and cross-check it against `delta_scan`, exactly like the SQLite-backed
    checks in test_verify_duckdb.py -- confirming the Postgres backend isn't just internally
    self-consistent but actually interoperates with real DuckLake tooling.
    """
    data_path = str(tmp_path / "data") + "/"
    bootstrap_catalog(pg_catalog_config, data_path)
    table_root = str(FIXTURES / "parquet-all-types")
    copy_table(table_root, pg_catalog_config, "t")

    con = duckdb.connect()
    try:
        con.sql("INSTALL ducklake; LOAD ducklake; INSTALL delta; LOAD delta;")
    except Exception as e:
        import pytest

        pytest.skip(f"DuckDB ducklake/delta extensions unavailable: {e}")

    con.sql(f"ATTACH '{pg_catalog_config.attach_url()}' AS dl (DATA_PATH '{data_path}', READ_ONLY)")
    try:
        q_dl = "SELECT * FROM dl.t"
        q_delta = f"SELECT * FROM delta_scan('{table_root}')"
        assert con.sql(f"({q_dl}) EXCEPT ({q_delta})").fetchall() == []
        assert con.sql(f"({q_delta}) EXCEPT ({q_dl})").fetchall() == []
    finally:
        con.sql("DETACH dl")
