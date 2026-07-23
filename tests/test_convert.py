import shutil
from pathlib import Path

import duckdb
import pytest

from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"


def _fresh_catalog(tmp_path) -> SQLiteCatalogConfig:
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    return config


def test_copy_table_basic(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_root = str(FIXTURES / "parquet-all-types")

    table_id = copy_table(table_root, config, "all_types")

    backend = config.connect()
    try:
        columns = backend.fetchall(
            "SELECT column_name, column_type FROM ducklake_column WHERE table_id = ? "
            "ORDER BY column_order",
            (table_id,),
        )
        names = [c[0] for c in columns]
        assert "ByteType" in names
        assert "nested_struct" in names
        assert dict(columns)["decimal"] == "decimal(10,2)"

        data_files = backend.fetchall(
            "SELECT path, path_is_relative, record_count, file_size_bytes "
            "FROM ducklake_data_file WHERE table_id = ?",
            (table_id,),
        )
        assert len(data_files) == 1
        path, path_is_relative, record_count, file_size_bytes = data_files[0]
        assert path == "part-00000-bf6680d4-5e83-4fce-8ebb-d2b60d7e69c9-c000.snappy.parquet"
        assert path_is_relative == 1
        assert record_count == 200
        assert file_size_bytes == 21057

        table_path, table_path_rel = backend.fetchone(
            "SELECT path, path_is_relative FROM ducklake_table WHERE table_id = ?", (table_id,)
        )
        assert table_path == table_root.rstrip("/") + "/"
        assert table_path_rel == 0

        (total_records, next_row_id, total_bytes) = backend.fetchone(
            "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats "
            "WHERE table_id = ?",
            (table_id,),
        )
        assert (total_records, next_row_id, total_bytes) == (200, 200, 21057)

        def _column_stats(column_name: str):
            return backend.fetchone(
                "SELECT s.contains_null, s.min_value, s.max_value "
                "FROM ducklake_table_column_stats s "
                "JOIN ducklake_column c USING (table_id, column_id) "
                "WHERE s.table_id = ? AND c.column_name = ?",
                (table_id, column_name),
            )

        # ByteType really has 3 nulls out of 200 rows in the source data (ground truth from the
        # fixture's own add.stats.nullCount) -- contains_null must reflect that, not just "False".
        assert _column_stats("ByteType") == (1, "-128", "127")

        # BooleanType/BinaryType get no min/max from this writer at all -- must stay NULL, not
        # some made-up default.
        assert _column_stats("BooleanType") == (1, None, None)
    finally:
        backend.close()


def test_copy_table_raises_if_table_already_exists(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_root = str(FIXTURES / "parquet-all-types")
    copy_table(table_root, config, "all_types")
    with pytest.raises(ValueError, match="already exists"):
        copy_table(table_root, config, "all_types")


def test_copy_table_with_partitioning(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_root = str(FIXTURES / "data-reader-partition-values")

    table_id = copy_table(table_root, config, "partitioned")

    backend = config.connect()
    try:
        partition_columns = backend.fetchall(
            "SELECT pc.partition_key_index, c.column_name "
            "FROM ducklake_partition_column pc JOIN ducklake_column c USING (table_id, column_id) "
            "WHERE pc.table_id = ? ORDER BY pc.partition_key_index",
            (table_id,),
        )
        assert [name for _, name in partition_columns] == [
            "as_int", "as_long", "as_byte", "as_short", "as_boolean", "as_float", "as_double",
            "as_string", "as_string_lit_null", "as_date", "as_timestamp", "as_big_decimal",
        ]

        # 3 data files: as_int=0..., as_int=__HIVE_DEFAULT_PARTITION__..., as_int=1...
        data_files = backend.fetchall(
            "SELECT data_file_id FROM ducklake_data_file WHERE table_id = ?", (table_id,)
        )
        assert len(data_files) == 3

        # The __HIVE_DEFAULT_PARTITION__ file's partition value must come through as NULL, not the
        # literal string "__HIVE_DEFAULT_PARTITION__" (Delta already normalizes this to JSON null).
        null_partition_values = backend.fetchall(
            "SELECT DISTINCT partition_value FROM ducklake_file_partition_value "
            "WHERE table_id = ? AND data_file_id = ("
            "  SELECT data_file_id FROM ducklake_data_file WHERE table_id = ? "
            "  AND path LIKE '%__HIVE_DEFAULT_PARTITION%')",
            (table_id, table_id),
        )
        assert null_partition_values == [(None,)]
    finally:
        backend.close()


def test_sync_table_add_and_remove_across_versions(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_root = str(FIXTURES / "snapshot-data3")

    # v0 (2 files, 5 rows each) + v1 (2 more, 5 rows each) = 4 files, 20 rows.
    table_id = copy_table(table_root, config, "evolving", end_version=1)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 4
        assert _table_record_count(backend, table_id) == 20
    finally:
        backend.close()

    # v2 replaces all 4 v0/v1 files with 2 new ones (5 rows each) -> net 2 files, 10 rows.
    sync_table(table_root, config, "evolving", end_version=2)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 2
        assert _table_record_count(backend, table_id) == 10
        # retired files must be tombstoned (end_snapshot set), not deleted outright.
        retired = backend.fetchone(
            "SELECT count(*) FROM ducklake_data_file "
            "WHERE table_id = ? AND end_snapshot IS NOT NULL",
            (table_id,),
        )
        assert retired[0] == 4
    finally:
        backend.close()

    # v3 adds 2 more files (10 rows each) on top -> 4 files, 30 rows.
    sync_table(table_root, config, "evolving", end_version=3)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 4
        assert _table_record_count(backend, table_id) == 30
    finally:
        backend.close()


def test_sync_table_is_noop_when_nothing_changed(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_root = str(FIXTURES / "parquet-all-types")
    copy_table(table_root, config, "all_types")

    backend = config.connect()
    before_snapshot_count = backend.fetchone("SELECT count(*) FROM ducklake_snapshot")[0]
    backend.close()

    sync_table(table_root, config, "all_types")

    backend = config.connect()
    after_snapshot_count = backend.fetchone("SELECT count(*) FROM ducklake_snapshot")[0]
    backend.close()
    assert after_snapshot_count == before_snapshot_count


def test_sync_table_raises_if_table_missing(tmp_path):
    config = _fresh_catalog(tmp_path)
    with pytest.raises(ValueError, match="does not exist"):
        sync_table(str(FIXTURES / "parquet-all-types"), config, "nope")


def test_sync_table_detects_path_removed_and_readded_in_place(tmp_path):
    """delete-re-add-same-file-different-transactions is a log-only fixture (no real Parquet
    files) designed to probe exactly this edge case, so we materialize trivial real Parquet files
    for its "foo"/"bar" paths ourselves to exercise the full copy_table/sync_table pipeline.
    """
    src_log = FIXTURES / "delete-re-add-same-file-different-transactions" / "_delta_log"
    table_root = tmp_path / "table"
    shutil.copytree(src_log, table_root / "_delta_log")

    con = duckdb.connect()
    con.sql(f"COPY (SELECT 1::INTEGER AS intCol) TO '{table_root}/foo' (FORMAT PARQUET)")
    con.sql(f"COPY (SELECT 2::INTEGER AS intCol) TO '{table_root}/bar' (FORMAT PARQUET)")

    config = _fresh_catalog(tmp_path)
    table_id = copy_table(str(table_root), config, "reused_path", end_version=0)

    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 1
        original_file_id = backend.fetchone(
            "SELECT data_file_id FROM ducklake_data_file "
            "WHERE table_id = ? AND end_snapshot IS NULL",
            (table_id,),
        )[0]
    finally:
        backend.close()

    # v1: "foo" removed -> table goes empty.
    sync_table(str(table_root), config, "reused_path", end_version=1)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 0
        assert _table_record_count(backend, table_id) == 0
    finally:
        backend.close()

    # v2: "foo" re-added (same path, different transaction) -- must be treated as a fresh file,
    # not silently ignored because the path was "already known".
    sync_table(str(table_root), config, "reused_path", end_version=2)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 1
        assert _table_record_count(backend, table_id) == 1
        new_file_id = backend.fetchone(
            "SELECT data_file_id FROM ducklake_data_file "
            "WHERE table_id = ? AND end_snapshot IS NULL",
            (table_id,),
        )[0]
        assert new_file_id != original_file_id
        old_row_retired = backend.fetchone(
            "SELECT end_snapshot FROM ducklake_data_file WHERE data_file_id = ?",
            (original_file_id,),
        )[0]
        assert old_row_retired is not None
    finally:
        backend.close()

    # v3: "bar" added on top -> 2 active files.
    sync_table(str(table_root), config, "reused_path", end_version=3)
    backend = config.connect()
    try:
        assert _active_file_count(backend, table_id) == 2
        assert _table_record_count(backend, table_id) == 2
    finally:
        backend.close()


def _active_file_count(backend, table_id) -> int:
    return backend.fetchone(
        "SELECT count(*) FROM ducklake_data_file WHERE table_id = ? AND end_snapshot IS NULL",
        (table_id,),
    )[0]


def _table_record_count(backend, table_id) -> int:
    return backend.fetchone(
        "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?", (table_id,)
    )[0]
