from pathlib import Path

import duckdb

from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

DELTA_IO = Path(__file__).parent / "fixtures" / "delta-io"
DELTA_RS = Path(__file__).parent / "fixtures" / "delta-rs"


def _fresh_catalog(tmp_path) -> SQLiteCatalogConfig:
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    return config


def test_copy_table_registers_delete_file_real_fixture(tmp_path):
    """table-with-dv-small: a real Databricks-written DV deletes rows 0 and 9 (value IN (0, 9))
    from a 10-row, single-column table -- already independently confirmed via
    test_deletion_vector.py to decode to exactly {0, 9}.
    """
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(str(DELTA_RS / "table-with-dv-small"), config, "dv")

    backend = config.connect()
    try:
        data_file_id, record_count = backend.fetchone(
            "SELECT data_file_id, record_count FROM ducklake_data_file WHERE table_id = ?",
            (table_id,),
        )
        # numRecords must reflect the physical (pre-delete) row count, per Delta's own protocol
        # requirement for files with a deletion vector -- not the 8 valid/logical rows.
        assert record_count == 10

        (
            delete_file_id,
            delete_data_file_id,
            path,
            path_is_relative,
            delete_count,
        ) = backend.fetchone(
            "SELECT delete_file_id, data_file_id, path, path_is_relative, delete_count "
            "FROM ducklake_delete_file WHERE table_id = ?",
            (table_id,),
        )
        assert delete_data_file_id == data_file_id
        assert delete_count == 2
        assert path_is_relative == 0  # delete files live in the ducklake-managed data_path
        assert path.startswith(str(tmp_path / "data"))

        # And the delete file itself is a real, readable Parquet file with the right shape.
        con = duckdb.connect()
        rows = con.sql(f"SELECT file_path, pos FROM read_parquet('{path}') ORDER BY pos").fetchall()
        assert [pos for _, pos in rows] == [0, 9]
        matched_data_file_path = backend.fetchone(
            "SELECT path FROM ducklake_data_file WHERE data_file_id = ?", (data_file_id,)
        )[0]
        assert all(fp.endswith(matched_data_file_path) for fp, _ in rows)
    finally:
        backend.close()


def test_sync_table_registers_deletion_vector_added_later(tmp_path):
    """table-with-dv-small's v0 has no DV at all; v1 adds one to the same file (a remove+re-add,
    exactly the "changed in place" pattern touched_paths_since exists to catch). copy_table() at
    v0 should register no delete file; sync_table() to v1 should add exactly one.
    """
    config = _fresh_catalog(tmp_path)
    table_root = str(DELTA_RS / "table-with-dv-small")
    table_id = copy_table(table_root, config, "dv", end_version=0)

    backend = config.connect()
    (count_v0,) = backend.fetchone(
        "SELECT count(*) FROM ducklake_delete_file WHERE table_id = ?", (table_id,)
    )
    backend.close()
    assert count_v0 == 0

    sync_table(table_root, config, "dv", end_version=1)

    backend = config.connect()
    try:
        delete_files = backend.fetchall(
            "SELECT delete_count FROM ducklake_delete_file WHERE table_id = ?", (table_id,)
        )
        assert delete_files == [(2,)]
    finally:
        backend.close()


def test_copy_table_deletion_vector_combined_with_column_mapping(tmp_path):
    """dv-with-columnmapping exercises phase 2 and phase 3 together: physical column names *and*
    a deletion vector on the same table.
    """
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(str(DELTA_IO / "dv-with-columnmapping"), config, "t")

    backend = config.connect()
    try:
        (mapping_type,) = backend.fetchone(
            "SELECT type FROM ducklake_column_mapping WHERE table_id = ?", (table_id,)
        )
        assert mapping_type == "map_by_name"
        (delete_file_count,) = backend.fetchone(
            "SELECT count(*) FROM ducklake_delete_file WHERE table_id = ?", (table_id,)
        )
        assert delete_file_count > 0
    finally:
        backend.close()
