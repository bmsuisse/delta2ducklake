"""materialize_partitions=... is the opt-in workaround for a real Databricks table whose partition
directories are opaque (column mapping enabled) rather than genuine `column=value` Hive style --
see docs/IMPLEMENTATION.md and delta.partition_layout's module docstring for why DuckDB's own
`ducklake` reader can't handle that directly, no matter what delta2ducklake's own catalog metadata
says. `table_with_column_mapping` (delta-rs, real Databricks output, "8v"/"BH" directories) is the
one vendored fixture that actually exhibits this.
"""

from pathlib import Path

import pytest

from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.delta.partition_layout import PartitionLayoutError
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

DELTA_RS = Path(__file__).parent / "fixtures" / "delta-rs"
TABLE_ROOT = str(DELTA_RS / "table_with_column_mapping")


def _fresh_catalog(tmp_path) -> SQLiteCatalogConfig:
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    return config


def _data_files(backend, table_id):
    return backend.fetchall(
        "SELECT path, path_is_relative FROM ducklake_data_file "
        "WHERE table_id = ? AND end_snapshot IS NULL",
        (table_id,),
    )


def test_copy_table_raises_without_materialize_partitions(tmp_path):
    config = _fresh_catalog(tmp_path)
    with pytest.raises(PartitionLayoutError, match="materialize_partitions"):
        copy_table(TABLE_ROOT, config, "cm")


def test_copy_table_materializes_auto_into_ducklake_data_path(tmp_path):
    config = _fresh_catalog(tmp_path)
    data_path = str(tmp_path / "data") + "/"
    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    backend = config.connect()
    try:
        files = _data_files(backend, table_id)
        assert len(files) == 2
        for path, path_is_relative in files:
            assert path_is_relative == 0
            assert path.startswith(data_path)
            assert "/main/cm/" in path
            # One Hive segment per partition column, using the *physical* column name.
            assert "col-173b4db9-b5ad-427f-9e75-516aae37fbbb=" in path
            assert Path(path).is_file()
    finally:
        backend.close()


def test_copy_table_materializes_into_custom_directory(tmp_path):
    config = _fresh_catalog(tmp_path)
    custom_dir = str(tmp_path / "elsewhere") + "/"
    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions=custom_dir)

    backend = config.connect()
    try:
        files = _data_files(backend, table_id)
        assert len(files) == 2
        for path, path_is_relative in files:
            assert path_is_relative == 0
            assert path.startswith(custom_dir)
            assert Path(path).is_file()
    finally:
        backend.close()


def test_materialized_file_bytes_match_source(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    backend = config.connect()
    try:
        paths = [p for p, _ in _data_files(backend, table_id)]
    finally:
        backend.close()

    source_sizes = sorted(
        p.stat().st_size for p in Path(TABLE_ROOT).glob("*/*.parquet")
    )
    copied_sizes = sorted(Path(p).stat().st_size for p in paths)
    assert copied_sizes == source_sizes


def test_sync_table_is_noop_after_materializing_and_does_not_recopy(tmp_path):
    """Regression guard: sync_table must translate a materialized file's stored (copy) path back
    to its original Delta path before diffing against the source's active-file set -- otherwise
    every materialized file looks removed-and-re-added on every sync, forcing a spurious new
    snapshot and a duplicate re-materialized copy each time, even though nothing upstream changed.
    """
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    backend = config.connect()
    try:
        snapshot_count_before = backend.fetchone("SELECT count(*) FROM ducklake_snapshot")[0]
        files_before = _data_files(backend, table_id)
    finally:
        backend.close()

    second_table_id = sync_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")
    assert second_table_id == table_id

    backend = config.connect()
    try:
        snapshot_count_after = backend.fetchone("SELECT count(*) FROM ducklake_snapshot")[0]
        files_after = _data_files(backend, table_id)
    finally:
        backend.close()

    assert snapshot_count_after == snapshot_count_before
    assert sorted(files_after) == sorted(files_before)
