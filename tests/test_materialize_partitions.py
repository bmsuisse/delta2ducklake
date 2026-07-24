"""materialize_partitions=... is the opt-in workaround for a real Databricks table whose partition
directories are opaque (column mapping enabled) rather than genuine `column=value` Hive style --
see docs/IMPLEMENTATION.md and delta.partition_layout's module docstring for why DuckDB's own
`ducklake` reader can't handle that directly, no matter what delta2ducklake's own catalog metadata
says. `table_with_column_mapping` (delta-rs, real Databricks output, "8v"/"BH" directories) is the
one vendored fixture that actually exhibits this.
"""

import dataclasses
from pathlib import Path

import pytest

from delta2ducklake import convert
from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.delta.actions import DeletionVectorDescriptor
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


def test_copy_table_rejects_destination_overlapping_source(tmp_path):
    """materialize_partitions must never point back inside the source Delta table -- that would
    silently write copies into a table this project otherwise only ever reads from."""
    config = _fresh_catalog(tmp_path)
    with pytest.raises(ValueError, match="overlaps the source"):
        copy_table(TABLE_ROOT, config, "cm", materialize_partitions=TABLE_ROOT)

    nested = str(Path(TABLE_ROOT) / "some_subdir") + "/"
    with pytest.raises(ValueError, match="overlaps the source"):
        copy_table(TABLE_ROOT, config, "cm2", materialize_partitions=nested)


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


def test_materialize_reuses_source_basename_so_retries_overwrite_not_accumulate(tmp_path):
    """Regression guard: the materialized destination filename must be derived from the source
    file's own (Delta-guaranteed-unique) basename, not a freshly generated uuid -- otherwise
    retrying a failed copy_table()/sync_table() call would leave the first attempt's copies
    orphaned on disk and write a whole new set on every retry, compounding wasted storage.
    """
    config = _fresh_catalog(tmp_path)
    data_path = str(tmp_path / "data") + "/"
    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    backend = config.connect()
    try:
        paths_first = sorted(p for p, _ in _data_files(backend, table_id))
    finally:
        backend.close()

    # Re-materializing the very same source files a second time (bypassing copy_table, which
    # would refuse a second run against an existing table) must land on the exact same paths.
    storage = convert.get_storage_backend(TABLE_ROOT)
    state = convert.load_table_state(storage, TABLE_ROOT, allow_column_mapping=True)
    schema_tree = convert.parse_schema_string(state.metadata.schema_string)
    partition_physical_names = convert._partition_physical_names(
        schema_tree, state.metadata.partition_columns
    )
    dest_storage = convert.get_storage_backend(data_path)
    paths_second = sorted(
        convert._materialize_partitioned_file(
            storage, TABLE_ROOT, add, partition_physical_names, data_path, dest_storage,
            "main", "cm",
        )
        for add in state.active_files.values()
    )
    assert paths_second == paths_first


def test_materialize_dest_storage_backend_built_once_per_call(tmp_path, monkeypatch):
    """`get_storage_backend` for the materialize destination must be built once per copy_table()
    call, not once per file -- rebuilding it per file (e.g. a fresh BlobServiceClient for Azure)
    would discard its own connection cache immediately after a single use."""
    config = _fresh_catalog(tmp_path)

    calls = []
    original = convert.get_storage_backend

    def counting_get_storage_backend(root, *, credential=None):
        calls.append(root)
        return original(root, credential=credential)

    monkeypatch.setattr(convert, "get_storage_backend", counting_get_storage_backend)

    copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    # One call for the source table root, one for the materialize destination -- not one per file
    # (this fixture has 2 files that both need materializing).
    assert len(calls) == 2


def test_materialize_partitions_deletion_vector_uses_materialized_path(tmp_path, monkeypatch):
    """When a materialized file also carries an active deletion vector, the delete file registered
    against it must reference the *materialized* copy's path -- what ducklake_data_file.path
    actually ends up being -- not the source table's own (unreadable, opaque-directory) path,
    otherwise DuckLake's positional-delete matching would silently fail to associate the two.
    """
    config = _fresh_catalog(tmp_path)

    captured = []

    def fake_register_deletion_vector(
        catalog, alloc, schema_name, table_name, snapshot_id, table_id, data_file_id, storage,
        delta_table_root, add, matched_file_path, ducklake_data_path, credential=None,
    ):
        captured.append((add.path, matched_file_path))

    monkeypatch.setattr(convert, "_register_deletion_vector", fake_register_deletion_vector)

    original_load_table_state = convert.load_table_state

    def load_table_state_with_fake_dv(*args, **kwargs):
        state = original_load_table_state(*args, **kwargs)
        path, add = next(iter(state.active_files.items()))
        fake_dv = DeletionVectorDescriptor(
            storage_type="i", path_or_inline_dv="dummy", size_in_bytes=1, cardinality=0
        )
        state.active_files[path] = dataclasses.replace(add, deletion_vector=fake_dv)
        return state

    monkeypatch.setattr(convert, "load_table_state", load_table_state_with_fake_dv)

    table_id = copy_table(TABLE_ROOT, config, "cm", materialize_partitions="auto")

    assert len(captured) == 1
    add_path, matched_file_path = captured[0]

    backend = config.connect()
    try:
        registered_paths = {p for p, _ in _data_files(backend, table_id)}
    finally:
        backend.close()

    # matched_file_path must be one of the *registered* (materialized) paths, not the source's own
    # unreadable path -- proving the deletion vector was matched against the right physical file.
    assert matched_file_path in registered_paths
    naive_source_path = convert.get_storage_backend(TABLE_ROOT).resolve(TABLE_ROOT, add_path)
    assert matched_file_path != naive_source_path
