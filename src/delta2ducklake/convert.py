"""Public API: `copy_table()` (fresh registration) and `sync_table()` (incremental update).

Both assume the target DuckLake catalog has already been bootstrapped (see
`ducklake.bootstrap.bootstrap_catalog`) -- that's a one-time setup step, kept separate so it's
never silently re-run as a side effect of a copy/sync call.
"""

from __future__ import annotations

import hashlib
import json
import uuid

from delta2ducklake.delta.deletion_vector import deleted_row_positions
from delta2ducklake.delta.partition_layout import (
    PartitionLayoutError,
    hive_path_segments,
    is_hive_style_layout,
)
from delta2ducklake.delta.schema import parse_schema_string
from delta2ducklake.delta.state import load_table_state, touched_paths_since
from delta2ducklake.delta.stats import (
    iter_leaf_column_stats,
    parse_add_stats,
    read_parquet_record_count,
)
from delta2ducklake.ducklake import writer as w
from delta2ducklake.ducklake.catalog import CatalogConfig
from delta2ducklake.ducklake.model import IdAllocator
from delta2ducklake.storage import get_storage_backend, to_duckdb_uri

SOURCE_PATH_KEY = "delta2ducklake.source_path"
SOURCE_VERSION_KEY = "delta2ducklake.source_version"
SCHEMA_HASH_KEY = "delta2ducklake.schema_hash"
MATERIALIZED_PATHS_KEY = "delta2ducklake.materialized_paths"


def _hash_schema_string(schema_string: str) -> str:
    return hashlib.sha256(schema_string.encode("utf-8")).hexdigest()


def _partition_physical_names(schema_tree, partition_columns: list[str]) -> list[str]:
    """Resolve each logical partition column name to its physical Parquet field name (identical
    to the logical name unless column mapping is on), in the same order as `partition_columns`.
    """
    by_name = {f.name: f for f in schema_tree.fields}
    return [(by_name[name].physical_name or name) for name in partition_columns]


def _materialize_partitioned_file(
    storage,
    delta_table_root: str,
    add,
    partition_physical_names: list[str],
    dest_root: str,
    schema_name: str,
    table_name: str,
    credential=None,
) -> str:
    """Copy `add`'s Parquet bytes, byte-for-byte, into `dest_root` laid out as a genuine
    `physical_name=value/.../file.parquet` Hive path -- a destination DuckDB's `ducklake` reader can
    actually parse partition values out of, unlike the source table's own (column-mapping-
    obfuscated) directory layout. Only reached when `materialize_partitions` opts into it;
    otherwise this project never copies or rewrites the source data.
    """
    source_path = storage.resolve(delta_table_root, add.path)
    data = storage.read_bytes(source_path)

    segments = hive_path_segments(partition_physical_names, add.partition_values)
    basename = add.path.rsplit("/", 1)[-1]
    dest_path = (
        f"{dest_root.rstrip('/')}/{schema_name}/{table_name}/"
        + "/".join(segments)
        + f"/{uuid.uuid4()}-{basename}"
    )

    dest_storage = get_storage_backend(dest_root, credential=credential)
    dest_storage.write_bytes(dest_path, data)
    return dest_path


def _resolve_data_file_path_override(
    storage,
    delta_table_root: str,
    add,
    partition_id: int | None,
    partition_physical_names: list[str],
    materialize_partitions: str | None,
    ducklake_data_path: str,
    schema_name: str,
    table_name: str,
    credential=None,
) -> str | None:
    """`None` if `add` can be registered in place (unpartitioned, or already a genuine Hive
    `column=value` layout); otherwise either raises `PartitionLayoutError` (no
    `materialize_partitions` destination given) or returns the materialized copy's path.
    """
    if partition_id is None or is_hive_style_layout(add.path, partition_physical_names):
        return None
    if materialize_partitions is None:
        raise PartitionLayoutError(
            f"{delta_table_root!r}: partition directory layout for {add.path!r} doesn't follow "
            "the `column=value` Hive convention DuckDB's `ducklake` reader requires to "
            "reconstruct partition values -- this happens when Delta column mapping is enabled "
            "on a partitioned table (Databricks then uses opaque directory names instead of "
            "column=value). Pass materialize_partitions='auto' (or a directory path) to "
            "copy_table()/sync_table() to copy affected files into a Hive-style layout under "
            "DuckLake's own storage instead, without touching the source table."
        )
    dest_root = ducklake_data_path if materialize_partitions == "auto" else materialize_partitions
    return _materialize_partitioned_file(
        storage, delta_table_root, add, partition_physical_names, dest_root, schema_name,
        table_name, credential=credential,
    )


def _register_deletion_vector(
    catalog,
    alloc: IdAllocator,
    schema_name: str,
    table_name: str,
    snapshot_id: int,
    table_id: int,
    data_file_id: int,
    storage,
    delta_table_root: str,
    add,
    matched_file_path: str,
    ducklake_data_path: str,
    credential=None,
) -> None:
    """Resolve a Delta deletion vector into row positions, write a DuckLake positional-delete
    Parquet file for it, and register it against `data_file_id`.

    `matched_file_path` must be the exact location `ducklake_data_file.path` was registered with
    for this file (DuckLake matches a delete file's positions to its data file by this path) --
    the source table's own resolved path normally, but the materialized copy's path instead for a
    file `materialize_partitions` relocated (see `_resolve_data_file_path_override`).

    The delete file itself is written into the DuckLake catalog's own managed `data_path` -- never
    into the Delta table's own directory, consistent with this project only ever *reading* from
    there (materialized data-file copies are the one deliberate, opt-in exception).
    """
    positions = deleted_row_positions(add.deletion_vector, storage, delta_table_root)
    parquet_bytes = w.build_positional_delete_parquet(matched_file_path, positions)

    ducklake_storage = get_storage_backend(ducklake_data_path, credential=credential)
    dest_path = (
        f"{ducklake_data_path.rstrip('/')}/{schema_name}/{table_name}/{uuid.uuid4()}-delete.parquet"
    )
    ducklake_storage.write_bytes(dest_path, parquet_bytes)

    delete_file_id = alloc.alloc_file_id()
    w.insert_delete_file(
        catalog, delete_file_id, table_id, snapshot_id, data_file_id, dest_path,
        len(positions), len(parquet_bytes),
    )


def _write_bookkeeping(
    catalog, table_id: int, delta_table_root: str, version: int, schema_hash: str
) -> None:
    catalog.execute(
        "DELETE FROM ducklake_metadata WHERE scope = 'table' AND scope_id = ? "
        "AND key IN (?, ?, ?)",
        (table_id, SOURCE_PATH_KEY, SOURCE_VERSION_KEY, SCHEMA_HASH_KEY),
    )
    catalog.executemany(
        "INSERT INTO ducklake_metadata (key, value, scope, scope_id) VALUES (?, ?, 'table', ?)",
        [
            (SOURCE_PATH_KEY, delta_table_root, table_id),
            (SOURCE_VERSION_KEY, str(version), table_id),
            (SCHEMA_HASH_KEY, schema_hash, table_id),
        ],
    )


def _read_bookkeeping(catalog, table_id: int) -> dict[str, str]:
    rows = catalog.fetchall(
        "SELECT key, value FROM ducklake_metadata WHERE scope = 'table' AND scope_id = ? "
        "AND key IN (?, ?, ?)",
        (table_id, SOURCE_PATH_KEY, SOURCE_VERSION_KEY, SCHEMA_HASH_KEY),
    )
    return dict(rows)


def _read_materialized_paths(catalog, table_id: int) -> dict[str, str]:
    """Delta-relative path -> materialized absolute path, for every file `materialize_partitions`
    has copied into a Hive-style layout so far. `sync_table` needs this to recognize such a file as
    still active (its `ducklake_data_file.path` no longer equals its Delta path) and to know which
    already-resolved path a deletion vector on it must match -- empty for tables with none.
    """
    row = catalog.fetchone(
        "SELECT value FROM ducklake_metadata WHERE scope = 'table' AND scope_id = ? AND key = ?",
        (table_id, MATERIALIZED_PATHS_KEY),
    )
    return json.loads(row[0]) if row is not None else {}


def _write_materialized_paths(
    catalog, table_id: int, materialized_paths: dict[str, str]
) -> None:
    catalog.execute(
        "DELETE FROM ducklake_metadata WHERE scope = 'table' AND scope_id = ? AND key = ?",
        (table_id, MATERIALIZED_PATHS_KEY),
    )
    if materialized_paths:
        catalog.execute(
            "INSERT INTO ducklake_metadata (key, value, scope, scope_id) VALUES (?, ?, 'table', ?)",
            (MATERIALIZED_PATHS_KEY, json.dumps(materialized_paths), table_id),
        )


def copy_table(
    delta_table_root: str,
    catalog_config: CatalogConfig,
    table_name: str,
    *,
    schema_name: str = "main",
    end_version: int | None = None,
    credential=None,
    materialize_partitions: str | None = None,
) -> int:
    """Register a Delta table's currently-active files (or its state as of `end_version`) into a
    brand-new DuckLake table. Fails if `table_name` already exists in `schema_name` -- call
    `sync_table()` to update an existing one instead. Returns the new `table_id`.

    `credential` (e.g. an `azure.core.credentials.TokenCredential`) is forwarded to the storage
    backend for both reading the Delta table and writing any DuckLake deletion-vector files --
    needed whenever `delta_table_root`/the catalog's `data_path` live on a non-public Azure
    storage account.

    `materialize_partitions` controls what happens if a partitioned table's on-disk directory
    layout isn't `column=value` Hive style (which happens when Delta column mapping is enabled on
    Databricks -- see `delta.partition_layout` for why DuckDB's `ducklake` reader can't handle that
    directly). Left as `None` (the default), this raises `PartitionLayoutError` rather than
    register a table DuckDB can't actually read back. Pass `"auto"` to instead copy each affected
    file, byte-for-byte, into a proper Hive layout under the DuckLake catalog's own `data_path`; or
    a directory path/URI to copy into that location instead. Ignored entirely for files that are
    already Hive-style (or for unpartitioned tables) -- this never copies more than it has to.
    """
    storage = get_storage_backend(delta_table_root, credential=credential)
    state = load_table_state(
        storage, delta_table_root, end_version=end_version, allow_column_mapping=True,
        allow_deletion_vectors=True,
    )
    schema_tree = parse_schema_string(state.metadata.schema_string)
    partition_physical_names = _partition_physical_names(
        schema_tree, state.metadata.partition_columns
    )

    catalog = catalog_config.connect()
    try:
        snapshot = w.read_latest_snapshot(catalog)
        new_snapshot_id = snapshot.snapshot_id + 1
        alloc = IdAllocator(snapshot.next_catalog_id, snapshot.next_file_id)

        schema_id = w.find_schema_id(catalog, schema_name, snapshot.snapshot_id)
        if schema_id is None:
            raise ValueError(f"DuckLake schema {schema_name!r} does not exist")
        if w.find_table_id(catalog, schema_id, table_name, snapshot.snapshot_id) is not None:
            raise ValueError(
                f"Table {schema_name}.{table_name} already exists; use sync_table() to update it"
            )

        table_id, columns = w.create_table(
            catalog, alloc, new_snapshot_id, schema_id, table_name, schema_tree.fields,
            to_duckdb_uri(delta_table_root),
        )
        path_to_id = w.path_to_column_id(columns)

        # Required whenever the source Parquet has no embedded field-ids (the normal case for a
        # plain Delta/Spark writer) -- see writer.create_name_mapping's docstring for why this
        # isn't optional even for tables Delta itself considers columnMapping.mode == "none".
        mapping_id = w.create_name_mapping(
            catalog, alloc, table_id, columns, state.metadata.partition_columns
        )

        partition_id = None
        if state.metadata.partition_columns:
            partition_id = w.create_partition_info(
                catalog, alloc, new_snapshot_id, table_id, state.metadata.partition_columns,
                path_to_id,
            )

        leaf_types = {
            leaf.path: leaf.delta_type for leaf in iter_leaf_column_stats(schema_tree.fields, None)
        }
        accumulators = w.new_column_accumulators(columns, leaf_types)
        ducklake_data_path = w.read_data_path(catalog)

        total_records = 0
        total_bytes = 0
        next_row_id = 0
        materialized_paths: dict[str, str] = {}

        for add in state.active_files.values():
            data_file_id = alloc.alloc_file_id()
            parsed_stats = parse_add_stats(add.stats)
            record_count = _resolve_record_count(storage, delta_table_root, add, parsed_stats)

            path_override = _resolve_data_file_path_override(
                storage, delta_table_root, add, partition_id, partition_physical_names,
                materialize_partitions, ducklake_data_path, schema_name, table_name,
                credential=credential,
            )
            if path_override is not None:
                materialized_paths[add.path] = path_override

            row_id_start = next_row_id
            w.insert_data_file(
                catalog, data_file_id, table_id, new_snapshot_id, add, record_count, row_id_start,
                partition_id, mapping_id, path_override=path_override,
            )

            for leaf in iter_leaf_column_stats(schema_tree.fields, parsed_stats):
                column_id = path_to_id[leaf.path]
                w.insert_file_column_stats(
                    catalog, data_file_id, table_id, column_id, leaf, record_count
                )
                w.fold_leaf_into_accumulator(accumulators[column_id], leaf, record_count)

            if partition_id is not None:
                w.insert_file_partition_values(
                    catalog, data_file_id, table_id, add, partition_physical_names
                )

            if add.deletion_vector is not None:
                matched_file_path = path_override or storage.resolve(delta_table_root, add.path)
                _register_deletion_vector(
                    catalog, alloc, schema_name, table_name, new_snapshot_id, table_id,
                    data_file_id, storage, delta_table_root, add, matched_file_path,
                    ducklake_data_path, credential=credential,
                )

            total_records += record_count
            total_bytes += add.size
            next_row_id += record_count

        w.insert_table_stats(catalog, table_id, total_records, next_row_id, total_bytes)
        for column_id, agg in accumulators.items():
            w.insert_table_column_stats(catalog, table_id, column_id, agg)

        _write_materialized_paths(catalog, table_id, materialized_paths)

        schema_hash = _hash_schema_string(state.metadata.schema_string)
        _write_bookkeeping(catalog, table_id, delta_table_root, state.version, schema_hash)

        changes = (
            f"created_table:{w.quote_name(table_name)},inserted_into_table:{table_id}"
        )
        w.insert_snapshot(catalog, new_snapshot_id, snapshot.schema_version + 1, alloc, changes)

        catalog.commit()
        return table_id
    except BaseException:
        catalog.rollback()
        raise
    finally:
        catalog.close()


def sync_table(
    delta_table_root: str,
    catalog_config: CatalogConfig,
    table_name: str,
    *,
    schema_name: str = "main",
    end_version: int | None = None,
    credential=None,
    materialize_partitions: str | None = None,
) -> int:
    """Bring a DuckLake table up to date with the Delta table's currently-active files (or its
    state as of `end_version`): newly-added files are registered, files no longer active are
    retired (their `ducklake_data_file.end_snapshot` set), and table-level stats/record counts are
    updated incrementally on top of what's already there. A no-op (no new snapshot) if nothing
    changed. If `table_name` doesn't exist yet in `schema_name`, this creates it via `copy_table()`
    instead of failing -- sync_table() is safe to call unconditionally, whether or not a previous
    copy_table()/sync_table() call has happened. Returns the table_id.

    `credential` is forwarded to the storage backend exactly as in `copy_table()`.

    `materialize_partitions` is exactly as in `copy_table()`, applied to newly-added files only --
    a file already registered as a materialized copy stays associated with its existing copy
    regardless of this argument (removing/re-materializing it happens automatically only if the
    Delta table itself removes and re-adds that path).

    Raises `NotImplementedError` if the Delta table's schema has changed since it was registered --
    schema evolution during sync is not yet supported.
    """
    storage = get_storage_backend(delta_table_root, credential=credential)
    state = load_table_state(
        storage, delta_table_root, end_version=end_version, allow_column_mapping=True,
        allow_deletion_vectors=True,
    )
    schema_tree = parse_schema_string(state.metadata.schema_string)
    partition_physical_names = _partition_physical_names(
        schema_tree, state.metadata.partition_columns
    )

    catalog = catalog_config.connect()
    try:
        snapshot = w.read_latest_snapshot(catalog)
        schema_id = w.find_schema_id(catalog, schema_name, snapshot.snapshot_id)
        if schema_id is None:
            raise ValueError(f"DuckLake schema {schema_name!r} does not exist")
        table_id = w.find_table_id(catalog, schema_id, table_name, snapshot.snapshot_id)
        if table_id is None:
            # Nothing written yet on this connection -- safe to hand off to a fresh copy_table()
            # call/connection instead of failing.
            catalog.rollback()
            return copy_table(
                delta_table_root, catalog_config, table_name,
                schema_name=schema_name, end_version=end_version, credential=credential,
                materialize_partitions=materialize_partitions,
            )

        bookkeeping = _read_bookkeeping(catalog, table_id)
        if not bookkeeping:
            raise ValueError(
                f"Table {schema_name}.{table_name} has no delta2ducklake bookkeeping metadata -- "
                "it wasn't created by copy_table(), so sync_table() doesn't know its Delta source"
            )
        if bookkeeping.get(SOURCE_PATH_KEY) != delta_table_root:
            raise ValueError(
                f"Table {schema_name}.{table_name} was created from "
                f"{bookkeeping.get(SOURCE_PATH_KEY)!r}, not {delta_table_root!r}"
            )
        schema_hash = _hash_schema_string(state.metadata.schema_string)
        if bookkeeping.get(SCHEMA_HASH_KEY) != schema_hash:
            raise NotImplementedError(
                "The Delta table's schema has changed since this DuckLake table was created; "
                "schema evolution during sync_table() is not yet supported"
            )

        materialized_paths = _read_materialized_paths(catalog, table_id)
        materialized_to_delta = {v: k for k, v in materialized_paths.items()}

        # A materialized file's stored `path` is its copy's location, not its Delta path --
        # translate back so `existing_files` stays keyed by Delta path like `state.active_files`,
        # otherwise every materialized file would look removed-and-re-added (and get re-copied) on
        # every sync.
        existing_files = {
            materialized_to_delta.get(path, path): data_file_id
            for path, data_file_id in catalog.fetchall(
                "SELECT path, data_file_id FROM ducklake_data_file "
                "WHERE table_id = ? AND end_snapshot IS NULL",
                (table_id,),
            )
        }
        active_now = set(state.active_files)
        previously_active = set(existing_files)

        # A path active both before and after the sync window might still have been removed and
        # re-added *within* it (new stats/size on the same path) -- a plain set diff can't see
        # that, so paths touched by any add/remove since the last sync are also treated as changed.
        last_synced_version = int(bookkeeping[SOURCE_VERSION_KEY])
        touched = touched_paths_since(storage, delta_table_root, last_synced_version, state.version)
        changed_in_place = touched & active_now & previously_active

        removed_paths = (previously_active - active_now) | changed_in_place
        new_paths = (active_now - previously_active) | changed_in_place

        if not new_paths and not removed_paths:
            catalog.rollback()
            return table_id

        new_snapshot_id = snapshot.snapshot_id + 1
        alloc = IdAllocator(snapshot.next_catalog_id, snapshot.next_file_id)

        removed_record_count = 0
        removed_file_size = 0
        if removed_paths:
            removed_ids = [existing_files[p] for p in removed_paths]
            placeholders = ", ".join("?" for _ in removed_ids)
            for record_count, file_size_bytes in catalog.fetchall(
                f"SELECT record_count, file_size_bytes FROM ducklake_data_file "
                f"WHERE data_file_id IN ({placeholders})",
                removed_ids,
            ):
                removed_record_count += record_count
                removed_file_size += file_size_bytes
            for path in removed_paths:
                catalog.execute(
                    "UPDATE ducklake_data_file SET end_snapshot = ? WHERE data_file_id = ?",
                    (new_snapshot_id, existing_files[path]),
                )
                materialized_paths.pop(path, None)

        columns = w.load_columns(catalog, table_id, snapshot.snapshot_id)
        path_to_id = w.path_to_column_id(columns)
        partition_id = w.load_partition_id(catalog, table_id, snapshot.snapshot_id)
        mapping_id = w.load_mapping_id(catalog, table_id)
        if mapping_id is None:
            # Every table copy_table() creates gets a map_by_name mapping (required for map
            # columns to read back correctly, see writer.create_name_mapping's docstring) -- a
            # missing one means this table wasn't created by copy_table(), and silently
            # registering new files with mapping_id=NULL would reproduce that same read failure.
            raise ValueError(
                f"Table {schema_name}.{table_name} has no ducklake_column_mapping row -- it "
                "wasn't created by copy_table(), so sync_table() can't safely add files to it"
            )

        leaf_types = {
            leaf.path: leaf.delta_type for leaf in iter_leaf_column_stats(schema_tree.fields, None)
        }
        accumulators = w.load_column_accumulators(catalog, table_id, leaf_types, path_to_id)
        cur_record_count, cur_next_row_id, cur_file_size = w.read_table_stats(catalog, table_id)
        next_row_id = cur_next_row_id
        added_records = 0
        added_bytes = 0
        ducklake_data_path = w.read_data_path(catalog)

        for path in new_paths:
            add = state.active_files[path]
            data_file_id = alloc.alloc_file_id()
            parsed_stats = parse_add_stats(add.stats)
            record_count = _resolve_record_count(storage, delta_table_root, add, parsed_stats)

            path_override = _resolve_data_file_path_override(
                storage, delta_table_root, add, partition_id, partition_physical_names,
                materialize_partitions, ducklake_data_path, schema_name, table_name,
                credential=credential,
            )
            if path_override is not None:
                materialized_paths[add.path] = path_override

            row_id_start = next_row_id
            w.insert_data_file(
                catalog, data_file_id, table_id, new_snapshot_id, add, record_count, row_id_start,
                partition_id, mapping_id, path_override=path_override,
            )

            for leaf in iter_leaf_column_stats(schema_tree.fields, parsed_stats):
                column_id = path_to_id[leaf.path]
                w.insert_file_column_stats(
                    catalog, data_file_id, table_id, column_id, leaf, record_count
                )
                w.fold_leaf_into_accumulator(accumulators[column_id], leaf, record_count)

            if partition_id is not None:
                w.insert_file_partition_values(
                    catalog, data_file_id, table_id, add, partition_physical_names
                )

            if add.deletion_vector is not None:
                matched_file_path = path_override or storage.resolve(delta_table_root, add.path)
                _register_deletion_vector(
                    catalog, alloc, schema_name, table_name, new_snapshot_id, table_id,
                    data_file_id, storage, delta_table_root, add, matched_file_path,
                    ducklake_data_path, credential=credential,
                )

            added_records += record_count
            added_bytes += add.size
            next_row_id += record_count

        _write_materialized_paths(catalog, table_id, materialized_paths)

        w.update_table_stats(
            catalog, table_id,
            cur_record_count - removed_record_count + added_records,
            next_row_id,
            cur_file_size - removed_file_size + added_bytes,
        )
        for column_id, agg in accumulators.items():
            w.update_table_column_stats(catalog, table_id, column_id, agg)

        _write_bookkeeping(catalog, table_id, delta_table_root, state.version, schema_hash)

        changes_parts = []
        if new_paths:
            changes_parts.append(f"inserted_into_table:{table_id}")
        if removed_paths:
            changes_parts.append(f"deleted_from_table:{table_id}")
        w.insert_snapshot(
            catalog, new_snapshot_id, snapshot.schema_version, alloc, ",".join(changes_parts)
        )

        catalog.commit()
        return table_id
    except BaseException:
        catalog.rollback()
        raise
    finally:
        catalog.close()


def _resolve_record_count(storage, delta_table_root, add, parsed_stats) -> int:
    if parsed_stats is not None and parsed_stats.num_records is not None:
        return parsed_stats.num_records
    return read_parquet_record_count(storage, storage.resolve(delta_table_root, add.path))
