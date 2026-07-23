"""Public API: `copy_table()` (fresh registration) and `sync_table()` (incremental update).

Both assume the target DuckLake catalog has already been bootstrapped (see
`ducklake.bootstrap.bootstrap_catalog`) -- that's a one-time setup step, kept separate so it's
never silently re-run as a side effect of a copy/sync call.
"""

from __future__ import annotations

import hashlib
import uuid

from delta2ducklake.delta.deletion_vector import deleted_row_positions
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
from delta2ducklake.storage import get_storage_backend

SOURCE_PATH_KEY = "delta2ducklake.source_path"
SOURCE_VERSION_KEY = "delta2ducklake.source_version"
SCHEMA_HASH_KEY = "delta2ducklake.schema_hash"


def _hash_schema_string(schema_string: str) -> str:
    return hashlib.sha256(schema_string.encode("utf-8")).hexdigest()


def _partition_physical_names(schema_tree, partition_columns: list[str]) -> list[str]:
    """Resolve each logical partition column name to its physical Parquet field name (identical
    to the logical name unless column mapping is on), in the same order as `partition_columns`.
    """
    by_name = {f.name: f for f in schema_tree.fields}
    return [(by_name[name].physical_name or name) for name in partition_columns]


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
    ducklake_data_path: str,
) -> None:
    """Resolve a Delta deletion vector into row positions, write a DuckLake positional-delete
    Parquet file for it, and register it against `data_file_id`.

    The delete file is written into the DuckLake catalog's own managed `data_path` -- never into
    the Delta table's own directory, consistent with this project only ever *reading* from there.
    """
    positions = deleted_row_positions(add.deletion_vector, storage, delta_table_root)
    matched_file_path = storage.resolve(delta_table_root, add.path)
    parquet_bytes = w.build_positional_delete_parquet(matched_file_path, positions)

    ducklake_storage = get_storage_backend(ducklake_data_path)
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


def copy_table(
    delta_table_root: str,
    catalog_config: CatalogConfig,
    table_name: str,
    *,
    schema_name: str = "main",
    end_version: int | None = None,
) -> int:
    """Register a Delta table's currently-active files (or its state as of `end_version`) into a
    brand-new DuckLake table. Fails if `table_name` already exists in `schema_name` -- call
    `sync_table()` to update an existing one instead. Returns the new `table_id`.
    """
    storage = get_storage_backend(delta_table_root)
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
            delta_table_root,
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

        for add in state.active_files.values():
            data_file_id = alloc.alloc_file_id()
            parsed_stats = parse_add_stats(add.stats)
            record_count = _resolve_record_count(storage, delta_table_root, add, parsed_stats)

            row_id_start = next_row_id
            w.insert_data_file(
                catalog, data_file_id, table_id, new_snapshot_id, add, record_count, row_id_start,
                partition_id, mapping_id,
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
                _register_deletion_vector(
                    catalog, alloc, schema_name, table_name, new_snapshot_id, table_id,
                    data_file_id, storage, delta_table_root, add, ducklake_data_path,
                )

            total_records += record_count
            total_bytes += add.size
            next_row_id += record_count

        w.insert_table_stats(catalog, table_id, total_records, next_row_id, total_bytes)
        for column_id, agg in accumulators.items():
            w.insert_table_column_stats(catalog, table_id, column_id, agg)

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
) -> int:
    """Bring a DuckLake table (previously created by `copy_table()`) up to date with the Delta
    table's currently-active files (or its state as of `end_version`): newly-added files are
    registered, files no longer active are retired (their `ducklake_data_file.end_snapshot` set),
    and table-level stats/record counts are updated incrementally on top of what's already there.
    A no-op (no new snapshot) if nothing changed. Returns the table_id.

    Raises `NotImplementedError` if the Delta table's schema has changed since it was registered --
    schema evolution during sync is not yet supported.
    """
    storage = get_storage_backend(delta_table_root)
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
            raise ValueError(
                f"Table {schema_name}.{table_name} does not exist; call copy_table() first"
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

        existing_files = dict(
            catalog.fetchall(
                "SELECT path, data_file_id FROM ducklake_data_file "
                "WHERE table_id = ? AND end_snapshot IS NULL",
                (table_id,),
            )
        )
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

            row_id_start = next_row_id
            w.insert_data_file(
                catalog, data_file_id, table_id, new_snapshot_id, add, record_count, row_id_start,
                partition_id, mapping_id,
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
                _register_deletion_vector(
                    catalog, alloc, schema_name, table_name, new_snapshot_id, table_id,
                    data_file_id, storage, delta_table_root, add, ducklake_data_path,
                )

            added_records += record_count
            added_bytes += add.size
            next_row_id += record_count

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
