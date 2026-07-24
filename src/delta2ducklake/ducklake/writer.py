"""Write Delta table state into an already-bootstrapped DuckLake catalog.

Everything here runs as plain parameterized SQL through a `CatalogBackend` (never through DuckDB
itself) -- this is delta2ducklake's actual value-add: DuckDB's own SQL surface has no way to
register a pre-existing external Parquet file into a DuckLake table without copying it.
"""

from __future__ import annotations

import math
import tempfile
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from delta2ducklake.delta.actions import AddAction
from delta2ducklake.delta.schema import (
    ArrayType,
    DeltaType,
    MapType,
    StructField,
    StructType,
    ducklake_column_type,
)
from delta2ducklake.delta.stats import (
    LeafColumnStats,
    StatValue,
    decode_ducklake_stat,
    encode_ducklake_stat,
)
from delta2ducklake.ducklake.catalog import CatalogBackend
from delta2ducklake.ducklake.model import FlattenedColumn, IdAllocator, Snapshot


def _now_iso() -> str:
    return datetime.now(UTC).isoformat(sep=" ", timespec="microseconds")


def quote_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# --- snapshots ---------------------------------------------------------------------------------


def read_latest_snapshot(catalog: CatalogBackend) -> Snapshot:
    row = catalog.fetchone(
        "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
        "FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1"
    )
    if row is None:
        raise RuntimeError("ducklake_snapshot has no rows -- catalog was never bootstrapped")
    return Snapshot(
        snapshot_id=row[0], schema_version=row[1], next_catalog_id=row[2], next_file_id=row[3]
    )


def insert_snapshot(
    catalog: CatalogBackend,
    snapshot_id: int,
    schema_version: int,
    alloc: IdAllocator,
    changes_made: str,
    author: str | None = None,
    commit_message: str | None = None,
) -> None:
    catalog.execute(
        "INSERT INTO ducklake_snapshot "
        "(snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id) "
        "VALUES (?, ?, ?, ?, ?)",
        (snapshot_id, _now_iso(), schema_version, alloc.next_catalog_id, alloc.next_file_id),
    )
    catalog.execute(
        "INSERT INTO ducklake_snapshot_changes "
        "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
        "VALUES (?, ?, ?, ?, ?)",
        (snapshot_id, changes_made, author, commit_message, None),
    )


# --- schema / table lookup -----------------------------------------------------------------------


def find_schema_id(catalog: CatalogBackend, schema_name: str, snapshot_id: int) -> int | None:
    row = catalog.fetchone(
        "SELECT schema_id FROM ducklake_schema WHERE schema_name = ? "
        "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
        (schema_name, snapshot_id, snapshot_id),
    )
    return row[0] if row else None


def find_table_id(
    catalog: CatalogBackend, schema_id: int, table_name: str, snapshot_id: int
) -> int | None:
    row = catalog.fetchone(
        "SELECT table_id FROM ducklake_table WHERE schema_id = ? AND table_name = ? "
        "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
        (schema_id, table_name, snapshot_id, snapshot_id),
    )
    return row[0] if row else None


# --- schema flattening ----------------------------------------------------------------------------


def flatten_schema(
    fields: tuple[StructField, ...], alloc: IdAllocator
) -> list[FlattenedColumn]:
    """Recursively flatten a Delta schema's fields into `ducklake_column` rows, assigning a fresh
    `column_id` (from the catalog-id counter) to every node -- including `struct`/`list`/`map`
    container columns themselves, which get their own row with no stats, per DuckLake's nested
    type model (`parent_column` links a child to its container).

    `list`/`map` children are synthesized as `"element"` / `"key"`+`"value"` (DuckDB's own naming
    for these, confirmed against DuckLake's `data_types.md` nested-type example) since Delta's
    schema JSON doesn't name them at all. This isn't optional: DuckDB's real `ducklake` extension
    crashes trying to read a `list`/`map` column that has no matching child row (confirmed by
    attaching a real catalog missing them and observing an internal DuckDB assertion failure).
    """
    result: list[FlattenedColumn] = []
    for order, f in enumerate(fields):
        _flatten_node(f.name, f.type, f.nullable, order, alloc, None, (), result, f.physical_name)
    return result


def _flatten_node(
    name: str,
    delta_type: DeltaType,
    nullable: bool,
    column_order: int,
    alloc: IdAllocator,
    parent_column_id: int | None,
    path: tuple[str, ...],
    out: list[FlattenedColumn],
    physical_name: str | None = None,
) -> None:
    column_id = alloc.alloc_catalog_id()
    child_path = (*path, name)
    out.append(
        FlattenedColumn(
            column_id=column_id,
            path=child_path,
            name=name,
            column_order=column_order,
            ducklake_type=ducklake_column_type(delta_type),
            nulls_allowed=nullable,
            parent_column_id=parent_column_id,
            physical_name=physical_name,
        )
    )
    if isinstance(delta_type, StructType):
        for order, f in enumerate(delta_type.fields):
            _flatten_node(
                f.name, f.type, f.nullable, order, alloc, column_id, child_path, out,
                f.physical_name,
            )
    elif isinstance(delta_type, ArrayType):
        # Delta's column mapping never applies to list elements individually -- Parquet's
        # conventional "element" name is used regardless of columnMapping.mode, so there's no
        # separate physical name to carry here.
        _flatten_node(
            "element", delta_type.element_type, delta_type.contains_null, 0, alloc, column_id,
            child_path, out,
        )
    elif isinstance(delta_type, MapType):
        _flatten_node("key", delta_type.key_type, False, 0, alloc, column_id, child_path, out)
        _flatten_node(
            "value", delta_type.value_type, delta_type.value_contains_null, 1, alloc, column_id,
            child_path, out,
        )


def insert_columns(
    catalog: CatalogBackend, snapshot_id: int, table_id: int, columns: list[FlattenedColumn]
) -> None:
    catalog.executemany(
        "INSERT INTO ducklake_column "
        "(column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, "
        "column_type, initial_default, default_value, nulls_allowed, parent_column, "
        "default_value_type, default_value_dialect) "
        "VALUES (?, ?, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL, NULL)",
        [
            (
                c.column_id,
                snapshot_id,
                table_id,
                c.column_order,
                c.name,
                c.ducklake_type,
                c.nulls_allowed,
                c.parent_column_id,
            )
            for c in columns
        ],
    )


def path_to_column_id(columns: list[FlattenedColumn]) -> dict[tuple[str, ...], int]:
    return {c.path: c.column_id for c in columns}


# --- table creation -------------------------------------------------------------------------------


def create_table(
    catalog: CatalogBackend,
    alloc: IdAllocator,
    snapshot_id: int,
    schema_id: int,
    table_name: str,
    fields: tuple[StructField, ...],
    table_path: str,
) -> tuple[int, list[FlattenedColumn]]:
    """Insert `ducklake_table` + all of its `ducklake_column` rows. `table_path` is stored as an
    absolute path (`path_is_relative = False`) pointing at the Delta table's own root directory --
    every data file's path is then just Delta's own relative `add.path` underneath it, so the
    physical file location never changes.

    DuckLake joins `table.path` and a relative file path by plain string concatenation (confirmed
    against a real catalog: bootstrap's own "main" schema is stored as `"main/"`, trailing slash
    included) rather than inserting a separator -- so the trailing slash is mandatory, not cosmetic.
    """
    table_id = alloc.alloc_catalog_id()
    normalized_path = table_path.rstrip("/") + "/"
    catalog.execute(
        "INSERT INTO ducklake_table "
        "(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, "
        "path_is_relative) VALUES (?, ?, ?, NULL, ?, ?, ?, ?)",
        (table_id, str(uuid.uuid4()), snapshot_id, schema_id, table_name, normalized_path, False),
    )
    columns = flatten_schema(fields, alloc)
    insert_columns(catalog, snapshot_id, table_id, columns)
    return table_id, columns


# --- name mapping (map_by_name) ------------------------------------------------------------------


def create_name_mapping(
    catalog: CatalogBackend,
    alloc: IdAllocator,
    table_id: int,
    columns: list[FlattenedColumn],
    partition_columns: list[str],
) -> int:
    """Register a `map_by_name` `ducklake_column_mapping` covering every column (including
    `struct`/`list`/`map` containers, not just leaves).

    Required, not optional, whenever the underlying Parquet files carry no field-ids of their own
    -- which is the normal case for a plain Delta/Spark writer with no column mapping enabled.
    Confirmed empirically: attaching a real DuckLake catalog that registered `map`-typed columns
    *without* this raised a genuine DuckDB error (`'key' of MAP did not map to a value...`) reading
    them back, even though `struct`/`list` columns happened to read back fine without it -- adding
    the mapping fixed it. Applied unconditionally to every table (harmless for already-simple
    schemas) rather than only when a `map` column is present, to keep behavior uniform.

    `source_name` is `c.physical_name` when set (Delta `columnMapping.mode = name`/`id`: the
    Parquet field's real on-disk name differs from the logical `ducklake_column.column_name`),
    else `c.name` -- the physical and logical names are identical for a plain
    `columnMapping.mode = none` table, which is the common case this mapping exists for anyway.
    """
    mapping_id = alloc.alloc_catalog_id()
    catalog.execute(
        "INSERT INTO ducklake_column_mapping (mapping_id, table_id, type) "
        "VALUES (?, ?, 'map_by_name')",
        (mapping_id, table_id),
    )
    catalog.executemany(
        "INSERT INTO ducklake_name_mapping "
        "(mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                mapping_id,
                c.column_id,
                c.physical_name or c.name,
                c.column_id,
                c.parent_column_id,
                len(c.path) == 1 and c.name in partition_columns,
            )
            for c in columns
        ],
    )
    return mapping_id


def load_mapping_id(catalog: CatalogBackend, table_id: int) -> int | None:
    row = catalog.fetchone(
        "SELECT mapping_id FROM ducklake_column_mapping WHERE table_id = ?", (table_id,)
    )
    return row[0] if row else None


# --- partitioning --------------------------------------------------------------------------------


def create_partition_info(
    catalog: CatalogBackend,
    alloc: IdAllocator,
    snapshot_id: int,
    table_id: int,
    partition_columns: list[str],
    path_to_id: dict[tuple[str, ...], int],
) -> int:
    """Register Delta's partition columns (always Hive-style, unmodified physical layout) as a
    DuckLake `identity`-transform partition spec -- a pure metadata overlay that doesn't require
    (or assume anything about) the actual directory layout of the Parquet files.
    """
    partition_id = alloc.alloc_catalog_id()
    catalog.execute(
        "INSERT INTO ducklake_partition_info "
        "(partition_id, table_id, begin_snapshot, end_snapshot) VALUES (?, ?, ?, NULL)",
        (partition_id, table_id, snapshot_id),
    )
    catalog.executemany(
        "INSERT INTO ducklake_partition_column "
        "(partition_id, table_id, partition_key_index, column_id, transform) "
        "VALUES (?, ?, ?, ?, 'identity')",
        [
            (partition_id, table_id, idx, path_to_id[(name,)])
            for idx, name in enumerate(partition_columns)
        ],
    )
    return partition_id


def insert_file_partition_values(
    catalog: CatalogBackend,
    data_file_id: int,
    table_id: int,
    add: AddAction,
    partition_physical_names: list[str],
) -> None:
    """`partition_physical_names` must be in the same order as Delta's `partitionColumns` and
    already resolved to *physical* names -- `add.partitionValues` is keyed by physical name under
    column mapping (confirmed against the real `table_with_column_mapping` fixture), identical to
    the logical name for a plain `columnMapping.mode = none` table.
    """
    catalog.executemany(
        "INSERT INTO ducklake_file_partition_value "
        "(data_file_id, table_id, partition_key_index, partition_value) VALUES (?, ?, ?, ?)",
        [
            (data_file_id, table_id, idx, add.partition_values.get(name))
            for idx, name in enumerate(partition_physical_names)
        ],
    )


# --- data files and stats -----------------------------------------------------------------------


def insert_data_file(
    catalog: CatalogBackend,
    data_file_id: int,
    table_id: int,
    snapshot_id: int,
    add: AddAction,
    record_count: int,
    row_id_start: int,
    partition_id: int | None,
    mapping_id: int,
    *,
    path_override: str | None = None,
) -> None:
    """`path_override`, when given, replaces `add.path` and is stored as an *absolute*
    (`path_is_relative = False`) location instead of Delta's own relative path -- used when the
    source file was materialized into a Hive-style copy under DuckLake's own storage (see
    `convert.py::_materialize_partitioned_file`) rather than registered in place.
    """
    path = add.path if path_override is None else path_override
    path_is_relative = path_override is None
    catalog.execute(
        "INSERT INTO ducklake_data_file "
        "(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, "
        "path_is_relative, file_format, record_count, file_size_bytes, footer_size, "
        "row_id_start, partition_id, encryption_key, mapping_id, partial_max) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?, 'parquet', ?, ?, NULL, ?, ?, NULL, ?, NULL)",
        (
            data_file_id,
            table_id,
            snapshot_id,
            data_file_id,  # file_order: any value unique-within-snapshot works; file_id serves fine
            path,
            path_is_relative,
            record_count,
            add.size,
            row_id_start,
            partition_id,
            mapping_id,
        ),
    )


def insert_file_column_stats(
    catalog: CatalogBackend,
    data_file_id: int,
    table_id: int,
    column_id: int,
    leaf: LeafColumnStats,
    record_count: int,
) -> None:
    min_enc, min_nan = encode_ducklake_stat(leaf.min_value, leaf.delta_type)
    max_enc, max_nan = encode_ducklake_stat(leaf.max_value, leaf.delta_type)
    catalog.execute(
        "INSERT INTO ducklake_file_column_stats "
        "(data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, "
        "min_value, max_value, contains_nan, extra_stats) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL)",
        (
            data_file_id,
            table_id,
            column_id,
            record_count,
            leaf.null_count,
            min_enc,
            max_enc,
            min_nan or max_nan,
        ),
    )


@dataclass
class ColumnStatsAccumulator:
    """Running aggregate across every file in a table, kept in typed (not string-encoded) form so
    comparisons are correct (see delta/stats.py's module docstring for why this matters)."""

    delta_type: str
    contains_null: bool = False
    contains_nan: bool = False
    min_value: StatValue | None = None
    max_value: StatValue | None = None
    min_set: bool = False
    max_set: bool = False
    unknown_null_count: bool = False


def new_column_accumulators(
    columns: list[FlattenedColumn], leaf_types: dict[tuple[str, ...], str]
) -> dict[int, ColumnStatsAccumulator]:
    return {
        c.column_id: ColumnStatsAccumulator(delta_type=leaf_types[c.path])
        for c in columns
        if c.path in leaf_types
    }


def _lt(a: StatValue, b: StatValue) -> bool:
    """Safe only because every value folded into one column's accumulator shares the same
    underlying type by construction (Delta/DuckLake stats are homogeneously typed per column)."""
    return a < b  # ty: ignore[unsupported-operator]


def _gt(a: StatValue, b: StatValue) -> bool:
    return a > b  # ty: ignore[unsupported-operator]


def fold_leaf_into_accumulator(
    agg: ColumnStatsAccumulator, leaf: LeafColumnStats, record_count: int | None
) -> None:
    if leaf.null_count is not None:
        if leaf.null_count > 0:
            agg.contains_null = True
    else:
        # No null count reported for this file/column at all -- can't prove there are no nulls.
        agg.unknown_null_count = True

    for value, is_min in ((leaf.min_value, True), (leaf.max_value, False)):
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            agg.contains_nan = True
            continue
        if is_min:
            if agg.min_value is None or _lt(value, agg.min_value):
                agg.min_value = value
                agg.min_set = True
        else:
            if agg.max_value is None or _gt(value, agg.max_value):
                agg.max_value = value
                agg.max_set = True


def insert_table_column_stats(
    catalog: CatalogBackend, table_id: int, column_id: int, agg: ColumnStatsAccumulator
) -> None:
    min_enc, _ = (
        encode_ducklake_stat(agg.min_value, agg.delta_type) if agg.min_set else (None, False)
    )
    max_enc, _ = (
        encode_ducklake_stat(agg.max_value, agg.delta_type) if agg.max_set else (None, False)
    )
    catalog.execute(
        "INSERT INTO ducklake_table_column_stats "
        "(table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
        "VALUES (?, ?, ?, ?, ?, ?, NULL)",
        (
            table_id,
            column_id,
            agg.contains_null or agg.unknown_null_count,
            agg.contains_nan,
            min_enc,
            max_enc,
        ),
    )


def insert_table_stats(
    catalog: CatalogBackend,
    table_id: int,
    record_count: int,
    next_row_id: int,
    file_size_bytes: int,
) -> None:
    catalog.execute(
        "INSERT INTO ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes) "
        "VALUES (?, ?, ?, ?)",
        (table_id, record_count, next_row_id, file_size_bytes),
    )


# --- reading back an existing table (for sync_table) ---------------------------------------------


def load_columns(catalog: CatalogBackend, table_id: int, snapshot_id: int) -> list[FlattenedColumn]:
    """Reconstruct `ducklake_column` rows (including each column's dotted `path`) for a table
    already registered as of `snapshot_id` -- used by `sync_table` to resolve column ids without
    re-flattening the schema (and thus without allocating new column ids for existing columns).
    """
    rows = catalog.fetchall(
        "SELECT column_id, column_order, column_name, column_type, nulls_allowed, parent_column "
        "FROM ducklake_column WHERE table_id = ? "
        "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL) "
        "ORDER BY column_order",
        (table_id, snapshot_id, snapshot_id),
    )
    by_id = {r[0]: r for r in rows}

    def _path(column_id: int) -> tuple[str, ...]:
        _, _, name, _, _, parent = by_id[column_id]
        return (*_path(parent), name) if parent is not None else (name,)

    return [
        FlattenedColumn(
            column_id=column_id,
            path=_path(column_id),
            name=name,
            column_order=order,
            ducklake_type=col_type,
            nulls_allowed=bool(nulls_allowed),
            parent_column_id=parent,
        )
        for column_id, order, name, col_type, nulls_allowed, parent in rows
    ]


def load_partition_id(catalog: CatalogBackend, table_id: int, snapshot_id: int) -> int | None:
    row = catalog.fetchone(
        "SELECT partition_id FROM ducklake_partition_info WHERE table_id = ? "
        "AND ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)",
        (table_id, snapshot_id, snapshot_id),
    )
    return row[0] if row else None


def read_table_stats(catalog: CatalogBackend, table_id: int) -> tuple[int, int, int]:
    row = catalog.fetchone(
        "SELECT record_count, next_row_id, file_size_bytes FROM ducklake_table_stats "
        "WHERE table_id = ?",
        (table_id,),
    )
    if row is None:
        raise RuntimeError(f"No ducklake_table_stats row for table_id={table_id}")
    return row


def update_table_stats(
    catalog: CatalogBackend,
    table_id: int,
    record_count: int,
    next_row_id: int,
    file_size_bytes: int,
) -> None:
    catalog.execute(
        "UPDATE ducklake_table_stats SET record_count = ?, next_row_id = ?, file_size_bytes = ? "
        "WHERE table_id = ?",
        (record_count, next_row_id, file_size_bytes, table_id),
    )


def load_column_accumulators(
    catalog: CatalogBackend,
    table_id: int,
    leaf_types: dict[tuple[str, ...], str],
    path_to_id: dict[tuple[str, ...], int],
) -> dict[int, ColumnStatsAccumulator]:
    """Seed one `ColumnStatsAccumulator` per leaf column from whatever is already stored in
    `ducklake_table_column_stats` (decoded back to typed values), so `sync_table` can fold in new
    files' stats on top of the existing aggregate instead of starting from scratch.
    """
    existing = {
        row[0]: row
        for row in catalog.fetchall(
            "SELECT column_id, contains_null, contains_nan, min_value, max_value "
            "FROM ducklake_table_column_stats WHERE table_id = ?",
            (table_id,),
        )
    }
    accumulators: dict[int, ColumnStatsAccumulator] = {}
    for path, delta_type in leaf_types.items():
        column_id = path_to_id[path]
        agg = ColumnStatsAccumulator(delta_type=delta_type)
        row = existing.get(column_id)
        if row is not None:
            _, contains_null, contains_nan, min_v, max_v = row
            agg.contains_null = bool(contains_null)
            agg.contains_nan = bool(contains_nan)
            if min_v is not None:
                agg.min_value = decode_ducklake_stat(min_v, delta_type)
                agg.min_set = True
            if max_v is not None:
                agg.max_value = decode_ducklake_stat(max_v, delta_type)
                agg.max_set = True
        accumulators[column_id] = agg
    return accumulators


def update_table_column_stats(
    catalog: CatalogBackend, table_id: int, column_id: int, agg: ColumnStatsAccumulator
) -> None:
    min_enc, _ = (
        encode_ducklake_stat(agg.min_value, agg.delta_type) if agg.min_set else (None, False)
    )
    max_enc, _ = (
        encode_ducklake_stat(agg.max_value, agg.delta_type) if agg.max_set else (None, False)
    )
    catalog.execute(
        "UPDATE ducklake_table_column_stats SET contains_null = ?, contains_nan = ?, "
        "min_value = ?, max_value = ? WHERE table_id = ? AND column_id = ?",
        (
            agg.contains_null or agg.unknown_null_count,
            agg.contains_nan,
            min_enc,
            max_enc,
            table_id,
            column_id,
        ),
    )


# --- deletion vectors -> positional delete files -------------------------------------------------


def read_data_path(catalog: CatalogBackend) -> str:
    row = catalog.fetchone("SELECT value FROM ducklake_metadata WHERE key = 'data_path'")
    if row is None:
        raise RuntimeError(
            "ducklake_metadata has no 'data_path' row -- catalog was never bootstrapped"
        )
    return row[0]


def build_positional_delete_parquet(matched_file_path: str, positions: set[int]) -> bytes:
    """Build a DuckLake positional-delete Parquet file (columns `file_path VARCHAR`, `pos BIGINT`)
    for the given deleted row positions, and return its raw bytes.

    Schema and column names confirmed empirically: had DuckDB's own `ducklake` extension perform a
    real `DELETE` against a table it manages (with data inlining disabled by using a big enough
    delete that it fell back to a real file), then read the resulting delete file back -- its
    `file_path` column held the data file's own *absolute*, already-percent-decoded path, which is
    what `matched_file_path` must be here too.
    """
    con = duckdb.connect()
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    tmp.close()
    try:
        # COPY ... TO doesn't bind its destination as a query parameter, so build the relation
        # with bound values first and write it out separately rather than parameterizing COPY.
        relation = con.sql(
            "SELECT ? AS file_path, UNNEST(?::BIGINT[]) AS pos",
            params=[matched_file_path, sorted(positions)],
        )
        relation.write_parquet(tmp.name)
        return Path(tmp.name).read_bytes()
    finally:
        Path(tmp.name).unlink(missing_ok=True)


def insert_delete_file(
    catalog: CatalogBackend,
    delete_file_id: int,
    table_id: int,
    snapshot_id: int,
    data_file_id: int,
    path: str,
    delete_count: int,
    file_size_bytes: int,
) -> None:
    """Register a positional-delete Parquet file. `path` is always absolute (`path_is_relative =
    False`): delete files are written into the DuckLake catalog's own managed `data_path`, never
    into the source Delta table's directory (which this project otherwise only ever reads from).
    """
    catalog.execute(
        "INSERT INTO ducklake_delete_file "
        "(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, "
        "path_is_relative, format, delete_count, file_size_bytes, footer_size, encryption_key, "
        "partial_max) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?, 'parquet', ?, ?, NULL, NULL, NULL)",
        (
            delete_file_id, table_id, snapshot_id, data_file_id, path, False, delete_count,
            file_size_bytes,
        ),
    )
