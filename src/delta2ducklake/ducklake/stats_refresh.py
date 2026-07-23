"""Standalone utility: (re)compute stats for some or all columns of any DuckLake table.

Doesn't touch Delta at all -- scans the table's own already-registered Parquet files directly via
DuckDB. Useful both as a public tool (Delta's `dataSkippingNumIndexedCols` cuts off per-file stats
collection after a configured number of columns, so trailing columns often have no stats at all in
a table copied from Delta) and, more generally, for any DuckLake table that needs its stats
(re)computed after the fact.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import duckdb

from delta2ducklake.ducklake import writer as w
from delta2ducklake.ducklake.catalog import CatalogConfig

_FLOAT_TYPES = ("float32", "float64")
_CONTAINER_TYPES = ("struct", "list", "map")


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _encode_value(value: object, ducklake_type: str) -> tuple[str | None, bool]:
    """Encode a native Python value (as returned directly by DuckDB, not a Delta JSON stats value)
    into DuckLake's string-encoded stats format. Sibling to `delta.stats.encode_ducklake_stat`,
    which instead encodes Delta's raw JSON-decoded stats values -- kept separate because the input
    domain is different (real `date`/`datetime`/`Decimal`/`bytes`/`uuid.UUID` objects here, vs.
    JSON-native int/float/str there) even though the output format is identical.
    """
    if value is None:
        return None, False
    if ducklake_type == "boolean":
        return ("1" if value else "0"), False
    if ducklake_type in ("int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"):
        return str(int(value)), False
    if ducklake_type in _FLOAT_TYPES:
        f = float(value)
        if math.isnan(f):
            return None, True
        if math.isinf(f):
            return ("inf" if f > 0 else "-inf"), False
        return repr(f), False
    if ducklake_type.startswith("decimal"):
        return str(value), False
    if ducklake_type == "date":
        return value.isoformat(), False
    if ducklake_type in ("timestamp", "timestamptz"):
        return value.isoformat(sep=" "), False
    if ducklake_type in ("varchar", "json"):
        return str(value), False
    if ducklake_type == "blob":
        return value.hex(), False
    if ducklake_type == "uuid":
        return str(value), False
    raise ValueError(f"Don't know how to encode a value for DuckLake type {ducklake_type!r}")


@dataclass
class _ColumnAgg:
    ducklake_type: str
    contains_null: bool = False
    contains_nan: bool = False
    min_value: object = None
    max_value: object = None
    min_set: bool = False
    max_set: bool = False


def refresh_stats(
    catalog_config: CatalogConfig,
    table_name: str,
    *,
    schema_name: str = "main",
    columns: list[str] | None = None,
) -> None:
    """(Re)compute `ducklake_file_column_stats`/`ducklake_table_column_stats` for `table_name` by
    scanning its currently-active data files directly. `columns` restricts this to a subset of
    top-level column names (default: every non-container top-level column). Existing stats rows
    for the affected (file, column) / (table, column) pairs are replaced, not merged.
    """
    catalog = catalog_config.connect()
    try:
        snapshot = w.read_latest_snapshot(catalog)
        schema_id = w.find_schema_id(catalog, schema_name, snapshot.snapshot_id)
        if schema_id is None:
            raise ValueError(f"DuckLake schema {schema_name!r} does not exist")
        table_id = w.find_table_id(catalog, schema_id, table_name, snapshot.snapshot_id)
        if table_id is None:
            raise ValueError(f"Table {schema_name}.{table_name} does not exist")

        table_path, table_path_is_relative = catalog.fetchone(
            "SELECT path, path_is_relative FROM ducklake_table WHERE table_id = ?", (table_id,)
        )
        if table_path_is_relative:
            raise NotImplementedError(
                "refresh_stats() only supports tables with an absolute path today "
                "(every table copy_table()/sync_table() create has one)"
            )

        leaf_columns = [
            c
            for c in w.load_columns(catalog, table_id, snapshot.snapshot_id)
            if len(c.path) == 1 and c.ducklake_type not in _CONTAINER_TYPES
        ]
        if columns is not None:
            wanted = set(columns)
            leaf_columns = [c for c in leaf_columns if c.name in wanted]
            missing = wanted - {c.name for c in leaf_columns}
            if missing:
                raise ValueError(
                    f"No such top-level column(s) on {table_name!r}: {sorted(missing)}"
                )
        if not leaf_columns:
            raise ValueError(f"No matching leaf columns to refresh stats for on {table_name!r}")

        data_files = catalog.fetchall(
            "SELECT data_file_id, path, path_is_relative FROM ducklake_data_file "
            "WHERE table_id = ? AND end_snapshot IS NULL",
            (table_id,),
        )

        con = duckdb.connect()
        accumulators = {c.column_id: _ColumnAgg(c.ducklake_type) for c in leaf_columns}

        for data_file_id, file_path, file_path_is_relative in data_files:
            full_path = (
                f"{table_path.rstrip('/')}/{file_path}" if file_path_is_relative else file_path
            )
            select_parts = ["count(*) AS __n"]
            for c in leaf_columns:
                ident = _quote_ident(c.name)
                select_parts.append(f'count({ident}) AS "{c.column_id}__nn"')
                select_parts.append(f'min({ident}) AS "{c.column_id}__min"')
                select_parts.append(f'max({ident}) AS "{c.column_id}__max"')
                if c.ducklake_type in _FLOAT_TYPES:
                    select_parts.append(
                        f'bool_or({ident} IS NOT NULL AND isnan({ident})) AS "{c.column_id}__nan"'
                    )
                else:
                    select_parts.append(f'false AS "{c.column_id}__nan"')
            quoted_path = full_path.replace("'", "''")
            rel = con.sql(f"SELECT {', '.join(select_parts)} FROM read_parquet('{quoted_path}')")
            values = dict(zip(rel.columns, rel.fetchone(), strict=True))
            n = values["__n"]

            for c in leaf_columns:
                null_count = n - values[f"{c.column_id}__nn"]
                min_v = values[f"{c.column_id}__min"]
                max_v = values[f"{c.column_id}__max"]
                any_nan = bool(values[f"{c.column_id}__nan"])

                min_enc, min_nan = _encode_value(min_v, c.ducklake_type)
                max_enc, max_nan = _encode_value(max_v, c.ducklake_type)
                catalog.execute(
                    "DELETE FROM ducklake_file_column_stats "
                    "WHERE data_file_id = ? AND column_id = ?",
                    (data_file_id, c.column_id),
                )
                catalog.execute(
                    "INSERT INTO ducklake_file_column_stats "
                    "(data_file_id, table_id, column_id, column_size_bytes, value_count, "
                    "null_count, min_value, max_value, contains_nan, extra_stats) "
                    "VALUES (?, ?, ?, NULL, ?, ?, ?, ?, ?, NULL)",
                    (
                        data_file_id, table_id, c.column_id, n, null_count, min_enc, max_enc,
                        any_nan or min_nan or max_nan,
                    ),
                )

                agg = accumulators[c.column_id]
                agg.contains_null = agg.contains_null or null_count > 0
                agg.contains_nan = agg.contains_nan or any_nan
                if min_v is not None and (not agg.min_set or min_v < agg.min_value):
                    agg.min_value = min_v
                    agg.min_set = True
                if max_v is not None and (not agg.max_set or max_v > agg.max_value):
                    agg.max_value = max_v
                    agg.max_set = True

        for c in leaf_columns:
            agg = accumulators[c.column_id]
            min_enc, _ = (
                _encode_value(agg.min_value, c.ducklake_type) if agg.min_set else (None, False)
            )
            max_enc, _ = (
                _encode_value(agg.max_value, c.ducklake_type) if agg.max_set else (None, False)
            )
            catalog.execute(
                "DELETE FROM ducklake_table_column_stats WHERE table_id = ? AND column_id = ?",
                (table_id, c.column_id),
            )
            catalog.execute(
                "INSERT INTO ducklake_table_column_stats "
                "(table_id, column_id, contains_null, contains_nan, min_value, max_value, "
                "extra_stats) VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (table_id, c.column_id, agg.contains_null, agg.contains_nan, min_enc, max_enc),
            )

        catalog.commit()
    except BaseException:
        catalog.rollback()
        raise
    finally:
        catalog.close()
