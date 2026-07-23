"""Parse Delta's per-file `add.stats` JSON and encode values into DuckLake's string-encoded stats
format (`ducklake_file_column_stats.min_value`/`max_value` etc., per DuckLake's `data_types.md`).
"""

from __future__ import annotations

import json
import math
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import duckdb

from delta2ducklake.delta.schema import PrimitiveType, StructField, StructType
from delta2ducklake.storage import LocalStorageBackend, StorageBackend


@dataclass(frozen=True)
class ParsedStats:
    num_records: int | None
    min_values: dict = field(default_factory=dict)
    max_values: dict = field(default_factory=dict)
    null_count: dict = field(default_factory=dict)


def parse_add_stats(stats_json: str | None) -> ParsedStats | None:
    """Parse `AddAction.stats` (a JSON string, or `None` if the writer collected no stats)."""
    if stats_json is None:
        return None
    d = json.loads(stats_json)
    return ParsedStats(
        num_records=d.get("numRecords"),
        min_values=d.get("minValues") or {},
        max_values=d.get("maxValues") or {},
        null_count=d.get("nullCount") or {},
    )


@dataclass(frozen=True)
class LeafColumnStats:
    path: tuple[str, ...]  # dotted path from the table root, e.g. ("a", "ac", "aca")
    delta_type: str  # Delta primitive type name, e.g. "long", "decimal(10,2)"
    min_value: object
    max_value: object
    null_count: int | None


def iter_leaf_column_stats(
    fields: tuple[StructField, ...], stats: ParsedStats | None, _prefix: tuple[str, ...] = ()
) -> Iterator[LeafColumnStats]:
    """Walk a schema's fields, yielding one `LeafColumnStats` per primitive column.

    Delta (like DuckLake) recurses into `struct` sub-fields for stats but does not collect
    min/max/null-count for `list`/`map` columns or their elements, so those are skipped entirely
    (they'll still get a `ducklake_column` row elsewhere, just no stats rows).
    """
    min_values = (stats.min_values if stats else {}) or {}
    max_values = (stats.max_values if stats else {}) or {}
    null_counts = (stats.null_count if stats else {}) or {}
    for f in fields:
        path = (*_prefix, f.name)
        if isinstance(f.type, StructType):
            child_stats = ParsedStats(
                num_records=stats.num_records if stats else None,
                min_values=min_values.get(f.name) or {},
                max_values=max_values.get(f.name) or {},
                null_count=null_counts.get(f.name) or {},
            )
            yield from iter_leaf_column_stats(f.type.fields, child_stats, path)
        elif isinstance(f.type, PrimitiveType):
            yield LeafColumnStats(
                path=path,
                delta_type=f.type.name,
                min_value=min_values.get(f.name),
                max_value=max_values.get(f.name),
                null_count=null_counts.get(f.name),
            )
        # ArrayType / MapType: Delta collects no stats for these, nothing to yield.


def _reformat_timestamp(value: str, *, with_offset: bool) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if with_offset:
        return dt.isoformat(sep=" ", timespec="microseconds")
    return dt.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")


def encode_ducklake_stat(value: object, delta_type: str) -> tuple[str | None, bool]:
    """Encode a raw JSON-decoded Delta stats value into DuckLake's string encoding for `delta_type`.

    Returns `(encoded_value, is_nan)`. `is_nan` is only ever `True` for float columns whose stats
    value was literally NaN — DuckLake excludes NaN from min/max and tracks it via a separate
    `contains_nan` flag instead, so the caller should treat that leg as "no bound from this file"
    while still recording that a NaN was seen.
    """
    if value is None:
        return None, False

    if delta_type == "boolean":
        return ("1" if value else "0"), False

    if delta_type in ("byte", "short", "integer", "long"):
        return str(int(value)), False

    if delta_type in ("float", "double"):
        f = float(value)
        if math.isnan(f):
            return None, True
        if math.isinf(f):
            return ("inf" if f > 0 else "-inf"), False
        return repr(f), False

    if delta_type.startswith("decimal"):
        # Delta's JSON stats represent decimals as JSON numbers; str() is a lower/upper *bound*,
        # not required to be exact (per DuckLake's own stats semantics).
        return str(value), False

    if delta_type == "date":
        # Delta already emits ISO 8601 date strings ("2024-01-15") -- matches DuckLake's encoding.
        return str(value), False

    if delta_type == "timestamp":
        return _reformat_timestamp(str(value), with_offset=True), False

    if delta_type == "timestamp_ntz":
        return _reformat_timestamp(str(value), with_offset=False), False

    if delta_type == "string":
        return str(value), False

    # `binary` (blob): Delta does not collect min/max stats for binary columns at all, so this
    # should never be reached in practice; treat defensively as "no stat" rather than erroring.
    if delta_type == "binary":
        return None, False

    raise ValueError(f"Don't know how to encode a stats value for Delta type {delta_type!r}")


def decode_ducklake_stat(value: str | None, delta_type: str) -> object:
    """Inverse of `encode_ducklake_stat`, for merging a newly-computed bound with a bound already
    stored in `ducklake_table_column_stats` (needed when `sync_table` adds files to an existing
    table). Only meaningful for values produced by `encode_ducklake_stat` itself -- numeric types
    are parsed back to `int`/`float` since comparing their encoded strings directly is wrong
    (`"10" < "9"`); date/timestamp/string values are left as-is and compared as strings, which is
    safe *only* because `encode_ducklake_stat` always normalizes them to one consistent format.
    """
    if value is None:
        return None
    if delta_type == "boolean":
        return value == "1"
    if delta_type in ("byte", "short", "integer", "long"):
        return int(value)
    if delta_type in ("float", "double") or delta_type.startswith("decimal"):
        return float(value)
    return value


def read_parquet_record_count(storage: StorageBackend, path: str) -> int:
    """Fallback row count for a data file whose `add` action carries no `stats` at all.

    Reads only the Parquet footer's row-group metadata (via DuckDB), never the actual row data.
    """
    con = duckdb.connect()
    if isinstance(storage, LocalStorageBackend):
        real_path = path
        tmp = None
    else:
        data = storage.read_bytes(path)
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.write(data)
        tmp.close()
        real_path = tmp.name
    try:
        quoted = real_path.replace("'", "''")
        (count,) = con.sql(
            f"SELECT sum(num_rows) FROM parquet_file_metadata('{quoted}')"
        ).fetchone()
        return int(count)
    finally:
        if tmp is not None:
            Path(tmp.name).unlink(missing_ok=True)
