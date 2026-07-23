"""Small internal data structures used while writing to a DuckLake catalog.

Not a full ORM over the 28 `ducklake_*` tables — just the pieces `writer.py` needs to pass around
between the ID-allocation, column-flattening, and stats-aggregation steps of building one snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Snapshot:
    snapshot_id: int
    schema_version: int
    next_catalog_id: int
    next_file_id: int


@dataclass(frozen=True)
class FlattenedColumn:
    """One row destined for `ducklake_column`, produced by flattening a Delta schema tree."""

    column_id: int
    path: tuple[str, ...]  # e.g. ("a", "ac", "aca") for a nested struct field -- logical names
    name: str  # logical column name (what ducklake_column.column_name / SQL queries use)
    column_order: int
    ducklake_type: str  # e.g. "int64", "struct", "list", "map" -- container types have no stats
    nulls_allowed: bool
    parent_column_id: int | None
    physical_name: str | None = None  # Parquet field name, only when it differs from `name`


class IdAllocator:
    """Hands out sequential catalog ids (schemas/tables/columns/partitions/mappings) and file ids
    (data/delete files) from a DuckLake snapshot's `next_catalog_id`/`next_file_id` counters.
    """

    def __init__(self, next_catalog_id: int, next_file_id: int):
        self._next_catalog_id = next_catalog_id
        self._next_file_id = next_file_id

    def alloc_catalog_id(self) -> int:
        value = self._next_catalog_id
        self._next_catalog_id += 1
        return value

    def alloc_file_id(self) -> int:
        value = self._next_file_id
        self._next_file_id += 1
        return value

    @property
    def next_catalog_id(self) -> int:
        return self._next_catalog_id

    @property
    def next_file_id(self) -> int:
        return self._next_file_id
