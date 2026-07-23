"""Parse a Delta `metaData.schemaString` (Spark's JSON schema format) into a type tree, and map
Delta primitive types to their DuckLake `column_type` string equivalents.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PrimitiveType:
    name: str  # Delta's primitive type name, e.g. "string", "long", "decimal(10,2)"


@dataclass(frozen=True)
class ArrayType:
    element_type: DeltaType
    contains_null: bool = True


@dataclass(frozen=True)
class MapType:
    key_type: DeltaType
    value_type: DeltaType
    value_contains_null: bool = True


@dataclass(frozen=True)
class StructField:
    name: str
    type: DeltaType
    nullable: bool = True
    physical_name: str | None = None
    column_mapping_id: int | None = None
    metadata: dict = field(default_factory=dict)


@dataclass(frozen=True)
class StructType:
    fields: tuple[StructField, ...]


DeltaType = PrimitiveType | ArrayType | MapType | StructType


def parse_schema_string(schema_string: str) -> StructType:
    """Parse `metaData.schemaString` into a `StructType` tree of the table's top-level columns."""
    parsed = _parse_type(json.loads(schema_string))
    if not isinstance(parsed, StructType):
        raise ValueError(f"Delta schemaString root must be a struct, got: {parsed!r}")
    return parsed


def _parse_type(node) -> DeltaType:
    if isinstance(node, str):
        return PrimitiveType(node)
    kind = node["type"]
    if kind == "array":
        return ArrayType(
            element_type=_parse_type(node["elementType"]),
            contains_null=node.get("containsNull", True),
        )
    if kind == "map":
        return MapType(
            key_type=_parse_type(node["keyType"]),
            value_type=_parse_type(node["valueType"]),
            value_contains_null=node.get("valueContainsNull", True),
        )
    if kind == "struct":
        return StructType(fields=tuple(_parse_field(f) for f in node["fields"]))
    # Leniently accept any other dict-shaped "type" node (e.g. exotic/future types) as an opaque
    # primitive keyed by its `type` string; ducklake_primitive_type() will reject it clearly if
    # asked to map it to a DuckLake type.
    return PrimitiveType(kind)


def _parse_field(node: dict) -> StructField:
    metadata = dict(node.get("metadata") or {})
    return StructField(
        name=node["name"],
        type=_parse_type(node["type"]),
        nullable=node.get("nullable", True),
        physical_name=metadata.get("delta.columnMapping.physicalName"),
        column_mapping_id=metadata.get("delta.columnMapping.id"),
        metadata=metadata,
    )


_PRIMITIVE_MAP = {
    "string": "varchar",
    "long": "int64",
    "integer": "int32",
    "short": "int16",
    "byte": "int8",
    "float": "float32",
    "double": "float64",
    "boolean": "boolean",
    "binary": "blob",
    "date": "date",
    # Delta's `timestamp` is UTC-normalized ("instant") semantics -> DuckLake's timestamptz.
    "timestamp": "timestamptz",
    # Delta's `timestamp_ntz` has no timezone semantics -> DuckLake's plain timestamp.
    "timestamp_ntz": "timestamp",
}

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def ducklake_primitive_type(delta_type_name: str) -> str:
    """Map a Delta primitive type name (e.g. `"long"`, `"decimal(10,2)"`) to a DuckLake
    `column_type` string. Raises `ValueError` for types DuckLake/Delta don't share (e.g. `void`).
    """
    mapped = _PRIMITIVE_MAP.get(delta_type_name)
    if mapped is not None:
        return mapped
    m = _DECIMAL_RE.match(delta_type_name)
    if m:
        precision, scale = m.groups()
        return f"decimal({precision},{scale})"
    raise ValueError(f"Unsupported/unmapped Delta primitive type: {delta_type_name!r}")
