"""Dataclasses for Delta transaction log actions (add/remove/metaData/protocol/...).

One JSON object in a `_delta_log/*.json` commit file, or one row of a checkpoint Parquet file,
has exactly one of these keys set (`add`, `remove`, `metaData`, `protocol`, `commitInfo`, `txn`,
`domainMetadata`, `cdc`, `sidecar`). `parse_action` dispatches on whichever key is present into the
matching dataclass; it accepts anything Mapping-like, so the same code parses both a `json.loads`'d
commit line and a DuckDB struct-as-dict row read out of a checkpoint Parquet file.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class DeletionVectorDescriptor:
    storage_type: str
    path_or_inline_dv: str
    size_in_bytes: int
    cardinality: int
    offset: int | None = None

    @property
    def unique_id(self) -> str:
        if self.offset is None:
            return f"{self.storage_type}{self.path_or_inline_dv}"
        return f"{self.storage_type}{self.path_or_inline_dv}@{self.offset}"

    @staticmethod
    def from_dict(d: Mapping) -> DeletionVectorDescriptor:
        return DeletionVectorDescriptor(
            storage_type=d["storageType"],
            path_or_inline_dv=d["pathOrInlineDv"],
            size_in_bytes=d["sizeInBytes"],
            cardinality=d["cardinality"],
            offset=d.get("offset"),
        )


@dataclass(frozen=True, slots=True)
class AddAction:
    path: str
    partition_values: dict[str, str | None]
    size: int
    modification_time: int
    data_change: bool
    stats: str | None = None
    tags: dict[str, str] | None = None
    deletion_vector: DeletionVectorDescriptor | None = None

    @staticmethod
    def from_dict(d: Mapping) -> AddAction:
        dv = d.get("deletionVector")
        return AddAction(
            path=d["path"],
            partition_values=dict(d.get("partitionValues") or {}),
            size=d["size"],
            modification_time=d["modificationTime"],
            data_change=d.get("dataChange", True),
            stats=d.get("stats"),
            tags=d.get("tags"),
            deletion_vector=DeletionVectorDescriptor.from_dict(dv) if dv else None,
        )


@dataclass(frozen=True, slots=True)
class RemoveAction:
    path: str
    data_change: bool
    deletion_timestamp: int | None = None
    partition_values: dict[str, str | None] | None = None
    size: int | None = None
    deletion_vector: DeletionVectorDescriptor | None = None

    @staticmethod
    def from_dict(d: Mapping) -> RemoveAction:
        dv = d.get("deletionVector")
        return RemoveAction(
            path=d["path"],
            data_change=d.get("dataChange", True),
            deletion_timestamp=d.get("deletionTimestamp"),
            partition_values=(
                dict(d["partitionValues"]) if d.get("partitionValues") is not None else None
            ),
            size=d.get("size"),
            deletion_vector=DeletionVectorDescriptor.from_dict(dv) if dv else None,
        )


@dataclass(frozen=True, slots=True)
class MetaData:
    id: str
    schema_string: str
    partition_columns: list[str] = field(default_factory=list)
    configuration: dict[str, str] = field(default_factory=dict)
    name: str | None = None
    description: str | None = None
    created_time: int | None = None

    @staticmethod
    def from_dict(d: Mapping) -> MetaData:
        return MetaData(
            id=d["id"],
            schema_string=d["schemaString"],
            partition_columns=list(d.get("partitionColumns") or []),
            configuration=dict(d.get("configuration") or {}),
            name=d.get("name"),
            description=d.get("description"),
            created_time=d.get("createdTime"),
        )

    @property
    def column_mapping_mode(self) -> str:
        return self.configuration.get("delta.columnMapping.mode", "none")


@dataclass(frozen=True, slots=True)
class Protocol:
    min_reader_version: int
    min_writer_version: int
    reader_features: tuple[str, ...] = ()
    writer_features: tuple[str, ...] = ()

    @staticmethod
    def from_dict(d: Mapping) -> Protocol:
        return Protocol(
            min_reader_version=d["minReaderVersion"],
            min_writer_version=d["minWriterVersion"],
            reader_features=tuple(d.get("readerFeatures") or ()),
            writer_features=tuple(d.get("writerFeatures") or ()),
        )


@dataclass(frozen=True, slots=True)
class SidecarAction:
    path: str
    size_in_bytes: int
    modification_time: int

    @staticmethod
    def from_dict(d: Mapping) -> SidecarAction:
        return SidecarAction(
            path=d["path"],
            size_in_bytes=d["sizeInBytes"],
            modification_time=d["modificationTime"],
        )


@dataclass(frozen=True, slots=True)
class CommitInfo:
    raw: Mapping

    @staticmethod
    def from_dict(d: Mapping) -> CommitInfo:
        return CommitInfo(raw=d)


Action = AddAction | RemoveAction | MetaData | Protocol | SidecarAction | CommitInfo

_DISPATCH = {
    "add": AddAction.from_dict,
    "remove": RemoveAction.from_dict,
    "metaData": MetaData.from_dict,
    "protocol": Protocol.from_dict,
    "sidecar": SidecarAction.from_dict,
    "commitInfo": CommitInfo.from_dict,
}

# Action keys that exist in the protocol but are irrelevant to conversion (application-level
# transaction bookkeeping, generic domain metadata, and Change Data Feed rows).
_IGNORED_KEYS = frozenset({"txn", "domainMetadata", "cdc"})


def parse_action(line: Mapping) -> Action | None:
    """Parse one action object (from a JSON commit line or a checkpoint Parquet row).

    Returns `None` for action kinds we don't need (`txn`, `domainMetadata`, `cdc`) or for an
    all-null checkpoint row (Parquet checkpoints are one struct-of-structs per action kind; a row
    contributing an `add` action has every other field `NULL`).
    """
    for key, parser in _DISPATCH.items():
        value = line.get(key)
        if value is not None:
            return parser(value)
    return None
