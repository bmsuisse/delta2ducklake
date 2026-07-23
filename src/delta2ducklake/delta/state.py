"""Replay Delta log actions into the active-file/schema/protocol state at a given version."""

from __future__ import annotations

from dataclasses import dataclass

from delta2ducklake.delta.actions import Action, AddAction, MetaData, Protocol, RemoveAction
from delta2ducklake.delta.log import iter_versions
from delta2ducklake.storage import StorageBackend


class UnsupportedTableFeatureError(NotImplementedError):
    """Raised when a table uses a feature this phase doesn't support yet (column mapping,
    deletion vectors) so callers get a clear error instead of silently-wrong output."""


@dataclass
class DeltaTableState:
    version: int
    metadata: MetaData
    protocol: Protocol
    active_files: dict[str, AddAction]  # path -> the AddAction currently registering that path


def _apply_version(
    active_files: dict[str, AddAction], actions: list[Action]
) -> tuple[MetaData | None, Protocol | None]:
    """Apply one version's actions to `active_files` in place; return any new metaData/protocol.

    Removes are applied before adds so that "remove path X, then add path X again" (e.g. a
    deletion-vector update, or a file path reused across transactions) nets out to the new add
    winning, matching Delta's log-replay semantics.
    """
    metadata: MetaData | None = None
    protocol: Protocol | None = None
    removes = [a for a in actions if isinstance(a, RemoveAction)]
    adds = [a for a in actions if isinstance(a, AddAction)]
    for a in actions:
        if isinstance(a, MetaData):
            metadata = a
        elif isinstance(a, Protocol):
            protocol = a
    for r in removes:
        active_files.pop(r.path, None)
    for a in adds:
        active_files[a.path] = a
    return metadata, protocol


def load_table_state(
    storage: StorageBackend,
    table_root: str,
    end_version: int | None = None,
    *,
    allow_column_mapping: bool = False,
    allow_deletion_vectors: bool = False,
) -> DeltaTableState:
    """Replay the Delta log up to `end_version` (or the latest commit) into a `DeltaTableState`.

    Raises `UnsupportedTableFeatureError` if the table uses column mapping or deletion vectors and
    the corresponding `allow_*` flag isn't set — phase 1 callers leave both `False` so unsupported
    tables fail loudly instead of producing a silently-wrong DuckLake catalog.
    """
    active_files: dict[str, AddAction] = {}
    metadata: MetaData | None = None
    protocol: Protocol | None = None
    last_version = 0

    for version, actions in iter_versions(storage, table_root, end_version=end_version):
        new_metadata, new_protocol = _apply_version(active_files, actions)
        metadata = new_metadata or metadata
        protocol = new_protocol or protocol
        last_version = version

    if metadata is None:
        raise ValueError(f"No metaData action found for Delta table at {table_root!r}")
    if protocol is None:
        raise ValueError(f"No protocol action found for Delta table at {table_root!r}")

    if not allow_column_mapping and metadata.column_mapping_mode != "none":
        raise UnsupportedTableFeatureError(
            f"Column mapping (delta.columnMapping.mode={metadata.column_mapping_mode!r}) is not "
            "yet supported by delta2ducklake's copy_table()/sync_table() (planned: phase 2)."
        )
    if not allow_deletion_vectors and any(
        a.deletion_vector is not None for a in active_files.values()
    ):
        raise UnsupportedTableFeatureError(
            "Deletion vectors are not yet supported by delta2ducklake's copy_table()/sync_table() "
            "(planned: phase 3)."
        )

    return DeltaTableState(
        version=last_version, metadata=metadata, protocol=protocol, active_files=active_files
    )
