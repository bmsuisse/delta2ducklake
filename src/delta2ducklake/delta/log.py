"""Read a Delta table's `_delta_log`: JSON commits and checkpoints (classic, multi-part, and V2).

No dependency on the `deltalake` package — commit files are plain JSON (one action object per
line) and checkpoints are Parquet, read via DuckDB. The public entry point is `iter_versions`,
which yields `(version, actions)` in strictly increasing version order: the first entry bundles
the "squashed" state as of the latest usable checkpoint (or is version 0 if there is none), and
every following entry is exactly one commit's actions.
"""

from __future__ import annotations

import json
import re
import tempfile
from collections.abc import Iterator
from pathlib import Path

import duckdb

from delta2ducklake.delta.actions import Action, parse_action
from delta2ducklake.storage import LocalStorageBackend, StorageBackend, StorageError

_COMMIT_RE = re.compile(r"^(\d{20})\.json$")
_CHECKPOINT_SINGLE_RE = re.compile(r"^(\d{20})\.checkpoint\.parquet$")
_CHECKPOINT_MULTIPART_RE = re.compile(r"^(\d{20})\.checkpoint\.(\d{10})\.(\d{10})\.parquet$")

LOG_DIR_NAME = "_delta_log"
SIDECARS_DIR_NAME = "_sidecars"


def log_dir(table_root: str) -> str:
    return f"{table_root.rstrip('/')}/{LOG_DIR_NAME}"


def _read_json_lines(storage: StorageBackend, path: str) -> list[dict]:
    raw = storage.read_bytes(path).decode("utf-8")
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _read_parquet_rows(storage: StorageBackend, paths: list[str]) -> list[dict]:
    """Read one or more Parquet files (all sharing the same schema) as a list of dict rows.

    Local paths are handed to DuckDB directly. For any other backend, bytes are fetched through
    `storage` and staged into temp files first — checkpoints are small metadata files, so this
    doesn't conflict with never copying the actual data Parquet files.
    """
    con = duckdb.connect()
    if isinstance(storage, LocalStorageBackend):
        real_paths = paths
        cleanup: list[Path] = []
    else:
        real_paths = []
        cleanup = []
        for p in paths:
            data = storage.read_bytes(p)
            tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
            tmp.write(data)
            tmp.close()
            real_paths.append(tmp.name)
            cleanup.append(Path(tmp.name))
    try:
        quoted = ", ".join("'" + p.replace("'", "''") + "'" for p in real_paths)
        rel = con.sql(f"SELECT * FROM read_parquet([{quoted}])")
        cols = rel.columns
        return [dict(zip(cols, row, strict=True)) for row in rel.fetchall()]
    finally:
        for p in cleanup:
            p.unlink(missing_ok=True)


def read_last_checkpoint(storage: StorageBackend, log_directory: str) -> dict | None:
    path = f"{log_directory}/_last_checkpoint"
    if not storage.exists(path):
        return None
    try:
        return json.loads(storage.read_bytes(path).decode("utf-8"))
    except (StorageError, json.JSONDecodeError):
        return None


def list_commit_versions(storage: StorageBackend, log_directory: str) -> list[int]:
    versions = []
    for name in storage.list_dir(log_directory):
        m = _COMMIT_RE.match(name)
        if m:
            versions.append(int(m.group(1)))
    return sorted(versions)


def find_latest_checkpoint_from_listing(
    storage: StorageBackend, log_directory: str
) -> tuple[int, int] | None:
    """Best-effort fallback (no V2 checkpoint support) when `_last_checkpoint` is missing/corrupt.

    Returns `(version, parts)` for the highest-versioned classic (single or multi-part) checkpoint
    found by listing the log directory, or `None` if none exists.
    """
    candidates: dict[int, int] = {}
    for name in storage.list_dir(log_directory):
        m = _CHECKPOINT_SINGLE_RE.match(name)
        if m:
            v = int(m.group(1))
            candidates[v] = candidates.get(v, 1)
            continue
        m = _CHECKPOINT_MULTIPART_RE.match(name)
        if m:
            v, parts = int(m.group(1)), int(m.group(3))
            candidates[v] = parts
    if not candidates:
        return None
    v = max(candidates)
    return v, candidates[v]


def _actions_from_rows(rows: list[dict]) -> list[Action]:
    actions = []
    for row in rows:
        action = parse_action(row)
        if action is not None:
            actions.append(action)
    return actions


def read_checkpoint_actions(
    storage: StorageBackend, log_directory: str, last_checkpoint: dict
) -> tuple[int, list[Action]]:
    """Return `(version, actions)` for the checkpoint described by a `_last_checkpoint` dict
    (as returned by `read_last_checkpoint`), handling classic, multi-part, and V2 checkpoints.
    """
    version = last_checkpoint["version"]
    v2 = last_checkpoint.get("v2Checkpoint")
    if v2 is not None:
        # _last_checkpoint already inlines the non-file actions and lists the sidecar files that
        # hold the add/remove rows -- no need to touch the top-level v2 checkpoint file itself.
        actions = _actions_from_rows(v2.get("nonFileActions") or [])
        sidecar_paths = [
            f"{log_directory}/{SIDECARS_DIR_NAME}/{sc['path']}"
            for sc in (v2.get("sidecarFiles") or [])
        ]
        if sidecar_paths:
            actions.extend(_actions_from_rows(_read_parquet_rows(storage, sidecar_paths)))
        return version, actions

    parts = last_checkpoint.get("parts", 1)
    if parts <= 1:
        paths = [f"{log_directory}/{version:020d}.checkpoint.parquet"]
    else:
        paths = [
            f"{log_directory}/{version:020d}.checkpoint.{i:010d}.{parts:010d}.parquet"
            for i in range(1, parts + 1)
        ]
    return version, _actions_from_rows(_read_parquet_rows(storage, paths))


def read_commit_actions(storage: StorageBackend, log_directory: str, version: int) -> list[Action]:
    path = f"{log_directory}/{version:020d}.json"
    return _actions_from_rows(_read_json_lines(storage, path))


def iter_versions(
    storage: StorageBackend, table_root: str, end_version: int | None = None
) -> Iterator[tuple[int, list[Action]]]:
    """Yield `(version, actions)` in strictly increasing version order.

    If a usable checkpoint exists (one at or before `end_version`), the first yielded entry
    bundles its squashed state under its own version number. Otherwise replay starts cold from
    version 0. Every following entry is exactly one commit's actions, up to and including
    `end_version` (or the latest available commit if `None`).
    """
    directory = log_dir(table_root)
    checkpoint_version = -1  # sentinel: no checkpoint used (distinct from "checkpoint at v0")

    last_checkpoint = read_last_checkpoint(storage, directory)
    if last_checkpoint is None:
        fallback = find_latest_checkpoint_from_listing(storage, directory)
        if fallback is not None:
            last_checkpoint = {"version": fallback[0], "parts": fallback[1]}

    if last_checkpoint is not None and (
        end_version is None or last_checkpoint["version"] <= end_version
    ):
        checkpoint_version, checkpoint_actions = read_checkpoint_actions(
            storage, directory, last_checkpoint
        )
        yield checkpoint_version, checkpoint_actions

    commit_versions = [
        v for v in list_commit_versions(storage, directory) if v > checkpoint_version
    ]
    if end_version is not None:
        commit_versions = [v for v in commit_versions if v <= end_version]

    for v in commit_versions:
        yield v, read_commit_actions(storage, directory, v)
