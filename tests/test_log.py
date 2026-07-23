from pathlib import Path

from delta2ducklake.delta.actions import AddAction, MetaData, Protocol, RemoveAction
from delta2ducklake.delta.log import (
    find_latest_checkpoint_from_listing,
    iter_versions,
    list_commit_versions,
    log_dir,
    read_last_checkpoint,
)
from delta2ducklake.storage import LocalStorageBackend

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"
STORAGE = LocalStorageBackend()


def _table(name: str) -> str:
    return str(FIXTURES / name)


def _replay(table_root: str, end_version=None):
    active: dict[str, AddAction] = {}
    metadata = None
    protocol = None
    last_version = 0
    for version, actions in iter_versions(STORAGE, table_root, end_version=end_version):
        for a in actions:
            if isinstance(a, RemoveAction):
                active.pop(a.path, None)
        for a in actions:
            if isinstance(a, AddAction):
                active[a.path] = a
            elif isinstance(a, MetaData):
                metadata = a
            elif isinstance(a, Protocol):
                protocol = a
        last_version = version
    return last_version, active, metadata, protocol


def test_list_commit_versions():
    versions = list_commit_versions(STORAGE, log_dir(_table("checkpoint")))
    assert versions == list(range(15))


def test_read_last_checkpoint_classic():
    lc = read_last_checkpoint(STORAGE, log_dir(_table("checkpoint")))
    assert lc["version"] == 10


def test_find_latest_checkpoint_from_listing_classic_single():
    result = find_latest_checkpoint_from_listing(STORAGE, log_dir(_table("checkpoint")))
    assert result == (10, 1)


def test_find_latest_checkpoint_from_listing_multipart():
    result = find_latest_checkpoint_from_listing(
        STORAGE, log_dir(_table("multi-part-checkpoint"))
    )
    assert result == (1, 2)


def test_classic_checkpoint_squash_matches_full_replay():
    # "checkpoint" fixture: 15 versions (0..14) of "remove predecessor, add successor" churn,
    # checkpointed at v10. Replaying to the end should land on the single last-added file.
    last_version, active, _, _ = _replay(_table("checkpoint"))
    assert last_version == 14
    assert set(active) == {"15"}

    # And replaying only up to the checkpoint's own version must match its squashed content.
    at_ckpt_version, at_ckpt_active, _, _ = _replay(_table("checkpoint"), end_version=10)
    assert at_ckpt_version == 10
    assert set(at_ckpt_active) == {"11"}


def test_multi_part_checkpoint_reads_all_parts():
    last_version, active, metadata, protocol = _replay(_table("multi-part-checkpoint"))
    assert metadata is not None
    assert protocol is not None
    assert len(active) > 0


def test_v2_checkpoint_parquet_reads_sidecars():
    last_version, active, metadata, protocol = _replay(_table("v2-checkpoint-parquet"))
    assert metadata is not None
    assert protocol is not None
    assert protocol.min_reader_version == 3
    assert len(active) == 4


def test_v2_checkpoint_json_reads_sidecars():
    last_version, active, metadata, protocol = _replay(_table("v2-checkpoint-json"))
    assert metadata is not None
    assert protocol is not None
    assert len(active) == 4


def test_v2_checkpoint_parquet_and_json_agree_on_active_file_count():
    # Independently-generated golden tables (different UUIDs per file), so compare shape not paths.
    _, active_parquet, _, protocol_parquet = _replay(_table("v2-checkpoint-parquet"))
    _, active_json, _, protocol_json = _replay(_table("v2-checkpoint-json"))
    assert len(active_parquet) == len(active_json) == 4
    assert protocol_parquet == protocol_json


def test_time_travel_end_version_stops_replay_early():
    table = _table("delete-re-add-same-file-different-transactions")
    v0, active0, _, _ = _replay(table, end_version=0)
    assert v0 == 0
    assert set(active0) == {"foo"}

    v1, active1, _, _ = _replay(table, end_version=1)
    assert v1 == 1
    assert active1 == {}

    v3, active3, _, _ = _replay(table, end_version=3)
    assert v3 == 3
    assert set(active3) == {"foo", "bar"}


def test_snapshot_data2_add_remove_across_versions():
    last_version, active, _, _ = _replay(_table("snapshot-data2"))
    assert last_version == 2
    assert len(active) == 2
    assert all("842017c2" in p or "e62ca5a1" in p for p in active)


def test_snapshot_data3_accumulates_on_top_of_data2_state():
    last_version, active, _, _ = _replay(_table("snapshot-data3"))
    assert last_version == 3
    assert len(active) == 4
