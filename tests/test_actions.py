import json
from pathlib import Path

from delta2ducklake.delta.actions import (
    AddAction,
    MetaData,
    RemoveAction,
    parse_action,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _read_commit_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_parse_dv_commit_add_remove_and_deletion_vector():
    lines = _read_commit_lines(
        FIXTURES / "delta-rs" / "table-with-dv-small" / "_delta_log" / "00000000000000000001.json"
    )
    actions = [parse_action(line) for line in lines]

    removes = [a for a in actions if isinstance(a, RemoveAction)]
    adds = [a for a in actions if isinstance(a, AddAction)]
    assert len(removes) == 1
    assert len(adds) == 1

    remove = removes[0]
    assert remove.path == "part-00000-fae5310a-a37d-4e51-827b-c3d5516560ca-c000.snappy.parquet"
    assert remove.size == 635

    add = adds[0]
    assert add.path == remove.path
    assert add.size == 635
    assert add.stats is not None
    assert json.loads(add.stats)["numRecords"] == 10

    dv = add.deletion_vector
    assert dv is not None
    assert dv.storage_type == "u"
    assert dv.path_or_inline_dv == "vBn[lx{q8@P<9BNH/isA"
    assert dv.offset == 1
    assert dv.size_in_bytes == 36
    assert dv.cardinality == 2
    assert dv.unique_id == "uvBn[lx{q8@P<9BNH/isA@1"


def test_parse_commit_with_no_actions_of_interest_returns_none():
    # commitInfo-only-ish action kinds we explicitly ignore
    assert parse_action({"txn": {"appId": "x", "version": 1}}) is None
    assert parse_action({"domainMetadata": {"domain": "x", "configuration": "{}"}}) is None
    assert parse_action({"cdc": {"path": "x"}}) is None


def test_parse_metadata_action_with_column_mapping():
    lines = _read_commit_lines(
        FIXTURES / "delta-rs" / "table_with_column_mapping" / "_delta_log"
        / "00000000000000000000.json"
    )
    metas = [parse_action(line) for line in lines]
    meta = next(a for a in metas if isinstance(a, MetaData))
    assert meta.column_mapping_mode == "name"
    assert meta.partition_columns == ["Company Very Short"]
    assert meta.configuration["delta.columnMapping.maxColumnId"] == "2"


def test_add_action_partition_values_and_defaults():
    add = AddAction.from_dict(
        {
            "path": "a.parquet",
            "size": 10,
            "modificationTime": 123,
            "dataChange": True,
        }
    )
    assert add.partition_values == {}
    assert add.stats is None
    assert add.deletion_vector is None
