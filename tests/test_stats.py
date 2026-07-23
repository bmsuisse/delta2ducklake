import json
from pathlib import Path

import pytest

from delta2ducklake.delta.schema import parse_schema_string
from delta2ducklake.delta.stats import (
    encode_ducklake_stat,
    iter_leaf_column_stats,
    parse_add_stats,
    read_parquet_record_count,
)
from delta2ducklake.storage import LocalStorageBackend

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"
STORAGE = LocalStorageBackend()


def _first_add_and_schema(table_name: str):
    lines = [
        json.loads(line)
        for line in (FIXTURES / table_name / "_delta_log" / "00000000000000000000.json")
        .read_text()
        .splitlines()
        if line.strip()
    ]
    meta = next(line["metaData"] for line in lines if "metaData" in line)
    add = next(line["add"] for line in lines if "add" in line)
    return parse_schema_string(meta["schemaString"]), add


def test_parse_add_stats_all_types_real_fixture():
    schema, add = _first_add_and_schema("data-skipping-basic-stats-all-types")
    stats = parse_add_stats(add["stats"])
    assert stats.num_records == 1
    assert stats.min_values["as_int"] == 0
    assert stats.null_count["as_string"] == 0

    leaves = {tuple(s.path): s for s in iter_leaf_column_stats(schema.fields, stats)}
    assert leaves[("as_int",)].delta_type == "integer"
    assert leaves[("as_date",)].min_value == "2000-01-01"
    assert leaves[("as_timestamp",)].min_value == "2000-01-01T00:00:00.000-08:00"
    assert leaves[("as_big_decimal",)].delta_type == "decimal(1,0)"


def test_iter_leaf_column_stats_recurses_into_structs():
    schema, add = _first_add_and_schema("data-reader-nested-struct")
    # data-reader-nested-struct's adds carry no stats at all.
    stats = parse_add_stats(add.get("stats"))
    assert stats is None

    leaves = {tuple(s.path): s for s in iter_leaf_column_stats(schema.fields, stats)}
    # a.aa, a.ab, a.ac.aca, a.ac.acb, b -- all leaves reachable with no stats populated.
    assert set(leaves) == {("a", "aa"), ("a", "ab"), ("a", "ac", "aca"), ("a", "ac", "acb"), ("b",)}
    assert all(leaf.min_value is None and leaf.max_value is None for leaf in leaves.values())


def test_iter_leaf_column_stats_skips_map_and_array_columns():
    schema, _ = _first_add_and_schema("data-reader-map")
    leaves = {tuple(s.path): s for s in iter_leaf_column_stats(schema.fields, None)}
    # only "i" (integer) is a leaf; a..f are all map-typed and contribute no stats rows.
    assert set(leaves) == {("i",)}


@pytest.mark.parametrize(
    ("value", "delta_type", "expected"),
    [
        (True, "boolean", "1"),
        (False, "boolean", "0"),
        (42, "integer", "42"),
        (0, "decimal(1,0)", "0"),
        ("2000-01-01", "date", "2000-01-01"),
        ("hello", "string", "hello"),
    ],
)
def test_encode_ducklake_stat_basic(value, delta_type, expected):
    encoded, is_nan = encode_ducklake_stat(value, delta_type)
    assert encoded == expected
    assert is_nan is False


def test_encode_ducklake_stat_timestamp_with_offset():
    encoded, is_nan = encode_ducklake_stat("2000-01-01T00:00:00.000-08:00", "timestamp")
    assert encoded == "2000-01-01 00:00:00.000000-08:00"
    assert is_nan is False


def test_encode_ducklake_stat_timestamp_ntz_drops_offset():
    encoded, is_nan = encode_ducklake_stat("2021-11-18T02:30:00.123456", "timestamp_ntz")
    assert encoded == "2021-11-18 02:30:00.123456"
    assert is_nan is False


def test_encode_ducklake_stat_none_passthrough():
    assert encode_ducklake_stat(None, "integer") == (None, False)


def test_encode_ducklake_stat_float_special_values():
    assert encode_ducklake_stat(float("inf"), "double") == ("inf", False)
    assert encode_ducklake_stat(float("-inf"), "double") == ("-inf", False)
    encoded, is_nan = encode_ducklake_stat(float("nan"), "double")
    assert encoded is None
    assert is_nan is True


def test_read_parquet_record_count_matches_real_file():
    file_name = "part-00000-bf6680d4-5e83-4fce-8ebb-d2b60d7e69c9-c000.snappy.parquet"
    add_path = FIXTURES / "parquet-all-types" / file_name
    assert read_parquet_record_count(STORAGE, str(add_path)) == 200
