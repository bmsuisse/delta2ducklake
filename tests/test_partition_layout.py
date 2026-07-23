from delta2ducklake.delta.partition_layout import (
    encode_hive_value,
    hive_path_segments,
    is_hive_style_layout,
)


def test_is_hive_style_layout_true_for_plain_hive_path():
    path = "as_int=0/as_long=0/part-00000-abc.c000.snappy.parquet"
    assert is_hive_style_layout(path, ["as_int", "as_long"])


def test_is_hive_style_layout_true_for_column_mapped_physical_name():
    path = "col-31f31113=2021-09-08/part-00000-abc.c000.snappy.parquet"
    assert is_hive_style_layout(path, ["col-31f31113"])


def test_is_hive_style_layout_false_for_opaque_directory_names():
    """Real Databricks output for a column-mapped + partitioned table (delta-rs's
    table_with_column_mapping fixture): directory names carry no `key=` prefix at all."""
    path = "8v/part-00001-69b4a452.c000.zstd.parquet"
    assert not is_hive_style_layout(path, ["some_physical_name"])


def test_is_hive_style_layout_false_when_segment_count_mismatches():
    path = "as_int=0/part-00000-abc.c000.snappy.parquet"
    assert not is_hive_style_layout(path, ["as_int", "as_long"])


def test_is_hive_style_layout_true_for_unpartitioned_table():
    assert is_hive_style_layout("part-00000-abc.c000.snappy.parquet", [])


def test_encode_hive_value_keeps_space_escapes_colon():
    """Matches the real Databricks-written fixture's on-disk folder name:
    `as_timestamp=2021-09-08 11%3A11%3A11` -- space bare, colon percent-encoded."""
    assert encode_hive_value("2021-09-08 11:11:11") == "2021-09-08 11%3A11%3A11"


def test_encode_hive_value_none_is_hive_default_partition():
    assert encode_hive_value(None) == "__HIVE_DEFAULT_PARTITION__"


def test_hive_path_segments_order_and_nulls():
    segments = hive_path_segments(
        ["as_int", "as_string"], {"as_int": "0", "as_string": None}
    )
    assert segments == ["as_int=0", "as_string=__HIVE_DEFAULT_PARTITION__"]
