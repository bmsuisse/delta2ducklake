from pathlib import Path

import pytest

from delta2ducklake.convert import copy_table
from delta2ducklake.delta.partition_layout import PartitionLayoutError
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

DELTA_IO = Path(__file__).parent / "fixtures" / "delta-io"
DELTA_RS = Path(__file__).parent / "fixtures" / "delta-rs"


def _fresh_catalog(tmp_path) -> SQLiteCatalogConfig:
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    return config


def test_copy_table_resolves_physical_names_real_fixture(tmp_path):
    """table_with_column_mapping is a real Databricks-written table (columnMapping.mode=name),
    partitioned by a physically-renamed column, confirmed earlier by direct inspection of its
    _delta_log: add.partitionValues/add.stats are keyed by the *physical* col-<uuid> names, not
    the logical "Company Very Short"/"Super Name" names.

    Its on-disk partition directories ("8v"/"BH") are opaque, not `column=value` Hive style (see
    test_materialize_partitions.py), so this needs `materialize_partitions="auto"` to succeed at
    all -- everything asserted below (name mapping, partition values, stats) is otherwise
    unaffected by that: it only changes `ducklake_data_file.path`/`path_is_relative`.
    """
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(
        str(DELTA_RS / "table_with_column_mapping"), config, "cm",
        materialize_partitions="auto",
    )

    backend = config.connect()
    try:
        columns = dict(
            backend.fetchall(
                "SELECT column_name, column_id FROM ducklake_column WHERE table_id = ?",
                (table_id,),
            )
        )
        assert set(columns) == {"Company Very Short", "Super Name"}

        # ducklake_column.column_name is always the logical name...
        mapping_id = backend.fetchone(
            "SELECT mapping_id FROM ducklake_column_mapping WHERE table_id = ?", (table_id,)
        )[0]
        name_mapping = backend.fetchall(
            "SELECT column_id, source_name, is_partition FROM ducklake_name_mapping "
            "WHERE mapping_id = ?",
            (mapping_id,),
        )
        by_column_id = {row[0]: row[1:] for row in name_mapping}
        # ...while ducklake_name_mapping.source_name is the *physical* Parquet field name.
        assert by_column_id[columns["Company Very Short"]] == (
            "col-173b4db9-b5ad-427f-9e75-516aae37fbbb",
            1,  # is_partition
        )
        assert by_column_id[columns["Super Name"]] == (
            "col-3877fd94-0973-4941-ac6b-646849a1ff65",
            0,
        )

        # 2 files, partitioned by the physically-renamed column -- values resolved from
        # add.partitionValues (keyed by physical name) despite ducklake_column using logical names.
        partition_values = {
            row[0] for row in backend.fetchall(
                "SELECT partition_value FROM ducklake_file_partition_value WHERE table_id = ?",
                (table_id,),
            )
        }
        assert partition_values == {"BMS", "BME"}

        # Stats keyed by physical name in add.stats resolve to the correct logical column_id: the
        # partition column gets no stats at all (ground truth: this writer never collected any),
        # "Super Name" aggregates correctly across both files' physical-keyed stats.
        stats = dict(
            backend.fetchall(
                "SELECT column_id, min_value FROM ducklake_table_column_stats WHERE table_id = ?",
                (table_id,),
            )
        )
        assert stats[columns["Company Very Short"]] is None
        assert stats[columns["Super Name"]] == "Anthony Johnson"

        data_files = backend.fetchall(
            "SELECT record_count FROM ducklake_data_file WHERE table_id = ? ORDER BY record_count",
            (table_id,),
        )
        assert [r[0] for r in data_files] == [1, 4]
    finally:
        backend.close()


def test_copy_table_rejects_opaque_partition_layout_by_default(tmp_path):
    """Without materialize_partitions, copy_table() must fail loudly rather than silently produce
    a catalog DuckDB's own ducklake reader can't actually read back (see docs/IMPLEMENTATION.md)."""
    config = _fresh_catalog(tmp_path)
    with pytest.raises(PartitionLayoutError, match="materialize_partitions"):
        copy_table(str(DELTA_RS / "table_with_column_mapping"), config, "cm")


def test_copy_table_column_mapping_mode_id_real_fixture(tmp_path):
    config = _fresh_catalog(tmp_path)
    table_id = copy_table(str(DELTA_IO / "table-with-columnmapping-mode-id"), config, "t")

    backend = config.connect()
    try:
        mapping_type = backend.fetchone(
            "SELECT type FROM ducklake_column_mapping WHERE table_id = ?", (table_id,)
        )
        assert mapping_type == ("map_by_name",)
        (record_count,) = backend.fetchone(
            "SELECT record_count FROM ducklake_table_stats WHERE table_id = ?", (table_id,)
        )
        assert record_count > 0
    finally:
        backend.close()
