from pathlib import Path

import pytest

from delta2ducklake.convert import copy_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig
from delta2ducklake.ducklake.stats_refresh import refresh_stats

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"


def _copied_table(tmp_path):
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    table_id = copy_table(str(FIXTURES / "parquet-all-types"), config, "all_types")
    return config, table_id


def _table_column_stats(backend, table_id, column_name):
    return backend.fetchone(
        "SELECT s.contains_null, s.contains_nan, s.min_value, s.max_value "
        "FROM ducklake_table_column_stats s JOIN ducklake_column c USING (table_id, column_id) "
        "WHERE s.table_id = ? AND c.column_name = ?",
        (table_id, column_name),
    )


def test_refresh_stats_reproduces_the_same_bounds_as_delta_stats(tmp_path):
    config, table_id = _copied_table(tmp_path)

    before = {}
    backend = config.connect()
    for name in ("ByteType", "IntegerType", "StringType", "DateType"):
        before[name] = _table_column_stats(backend, table_id, name)
    backend.close()

    refresh_stats(config, "all_types")

    backend = config.connect()
    try:
        for name, expected in before.items():
            assert _table_column_stats(backend, table_id, name) == expected
    finally:
        backend.close()


def test_refresh_stats_can_backfill_a_column_delta_never_collected_stats_for(tmp_path):
    config, table_id = _copied_table(tmp_path)

    # BooleanType has no stats at all in this fixture's own add.stats (simulating
    # dataSkippingNumIndexedCols cutting it off) -- refresh_stats should be able to fill it in by
    # reading the real Parquet data directly.
    backend = config.connect()
    try:
        assert _table_column_stats(backend, table_id, "BooleanType") == (1, 0, None, None)
    finally:
        backend.close()

    refresh_stats(config, "all_types", columns=["BooleanType"])

    backend = config.connect()
    try:
        contains_null, contains_nan, min_v, max_v = _table_column_stats(
            backend, table_id, "BooleanType"
        )
        assert contains_null == 1
        assert min_v == "0"
        assert max_v == "1"
    finally:
        backend.close()


def test_refresh_stats_only_touches_requested_columns(tmp_path):
    config, table_id = _copied_table(tmp_path)
    backend = config.connect()
    before_int = _table_column_stats(backend, table_id, "IntegerType")
    backend.close()

    refresh_stats(config, "all_types", columns=["BooleanType"])

    backend = config.connect()
    try:
        after_int = _table_column_stats(backend, table_id, "IntegerType")
        assert after_int == before_int
    finally:
        backend.close()


def test_refresh_stats_raises_for_unknown_column(tmp_path):
    config, _ = _copied_table(tmp_path)
    with pytest.raises(ValueError, match="No such top-level column"):
        refresh_stats(config, "all_types", columns=["NotAColumn"])


def test_refresh_stats_raises_for_unknown_table(tmp_path):
    config = SQLiteCatalogConfig(str(tmp_path / "catalog.sqlite"))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")
    with pytest.raises(ValueError, match="does not exist"):
        refresh_stats(config, "nope")
