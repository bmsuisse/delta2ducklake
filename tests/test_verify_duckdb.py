"""End-to-end verification against real DuckDB: not just our own unit tests trusting our own
reader, but cross-checking a copy_table()'d DuckLake catalog -- read back through DuckDB's actual
`ducklake` extension -- against the same source Delta table read through DuckDB's actual `delta`
extension (`delta_scan`). Row-for-row (`EXCEPT` both directions), not just row counts.
"""

from pathlib import Path

import duckdb
import pytest

from delta2ducklake.convert import copy_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

MATERIALIZE_FIXTURE = "table_with_column_mapping"

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"
DELTA_RS_FIXTURES = Path(__file__).parent / "fixtures" / "delta-rs"

# Fixtures with real backing Parquet files, covering: flat/wide primitives, nested struct/list/map,
# partitioning, multi-file tables, decimals, all three checkpoint shapes (classic multi-part, V2
# JSON, V2 Parquet), both column mapping modes, and deletion vectors (plain, partitioned+
# checkpointed, combined with column mapping, and assorted log-replay edge cases).
FIXTURE_NAMES = [
    "parquet-all-types",
    "data-reader-partition-values",
    "data-skipping-basic-stats-all-types",
    "decimal-various-scale-precision",
    "v2-checkpoint-parquet",
    "v2-checkpoint-json",
    "multi-part-checkpoint",
    "snapshot-data3",
    "data-reader-nested-struct",
    "data-reader-map",
    "data-reader-array-complex-objects",
    "table-with-columnmapping-mode-name",
    "table-with-columnmapping-mode-id",
    "dv-partitioned-with-checkpoint",
    "dv-with-columnmapping",
    "log-replay-dv-key-cases",
]

# Real Databricks-written deletion-vector fixture from delta-rs (a different engine than the
# delta-io fixtures above), lives in a separate vendored directory.
DELTA_RS_FIXTURE_NAMES = ["table-with-dv-small"]

# data-reader-partition-values has a 12-level nested Hive partition path, one level of which is a
# timestamp containing a colon (percent-encoded in the path). DuckDB's `delta_scan` extension
# reads it fine standalone, but throws an IO error over a *double*-percent-encoded version of that
# same path as soon as it appears alongside another relation in one query (reproduced with a fresh
# connection and no ducklake catalog involved at all -- the error only ever names the raw Delta
# path, never anything under delta2ducklake's own data_path). A delta_scan-side quirk on this one
# fixture's path, not something delta2ducklake produced; row-count is still checked, just not the
# row-for-row EXCEPT comparison. Partitioning logic itself has its own dedicated unit test
# (test_convert.py::test_copy_table_with_partitioning), including the NULL-partition-value case.
#
# Separately (not in this fixture list at all): a *real* Databricks-written column-mapped +
# partitioned table (delta-rs's table_with_column_mapping) uses non-Hive-style directory names
# ("BH"/"8v" instead of "col=value"). DuckDB's own `ducklake` reader turns out to materialize
# partition column values by parsing the Hive path segment, not from `ducklake_file_partition_value`
# (confirmed: opening *any* column of such a file fails, not just the partition column) -- a hard
# requirement of the real reader that no amount of correct catalog metadata can work around without
# physically rewriting the files (which would violate "never copy/rewrite the data"). That table's
# catalog correctness (name mapping, partition values, stats) is instead verified directly via SQL
# in test_column_mapping.py; see docs/IMPLEMENTATION.md for the full writeup.
_SKIP_ROW_CONTENT_CHECK = {"data-reader-partition-values"}


@pytest.fixture(scope="module")
def duckdb_con():
    con = duckdb.connect()
    try:
        con.sql("INSTALL ducklake; LOAD ducklake; INSTALL delta; LOAD delta;")
    except Exception as e:
        pytest.skip(f"DuckDB ducklake/delta extensions unavailable: {e}")
    yield con
    con.close()


def _assert_copy_table_matches_delta_scan(duckdb_con, tmp_path, table_root, fixture_name):
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, data_path)
    copy_table(table_root, config, "t")

    schema_name = f"dl_{fixture_name.replace('-', '_')}"
    duckdb_con.sql(
        f"ATTACH '{config.attach_url()}' AS {schema_name} (DATA_PATH '{data_path}', READ_ONLY)"
    )
    try:
        dl_count = duckdb_con.sql(f"SELECT count(*) FROM {schema_name}.t").fetchone()[0]
        delta_count = duckdb_con.sql(
            f"SELECT count(*) FROM delta_scan('{table_root}')"
        ).fetchone()[0]
        assert dl_count == delta_count, f"{fixture_name}: row count mismatch"

        if fixture_name in _SKIP_ROW_CONTENT_CHECK:
            return

        q_dl = f"SELECT * FROM {schema_name}.t"
        q_delta = f"SELECT * FROM delta_scan('{table_root}')"
        only_in_dl = duckdb_con.sql(f"({q_dl}) EXCEPT ({q_delta})").fetchall()
        only_in_delta = duckdb_con.sql(f"({q_delta}) EXCEPT ({q_dl})").fetchall()
        assert only_in_dl == [], f"{fixture_name}: rows only in ducklake catalog: {only_in_dl[:3]}"
        assert only_in_delta == [], f"{fixture_name}: rows only in delta_scan: {only_in_delta[:3]}"
    finally:
        duckdb_con.sql(f"DETACH {schema_name}")


@pytest.mark.parametrize("fixture_name", FIXTURE_NAMES)
def test_copy_table_matches_delta_scan_row_for_row(duckdb_con, tmp_path, fixture_name):
    _assert_copy_table_matches_delta_scan(
        duckdb_con, tmp_path, str(FIXTURES / fixture_name), fixture_name
    )


@pytest.mark.parametrize("fixture_name", DELTA_RS_FIXTURE_NAMES)
def test_copy_table_matches_delta_scan_row_for_row_delta_rs(duckdb_con, tmp_path, fixture_name):
    """Same check, against delta-rs's vendored fixtures (a different writer/engine than the
    delta-io fixtures above) -- currently just the real Databricks-written deletion-vector table.
    """
    _assert_copy_table_matches_delta_scan(
        duckdb_con, tmp_path, str(DELTA_RS_FIXTURES / fixture_name), fixture_name
    )


def test_materialize_partitions_readable_through_real_ducklake_extension(duckdb_con, tmp_path):
    """The one case `_assert_copy_table_matches_delta_scan`'s FIXTURE_NAMES/DELTA_RS_FIXTURE_NAMES
    loop can't cover directly: `table_with_column_mapping`'s own directory layout ("8v"/"BH",
    opaque -- no `column=value` at all) makes DuckDB's real `ducklake` reader fail outright without
    materialize_partitions (see docs/IMPLEMENTATION.md). With it, the materialized copies live
    under a genuine Hive path this project builds itself (delta.partition_layout.encode_hive_value)
    -- this is the empirical proof that DuckDB's own Hive-partition decoder actually accepts that
    encoding and reconstructs the right values, not just that delta2ducklake's own catalog rows
    look correct (already unit-tested in test_column_mapping.py).
    """
    table_root = str(DELTA_RS_FIXTURES / MATERIALIZE_FIXTURE)
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, data_path)
    copy_table(table_root, config, "t", materialize_partitions="auto")

    duckdb_con.sql(
        f"ATTACH '{config.attach_url()}' AS dl_materialize (DATA_PATH '{data_path}', READ_ONLY)"
    )
    try:
        q_dl = "SELECT * FROM dl_materialize.t"
        q_delta = f"SELECT * FROM delta_scan('{table_root}')"
        only_in_dl = duckdb_con.sql(f"({q_dl}) EXCEPT ({q_delta})").fetchall()
        only_in_delta = duckdb_con.sql(f"({q_delta}) EXCEPT ({q_dl})").fetchall()
        assert only_in_dl == [], f"rows only in materialized ducklake catalog: {only_in_dl[:3]}"
        assert only_in_delta == [], f"rows only in delta_scan: {only_in_delta[:3]}"
    finally:
        duckdb_con.sql("DETACH dl_materialize")


def test_stats_enable_file_pruning_matching_delta_scan_results(duckdb_con, tmp_path):
    """Not just "stats exist" (already unit-tested) -- confirm DuckDB's real query engine can
    actually use them to prune files and still return the exact same rows a full scan would.
    """
    table_root = str(FIXTURES / "data-skipping-basic-stats-all-types")
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, data_path)
    copy_table(table_root, config, "t")

    duckdb_con.sql(
        f"ATTACH '{config.attach_url()}' AS dl_pruning (DATA_PATH '{data_path}', READ_ONLY)"
    )
    try:
        q_dl = "SELECT as_int, as_string FROM dl_pruning.t WHERE as_int > 0"
        q_delta = f"SELECT as_int, as_string FROM delta_scan('{table_root}') WHERE as_int > 0"
        only_in_dl = duckdb_con.sql(f"({q_dl}) EXCEPT ({q_delta})").fetchall()
        only_in_delta = duckdb_con.sql(f"({q_delta}) EXCEPT ({q_dl})").fetchall()
        assert only_in_dl == []
        assert only_in_delta == []
    finally:
        duckdb_con.sql("DETACH dl_pruning")
