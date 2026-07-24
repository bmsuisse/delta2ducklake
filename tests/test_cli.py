from pathlib import Path

import pytest

from delta2ducklake.cli import main
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig

FIXTURES = Path(__file__).parent / "fixtures" / "delta-io"
DELTA_RS_FIXTURES = Path(__file__).parent / "fixtures" / "delta-rs"


def test_bootstrap_copy_and_refresh_stats(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.db"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(FIXTURES / "parquet-all-types")

    assert main(["bootstrap", "--catalog", f"sqlite:{catalog_path}", "--data-path", data_path]) == 0
    out = capsys.readouterr().out
    assert "Bootstrapped" in out

    assert (
        main(
            [
                "copy", table_root,
                "--catalog", f"sqlite:{catalog_path}",
                "--table", "all_types",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "Registered main.all_types" in out

    assert (
        main(
            [
                "refresh-stats",
                "--catalog", f"sqlite:{catalog_path}",
                "--table", "all_types",
                "--columns", "BooleanType",
            ]
        )
        == 0
    )
    assert "Refreshed stats" in capsys.readouterr().out

    config = SQLiteCatalogConfig(str(catalog_path))
    backend = config.connect()
    try:
        (record_count,) = backend.fetchone("SELECT record_count FROM ducklake_table_stats")
        assert record_count == 200
    finally:
        backend.close()


def test_copy_twice_returns_error_exit_code(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.db"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(FIXTURES / "parquet-all-types")
    catalog_arg = f"sqlite:{catalog_path}"

    main(["bootstrap", "--catalog", catalog_arg, "--data-path", data_path])
    main(["copy", table_root, "--catalog", catalog_arg, "--table", "t"])
    capsys.readouterr()

    exit_code = main(["copy", table_root, "--catalog", catalog_arg, "--table", "t"])
    assert exit_code == 1
    assert "already exists" in capsys.readouterr().err


def test_sync_after_copy(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.db"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(FIXTURES / "snapshot-data3")
    catalog_arg = f"sqlite:{catalog_path}"

    main(["bootstrap", "--catalog", catalog_arg, "--data-path", data_path])
    main(["copy", table_root, "--catalog", catalog_arg, "--table", "t", "--version", "1"])
    capsys.readouterr()

    exit_code = main(
        ["sync", table_root, "--catalog", catalog_arg, "--table", "t", "--version", "3"]
    )
    assert exit_code == 0
    assert "Synced main.t" in capsys.readouterr().out


def test_bootstrap_and_copy_with_duckdb_catalog(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.ducklake"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(FIXTURES / "parquet-all-types")
    catalog_arg = f"duckdb:{catalog_path}"

    assert main(["bootstrap", "--catalog", catalog_arg, "--data-path", data_path]) == 0
    capsys.readouterr()

    assert (
        main(["copy", table_root, "--catalog", catalog_arg, "--table", "all_types"]) == 0
    )
    assert "Registered main.all_types" in capsys.readouterr().out


def test_copy_reports_error_for_opaque_partition_layout_without_flag(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.db"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(DELTA_RS_FIXTURES / "table_with_column_mapping")
    catalog_arg = f"sqlite:{catalog_path}"

    main(["bootstrap", "--catalog", catalog_arg, "--data-path", data_path])
    capsys.readouterr()

    exit_code = main(["copy", table_root, "--catalog", catalog_arg, "--table", "cm"])
    assert exit_code == 1
    assert "materialize_partitions" in capsys.readouterr().err


def test_copy_with_materialize_partitions_flag_succeeds(tmp_path, capsys):
    catalog_path = tmp_path / "catalog.db"
    data_path = str(tmp_path / "data") + "/"
    table_root = str(DELTA_RS_FIXTURES / "table_with_column_mapping")
    catalog_arg = f"sqlite:{catalog_path}"

    main(["bootstrap", "--catalog", catalog_arg, "--data-path", data_path])
    capsys.readouterr()

    exit_code = main(
        [
            "copy", table_root,
            "--catalog", catalog_arg,
            "--table", "cm",
            "--materialize-partitions", "auto",
        ]
    )
    assert exit_code == 0
    assert "Registered main.cm" in capsys.readouterr().out


def test_invalid_catalog_spec_errors():
    # argparse's own `type=` validation failure -- raises SystemExit(2), not a plain return value.
    with pytest.raises(SystemExit) as exc_info:
        main(["copy", "somewhere", "--catalog", "not-a-valid-spec", "--table", "t"])
    assert exc_info.value.code == 2
