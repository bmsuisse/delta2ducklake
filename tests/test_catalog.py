from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig


def test_bootstrap_creates_all_28_tables(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))

    bootstrap_catalog(config, data_path)

    backend = config.connect()
    try:
        tables = {
            r[0]
            for r in backend.fetchall(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        }
        assert len(tables) == 28
        assert "ducklake_data_file" in tables
        assert "ducklake_column_mapping" in tables
        assert "ducklake_delete_file" in tables
    finally:
        backend.close()


def test_bootstrap_sets_expected_metadata_rows(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, data_path)

    backend = config.connect()
    try:
        rows = dict(backend.fetchall("SELECT key, value FROM ducklake_metadata"))
        assert rows["data_path"] == data_path
        assert "version" in rows
    finally:
        backend.close()


def test_bootstrap_creates_initial_snapshot_and_main_schema(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, data_path)

    backend = config.connect()
    try:
        snapshots = backend.fetchall("SELECT snapshot_id, next_catalog_id FROM ducklake_snapshot")
        assert len(snapshots) == 1
        schemas = backend.fetchall("SELECT schema_name FROM ducklake_schema")
        assert schemas == [("main",)]
    finally:
        backend.close()


def test_bootstrap_is_idempotent(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    data_path = str(tmp_path / "data") + "/"
    config = SQLiteCatalogConfig(str(catalog_path))

    bootstrap_catalog(config, data_path)
    bootstrap_catalog(config, data_path)

    backend = config.connect()
    try:
        (count,) = backend.fetchone("SELECT count(*) FROM ducklake_snapshot")
        assert count == 1
    finally:
        backend.close()


def test_sqlite_catalog_execute_and_transaction(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")

    backend = config.connect()
    try:
        backend.execute(
            "INSERT INTO ducklake_metadata (key, value) VALUES (?, ?)", ("custom_key", "42")
        )
        backend.commit()
        assert backend.fetchone(
            "SELECT value FROM ducklake_metadata WHERE key = ?", ("custom_key",)
        ) == ("42",)
    finally:
        backend.close()


def test_sqlite_catalog_rollback(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")

    backend = config.connect()
    try:
        backend.execute(
            "INSERT INTO ducklake_metadata (key, value) VALUES (?, ?)", ("rolled_back", "x")
        )
        backend.rollback()
        assert (
            backend.fetchone(
                "SELECT value FROM ducklake_metadata WHERE key = ?", ("rolled_back",)
            )
            is None
        )
    finally:
        backend.close()


def test_sqlite_catalog_executemany(tmp_path):
    catalog_path = tmp_path / "catalog.sqlite"
    config = SQLiteCatalogConfig(str(catalog_path))
    bootstrap_catalog(config, str(tmp_path / "data") + "/")

    backend = config.connect()
    try:
        backend.executemany(
            "INSERT INTO ducklake_metadata (key, value) VALUES (?, ?)",
            [("k1", "v1"), ("k2", "v2")],
        )
        backend.commit()
        rows = dict(
            backend.fetchall(
                "SELECT key, value FROM ducklake_metadata WHERE key IN (?, ?)", ("k1", "k2")
            )
        )
        assert rows == {"k1": "v1", "k2": "v2"}
    finally:
        backend.close()


def test_postgres_catalog_bootstrap_and_roundtrip(pg_catalog_config, tmp_path):
    config = pg_catalog_config
    data_path = str(tmp_path / "data") + "/"
    bootstrap_catalog(config, data_path)

    backend = config.connect()
    try:
        tables = {
            r[0]
            for r in backend.fetchall(
                # DuckLake's catalog system tables (ducklake_*) live in Postgres's default
                # "public" schema -- "main" is the separate logical schema DuckLake creates for
                # *user* tables (bootstrap_catalog's own "main" ducklake_schema row), unrelated to
                # where the catalog's own bookkeeping tables are stored.
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
        }
        assert len(tables) >= 28
        backend.execute(
            "INSERT INTO ducklake_metadata (key, value) VALUES (?, ?)", ("custom_key", "42")
        )
        backend.commit()
        assert backend.fetchone(
            "SELECT value FROM ducklake_metadata WHERE key = ?", ("custom_key",)
        ) == ("42",)
    finally:
        backend.close()
