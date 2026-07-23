"""Shared pytest fixtures.

`pg_catalog_config` provisions a fresh, uniquely-named Postgres database per test (and drops it
afterward) so Postgres-backed integration tests get the same per-test isolation SQLite tests get
for free from `tmp_path` -- `bootstrap_catalog()` pins a catalog to the `data_path` it was first
created with, so reusing one shared database across tests/runs causes spurious "DATA_PATH doesn't
match" errors otherwise.
"""

from __future__ import annotations

import os
import uuid

import psycopg
import pytest

from delta2ducklake.ducklake.catalog import PostgresCatalogConfig

PG_BASE_DSN = os.environ.get("DELTA2DUCKLAKE_TEST_PG_DSN")


def _dsn_with_dbname(base_dsn: str, dbname: str) -> str:
    """Replace (or add) the `dbname=` component of a libpq keyword=value DSN string."""
    parts = [p for p in base_dsn.split() if not p.startswith("dbname=")]
    parts.append(f"dbname={dbname}")
    return " ".join(parts)


@pytest.fixture
def pg_catalog_config():
    if not PG_BASE_DSN:
        pytest.skip("set DELTA2DUCKLAKE_TEST_PG_DSN to run Postgres integration tests")

    db_name = f"delta2ducklake_test_{uuid.uuid4().hex[:12]}"
    admin_dsn = _dsn_with_dbname(PG_BASE_DSN, "postgres")
    with psycopg.connect(admin_dsn, autocommit=True) as admin_conn:
        admin_conn.execute(f'CREATE DATABASE "{db_name}"')

    try:
        yield PostgresCatalogConfig(_dsn_with_dbname(PG_BASE_DSN, db_name))
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin_conn:
            admin_conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            admin_conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
