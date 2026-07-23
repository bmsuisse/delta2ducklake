"""SQLite and Postgres catalog backends for reading/writing a DuckLake catalog's metadata tables.

Bootstrapping (creating the 28-table schema) is delegated to DuckDB's own `ducklake` extension --
see `bootstrap.py` -- so these backends only ever need to run plain parameterized SQL against an
already-initialized catalog. All SQL in this package is authored using SQLite's native `?`
placeholder; `PostgresCatalog` translates it to psycopg's `%s`, which is safe here because
delta2ducklake only ever runs its own static SQL templates, never SQL built from external input.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from typing import LiteralString, Protocol, cast, runtime_checkable


@runtime_checkable
class CatalogBackend(Protocol):
    def execute(self, sql: str, params: Sequence = ()) -> None: ...

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> None: ...

    def fetchall(self, sql: str, params: Sequence = ()) -> list[tuple]: ...

    def fetchone(self, sql: str, params: Sequence = ()) -> tuple | None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SQLiteCatalog:
    """Talks to a DuckLake catalog stored in SQLite via the stdlib `sqlite3` module."""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA foreign_keys = OFF")

    def execute(self, sql: str, params: Sequence = ()) -> None:
        self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> None:
        self._conn.executemany(sql, list(seq_of_params))

    def fetchall(self, sql: str, params: Sequence = ()) -> list[tuple]:
        return self._conn.execute(sql, params).fetchall()

    def fetchone(self, sql: str, params: Sequence = ()) -> tuple | None:
        return self._conn.execute(sql, params).fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


class PostgresCatalog:
    """Talks to a DuckLake catalog stored in Postgres via `psycopg` (v3)."""

    def __init__(self, dsn: str):
        import psycopg

        self._conn = psycopg.connect(dsn)

    @staticmethod
    def _translate(sql: str) -> LiteralString:
        # psycopg's stubs require LiteralString for a raw query string (its way of flagging
        # "not attacker-controlled"); safe here since delta2ducklake only ever translates its own
        # static SQL templates, never a string built from external input.
        return cast(LiteralString, sql.replace("?", "%s"))

    def execute(self, sql: str, params: Sequence = ()) -> None:
        with self._conn.cursor() as cur:
            cur.execute(self._translate(sql), params)

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> None:
        with self._conn.cursor() as cur:
            cur.executemany(self._translate(sql), list(seq_of_params))

    def fetchall(self, sql: str, params: Sequence = ()) -> list[tuple]:
        with self._conn.cursor() as cur:
            cur.execute(self._translate(sql), params)
            return cur.fetchall()

    def fetchone(self, sql: str, params: Sequence = ()) -> tuple | None:
        with self._conn.cursor() as cur:
            cur.execute(self._translate(sql), params)
            return cur.fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        self._conn.close()


@dataclass(frozen=True)
class SQLiteCatalogConfig:
    """Points at a DuckLake catalog stored in a local SQLite file."""

    path: str

    def attach_url(self) -> str:
        return f"ducklake:sqlite:{self.path}"

    def connect(self) -> SQLiteCatalog:
        return SQLiteCatalog(self.path)


@dataclass(frozen=True)
class PostgresCatalogConfig:
    """Points at a DuckLake catalog stored in Postgres. `dsn` is a libpq keyword=value
    connection string (e.g. ``"host=localhost dbname=mydb user=me password=..."``), accepted
    as-is by both DuckDB's `ducklake:postgres:` attach syntax and `psycopg.connect`.
    """

    dsn: str

    def attach_url(self) -> str:
        return f"ducklake:postgres:{self.dsn}"

    def connect(self) -> PostgresCatalog:
        return PostgresCatalog(self.dsn)


CatalogConfig = SQLiteCatalogConfig | PostgresCatalogConfig
