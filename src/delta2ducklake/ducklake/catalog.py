"""SQLite, Postgres, and Quack (remote DuckDB) catalog backends for reading/writing a DuckLake
catalog's metadata tables.

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

import duckdb


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


class DuckDBCatalog:
    """Talks to a DuckLake catalog stored in a local DuckDB database file -- DuckLake's own native
    catalog format (no `sqlite:`/`postgres:` scheme prefix; a bare path is DuckDB's default).

    `path` may also be an already-open `duckdb.DuckDBPyConnection` (e.g. an in-memory connection
    the caller already manages, or one they've opened directly against the catalog file for other
    use) -- in that case the connection is reused as-is rather than opening a new one, and `close()`
    leaves it open since the caller, not this class, owns its lifecycle.

    A plain `duckdb.connect(path)` commits each statement immediately by default (same behavior
    confirmed for `QuackCatalog`), so this opens an explicit transaction right after connecting and
    re-opens one after every commit/rollback -- including on a caller-supplied connection, which
    means this class takes over that connection's transaction state while in use.
    """

    def __init__(self, path: str | duckdb.DuckDBPyConnection):
        if isinstance(path, duckdb.DuckDBPyConnection):
            self._conn = path
            self._owns_connection = False
        else:
            self._conn = duckdb.connect(path)
            self._owns_connection = True
        self._conn.execute("BEGIN TRANSACTION")

    def execute(self, sql: str, params: Sequence = ()) -> None:
        self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> None:
        self._conn.executemany(sql, list(seq_of_params))

    def fetchall(self, sql: str, params: Sequence = ()) -> list[tuple]:
        return self._conn.execute(sql, params).fetchall()

    def fetchone(self, sql: str, params: Sequence = ()) -> tuple | None:
        return self._conn.execute(sql, params).fetchone()

    def commit(self) -> None:
        self._conn.execute("COMMIT")
        self._conn.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")
        self._conn.execute("BEGIN TRANSACTION")

    def close(self) -> None:
        if self._owns_connection:
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


class QuackCatalog:
    """Talks to a DuckLake catalog reachable via DuckDB's experimental Quack client-server
    protocol (https://duckdb.org/quack/) -- a remote DuckDB instance acting as the catalog,
    addressed as `quack:<host>:<port>`, instead of a local SQLite/Postgres/DuckDB-file catalog.

    KNOWN LIMITATION (confirmed empirically against a real `quack_serve()` instance, DuckDB
    v1.5.5): tables reached via a raw `ATTACH 'quack:...' AS remote` only support `INSERT`/
    `SELECT`. Both `UPDATE` and `DELETE` fail with `Binder Error: Can only update/delete from
    base table` -- a current limitation of the experimental protocol itself, not something
    delta2ducklake can work around. Since `convert.copy_table()`/`sync_table()` rely on `DELETE`
    (bookkeeping) and `UPDATE` (retiring removed files during sync), **neither currently works
    against a Quack-backed catalog**. This class is still provided -- bootstrapping and read-only
    use already work today, and it should start working for writes with no changes needed here
    once Quack's DML support matures.

    Unlike SQLiteCatalog/PostgresCatalog (whose native drivers hold an implicit transaction open
    across statements until an explicit commit/rollback), a plain DuckDB connection commits each
    statement immediately by default (confirmed empirically against a real `.duckdb` file: calling
    `rollback()` after an uncommitted `INSERT` without an explicit `BEGIN TRANSACTION` first raises
    `TransactionException: cannot rollback - no transaction is active`). So this opens an explicit
    transaction right after connecting, and re-opens one immediately after every commit/rollback,
    to match the same "one open transaction per session" behavior the rest of this package's
    rollback-on-error pattern (see `convert.py`) relies on.
    """

    def __init__(self, endpoint: str, token: str | None = None):
        self._conn = duckdb.connect()
        self._conn.sql("INSTALL quack")
        self._conn.sql("LOAD quack")
        if token is not None:
            escaped_token = token.replace("'", "''")
            self._conn.sql(f"CREATE SECRET (TYPE quack, TOKEN '{escaped_token}')")
        escaped_endpoint = endpoint.replace("'", "''")
        self._conn.sql(f"ATTACH 'quack:{escaped_endpoint}' AS remote")
        self._conn.sql("USE remote")
        self._conn.execute("BEGIN TRANSACTION")

    def execute(self, sql: str, params: Sequence = ()) -> None:
        self._conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> None:
        self._conn.executemany(sql, list(seq_of_params))

    def fetchall(self, sql: str, params: Sequence = ()) -> list[tuple]:
        return self._conn.execute(sql, params).fetchall()

    def fetchone(self, sql: str, params: Sequence = ()) -> tuple | None:
        return self._conn.execute(sql, params).fetchone()

    def commit(self) -> None:
        self._conn.execute("COMMIT")
        self._conn.execute("BEGIN TRANSACTION")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")
        self._conn.execute("BEGIN TRANSACTION")

    def close(self) -> None:
        self._conn.close()


@dataclass(frozen=True)
class DuckDBCatalogConfig:
    """Points at a DuckLake catalog stored in a local DuckDB database file -- DuckLake's own
    native/default catalog format, e.g. `ATTACH 'ducklake:catalog.ducklake' AS x (DATA_PATH ...)`.
    Simplest option when there's no need for SQLite/Postgres/Quack specifically.

    `path` may also be an already-open `duckdb.DuckDBPyConnection` that `connect()` will reuse
    as-is (see `DuckDBCatalog`) instead of opening a new one -- useful for in-memory testing or
    when the caller already manages a connection to the catalog file. In that case `attach_url()`
    (used only by `bootstrap_catalog()`, which needs a real path to ATTACH from a *separate*
    connection) isn't available; bootstrap the schema before wrapping the connection, e.g. by
    calling `bootstrap_catalog()` with a `path`-based config first.
    """

    path: str | duckdb.DuckDBPyConnection

    def attach_url(self) -> str:
        if isinstance(self.path, duckdb.DuckDBPyConnection):
            raise ValueError(
                "bootstrap_catalog() needs a file path, not a preexisting connection -- "
                "bootstrap the catalog file first, then wrap a connection to it"
            )
        return f"ducklake:{self.path}"

    def connect(self) -> DuckDBCatalog:
        return DuckDBCatalog(self.path)

    def prepare_attach(self, con: duckdb.DuckDBPyConnection) -> None:
        """No extra setup needed before `bootstrap_catalog()` ATTACHes a DuckDB-file catalog."""


@dataclass(frozen=True)
class SQLiteCatalogConfig:
    """Points at a DuckLake catalog stored in a local SQLite file."""

    path: str

    def attach_url(self) -> str:
        return f"ducklake:sqlite:{self.path}"

    def connect(self) -> SQLiteCatalog:
        return SQLiteCatalog(self.path)

    def prepare_attach(self, con: duckdb.DuckDBPyConnection) -> None:
        """No extra setup needed before `bootstrap_catalog()` ATTACHes a SQLite-backed catalog."""


@dataclass(frozen=True)
class PostgresCatalogConfig:
    """Points at a DuckLake catalog stored in Postgres. `dsn` is a libpq keyword=value
    connection string (e.g. ``"host=localhost dbname=mydb user=me password=..."``), accepted
    as-is by both DuckDB's `ducklake:postgres:` attach syntax and `psycopg.connect`.

    Set `entra_user` to authenticate to Azure Database for PostgreSQL with a Entra ID (Azure AD)
    token instead of a static password, matching the pattern used by `bmsuisse/pgdevkit`: an AAD
    access token is fetched and used directly as the password. `dsn` should omit `user`/`password`
    (or they're overridden) in that case. Requires the `delta2ducklake[azure]` extra. Since tokens
    expire, a fresh one is fetched on every `connect()`/`attach_url()` call rather than once.
    """

    dsn: str
    entra_user: str | None = None
    managed_identity: bool = False

    def _resolved_dsn(self) -> str:
        if self.entra_user is None:
            return self.dsn
        import psycopg.conninfo

        from delta2ducklake.azure_auth import get_azure_postgres_password

        password = get_azure_postgres_password(managed_identity=self.managed_identity)
        return psycopg.conninfo.make_conninfo(
            self.dsn, user=self.entra_user, password=password
        )

    def attach_url(self) -> str:
        return f"ducklake:postgres:{self._resolved_dsn()}"

    def connect(self) -> PostgresCatalog:
        return PostgresCatalog(self._resolved_dsn())

    def prepare_attach(self, con: duckdb.DuckDBPyConnection) -> None:
        """No extra setup needed before `bootstrap_catalog()` ATTACHes a Postgres-backed catalog."""


@dataclass(frozen=True)
class QuackCatalogConfig:
    """Points at a DuckLake catalog reachable via DuckDB's experimental Quack client-server
    protocol (https://duckdb.org/quack/) -- a remote DuckDB instance, started with e.g.
    ``CALL quack_serve('quack:0.0.0.0:9494', token='...')``, acting as the catalog. `endpoint` is
    `host:port` (e.g. `"localhost:9494"`); `token` is the shared secret configured on the
    `quack_serve(...)` side, if any.

    See `QuackCatalog` for a known limitation: `copy_table()`/`sync_table()` do not currently work
    against a Quack-backed catalog, since Quack doesn't yet support `UPDATE`/`DELETE` on
    remote-attached tables. Bootstrapping and read-only queries do work today.
    """

    endpoint: str
    token: str | None = None

    def attach_url(self) -> str:
        return f"ducklake:quack:{self.endpoint}"

    def connect(self) -> QuackCatalog:
        return QuackCatalog(self.endpoint, self.token)

    def prepare_attach(self, con: duckdb.DuckDBPyConnection) -> None:
        """Load the quack extension and register the auth secret on `con` before
        `bootstrap_catalog()` issues its `ATTACH 'ducklake:quack:...'`."""
        con.sql("INSTALL quack")
        con.sql("LOAD quack")
        if self.token is not None:
            escaped_token = self.token.replace("'", "''")
            con.sql(f"CREATE SECRET (TYPE quack, TOKEN '{escaped_token}')")


CatalogConfig = (
    DuckDBCatalogConfig | SQLiteCatalogConfig | PostgresCatalogConfig | QuackCatalogConfig
)
