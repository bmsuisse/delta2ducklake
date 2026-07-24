---
name: delta2ducklake
description: >
  Use delta2ducklake to register a Delta Lake table into a DuckLake catalog
  without copying or rewriting the underlying Parquet files, or to keep an
  already-registered table in sync as the Delta table gets new commits. Use
  whenever the user wants to query a Delta table through DuckLake/DuckDB,
  asks things like "register this delta table in ducklake", "convert delta
  to ducklake", "sync my ducklake table", "bootstrap a ducklake catalog",
  "recompute/backfill stats for a ducklake table", or is choosing between
  DuckDB-file/SQLite/Postgres/Quack as a DuckLake catalog backend.
---

# delta2ducklake

Reads a [Delta Lake](https://delta.io) table's transaction log directly (JSON commits +
checkpoints — no dependency on the `deltalake` Python package) and writes the corresponding
metadata into a [DuckLake](https://ducklake.select) catalog, so the table becomes queryable through
DuckLake/DuckDB **without copying or rewriting a single Parquet file**. Handles column mapping,
deletion vectors, Hive-style partitioning, and table/file statistics.

Two entry points cover the whole lifecycle:

- `copy_table(...)` — register a Delta table's current state as a brand-new DuckLake table; raises
  if the table name already exists.
- `sync_table(...)` — bring a table up to the Delta table's current state (adds new files, retires
  removed ones); creates the table via `copy_table` first if it doesn't exist yet, so it's safe to
  call unconditionally. Raises `NotImplementedError` if the Delta table's schema changed since it
  was registered — schema evolution during sync isn't supported yet.

Neither ever touches the Delta table's own files or `_delta_log` — only the DuckLake catalog is
written to, with one opt-in exception: see `materialize_partitions` below.

## Partitioned + column-mapped tables (common on Databricks/Unity Catalog)

Enabling Delta column mapping on a *partitioned* table makes Databricks replace its Hive-style
`column=value/` partition directories with opaque, unparseable names. DuckDB's own `ducklake`
reader needs that Hive-style layout to reconstruct partition values — it doesn't consult
delta2ducklake's catalog metadata for this — so by default `copy_table`/`sync_table` raise
`PartitionLayoutError` for such a table rather than register one DuckDB can't actually read back.

Pass `materialize_partitions="auto"` (copy affected files into the catalog's own `data_path`) or a
directory/URL (copy them there instead) to work around it — this makes one real, byte-for-byte
copy of each affected Parquet file into a genuine Hive layout this project builds itself. It's the
one deliberate exception to "never copy the data," so only reach for it when a table actually hits
this failure mode (unpartitioned tables and already-Hive-style layouts are never copied, regardless
of this argument).

## Install

```bash
uv add delta2ducklake
uv add "delta2ducklake[azure]"   # only if the Delta table or a Postgres catalog lives on Azure
```

## Core rules

- **Bootstrap the catalog exactly once** with `bootstrap_catalog(config, data_path=...)` before any
  `copy_table`/`sync_table` call. It's idempotent (safe to call again), but every call for the same
  catalog must use the **identical `data_path` string** — DuckDB's `ducklake` extension pins a
  catalog to the exact `DATA_PATH` it was first bootstrapped with. Always pass **absolute paths**;
  a relative path resolves differently depending on the current working directory and will
  spuriously conflict across different callers (cron job vs. shell vs. CI).
- **Prefer `sync_table` for anything that might run more than once** — it creates the table (via
  `copy_table`) if it doesn't exist yet, then updates it on every later call, so it's safe to call
  unconditionally (e.g. from a recurring job) without checking whether a prior run already
  registered the table. Reach for `copy_table` directly only when the caller specifically wants a
  hard failure if the table already exists — it raises rather than silently overwriting.
- **Never hand-roll delta log parsing or catalog SQL** — always go through `copy_table`/
  `sync_table`/`refresh_stats`/`bootstrap_catalog`. There's no supported lower-level API.
- **Don't add `deltalake` (the Python package) as a dependency to reach for instead of this
  library** — that's the exact heavyweight dependency this package exists to avoid; if something
  seems missing, it belongs in delta2ducklake itself, not a workaround via `deltalake`.

## Choosing a catalog backend

| Backend | Config class | When to use |
|---|---|---|
| Local DuckDB file | `DuckDBCatalogConfig` | Default choice — DuckLake's own native catalog format, simplest setup, no extra service needed |
| SQLite | `SQLiteCatalogConfig` | Equivalent to the DuckDB-file backend; pick if the catalog needs to be inspectable via plain `sqlite3` tooling |
| PostgreSQL | `PostgresCatalogConfig` | Multiple readers/writers, a shared/remote catalog. Supports Entra ID (Azure AD) token auth for Azure Database for PostgreSQL via `entra_user=` |
| Quack (experimental) | `QuackCatalogConfig` | **Do not use for `copy_table`/`sync_table` yet** — see limitation below |

```python
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import DuckDBCatalogConfig
from delta2ducklake.convert import copy_table, sync_table

catalog = DuckDBCatalogConfig("/abs/path/to/catalog.ducklake")
bootstrap_catalog(catalog, data_path="/abs/path/to/ducklake_data/")

copy_table("/abs/path/to/my_delta_table", catalog, "my_table")
# ... later, after new commits land in the Delta table ...
sync_table("/abs/path/to/my_delta_table", catalog, "my_table")
```

`DuckDBCatalogConfig` also accepts an already-open `duckdb.DuckDBPyConnection` instead of a path
(useful for in-memory tests, or reusing a connection the caller already manages) — `connect()`
reuses it as-is and doesn't close it, since the caller owns its lifecycle. `bootstrap_catalog()`
itself still needs a real path (it ATTACHes from a separate connection).

For Postgres with Entra ID auth (needs `delta2ducklake[azure]`):

```python
from delta2ducklake.ducklake.catalog import PostgresCatalogConfig

catalog = PostgresCatalogConfig(
    "host=myserver.postgres.database.azure.com dbname=mydb",
    entra_user="app@mydb",   # omit user/password from the DSN
)
```

**Quack limitation, confirmed by direct testing:** tables reached via `QuackCatalogConfig`'s
`ATTACH 'quack:...'` only support `INSERT`/`SELECT` as of DuckDB v1.5.5 — `UPDATE`/`DELETE` fail
with `Binder Error: Can only update/delete from base table`. Since `copy_table`/`sync_table` rely
on both, **they do not currently work against a Quack-backed catalog** — only
`bootstrap_catalog()` and read-only queries do. Steer users away from Quack for anything beyond
bootstrapping/reading until this is confirmed fixed upstream.

## Reading the result

```sql
INSTALL ducklake;
ATTACH 'ducklake:/abs/path/to/catalog.ducklake' AS dl (DATA_PATH '/abs/path/to/ducklake_data/');
SELECT * FROM dl.my_table;
```

(Swap the attach string for `ducklake:sqlite:...`/`ducklake:postgres:...`/`ducklake:quack:...` to
match whichever `CatalogConfig` was used — each config's own `.attach_url()` produces this string.)

## Stats backfill

Delta's `dataSkippingNumIndexedCols` often leaves trailing columns with no stats at all.
`refresh_stats` (re)computes stats for any DuckLake table's columns directly from the registered
Parquet files, independent of Delta:

```python
from delta2ducklake.ducklake.stats_refresh import refresh_stats

refresh_stats(catalog, "my_table", columns=["a_column_delta_never_indexed"])  # omit columns for all
```

## CLI

Same operations as `delta2ducklake <command>` (installed as a console script) — useful for cron
jobs/orchestration rather than embedding in Python:

```bash
delta2ducklake bootstrap --catalog duckdb:./catalog.ducklake --data-path ./ducklake_data/
delta2ducklake copy   /path/to/my_delta_table --catalog duckdb:./catalog.ducklake --table my_table
delta2ducklake sync   /path/to/my_delta_table --catalog duckdb:./catalog.ducklake --table my_table
delta2ducklake refresh-stats --catalog duckdb:./catalog.ducklake --table my_table --columns a,b
```

`--catalog` accepts `duckdb:<path>`, `sqlite:<path>`, `postgres:<libpq DSN>`, or `quack:<host:port>`.
`--entra-user`/`--managed-identity` apply only to `postgres:`; `--quack-token` only to `quack:`.

## Quick checklist

- [ ] `bootstrap_catalog()` called once, with an **absolute** `data_path`, before any copy/sync
- [ ] Every later `bootstrap_catalog()`/ATTACH for the same catalog uses the identical `data_path`
- [ ] `sync_table` used for anything that might run more than once (safe unconditionally); `copy_table`
      reached for directly only when a hard failure on an already-existing table is wanted
- [ ] Not reaching for the `deltalake` Python package as a workaround — if something's missing,
      it belongs in delta2ducklake
- [ ] Quack (`QuackCatalogConfig`) not used for `copy_table`/`sync_table` — bootstrap/read-only only
- [ ] `refresh_stats()` used to backfill columns Delta itself never collected stats for
- [ ] If `copy_table`/`sync_table` raises `PartitionLayoutError`, that's a partitioned +
      column-mapped Databricks table — pass `materialize_partitions="auto"` (or a directory)
      rather than working around it another way
