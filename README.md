# delta2ducklake

Register a [Delta Lake](https://delta.io) table into a [DuckLake](https://ducklake.select) catalog
**without copying or rewriting the underlying Parquet files**. Reads the Delta transaction log
directly (JSON commits + checkpoints), no dependency on the `deltalake` Python package.

Status: early development, not yet published to PyPI.

## Supported

- Full (`copy_table`) and incremental (`sync_table`) conversion, including table/file statistics
  and Hive-style partitioning
- Delta column mapping (`columnMapping.mode = name` / `id`)
- Deletion vectors, converted to DuckLake positional delete files
- A standalone `refresh_stats()` utility to (re)compute stats for any DuckLake table's columns
  (useful since Delta's `dataSkippingNumIndexedCols` often leaves trailing columns with none)
- Catalog backends: SQLite, PostgreSQL
- Storage backends: local filesystem, Azure Blob/ADLS Gen2 (`delta2ducklake[azure]`)

See [`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md) for design notes, protocol details pinned
down against real fixtures, and known limitations (e.g. DuckDB's own `ducklake` reader can't
materialize partition column values for tables whose physical layout isn't Hive-style).

## Install

Not yet on PyPI — for now, install from a local clone:

```bash
uv add /path/to/delta2ducklake          # or: pip install /path/to/delta2ducklake
uv add "delta2ducklake[azure]" ...       # if your Delta table or catalog lives on Azure
```

## Quick start

A DuckLake catalog needs to be **bootstrapped** once (creates its metadata schema), then tables
are registered into it with `copy_table()` and kept up to date with `sync_table()`:

```python
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import SQLiteCatalogConfig
from delta2ducklake.convert import copy_table, sync_table

catalog = SQLiteCatalogConfig("/abs/path/to/catalog.sqlite")
bootstrap_catalog(catalog, data_path="/abs/path/to/ducklake_data/")  # one-time; safe to re-run

# Register a Delta table's current state as a new DuckLake table.
copy_table("/path/to/my_delta_table", catalog, "my_table")

# ... later, after the Delta table has new commits ...
sync_table("/path/to/my_delta_table", catalog, "my_table")
```

> **Use absolute paths.** DuckDB's own `ducklake` extension pins a catalog to the exact
> `DATA_PATH` string it was first bootstrapped with and rejects any later `ATTACH` whose
> `DATA_PATH` doesn't match *that exact string* — a relative path resolves differently depending
> on the current working directory, so `bootstrap_catalog()`/`ATTACH` calls made from different
> directories (or a cron job vs. a shell) will spuriously conflict unless every caller passes the
> identical absolute path.

For Postgres, swap in a DSN:

```python
from delta2ducklake.ducklake.catalog import PostgresCatalogConfig

catalog = PostgresCatalogConfig("host=localhost dbname=mydb user=me password=...")
bootstrap_catalog(catalog, data_path="/abs/path/to/ducklake_data/")  # or an Azure URL, see below
```

For Azure Database for PostgreSQL, authenticate with an Entra ID (Azure AD) token instead of a
static password by setting `entra_user` (needs `delta2ducklake[azure]`; mirrors the pattern used by
[`bmsuisse/pgdevkit`](https://github.com/bmsuisse/pgdevkit)). A fresh token is fetched on every
`connect()`/bootstrap, since Entra tokens expire:

```python
catalog = PostgresCatalogConfig(
    "host=myserver.postgres.database.azure.com dbname=mydb",
    entra_user="app@mydb",       # omit user/password from the DSN — these are supplied for you
    managed_identity=False,      # True to use ManagedIdentityCredential instead of DefaultAzureCredential
)
```

Then query the result with DuckDB directly:

```sql
INSTALL ducklake;
ATTACH 'ducklake:sqlite:/abs/path/to/catalog.sqlite' AS dl (DATA_PATH '/abs/path/to/ducklake_data/');
SELECT * FROM dl.my_table;
```

Backfill or recompute stats for specific columns of any DuckLake table (not just ones this
package created):

```python
from delta2ducklake.ducklake.stats_refresh import refresh_stats

refresh_stats(catalog, "my_table", columns=["a_wide_table_column_delta_never_indexed"])
```

## CLI

The same operations are available as `delta2ducklake <command>` (installed as a console script):

```bash
delta2ducklake bootstrap --catalog sqlite:catalog.sqlite --data-path ./ducklake_data/
delta2ducklake copy   /path/to/my_delta_table --catalog sqlite:catalog.sqlite --table my_table
delta2ducklake sync   /path/to/my_delta_table --catalog sqlite:catalog.sqlite --table my_table
delta2ducklake refresh-stats --catalog sqlite:catalog.sqlite --table my_table --columns a,b

# Postgres: --catalog "postgres:host=localhost dbname=mydb user=me password=..."

# Azure Postgres with an Entra ID token instead of a static password:
delta2ducklake copy /path/to/my_delta_table \
  --catalog "postgres:host=myserver.postgres.database.azure.com dbname=mydb" \
  --entra-user app@mydb --table my_table
```

Run `delta2ducklake <command> --help` for the full option list (e.g. `--schema`, `--version` for
time travel).

## Development

```bash
uv sync --all-extras --all-groups
uv run pytest
uv run ruff check src tests
uv run ty check src/delta2ducklake
```

Postgres-backed tests are skipped unless `DELTA2DUCKLAKE_TEST_PG_DSN` is set to a libpq
keyword=value connection string for a database the test user can create/drop databases in (each
test provisions and tears down its own isolated database):

```bash
export DELTA2DUCKLAKE_TEST_PG_DSN="host=localhost port=5432 user=postgres password=..."
uv run pytest
```

CI (`.github/workflows/python-test.yml`) runs lint, `ty check`, and the full test suite (including
Postgres, against a `postgres:` service container) on every push/PR. Publishing
(`.github/workflows/python-publish.yml`) runs on GitHub Release (or manual dispatch) and pushes to
PyPI via [Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC, no stored token) —
set that up once in the PyPI project's settings before cutting a release.
