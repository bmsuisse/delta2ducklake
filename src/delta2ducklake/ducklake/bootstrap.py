"""Create (or verify) a DuckLake catalog's metadata schema.

Rather than hand-transcribing DuckLake's 28-table bootstrap DDL (a real risk of transcription
error or drift as the DuckLake format evolves), this delegates to DuckDB's own `ducklake`
extension: a plain `ATTACH 'ducklake:...' AS x (DATA_PATH '...')` against an empty/missing catalog
file creates the full schema plus the initial snapshot (id 0) and default "main" schema, using
whatever backend-specific type choices DuckDB itself makes (confirmed by inspection: e.g. a SQLite
catalog gets `BOOLEAN` columns declared as `BIGINT`, `UUID` as `VARCHAR` -- DuckDB's own choice for
that backend, not something delta2ducklake should second-guess by hardcoding a generic script).

Re-running this against an already-bootstrapped catalog is a no-op (confirmed empirically: no
duplicate snapshot/schema rows are created), so callers can call it unconditionally before use.
"""

from __future__ import annotations

import duckdb

from delta2ducklake.ducklake.catalog import CatalogConfig


def bootstrap_catalog(config: CatalogConfig, data_path: str) -> None:
    """Ensure the DuckLake catalog described by `config` is initialized, with data files rooted
    at `data_path` (only takes effect the first time; subsequent calls are a no-op).
    """
    attach_url = config.attach_url().replace("'", "''")
    quoted_data_path = data_path.replace("'", "''")
    con = duckdb.connect()
    try:
        con.sql("INSTALL ducklake")
        con.sql(f"ATTACH '{attach_url}' AS bootstrap_target (DATA_PATH '{quoted_data_path}')")
    finally:
        con.close()
