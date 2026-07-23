# delta2ducklake

Register a [Delta Lake](https://delta.io) table into a [DuckLake](https://ducklake.select) catalog
**without copying or rewriting the underlying Parquet files**. Reads the Delta transaction log
directly (JSON commits + checkpoints), no dependency on the `deltalake` Python package.

Status: early development, not yet published.

## Supported

- Catalog backends: SQLite, PostgreSQL
- Storage backends: local filesystem, Azure Blob/ADLS (`delta2ducklake[azure]`)
- Full and incremental (`copy_table` / `sync_table`) conversion, including table/file statistics
  and Hive-style partitioning
- Column mapping and deletion vectors (in progress)

## Development

```bash
uv sync --all-extras
uv run pytest
```
