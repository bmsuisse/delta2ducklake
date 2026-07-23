# Implementation notes

This documents how `delta2ducklake` actually works, as it's built — not a design proposal, a record
of decisions made and why. See `README.md` for user-facing usage; this is for maintainers.

Overall approach: read a Delta table's transaction log directly (no `deltalake` Python dependency),
reconstruct the set of currently-active Parquet files + schema, then register that same set of
files into a DuckLake catalog (SQLite or Postgres) — never copying or rewriting the Parquet data
itself.

## Module map

```
storage.py            local filesystem + Azure Blob/ADLS Gen2 byte-level reads
delta/actions.py       dataclasses for add/remove/metaData/protocol/sidecar/commitInfo
delta/schema.py        Delta schemaString -> type tree, Delta type -> DuckLake type name
delta/log.py           _delta_log reader: JSON commits + checkpoints (classic/multi-part/V2)
delta/state.py         replay actions -> active files + current schema/protocol at a version
delta/stats.py         add.stats -> typed per-column stats, DuckLake string encoding
ducklake/...           (not yet built) catalog backends, bootstrap DDL, writer, stats_refresh
convert.py             (not yet built) copy_table() / sync_table() public API
```

## storage.py

A deliberately tiny `StorageBackend` protocol (`read_bytes`, `read_range`, `list_dir`, `exists`,
`resolve`) with `LocalStorageBackend` (stdlib `pathlib`/`open`) and `AzureStorageBackend`
(`azure-storage-blob`, lazily imported so the `azure` extra stays optional). No `fsspec` — the user
explicitly wanted local + Azure only, talking to Azure directly via the official SDK.

`resolve(table_root, relative_path)` centralizes one subtlety: Delta's `add.path` values are
percent-encoded (RFC 2396-ish) relative paths (e.g. a Hive partition directory with a space or `=`
in the value gets `%20`/`%3D`). Both backends decode via `urllib.parse.unquote` before touching the
actual filesystem/blob path.

`AzureStorageBackend` accepts either `https://<account>.blob.core.windows.net/<container>/<path>`
or `abfss://<container>@<account>.dfs.core.windows.net/<path>` (ADLS Gen2) — both are normalized to
the same blob container client since they address the same underlying storage.

## delta/actions.py

Frozen dataclasses for the action kinds we care about: `AddAction`, `RemoveAction`, `MetaData`,
`Protocol`, `SidecarAction` (V2 checkpoints), `CommitInfo`, plus `DeletionVectorDescriptor` for
`add.deletionVector`. `parse_action(line: Mapping)` dispatches on whichever single key is present.

Deliberately accepts any `Mapping`, not just `dict` from `json.loads` — this is what lets the exact
same parsing code handle a JSON commit line *and* a DuckDB struct-as-dict row read out of a
checkpoint Parquet file (confirmed empirically: DuckDB's Python client converts Parquet `STRUCT` ->
`dict`, `MAP` -> `dict`, `LIST` -> `list` on fetch, so a checkpoint row's `add`/`remove`/... columns
arrive in exactly the same shape as `json.loads`'d JSON).

`txn`, `domainMetadata`, `cdc` action kinds are recognized but ignored (return `None`) — none of
them affect which files are active or what the schema is.

## delta/schema.py

Parses `metaData.schemaString` (Spark's JSON schema format) into a small recursive type tree:
`PrimitiveType | ArrayType | MapType | StructType`, with `StructField` carrying `nullable` plus,
when column mapping is on, `physical_name`/`column_mapping_id` pulled out of the field's
`metadata.delta.columnMapping.*` keys.

`ducklake_primitive_type()` maps Delta primitive type names to DuckLake `column_type` strings per
DuckLake's own `data_types.md`. Two non-obvious mappings, confirmed against the spec:
- Delta `timestamp` (instant/UTC-normalized semantics) -> DuckLake `timestamptz`
- Delta `timestamp_ntz` (no timezone) -> DuckLake plain `timestamp`

`decimal(P, S)` is parsed via regex and re-emitted without the space (`decimal(10,2)`); DuckDB's SQL
type parser doesn't care about the whitespace either way, but the vendored spec examples don't
include one.

## delta/log.py

No dependency on `deltalake`. Two file kinds:

- **Commits** (`_delta_log/<v>.json`): newline-delimited JSON, one action object per line.
- **Checkpoints**: Parquet, read via `duckdb.sql("SELECT * FROM read_parquet(...)")`. Local files
  are handed to DuckDB directly (zero-copy); for any other backend (Azure), bytes are fetched
  through `storage` and staged into a temp file first (checkpoints are small metadata files, this
  doesn't conflict with "never copy the actual data Parquet files").

Checkpoint discovery reads `_delta_log/_last_checkpoint` first. Three checkpoint shapes, confirmed
against real vendored fixtures:

1. **Classic single**: `{"version": V, "size": N}` -> `<V>.checkpoint.parquet`.
2. **Classic multi-part**: adds `"parts": N` -> `<V>.checkpoint.<i>.<N>.parquet` for `i` in `1..N`.
3. **V2** (`vendored table v2-checkpoint-{parquet,json}`): `_last_checkpoint` gains a `"v2Checkpoint"`
   key that **already inlines everything needed** — `nonFileActions` (protocol/metaData/
   checkpointMetadata) and `sidecarFiles` (the Parquet files under `_delta_log/_sidecars/` holding
   the actual `add`/`remove` rows). So V2 checkpoints never need to open the top-level
   UUID-named checkpoint file itself in the common case — `_last_checkpoint` already has the
   answer. (A from-scratch-listing fallback, used when `_last_checkpoint` is missing/corrupt, only
   handles the classic shapes; V2 tables with a missing `_last_checkpoint` are an accepted gap for
   now — `corrupted-last-checkpoint*` fixtures are explicitly marked stretch/non-blocking in the
   plan.)

`iter_versions(storage, table_root, end_version=None)` is the public entry point: yields
`(version, actions)` in strictly increasing order — the checkpoint's squashed state first (under
its own version number) if one is usable for the requested `end_version`, then each subsequent
commit file's actions individually, up to `end_version` or the latest commit found.

**Bug found and fixed during testing**: the first implementation used `0` as the sentinel for "no
checkpoint," which is indistinguishable from "checkpoint at version 0" — on any table with no
checkpoint at all, this silently filtered out the version-0 commit (`v > checkpoint_version` with
`checkpoint_version = 0` excludes `v = 0`). Fixed by using `-1` as the no-checkpoint sentinel.
Caught by `test_state.py::test_column_mapping_table_raises_by_default` and friends failing with
"No metaData action found" against tables that visibly have one in their `00000000000000000000.json`
— a good example of why the plan called for testing against real fixtures rather than synthetic
ones only.

## delta/state.py

`load_table_state()` replays the action stream from `iter_versions` into a `DeltaTableState`
(current `metadata`, `protocol`, and `active_files: dict[path, AddAction]`) as of a version.

Per version, **removes are applied before adds**: `for r in removes: active_files.pop(r.path,
None)` then `for a in adds: active_files[a.path] = a`. This matters for the real pattern seen in the
vendored `table-with-dv-small` fixture: a single commit does `remove(path)` (tombstoning the old
entry, no DV) immediately followed by `add(path)` again with a *new* `deletionVector` attached —
i.e. "update this file's deletion vector." Remove-then-add nets out to the new (DV-bearing) add
winning, which is correct. Same logic also handles the rarer "same path reused across two
*different* transactions" case (see the vendored `delete-re-add-same-file-different-transactions`
fixture: v0 adds `foo`, v1 removes `foo`, v2 re-adds `foo`, v3 adds `bar`) since each version is
processed independently in commit order.

`UnsupportedTableFeatureError` (subclass of `NotImplementedError`) is raised — after the full
replay, so we know the table's real final schema/active-file state — if `metadata.
column_mapping_mode != "none"` or any active file has a `deletion_vector`, unless the caller passes
`allow_column_mapping=True` / `allow_deletion_vectors=True`. Phase 1's `convert.py` will call this
with both flags `False`, so unsupported tables fail loudly with a clear message pointing at the
relevant phase, rather than silently producing a DuckLake catalog with wrong physical-name-vs-
logical-name stats or missing deletes. Verified against `dv-with-columnmapping` (both features at
once) that each flag is independently required.

## delta/stats.py

Two separate concerns, kept apart deliberately:

1. `parse_add_stats` / `iter_leaf_column_stats`: turn `add.stats` (a JSON string covering
   `numRecords`/`minValues`/`maxValues`/`nullCount`) plus the parsed schema into one
   `LeafColumnStats` per **primitive** column, recursing into `struct` sub-fields the same way
   DuckLake does (confirmed against the real `data-reader-nested-struct` fixture: stats for `a.ac.aca`
   live at `minValues.a.ac.aca` in Delta's JSON, mirroring the schema's nesting exactly). `list`/`map`
   columns get no stats at all in Delta (same as DuckLake), so they're skipped rather than guessed at.
2. `encode_ducklake_stat`: pure value -> string encoder per DuckLake's `data_types.md` encoding
   table, keyed off the *Delta* primitive type name (which maps 1:1 to how DuckLake wants it encoded).

Values are kept as native Python objects (int/float/str) through the whole leaf-stats walk and only
turned into DuckLake's VARCHAR encoding at the very last step — this matters because DuckLake's own
spec explicitly warns that comparing the encoded *strings* directly is wrong (`"10" < "9"`
lexicographically); aggregating across files (done later in `ducklake/writer.py`) must compare the
typed values, not the encoded ones.

Two formats needed empirical confirmation rather than guessing from the prose spec (checked against
the real `data-skipping-basic-stats-all-types` fixture):
- Delta already emits `date` stats as bare ISO dates (`"2000-01-01"`) — passes straight through.
- Delta emits `timestamp` stats as `"2000-01-01T00:00:00.000-08:00"` (`T` separator, milliseconds,
  colon-in-offset). DuckLake's own docs show space-separator/microseconds/no-colon-offset
  (`"2024-01-15 12:30:00.123456+00"`), but since the target read path is `CAST(varchar AS
  TIMESTAMPTZ)` rather than a byte-exact match, any valid ISO-8601 variant works — reformatted via
  `datetime.fromisoformat` + `isoformat(sep=" ")` rather than chasing DuckDB's exact self-serialization.

`contains_nan`: Delta's protocol has no dedicated NaN flag in `add.stats` at all (unlike DuckLake's
`ducklake_file_column_stats.contains_nan`). `encode_ducklake_stat` returns `(None, True)` when a
min/max value is literally `NaN` (Python's `json.loads` parses the non-standard bare `NaN`/
`Infinity`/`-Infinity` tokens Spark's JSON writer emits, so this works with no special-casing at
parse time) — the caller treats that as "no bound from this file, but note a NaN was seen," which is
the best information-preserving choice given Delta doesn't tell us more.

`read_parquet_record_count` is the fallback for the (rare) case where `add.stats` is `None` entirely
— reads only the Parquet footer's row-group metadata via DuckDB's `parquet_file_metadata()` table
function, never the actual row data, confirmed against a real 200-row fixture file.

## Testing approach so far

All tests run against real vendored fixtures (see `tests/fixtures/NOTICE` for provenance), not
hand-rolled synthetic Delta logs — deliberately, since the log-replay bug above was only caught
because a real fixture's actual commit-0 content contradicted the (buggy) implementation's
behavior. `uv run pytest` — 66 passing as of this module set, 0 skipped.
