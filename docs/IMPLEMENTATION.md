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

## ducklake/bootstrap.py and ducklake/catalog.py

**Design change from the original plan, made after empirical probing.** The plan called for
hand-transcribing DuckLake's published bootstrap DDL script (28 `CREATE TABLE` statements) and
running it verbatim against SQLite/Postgres. Before doing that, `INSTALL ducklake; ATTACH
'ducklake:sqlite:...' AS x (DATA_PATH '...')` was tried directly against a real DuckDB connection
(network access to the extension repository is available in this environment) to see exactly what
a fresh catalog looks like — and it revealed the hand-transcribed approach would have been *wrong*
in a subtle way: DuckDB's own SQLite writer declares `BOOLEAN` and `UUID`-typed spec columns as
`BIGINT`/`VARCHAR` in the actual `CREATE TABLE` it emits (e.g. `ducklake_data_file.path_is_relative`
is `BIGINT` in the real SQLite catalog, not `BOOLEAN` as the published spec script literally shows).
Hardcoding the spec's generic script would have created a schema *shaped* like DuckLake but not
*byte-identical* to what DuckDB itself produces and expects — a correctness risk with no upside,
plus a maintenance burden every time the DuckLake format version bumps.

So `bootstrap_catalog()` just delegates entirely to the real extension instead of shipping any DDL
of our own: `ATTACH` against an empty/missing catalog file creates the full 28-table schema, the
initial snapshot (`snapshot_id = 0`, `next_catalog_id = 1`), and a default `"main"`
`ducklake_schema` row, using whatever backend-specific types the installed DuckLake version wants —
guaranteed self-consistent with what will later read it back, and automatically correct across
format versions since it's not a snapshot frozen in our own source. Confirmed empirically:
re-running `ATTACH` against an already-bootstrapped catalog is a no-op (no duplicate snapshot/schema
rows), so `bootstrap_catalog()` is safe to call unconditionally before use. This also directly
verified the exact bootstrap `ducklake_metadata` values (`version = "1.0"`, `created_by = "DuckDB
<hash>"`, `data_path`, `encrypted = "false"`) rather than guessing from prose.

After bootstrap, all of delta2ducklake's *own* reads/writes (registering data files, stats,
partitions — the actual value-add, since DuckDB's SQL surface has no way to register a pre-existing
external Parquet file into a DuckLake table without copying it) go through `catalog.py`'s
`CatalogBackend` protocol directly via the stdlib `sqlite3` module or `psycopg` (v3) — not through
DuckDB — for full control over snapshot/file-ID bookkeeping and to avoid a DuckDB connection/locking
dependency on the hot path. SQL is authored once using SQLite's native `?` placeholder;
`PostgresCatalog` does a blind `?` -> `%s` string translation, safe here because delta2ducklake only
ever runs its own static SQL templates, never SQL built from external input — so far this has been
enough ANSI-compatible SQL that `sqlglot` hasn't been needed at all (kept as a dependency for if a
real dialect incompatibility surfaces later, per the original plan).

Postgres-backed tests are skipped unless `DELTA2DUCKLAKE_TEST_PG_DSN` is set — a Postgres cluster
happens to be running in this dev environment already (on a nonstandard port, no known credentials
supplied), so no attempt was made to guess into it; the skip path was exercised instead.

## Testing approach so far

All tests run against real vendored fixtures (see `tests/fixtures/NOTICE` for provenance), not
hand-rolled synthetic Delta logs — deliberately, since the log-replay bug above was only caught
because a real fixture's actual commit-0 content contradicted the (buggy) implementation's
behavior. `uv run pytest` — 73 passing, 1 skipped (Postgres, no DSN configured) as of this module set.

## ducklake/model.py, ducklake/writer.py, convert.py

`writer.py` is where the schema/stats/log-replay pieces actually turn into `ducklake_*` rows;
`convert.py` is the public `copy_table()`/`sync_table()` orchestration on top of it. A few design
points worth recording:

**Column flattening and IDs.** `flatten_schema()` walks the `StructField` tree and gives *every*
node — including `struct`/`list`/`map` container columns themselves, not just leaves — its own
`ducklake_column` row and a fresh `column_id`, matching DuckLake's nested-column model
(`parent_column` links a child to its container). IDs for columns/tables/schemas/partitions all
come from one shared counter (`IdAllocator.alloc_catalog_id()`), separate from the
`alloc_file_id()` counter for data/delete files — mirroring `ducklake_snapshot`'s own
`next_catalog_id`/`next_file_id` split. `list`/`map` element/key/value types don't get their own
named child columns yet (Delta's schema JSON doesn't name them and phase 1 doesn't need per-element
pruning) — schema/row-count correctness is unaffected, only stats on array/map elements would be
missing; noted as a gap rather than silently wrong.

**Table path, never copying data.** `ducklake_table.path` is set to the Delta table's own root
(absolute, `path_is_relative = False`); every `ducklake_data_file.path` is then just Delta's own
relative `add.path` underneath it (`path_is_relative = True`). Physical file location never
changes — this is the whole point of the project.

**Stats aggregation, typed until the last step.** `ColumnStatsAccumulator` keeps running min/max in
native Python types (see `delta/stats.py`'s note on why), only calling `encode_ducklake_stat` once,
at the point of writing the final `ducklake_table_column_stats` row. Verified end-to-end against
`parquet-all-types`'s real per-file stats (`nullCount`, not a guess) — e.g. `ByteType` really has 3
nulls out of 200 rows in the source data, so `contains_null` must come out `True`; `BooleanType`/
`BinaryType` get no min/max from this particular writer at all, so those must stay `NULL`, not some
default. Both checked directly against the fixture's own `add.stats` JSON before writing the test.

**Bootstrap is a separate step.** `copy_table()`/`sync_table()` deliberately do *not* call
`bootstrap_catalog()` themselves — that's left to the caller (or the future CLI) as an explicit,
one-time action, so it's never silently re-run as a side effect of a routine copy/sync call.

**`sync_table`'s bookkeeping.** Three scoped `ducklake_metadata` rows per table (`scope='table'`)
record the Delta source path, the last-synced Delta version, and a hash of `schemaString`.
Schema-evolution during sync is out of scope for phase 1 — detected via the schema hash and raised
as `NotImplementedError` with a clear message, rather than attempting a partial/best-effort merge.

**A second real bug, found by testing against `snapshot-data3` and a synthetic "path reused across
transactions" case:** the first `sync_table` draft diffed *only* the before/after active-file path
sets. That's wrong whenever a path is active both before and after the sync window but was actually
removed-and-re-added *within* it (a deletion-vector update in real data, or — as the vendored
`delete-re-add-same-file-different-transactions` fixture specifically exists to test — literally the
same path reused across two separate transactions): a plain set diff sees "still active" and does
nothing, silently keeping the *stale* registration. Fixed by adding
`delta.state.touched_paths_since()`, which scans the action range since the last sync for any
path touched by an add *or* remove, and folding that into the diff: a path active both before and
after but touched in between is retired *and* re-registered, not left alone. That fixture has no
real backing Parquet files (it's a log-only conformance fixture upstream), so the test materializes
trivial ones itself via `duckdb.sql("COPY (SELECT ...) TO ... (FORMAT PARQUET)")` to exercise the
full pipeline, including the stats-less-file record-count fallback.

**Record count/file size are corrected on removal, not just added to on insert.** A related fix:
the first draft only ever added new files' `record_count`/`file_size_bytes` to the table's running
totals, never subtracting a removed (or changed-in-place, i.e. retired-and-replaced) file's
contribution — silent drift on every deletion. Fixed by reading the removed files' own
`ducklake_data_file.record_count`/`file_size_bytes` before retiring them and subtracting that from
the baseline. (DuckLake's own spec explicitly allows these counts to be approximate, and says
deletions don't require *stats bounds* to be updated at all since bounds only need to be valid, not
tight — but letting counts silently drift arbitrarily far from the truth over many sync cycles
seemed like bad practice worth the small extra query, even where the spec would tolerate it.)

Verified end-to-end against `snapshot-data3` (a real multi-version table: `copy_table` at v1 → 4
files/20 rows, `sync_table` to v2 → 2 files/10 rows with the other 4 correctly tombstoned via
`end_snapshot` rather than deleted outright, `sync_table` to v3 → 4 files/30 rows) and the
synthetic changed-in-place case above (old `data_file_id` retired, new one allocated, record count
not double-counted).
