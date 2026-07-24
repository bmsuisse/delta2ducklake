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

`to_duckdb_uri(path)` handles a separate concern: Databricks/Spark hand out that same `abfss://`
form with the *container* in the netloc (`abfss://container@account.dfs.core.windows.net/...`), but
DuckDB's own `azure` extension only understands the *account* in the netloc
(`abfss://account.dfs.core.windows.net/container/...`). `copy_table` runs `delta_table_root` through
this before storing it as `ducklake_table.path`, since that value is read back later by DuckDB
itself (via `ducklake`+`azure`), not by delta2ducklake's own `AzureStorageBackend` (which parses both
forms fine and is left untouched everywhere else — reading the source table, the bookkeeping
source-path check, etc.).

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

## ducklake/stats_refresh.py

The standalone "extend/recompute stats for given columns" utility, requested explicitly rather than
inferred: `refresh_stats(catalog_config, table_name, columns=None)` scans a table's already-
registered Parquet files directly via DuckDB and (re)writes `ducklake_file_column_stats`/
`ducklake_table_column_stats` — independent of Delta entirely, so it works on any DuckLake table,
not just ones `copy_table()` produced. Its main real-world use: Delta's own
`dataSkippingNumIndexedCols` setting caps how many columns get per-file stats at write time, so
trailing columns in a wide table are frequently missing stats altogether even in a
perfectly-normal, unmodified Delta table — this fills them in from the actual data.

Encoding here needed its own small sibling to `delta/stats.py`'s `encode_ducklake_stat`:
`_encode_value()` operates on values DuckDB itself returns from `read_parquet()` (real
`datetime.date`/`datetime.datetime`/`decimal.Decimal`/`bytes`/`uuid.UUID` objects), not Delta's
JSON-decoded stats values — different input domain, identical output format, so keeping them as two
small functions was clearer than forcing one function to handle both. One column-count aggregate
query per data file computes `count(*)`, `count(col)`, `min(col)`, `max(col)`, and (float columns
only — `isnan()` errors on non-float types in DuckDB) a NaN check for every requested column at
once, rather than one round trip per column.

Verified two ways against `parquet-all-types` (already `copy_table()`-registered): (1) re-running
`refresh_stats()` over columns Delta *did* already collect stats for (`ByteType`, `IntegerType`,
`StringType`, `DateType`) reproduces byte-identical `contains_null`/`min_value`/`max_value` to what
`copy_table()` got straight from Delta's own `add.stats` — a good independent cross-check that both
paths agree on the same ground truth. (`TimestampType` was deliberately excluded from this
particular check: DuckDB normalizes `TIMESTAMPTZ` values it reads back to a canonical offset, which
can legitimately differ in *string form* from whatever offset Delta's writer originally recorded for
the same instant — both representations are valid, they just don't compare equal as strings, so
that's not a fair byte-for-byte check.) (2) `refresh_stats(columns=["BooleanType"])` correctly
backfills real bounds (`"0"`/`"1"`) for the one column this fixture's writer collected *no* stats
for at all, without touching any other column's existing stats — the actual "Delta never collected
this" scenario the utility exists for.

## Real-DuckDB verification (`tests/test_verify_duckdb.py`) — two more bugs found

Every prior test validated delta2ducklake against its own SQL queries — the same kind of
self-referential check that let the earlier log-replay bug slip through unit tests until a *real*
fixture's content contradicted it. So before calling phase 1 done, a `copy_table()`-produced
catalog was read back through DuckDB's actual `ducklake` extension (`INSTALL ducklake; ATTACH
'ducklake:sqlite:...'`) and cross-checked row-for-row (`EXCEPT` both directions, not just counts)
against the same source table read through DuckDB's actual `delta` extension (`delta_scan`) — an
independent reader on both sides of the comparison, neither of them code this project wrote. This
immediately surfaced two real bugs that 85 passing self-consistent tests had completely missed:

**Bug 1 — `list`/`map` columns need child rows, or DuckDB's reader crashes.** The first version of
`flatten_schema()` gave `struct` columns their field children but left `list`/`map` columns as bare
leaf-like rows with no children at all (reasoned at the time to be an acceptable gap since "phase 1
doesn't need per-element stats"). Attaching a real catalog built that way and reading a `map`
column crashed DuckDB with an internal assertion failure (`Attempting to dereference an optional
pointer that is not set`) inside `DuckLakeCatalog::LoadSchemaForSnapshot` — not a graceful error, a
crash, on ordinary data. Fixed by synthesizing the child columns DuckLake's own nested-type model
expects: a `list` gets one child named `"element"`, a `map` gets two named `"key"`/`"value"`
(exactly the naming used in DuckLake's own `data_types.md` nested-type example), recursively for
arbitrarily nested combinations (list-of-list-of-struct, map-of-list, etc). `struct`/`list` columns
turned out to already read back correctly even *before* this fix (confirmed by testing each nested
column in isolation) — only `map` actually crashed — but the fix was applied uniformly since the
schema/row-count cost of the extra rows is negligible and "some nested container types get
children, others don't" isn't a distinction worth maintaining.

**Bug 2 — `ducklake_table.path` needs a trailing slash; DuckLake does not insert a separator.**
`create_table()` originally stored the Delta table root exactly as given (e.g.
`".../data-reader-primitives"`, no trailing `/`). Reading a file back through the real extension
failed with a plain `IOException: Cannot open file ".../data-reader-primitivespart-00000-...
parquet"` — table path and relative file path concatenated with **no separator at all**. Confirmed
against bootstrap's own convention (the "main" schema it creates is stored as `"main/"`, trailing
slash included) and fixed by normalizing every table path to end in `/` before storing.

**Bug 3 (the significant one) — `map_by_name` column mapping is not a phase-2-only concern.** After
fixing bug 1, a *third* issue surfaced reading `map` columns specifically: a real, valid-looking
DuckDB error (`'key' of MAP did not map to a value and the registered DEFAULT is NULL, which is not
allowed`) — the same error you'd get from constructing a literal `MAP` with a null key in plain SQL,
despite the underlying data being an all-NULL map column with nothing resembling a null key.
`struct`/`list` columns read back fine without any `ducklake_column_mapping` row at all; only `map`
broke. The working theory (confirmed by manually patching a `map_by_name` mapping into the SQLite
catalog by hand and watching the exact same failing query succeed): Parquet's own physical
encoding of a `MAP` always inserts an implicit `key_value` repeated-group wrapper between the map
and its key/value children (confirmed via `parquet_schema()` on the raw file) that DuckLake's
2-level *logical* model (map → key, value directly, matching its own documented nested-type
example) doesn't mirror — and without either embedded Parquet field-ids (absent here, confirmed via
`parquet_schema()`: every column shows `field_id: None`, the normal case for a plain Delta/Spark
writer) or an explicit name-based mapping telling DuckLake how to bridge that wrapper, resolving a
`map` column's data is ambiguous enough to produce a genuine misread, not just a missing
optimization.

This means `ducklake_column_mapping`/`ducklake_name_mapping` are **not** an optional feature that
only matters for Delta's own `columnMapping.mode = name`/`id` (the original phase-2 scope) — they
are required, in practice, for essentially every table converted from a plain Delta/Spark writer,
because such writers don't embed Parquet field-ids by default. Phase 1 now creates one
`map_by_name` mapping unconditionally for every table `create_table()` registers (covering every
column, container and leaf alike, with `is_partition` set for partition columns per the spec),
regardless of whether the source Delta table itself uses column mapping — that Delta-side feature
(physical vs. logical *names* differing, per Delta's own `delta.columnMapping.mode`) remains a
distinct, separate phase-2 concern layered on top of this baseline mechanism, not a prerequisite
for it.

**End state**: `parquet-all-types` (13 primitive types + nested struct/list/map/map-of-list) now
matches `delta_scan` **byte-for-byte** across all 20 columns and 200 rows (`EXCEPT` both directions
→ 0 rows), including stats-based file pruning returning identical filtered results. 12 fixtures
covering flat/wide primitives, nested types, partitioning, multi-file tables, decimals, and all
three checkpoint shapes (classic multi-part, V2 JSON, V2 Parquet) pass the same row-for-row check
in `tests/test_verify_duckdb.py`, with one narrow, documented exception: `data-reader-partition-
values`' 12-level nested Hive path (one level being a colon-containing timestamp) trips a
`delta_scan`-side quirk when combined into a multi-relation query — reproduced with a fresh
connection and no ducklake catalog involved at all, so it's DuckDB's `delta` extension, not this
project; row *count* is still checked for that fixture, and partitioning logic itself has its own
dedicated non-DuckDB unit test.

## Phase 2 — column mapping

Delta's `columnMapping.mode = name`/`id` means the *physical* Parquet field name differs from the
*logical* column name a query uses — e.g. a real Databricks-written field named `"Company Very
Short"` physically stored as `col-173b4db9-b5ad-427f-9e75-516aae37fbbb` (confirmed against the real
vendored `table_with_column_mapping` fixture, and its own `delta.columnMapping.physicalName` field
metadata, already captured on `StructField.physical_name` by `delta/schema.py` back in phase 1).

Given phase 1 already required a `map_by_name` `ducklake_column_mapping` unconditionally for every
table (see the phase-1 writeup above — Bug 3), phase 2 turned out to be a small, targeted change
rather than a new mechanism: everywhere a value needs to be looked up by the Parquet field's *real*
on-disk name rather than the logical name a query sees, swap in `physical_name or name`:

- `delta/stats.py::iter_leaf_column_stats` looks up `minValues`/`maxValues`/`nullCount` from
  `add.stats` by `f.physical_name or f.name` instead of always `f.name` (`LeafColumnStats.path`
  itself stays logical-name-keyed throughout, since that's what resolves to `column_id` — only the
  *JSON key* used to pull the raw value out of Delta's stats needs the physical name).
- `ducklake/writer.py::create_name_mapping`'s `source_name` is `c.physical_name or c.name` (was
  unconditionally `c.name`) — `FlattenedColumn` grew a `physical_name` field, threaded through
  `flatten_schema`'s recursion from each `StructField.physical_name` (synthetic `list`/`map`
  children — `"element"`/`"key"`/`"value"` — never have one of their own: Delta's column mapping
  never renames array elements or map keys/values individually, so the Parquet convention name is
  used regardless of mapping mode).
- `convert.py::_partition_physical_names` resolves each logical partition column name to its
  physical Parquet field name once per `copy_table()`/`sync_table()` call, since `add.partitionValues`
  is keyed by physical name too (same fixture, confirmed).
- `load_table_state(..., allow_column_mapping=True)` is now the default inside `copy_table()`/
  `sync_table()` — the phase-1 `UnsupportedTableFeatureError` guard in `delta/state.py` stays as
  written (still gates on `allow_deletion_vectors` for phase 3), it's just no longer tripped by
  column mapping specifically.

Verified against two real fixtures: `table_with_column_mapping` (delta-rs, mode=`name`, partitioned,
real Databricks-written stats/partition values) via direct SQL inspection — confirmed
`ducklake_column.column_name` stays logical ("Company Very Short") while
`ducklake_name_mapping.source_name` is the physical UUID name, partition values resolve to the
correct `"BMS"`/`"BME"` strings (not garbage from a logical-name lookup miss), and per-column stats
land on the right `column_id` despite being keyed by physical name in the source JSON. And, more
thoroughly, `table-with-columnmapping-mode-name` / `-mode-id` (delta-io, unpartitioned but with a
genuinely complex nested schema — `map_of_maps`, `struct_of_arrays_maps_of_structs`,
`array_of_map_of_arrays`) both match `delta_scan` **byte-for-byte** through the real DuckDB
`ducklake` extension (0 rows either direction via `EXCEPT`), added to `test_verify_duckdb.py`.

**A real, separate limitation found along the way (not a column-mapping bug, but surfaced while
testing one): DuckDB's own `ducklake` reader requires Hive-style directory paths to materialize
partition column values, no matter what's in `ducklake_file_partition_value`.** Discovered when
`table_with_column_mapping` (real Databricks output, physically laid out with opaque directory
names — `BH/`, `8v/` — instead of the usual `col=value/` convention) failed reading *any* column,
not just the partition one, with `Column "..." should have been read from hive partitions - but it
was not found in filename`. This means `ducklake_file_partition_value` — despite being, per its own
spec, the *authoritative* record of a file's partition values — is not actually consulted by the
real reader for that purpose; it parses the Hive path segment itself instead. For a table whose
physical layout doesn't follow that convention, there is no correct catalog metadata that fixes
this without physically relocating the files (moving them into `col=value/` directories) -- so by
default `copy_table()`/`sync_table()` refuse such a table outright (`PartitionLayoutError`) rather
than register a catalog DuckDB can't actually read back. This table's *catalog* correctness (name
mapping, partition values, stats — all independently confirmed correct via direct SQL above) is
real; what wasn't achievable, until `materialize_partitions` below, was reading it back through
DuckDB's `ducklake` extension specifically, given its physical layout. Separately, this also
explained a second failure on `data-reader-timestamp_ntz-name-mode` (Hive-style, physical name in
path, but one partition value contains a colon): the *same* double-percent-encoding quirk already
documented above for `delta_scan` also affects the `ducklake` reader's own Hive-path parsing --
confirming it's a shared, pre-existing DuckDB-side encoding issue with colon-containing Hive
partition values, not something introduced by column mapping or specific to one extension.

**`materialize_partitions`: the one deliberate, opt-in exception to "never copy or rewrite the
data."** `delta.partition_layout.is_hive_style_layout` checks each file's own path against its
partition columns' physical names (`name=` prefix only, not exact-value match -- replicating
Delta's own escaping byte-for-byte isn't needed just to detect the opaque-directory failure mode).
When that check fails and the caller passed `materialize_partitions="auto"` (copy into the DuckLake
catalog's own `data_path`) or a directory/URI (copy there instead), `convert._materialize_partitioned_file`
copies the file's bytes verbatim into a *new*, genuine `physical_name=value/.../file.parquet` Hive
path it builds itself (`delta.partition_layout.hive_path_segments`), and that copy's absolute path
is what gets registered in `ducklake_data_file` (`path_is_relative = False`) instead of the source's
own path. `encode_hive_value` replicates Delta/Hive's own on-disk escaping (percent-encode
everything except a literal space -- confirmed against `data-reader-partition-values`'s real
`as_timestamp=2021-09-08 11%3A11%3A11` folder name) rather than inventing a new scheme, since that's
the exact encoding already proven to round-trip through the real `ducklake` reader. Verified
end-to-end (not just "the catalog rows look right"): `test_materialize_partitions_readable_through_real_ducklake_extension`
in `test_verify_duckdb.py` attaches the resulting catalog with real DuckDB and matches it row-for-row
against `delta_scan` on `table_with_column_mapping` -- proof DuckDB's own Hive-partition decoder
actually accepts this project's encoding, not an assumption.

Two things this does *not* solve, left as known scope boundaries (consistent with delete files,
which have the same gap already): materialized copies are never cleaned up when their source file
is later retired by `sync_table` (they just become orphaned bytes under the destination, same as a
superseded delete file already is -- no vacuum exists for either yet), and a materialized file's
original Delta path is tracked in `ducklake_metadata` (key `delta2ducklake.materialized_paths`,
JSON-encoded per table) purely so `sync_table`'s active-file diffing keeps recognizing it as
unchanged -- without that translation, every materialized file would look removed-and-re-added (and
get needlessly re-copied) on every single sync.

## Phase 3 — deletion vectors

Delta's deletion vectors mark rows as logically deleted without physically rewriting the Parquet
file; DuckLake's equivalent is a `ducklake_delete_file` (`format = 'parquet'`, a *positional*
delete file) registered against a `ducklake_data_file`. Converting one to the other means: decode
Delta's DV binary format into a set of deleted row positions, then write a new Parquet file in
DuckLake's own expected shape.

**No RoaringBitmap library dependency.** Delta's deletion vector is a magic number + a portable
64-bit RoaringBitmap (itself built from 32-bit RoaringBitmaps, one per 32-bit "key" bucket), per
`PROTOCOL.md` and the RoaringBitmap project's own `RoaringFormatSpec`. Rather than reaching for a
compiled library (`pyroaring`, wrapping CRoaring) the way the user anticipated might be necessary,
`delta/deletion_vector.py` hand-decodes both this and the **Z85** (RFC 32) encoding used for
`pathOrInlineDv` — both are small, fully-specified, stable binary formats (unlike Parquet, which
absolutely is outsourced to DuckDB), and every byte offset/order was pinned down empirically before
writing a line of the parser:

- Z85 alphabet and decode logic validated against the RFC's own published test vector
  (`"HelloWorld"` <-> a specific 8-byte sequence) *and* against real data: decoding the real
  `table-with-dv-small` fixture's `pathOrInlineDv` reconstructs the UUID `61d16c75-6994-46b7-
  a15b-8b538852e50e` — which is *exactly* the real `deletion_vector_<uuid>.bin` filename already
  sitting in that vendored fixture directory.
- The on-disk DV file wrapper format's byte order (**big-endian** — confirmed by manually walking
  the real 45-byte `.bin` file: byte 0 is the version, `data[offset:offset+4]` big-endian-decodes
  to exactly `sizeInBytes=36` from the same commit's JSON) is a different order than the bitmap
  payload itself (**little-endian**, per `PROTOCOL.md`) — confirmed by checking the bitmap's first
  4 bytes decode to the documented magic number `1681511377` exactly.
- The full parse of that same file's bitmap payload (cookie header ->
  `SERIAL_COOKIE_NO_RUNCONTAINER` -> 1 array container, cardinality 2) walked byte-by-byte by hand
  and decoded to row positions `{0, 9}` — which independently matches that commit's own
  `DELETE ... WHERE value IN (0, 9)` operation parameter recorded in the same commit's
  `commitInfo`. Two unrelated pieces of ground truth (the binary DV and the human-readable delete
  predicate) agreeing is about as strong a correctness signal as a hand-rolled binary parser can
  get before shipping it.

**Positional delete file schema, determined empirically (not from any published DuckLake spec
page).** Had DuckDB's own `ducklake` extension perform a real `DELETE` against a table it manages
and read the resulting file back: `file_path VARCHAR, pos BIGINT`, where `file_path` holds the
data file's *absolute*, already-decoded path. (Along the way: DuckLake's own **data inlining**
feature stores small inserts/deletes directly in the catalog database instead of Parquet files at
all — the first two probe attempts silently produced *no* delete file on disk because the delete
was too small to trigger a real file; had to delete >100-ish rows on a large table to see the real
positional-delete Parquet DuckDB itself writes.)

**Where new delete-file Parquet gets written.** Unlike everything else in this project, this is
genuinely new derived data with no Delta-side equivalent to reference — it has to be written
somewhere. Chose the DuckLake catalog's own managed `data_path` (read from `ducklake_metadata`),
never the Delta table's own directory, consistent with this project only ever *reading* from the
source table. `storage.py` gained a `write_bytes()` method (Local: `Path.write_bytes`, Azure:
`ContainerClient.upload_blob`) for this one purpose. `ducklake/writer.py::
build_positional_delete_parquet()` generates the file via DuckDB itself (writing Parquet, unlike
everywhere else in this project where DuckDB only ever *reads* one) into a local temp file, whose
bytes are then handed to `storage.write_bytes()` — one DuckDB API quirk surfaced here: `COPY ...
TO ?` doesn't bind its destination as a query parameter (parameters silently shifted, converting
a filename string to `BIGINT[]` and erroring) — fixed by building the relation with bound values
via `con.sql(..., params=[...])` and calling `.write_parquet()` on it separately, rather than
parameterizing the `COPY` statement's target path at all.

**Stats/record-count semantics confirmed via the same real-extension probe**: `ducklake_data_file.
record_count` stays at the *physical* (pre-delete) row count, matching Delta's own protocol
requirement ("`numRecords` must be present and accurate... equal to the number of records in the
data file, not the valid records in the logical file" for any file with a DV) -- and, more
surprisingly, `ducklake_table_stats.record_count` *also* stayed at the physical total after a real
2000-row delete out of 20000, not the logical (post-delete) count. This matches the spec's explicit
statement that `DELETE` doesn't require statistics updates at all (bounds only need to stay valid,
not tight) -- so registering deletion vectors needed no changes to any of the existing record-count/
stats accumulation logic from phases 1-2, just the new delete-file registration step alongside it.

**Wiring**: `copy_table()`/`sync_table()` now pass `allow_deletion_vectors=True` to
`load_table_state()`; for any active file whose `add.deletion_vector` is set, `_register_deletion_
vector()` (shared by both entry points) resolves positions, builds and writes the delete Parquet,
and registers it. No special-casing needed in `sync_table`'s diffing logic at all: a DV added to an
existing file is, per Delta's own log semantics, a remove+re-add of that same path -- exactly the
"changed in place" pattern `touched_paths_since()` (added in phase 1 bug-fixing) already detects
and handles by retiring the old registration and creating a fresh one, which naturally picks up the
new deletion vector too. Verified directly: `copy_table()` at `table-with-dv-small`'s v0 (no DV)
registers zero delete files; `sync_table()` to v1 (DV added) registers exactly one.

**Verification**: all four real DV fixtures — `table-with-dv-small` (delta-rs, the hand-decoded
ground truth above), `dv-partitioned-with-checkpoint`, `dv-with-columnmapping` (phases 2 and 3
together), and `log-replay-dv-key-cases` (delta-io) — match `delta_scan` **byte-for-byte** through
the real DuckDB `ducklake` extension (0 rows differing either direction via `EXCEPT`), added to
`test_verify_duckdb.py`. This means two entirely independent deletion-vector implementations (DuckDB
's own `delta_scan` DV logic, and this project's hand-rolled RoaringBitmap decoder + positional-
delete-file writer) agree exactly on which rows survive.

## Additional catalog backends: DuckDB file, Entra ID (Postgres), Quack

**`DuckDBCatalogConfig`/`DuckDBCatalog`** — a local `.duckdb`/`.ducklake` file is DuckLake's own
native catalog format (no `sqlite:`/`postgres:` scheme prefix in the ATTACH string). Confirmed by
inspection that DuckDB's `ducklake` extension, given a bare path, just uses a plain DuckDB database
file to hold the `ducklake_*` metadata tables — connecting to that same file directly via
`duckdb.connect(path)` exposes those tables in the `main` schema, exactly like `SQLiteCatalog`
bypasses the extension and talks to the raw SQLite file directly. One wrinkle: a plain
`duckdb.connect()` autocommits every statement immediately by default (same behavior already found
for `QuackCatalog`), so `DuckDBCatalog` opens an explicit `BEGIN TRANSACTION` after connecting and
re-opens one after every commit/rollback. `DuckDBCatalogConfig.path` also accepts an already-open
`duckdb.DuckDBPyConnection` directly (for in-memory testing or connection reuse) — in that case
`DuckDBCatalog` doesn't open or close the connection itself, since the caller owns its lifecycle;
`attach_url()` (only used by `bootstrap_catalog()`, which needs a real path to ATTACH from a
*separate* connection) raises `ValueError` if given a connection instead of a path.

**Entra ID (Azure AD) auth for `PostgresCatalogConfig`** — mirrors `bmsuisse/pgdevkit`'s pattern
exactly: fetch an AAD access token via `azure-identity` (`DefaultAzureCredential` or
`ManagedIdentityCredential`) and use it directly as the Postgres password, since Azure Database for
PostgreSQL accepts AAD tokens as passwords for a matching Entra-mapped user. `entra_user`/
`managed_identity` fields added to `PostgresCatalogConfig`; a fresh token is fetched on every
`connect()`/`attach_url()` call (not cached on the frozen dataclass) since tokens expire, using
`psycopg.conninfo.make_conninfo()` to overlay `user`/`password` onto the base DSN without needing
to hand-parse libpq keyword=value syntax. Credential objects themselves *are* process-cached
(module-level globals in `azure_auth.py`), matching pgdevkit's approach, so repeated token refreshes
don't re-probe the credential chain. Gated behind the existing `delta2ducklake[azure]` extra (added
`azure-identity`).

**`QuackCatalogConfig`/`QuackCatalog`** — [Quack](https://duckdb.org/quack/) is DuckDB's
experimental (as of v1.5.5) client-server RPC protocol: `CALL quack_serve('quack:host:port',
token=...)` on a server, `ATTACH 'quack:host:port' AS x` on a client. DuckLake is being integrated
with it, so `ATTACH 'ducklake:quack:host:port' AS x (DATA_PATH ...)` works for bootstrapping — added
`prepare_attach()` to every `CatalogConfig` (a no-op for DuckDB-file/SQLite/Postgres) so
`bootstrap_catalog()` can `INSTALL`/`LOAD quack` and register the auth secret on its throwaway
connection before the ATTACH. `QuackCatalog` needs the same explicit-transaction treatment as
`DuckDBCatalog` (plain `duckdb.connect()` autocommits by default).

**Known limitation, confirmed by direct testing against a real running `quack_serve()` instance**:
tables reached via a raw `ATTACH 'quack:...' AS remote` only support `INSERT`/`SELECT`. Both
`UPDATE` and `DELETE` fail at the binder level:
```
UPDATE ducklake_metadata SET value = 'x' WHERE key = 'version'  -- Binder Error: Can only update base table
DELETE FROM ducklake_metadata WHERE key = 'nonexistent_key_xyz'  -- Binder Error: Can only delete from base table
```
reproduced consistently, isolated into separate `BEGIN`/`ROLLBACK` blocks per statement (running
multiple DML statements plus a `SHOW TABLES` in one transaction separately hit `NotImplementedException:
Multiple streaming scans ... not currently supported`, suggesting Quack's remote-table access goes
through some kind of streaming-scan abstraction the DuckDB binder doesn't yet recognize as an
updatable/deletable base table). Since `convert._write_bookkeeping()` uses `DELETE`+`INSERT` on
*every* `copy_table()`/`sync_table()` call, and `sync_table()` retires removed files via `UPDATE
ducklake_data_file SET end_snapshot = ...`, **neither function currently works against a
Quack-backed catalog** — this is a hard blocker stemming from the experimental protocol's current
DML support, not a bug in this package. Shipped anyway (per explicit decision): `bootstrap_catalog()`
and read-only queries already work today, and this should start working for writes with no code
changes here once Quack gains `UPDATE`/`DELETE` support. Documented prominently in both classes'
docstrings and in the README so users don't discover this the hard way.

## `sync_table()` now creates the table if it doesn't exist

Previously `sync_table()` raised `ValueError` if `table_name` wasn't already registered, requiring
callers to know in advance whether a first `copy_table()` call had happened (awkward for a
recurring job that just wants "make sure this table reflects the Delta table's current state,
whatever the starting point"). Now, if `find_table_id()` comes back `None`, `sync_table()` rolls
back its (so-far read-only) connection and delegates to `copy_table()` instead of raising -- safe
because nothing has been written yet at that point in the function, so handing off to a second,
independently-connected `copy_table()` call has no partial-state cleanup to worry about.
`copy_table()` itself is unchanged and still raises if the table already exists, for callers that
specifically want that hard failure instead of an update.

## Optional `credential` for non-public Azure storage accounts

`AzureStorageBackend` previously only ever constructed `BlobServiceClient(account_url,
credential=None)`, which only works against a publicly-readable container -- there was no way for
a caller to authenticate against a private storage account at all. Added an optional `credential`
parameter, threaded through `get_storage_backend()` → `copy_table()`/`sync_table()` (and the
internal `_register_deletion_vector()` write path, which needed the same credential for the
catalog's own `data_path`) so callers can pass any `azure.core.credentials.TokenCredential` (e.g.
`DefaultAzureCredential`, `ManagedIdentityCredential`, or a Databricks Unity Catalog service
credential obtained via `dbutils.credentials.getServiceCredentialsProvider(...)`). Defaults to
`None` everywhere, so existing callers relying on anonymous access are unaffected.
