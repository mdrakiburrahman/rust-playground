# ArrowCol experiment primitive

| Purpose | Contract |
| --- | --- |
| Publisher | Real `BufferManager::admit_tracked_at` → `DeltaPublicationSink` → `BatchedTable`, Arrow Parquet encoder and Delta Kernel commits. This is the sink wrapped by the production collector's `ProductionDeltaSink`. |
| Not running | No loadtest runner, Flight service, SQL Server, router, HMS, external collector or durable receipt recovery. Catalog and receipts exist only in this process; never retry an uncertain invocation blindly. |
| Cloud fence | Existing production publisher session/ready-v1 generation and guarded Azure stores. Requires the parent's isolated owned environment, including `ARROWCOL_RESET_GENERATION`, data/control-container settings and credentials. This binary does not bootstrap/reset infrastructure. |
| Root allowlist | `--root` must exactly equal `ARROWCOL_DELTA_ROOT` for `abfss` roots. Local absolute paths/file URLs are for disposable offline roots only. |
| Table allowlist | `--table experiment_NAME --allow-table experiment_NAME`; lowercase letters, digits and underscores only, at most 63 bytes. |
| Physical location | `<root>/experiment/fixtures/<table>/` |
| Schema | `id:int64 NOT NULL`, `value:int64 NOT NULL`, `__arrowcol_ingestion:string` (minute Hive partition label `YYYY-MM-DD-HH-MM`). `value = id * 10 + 7`; generated IDs are nonnegative and JSON-safe. |
| Output | One JSON object on stdout, no root/credential/URL/error-chain echoes. `ok:false` and nonzero exit on failure. `--help` is human-readable. |
| Bounds | At most 24,000 seed rows and 100,000 total oracle rows; 10,000 active files, 1 KiB/path, 64 MiB/file, 256 MiB total Parquet. Configurable hard deadline (default 900s, max 3600s), stop scheduling 30s before it (minimum 1s). Sequential publications, at most one active flush. |

## Commands

Set `ARROWCOL_PARTITION_GRANULARITY=minute` for every command. Load private `.env.azure` in the parent; do not put credentials in CLI arguments.

```bash
cargo build -p arrowcol-experiment -j 2
export ARROWCOL_PARTITION_GRANULARITY=minute
driver=target/debug/arrowcol-experiment

# These are disposable example paths, not the benchmark root.
$driver --root "$ARROWCOL_DELTA_ROOT" \
  --table experiment_demo --allow-table experiment_demo \
  seed --anchor-ms 1793356800000 --rows-per-file 16 --files-per-partition 3

$driver --root "$ARROWCOL_DELTA_ROOT" \
  --table experiment_demo --allow-table experiment_demo \
  append --start-id 128 --batches 120 --rows-per-batch 16 --interval-ms 1000

$driver --root "$ARROWCOL_DELTA_ROOT" \
  --table experiment_demo --allow-table experiment_demo inspect
```

| Action | Behavior |
| --- | --- |
| `seed` | Fresh table only. `--anchor-ms` must be a minute-aligned epoch in milliseconds. Optional `--start-id` defaults to 0. Partitions: `expired` anchor−3 days (N files), `compact` anchor−2 hours (N files), `control` anchor−1 hour (one file), `live` anchor (one file). N defaults to 3; 16 rows/file by default. Deliberate past timestamps use the production durable-replay admission API, not handcrafted logs or uploads. |
| `append` | Existing table only. Required `--start-id`; `--batches` defaults to 1, `--rows-per-batch` 16, `--interval-ms` 0. Omit `--partition-ms` for actual per-batch current admission time (live frontier), or specify an aligned epoch to test an explicit partition. Each batch waits for confirmed publication before submitting the next. Run this process concurrently with separately owned maintenance. Duplicate IDs are retained, not deduplicated. |
| `inspect` | Read-only Kernel snapshot active-Add set, then reads exactly those Parquet files. Does not list all blobs as data or count tombstoned files. Rejects deletion vectors, wrong partitioning, size mismatches, missing/vacuum-raced files and budget overflow. Records retain duplicate multiplicity and are sorted lexicographically by `(id,value,partition)`. |

Mutation JSON includes `action`, `table`, `version`, `batches`, `rows`, `startId`, `nextId`, `cancelled`, `publisherClosed`, `partitions` (seed roles/labels/stamps/file counts), `fingerprint`, and exact appended `records`.

Inspection shape:

```json
{"ok":true,"action":"inspect","table":"experiment_demo","version":8,"files":[{"path":"__arrowcol_ingestion=2026-10-30-16-00/part-EXAMPLE.parquet","bytes":1234,"partition":"2026-10-30-16-00"}],"rows":2,"fingerprint":"sha256-of-canonical-records-json","records":[[1,17,"2026-10-30-16-00"],[2,27,"2026-10-30-16-00"]],"elapsedMs":12}
```

`fingerprint` is lowercase SHA-256 over UTF-8 compact JSON of the sorted array of `[id,value,partition]` tuples, without a trailing newline. Compare the records themselves for the exact multiset oracle; fingerprints are convenience evidence, not a substitute. Extra `elapsedMs` is not deterministic.

Example external-table KQL logical schema:

```kusto
(id:long, value:long, __arrowcol_ingestion:string)
```

The parent owns external-table URI/partition mapping and must materialize the Hive label into `__arrowcol_ingestion`; partition columns are not stored in physical Parquet. Compare `project id, value, __arrowcol_ingestion | order by id asc, value asc, __arrowcol_ingestion asc` with `records`.

For partial OPTIMIZE with the existing maintenance CLI, `--latest-partitions 2` selects the single-file control (no bin) plus compact partition while excluding protected live and leaving expired files untouched. `--latest-partitions 1` selects only the control and is a no-op. This assumes only the four fixture labels: use `append --partition-ms <seed anchor>` for a stable newest frontier during this phase, or account for extra closed frontier labels when choosing the window. Unset inherited explicit `ARROWCOL_LAYOUT_SORT_COLUMNS`/`ARROWCOL_LAYOUT_BLOOM_COLUMNS` unless they target fixture columns (`id`,`value`).

SIGINT/SIGTERM stop new submissions, join the current publication and drain before releasing publisher membership. A hard deadline or ambiguous publication exits nonzero and never claims `publisherClosed:true`; investigate retained membership and actual Delta state before cleanup/retry. Process-local receipts cannot recover after a crash. Cloud same-table multiwriter ownership is not provided by the in-process lease: the parent must exclusively own the allowlisted table.

## Validation

```bash
cargo fmt --package arrowcol-experiment --check
CARGO_BUILD_JOBS=2 cargo test -p arrowcol-experiment
```
