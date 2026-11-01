# ArrowCol experiment primitive

| Purpose | Contract |
| --- | --- |
| Publisher | Real `BufferManager::admit_tracked_at` → `DeltaPublicationSink` → `BatchedTable`, Arrow Parquet encoder and Delta Kernel commits. This is the sink wrapped by the production collector's `ProductionDeltaSink`. |
| Not running | No loadtest runner, Flight service, SQL Server, router, HMS, external collector or durable receipt recovery. Catalog and receipts exist only in this process; never retry an uncertain invocation blindly. |
| Cloud fence | Existing production publisher session/ready-v1 generation and guarded Azure stores. Requires the parent's isolated owned environment, including `ARROWCOL_RESET_GENERATION`, data/control-container settings and credentials. This binary does not bootstrap/reset infrastructure. |
| Root allowlist | `--root` must exactly equal `ARROWCOL_DELTA_ROOT` for `abfss` roots. Local absolute paths/file URLs are for disposable offline roots only. |
| Table allowlist | `--table NAME --allow-table NAME`; starts with a lowercase letter, then lowercase letters, digits and underscores, at most 63 bytes. |
| Physical location | `<root>/<table>/`, the same layout `delta-maintenance` uses, so both binaries can share one `--root`. |
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

## Server-side-copy fixture (`seed-scale`, `inspect-scale`)

The [Azure QAP demo](../../experiments/azure-qap/README.md) seeds every trial
with `seed-scale`, so per-file size and count are cheap to tune. The commands and
bounds above are unchanged.

| Contract | Scale mode |
| --- | --- |
| Commands | `seed-scale --anchor-ms MS --seed-mib 32 --files-per-partition 16 --copy-concurrency 4 --max-total-bytes 2147483648 [--tombstones]`; `inspect-scale` |
| Tombstones | `--tombstones` runs a production PURGE of the `expired` partition right after seeding. The table then already holds unreferenced files, so a later zero-retention VACUUM only deletes those and changes no active file. The output's `tombstones` is the purged file count (`null` without the flag). |
| Schema | `(id:long, value:long, payload:string, __arrowcol_ingestion:string)`; partition column is virtual, never copied physically. |
| Seed | 1–32 MiB (`--seed-mib`) of deterministic payload (default 16), 65,536 bytes/row, IDs `0..seedMiB*16-1`, `value=id*10+7`. Uncompressed Parquet prevents a misleading highly compressed tiny fixture. Actual bytes and row-group layout are measured. |
| Layout | Same four roles as rapid mode. Expired and compact each have N files (`--files-per-partition`, 2–511, default 31); control/live one each. Identical seed rows intentionally repeat in every copy. |
| Copy | Authenticated synchronous **Put Blob From URL**, not asynchronous SDK Copy Blob. Create-only destinations, source ETag condition, ownership metadata, completed status/HEAD size/ETag checks, and production publisher mutation tracking. Payload never passes through the driver for copies. |
| Limits | Copy concurrency 1–8, at most 1,024 files, 8 GiB total physical bytes (default ceiling 2 GiB), 32 MiB generated payload in memory. Inspection refuses files over 512 MiB, metadata over 1 MiB/file, over 128 MiB of projected key columns, or over 1,000,000 rows. |
| Staging | For cloud writes set `TMPDIR` to an existing workspace-contained scratch directory, e.g. `mkdir -p .temp/qap-stage; export TMPDIR="$PWD/.temp/qap-stage"`. This keeps production writer staging out of system temporary directories. |
| Resume | Rerun **identical** seed options. `_scale/manifest.json` binds seed ETag, size, content digest, deterministic payload digests and candidate paths. `_scale/copies/` retains create-only completion receipts (including service create/resume request IDs, bytes and ETags). Completed copies are reused without overwriting. Resume after maintenance/foreign writes is refused. Uncertain failures retain candidates and emit `orphanInventory`; never delete them blindly. A crash before the initial seed manifest exists requires explicit reconciliation, not automatic adoption. |
| Oracle | Kernel active-file inventory plus exact projected `id/value` reads; no copied payload download. Expected per-row payload digests come from deterministic immutable seed truth. Supports OPTIMIZE, whole-partition PURGE and VACUUM; no deletion vectors or arbitrary foreign schemas/writes. Must still validate payload integrity in Kusto. |

Use the ordinary root/table/allow-table flags before either scale command.
`seed-scale` returns the `inspect-scale` fields plus `partitions`, `copies`
(`path`, `bytes`, destination `etag`, service `requestId`, `completed`),
`tombstones`, `publisherClosed` and `cancelled`. `inspect-scale` returns:

```json
{"ok":true,"action":"inspect-scale","table":"experiment_demo","version":2,"rows":17408,"totalBytes":1141000000,"fingerprint":"sha256-of-sorted-expected-groups-json","seedRows":512,"payloadBytes":65536,"projectedKeyBytes":123456,"files":[{"path":"__arrowcol_ingestion=2026-10-30-14-00/qap-r3-0000.parquet","bytes":33560000,"partition":"2026-10-30-14-00"}],"expectedGroups":[{"partition":"2026-10-30-14-00","id":0,"value":7,"payloadSha256":"sha256-of-this-row-payload","multiplicity":16}],"elapsedMs":12}
```

Arrays above are abbreviated; compare **every** expected group and reject extra,
missing or null groups, not merely totals or a single fingerprint. Kusto's bounded
result is:

```kusto
external_table("fixture")
| summarize multiplicity=count()
    by partition=__arrowcol_ingestion, id, value, payloadSha256=hash_sha256(payload)
| take 2049 // 4 * maximum seedRows (512) + 1; an overflow is a failure
| order by partition asc, id asc, value asc, payloadSha256 asc
```

Each payload is the concatenation of lowercase SHA-256 hex strings of UTF-8
`qap-r3-v1:{id}:{chunk}`, for `chunk=0..1023`. Exact key/value/multiplicity
comparison plus each row's payload digest is the supported integrity check.
Projected oracle reads are maintenance/oracle traffic, not Kusto ingestion.
The inventory and grouped oracle are tested against independent full-payload
Parquet scans before/after real production OPTIMIZE and PURGE on small fixtures.
Use maintenance `--target-file-mib 128 --latest-partitions 2` for a real partial
merge with the default layout; outputs must remain below the inspection limit.

## Validation

```bash
cargo fmt --package arrowcol-experiment --check
CARGO_BUILD_JOBS=2 cargo test -p arrowcol-experiment
```
