# Checkpointed tenant forks

`arrow-tenant-stream` is an append-only, historical, single-customer Delta fork.
It captures an immutable Kernel-validated active-file snapshot, processes that
snapshot, then follows contiguous committed JSON actions. DataFusion 54.1 uses the
same Arrow/Parquet 58.x family (58.3 minimum) as Delta Kernel 0.29; there is no second Arrow
representation or delta-rs production reader.

The customer predicate is equality on the explicit `customer_id` UTF-8 column.
Projection is optional, but must retain that key and every source partition
column. Hive partition values come from Delta Add actions, not event timestamps
or directory guessing. Output files belong to the target, so source VACUUM
cannot invalidate successfully copied rows.

## Semantics and failure boundaries

The bootstrap manifest is frozen before processing. A restart does not replace
it with the current source snapshot. Offsets carry source identity and a
version/action-index cursor; immutable per-batch plans preserve exact inputs
across crashes. `dataChange=false` OPTIMIZE Add/Remove pairs are ignored. Pure
DELETE/PURGE Removes are not propagated: rows already copied remain in the fork.
Mixed data-changing Add/Remove transactions (UPDATE, MERGE, overwrite), deletion
vectors, column mapping, row tracking, CDF actions, unknown actions, and
schema/protocol evolution fail explicitly rather than silently losing rows.

Required history/data that has been vacuumed or removed is an error. Retention
must exceed consumer lag and the intended recovery window. Each JSON commit has
a 64 MiB planning budget; microbatches cap the number of examined actions and
read one source file at a time as bounded Arrow batches. Snapshot capture
rejects manifests exceeding 262,144 files or 64 MiB of paths/partition values
rather than accumulating an unbounded active-file list.

Sink transaction identities are `(query UUID, batch number)`, not hashes of
customer payloads. An offset is durable before processing and a commit marker
is durable only after the sink transaction. A sink commit followed by a crash
before its checkpoint commit must replay as an already-committed transaction,
not insert the rows again.
Consumed batches with no matching rows still publish a transaction-only
progress record, not an empty Add file. An idle poll creates neither an offset
nor a sink commit. `tenant_stream_txn_only_batches_total` distinguishes necessary
stream progress from maintenance no-ops.

Local checkpoint ownership is a lifetime nonblocking filesystem lock. Local
immutable publication uses a synchronized temporary file, an atomic
create-only hard link, and directory synchronization. Cloud ownership is a
create-only, never-automatically-stolen lock; ordinary release uses conditional
updates. After a cloud process crashes, an operator must stop/fence every old
writer before explicitly clearing that lock. A timeout is not evidence that
the old process stopped. Cloud storage must support atomic create; unsupported
stores fail closed. Object-store durability is not described as local `fsync`.
Azure checkpoint mutations use the shared storage provider and check the
captured owned, ready reset generation immediately before and after each
mutation. A failed authority check permanently fences that handle; it must
not release a lock through a stale generation. Bounded-auth and live reset
qualification remain required; pre/post checks alone do not prove that a
blocked credential provider cannot publish a late write.

## Spark checkpoint wire format

The applicable **V1 stateless, one-source** Spark log format is used, referenced
against Apache Spark commit
`f784da87e6db27672ab778c79d1a76420d7c3b83`:
`StreamingQueryCheckpointMetadata`, `StreamMetadata`, `OffsetSeqLog`,
`OffsetSeqMetadata`, `CommitLog`, and `HDFSMetadataLog`. Source offsets are
source-defined JSON; this service does not claim that a Spark Delta connector
can resume this Rust job or vice versa.

`metadata` is compact JSON containing `id`, without a version header.
`offsets/<batch>` contains `v1`, offset metadata, then one source offset,
separated by newlines. `commits/<batch>` contains `v1`, newline, then
`{"nextBatchWatermarkMs":0}`. Writers do not append an extra newline. There is
no state-store directory because this predicate/projection query has no
stateful aggregation.

## Running

Run inside the repository devcontainer:

```bash
cargo run --locked -p arrow-tenant-stream -- \
  --source /data/source \
  --target /data/tenant-a \
  --checkpoint /data/checkpoints/tenant-a \
  --customer-id customer-0000 \
  --metrics-listen 0.0.0.0:9094
```

`--projection customer_id,value,__arrowcol_ingestion` is optional and must match
the source schema; omit it to retain every normalized field and precision
companion column. `--max-actions` defaults to 128, `--poll-ms` to 1000,
`--file-rows` to 262144, `--file-input-bytes` to 67108864, and
`--row-group-rows` to 16384. `sample_time_utc`, when present, is a secondary
file-sort key. Source and target must be separate, non-nested Delta roots;
checkpoints cannot be placed inside either vacuumable table root.

`--once` drains currently available work until the first idle poll. Stop an
indefinitely active producer first if a finite drain is required. SIGINT and
SIGTERM stop processing and release checkpoint ownership cleanly.

Cloud source/target/checkpoint roots use storage URLs and environment/identity
configuration, not credentials on the command line. The Debian bookworm
runtime installs `azure-cli=2.91.0-1~bookworm` through Microsoft's signed
package repository. A deployment must still provide the approved runtime
identity configuration; installing the CLI does not authenticate it or share
a developer host's login. Never copy a token cache or `.env` into the image.

After a cloud crash, stop every old writer for the checkpoint and only then
restart with both `--recover-stale-checkpoint-lock` and
`--confirm-previous-writer-stopped`. These flags assert an operator decision;
they do not prove that the old writer stopped. They are rejected for local
locks, which the operating system releases on process exit.

The service image builds from the workspace root with
`docker build -f crates/arrow-tenant-stream/Dockerfile .`, runs as UID 10001,
uses `/var/lib/tenant-stream`, and exposes port 9094. `/metrics` publishes
snapshot/tail batches, prepared offsets, sink/checkpoint commits, transaction
replay, transaction-only progress, idle/failure counts, bootstrap file backlog,
version lag, and low-cardinality DataFusion pruning/scan counters. It never uses
customer IDs, filenames, or checkpoint paths as metric labels.

## File layout and read evidence

`arrow_deltalake::layout::FileSortBuffer` belongs to **one complete output
file**, combining batches before sorting. It does not claim that independently
sorted batches form a globally sorted file. `try_push` returns backpressure
without retaining an over-budget batch; a batch that cannot fit an empty buffer
fails with an explicit request to slice it. Default retained-input limits are
262,144 rows and 64 MiB. Concatenation, sort permutation, and Parquet encoding
require additional proportional working memory; these limits are not a claim
of an exact process RSS ceiling or an external-sort spill implementation.

Policies expose row-group/page row limits, page statistics and indexes, and
selected-column Bloom filters. Bloom filters/indexes being present does not
prove that a reader uses them; the DataFusion execution-plan metrics are the
evidence. The seeded read-efficiency test compares sorted filtered, unsorted
filtered, sorted unfiltered, and absent-customer queries with multiple row
groups, and checks both row identity and actual `bytes_scanned`/row-group
pruning counters. No custom indexes or end-to-end zero-copy claim is made.
`bytes_scanned` is DataFusion's execution scan byte counter, not a measurement
of all network traffic. Planning, listing, explicit source-file HEAD checks,
and authentication traffic are not fully represented by that counter.
File-range pruning counters are labeled as ranges, not whole files.

The seeded fixture has 65,536 interleaved rows, 16 customers, and 1,024-row
groups. A recorded devcontainer execution returned the same 4,096 customer
rows for both predicate layouts:

| Scan | Rows returned | DataFusion bytes scanned | Row groups pruned by statistics | File ranges pruned |
|---|---:|---:|---:|---:|
| Sorted, customer predicate | 4,096 | 22,188 | 60 / 64 | 0 |
| Unsorted, customer predicate | 4,096 | 404,480 | 0 / 64 | 0 |
| Sorted, unfiltered projection | 65,536 | 352,000 | 0 / 64 | 0 |
| Absent, outside customer min/max | 0 | 0 | Not evaluated | 1 |

These are fixture observations, not universal performance guarantees. A
separate absent key within row-group min/max pruned all 64 groups by Bloom
filters. A single-large-row-group variant pruned 61,440 of 65,536 rows using
page indexes. The absent-customer file-range result still reported one opened
file; it is not evidence that footer/metadata I/O was eliminated.

Run focused checks inside the repository devcontainer:

```bash
cargo test -p arrow-deltalake layout::tests
cargo test -p arrow-tenant-stream -- --nocapture
```
