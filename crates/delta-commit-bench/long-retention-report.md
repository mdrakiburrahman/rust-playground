# Long-retention report: the ArrowCol commit loop on year-long Delta tables

Status: complete for Q1 to Q7. Local runs on a 16-core, 27 GB workstation; Azure runs on one
Standard_D16s_v5 (64 GB) against the ADLS Gen2 hierarchical-namespace (HNS) account. Kusto QAP, Spark
and Fabric were not available as checkpoint readers; DuckDB was.

## Summary

| Question | Answer |
|---|---|
| Q1: does a `deltalake-core` 2.0.0 blind append load the file list? | Yes. 2.0.0 behaves the same as 1.1.1. Commit memory and latency are linear in active files: 1 M files cost 4.7 GB and 11 to 13 s per commit. 30 M projects to about 140 GB and minutes per commit. Fails. |
| Q2: can delta-kernel-rs do the commit? | Yes. Blind append, several `txn` entries per commit, `txn` reads, and put-if-absent conflict detection all work. Memory and latency are flat in active files: 30 M files commit at p50 0.9 ms locally and 22 ms on HNS, with 37 to 97 MB RSS. Kernel has no conflict checker and no rebase; the caller retries. Passes. |
| Q3: snapshot load for a claim or recovery | Kernel: 1.1 s and 40 MB at 30 M on HNS (0.18 s after a fresh checkpoint). delta-rs: 24 to 26 s and 3.5 GB at 1 M on HNS, 170 s and 13.7 GB at 4 M locally, linear to about 100 GB at 30 M. Kernel passes; delta-rs fails. |
| Q4: checkpoints | Only classic V1 and V2 writers exist; no library writes multi-part checkpoints. Kernel's default Parquet settings need 10 GB to checkpoint 4 M files. A 40-line custom writer (zstd, 128 k-row groups) keeps peak memory under 0.6 GB at 30 M. A 30 M checkpoint is 12.45 GB and takes 271 s locally and 576 s on HNS. |
| Q5: statistics | Stats are 90% of checkpoint bytes. Default stats cost 418 B per file in the checkpoint and 2.9 KB per file in a commit. Three indexed columns cut that to 55 B and 0.8 KB, and the 30 M checkpoint from 12.45 GB to about 1.6 GB. Kernel commit cost does not depend on stats. |
| Q6: maintenance during appends | Kernel appends never fail on maintenance commits. The colliding append costs one retry (78 ms to 1.5 s). A large maintenance commit can starve behind appends: on HNS an 18.8 MB drop needed 55 attempts over 10.6 s against 10 appends/s. |
| Q7: fault suite on kernel | G1 to G4 hold in every scenario where they held on delta-rs, plus one where delta-rs failed. Crash before and after a claim and an adopter that invalidated first all pass, locally and on HNS. |

## Recommendation

**Library: move the commit path to delta-kernel-rs (0.30.1), for commits and claims.** delta-rs 1.1.1 and
2.0.0 both hold every active file in the writer's snapshot (F12), grow across checkpoints (F9), and keep
the blocking-pool deadlock mechanism (F14). None of these exist in kernel's commit path. Kernel needs four
pieces of ArrowCol code, all built and tested in this bench:

| Piece | Why | Bench code |
|---|---|---|
| Retry loop with the landed check | Kernel returns `Conflicted` when the version exists and has no rebase. The loop refreshes, runs the landed check, and recommits on a definite "no". | `src/backend.rs` `kernel_commit`, `src/commit_loop.rs` |
| Refresh with retry | An incremental refresh can fail transiently. On local disk the listing race produced "Table version N is missing", and the panic killed the engine's executor. | `lr/src/kernel.rs` `refresh` |
| Checkpoint Parquet writer | Kernel's default engine writes checkpoints uncompressed with 1 M-row groups. | `lr/src/kernel.rs` `LrParquet` |
| Log cleanup | Kernel 0.30.1 has no expired-log cleanup. | Not built; belongs to maintenance |

delta-rs can stay for other paths (table creation, maintenance), where it does not need to hold a
long-lived snapshot.

**Checkpoint format: classic V1, single file, zstd, 128 k-row groups.** Classic V1 is the only format
every target reader is known to accept. DuckDB reads V1 and V2. Spark, Fabric and Kusto QAP were not
tested, and the handoff rules V2 out until every reader supports it.

**Open decision: who writes checkpoints.** Kernel commits do not read checkpoints, so the append path
stays fast without them. Readers, claims and recovery need them to bound snapshot load time.

| Owner | For | Against |
|---|---|---|
| ArrowCol | Checkpoints follow commits closely. No new process. | A 30 M checkpoint takes about 10 minutes and 0.6 GB on HNS. With 100 warm tables, about 20 checkpoints a minute at 10.5 s each; 3 slots let the queue wait reach 314 s. Each slot costs 300 to 450 MB. |
| Maintenance | Keeps batch work out of the commit process. Maintenance already has to clean up the log, because kernel has no log cleanup. | Needs its own schedule per table. Checkpoints lag commits by that schedule. |

If ArrowCol writes them: use jemalloc, write them in the background, and size the concurrency cap to the
table count.

**Open decision: large maintenance commits on hot tables.** Re-listing alone does not bound the retries
on HNS. An 18.8 MB drop needed 55 attempts over 10.6 s against 10 appends/s.

| Option | Effect |
|---|---|
| The owner pauses appends briefly when maintenance asks | The drop stays atomic. Needs a coordination signal. |
| Maintenance splits the drop into smaller commits | Each commit wins quickly. Readers can see a partial drop. |
| Accept unbounded retries | No new mechanism. Fine for cold tables; risky for hot ones. |

**Process settings for kernel at 100 tables per process:** jemalloc (glibc grew past 7.5 GB with
concurrent checkpoints; jemalloc stayed at 1.9 GB), one Azure client per storage account shared by all
tables, and the earlier caps on concurrent commits and checkpoints.

**Statistics:** set `delta.dataSkippingStatsColumns` to the columns readers prune on. The design owner
decides the list; the cost of the default 32 columns is in Q5.

## Setup and method

| Item | Value |
|---|---|
| Libraries | `deltalake-core` 1.1.1 ("dl1"); delta-rs main at 460c904 ("dl2", the unreleased 2.0.0); `delta_kernel` and `delta_kernel_default_engine` 0.30.1 |
| Other crates | object_store 0.13.2, arrow and parquet 59, tokio 1, rustc 1.96.1 release build |
| Local | 16 cores, 27 GB RAM, ext4 on NVMe, `file://` tables |
| Azure | westus2, one Standard_D16s_v5 (16 vCPU, 64 GB), Ubuntu 22.04, HNS account `emdcbhnsf35219`, managed identity, access through Bastion (`scripts/infra.sh`) |
| Table | 38 columns, partitioned by `date`, 365 days. Default stats index the first 32 leaves (30 with min/max). |
| Checkpoint writer | Kernel with the bench's `LrParquet` handler (zstd, 128 k-row groups) unless a row says otherwise |

**Synthetic tables.** `lrb gen` writes one classic checkpoint holding N `add` actions with realistic paths,
sizes and stats, 1000 `txn` appIds, and a 10-commit JSON tail. N reaches 30 M without writing 30 M data
files.

**Validation against real appends.** A table built from 1000 kernel commits of 100 files each, then
checkpointed, matched a synthetic table of the same size:

| Measure | Real appends (101,100 files) | Synthetic (101,100 files) |
|---|---|---|
| Checkpoint bytes | 41.6 MB | 42.2 MB |
| Stats JSON per file | 2096 B | 2097 B |
| Commit p50 / p99 (300 commits) | 1.11 / 4.1 ms | 1.16 / 3.9 ms |
| Commit RSS | 31 MB | 32 MB |
| Load (open + `txn` lookup) | 31 + 15 ms | 5 + 2 ms |

Both tables use the same stats generator, so this validates the action and checkpoint encoding. It does
not validate stats realism. The load difference comes from row order: kernel's checkpoint writer puts
`protocol`, `metaData` and `txn` after the adds, while the generator puts them first. With one row group
kernel decodes the whole file. At 1 M files the gap is 31 + 40 ms against 12 + 7 ms, and at 30 M it
disappears (20 ms open after a kernel checkpoint), because row-group statistics let kernel skip.

## Q1: delta-rs 2.0.0 blind-append commit

5 commits of 1 file each, `txn` set, after a load. Local disk.

| Active files | Library | Load | Commit p50 | RSS after load | Peak RSS |
|---|---|---|---|---|---|
| 200 k | 1.1.1 | 3.4 s | 2.5 s | 728 MB | 996 MB |
| 200 k | 2.0.0 | 3.3 s | 2.6 s | 737 MB | 1015 MB |
| 1 M | 1.1.1 | 16.1 s | 13.3 s | 3.45 GB | 4.68 GB |
| 1 M | 2.0.0 | 16.4 s | 12.3 s | 3.45 GB | 4.68 GB |
| 2 M | 1.1.1 | 31.9 s | 22.8 s | 6.87 GB | 9.31 GB |
| 2 M | 2.0.0 | 34.2 s | 18.7 s | 6.87 GB | 9.30 GB |

On HNS at 1 M: commit p50 11.9 s (1.1.1) and 11.4 s (2.0.0), peak 4.7 GB.

- The put itself takes about 20 ms. The rest is the post-commit snapshot rebuild, which replays every
  active file and parses its stats.
- In 2.0.0, `CommitBuilder::build` takes a `TableReference`, which only `EagerSnapshot` and
  `DeltaTableState` implement, and both materialize the file list. `require_files` is ignored in both
  versions. delta-rs issue 4584 tracks the migration away from eager snapshots.
- Fewer stats columns reduce the cost without removing the growth: at 200 k, commit p50 drops to 380 to
  590 ms with three columns and 430 to 480 ms with none.
- 30 M projects to about 140 GB and several minutes per commit. 10 M and 30 M were not run.

## Q2: delta-kernel-rs commit

**API.** Everything ArrowCol needs exists.

| Need | Kernel 0.30.1 |
|---|---|
| Blind append | `Transaction::add_files`; `dataChange` set by the caller |
| `txn` per batch and claim | `with_transaction_id(app_id, version)`, several per commit (one per appId) |
| `txn`-only commit (fence, claim) | Accepted with no files |
| Read an appId's `txn` | `Snapshot::get_app_id_version` |
| Conflict detection | `CommitResult::Conflicted` when the version exists. No rebase and no logical conflict check. |
| Retry | `Retryable` only for kernel I/O errors. Object-store errors return as hard errors. The caller owns all retry. |
| Commit metadata | `with_commit_info` puts custom keys at the top level of `commitInfo` |
| Checkpoint | `Snapshot::checkpoint` writes classic V1 or V2 (with or without sidecars). No multi-part writer. |
| Log cleanup | Missing |

**One table, one writer.** 300 commits of 1 file. Load is in Q3.

| Active files | Where | Interval | Commit p50 | Commit p99 | Peak RSS |
|---|---|---|---|---|---|
| 200 k | Local | Back to back | 0.66 ms | | 22 MB |
| 1 M | Local | Back to back | 0.59 ms | | 23 MB |
| 30 M | Local | Back to back | 0.87 ms | 8 ms | 37 MB |
| 30 M + 10 M tombstones | Local | Back to back | 1.41 ms | 3.8 ms | 54 MB |
| 1 M | HNS | Back to back (34/s) | 25 ms | 60 ms | 61 MB |
| 30 M | HNS | Back to back (40/s) | 22 ms | 45 ms | 75 MB |
| 1 M | HNS | 100 ms (7.8/s) | 23 ms | 55 ms | 93 MB |
| 30 M | HNS | 100 ms (7.8/s) | 22 ms | 63 ms | 97 MB |

Latency drifts with commits since the last checkpoint: 0.7 ms to about 2 ms over 5000 commits locally.
RSS grows with the same JSON tail. The active file count has no effect.

**One table, several writers.** Local, 1 M files, 100 commits per writer.

| Writers | Commits/s | Conflicts | p99 | Max | Peak RSS |
|---|---|---|---|---|---|
| 1 | 1084 | 0 | | | |
| 4 | 579 | 15 | 3.4 ms | | |
| 16 | 160 | 127 | 5.3 ms | 9.1 s | 1.4 GB |
| 32 | 56 | 503 | 19 ms | 54 s | 3.4 GB |

The `txn` check passed in every run. Throughput falls with writers and losers starve, so one writer per
table stays the design point.

**Many tables in one process.** One hot table (30 M files, 10 commits/s, checkpoint every 3000 commits)
and 100 warm tables (550 k files each, 1 commit/s, checkpoint every 300 commits, staggered). Caps: 16
concurrent commits, 3 concurrent checkpoints. 600 s, local.

| Allocator | Peak RSS | End RSS | Hot p99 | Warm p99 | Max checkpoint wait | Reload p99 |
|---|---|---|---|---|---|---|
| glibc | 7.5 GB, still rising | 4.7 GB | 5.7 ms | 4.1 ms | 2.4 s | 11 ms |
| glibc, `MALLOC_ARENA_MAX=2` | 2.5 GB | 2.0 GB | 49 ms | 22 ms | 212 s | 207 ms |
| jemalloc | 1.9 GB | 1.3 GB | 6.9 ms | 4.1 ms | 30 s | 21 ms |

- Each run made about 60 k warm and 6 k hot commits with no conflicts. Every `txn` check passed.
- Without checkpoints the same 101 tables used 160 to 400 MB. Concurrent checkpoints (300 to 450 MB each)
  and glibc fragmentation drive the rest.
- On HNS, jemalloc, same setup: peak RSS 1.9 GB (2.15 GB from `time`) and 0.38 GB at the end. 0 errors.

| Where | Commits | Commit p50 | Commit p99 | Max behind schedule | Checkpoints | Max checkpoint wait | Reload p99 |
|---|---|---|---|---|---|---|---|
| HNS, hot | 5923 | 25 ms | 88 ms | 7.3 s | 1, 530 s | 97 s | |
| HNS, warm | 59,235 | 26 ms | 103 ms | 13 s | 197, p50 10.5 s | 314 s | 369 ms |

- Every `txn` check passed. No conflicts.
- Memory on HNS matches local. Checkpoints still drive it.
- With 3 checkpoint slots, a 10 s warm checkpoint waited up to 314 s. Checkpoint concurrency needs to be
  sized against the number of tables. Commits are unaffected because they never wait on checkpoints.

## Q3: snapshot load for a claim or recovery

Load protocol and metadata, then read one appId's `txn` version.

| Active files | Library | Where | Time | Peak RSS |
|---|---|---|---|---|
| 200 k | 1.1.1 / 2.0.0 | HNS | 5.5 / 5.2 s | 818 / 820 MB |
| 1 M | 1.1.1 / 2.0.0 | HNS | 26.7 / 24.1 s | 3.5 / 3.5 GB |
| 4 M | 1.1.1 | Local | 170 s | 13.7 GB |
| 1 M | kernel | HNS | 0.21 s | 27 MB |
| 30 M | kernel | Local | 20 ms + 9 to 29 ms | 37 MB |
| 30 M | kernel | HNS | 1.12 s | 40 MB |
| 30 M, after a kernel checkpoint | kernel | HNS | 0.18 s | 36 MB |
| 30 M + 10 M tombstones | kernel | Local | 60 to 80 ms | 31 to 41 MB |

delta-rs memory is linear at about 3.4 GB per million files, so 30 M needs about 100 GB. Kernel reads
only the protocol, metadata and `txn` columns.

## Q4: checkpoints

**Who can write them.**

| Writer | Classic V1 | Multi-part | V2 |
|---|---|---|---|
| delta-rs 1.1.1 / 2.0.0 | Yes (through kernel) | No | No |
| Kernel 0.30.1 | Yes | No | Yes, with or without sidecars |

**Kernel's default writer needs a custom Parquet handler.** It uses parquet-rs defaults: no compression
and 1 M-row groups.

| Active files | Writer | Time | Peak RSS | File |
|---|---|---|---|---|
| 1 M | Kernel default | 9.0 s | 4.2 GB | 2.19 GB |
| 1 M | delta-rs 1.1.1 `create_checkpoint` | 9.5 s + 14.8 s load | 7.7 GB | 2.19 GB |
| 1 M | delta-rs 2.0.0 `create_checkpoint` | 10.7 s + 17.2 s load | 7.7 GB | 2.19 GB |
| 4 M | Kernel default | 33 to 35 s | 9.5 to 10.2 GB | 8.76 GB |
| 4 M | Custom, snappy, 1 M-row groups | 38 s | 1.9 GB | 3.22 GB |
| 4 M | Custom, zstd, 1 M-row groups | 38 s | 1.1 GB | 1.65 GB |
| 4 M | Custom, zstd, 128 k-row groups | 36 s | 360 MB | 1.67 GB |
| 30 M | Custom, zstd, 128 k-row groups, local | 271 s | 423 MB | 12.45 GB |
| 30 M | Custom, zstd, 128 k-row groups, HNS | 576 s | 584 MB | 12.45 GB |

A checkpoint after 1000 commits of 1000 files with no earlier checkpoint (1 M adds read from JSON) took
23 s and 828 MB.

**Formats.** 200 k files, custom writer.

| Spec | Output | Time | Peak RSS |
|---|---|---|---|
| Classic V1 | One Parquet file | 1.87 s | 233 MB |
| V2, sidecars of 50,000 | 35 KB manifest and 4 sidecars | 1.77 s | 187 MB |
| V2, no sidecar | One 84 MB Parquet file | 1.96 s | 195 MB |

V2 with sidecars saves little time or memory at this size. Its benefit is incremental reuse of sidecars,
which kernel 0.30.1 does not do.

**Readers.** DuckDB's `delta` extension read all three formats and the zstd checkpoint. Spark, Fabric
through OneLake and Kusto QAP were not tested.

**Ownership.** Open; see the recommendation. Concurrent checkpoints dominate commit-process memory (Q2),
and a hot-table checkpoint takes 10 minutes on HNS.

## Q5: per-file statistics

4 M files, classic checkpoint. "Few" sets `delta.dataSkippingStatsColumns = ts,severity,service`. "All" sets
`delta.dataSkippingNumIndexedCols = -1`.

| Stats | Leaves with min/max of total | Checkpoint (snappy) | Checkpoint (zstd) | Bytes per file (zstd) | Write time | Peak RSS | Commit JSON for 1 file |
|---|---|---|---|---|---|---|---|
| None | 0 | 149 MB | 94 MB | 23 | 4.9 s | 142 MB | 525 B |
| Few | 3 | 375 MB | 219 MB | 55 | 8.5 s | 255 MB | 779 B |
| Default | 30 of 32 | 3.23 GB | 1.67 GB | 418 | 41 s | 347 MB | 2859 B |
| All | 40 of 45 | 4.28 GB | 2.07 GB | 518 | 53 s | 447 MB | 3882 B |

At 30 M (default measured, others scaled): none 0.7 GB, few 1.6 GB, default 12.45 GB, all about 15.5 GB.

- Effect on Q1: kernel commit cost does not depend on stats. delta-rs commit and load cost does.
- Effect on Q4: checkpoint time is linear in stats bytes. Peak memory stays under 0.5 GB with the custom
  writer.
- The cost of fewer columns is reader pruning on the dropped columns.

## Q6: maintenance during appends

A partition drop (`remove` for one day's files) or OPTIMIZE (`remove` plus 1 `add` per 8 removed) is
written as one commit by a separate process while kernel appends run. The maintenance writer lists the
newest version and retries put-if-absent at the next version.

| Table | Where | Operation | Commit size | Append rate | Maintenance attempts | Colliding append | Other appends p50 / p99 |
|---|---|---|---|---|---|---|---|
| 1 M | Local | Drop, 2739 removes | 627 KB | Unthrottled | 9 | 241 ms | 1.2 / 3.2 ms |
| 1 M | Local | OPTIMIZE, 2739 / 343 | 1.5 MB | Unthrottled | 13 | 245 ms | 1.2 / 3.5 ms |
| 30 M | Local | Drop, 82,191 removes | 18.8 MB | 10/s | 1 | 78 ms | 1.26 / 8.9 ms |
| 30 M | Local | OPTIMIZE, 82,191 / 10,274 | 45.3 MB | 10/s | 1 | 159 ms | 1.25 / 9.0 ms |
| 30 M | Local | Drop or OPTIMIZE | 18.8 / 45.3 MB | Unthrottled (400/s) | Over 7000; landed after appends stopped | | 2.2 to 2.4 / 5.7 to 6.4 ms |
| 1 M | HNS | Drop, 2739 removes | 627 KB | 10/s (7.8 achieved) | 2, 0.05 s | 249 ms | 23 / 61 ms |
| 30 M | HNS | Drop, 82,191 removes | 18.8 MB | 10/s (6.9 achieved) | 55, 10.6 s | 1.47 s | 24 / 199 ms |

- Appends never fail because of a maintenance commit. Every run had exactly one conflict and passed the
  `txn` check. Kernel has no logical conflict check, and ArrowCol only appends.
- The colliding append pays one refresh that reads the large commit. Later appends are unaffected.
- A large maintenance commit loses every race against an unthrottled appender, because its put takes
  longer than the gap between appends.
- On HNS at 10 appends/s, an 18.8 MB put takes longer than the append interval. The drop needed 55
  attempts over 10.6 s and landed when a put happened to fit between appends. It is not bounded. A
  faster append rate or a larger commit can starve it. The options are an open decision in the
  recommendation.
- A first HNS run without re-listing (L5) took 194 attempts.
- Load after OPTIMIZE (45 MB commit in the tail): 0.22 s and 40 to 48 MB.

## Q7: fault suite on kernel

The crate's commit loop now runs on either library (`dcb run --lib dl1|kernel`). The kernel backend
commits at snapshot version + 1 with put-if-absent. On `Conflicted` or an error it returns to the loop,
which refreshes, runs the landed check, and recommits only on a definite "no". Claims and fences are
`txn`-only commits with the `2^62 + v + 1` invalidation encoding.

| Suite | Local, kernel | HNS, kernel |
|---|---|---|
| `faults.sh` (exp 7 scenarios) | All pass, except identity change without adoption with a crash (G1, expected and documented) | Same as local |
| `ownership.sh`, tenure epochs | All pass: crash before and after, paused in flight, two holders at 0, 100 and 300 ms | All 12 pass, including drain load and misroute |
| `q7.sh`, crash before the claim commit | Pass. 37 accepted, 0 missing. | Pass. 37 accepted, 0 missing. |
| `q7.sh`, crash after the claim commit | Pass. 37 accepted, 6 appIds. | Pass. 38 accepted, 0 missing. |
| `q7.sh`, adopter invalidated first | Pass. The paused owner was fenced at the claim, lost, and landed nothing under its old appId. | Pass. Same outcome. |

Differences from delta-rs:

- `ambiguous-conflict-scan-log` passes on kernel. It failed G2 on delta-rs because the library rebased
  and retried. Kernel never retries by itself.
- Library retries were 0 in every scenario. Injected 503s (69 in `errors-503`) became 12 loop retries.
- Liveness costs more without rebase. With two holders and a 150 ms injected log delay, the draining
  holder needed 4 claim attempts and one commit needed 9 loop attempts (about 27 s). Correctness held.
- `q7.sh` also passes on delta-rs 1.1.1 (`--lib dl1`).

## delta-rs 1.1.1 to 2.0.0

**Upgrade notes.**

- 2.0.0 is unreleased. crates.io stops at 1.1.3; this report pins main at 460c904.
- The bench's commit, load and checkpoint code compiles unchanged against both versions.
- `CommitBuilder::build` takes `Option<&dyn TableReference>`. Only `EagerSnapshot` and `DeltaTableState`
  implement it.
- Regression: any boolean column in the stats makes 2.0.0 log "existing stats_parsed did not satisfy
  requested stats schema; falling back to raw stats" per batch and parse JSON stats. Reproduced on a
  table written only by delta-rs. Not filed upstream.

**Earlier findings on 2.0.0.**

| Finding | Status on 2.0.0 | Evidence |
|---|---|---|
| F9: snapshot grows across checkpoints | Unchanged | 1 k-file table, 1000 commits, a checkpoint every 100. Commit p50 80 ms to 531 ms and RSS 337 MB to 1.04 GB, the same as 1.1.1. `EagerSnapshot::update` still seeds from the previous materialized files. |
| F12: snapshot holds every active file | Unchanged | Q1 |
| F14: blocking-pool deadlock | Mechanism unchanged. Not rerun. | `create_checkpoint_for` still calls the synchronous kernel checkpoint on an async worker (`protocol/checkpoints.rs:55`). |

## Other findings

| # | Finding | Effect |
|---|---|---|
| L1 | Kernel's default engine writes checkpoints with no compression and 1 M-row groups. | 10 GB peak at 4 M files. Fixed with a custom `ParquetHandler::write_parquet_file`. |
| L2 | An incremental kernel refresh that hit "Table version N is missing" (local listing race with 16 writers) panicked and killed the engine's executor. | ArrowCol must retry refresh errors. |
| L3 | A separate Azure client per table sent one managed-identity token request per table. At 100 tables IMDS throttled them, and the 1024 open-file limit broke requests. | Share one client per storage account; raise the open-file limit. |
| L4 | glibc malloc fragmented under concurrent checkpoints. | Use jemalloc. |
| L5 | A maintenance writer that retries at v+1 after a conflict never catches up when its put is slower than the append interval. | Re-list the newest version on each conflict. |
| L6 | delta-rs 2.0.0 falls back to JSON stats when any boolean column has stats. | Avoid boolean stats columns or stay on kernel. |

## Design follow-ups

| Handoff # | Follow-up | Result |
|---|---|---|
| 1 | Commit-loop memory and checkpoint size grow with retained files | Memory: solved by kernel (flat). Checkpoint size: 12.45 GB at 30 M with default stats, 1.6 GB with three columns. |
| 2 | Checkpoint owner and format | Maintenance, classic V1, single file, zstd. |
| 6 | Claim, recovery and startup time | Kernel load 1.1 s at 30 M on HNS. |
| 7 | Per-file statistics size | Q5 |

## Repro

Build: `cargo build --release` in `lr/` (binary `lr/target/release/lrb`), with `--features jemalloc
--target-dir target-je` for jemalloc. `dcb` builds from the crate root. Set `LR_CKPT_COMPRESSION=zstd
LR_CKPT_ROWGROUP=131072` for the custom checkpoint writer.

| What | Command |
|---|---|
| Synthetic table | `lrb gen --table-url <url> --active <n> [--tombstones <n>] [--stats none\|few\|default\|all] [--format classic\|v2]` |
| Q1, Q2 commit | `lrb commit --lib dl1\|dl2\|kernel --table-url <url> --commits <n> [--writers <n>] [--interval-ms <ms>] [--trace]` |
| F9 on delta-rs | `lrb commit --lib dl2 --table-url <url> --commits 1000 --dl-checkpoint --trace` |
| Q2 many tables | `lrb multi --hot <url> --warm-prefix <url>/t --warm-count 100 --max-commits 16 --max-ckpts 3 --duration-s 600` |
| Q3 load | `lrb load --lib dl1\|dl2\|kernel --table-url <url>` |
| Q4 checkpoint | `lrb checkpoint --lib kernel\|dl1\|dl2 --table-url <url> [--spec v1\|v2\|v2-sidecar-<n>]` |
| Q6 | `lrb maint --table-url <url> --kind drop\|optimize --active <n> --day <d> --delay-s 10` next to `lrb commit --lib kernel --interval-ms 100` |
| Azure plan | `lr/scripts/azure.sh abfss://<container>@<account>.dfs.core.windows.net/lr <out-dir> [gen dl kernel ckpt q6 multi]` |
| Q7 | `DCB_ARGS="--lib kernel" scripts/faults.sh <base-url> <work>`, `DCB_ARGS="--lib kernel" EPOCHS=tenure scripts/ownership.sh <base-url> <work>`, `DCB_ARGS="--lib kernel" scripts/q7.sh <base-url> <work>` |
| Infra | `VM_COUNT=1 scripts/infra.sh up`, `scripts/infra.sh down` |

Raw run output is not committed.
