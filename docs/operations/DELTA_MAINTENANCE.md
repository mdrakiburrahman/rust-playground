# delta-maintenance

A standalone Delta Lake maintenance service built on `arrow_deltalake::maintenance`. It runs on Delta Kernel 0.29 only, with no delta-rs.

It is a separate process, not a collector task. It opens tables with `DeltaTable::open_for_maintenance`, so it does not take the collector's writer-lifetime lock. Every publish and delete goes through the core's `check_publication()` hook and its `GuardedCommitter`.

## Operations

| Operation | Effect | Commit |
| --- | --- | --- |
| `optimize` | Rewrites small files in the latest-N closed Hive partitions into sorted files of about 128 MiB. Rows, schema and partition values are preserved. | One atomic `dataChange=false` add+remove |
| `purge` | Removes still-active files in expired `__arrowcol_ingestion` partitions. | `dataChange=true` removes, plus the `arrowcol.purge.before-ms` watermark, in the same transaction |
| `vacuum` | VACUUM LITE: physically deletes tombstoned files older than the retention period, using `_delta_log` only (no data directory listing). | None |
| `checkpoint` | Writes a classic Kernel checkpoint once `--checkpoint-every-commits` (default 100) commits follow the hinted checkpoint. This bounds snapshot replay for the collector and readers. Application transactions (publication and purge identities) and metadata are retained. JSON history is never deleted. | None (checkpoint Parquet + `_last_checkpoint` hint) |
| `prune-log` | Deletes `_delta_log` history (`NNN.json` strictly below `C`, and older checkpoints; `C.json` is retained) once checkpoint `C` is proven equivalent to it. See [Log pruning](#log-pruning). | None |

## Safety

- **Dry run by default.** The service plans and prints JSON reports and changes nothing unless you pass `--apply`.
- **Applying revalidates first.** Before applying a plan the service rechecks the table id, metadata, protocol and the exact readset. Stale plans fail with `stale_plan` or `metadata_changed`.
- **Commit retries are bounded.** Commit collisions retry at most 8 times within 10 seconds, with jitter.
- **Default frontier protection.** OPTIMIZE protects the newest ingestion partition and every still-open partition. Only the explicit one-shot `--sealed-final` OPTIMIZE policy bypasses this churn protection. PURGE protection is unchanged.
- **Bad partitions are never deleted.** Malformed or null partition values fail closed.
- **No empty commits.** No-op runs and repeated runs leave the table version unchanged. The watermark is never committed on its own.
- **VACUUM retention has a 7-day floor.** Shorter retention needs `--unsafe-short-vacuum-for-tests` and a marked disposable temp root.
- **VACUUM needs explained history.** It requires contiguous JSON commits `0..=version`, or, after `prune-log`, a durable log floor `F` whose checkpoint exists, covered by the VACUUM LITE progress record, followed by contiguous JSON `F+1..=version`. Any other gap fails visibly with `unsupported`.
- **VACUUM rejects unsafe paths.** Traversal and external paths are rejected.
- **VACUUM reports partial progress.** Cancelling mid-delete reports a `partial` outcome.

On Azure the service opens tables through the shared publisher session and closes it after the run; a failed close exits nonzero and leaves its registration in place. Reset execution stays disabled, so the service does not claim infra-reset safety.

### Log pruning

`prune-log` never guesses; when any condition fails it reports `noop` (or refuses with `unsupported` on a disagreement) and deletes nothing.

**VACUUM LITE progress.** Only a complete VACUUM LITE apply (not a dry run, not partial or cancelled, no backlog) writes `_arrowcol_maintenance/vacuum-lite.json` under the table root (never inside `_delta_log`): `{table_id, version, cutoff_ms}`. It states that every remove in commits `0..=version` with `deletionTimestamp < cutoff_ms` was physically deleted or confirmed absent. `version` is the version that VACUUM actually replayed. The record is bound to the table id and is monotonic: it is replaced only by a record with both a greater-or-equal version and a greater-or-equal cutoff. It is written with an exclusive lock, temp file, fsync and rename on local roots, and with create-if-absent or etag-conditional puts on object stores.

**Proof.** A plan picks the newest classic checkpoint `C` such that:

1. checkpoint `C` exists, `_last_checkpoint` points at a version `>= C`, and no multi-part, V2 or compacted log object exists at or below it;
2. an independent, hand-written replay of every JSON commit `0..=C` (every file must be present) yields the same recovery-relevant state as both a direct Parquet read of checkpoint `C` and Delta Kernel loading the snapshot at `C` from it. That state is: the table id; every active add path with its normalized `partitionValues` and `size`; the protocol (reader/writer versions and features); the metadata schema, `partitionColumns` and `configuration`; and `appId -> version` for every application transaction (publications, maintenance plans and the `arrowcol.purge.before-ms` watermark). Per-file `stats` are advisory and not compared, and nothing is compared as serialized bytes. Txn-retention properties are refused;
3. the progress record has the same table id, `version >= C`, and every remove in `0..=C` has a `deletionTimestamp` (missing fails) older than its `cutoff_ms`. Kernel checkpoints drop expired tombstones, so this is what keeps pruning from leaking never-vacuumed files;
4. every deleted commit (`< C`) is older than `now - log retention`, using the later of `commitInfo.timestamp`/`inCommitTimestamp` and the file's modification time. `C.json` is retained, so its age does not matter, but its removes still count for rule 3. Every checkpoint below `C` that would be deleted (including an old floor checkpoint when the floor advances) must also have a modification time older than the cutoff; otherwise the plan is `noop` and a pending apply is `stale-plan`.

Like Delta's own log cleanup (which keeps commits from the latest checkpoint version on), pruning never deletes `C.json`: writers such as delta-rs need the commit at the checkpoint version to append. A `_last_checkpoint` that is malformed is an `unsupported` error for pruning (checkpointing treats it as absent and repairs it). One that points below the durable floor, for example after a delayed older checkpoint, makes the plan `noop` and a pending apply `stale-plan`, with nothing deleted.

Log retention is `--log-retention-secs`, defaulting to `delta.logRetentionDuration` (30 days when unset). A shorter value needs `--unsafe-short-vacuum-for-tests` and a marked disposable temp root, exactly like short VACUUM retention.

**Apply.** Apply re-proves everything against fresh state and checks publication authority. It then durably writes `_arrowcol_maintenance/log-floor.json` (`{table_id, checkpoint_version: C}`; table-bound, never lowered, fsynced) **before** the first delete. It then deletes `NNN.json` below `C` and checkpoints below `C` in ascending order. `NotFound` counts as already absent. At most `--max-prune-files` (default 10,000) objects are deleted per run; the rest is reported as backlog. It stops between deletes on cancel or deadline with a `partial` outcome. Checkpoint `C`, `C.json`, `_last_checkpoint` and anything newer are never deleted, and no Delta commit is written.

After a floor `F` exists, the next proof replays the Parquet contents of checkpoint `F` plus JSON `F+1..=C'`; checkpoint `F`'s tombstones must also be older than the progress cutoff. The retained `F.json` becomes deletable when the floor advances, so it must pass the same age and remove checks. A run interrupted after writing the floor is completed by a later plan that re-verifies checkpoint `F` against Kernel and deletes the leftovers below it (JSON `< F`). The full proof held before that floor was written.

### Log growth

Without `prune-log`, `_delta_log` grows without bound: VACUUM LITE proves tombstones from the log, and `checkpoint` bounds replay *time*, not log *size*. VACUUM refuses with `resource_limit` once the replayed history exceeds `max_log_files` (100,000 commits); it never falls back to a partial scan.

With `vacuum`, `checkpoint` and `prune-log` scheduled, the **number** of JSON commits and old checkpoint files plateaus at about max(VACUUM retention, log retention) worth of commits, plus the checkpoint interval. A commit becomes prunable only once its removes are vacuumed (VACUUM retention, at least 7 days) and it is older than the log retention (30 days by default). That plateau is reachable inside a short stress window only under the test overrides (`--vacuum-retention-secs 0 --log-retention-secs 0 --unsafe-short-vacuum-for-tests` on a disposable root).

**Known limitation: total log bytes are not bounded.** Checkpoints carry every application transaction forever, because transaction retention is disallowed. That includes one `arrowcol.publication.<uuid>` per receipt and one `arrowcol.purge.plan.<uuid>` per maintenance plan. The newest checkpoint therefore grows with the number of publications and plans. Pruning bounds only the count of historical JSON commits and old checkpoint files, not total `_delta_log` bytes. Only classic single-file checkpoints are supported.

## Usage

```bash
# One-shot dry run, then apply
delta-maintenance --root file:///data/delta/ --table events --operation optimize --operation purge \
  --granularity minute --purge-retention-secs 86400
delta-maintenance ... --apply

# Scheduled service with Prometheus metrics on 127.0.0.1:9467
delta-maintenance --root abfss://c@acct.dfs.core.windows.net/delta/ --table events \
  --operation optimize --operation vacuum --interval-secs 300 --apply
```

### Scheduled mode

- Tables are processed one operation at a time per table. Use `--max-concurrent-tables` (1..=16) to process several tables in parallel.
- When a cycle finds no work, the next interval doubles, up to `--max-idle-backoff-secs`.
- Each operation is limited by `--operation-timeout-secs`.
- A heartbeat line goes to stderr every cycle.
- SIGINT and SIGTERM stop the service between bounded steps.

### Final-compaction policy and evidence

`--sealed-final` is an opt-in **OPTIMIZE churn policy**, not a verified seal,
publisher-release receipt, reader lease, or retention override. Its default is
false. It includes valid newest and still-open ingestion partitions in the same
small-file planner. Null/malformed partitions remain skipped. Every existing
byte, row, bin, action and decoding bound still applies; one file per partition
is not guaranteed. Unrelated blind appends remain compatible: apply revalidates
each planned input and preserves files appended after planning.

The CLI accepts this flag only for one-shot `--operation optimize`, both dry-run
and apply. It rejects `--interval-secs`, `--max-cycles`, and any mixed or other
operation before opening tables or starting telemetry/metrics. It changes no
VACUUM, PURGE, log-cleanup, publication-transaction or retention safety rule.
Compaction adds its existing maintenance-plan transaction; existing publication
transactions remain intact.

```bash
# Inspect the real final-compaction plan; no files or commits are changed.
delta-maintenance --root file:///data/delta/ --table events --operation optimize \
  --granularity minute --sealed-final
# Apply one bounded plan; repeat planning explicitly if work remains.
delta-maintenance --root file:///data/delta/ --table events --operation optimize \
  --granularity minute --sealed-final --apply
```

Each OPTIMIZE JSON line includes `optimize.config` (the effective policy and
limits, including its resolved `now_ms`) and `optimize.plan` (the actual typed
plan, with `header.read_version`, partition-valued input `bins`, `backlog_files`,
`protected_partitions`, and `skipped_partitions`). A null config `layout` means
the existing schema-derived default layout is used. Outputs cannot be known
until real Parquet rewriting: `report.outputs` and `report.files_added` describe
actual apply results, not estimated dry-run outputs. `report.read_version` is
the plan version; `report.version` is the observed/apply result version.
Evidence is retained on an apply error when planning succeeded.

A fresh **dry-run** empty plan still has `report.outcome = "planned"`, not
`"noop"`. An empty-plan **apply** has `report.outcome = "noop"` and makes no
commit. Zero `report.files_added` alone proves neither emptiness nor success.
Callers must require a successful exit, exactly one error-free OPTIMIZE result
for every expected table, and real `optimize.plan.bins = []` with no backlog.
For a complete final-compaction gate they must also reject skipped/protected
partitions and verify the expected policy/config, table/root/run bindings and
the current versions. Missing reports or operation failures are not no-ops.

Lifecycle orchestration must independently establish positive writer release,
clean mutator-drain evidence and run/root/table/SDK bindings before final
compaction, and join all maintenance processes before capturing immutable reader
snapshots. This CLI neither verifies those receipts nor orchestrates repeated
passes. OPTIMIZE preserves exact rows, schema, partition values and history
using `dataChange=false`; it does not promise a particular external cache
convergence time or discard historical acceleration work.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | An operation failed. Its JSON report includes `error_kind`. |
| 2 | Invalid configuration |

## Metrics

**Prometheus.** `GET /metrics` is served on `--listen` (default `127.0.0.1:9467`) in scheduled mode. In containers, pass `--listen 0.0.0.0:9467`.

The only labels are `operation` and `outcome`. Table names are never used as labels. Series:

- `delta_maintenance_runs_total`, with outcomes: planned, noop, committed, already_applied, deleted, checkpointed, pruned, partial, and error kinds
- Files, rows and bytes processed
- Commit collisions, retries and exhausted retries
- Backlog
- Retention cutoff
- Cycles
- Heartbeat

**OTLP.** When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, the same instruments are exported over OTLP http/protobuf every 2 s. Delta Kernel metrics are attached through `arrow_deltalake::telemetry::kernel_metrics_reporter()`. Use `RUST_LOG` to control stderr logs (default `warn`).

## Container

The `Dockerfile` installs the Azure CLI with the shared signed helper `infra/docker/install-azure-cli.sh` and runs as UID 10001.

`AZURE_CONFIG_DIR=/var/lib/delta-maintenance-azure` is an empty 0700 directory owned by UID 10001. Runners should mount an isolated runtime directory over it. No credentials or config are baked into the image.

`tini` is PID 1 so orphaned Azure CLI descendants are reaped.

The `HEALTHCHECK` runs a bounded `curl` (3 s, 1 MiB) against the real Prometheus listener at `DELTA_MAINTENANCE_HEALTH_URL` (default `http://127.0.0.1:9467/metrics`). Only scheduled runs serve the listener, so one-shot runs never report healthy. Set the variable when using a different `--listen`.

## Nx

```bash
npx nx run delta-maintenance:verify       # format-check, lint, build, test
npx nx run delta-maintenance:image-smoke
```
