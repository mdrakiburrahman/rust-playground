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

## Safety

- **Dry run by default.** The service plans and prints JSON reports and changes nothing unless you pass `--apply`.
- **Applying revalidates first.** Before applying a plan the service rechecks the table id, metadata, protocol and the exact readset. Stale plans fail with `stale_plan` or `metadata_changed`.
- **Commit retries are bounded.** Commit collisions retry at most 8 times within 10 seconds, with jitter.
- **Newest partition is protected.** The newest, still-open ingestion partition is never optimized or purged.
- **Bad partitions are never deleted.** Malformed or null partition values fail closed.
- **No empty commits.** No-op runs and repeated runs leave the table version unchanged. The watermark is never committed on its own.
- **VACUUM retention has a 7-day floor.** Shorter retention needs `--unsafe-short-vacuum-for-tests` and a marked disposable temp root.
- **VACUUM needs complete history.** It requires contiguous JSON commits `0..=version` and fails visibly otherwise.
- **VACUUM rejects unsafe paths.** Traversal and external paths are rejected.
- **VACUUM reports partial progress.** Cancelling mid-delete reports a `partial` outcome.

On Azure the service opens tables through the shared publisher session and closes it after the run; a failed close exits nonzero and leaves its registration in place. Reset execution stays disabled, so the service does not claim infra-reset safety.

### Log growth

`_delta_log` grows without bound by design. VACUUM LITE proves tombstones from contiguous JSON commits `0..=version`, and the service never deletes JSON history, checkpoints, or the transaction identities they carry. Each append adds one JSON commit, and each `checkpoint` adds one Parquet file. `checkpoint` bounds replay *time*, not log *size*. VACUUM refuses with `resource_limit` once the log exceeds `max_log_files` (100,000 commits); it never falls back to a partial scan. Long-running tables need a future, explicitly designed history-cleanup policy that preserves those proofs.

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

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success |
| 1 | An operation failed. Its JSON report includes `error_kind`. |
| 2 | Invalid configuration |

## Metrics

**Prometheus.** `GET /metrics` is served on `--listen` (default `127.0.0.1:9467`) in scheduled mode. In containers, pass `--listen 0.0.0.0:9467`.

The only labels are `operation` and `outcome`. Table names are never used as labels. Series:

- `delta_maintenance_runs_total`, with outcomes: planned, noop, committed, already_applied, deleted, partial, and error kinds
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
