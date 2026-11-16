# Single-table throughput campaign

| Item | Contract |
| --- | --- |
| Score | Sustained newly committed Delta rows/s from commit-window evidence; admission and rewrites are excluded. |
| Scope | Local mode, final public DMV table `sqlserver_availability_replica_states`, replicas `[1]`, one leased box per cohort. |
| Profile | Every manifest sets `campaign_profile: "single_hot_table"`: one DMV table, replicas `[1]`, WAL disabled, and continuous maintenance applying optimize, purge, vacuum, checkpoint, and prune-log. |
| Maintenance | Continuous, enabled, and applying in every cell. Idle work must be a no-op; due work stays enabled. |
| Verdict | Set `dataflow_verdict_policy` to `single_hot_table_campaign` to make QAP lag a separate finding. Missing QAP observations remain UNKNOWN/non-PASS; default legacy semantics are unchanged. |
| Files | `oat-config-throughput-C1.json` through `C8.json` are provisional bounded manifests, not launch configs; all are marked `pending-E1-registry` and are not runnable until checked against the E1 registry. |
| Pictures | Result pictures may show exploratory observations; no sustainable winner is shown until contiguous 30-second buckets, reconciliation, and validated timestamped pressure/maintenance-debt evidence qualify. The current runner's `backlog: null` keeps results unqualified. |

## Qualification

PR #125's final qualification supports only **16 workers × 1 stream**. It does not qualify 8×16 or extreme 16×16, and those shapes are not capacity recommendations. Sweep them as unproven candidates only, with each axis changed separately and all actual rates recorded.

## Run protocol

| Stage | Bound and acceptance |
| --- | --- |
| Window | Complete 30-second commit windows. A pilot is capped at 600 seconds. |
| Stable point | At least five complete windows; committed-rate range / mean ≤5%; no sustained positive pending rows, bytes, or oldest-age trend. |
| Ramp | At most eight coarse points plus four refinements per frozen configuration. Keep the first invalid, stalled, or backlog-growing point. |
| Repeats | Baseline and winner three times in alternating order with identical effective load and resources. |
| Latency | Measure at approximately 50% and 90% of the sustainable ceiling. Report the throughput/latency trade-off; no hard latency cutoff. |
| Stop | Stop admission at ≥90% of a hard memory/buffer cap, or after two complete windows with outstanding work and zero commit progress. CPU saturation alone is a ceiling observation. |

Compare candidates within the same box. Change one variable at a time; only test interactions on top picks. Record requested and effective values, full-window rates, repeat ranges, memory and pending trends, file sizes, maintenance work, latency, and what failed first. A peak or overload point is not a winner.

## Cohorts

Each runnable comparison contains at most four candidates, except the C1 coarse codec sweep (eight); sequential comparisons in a cohort stay on the same box. Candidate changes are overlays on the baseline, not additional writers or tables. The C8 staging-overlap comparison is outside the runnable cohort in `pending_candidate_sets` with status `pending-L1b`; it depends on L1b and must not enter E1 registry validation until that tunable ships.

| Cohort | Comparisons |
| --- | --- |
| C1 | Coarse codec comparison: uncompressed, Snappy, LZ4_RAW, gzip, Brotli, and Zstd 1/3/6 (eight points). If Zstd wins, refine around its winning level with up to three adjacent registry-supported levels while retaining the winner as control (four candidates max). |
| C2 | Dictionary on/off by cardinality; Delta Binary Packed and Byte Stream Split on schema-compatible columns. |
| C3 | Row groups and maxRows include 16K/64K/128K/1M; page rows are separate; input-cap candidates include 128/256/512 MiB, setting layout, per-flush, and writer caps together so effective cap is `min(layout, per-flush, writer)`; coalescing remains a separate comparison. |
| C4 | Low-to-high cardinality sort candidates for the final availability-replica table: none; `sample_time_utc`; `replica_server_name, sample_time_utc`; `availability_group_id, replica_server_name, sample_time_utc`. `customer_id` is constant per client and excluded. |
| C5 | Workers, streams, connections, corpus rows, send interval (including zero), and IPC compression as separate comparisons. |
| C6 | DFE cores, CPU workers, upload concurrency, flush concurrency, then router pooling if supported by the registry. |
| C7 | Maintenance cadence, small-file threshold, bounded retry budget, and checkpoint interval; every operation remains enabled and applying. |
| C8 | Runnable comparisons cover commit window and files-per-commit. Staging-overlap remains a separate `pending-L1b` candidate set until the L1b tunable ships. |

Generate or refresh manifests with:

```bash
npx --no-install tsx projects/arrowcol-oat/throughput-sweeps.ts \
  --registry docs/experiments/arrowcol-oat/oat-config.tuned.json \
  --out docs/experiments/arrowcol-oat
```

The generator hashes the PR #125 tunables profile as a seed; E1 capability metadata is not yet wired as a complete typed registry. Each runnable candidate must therefore be rendered and passed through `--validate-config` against a recorded E1 source SHA. Record the result as `config-validated`; do not label it registry-generated or registry-complete. Keep the registry metadata status pending until it is authoritative. The C8 staging-overlap set remains outside runnable comparisons as `pending-L1b`. The current per-manifest outcomes and machine-readable 82-cell matrix are in `oat-config-throughput-validation.json`; they record config validation only, not registry completeness.

## Picture input

The results JSON is an array of `cohort`, `candidate`, factor/value pairs, positive integer `repeat`, `expectedRepeats`, and `committedRowsPerSecond` (number or null). A Kustainer failure does not invalidate independently measured Delta commits: preserve the numeric Delta rate, set unavailable QAP fields to null, and retain `diedFirst: "Kustainer"`, a concrete `comment`, and collector-only status.

Sustainable leaderboard entries require at least three expected repeats, with exactly one official OAT run per repeat. Each run carries the raw run status, result rows, expected source SHA, mode, and config; the existing `single_hot_table_campaign` verdict checks ingest validity. The renderer reads `durable_commits.complete_30_second_commit_windows` from that run’s `evidence_json`: the measurement must span at least 150 seconds and include every positive, contiguous, complete 30-second bucket from that single run. Separate runs or gaps cannot be combined into a sustained repeat. Reconciliation is read from the emitted `reconciliation` object and must be settled and within its lower/upper bounds; exact equality is required only when `exact_match_required` is true. `factorConfigPaths` must name two distinct actual leaf settings (under `tunables` or `dfe_worker_concurrency`), never parent objects; removing them must leave identical source, image, resource, and load baselines. Qualified rows are sorted by descending repeat mean, then candidate name.

Complete 30-second buckets must sum exactly to the run output when the measured duration is an exact multiple of 30 seconds; a partial trailing interval may leave output rows beyond the complete-bucket sum. Compaction debt is accepted only from complete maintenance process logs with no dropped reports or maintenance operation errors, an active policy for `sqlserver_availability_replica_states`, and a successful applied non-probe `optimize` report whose `observed_at_ms` falls inside the same official run interval. The `compaction_debt` source and integer fields must validate, with backlog files/bytes no greater than eligible files/bytes; older reports are not evidence, and missing, failed, out-of-window, or truncated samples remain unavailable. This proves only bounded optimize-plan debt; other maintenance due-work vectors remain unknown. The current OAT producer still emits `backlog: null` and has no timestamped publication-pressure series, so no sustainable winner may be shown until E1/L4 pressure evidence is present and validated. Retry counts alone do not prove bounded maintenance debt. Committed Delta rates remain visible as exploratory observations; no missing proof is synthesized. Duplicate candidate/repeat identities and incomplete or incompatible baselines are rejected, and completed/expected repeat counts remain visible.
