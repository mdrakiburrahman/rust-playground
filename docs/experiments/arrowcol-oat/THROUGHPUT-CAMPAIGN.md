# Single-table throughput campaign

| Item | Contract |
| --- | --- |
| Score | Sustained newly committed Delta rows/s from commit-window evidence; admission and rewrites are excluded. |
| Scope | Local mode, final public DMV table `sqlserver_availability_replica_states`, replicas `[1]`, one leased box per cohort. |
| Profile | Every manifest sets `campaign_profile: "single_hot_table"`: one DMV table, replicas `[1]`, WAL disabled, and continuous maintenance applying optimize, purge, vacuum, checkpoint, and prune-log. |
| Maintenance | Continuous, enabled, and applying in every cell. Idle work must be a no-op; due work stays enabled. |
| Verdict | Set `dataflow_verdict_policy` to `single_hot_table_campaign` to make QAP lag a separate finding. Missing QAP observations remain UNKNOWN/non-PASS; default legacy semantics are unchanged. |
| Files | `oat-config-throughput-C1.json` through `C8.json` are provisional bounded manifests, not launch configs; all are marked `pending-E1-registry` and are not runnable until checked against the E1 registry. |
| Pictures | `npx --no-install tsx projects/arrowcol-oat/campaign-results.ts --input <results.json> --out <picture-dir>` writes one SVG per cohort. |

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

The generator currently hashes the PR #125 tunables profile only as a seed. Every generated manifest is marked `pending-E1-registry` and is not runnable: names, types, defaults, and bounds have not yet been validated against the E1 contract. When that contract lands, validate the candidate overlays against it, point the generator at its source, and regenerate; do not hand-copy registry names or bounds into the manifests. C8 staging-overlap is additionally pending until E1 actually exposes that control.

## Picture input

The results JSON is an array with `cohort`, `candidate`, `factorX`, `valueX`, `factorY`, `valueY`, positive integer `repeat`, `committedRowsPerSecond` (number or null), and optional `diedFirst` and `comment`. The renderer plots only observed numeric rates and shows repeat min/max ranges; null is never coerced to zero. If Kustainer dies, store `committedRowsPerSecond: null`, `diedFirst: "Kustainer"`, and a concrete `comment`. That downstream failure remains visible without relabeling collector evidence as a measured rate.
