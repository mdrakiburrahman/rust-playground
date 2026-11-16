# Single-table throughput campaign

| Item | Contract |
| --- | --- |
| Score | Sustained newly committed Delta rows/s from commit-window evidence; admission and rewrites are excluded. |
| Scope | Local mode, one public DMV table (`sqlserver_active_sessions` quick profile), one writer replica, one leased box per cohort. |
| Maintenance | Enabled and applying in every cell: optimize, purge, vacuum, checkpoint, and prune-log. Idle work must be a no-op; due work stays enabled. |
| Verdict | Set `dataflow_verdict_policy` to `single_hot_table_campaign` to make QAP lag a separate finding. Missing QAP observations remain UNKNOWN/non-PASS; default legacy semantics are unchanged. |
| Files | `oat-config-throughput-C1.json` through `C8.json` are bounded sweep manifests, not launch configs. Resolve one candidate against the referenced base OAT config before each run. |
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

Each comparison contains at most four candidates; sequential comparisons in a cohort stay on the same box. Candidate changes are overlays on the baseline, not additional writers or tables.

| Cohort | Comparisons |
| --- | --- |
| C1 | Uncompressed, Snappy, Zstd 1/3; then Zstd 3 vs 6 refinement. LZ4_RAW is excluded unless every reader is proven compatible. |
| C2 | Dictionary on/off by cardinality; Delta Binary Packed and Byte Stream Split on schema-compatible columns. |
| C3 | 16K/64K/128K row groups, page rows, input/rollover caps around 128 MiB, and coalescing off/on as separate comparisons. |
| C4 | No sort vs nonconstant, schema-valid `session_id`, `database_name`, or `sample_time_utc`; never use constant `customer_id`. |
| C5 | Workers, streams, connections, corpus rows, send interval (including zero), and IPC compression as separate comparisons. |
| C6 | DFE cores, CPU workers, upload concurrency, flush concurrency, then router pooling if supported by the registry. |
| C7 | Maintenance cadence, small-file threshold, bounded retry budget, and checkpoint interval; every operation remains enabled and applying. |
| C8 | Commit window and files-per-commit. Staging-overlap candidates are marked pending until the E1 registry exposes and validates that control. |

Generate or refresh manifests with:

```bash
npx --no-install tsx projects/arrowcol-oat/throughput-sweeps.ts \
  --registry docs/experiments/arrowcol-oat/oat-config.tuned.json \
  --out docs/experiments/arrowcol-oat
```

The generator hashes the tunables profile used to seed the manifests. When the contract lands, point it at the E1 registry source and regenerate; do not hand-copy registry names or bounds into the manifests. A `registryStatus` of `pending-E1-registry` is not runnable and must not be silently treated as a supported tunable.

## Picture input

The results JSON is an array with `cohort`, `candidate`, `factorX`, `valueX`, `factorY`, `valueY`, positive integer `repeat`, `committedRowsPerSecond` (number or null), and optional `diedFirst` and `comment`. The renderer plots only observed numeric rates and shows repeat min/max ranges; null is never coerced to zero. If Kustainer dies, store `committedRowsPerSecond: null`, `diedFirst: "Kustainer"`, and a concrete `comment`. That downstream failure remains visible without relabeling collector evidence as a measured rate.
