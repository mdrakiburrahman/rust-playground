# Single-table throughput campaign

| Item | Contract |
| --- | --- |
| Score | Sustained newly committed Delta rows/s from commit-window evidence; admission and rewrites are excluded. |
| Scope | Local mode, final public DMV table `sqlserver_availability_replica_states`, replicas `[1]`, one leased box per cohort. |
| Profile | Every manifest sets `campaign_profile: "single_hot_table"`: one DMV table, replicas `[1]`, WAL disabled, and continuous maintenance applying optimize, purge, vacuum, checkpoint, and prune-log. |
| Maintenance | Continuous, enabled, and applying in every cell. Idle work must be a no-op; due work stays enabled. |
| Verdict | Set `dataflow_verdict_policy` to `single_hot_table_campaign` to make QAP lag a separate finding. Missing QAP observations remain UNKNOWN/non-PASS; default legacy semantics are unchanged. |
| Files | `oat-config-throughput-C1.json` through `C8.json` are bounded manifests, not launch configs. Their eight baselines and 82 existing candidates were config-validated at integration source `a24ec92ea7a4c9b3b0397d38ebe231844714c7e8`; this does not establish runtime readiness. |
| Pictures | Result pictures may show exploratory observations; no sustainable winner is shown until contiguous 30-second buckets, reconciliation, and validated timestamped pressure/maintenance-debt evidence qualify. Missing or unavailable evidence remains UNKNOWN. |

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

Each comparison contains at most four candidates, except the C1 coarse codec sweep (eight); sequential comparisons in a cohort stay on the same box. Candidate changes are overlays on the baseline, not additional writers or tables. The C8 staging-overlap comparison remains outside executable candidates in `pending_candidate_sets` with status `pending-L1b`; it is not config-validated.

| Cohort | Comparisons |
| --- | --- |
| C1 | Coarse codec comparison: uncompressed, Snappy, LZ4_RAW, gzip, Brotli, and Zstd 1/3/6 (eight points). If Zstd wins, refine around its winning level with up to three adjacent registry-supported levels while retaining the winner as control (four candidates max). |
| C2 | Dictionary on/off by cardinality; Delta Binary Packed and Byte Stream Split on schema-compatible columns. |
| C3 | Row groups and maxRows include 16K/64K/128K/1M; page rows are separate; input-cap candidates include 128/256/512 MiB, setting layout, per-flush, and writer caps together so effective cap is `min(layout, per-flush, writer)`; coalescing remains a separate comparison. |
| C4 | Low-to-high cardinality sort candidates for the final availability-replica table: none; `sample_time_utc`; `replica_server_name, sample_time_utc`; `availability_group_id, replica_server_name, sample_time_utc`. `customer_id` is constant per client and excluded. |
| C5 | Workers, streams, connections, corpus rows, send interval (including zero), and IPC compression as separate comparisons. |
| C6 | DFE cores, CPU workers, upload concurrency, flush concurrency, then router pooling if supported by the registry. |
| C7 | Maintenance cadence, small-file threshold, bounded retry budget, and checkpoint interval; every operation remains enabled and applying. Cadence 30/60/120 seconds is explicitly exploratory: it cannot reliably provide an observation in every 30-second window. Idle-backoff is coupled to the cadence to satisfy the configuration constraint. |
| C8 | Comparisons cover commit window and files-per-commit. Staging-overlap remains a separate `pending-L1b` candidate set until its consumer and safety clearance are integrated. |

Generate or refresh manifests with:

```bash
npx --no-install tsx projects/arrowcol-oat/throughput-sweeps.ts \
  --registry docs/experiments/arrowcol-oat/oat-config.tuned.json \
  --out docs/experiments/arrowcol-oat
```

Render CLI-consumable baselines and candidate overlays from the checked-in manifests:

```bash
npx --no-install tsx projects/arrowcol-oat/throughput-sweeps.ts --render-all \
  --out out/arrowcol-oat/sweep-configs
cargo run --locked --package arrowcol-oat-rust -- --validate-config \
  out/arrowcol-oat/sweep-configs/C1/codec/snappy.json
```

The generator hashes the PR #125 tunables profile as a seed, not a complete capability registry. Rendering rejects a different seed. `oat-config-throughput-validation.json` records SHA-256 per rendered config and the exact source of the validator: 8 baselines and 82 candidates passed `--validate-config` at `a24ec92ea7a4c9b3b0397d38ebe231844714c7e8`; no staging-overlap candidates were included. This is config validation only, not an admission, effective-setting, runtime, or sustainable qualification. `runnable: false` retains the runtime hold. Regeneration resets each manifest's validation status; revalidate after source, seed, manifest, or rendered config changes rather than relying on the old matrix.

The baseline mirrors the unscored diagnostic's 150-second measurement, 15-second maintenance interval and idle-backoff, hour granularity, 30-day purge retention, one table, and one writer. A 15-second cadence does not guarantee per-window observations. Hour granularity with closed-only compaction can accumulate protected current-partition backlog; zero eligible work alone does not establish bounded debt. New maintenance knobs are not executable candidates until their consumer and OAT projection are integrated and validated.

**Before any authorized run**, replace the shared template's Compose project/state paths and output destinations with isolated run-owned paths, set the correct local endpoint/bind settings, and revalidate the modified config; rendered files reuse template paths and must not be run concurrently as-is. Record requested and effective values separately.

## Picture input

The results JSON is an array of `cohort`, `candidate`, factor/value pairs, positive integer `repeat`, `expectedRepeats`, and `committedRowsPerSecond` (number or null). A Kustainer failure does not invalidate independently measured Delta commits: preserve the numeric Delta rate, set unavailable QAP fields to null, and retain `diedFirst: "Kustainer"`, a concrete `comment`, and collector-only status.

Sustainable leaderboard entries require at least three expected repeats, with exactly one official OAT run per repeat. Each run carries the raw run status, result rows, expected source SHA, mode, and config; the existing `single_hot_table_campaign` verdict checks ingest validity. The renderer reads `durable_commits.complete_30_second_commit_windows` from that run’s `evidence_json`: the measurement must span at least 150 seconds and include every positive, contiguous, complete 30-second bucket from that single run. Separate runs or gaps cannot be combined into a sustained repeat. Reconciliation is read from the emitted `reconciliation` object and must be settled and within its lower/upper bounds; exact equality is required only when `exact_match_required` is true. `factorConfigPaths` must name two distinct actual leaf settings (under `tunables` or `dfe_worker_concurrency`), never parent objects; removing them must leave identical source, image, resource, and load baselines. Qualified rows are sorted by descending repeat mean, then candidate name.

Complete 30-second buckets must sum exactly to the run output when the measured duration is an exact multiple of 30 seconds; a partial trailing interval may leave output rows beyond the complete-bucket sum. Committed Delta rates remain exploratory when proof is incomplete. Duplicate candidate/repeat identities and incompatible baselines are rejected; completed/expected repeat counts remain visible.

| Evidence | Current gate |
| --- | --- |
| Publication pressure | The approved `backlog` version-1 contract covers unique table-owner admissions not confirmed durably committed, **not pre-queue or entire-client backlog**. Direct scrape request intervals must align with the official measurement interval and provide at least two valid snapshots per complete commit window. Single-flight requests may skip ticks: a 1,000 ms cadence permits a 3,000 ms timeout; gaps must remain within interval plus timeout. Each recorded request budget must be positive, no larger than the configured timeout or remaining measurement time, and cover its actual elapsed time. Cadence and configured timeout are independently capped at 30,000 ms. Failed/unavailable/null gauges, dropped samples, gaps, reversed clocks, and positive window-mean publications/rows/bytes/age trends are rejected without zero-filling. Parser input is capped at 10,000 samples and requires safe integer counters. |
| Maintenance due-work | Require complete, source- and unit-valid `maintenance_due_work` for every enabled operation in each of at least five contiguous half-open 30-second windows. Every report must have a valid status and timestamped due-work object; missing/null/invalid reports cannot be skipped or assumed outside the window. Missing, incomplete, failed, dropped, stale, or out-of-window observations are **UNKNOWN**, with no carry-forward. Accept only the producer's operation/source/unit mappings; complete zero is measured only when counts are explicitly zero. |
| Sustained maintenance debt | Require eligible counts for every operation and backlog counts except CHECKPOINT, where a null backlog count is permitted. OPTIMIZE/PURGE also require both measured byte counts; VACUUM, CHECKPOINT, and PRUNE_LOG may retain paired null byte vectors when bytes are not measured. Never zero-fill nulls. Positive growth in either measured series, inconsistent byte coverage, missing operation coverage, or unsupported debt vectors prevents qualification. A no-write OPTIMIZE negative probe is valid only when `bounded_physical_inventory` reports a complete measured zero. Retry counts are not debt proof. |
| Cross-language approval | Actual Rust `OperationResult` fixtures from L2@`0e04404` pass through the E1@`95987a6` projection and TypeScript parser; this proves schema compatibility only. One-pass fixtures remain below the five-window gate. Synthetic contract tests are not measured output; neither fixture proves live pressure or sustainable performance. |
