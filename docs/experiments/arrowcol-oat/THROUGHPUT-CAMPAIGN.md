# Single-table throughput campaign

| Item | Contract |
| --- | --- |
| Score | Sustained newly committed Delta rows/s from commit-window evidence; admission and rewrites are excluded. |
| Scope | Local mode, final public DMV table `sqlserver_availability_replica_states`, replicas `[1]`, one leased box per cohort. |
| Profile | Every manifest sets `campaign_profile: "single_hot_table"`: one DMV table, replicas `[1]`, WAL disabled, and continuous maintenance applying optimize, purge, vacuum, checkpoint, and prune-log. |
| Guard and envelope | Every baseline requests `overload_guard` enabled, cap fraction 0.90, two zero-progress windows, and a 4 GiB local collector memory limit. This common initial request is not measured headroom or proof of host suitability. |
| Maintenance | Continuous, enabled, and applying in every cell. Idle work must be a no-op; due work stays enabled. |
| Verdict | Set `dataflow_verdict_policy` to `single_hot_table_campaign` to make QAP lag a separate finding. Missing QAP observations remain UNKNOWN/non-PASS; default legacy semantics are unchanged. |
| Files | `oat-config-throughput-C1.json` through `C8.json` are bounded manifests, not launch configs. All 116 configs passed Rust config validation at source `22c1490`; they remain runtime-unqualified. See `oat-config-throughput-validation.json` for evidence. |
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

Every executable comparison contains at most four entries, including any repeated matched control; sequential comparisons in a cohort stay on the same box. Candidate changes are overlays on the baseline, not additional writers or tables. The C8 staging-overlap comparison remains outside executable candidates in `pending_candidate_sets` with status `pending-L1b`; it is not config-validated.

| Cohort | Comparisons |
| --- | --- |
| C1 | Three bounded comparisons share an explicit uncompressed control: Zstd 1/3/6 (four entries), Snappy/LZ4_RAW (three), and gzip/Brotli (three). If Zstd wins, refine around its winning level with up to three adjacent registry-supported levels while retaining the winner as control (four entries max). |
| C2 | Dictionary on/off; Delta Binary Packed and Byte Stream Split on integer columns; explicit Plain, Delta Length Byte Array, and Delta Byte Array on `availability_group_name` with dictionary disabled. The client consumes projected `dictionary_handling`; `hydrate` is its default, while `resend` is rejected for the frozen plain DMV schema. |
| C3 | Row groups and maxRows include 16K/64K/128K/1M; page rows are separate; input-cap candidates include 128/256/512 MiB, setting layout, per-flush, and writer caps together so effective cap is `min(layout, per-flush, writer)`; coalescing, flush age (1s/5s/15s/60s), and age-flush policy (`immediate`/`drain_ready`) are separate comparisons. |
| C4 | Low-to-high cardinality sort candidates for the final availability-replica table: none; `sample_time_utc`; `replica_server_name, sample_time_utc`; `availability_group_id, replica_server_name, sample_time_utc`. `customer_id` is constant per client and excluded. |
| C5 | Workers, streams, connections, shared/independent data mode, corpus rows, send interval (including zero), and IPC compression as separate comparisons. Worker, stream, and connection candidates override only their own axis against the committed 2×2×1 seed. gRPC request and response directions use matched client/receiver settings for `none`, `gzip`, and `zstd`; two additional comparisons cover gzip-request/zstd-response and zstd-request/gzip-response. These are configuration-only candidates, not performance or runtime qualification. |
| C6 | DFE cores, CPU workers, upload limit, and flush concurrency. Collector `buffered_multipart` upload is excluded by the pinned backend capability (no conditional completion); request concurrency is only valid with that mode. |
| C7 | Maintenance cadence, stale-plan retry bound, sealed-partition preference, small-file threshold, checkpoint interval, input-read strategy (`memory`/`spool`/`range`), granularity, and OPTIMIZE upload mode. All operations remain enabled and applying; buffered-multipart uses the coupled 8 MiB buffer and is exploratory/unqualified. Cadence 30/60/120 seconds is exploratory; idle-backoff is coupled to cadence. |
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
  out/arrowcol-oat/sweep-configs/C1/codec-snappy-lz4raw/snappy.json
```

The generator hashes the PR #125 tunables profile as a seed, not a complete capability registry. Rendering rejects a different seed. The current matrix contains 8 baselines and 108 candidates (116 configs); all 116 passed the production `--validate-config` binary built with `cargo build --locked --package arrowcol-oat-rust` from source commit `22c1490edd7479831916be701213061fa39fb7d4`. The binary SHA-256 is `9c5bc506371ba533ade8bc3477be17ade82cda2a3139988d23cf3219f0706901`. `oat-config-throughput-validation.json` binds each rendered config SHA-256 to its final manifest SHA-256; the successor session evidence preserves exact commands, input hashes, exit codes, stdout/stderr, build logs, and hash-command output. The Rust validator source set has no diff from pinned base `5687e6459c424362f4504d21c32f954aa3b07d05`. The earlier 112-config result remains owner-attested only and is superseded. No staging-overlap candidates are included. Parser success proves config shape only, not admission, effective settings, runtime, or sustainable qualification. `runnable: false` retains the runtime hold. Regeneration resets validation status; do not treat config generation as validation.

The directional gRPC set is in supplemental `oat-config-throughput-C5-grpc.json`, preserving the original C5 manifest and evidence. Its 30 candidates include the original 21 plus 9 directional entries and remain non-runnable/unqualified. All 125 rendered configs (8 baselines, 117 candidates) passed strict Rust config validation at source `731085583a486d252faf0d6c410d5cc7173fbde7` using binary SHA-256 `91e474cb7188f323d5b37a276e3668bc208f9878bb9068496798bdf8d2e75f8e` and lock SHA-256 `7fbc71f511e50ac954563d7556827a767f0f371cf7c09af05c35bf6b0a760df3`. `oat-config-throughput-validation-grpc.json` binds each input and manifest hash, exact command/output/exit, and malformed enum/type rejections; the original C5 manifest and 116-config validation matrix are unchanged. This is config-shape evidence only, not runtime or performance qualification.

C3's flush-age comparison requests `tunables.collector.batching.max_age` at 1s, 5s, 15s, and 60s; the shared baseline request is 1s. Record the effective age separately from the request using the collector's startup `consumer_effective_config.batching.max_age` readback. A longer age alone need not produce larger files if row, input-byte, memory, or adaptive target thresholds trigger first. Interpret each run with per-run deltas of `arrowcol_pipeline_flushes_total` by reason (especially `age`, `row_cap`, `batch_cap`, `memory_cap`, `min_input_bytes`, `max_input_bytes`, and `target_parquet_bytes`) alongside physical file sizes and latency. The earlier 231-file observation with all files under 8 MiB does not establish that age was the limiting factor. No performance effect is claimed by this configuration-only comparison.

The C1/C2 manifests distinguish accepted enum values from configured candidates and exclusions. RLE is accepted only for BOOLEAN, absent from the frozen table. Legacy `lz4` remains accepted for compatibility but is excluded from the campaign by consumer/deprecation policy; `lz4_raw` is configured instead. Configured values have not thereby been runtime-screened.

The manifests’ `source_enum_coverage` records typed acceptance, explicit seed requests, effective defaults, configured candidates, exclusions, and pending work separately. Values are grounded in the Tunables and component consumers at source `5687e6459c424362f4504d21c32f954aa3b07d05`; parser acceptance or config validation does not make a value runtime-qualified.

| Path | Accepted, seed request, or configured candidates | Excluded, effective default, or pending |
| --- | --- | --- |
| C2 `transport.dictionary_handling` | Enum accepts `hydrate`/`resend`; seed omits the field | Client consumes the projected value; effective default is `hydrate`. `resend` is excluded because validation rejects it for the frozen plain DMV schema. |
| C5 `transport.ipc_compression` | Candidates `none`, `lz4_frame`, `zstd`; seed explicitly requests `zstd` | None; still runtime-unqualified. |
| C5 `client.data_mode` | Matched `shared`/`independent` candidates; seed omits the field | Both are projected and consumed. Shared uses one corpus copy; independent uses one per worker under the same memory cap. Config-screened only; runtime-unqualified. |
| C5 `transport.grpc_send_compression` | Enum accepts and compares `none`/`gzip`/`zstd` | Matched with `collector.limits.grpc_accept_compression` for the client-to-receiver request direction. Config-screened only; runtime-unqualified. |
| C5 `transport.grpc_accept_compression` | Enum accepts and compares `none`/`gzip`/`zstd` | Matched with `collector.limits.grpc_send_compression` for the receiver-to-client response direction. Config-screened only; runtime-unqualified. |
| C5 `collector.limits.grpc_accept_compression` | Receiver accepts and compares `none`/`gzip`/`zstd` | Matched with client `transport.grpc_send_compression`; projected through `ARROWCOL_COLLECTOR_LIMITS_CONFIG`. Config-screened only; runtime-unqualified. |
| C5 `collector.limits.grpc_send_compression` | Receiver accepts and compares `none`/`gzip`/`zstd` | Matched with client `transport.grpc_accept_compression`; projected through `ARROWCOL_COLLECTOR_LIMITS_CONFIG`. Config-screened only; runtime-unqualified. |
| C6 `collector.upload.mode` | Typed values; seed omits mode; no candidate | Effective default is `put_create`; `buffered_multipart` is excluded because the pinned collector lacks conditional completion. |
| C7 `maintenance.input_read_strategy` | Candidates `memory`, `spool`, `range`; seed omits the field | Effective default is `memory`; these change OPTIMIZE input reading, not WAL/collector durability. |
| C7 `maintenance.granularity` | Candidates `minute`, `hour`, `day`; generated baseline explicitly uses `hour` | No exclusion; maintenance operations remain fixed and enabled. |
| C7 `maintenance.optimize_upload_mode` | Candidates `put_create`, `buffered_multipart`; seed omits the field | Effective default is `put_create`; buffered multipart is config-tested but exploratory and not scored as qualified. |
| C8 `collector.commit.staging_overlap` | Not configured | Pending `pending-L1b` until its consumer and safety clearance are integrated. |

On that source the CLI also accepted the 8 MiB and 1 TiB cap boundaries and an omitted cap, and rejected 12 malformed or out-of-range guard/cap inputs. An omitted cap is config-valid but cannot establish guarded local admission.

The baseline mirrors the unscored diagnostic's 150-second measurement, 15-second maintenance interval and idle-backoff, hour granularity, 30-day purge retention, one table, and one writer. A 15-second cadence does not guarantee per-window observations. Hour granularity with closed-only compaction can accumulate protected current-partition backlog; zero eligible work alone does not establish bounded debt. C7 varies `max_consecutive_stale_plans` (1..1024, default 8) and `sealed_partitions_only`. When omitted, the OAT request is null: the stale-plan startup and effective values are 8; the sealed-only startup value is null and effective value defaults true unless a table-property override says otherwise. Explicit sealed-only false requires OPTIMIZE and cannot be combined with `sealed_final`. Record the requested startup value separately from the effective table-property value `arrowcol.optimize.sealedPartitionsOnly`; requested false does not imply effective false. Config-validation does not qualify effective settings or runtime evidence.

**Before any authorized run**, replace the shared template's Compose project/state paths and output destinations with isolated run-owned paths, set the correct local endpoint/bind settings, and revalidate the modified config; rendered files reuse template paths and must not be run concurrently as-is. Verify actual collector cgroup limit/readback and host fit for the requested 4 GiB envelope, and record requested and effective values separately. Config validation does not establish either check.

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
