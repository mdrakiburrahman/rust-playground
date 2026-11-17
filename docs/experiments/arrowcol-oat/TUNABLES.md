# OAT tunables and measurement evidence

| Contract | Behavior |
| --- | --- |
| Input | Optional strict `tunables` object; unknown fields, invalid enums, levels, bounds and unsupported combinations fail validation. Omitted settings preserve component defaults. |
| Application | Typed role adapters inject only supported settings into the run-owned Compose overlay or AKS containers. There is no arbitrary env/CLI pass-through. |
| Scope | Startup settings apply to each newly isolated matrix cell. Rebuild-only capabilities are not runtime overrides. |
| Proof | Requested JSON is labelled requested. Effective settings come from consumer startup readback and must match every explicitly injected value on every process instance. |
| Provenance | Actual container image IDs and available source revision labels are recorded separately from the OAT server revision and capability catalog. Missing evidence is not inferred. |
| Durability | Requested WAL class is recorded separately from consumer evidence. Nondefault weaker durability must not be compared as equivalent to durable configurations. |
| Cleanup | Only run-owned resources/configuration are changed. Existing namespace, identity, pinning and cleanup guards remain required. Never delete a resource group to clean up an experiment. |

## AKS per-role resources

| Role | Default request | Default limit |
| --- | --- | --- |
| Collector | 2 CPU / 4Gi | 2 CPU / 6Gi |
| Client | 250m CPU / 256Mi | 2 CPU / 2Gi |
| Maintenance | 250m CPU / 512Mi | 1 CPU / 2Gi |
| Router | 500m CPU / 512Mi | 2 CPU / 1Gi |
| Catalog | 500m CPU / 1Gi | 2 CPU / 4Gi |

`platform.resources` may override any role with CPU/memory requests and limits. Only `scale_out_single_table` may omit or set `collector.limits.cpu: null` for an uncapped collector; a finite CPU limit is strictly parsed and preserved. Other resource fields remain required, and unknown fields, malformed quantities, and requests above finite limits fail before setup. This profile requires the exact hot table, memory-only buffering, matching 1..16 collector/router replicas, hostname anti-affinity, and enabled same-table maintenance with all five operations. AKS applies the settings to rendered workloads and client Jobs, then verifies live resource specs and node names. Per-cell evidence records requested/effective values, configured concurrency, CPU limits, and oversubscription ratios. OAT measures per-container CFS throttled-period deltas from kubelet cAdvisor; unavailable counters remain explicitly unavailable.

### D53 capacity reference

| Resource | Published target | Interpretation |
| --- | --- | --- |
| `Standard_D64ds_v6` / `Standard_E64ds_v6` | 30,000 Mbps aggregate network bandwidth per VM | Four nodes imply 120 Gbps of nominal VM bandwidth per pool; this is not a workload guarantee. Azure measures the VM allocation on outbound traffic. |
| Standard GPv2 storage in Canada Central | 40,000 requests/s, 60 Gbps ingress, 200 Gbps egress | The campaign account is StorageV2, `Standard_LRS`, with HNS enabled. These are account targets, not a guarantee; increase limits only if measured throttling or saturation identifies storage as the bottleneck. |

Sources: [Ddsv6 sizes](https://learn.microsoft.com/azure/virtual-machines/sizes/general-purpose/ddsv6-series), [Edsv6 sizes](https://learn.microsoft.com/azure/virtual-machines/sizes/memory-optimized/edsv6-series), [VM network bandwidth](https://learn.microsoft.com/azure/virtual-network/virtual-machine-network-throughput), and [standard storage scalability targets](https://learn.microsoft.com/azure/storage/common/scalability-targets-standard-account).

## Single-table preparation and commit overlap

| Setting | Contract |
| --- | --- |
| `collector.batching.pipeline_depth` | Integer `1..=64`, default `1`. Bounds in-flight preparation/publication groups per logical table through durable receipts. Values above `1` require `coalesce_publications: true`. Projected unchanged inside `ARROWCOL_COLLECTOR_BATCHING_CONFIG`; exporter startup readback reports the resolved value. |
| `collector.batching.max_concurrent_flushes` | Existing exporter-backend semaphore, shared across cores using the same root. Bounds active file groups including their commit wait; independent of the per-table pipeline depth. |
| `collector.commit.max_files_per_commit` | Existing maximum add actions per version (default `64`). A coalesced group remains indivisible; separate groups may share a version only when their identities do not overlap and their combined file count fits. |
| `collector.commit.batch_window` | Existing maximum collection interval (default `10s`), starting at the first ready group. Depth `1` preserves immediate forced group commits; pipelined groups use this interval or the file cap. Explicit flush, schema transitions and shutdown wait for all prior groups. |
| `collector.commit.max_pending_files` | Existing file admission bound (default `256`), held through terminal commit results. |
| `collector.runtime.cpu_workers` / `upload_limit` | Existing CPU pool and upload admission bounds. These do not imply that more file groups are active; pipeline depth, flush slots and Arrow queue capacities must also permit overlap. |

Coalesced preparation uses the same opened table as the single Delta commit worker,
without acquiring another writer. Arrow reservations remain charged until terminal
publication results. Kernel commit-time identity, schema, retention and ownership
checks remain authoritative; staging is not acknowledgment. Existing bounded append
conflict retries refresh and replan against maintenance commits and emit publication
attempt/conflict telemetry. A larger depth is not evidence of a throughput gain.

## Router timeout compatibility

| Setting | Contract |
| --- | --- |
| `router.upstream_idle_timeout_seconds` | Accepts `0..=86400`; zero disables idle upstream pooling and is the value emitted by the main OAT runtime. |
| Router downstream `server.h2.idle_timeout_seconds` | Deliberately constrained to `1..=86400`, whereas main accepted an unbounded value. Shipped router YAMLs use 30 or 60 seconds. |

## Router SQL pooling and log volume

| Setting | Contract |
| --- | --- |
| `router.sql_pool_max_size` | Env `ARROWCOL_ROUTER_SQL_POOL_MAX_SIZE`, CLI `--sql-pool-max-size`; default `0` (unpooled, matching main), valid `0..=1024`. A positive value enables the shared pool. |
| `router.sql_pool_acquire_timeout_ms` | Env `ARROWCOL_ROUTER_SQL_POOL_ACQUIRE_TIMEOUT_MS`, CLI `--sql-pool-acquire-timeout-ms`; defaults to `10000` in the router when pooling is enabled, valid `1..=600000`, and requires a positive pool size when explicitly set. |
| `router.max_future_skew_ms` | Default `2000`; increasing it permits more future-dated pressure observations, weakening the clock-skew guard. It does not repair stale observations or expired ownership. |
| `router.refresh_interval_ms` | Default `1000`; controls background refresh, not per-request OAT authority reads. Increasing it can reduce background SQL traffic but increase cache staleness and readiness risk. |
| `logging.router` | Strict enum `error`, `warn`, `info`, `debug`, `trace`, or `inherit`, component CLI `--log-level`, projected as `RUST_LOG`; default is `warn,tiberius=error` so Tiberius certificate-trust chatter is suppressed without hiding errors. |
| SQL evidence | `evidence_json.router_sql` records measurement-window deltas for physical connections opened, SQL operation counts by operation/outcome (cancellations count as failures), and operation-latency histogram buckets, counts and sums. Request p50/p99 are histogram upper bounds; connection and operation rates use the recorded start/end scrape-midpoint interval. A router restart/counter reset makes this evidence unavailable rather than negative. |

For the planned zero-interval comparison, keep Profile A unpooled (`sql_pool_max_size: 0`) and set `logging.router: inherit` so its YAML filter matches the main router; use a positive pool size for Profile C (`64` is the documented comparison value), whose default `warn,tiberius=error` filter differs from A. Set `collector.cpu_pressure: process` in AKS Profile C; Profile A keeps the default `off`. Attribute any A/C result to both pooling and the log-filter change, and note that excluding CPU only changes admission when CPU sampling is `process`. Pooling changes connection reuse only; it does not cache, weaken or skip authoritative pin reads.

## Flight gRPC compression direction

These controls are independent of Arrow IPC and Parquet compression. Each end defaults to `none` and accepts the strict enum `none | gzip | zstd`; a default of `none` does not enable compression.

| Setting | Direction and required peer match |
| --- | --- |
| `transport.grpc_send_compression` | Client request encoding; match with receiver `collector.limits.grpc_accept_compression`. |
| `transport.grpc_accept_compression` | Client response acceptance; match with receiver `collector.limits.grpc_send_compression`. |
| `collector.limits.grpc_accept_compression` | Receiver request acceptance; match with client `transport.grpc_send_compression`. |
| `collector.limits.grpc_send_compression` | Receiver response encoding; match with client `transport.grpc_accept_compression`. |

Receiver settings are projected through `ARROWCOL_COLLECTOR_LIMITS_CONFIG`. See the [Flight receiver configuration and negotiation notes](../../../crates/arrowcol-flight/README.md). These options configure codec negotiation only; they do not prove wire-byte savings, durable Delta writes, or performance qualification.

## Overload retry and admission policy

| Setting or signal | Contract |
| --- | --- |
| `client.stream_retry_max_attempts` | Attempts per continuous stream, including the first; accepts `1..=16`, default `4`. Only the explicit pre-admission marker is retried. |
| `client.stream_retry_base_backoff_ms` / `client.stream_retry_max_backoff_ms` | Defaults `10` / `250` ms; validation requires `1 <= base <= max <= 60000`. Retries use bounded jittered exponential backoff. |
| `router.excluded_admission_gauges` | CLI `--excluded-admission-gauges`; env `ARROWCOL_ROUTER_EXCLUDED_ADMISSION_GAUGES` accepts CSV or a JSON array. Duplicates are rejected, and at least one admission gauge must remain enabled. Exclusion affects both hard-pressure eligibility and rendezvous weighting. |
| Pressure gauge | `arrowcol_router_collector_pressure_gauge_bps{pool,gauge}` reports each admission gauge separately, including the gauge driving the hard-pressure maximum. With CPU sampling `off`, OAT evidence labels CPU `not_sampled` and stores null rather than interpreting the legacy zero as observed utilization. |
| Ambiguous outcomes | Continuous mode does not retry unmarked failures and keeps running. Process-cumulative and measurement-window `ambiguous_streams` / `ambiguous_rows` bound uncertain delivery; one-shot and round modes remain terminal. |
| Attempt counters | `error_rows` and `rejected_rows` count attempts, not unique rows. A retry of the same encoded batch is another attempt. |
| Counter readback | Rejected, retried, given-up and ambiguous stream/row counters are retained per worker for the measurement-start delta and, where available, the shared measurement window. Older clients default missing counters to zero. |

## In-flight overload guard

These are root OAT settings, not component `tunables`. Memory-only runs do not
need WAL or Quiver; disk protection is explicitly not applicable.

| Setting or evidence | Contract |
| --- | --- |
| `overload_guard.enabled` | Strict bool, default `true`. Disabled runs cannot qualify for the throughput campaign. |
| `overload_guard.cap_fraction` | Finite number in `(0, 0.90]`, default `0.90`; lower values stop earlier, never weaken the approved ceiling. |
| `overload_guard.zero_progress_windows` | Strict integer `1..=2`, default `2`; complete 30-second windows with outstanding publications, rows or logical bytes and no new ingestion commit. Idle windows do not trigger. |
| `platform.local.collector_memory_limit_bytes` | Optional strict integer bytes, `8388608..=1099511627776` (8 MiB–1 TiB); strings/units, fractions and negative values fail. Projects `mem_limit` only into the run-owned local collector. Omission does not change existing Compose limits: an unlimited collector remains unprotected and cannot admit a guarded cell. Example explicit request: `4294967296` (4 GiB). AKS rejects this local-only override and preserves its existing pod limit. Requested bytes and actual cgroup cap are recorded separately and must match when requested. |
| Hard-cap accounting | Exactly one run-owned collector: read its actual cgroup-v2 `memory.current`/finite `memory.max` in its private cgroup namespace, and its pressure endpoint's actual RSS and buffered bytes against startup-confirmed limits. Never compare aggregate pod/Docker working-set memory or publication logical bytes with a different cap. Unlimited/unsupported cgroups, mismatched identities/accounting, missing readback or a sampling gap over 5 seconds are unavailable, not healthy under-cap observations. |
| Coverage and progress | Pre-admission readback, then 1-second sampling through warm-up and measurement (3-second request deadlines). Startup capability is read once; live Docker/Kubernetes logs use an incremental timestamp cursor with a 5-second overlap, not full-log replay. Ingestion-only durable commit telemetry is deduplicated by table/version and assigned by commit finish time; rewrites and replayed retries cannot create progress. |
| Stop and custody | `evidence_json.overload_stop` carries `classification: above_ceiling`, reason, metric, value, cap/fraction (null for no-progress), zero-based window index and Unix-ms timestamp. Cancel active clients and retain the normal drain, maintenance and ownership-safe cleanup path; never destroy on failed stop. |
| Unknown protection | Admission fails closed before clients are created if required readback is unavailable. Mid-cell unavailability stops clients and records guard status `unavailable`, with null current caps; it is not mislabelled as an actual threshold trigger. |
| Local endpoint lifecycle | Rediscover published router, collector and downstream bindings after setup and each scale/recreate, before pins, maintenance and admission. Failed discovery clears the old URL: required router failure aborts setup/scaling; missing collector protection refuses guarded admission; optional downstream remains unavailable. Samplers use the refreshed collector binding and the guard independently discovers the current container identity. |
| Qualification | `overload_guard` retains settings, run/schedule identity, coverage, sampling gaps, peak cap fraction and complete progress windows. Missing/disabled/unavailable/incomplete protection and actual stops cannot qualify; a stop is rendered `above_ceiling` even for a failed official row. Existing debt, backlog, reconciliation and 30-second rate gates remain unchanged. |

The collector RSS limit comes from its registration configuration, **not** from
the cgroup cap. The guard checks both independently against actual readback.
Slow CPU/resource statistics run as a separate single-flight task; they do not
serialize cap/backlog/commit observations. Incomplete resource samples remain
unavailable rather than delaying the safety loop.

The ignored integration check below requires separate authorization. It creates
only test-owned no-load collectors with memory catalog/routing and local storage,
no clients, cloud calls or shared-stack changes. Run inside a Linux Docker
devcontainer whose ID is `OAT_GUARD_DOCKER_RUNNER`; it records the actual cgroup
namespace, capped/uncapped admission, pressure/startup agreement and bounded idle
sampling, then removes only its own containers. This is not AKS portability or
benchmark qualification proof.

```bash
cargo build --locked --package arrowcol-dataflow-rust
OAT_GUARD_COLLECTOR_BINARY="$PWD/target/debug/arrowcol-dataflow-rust" \
OAT_GUARD_DOCKER_RUNNER="$HOSTNAME" \
cargo test --locked --package arrowcol-oat-rust --lib \
  overload_actual_docker_admission_and_idle_sampling -- --ignored --nocapture
```

The separately authorized lifecycle check below uses existing cached component
images without rebuilding or relabelling them. Its local single-hot-table config
must request a 4 GiB collector cap, hour granularity and `sealed_partitions_only:
false`. It creates an isolated stack, scales/recreates the collector, initializes
pins and maintenance, then prepares guard/policy admission without creating
workload clients or arming a schedule. It retains an unscored receipt (old/new
container IDs and endpoints, HTTP status, cgroup and policy readbacks, cleanup)
under `out/arrowcol-oat/endpoint-lifecycle-<run-id>/`. This is not a load diagnostic.
The test uses the normal non-snapshot stop/destroy path because no committed rows
exist; it does not claim an empty successful snapshot. Any remaining test-owned
containers must be verified stopped before the harness removes them.

```bash
OAT_ENDPOINT_LIFECYCLE_CONFIG=/absolute/path/to/authorized-local-config.json \
cargo test --locked --package arrowcol-oat-rust --lib \
  local_endpoint_actual_docker_lifecycle_without_clients -- --ignored --nocapture
```

## AKS rollout readiness

| Setting | Contract |
| --- | --- |
| `aks_rollout_timeout_seconds` | Overall Deployment readiness deadline; defaults to 600 seconds and accepts `600..=3600`. Image-pull and container-config errors fail immediately; an unready container fails at four or more restarts. Failure details include previous logs and `kubectl describe` output. |
| `reconciliation_settle_timeout_seconds` | Post-client Delta commit drain deadline; defaults to 120 seconds and accepts `0..=1800`. Zero performs one immediate read. Settling is outside the measurement window. Commit evidence uses appended-log offsets plus ingress timestamps at or after the cell start. A log-prefix mismatch/rotation safely falls back to ingress-time-only scope and records that choice; prior-cell tail events are excluded, while commits crossing the cell boundary are reported unavailable. |
| `collector_stop_grace_seconds` | Collector shutdown grace, default 300 seconds, range `30..=3600`; projected to local Compose and AKS, verified against the effective runtime configuration, and included in the run-pin lifetime budget. |

## Quick-run measurement scopes

| Measurement | Interpretation |
| --- | --- |
| Supplied local/AKS quick windows | 10-second warm-up, 30-second measurement, unchanged load. Shipped OAT YAML uses `batching.max_age: 5s` and `commit.batch_window: 2s`; injected settings may change these timings. |
| Local quick observation budgets | 300-second post-client reconciliation and 60-second QAP timeout. |
| AKS quick observation budgets | 600-second post-client reconciliation and 180-second QAP timeout. The QAP timeout bounds preparation and post-measurement observation; required verdict gates are unchanged. |
| Admission rows/s | Client completions within the exact shared measurement window divided by its duration; not durable output. |
| Commit-window rows/s | Rows in new ingestion commits finished within that exact window; not a client cohort and not post-window settling. |
| Zero commit-window output | A short run may admit rows before commits finish. Known zero remains zero in the summary/table/result with an explanation; a dash means unavailable evidence. Size-triggered flushes and warm-up backlog can still produce commits in short windows. |
| Whole-cell reconciliation | Compare process-cumulative completed rows with committed Delta rows after settling, including warm-up. This is separate from either window rate. The quick profiles extend observation of continuing backlog commits, not the measurement window; the generic settle default remains 120 seconds. |

Short admission/smoke runs remain supported; duration below a flush timer alone
does not make a configuration invalid. Increase the measurement window when
evaluating sustained durable output, and report the changed duration explicitly.

## Collector runtime compatibility

| Setting | Contract |
| --- | --- |
| `collector.runtime.upload_limit` | Defaults to 32 on every host, matching main's fixed upload capacity; OAT still accepts `1..=256`. |
| `collector.runtime.read_ahead` | CPU-scaled default clamped to `8..=16`, preserving main's floor of 8. |
| `collector.runtime.batch_rows` | CPU-scaled default clamped to `4096..=32768`; this deliberately differs from main's fixed 256. Set 256 for main-equivalent runs. |
| `collector.runtime.cpu_workers` | Adds a dedicated CPU pool; main has no separate CPU-worker setting, so no value reproduces its runtime. |

Use the existing `tunables` sections (`client`, `transport`, `router`,
`collector`, `parquet_writer`, `wal`, `maintenance`) rather than
component environment names. The shared `arrowcol-tunables` crate is the
authoritative schema and capability catalog; the Parquet writer uses the actual
shared `ParquetWriterConfig` type.
The [tuned local example](oat-config.tuned.json) demonstrates typed overrides,
IPC/Parquet compression, shared runtime sizing and opt-in dry-run maintenance.

Validate without deployment:

```bash
cargo run --locked --package arrowcol-oat-rust -- --validate-config <config.json>
```

## Maintenance worker

| Selection | Behavior |
| --- | --- |
| Omitted or `maintenance.enabled: false` | No worker, maintenance image or maintenance environment is required. Existing configs remain valid. |
| `enabled: true` | Requires a nonempty explicit operations list; `apply` defaults false. The worker starts only after run table pin/catalog initialization. |
| Scope | Every table is a qualified benchmark allowlist entry in the isolated run catalog; discovery-all and arbitrary roots are not used. |
| Local image | Built from the selected source when absent, then resolved and launched by immutable Docker content ID. No `:dev` fallback is executed. |
| AKS image | `aks setup --maintenance` builds/publishes/verifies a sixth revision-labelled image and captures its registry digest as `maintenance_image`. An enabled run requires that pinned image. |
| Policy | Startup defaults and each table's resolved policy are recorded, including negative/no-op probes. Table properties override process defaults; those overrides remain visible. Missing per-table readback fails. |
| Writer policy | `parquet_writer_overrides` proves startup override consumption, not full per-schema physical properties. Schema compatibility is checked by the writer; inherited/null values are not invented defaults. |
| Operation outcomes | Process-lifetime and per-cell receipts retain operation/error counts and separate `retry_kind_counts` (`stale_plan` only). Per-operation cell counts use separate stdout/stderr offsets captured before clients start, retained through drain. Missing/partial/unscopable records yield null/UNKNOWN, never a clean zero; raw error strings are omitted. |
| Maintenance shutdown | Keep raw exit code separate from record-derived `maintenance_operation_failure`. Exit 1 never qualifies: retries alone cannot explain terminal health, including a stale-plan limit latched before later success. Unclassified exits remain UNKNOWN. Persistent stale-plan starvation is unhealthy, not a transient retry. `deadline_exceeded` remains a genuine operation error; the consumer's signal-cancel exception is unchanged. Exit 2, 137 and OOM remain fatal. |
| `maintenance.max_consecutive_stale_plans` | Strict integer 1–1024, consumer default 8. `ARROWCOL_MAINT_MAX_CONSECUTIVE_STALE_PLANS` / `--max-consecutive-stale-plans`. Per-(table,operation) scheduled counter resets only on error-free completion; reaching the limit latches unhealthy. Requested startup and effective operation settings are checked. |
| `maintenance.sealed_partitions_only` | Strict opt-in boolean, consumer default true. `ARROWCOL_MAINT_SEALED_PARTITIONS_ONLY` / `--sealed-partitions-only <true\|false>`. Requires OPTIMIZE; cannot be combined with `sealed_final`. Startup bool/null is the requested process override; effective operation/table policy is separate and table properties win. False permits newest/open partition optimization; it provides no file-age guarantee. |
| Protected partition debt | Preserve nullable `compaction_debt.protected_partition_backlog` (files, physical Parquet bytes, oldest UTC partition-start ms). A zero object is measured none; absent/null is UNKNOWN. Qualify its per-window trend separately from eligible/due debt, without adding the vectors twice. |
| Mutations | Only explicit `apply: true` adds `--apply`. Supported operations and their applicable knobs are validated; normal retention protections remain active. |

## Component adapter notes

| Settings | Adapter and constraints |
| --- | --- |
| Collector catalog | `catalog.maximum_cache_age` is emitted as `ARROWCOL_COLLECTOR_CATALOG_CONFIG` (default `10s`, remote catalog only). Readback identifies `catalog.type` and the resolved cache age. |
| Collector layout | `layout.enabled`, `sort_columns`, `bloom_columns`, and `layout.*` limits map to `ARROWCOL_COLLECTOR_LAYOUT_ENABLED`, `SORT_COLUMNS`, `BLOOM_COLUMNS`, and `LAYOUT_*`. Defaults are customer-id sort/Bloom, 262144 rows, 67108864 input bytes, 16384 rows per group, and 2048 rows per page. Selected DMV schemas are checked before deployment; `enabled: false` cannot be combined with layout overrides. Startup `layout.process_policy` is not table proof. Explicit overrides require separate canonical-table-matched `layout_table_policies` file-construction evidence across the collector fleet. |
| Parquet schema checks | Every selected DMV schema is checked against Parquet physical column paths and encodings before side effects. Sorting metadata remains rejected unless OAT can prove the exact file order; startup writer readback does not claim per-schema resolved properties. |
| SQL routing | `routing.connect_timeout` / `command_timeout` are emitted as `ARROWCOL_COLLECTOR_ROUTING_CONFIG` (defaults `15s` / `30s`) to receiver and exporter; they apply only to SQL Server routing. Readback identifies SQL Server, memory or standalone routing. |
| Exporter authority | `exporter.authority_timeout` is emitted as `ARROWCOL_COLLECTOR_EXPORTER_CONFIG` (default `10s`). |
| Pressure registration | `pressure.heartbeat_interval` / `registration_ttl` share `ARROWCOL_COLLECTOR_PRESSURE_CONFIG` (defaults `5s` / `20s`). TTL must exceed heartbeat, be at least `1ms`, and have a representable Unix-millisecond expiry. |
| `collector.cpu_pressure` | Strict `off` or `process`, default `off`; env `ARROWCOL_COLLECTOR_CPU_PRESSURE`. `process` samples process CPU-time delta per pressure-publication tick against wall time × cgroup v2 quota (when limited) or available parallelism, clamps to 10000 bps, and reports zero on the first tick. It is a startup setting. |
| Duration spelling | Known collector duration fields are parsed and emitted in canonical humantime form (for example `1000ms` becomes `1s`); arbitrary strings are not normalized. |
| Maintenance reads | `input_read_strategy` accepts `memory` (default), `spool`, or `range`. Range reads use a strong ETag with `If-Match` and validate ETag, range and length for every response. |
| Operation limits | `PURGE_MAX_FILES_PER_COMMIT=10000`, `PURGE_MAX_PLAN_BYTES=4194304`, `VACUUM_DELETE_CHUNK=256`, `VACUUM_MAX_PLAN_BYTES=8388608`, `REPLAY_MAX_LOG_FILES=100000`, `REPLAY_MAX_LOG_FILE_BYTES=67108864`, `REPLAY_MAX_CHECKPOINT_BYTES=268435456`, `OPERATION_MAX_ACTIVE_FILES=1000000`, `DISCOVERY_MAX_LOG_FILES=100000`, and `DISCOVERY_MAX_LOG_FILE_BYTES=67108864` are `ARROWCOL_MAINT_*` startup settings, read back under `startup.operation_limits`. Plan-byte limits must be at least 1024; all other limits must be positive. |
| WAL topology | Omitted `wal` keeps the non-Quiver profile; explicit `wal: {}` or typed overrides select `config.quiver.yaml` in both paths. Neither environment injection nor client upload acknowledgement proves WAL fsync. Every processor core must appear in consumer aggregate readback; actual class is rederived from those values. |
| WAL storage | Local state is a run-owned bind mount with separate hostname directories. AKS uses one writable `emptyDir` per pod. Filesystem fsync classes do not imply survival of pod/node loss or durable Delta delivery; storage scope is recorded separately. Existing durable WAL defaults remain unchanged. |
| Local worker count | Both profiles are rendered into run-owned configuration without resource-policy overrides that could shadow `--num-cores`; source profiles are not edited. |
| Client automatic values | Explicit zero workers/runtime threads require the consumer's matching available-parallelism observation. Zero batch/corpus rows and streams use verified legacy rows/concurrency; byte-target corpus sizing remains deferred until preparation. Arbitrary positive values are not accepted as automatic-resolution proof. |

## Campaign verdict policy

| Config field | Behavior |
| --- | --- |
| Omitted or `dataflow_verdict_policy: "legacy"` | Preserves the existing quick/legacy QAP verdict, including its 100% acceleration, zero-pending, and healthy-catalog gates. |
| `dataflow_verdict_policy: "single_hot_table_campaign"` | Keeps ingest validity separate from downstream state. Observed QAP lag is a `FINDING`, not an ingest failure; incomplete/timeout/missing QAP evidence is UNKNOWN and cannot PASS. Missing commit telemetry, zero committed output, and unbalanced matrices also cannot PASS. |

## Result evidence (schema v3)

| Evidence | Attribution and limitations |
| --- | --- |
| `rows`, `rows_per_second` | Client-observed admission completions within the half-open shared window. Neither upload acknowledgement nor WAL stream type proves Delta durability or exactly-once delivery. |
| Client phase counters | Completed minus measurement-start process snapshots. Attempts, errors, cancellations and transport bytes retain this wider snapshot scope; warm-up in-flight work can cross the boundary. |
| Client latency | Per-worker process-cumulative percentiles, explicitly including preflight/warm-up. Do not average percentiles or label them measurement-only. |
| Generator metrics | Prepared corpus sizes, memory reservation and preparation CPU/wall timing reported by the client. Prepared mode does not generate/encode in the hot path. |
| Startup readback | Concrete process settings remain in `effective_config`; `writer_policy_overrides` separately records configured Parquet policy, never schema-resolved or exercised file properties. |
| Layout application | Table-effective layout records are kept separate from startup policy, including catalog/Parquet overlay precedence. Disabled layout has null inapplicable limits. Records cover observed file construction, not a full-run policy history or proof those files committed. Missing table proof, ambiguous table hashes or truncated policy evidence fail explicitly. |
| Primary durable output throughput | Newly committed, synced ingestion AddFiles whose producer commit-finished timestamp lies in `[start,end)` and whose ingress belongs to the current cell, deduplicated by table hash and Delta version. Rows/files/bytes and rows per exact window second include warm-up backlog committed in-window. OPTIMIZE rewrites are excluded. Complete logs and observed ingestion-only telemetry/clock capability are required; known zero differs from missing telemetry. Cross-node clock skew is not measured. |
| Ingress-cohort commit evidence | Separately retains commits whose ingestion stamps lie wholly within the client window. Prior/warm-up commits are excluded here; mixed-window or uncertain reconciliation yields null plus a reason. This stricter attribution is not the primary output-throughput metric. |
| Cumulative reconciliation | Compares process-cumulative client completions with this cell's cumulative new-ingestion Delta rows, including warm-up: `completed_rows <= delta_rows <= completed_rows + ambiguous_rows`. Zero ambiguity requires equality. The bounded settle loop runs after clients finish and does not change window metrics; a lower-bound miss after timeout is `unsettled` with a gap, and a persistent miss after a settled drain or any upper-bound violation is `outside_bound`. Unknown Delta rows report `reconciliation: unavailable` with a reason. |
| Delivery completeness | Commit log readback is a bounded observation, not an end-to-end drain barrier. Missing late commits, client retries and replay do not establish unique delivered rows; that field remains null. |
| Compression | Collector-reported input bytes divided by committed Parquet file bytes (including metadata). This is not a wire ratio or a pure codec-only savings estimate. |
| Commit latency | Event elapsed time for commit/retry/sync, not encoding, admission or end-to-end visibility. Percentiles use actual observed commit samples only. |
| Resources | Independent one-second sampler; each call has a three-second deadline and cannot block client supervision. Only samples completed inside measurement are retained, up to the latest 4096; failures/truncation are explicit. |
| Backlog/stages | Unavailable metrics are null with their evidence scope, never guessed from a global counter or an end snapshot. |

Local Kustainer initialization/observation unavailability preserves viable
collector measurements with null QAP metrics and a fixed sanitized comment.
Required setup, collector, pinning, identity and cleanup failures still fail.
Initialization timeouts retain unresolved-outcome accounting. AKS does not
permit downstream degradation: its required QAP verdict gates remain strict.

Exact historical v2 schemas append into v3 by widening the existing nullable
measurement amendment and adding null `evidence_json`; historical version
numbers and zero values are preserved. Other schema changes are rejected.

The viewer summarizes commit-window output, Parquet size/compression and commit
percentiles separately from admission. The v3 verdict requires complete
collector image/log/capability evidence and consistent output timing/counters.
Known zero commits pass without a delivery claim; unavailable telemetry does
not. Legacy v2 admission results remain readable, with no durable backfill.
