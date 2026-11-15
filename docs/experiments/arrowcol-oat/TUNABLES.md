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
| `logging.router` | Strict enum `error`, `warn`, `info`, `debug`, `trace`, or `inherit`, component CLI `--log-level`, projected as `RUST_LOG`; default is `warn,tiberius=error` so Tiberius certificate-trust chatter is suppressed without hiding errors. |
| SQL evidence | `evidence_json.router_sql` records measurement-window deltas for physical connections opened, SQL operation counts by operation/outcome (cancellations count as failures), and operation-latency histogram buckets, counts and sums. Request p50/p99 are histogram upper bounds; connection and operation rates use the recorded start/end scrape-midpoint interval. A router restart/counter reset makes this evidence unavailable rather than negative. |

For the planned zero-interval comparison, keep Profile A unpooled (`sql_pool_max_size: 0`) and set `logging.router: inherit` so its YAML filter matches the main router; use a positive pool size for Profile C (`64` is the documented comparison value), whose default `warn,tiberius=error` filter differs from A. Attribute any A/C result to both pooling and this log-filter change. Pooling changes connection reuse only; it does not cache, weaken or skip authoritative pin reads.

## Overload retry and admission policy

| Setting or signal | Contract |
| --- | --- |
| `client.stream_retry_max_attempts` | Attempts per continuous stream, including the first; accepts `1..=16`, default `4`. Only the explicit pre-admission marker is retried. |
| `client.stream_retry_base_backoff_ms` / `client.stream_retry_max_backoff_ms` | Defaults `10` / `250` ms; validation requires `1 <= base <= max <= 60000`. Retries use bounded jittered exponential backoff. |
| `router.excluded_admission_gauges` | CLI `--excluded-admission-gauges`; env `ARROWCOL_ROUTER_EXCLUDED_ADMISSION_GAUGES` accepts CSV or a JSON array. Duplicates are rejected, and at least one admission gauge must remain enabled. Exclusion affects both hard-pressure eligibility and rendezvous weighting. |
| Pressure gauge | `arrowcol_router_collector_pressure_gauge_bps{pool,gauge}` reports each admission gauge separately, including the gauge driving the hard-pressure maximum. |
| Ambiguous outcomes | Continuous mode does not retry unmarked failures and keeps running. Process-cumulative and measurement-window `ambiguous_streams` / `ambiguous_rows` bound uncertain delivery; one-shot and round modes remain terminal. |
| Attempt counters | `error_rows` and `rejected_rows` count attempts, not unique rows. A retry of the same encoded batch is another attempt. |
| Counter readback | Rejected, retried, given-up and ambiguous stream/row counters are retained per worker for the measurement-start delta and, where available, the shared measurement window. Older clients default missing counters to zero. |

## AKS rollout readiness

| Setting | Contract |
| --- | --- |
| `aks_rollout_timeout_seconds` | Overall Deployment readiness deadline; defaults to 600 seconds and accepts `600..=3600`. Pods fail fast on crash-loop, image/config pull errors, or at least two restarts; failure details include previous logs and `kubectl describe` output. |
| `reconciliation_settle_timeout_seconds` | Post-client Delta commit drain deadline; defaults to 120 seconds and accepts `0..=1800`. Zero performs one immediate read. Settling is outside the measurement window. Commit evidence uses appended-log offsets plus ingress timestamps at or after the cell start. A log-prefix mismatch/rotation safely falls back to ingress-time-only scope and records that choice; prior-cell tail events are excluded, while commits crossing the cell boundary are reported unavailable. |

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
| Operation outcomes | `operation_outcomes` records process-lifetime operation/error counts and sanitized `error_kind_counts`; raw error strings are omitted and a fixed comment explains the evidence. Per-cell `maintenance_operation_outcomes` uses separate stdout/stderr offsets; when logs cannot be scoped, its counts are null with a sanitized comment. Truncated maintenance logs still fail. |
| Maintenance shutdown | `deadline_exceeded` remains a genuine operation error; `cancelled` is healthy only when the signal-driven cancel flag is set, so signal-cancelled work does not make graceful shutdown unhealthy. Local snapshot evidence records `delta-maintenance` exit 1 as `maintenance_operation_failure`; exit 2, 137 and OOM remain fatal. |
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
| Duration spelling | Known collector duration fields are parsed and emitted in canonical humantime form (for example `1000ms` becomes `1s`); arbitrary strings are not normalized. |
| Maintenance reads | `input_read_strategy` accepts `memory` (default), `spool`, or `range`. Range reads use a strong ETag with `If-Match` and validate ETag, range and length for every response. |
| Operation limits | `PURGE_MAX_FILES_PER_COMMIT=10000`, `PURGE_MAX_PLAN_BYTES=4194304`, `VACUUM_DELETE_CHUNK=256`, `VACUUM_MAX_PLAN_BYTES=8388608`, `REPLAY_MAX_LOG_FILES=100000`, `REPLAY_MAX_LOG_FILE_BYTES=67108864`, `REPLAY_MAX_CHECKPOINT_BYTES=268435456`, `OPERATION_MAX_ACTIVE_FILES=1000000`, `DISCOVERY_MAX_LOG_FILES=100000`, and `DISCOVERY_MAX_LOG_FILE_BYTES=67108864` are `ARROWCOL_MAINT_*` startup settings, read back under `startup.operation_limits`. Plan-byte limits must be at least 1024; all other limits must be positive. |
| WAL topology | Omitted `wal` keeps the non-Quiver profile; explicit `wal: {}` or typed overrides select `config.quiver.yaml` in both paths. Neither environment injection nor client upload acknowledgement proves WAL fsync. Every processor core must appear in consumer aggregate readback; actual class is rederived from those values. |
| WAL storage | Local state is a run-owned bind mount with separate hostname directories. AKS uses one writable `emptyDir` per pod. Filesystem fsync classes do not imply survival of pod/node loss or durable Delta delivery; storage scope is recorded separately. Existing durable WAL defaults remain unchanged. |
| Local worker count | Both profiles are rendered into run-owned configuration without resource-policy overrides that could shadow `--num-cores`; source profiles are not edited. |
| Client automatic values | Explicit zero workers/runtime threads require the consumer's matching available-parallelism observation. Zero batch/corpus rows and streams use verified legacy rows/concurrency; byte-target corpus sizing remains deferred until preparation. Arbitrary positive values are not accepted as automatic-resolution proof. |

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
