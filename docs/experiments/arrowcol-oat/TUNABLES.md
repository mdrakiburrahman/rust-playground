# OAT tunables and measurement evidence

| Contract | Behavior |
| --- | --- |
| Input | Optional strict `tunables` object; unknown fields, invalid enums, levels, bounds and unsupported combinations fail validation. Malformed values report their field path. Omitted settings preserve component defaults. |
| Application | Typed role adapters inject only supported settings into the run-owned Compose overlay or AKS containers. There is no arbitrary env/CLI pass-through. |
| Scope | Startup settings apply to each newly isolated matrix cell. Rebuild-only capabilities are not runtime overrides. |
| Proof | Requested JSON is labelled requested. Effective settings come from consumer startup readback and must match every explicitly injected value on every process instance. |
| Provenance | Actual container image IDs and available source revision labels are recorded separately from the OAT server revision and capability catalog. Missing evidence is not inferred. |
| Durability | Requested WAL class is recorded separately from consumer evidence. Nondefault weaker durability must not be compared as equivalent to durable configurations. |
| Cleanup | Only run-owned resources/configuration are changed. Existing namespace, identity, pinning and cleanup guards remain required. Never delete a resource group to clean up an experiment. |

## Collector build profile and image arms

These controls are rebuild-only. The canonical profile key is `allocator=<value>;lto=<value>;codegen_units=<value>;opt_level=<value>;target_cpu=<value>`; its SHA-256 selects the profile-specific image tag and isolated BuildKit target cache. OAT selects the matching local image or digest-pinned AKS image; a missing or mismatched image fails before the arm starts. Omitted build fields preserve the Cargo release defaults and existing image path.

| Setting | Type and range | Default | Apply boundary | Influence and interactions |
| --- | --- | --- | --- | --- |
| `collector.allocator` | `system` or `mimalloc` | `system` | Collector image rebuild | Global allocator and allocation contention; compiled feature and read back at startup. Cross-validates with allocator environment controls. |
| `collector.lto` | `off`, `thin-local`, `thin`, or `fat` | `off` | Collector image rebuild | `off` disables LTO; `thin-local` selects Cargo’s local-only LTO mode; `thin` and `fat` use their corresponding LTO modes. These distinct modes affect compile time and may interact with codegen-unit count. |
| `collector.codegen_units` | Integer `1..=256` | `16` | Collector image rebuild | Parallel code generation and optimization scope; interacts with LTO. |
| `collector.opt_level` | Integer `2` or `3` | `3` | Collector image rebuild | LLVM optimization level; may trade build time and code size for runtime speed. |
| `collector.target_cpu` | `generic`, `x86-64-v2`, `x86-64-v3`, or `x86-64-v4`; `native` is forbidden | `generic` | Collector image rebuild and CPU preflight | Enables the selected x86-64 ISA baseline; `generic` leaves rustc’s default target CPU unchanged. Local OAT checks host CPU flags; AKS requires matching Node Feature Discovery CPU labels on every eligible collector node. Missing flags or labels fail closed before launch; no throughput result is accepted. |
| `collector.panic` | Unsupported | Rust release default (`unwind`) | Not configurable | `panic=abort` is not exposed because panic/unwind behavior participates in the collector startup and shutdown error contract. |
| PGO | Unsupported | N/A | Not configurable | Requires a representative training run and profile artifact lifecycle; no safe, deterministic profile collection/consumption pipeline exists in the image build. |
| BOLT | Unsupported | N/A | Not configurable | Requires post-link instrumentation, profile collection and binary rewrite tooling that are not present in the image pipeline. |

Every profile hash, requested field, image identity, and compiled startup value is recorded separately. A startup-profile mismatch is a failed arm, never a throughput result.

## OAT client release build

The local `arrowcol-oat-client` image uses an explicit release profile: `opt-level=3`, `lto=off` (no LTO), `codegen-units=16`, and `target_cpu=generic` (no `-C target-cpu` flag). `arrowcol-dmv` emits these compiled values in `OAT_CLIENT_BUILD_PROFILE`; the runner verifies the readback before recording client throughput.

## Client data profile and identity

Generator settings below apply at **process startup**.

| Setting | Type / allowed values | Diagnostic default | Realistic default |
| --- | --- | --- | --- |
| `client.string_length_distribution` | Enum: `fixed`, `uniform` | `fixed` | `uniform` |
| `client.value_variation`, `client.guid_uniqueness` | Enum: `cardinality`, `per_row` | `cardinality` | `per_row` |
| `client.string_width` | Integer: 0..65536 bytes | 0 (canonical fallback) | 0 (resolves to 96) |
| `client.cardinality` | Positive integer fitting `usize` | 1024 | 1024 |
| `client.null_fraction`, `client.skew` | Finite number: 0..1 | 0 | 0 |
| `client.entropy` | Finite number: 0..1 | 1 | 1 |
| `client.seed` | Unsigned 64-bit integer | 20260923 | 20260923 |
| `client.pod_ordinal` | Unsigned 32-bit integer | 0 | 0 |

| Setting | Contract |
| --- | --- |
| `client.data_profile` | Strict `diagnostic` (default) or `realistic`. Diagnostic preserves the older replayable values; `realistic` must be explicitly selected for D53 and uses documented field-specific Always On state categories, variable-width strings elsewhere, per-row numeric/timestamp values, GUID uniqueness, and a fresh row identity in `endpoint_url`. Diagnostic replay is not eligible for unique-delivery proof. |
| `client.send_iter` | Startup enum `clone_vec` (default) or `arc_cursor`: clone all FlightData containers before each request, or retain shared frames and clone each container as consumed. Payload bytes, retry identity and framing are unchanged; applies to both data profiles. |
| `client.identity_builder` | Startup enum `formatted_rows` (default) or `direct_utf8`, configurable in OAT only with `client.data_profile=realistic`. The latter builds identical identity strings using one reusable scratch buffer and a preallocated Arrow UTF-8 builder; it does not change delivery identity or retries. |
| `client.null_fraction` evidence | The current DMV schemas have no eligible nullable columns, so each actual prepared corpus marks this setting inert. Evidence counts nullable columns and rows, excluding `customer_id` and realistic delivery-identity `endpoint_url`. Missing evidence or future eligible columns stay projection-only; null counts are descriptive, not a throughput or distribution-quality claim. |
| `client.pod_ordinal` evidence | Counts actual non-null generated UTF-8 `_id` rows when GUID uniqueness resolves to `per_row`. Readback is verified only when every client reports matching eligible corpus rows; zero eligible rows are inert and missing or inconsistent evidence remains projection-only. |
| `client.guid_uniqueness` evidence | Counts actual non-null generated UTF-8 `_id` rows in either GUID mode. Zero eligible rows are inert; missing or inconsistent per-client corpus evidence remains projection-only. |
| `client.value_variation` evidence | Counts actual non-null generated numeric, boolean and timestamp rows. Zero eligible rows or skew 1 are inert; skew between 0 and 1 and missing evidence stay projection-only. |
| `client.string_width` evidence | Counts non-null generated strings excluding customer ID, realistic identity URL and categories, and per-row GUIDs. Zero eligible rows are inert; missing corpus evidence stays projection-only. A width of zero resolves to 96 in realistic mode and the diagnostic canonical fallback otherwise. |
| `client.string_length_distribution` evidence | Uses the same eligible string counts as `client.string_width`. A maximum width of one byte or zero eligible rows is inert; missing evidence remains projection-only. |
| `client.entropy` evidence | Counts generated non-null strings only when every possible prefix and suffix leaves at least one body byte. No guaranteed body or missing evidence stays projection-only; categories, GUIDs and delivery identities are excluded. |
| `client.cardinality` evidence | Numeric corpus readback is verified when non-null numeric rows exist, value variation is `cardinality`, and skew is zero. Skew one is inert; partial skew, other modes and missing evidence remain projection-only even if string generation could use cardinality. |
| `client.seed` evidence | Numeric corpus readback is verified when non-null numeric rows exist, skew is zero, and value variation is `per_row` or cardinality exceeds one. Other shapes remain projection-only even if GUID or string generation uses the seed. |
| `client.skew` evidence | Numeric corpus readback is verified when non-null numeric rows exist and value variation is `per_row` or cardinality exceeds one. Other shapes remain projection-only. |
| Generation telemetry | Both profiles prepare the corpus before load. Realistic phase counters must report positive corpus rows, encoded and retained bytes, and memory reservation, plus `preparation.steady_state_generation: true` because each logical send adds a fresh identity and re-encodes a prepared batch. Diagnostic replay reports `false`; older Diagnostic clients may omit the field or snapshots. Startup generation rates do not represent the Realistic send path. |
| `client.max_memory_bytes` | Strict positive byte cap. OAT config preflight checks explicit `corpus_bytes` against `ceil(4.0014 × corpus_bytes) + 1 GiB`, plus 260 MB per configured live slot for `realistic`; `independent` mode also multiplies corpus reservation by workers. Under-reservation fails before setup. Client startup then checks the exact schema-based encoded-corpus and live-batch reservation before registration or generation; realistic stream admission is semaphore-bounded to slots that fit, and each live slot remains reserved through retries until completion, error, or cancellation. Row-target corpora are checked against the exact reservation during client preparation. |
| Byte accounting | `completed_flight_data_bytes_uncompressed` and `completed_flight_data_bytes_configured_ipc_compression` count serialized FlightData protobuf lengths before and after the configured Arrow IPC compression. They exclude gRPC/HTTP/2 framing and are not logical Arrow buffer sizes. Legacy `completed_raw_arrow_bytes` / `completed_wire_bytes` fields remain aliases. |
| `arrow_ipc.alignment` | Arrow IPC buffer alignment in bytes: `8`, `16`, `32`, or `64`; defaults to `64`. Smaller alignment can reduce padding; measure decode and transfer costs separately. |
| `client.string_width` / `client.string_length_distribution` | Width defaults to 96 for generated realistic strings; distribution defaults to `uniform` for realistic and `fixed` for diagnostic. Categorical state labels use their natural lengths. The identity URL in realistic mode is additional required width and is included in the uncompressed FlightData byte total. |
| `client.value_variation` / `client.guid_uniqueness` | Each accepts `cardinality` or `per_row`; realistic defaults to `per_row`, diagnostic to `cardinality`. `client.cardinality` and `client.skew` also select category frequencies; `client.entropy` controls generated strings, not categorical state labels. Required fixture columns are never nulled. |
| Logical row identity | Realistic rows encode a client-generated `run_uuid`, client-incarnation UUID, worker, logical-batch sequence, and row ordinal in the existing `endpoint_url` value. The client-pod `client.pod_ordinal` (strict unsigned 32-bit; assign a distinct value to each client pod) salts generated GUIDs separately and is independent of routing. `run_uuid_source` and `incarnation_uuid_source` are `client_generated`; `oat_run_uuid` is null because OAT does not supply that UUID. Retries reuse the exact same values and encoded bytes; fresh sends and process restarts use disjoint identities. |
| `logical_batch_manifest_v1` | The terminal `OAT_CLIENT_METRICS` receipt includes every logical batch's attempted count, acknowledgement/ambiguity outcome, retry count, and cohort. Cohorts use the exact half-open measurement interval `[start_unix_ms, end_unix_ms)`. Contiguous equivalent batches may be range-coalesced without changing their identities. |
| Manifest limits | At most 1,000,000 logical batches and 1 MiB serialized terminal manifest. The complete line with the `OAT_CLIENT_METRICS` prefix followed by a space, including its newline, is capped at 1 MiB. The client remeasures after manifest truncation; if non-manifest fields still exceed the cap, it emits a minimal bounded failed receipt that preserves the first terminal failure reason. Oversized or incomplete evidence cannot qualify as a pass. |

## AKS per-role resources

| Role | Default request | Default limit |
| --- | --- | --- |
| Collector | 2 CPU / 4Gi | 2 CPU / 6Gi |
| Client | 250m CPU / 256Mi | 2 CPU / 2Gi |
| Maintenance | 250m CPU / 512Mi | 1 CPU / 2Gi |
| Router | 500m CPU / 512Mi | 2 CPU / 1Gi |
| Catalog | 500m CPU / 1Gi | 2 CPU / 4Gi |

`platform.resources` may override any role with CPU/memory requests and limits. Only `scale_out_single_table` may omit or set `collector.limits.cpu: null` for an uncapped collector; a finite CPU limit is strictly parsed and preserved. Other resource fields remain required, and unknown fields, malformed quantities, and requests above finite limits fail before setup. This profile requires the exact hot table, memory-only buffering, exactly one collector and one router replica, hostname anti-affinity, and enabled same-table maintenance with all five operations. Replica counts other than one are rejected, never clamped; only clients scale out (1..128 client pods per table). The D53 maximum-throughput campaign selects uncapped collector CPU on one high-bandwidth node. AKS applies the settings to rendered workloads and client Jobs, then verifies live resource specs and node names. Per-cell evidence records requested/effective values, configured concurrency, CPU limits, and oversubscription ratios. OAT measures per-container CFS throttled-period deltas from kubelet cAdvisor; unavailable counters remain explicitly unavailable.

### D53 capacity reference

| Resource | Published target | Interpretation |
| --- | --- | --- |
| `Standard_D64ds_v6` / `Standard_E64ds_v6` | 64 vCPUs; 30,000 Mbps per VM | Existing pool sizes; additional client nodes increase offered load, not one collector's bandwidth. |
| `Standard_D192ds_v6` | 192 vCPUs; 82,000 Mbps per VM | Scale-up fallback; 10.25 GB/s nominal outbound before protocol overhead. |
| `Standard_E128ns_v6` | 128 vCPUs; 200,000 Mbps per VM | Network-optimized alternative with no local temporary disk. |
| `Standard_E192ids_v6` | 192 vCPUs; 200,000 Mbps per VM | Preferred collector candidate among these verified sizes: tied highest bandwidth, most cores. Regional availability, AKS support, quota and allocatable resources are not yet verified. |
| Standard GPv2 storage in Canada Central | 40,000 requests/s, 60 Gbps ingress, 200 Gbps egress | The campaign account is StorageV2, `Standard_LRS`, with HNS enabled. These are account targets, not a guarantee; increase limits only if measured throttling or saturation identifies storage as the bottleneck. |

The target 17.4 GB/s is 139.2 Gbps of uncompressed Arrow payload. D53 requires lz4 or zstd wire IPC compression and measures actual compressed bytes and decode cost; Parquet compression is a separate ratio. Published network values aggregate all NICs and are not guarantees. Azure enforces the VM allocation on egress, not as a direct ingress cap; client/router egress and collector processing remain separate bottlenecks. A 200 Gbps reference does not prove a single pod can receive 139.2 Gbps. Keep one ingest committer plus maintenance; report the measured single-pod ceiling instead of adding collectors. Scale campaign bench pools to verified zero afterward, preserving data.

Sources: [Ddsv6 sizes](https://learn.microsoft.com/azure/virtual-machines/sizes/general-purpose/ddsv6-series), [Edsv6 sizes](https://learn.microsoft.com/azure/virtual-machines/sizes/memory-optimized/edsv6-series), [Ensv6 sizes](https://learn.microsoft.com/azure/virtual-machines/sizes/memory-optimized/ensv6-series), [VM network bandwidth](https://learn.microsoft.com/azure/virtual-network/virtual-machine-network-throughput), and [standard storage scalability targets](https://learn.microsoft.com/azure/storage/common/scalability-targets-standard-account).

## Single-table preparation and commit overlap

| Setting | Contract |
| --- | --- |
| `collector.batching.global_capacity.bytes` / `per_table_capacity.bytes` | Exact logical-byte pools may exceed 4 GiB, up to the smaller of Tokio's target-specific semaphore limit and 9,007,199,254,740,991 (the largest exact JSON integer). Each individual batch still cannot reserve more than `u32::MAX` bytes; reservations remain held through the durable receipt. |
| `collector.batching.pipeline_depth` | Integer `1..=64`, default `1`. Bounds in-flight preparation/publication groups per logical table through durable receipts. Values above `1` require `coalesce_publications: true`. Projected unchanged inside `ARROWCOL_COLLECTOR_BATCHING_CONFIG`; exporter startup readback reports the resolved value. |
| `collector.batching.max_concurrent_flushes` | Existing exporter-backend semaphore, shared across cores using the same root. Bounds active file groups including their commit wait; independent of the per-table pipeline depth. |
| `collector.commit.max_files_per_commit` | Existing maximum add actions per version (default `64`). A coalesced group remains indivisible; separate groups may share a version only when their identities do not overlap and their combined file count fits. |
| `collector.commit.batch_window` | Existing maximum collection interval (default `10s`), starting at the first ready group. Depth `1` preserves immediate forced group commits; pipelined groups use this interval or the file cap. Explicit flush, schema transitions and shutdown wait for all prior groups. |
| `collector.commit.max_pending_files` | Existing file admission bound (default `256`), held through terminal commit results. |
| `collector.runtime.timestamp_kernel` | `reference` (default) or `fused_checked`. The latter writes UTC microseconds and exact epoch nanoseconds in one checked pass per timestamp column, including nested fields. Negative timestamps round down; nulls remain null and unrepresentable nanoseconds fail the batch. |
| `collector.runtime.cpu_workers` / `upload_limit` | Existing CPU pool and upload admission bounds. These do not imply that more file groups are active; pipeline depth, flush slots and Arrow queue capacities must also permit overlap. |
| `collector.runtime.parquet_column_workers` | Integer `1..=16`, default `1` (the unchanged `AsyncArrowWriter` path). Above `1`, bounds concurrent leaf encoders per row group; one group is in flight, chunks append in schema order on the bounded blocking-I/O executor, and a file-backed sink avoids retaining a whole output file in memory. Encoded pages and dictionaries remain buffered for the open row group, plus at most the configured number of leaf tasks; this is not a hard RSS cap. Effective concurrency is also limited by `cpu_workers`. Sweep `1,2,4,8,16` with fixed row-group and compression settings; compare rows/s, CPU/row, size and latency. Content-defined chunking is not reachable through supported OAT JSON; API-supplied chunking is rejected because the public parallel API cannot preserve its encoder state. |
| `collector.commit.delta_stats_columns` | Optional list of up to 256 unique dotted primitive-leaf paths (ASCII letters, digits, underscores; each path at most 1024 bytes). Omission retains Delta's existing defaults. Nonempty lists set `delta.dataSkippingStatsColumns` on creation; `[]` sets `delta.dataSkippingNumIndexedCols=0`. Paths must resolve against the canonical table schema. The collector, OAT's fresh run-table initializer, and local partitioned/maintenance setup consume the policy. Existing tables must already have the matching committed properties; no implicit metadata mutation is performed. `arrowcol_delta_stats_config` proves table ID, committed version and metadata properties separately from writer startup settings. OAT promotes the leaf only with matching complete-log metadata evidence for every requested table. For pre-existing/catalog-created tables whose creation is outside these consumers, changing this policy is not delivered: mismatches fail before staging. |
| `collector.runtime.blocking_fs_reserve` | Integer `1..=255`, default `1`, strictly less than `blocking_workers`. Values above `31` require an explicit `blocking_workers`; otherwise the host default is at least 32. Reserves blocking threads for filesystem handoffs. |
| `collector.runtime.partition_grouping` | Startup enum: `scalar_map` (default) or `typed_runs`, via `ARROWCOL_COLLECTOR_PARTITION_GROUPING`. The latter compares contiguous values for one Utf8, Int32 or Int64 partition column, including nulls. Interleaved keys, more than 64 runs, and unsupported schemas use scalar grouping. Both modes materialize the same owned groups and retain file and sort budgets. Running collector readback: `runtime.partition_grouping`. |
| `collector.runtime.generated_partition_binding` | `revalidate_rows` (default) checks bound values row by row. `proven_constant` skips that check only for the sole ingestion partition generated locally for a coalesced batch. Caller-supplied and multi-column partitions take the normal binding path; mismatches still fail. This does not change `generated_partition_fast_path` (grouping). Collector startup reports the selected mode; `delta.partition.binding.batches` counts actual opt-in writer decisions (`path=proven_constant` or `fallback_revalidate_rows`), not commits. |
| `collector.runtime.cpu_queue_jobs` | Integer `1..=128`, default resolved `cpu_workers`. Bounds queued CPU jobs independently of worker count. |
| `collector.runtime.file_mapping_cache` | JSON boolean, default `false`. Reuse the bound logical-to-physical projection and evaluator for one Delta file only. Each batch still undergoes normalization or canonical validation, partition validation, alignment, evaluation, and statistics accounting. |
| `collector.runtime.event_interval` | Integer `1..=65535`; unset uses Tokio default 61. Startup only. Controls Delta I/O scheduler event polling frequency. |
| `collector.runtime.global_queue_interval` | Integer `1..=65535`; unset uses Tokio's adaptive default. Startup only. Controls Delta I/O global queue polling frequency; interacts with local queue and scheduler load. |
| `collector.runtime.thread_stack_size` | Integer bytes `65536..=67108864`; unset uses platform default. Startup only. Configures Delta I/O worker and blocking thread stack size; the OS may round the allocation. Readback is the builder value, not an OS stack measurement. |
| `collector.runtime.thread_keep_alive_ms` | Integer milliseconds `1..=86400000`; unset uses Tokio default 10000. Startup only. Controls idle Delta I/O blocking worker lifetime and thread churn. |
| `collector.runtime.cpu_thread_stack_size` | Integer bytes `65536..=67108864`; unset uses platform default. Startup only. Configures dedicated Delta CPU worker stacks; the OS may round the allocation. Readback is the builder value, not an OS stack measurement, and consumer evidence requires the lazy CPU pool on every collector. |
| `collector.socket.recv_buffer_bytes` / `send_buffer_bytes` | Integer bytes `1..=1073741823`; unset preserves the OS listener default. Linux startup only. Sets `SO_RCVBUF` / `SO_SNDBUF` on each Flight listener after bind/listen; accepted sockets inherit them. Linux reports doubled values for bookkeeping; startup fails if the kernel-reported listener size is not exactly twice the request, rejecting both upward and downward clamping. Readback records the kernel-reported listener bytes. The listener already accepts handshakes during the brief interval before configuration, so these settings are not pre-bind controls. |
| `collector.socket.tcp_nodelay` | Boolean; unset leaves the OS default unchanged. Startup only. Sets `TCP_NODELAY` on every accepted Flight connection, changing server-side Nagle buffering. Consumer evidence requires an accepted socket on every collector. |
| `collector.thp_disabled` | Boolean; unset preserves the inherited Linux process flag. Startup only. `true` disables transparent huge pages for the process; `false` clears that flag but does not override system THP policy. Readback is the kernel flag, not proof of actual huge-page use. Non-Linux collectors reject the setting. |
| `dfe_worker_concurrency` | OAT axis of distinct integers `1..=256`, projected as `--num-cores` at collector startup. The receiver reports `dfe_worker_concurrency` from its DFE pipeline context; higher values increase per-core pipeline parallelism and resource use. Fleet `sizing.collector.dfe_workers` sets this axis. |
| `logging.maintenance` | Inert when maintenance is disabled because no maintenance process is running; listed in the cell's `tunables_json.inert_fields` and `evidence_json.inert_fields`, and Darwin receipt `inert_fields`. A null receipt field means evidence was unavailable, not that no fields were inert. |
| `maintenance.*` | Except for the `enabled` runner control, fields are inert when maintenance is disabled. Values remain range- and cross-field-validated; enabling the role also requires the corresponding operations. |
| `parquet_writer` dictionary page limits | A column limit is inert when its column disables dictionary encoding, including when it inherits a disabled default. A default limit is inert when dictionaries are disabled by default and no column explicitly enables them. Inert factors are not ranked or attributed effects. |

Coverage evidence distinguishes runner controls, inert fields, and projected fields
whose consumer startup verification is unproven. Missing coverage remains unknown;
projection-only cells are null until applicability is proven and are excluded from
selection and effect claims.

| Unsupported process control | Reason |
| --- | --- |
| Delta I/O `disable_lifo_slot` | The pinned Tokio API requires the `tokio_unstable` build configuration; it is not an OAT runtime field. |
| DFE scheduler builder options | The DFE engine constructs its current-thread Tokio runtime internally without a collector-side builder hook. Delta I/O settings do not change it. |
| Other receiver TCP options and pre-bind socket settings | The DFE listener is bound and listening before the collector receives it; the current listener API offers no pre-bind socket-option hook. The documented listener buffers and accepted-socket `TCP_NODELAY` are applied separately. |
| Allocator environment tuning | `MALLOC_*` and `MIMALLOC_*` are not typed OAT fields here. The effective allocator build readback does not establish that an environment option was consumed. Even with mimalloc for the Rust heap, C libraries can still use glibc allocation. |
| Per-allocation `madvise` | No collector allocation-specific control/readback is available. The process THP-disable flag does not establish actual huge-page use. |

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
| `overload_guard.memory_signal` | Strict enum: `process_rss` (default) or `cgroup_memory`. The latter requires explicit positive `tunables.collector.pressure.rss_limit_bytes` and `buffered_bytes_limit`, and matching startup and live pressure limits. It compares collector cgroup memory, not process RSS, with the same RSS numeric threshold; cgroup memory can include cache. The separate container cap and live pressure checks remain mandatory. Missing, zero or late measurements fail closed. Paired arms must use the same signal and thresholds. |
| `platform.local.collector_memory_limit_bytes` | Optional strict integer bytes, `8388608..=1099511627776` (8 MiB–1 TiB); strings/units, fractions and negative values fail. Projects `mem_limit` only into the run-owned local collector. Omission does not change existing Compose limits: an unlimited collector remains unprotected and cannot admit a guarded cell. Example explicit request: `4294967296` (4 GiB). AKS rejects this local-only override and preserves its existing pod limit. Requested bytes and actual cgroup cap are recorded separately and must match when requested. |
| Hard-cap accounting | Exactly one run-owned collector: read its cgroup memory and finite cap, and its pressure endpoint's RSS and buffered bytes against startup-confirmed limits. The selected memory signal determines which measurement is compared with the RSS threshold; container memory is also checked against its separate cap. Never compare aggregate pod/Docker working-set memory or publication logical bytes with a different cap. Unlimited/unsupported cgroups, mismatched identities/accounting, missing readback or a sampling gap over 5 seconds are unavailable, not healthy under-cap observations. |
| Coverage and progress | Pre-admission readback, then 1-second sampling through warm-up and measurement (3-second request deadlines). Startup capability is read once; live Docker/Kubernetes logs use an incremental timestamp cursor with a 5-second overlap, not full-log replay. Ingestion-only durable commit telemetry is deduplicated by table/version and assigned by commit finish time; rewrites and replayed retries cannot create progress. |
| Stop and custody | `evidence_json.overload_stop` carries `classification: above_ceiling`, reason, metric, value, cap/fraction (null for no-progress), zero-based window index and Unix-ms timestamp. Cancel active clients and retain the normal drain, maintenance and ownership-safe cleanup path; never destroy on failed stop. |
| Unknown protection | Admission fails closed before clients are created if required readback is unavailable. Mid-cell unavailability stops clients and records guard status `unavailable`, with null current caps; it is not mislabelled as an actual threshold trigger. |
| Local endpoint lifecycle | Rediscover published router, collector and downstream bindings after setup and each scale/recreate, before pins, maintenance and admission. Failed discovery clears the old URL: required router failure aborts setup/scaling; missing collector protection refuses guarded admission; optional downstream remains unavailable. Samplers use the refreshed collector binding and the guard independently discovers the current container identity. |
| Qualification | `overload_guard` retains settings, run/schedule identity, coverage, sampling gaps, peak cap fraction and complete progress windows. Missing/disabled/unavailable/incomplete protection and actual stops cannot qualify; a stop is rendered `above_ceiling` even for a failed official row. Existing debt, backlog, reconciliation and 30-second rate gates remain unchanged. |

The RSS threshold comes from collector registration, **not** from the cgroup
cap. The selected signal and the separate container cap are checked independently.
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
| `collector_stop_grace_seconds` | Outer stop grace for local Compose and AKS (default 300 seconds, `30..=3600`). Must also cover twice the DFE drain budget plus 60 seconds for terminal writes. Verified on the runtime and included in the run-pin lifetime budget. |
| `tunables.collector.shutdown_drain_timeout_seconds` | DFE SIGTERM drain budget per phase. Whole JSON seconds (`1..=1500`, default `60`); applies at process startup. OAT verifies the DFE's startup readback, but classifies it inert for measurement heatmaps because collector shutdown follows result capture. |

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
| Collector channels | `channel_capacity.control_node`, `control_pipeline`, `control_completion`, and `pdata` set DFE control and pdata channel buffering at startup. Each is an integer from `1` through `65536`; shipped defaults are `64`, `64`, `512`, and `32`. DFE preallocates channel buffers; larger settings and more pipelines or cores increase memory use. OAT checks resolved immutable single-pipeline startup policy for every collector; a missing or mismatched readback fails. |
| Flight receiver channels | `limits.batch_channel_capacity` and `limits.put_result_channel_capacity` set per-stream decoder/admission and Flight response buffering. Integers must be between `1` and `usize::MAX >> 3`; shipped OAT defaults are `4` and `16`. Startup readback must match on every collector. |
| Collector upload | `collector.upload.mode` defaults to strict `put_create`; `buffered_multipart` is opt-in. With multipart, `buffer_capacity_bytes` defaults to 8 MiB (1..=64 MiB) and `request_concurrency` to 2 (1..=16). `collector.runtime.upload_limit` defaults to 32 (1..=256). Startup validation bounds aggregate upload working memory to 1 GiB. Sweep mode first, then buffer size and request concurrency while accounting for `upload_limit`; compare CPU, committed Parquet bytes, latency, and upload errors. `file:` tables bypass this path, so use a remote object store to measure upload effects. |
| Collector layout | `layout.enabled`, `sort_columns`, `bloom_columns`, and `layout.*` limits map to `ARROWCOL_COLLECTOR_LAYOUT_ENABLED`, `SORT_COLUMNS`, `BLOOM_COLUMNS`, and `LAYOUT_*`. Defaults are customer-id sort/Bloom, 262144 rows, 67108864 input bytes, 16384 rows per group, and 2048 rows per page. Selected DMV schemas are checked before deployment; `enabled: false` cannot be combined with layout overrides. Startup `layout.process_policy` is not table proof. Explicit overrides require separate canonical-table-matched `layout_table_policies` file-construction evidence across the collector fleet. |
| Sorted-buffer detachment | `collector.layout.detach_ratio`: integer 1–8, default 2; set at collector startup and applied per layout-enabled file. Detach a slice from its parent when charged Arrow bytes exceed this multiple of logical bytes; lower ratios copy more often, higher ratios may retain larger parents. File byte/row budgets remain hard bounds. OAT records the process setting and the active file policy separately. |
| Collector sort execution | `collector.layout.sort_execution` (`ARROWCOL_COLLECTOR_LAYOUT_SORT_EXECUTION`) defaults to `concatenate`: the existing whole-file sort and encoding path. `stream_unordered` writes batches in arrival order and requires enabled layout with empty `sort_columns`. `verify_order` writes without reordering and requires enabled layout with at least one sort key; any inversion within a batch or across consecutive nonempty batches fails the file. Equal keys are allowed. Both modes enforce the existing file budgets. The collector checks the resolved table layout, including overrides. OAT compares startup policy and table-effective file-construction evidence. Maintenance is unchanged. |
| Sort options | `collector.layout.sort_options` and `maintenance.layout_sort_options` accept ordered `{ "column": "customer_id", "direction": "descending", "nulls": "first" }` entries; direction is `ascending` or `descending`, nulls is `first` or `last`. Each requires its explicit, identically ordered sort-column list; disabled layout rejects options. Omission retains ascending/nulls-last. When collector options are set and maintenance automatic layout is enabled, maintenance options must be explicit; deliberately different options are allowed. `arrowcol.layout.sortOptions` uses comma-separated `column:direction:nulls` entries with table-over-process precedence; incompatible resolved lists fail. Startup and maintenance effective readback are checked; collector table-effective coverage remains projection-only. |
| Parquet schema checks | Every selected DMV schema is checked against Parquet physical column paths and encodings before side effects. Sorting metadata remains rejected unless OAT can prove the exact file order; startup writer readback does not claim per-schema resolved properties. |
| SQL routing | `routing.connect_timeout` / `command_timeout` are emitted as `ARROWCOL_COLLECTOR_ROUTING_CONFIG` (defaults `15s` / `30s`) to receiver and exporter; they apply only to SQL Server routing. Readback identifies SQL Server, memory or standalone routing. |
| Exporter authority | `exporter.authority_timeout` bounds synchronous authority checks; it is a duration of at least `1ms`, default `10s`. OAT projects it at startup and checks matching readback on every collector. |
| Pressure registration | `pressure.heartbeat_interval` / `registration_ttl` share `ARROWCOL_COLLECTOR_PRESSURE_CONFIG` (shipped OAT YAML defaults `5s` / `30s`; standalone component TTL `20s`). TTL must exceed heartbeat, be at least `1ms`, and have a representable Unix-millisecond expiry. OAT YAML defaults for RSS / buffered bytes are `4294967296` / `2147483648`; adaptive minimum input is `1048576` bytes. Partial overrides are validated against these values. |
| `collector.cpu_pressure` | Strict `off` or `process`, default `off`; env `ARROWCOL_COLLECTOR_CPU_PRESSURE`. `process` samples process CPU-time delta per pressure-publication tick against wall time × cgroup v2 quota (when limited) or available parallelism, clamps to 10000 bps, and reports zero on the first tick. It is a startup setting. |
| Duration spelling | Known collector duration fields are parsed and emitted in canonical humantime form (for example `1000ms` becomes `1s`); arbitrary strings are not normalized. |
| `maintenance.optimize_upload_concurrency` | Integer 1–16, default 1; startup `ARROWCOL_MAINT_OPTIMIZE_UPLOAD_CONCURRENCY` / `--optimize-upload-concurrency`. Only `buffered_multipart` OPTIMIZE cloud uploads consume it; local files and `put_create` do not. Explicit values require buffered multipart and `(concurrency + 1) × buffer_bytes × bin_concurrency × max_concurrent_tables ≤ 512 MiB`; this bounds configured part buffers, not process RSS. Startup and effective mode-dependent readback are recorded. |
| `maintenance.bin_concurrency` | Integer 1–8, default 1; startup `ARROWCOL_MAINT_BIN_CONCURRENCY` / `--bin-concurrency`. Runs at most this many same-table OPTIMIZE bin rewrites concurrently; results are processed in plan order and published together in one validated transaction. Values above 1 require `max_concurrent_decoded_bytes` to cover `max_decoded_bytes_per_bin × bin_concurrency × max_concurrent_tables`; that bound is not an RSS cap. Multipart buffer limits also multiply by bin concurrency. Startup and per-operation effective readback are recorded. |
| `maintenance.input_prefetch_files` | Integer 1–8, default 1; startup `ARROWCOL_MAINT_INPUT_PREFETCH_FILES` / `--input-prefetch-files`. Values above 1 overlap whole-file memory reads while decoding, in original input order; spool and range reject them. `small_file_bytes × input_prefetch_files × bin_concurrency × max_concurrent_tables` must not exceed 512 MiB, a configured encoded-input bound rather than RSS. Startup and per-operation effective readback are recorded. |
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
| Coverage | Explicit collector executor, batching capacity/flush/coalescing, commit and pressure settings and excluded router gauges are `consumer_verified` only when every expected role instance has a distinct identity and matching complete startup readback for the projected values. Per table, client workers, runtime, corpus, batch, streams, connections, pacing, data mode/profile, payload identity, memory budget, IPC alignment and applicable stream retry settings, plus applicable transport codecs, message/frame/header/window/keepalive settings, timeouts, retries, TCP_NODELAY, request limiting and endpoint concurrency/buffering, require matching readbacks from all expected distinct clients. Stream retry backoffs are inert when only one attempt is allowed. Upload settings have process effective-value readback, but local `file:` tables bypass upload; remote object-store throughput effects require an applicable sweep. Other projected leaves remain `projection_only` unless independently proven; this is startup configuration evidence, not proof of durable ingestion or a throughput effect. |
| Table writer limits | Explicit Parquet row-group and page-row limits become `consumer_verified` only when every target table has matching enabled-layout readback at file construction and all observed table policies agree. Missing or disabled-layout evidence remains `projection_only`; the readback is not a full history or a decoded-file proof. |
| WAL and receiver limits | Explicit WAL `max_in_flight` requires matching nonempty per-core WAL aggregate readback from every collector. Receiver metadata/schema/message limits and gRPC codecs require matching collector fleet startup readback. This is configuration evidence, not proof of actual traffic, queue saturation or throughput. |
| Table layout | Explicit collector layout settings require matching table-effective file-construction readback for every target table; settings other than `enabled` also require enabled layout. Missing or overridden values remain `projection_only`. |
| Adaptive sizing | Explicit adaptive-file settings require matching estimator-installation readbacks for every target table, in addition to complete collector startup readback. Missing or overridden values remain `projection_only`; readback is not a full history or proof of a throughput effect. |
| Table codecs | Explicit default and column codecs and levels require matching schema-resolved physical column properties at file construction for every target table. A default is verified only for columns without codec overrides and only if at least one such column exists; level proof also requires the expected codec. Missing, ambiguous, malformed or oversized column readback remains `projection_only`. The readback is not a full file history or proof of a performance effect. |
| Table writer properties | Writer version, maximum row-group bytes, write batch size, offset-index enablement and index/statistics truncation limits require matching file-construction properties for every target table. Truncation proof also requires applicable page or chunk statistics. Missing, malformed or contradictory properties remain `projection_only`. |
| Column writer properties | Encoding, dictionary and data-page limits, statistics and Bloom settings use bounded physical-column readback across all target tables. Default settings require an applicable non-overridden column; dictionary limits require an enabled dictionary, page-header statistics require page statistics, Bloom sizing requires an enabled filter, and the V2 compression threshold requires writer V2 and compression. Readback proves configured writer properties, not that every fallback or threshold was exercised. |
| One-shot row limit | `client.max_rows` limits one-shot sends and is inert for OAT's duration-based runs. Client preparation still rejects a nonzero value smaller than one corpus batch. |
| Connection retry backoff | `transport.retry_backoff_ms` is inert when `transport.connect_retries` is zero or omitted (the client default). |
| HTTP/2 applicability | Initial stream and connection windows are overridden by adaptive window mode. Keepalive timeout and idle behavior are inert when the keepalive interval is zero. |
| TCP applicability | TCP keepalive retransmission interval and retry count are inert when `tcp_keepalive_ms` is zero. |
| Client request limiting | `transport.rate_limit_period_ms` is `1..=86400000`, default `1000`, and inert when `rate_limit_rps` is zero. Despite its legacy name, `rate_limit_rps` is the maximum requests per configured period, per connection. |
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
