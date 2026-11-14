# Collector performance controls

| Interface | Effect | Timing |
| --- | --- | --- |
| `ARROWCOL_PARQUET_CONFIG` | Strict `arrow_deltalake::writer::ParquetWriterConfig` JSON: global/per-physical-column codecs, levels, encoding, dictionary, statistics/page indexes, offset index, Bloom filters, pages, row groups, sorting metadata | Startup |
| `ARROWCOL_COLLECTOR_BATCHING_CONFIG` | Deep JSON override of exporter `batching`: byte/row/batch capacities and flush limits, age, flush concurrency, adaptive sizing, `coalesce_publications` | Startup |
| `ARROWCOL_COLLECTOR_COMMIT_CONFIG` | Deep JSON override of exporter `commit`: `batch_window`, `max_files_per_commit`, `max_pending_files` | Startup |
| `ARROWCOL_COLLECTOR_LIMITS_CONFIG` | Deep JSON override of receiver `limits`: Flight sizes, channels and deadlines | Startup |
| `ARROWCOL_COLLECTOR_PRESSURE_CONFIG` | Stream/RSS/buffered limits, pressure/heartbeat intervals and registration TTL; no identities/endpoints | Startup |
| `ARROWCOL_COLLECTOR_CATALOG_CONFIG` | Remote catalog `maximum_cache_age`; rejects memory catalog | Startup |
| `ARROWCOL_COLLECTOR_ROUTING_CONFIG` | SQL routing `connect_timeout` and `command_timeout` for receiver/exporter; rejects other backends | Startup |
| `ARROWCOL_COLLECTOR_EXPORTER_CONFIG` | Exporter `authority_timeout` | Startup |
| `ARROWCOL_COLLECTOR_RUNTIME_WORKERS`, `BLOCKING_WORKERS`, `UPLOAD_LIMIT`, `TASK_LIMIT`, `READ_AHEAD`, `BATCH_ROWS`, `CPU_WORKERS` | Independently bounded Delta async, blocking, upload, Kernel task, scan and dedicated CPU resources; each suffix uses the full `ARROWCOL_COLLECTOR_` prefix | Startup |
| Cargo feature `allocator-mimalloc` | Opt-in mimalloc allocator for the collector binary; system allocator remains default | Rebuild |
| `RUSTFLAGS="-C target-cpu=native"` | Host-specific compiler ISA optimization; not a Cargo feature or per-cell runtime setting | Rebuild |

JSON collector overrides preserve unspecified pipeline YAML values. Unknown
keys, malformed numbers, zero capacities, invalid timeouts and inconsistent
budgets fail validation. Durations use humantime strings; sizes use bytes.
Runtime scalar inputs are unsigned decimal integers, never silently clamped.
Effective startup policy is printed as `arrowcol_effective_config <JSON>`.
Pressure overrides also accept `heartbeat_interval` and `registration_ttl`.
Resolved TTL must exceed heartbeat and fit the millisecond lease clock; all
control-plane durations must be positive and clock-representable. These
overrides cannot change identities, endpoints, authentication or fencing.
Readback includes safe catalog/routing backend names and resolved timeouts,
never backend addresses or credentials.

Example (workload-dependent, not a universal performance recommendation):

```bash
ARROWCOL_PARQUET_CONFIG='{"defaults":{"compression":"zstd","compression_level":1},"max_row_group_rows":65536}' \
ARROWCOL_COLLECTOR_BATCHING_CONFIG='{"max_age":"2s","max_concurrent_flushes":4}' \
cargo run --locked --package arrowcol-dataflow-rust -- --config projects/arrowcol-dataflow-rust/config.quiver.yaml
```

Parquet compression accepts uncompressed, snappy, gzip (0..9), brotli (0..11),
zstd (1..22), lz4_raw and legacy lz4. Levels are forbidden on codecs without a
level. `defaults` and `columns` accept the same `ColumnOptions`; column selectors
must uniquely resolve to an actual physical Parquet leaf, after normalization
and partition projection. Dictionary encoding is controlled separately from
fallback encoding. FPP must be finite and strictly between zero and one.
Sorting metadata must match a verified physical sort; it cannot assert ordering
for an unsorted file. Explicit `sorting_columns: []` clears inherited metadata.
Row-group byte and page limits are estimates, not process-memory ceilings.
Disabling offset indexes requires `statistics: "none"` or `"chunk"` on every
physical column; page statistics force offset indexes in Parquet 58.4, so
conflicting requests fail instead of silently ignoring the setting.

Cross-publication coalescing is disabled by default. When enabled, replay IDs
and all output files form one indivisible Delta commit unit. Already committed
IDs are filtered before encoding; a mixed-ID race rejects and rebuilds on retry.
Over-cap multi-publication groups fall back to intact publication units.
A single indivisible publication that cannot fit fails with an actionable
exporter configuration error before queueing; the node stops without sending
ACK/NACK, so native Quiver leaves its durable bundle unresolved for recovery
after the operator increases the commit limit and restarts. It does not enter
automatic NACK retry or permanently reject the bundle, and never records IDs
for partial output. Partition lookup is keyed and staging checks file caps early, before
encoding high-cardinality output. Schema, partition, retention, fencing,
no-overwrite and durability boundaries remain.
Increasing a flush target alone does not coalesce publications.

### Layout readback

Startup `arrowcol_effective_config <JSON>` includes a root `layout` object with
`process_policy: {enabled, sort_columns, bloom_columns, max_rows, max_input_bytes,
row_group_rows, page_rows}` and `table_properties_override: true`.
It validates the existing legacy/collector aliases,
numeric bounds and column lists before DFE initialization. `enabled: null`
means schema-dependent auto enablement, not disabled or applied. Column existence
is validated later against the table's physical schema.

Publication writers emit a separate change-only event at file preparation:

```json
{
  "component": "layout",
  "scope": "table_effective",
  "table_id": "<same SHA256 identity as arrowcol_data_commit>",
  "table_name": "catalog.schema.table",
  "applied": true,
  "inactive_controls": [],
  "precedence": ["table_properties", "process_policy", "registry_defaults"],
  "encoding_precedence": ["parquet_config", "resolved_layout", "parquet_defaults"],
  "table_property_overrides": ["arrowcol.layout.maxRows"],
  "config": {
    "enabled": true,
    "sort_columns": ["customer_id"],
    "bloom_columns": ["customer_id"],
    "max_rows": 262144,
    "max_input_bytes": 67108864,
    "row_group_rows": 16384,
    "page_rows": 2048
  }
}
```

The startup `layout.process_policy` object has the same typed config fields;
it has no table identity or application claim. All numeric controls are integer
row counts except `max_input_bytes`, which is bytes. Table-property overrides
win over process layout defaults; reported row-group/page/Bloom controls also
include final `ARROWCOL_PARQUET_CONFIG` overrides. With layout disabled,
`config.enabled` and `applied` are false, all six remaining config fields are
null, and `inactive_controls` lists them; independently configured encoding
controls are not claimed as applied layout. Events are emitted only when resolved policy or physical
schema changes, not for individual rows/batches. They prove resolved file policy,
not successful publication; correlate actual output with commit events.

`arrowcol_data_commit <JSON>` records new durable AddFiles, their rows/bytes,
version, table identity, min/max admission stamps and individual commit elapsed
time. It is not client acknowledgement throughput. Primary commit-window output
counts newly durable AddFiles finished in `[start,end)`, including any warmup
backlog committed inside that window. Ingress-cohort attribution is separate:
require admission stamps wholly within cohort boundaries and report mixed or
unknown cohorts as uncertain. Reconciled uncertain commit attribution has null
version/counters/stamps, table identity, finish/elapsed time and a fixed reason.
Deduplicate table/version observations and require
complete logs; global counters alone do not establish either measurement.
Startup `commit_telemetry` identifies schema 1, new ingestion AddFiles after sync,
and system Unix-millisecond finish timestamps. Rewrite/OPTIMIZE events are excluded.

Multipart sizing is deliberately unsupported: pinned object_store 0.13.2 does
not expose conditional multipart completion, while this writer requires
`PutMode::Create`. Unconditional multipart completion would weaken no-overwrite.

Build allocator/ISA variants separately and record their image/source identity:

```bash
cargo build --locked --release --package arrowcol-dataflow-rust --features allocator-mimalloc
RUSTFLAGS="-C target-cpu=native" cargo build --locked --release --package arrowcol-dataflow-rust
```

Native-CPU binaries must run only on compatible CPUs. Jemalloc was considered;
this build offers one opt-in alternative allocator rather than introducing two
native allocator dependencies without comparative workload evidence. No
allocator, compression or concurrency setting is claimed universally faster.
