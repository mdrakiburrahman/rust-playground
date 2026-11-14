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
| Mutations | Only explicit `apply: true` adds `--apply`. Supported operations and their applicable knobs are validated; normal retention protections remain active. |

## Result evidence (schema v3)

| Evidence | Attribution and limitations |
| --- | --- |
| `rows`, `rows_per_second` | Client-observed admission completions within the half-open shared window. Neither upload acknowledgement nor WAL stream type proves Delta durability or exactly-once delivery. |
| Client phase counters | Completed minus measurement-start process snapshots. Attempts, errors, cancellations and transport bytes retain this wider snapshot scope; warm-up in-flight work can cross the boundary. |
| Client latency | Per-worker process-cumulative percentiles, explicitly including preflight/warm-up. Do not average percentiles or label them measurement-only. |
| Generator metrics | Prepared corpus sizes, memory reservation and preparation CPU/wall timing reported by the client. Prepared mode does not generate/encode in the hot path. |
| Primary durable output throughput | Newly committed, synced ingestion AddFiles whose producer commit-finished timestamp lies in `[start,end)`, deduplicated by table hash and Delta version. Rows/files/bytes and rows per exact window second include warm-up backlog committed in-window. OPTIMIZE rewrites are excluded. Complete logs and observed ingestion-only telemetry/clock capability are required; known zero differs from missing telemetry. Cross-node clock skew is not measured. |
| Ingress-cohort commit evidence | Separately retains commits whose ingestion stamps lie wholly within the client window. Prior/warm-up commits are excluded here; mixed-window or uncertain reconciliation yields null plus a reason. This stricter attribution is not the primary output-throughput metric. |
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
