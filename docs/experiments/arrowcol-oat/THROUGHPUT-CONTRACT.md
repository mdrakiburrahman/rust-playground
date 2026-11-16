# ArrowCol throughput evidence contract

## Objective and fixed workload

| Contract | Requirement |
| --- | --- |
| Objective | Maximize sustained committed **new ingestion rows/s** through one ArrowCollector writer into one hot Delta table; report the throughput/latency trade-off and the first observed failure. |
| Topology | Exactly one ArrowCollector replica/writer and one public synthetic DMV table. Memory-only; no Quiver or crash-recovery claim. Maintenance is enabled and applying throughout. |
| Files | Target approximately 128 MiB (134217728 bytes) of physical Parquet per file. This is a hypothesis to measure, not a row-count guarantee; judge object length including the footer and penalize persistent tiny files. |
| Maintenance | An idle no-work pass writes no Delta log records and creates no commit. Minimize and measure eligibility reads; genuinely due retention, checkpoint, log cleanup and other maintenance still runs. |
| Downstream | QAP/Kusto lag is reported separately from ingest validity. A QAP timeout or incomplete acceleration is not a Delta throughput failure. |

## Primary rate and stage telemetry

For a complete measurement interval `[start, end)`, calculate:

```text
committed_new_rows/s = sum(new ingestion rows from durable commits finished in [start, end)) / (end - start)
```

Deduplicate by `(table identity, Delta version)`. Count only ingestion appends whose durable commit finished in the interval; exclude OPTIMIZE/other rewrites, retries, admission completions and post-window drain. Keep `arrow-deltalake::telemetry::data_commit` and OAT `runner_commit_evidence::commit_window_output` as the primary output series. Keep offered, admitted, committed-window and drain rates separate. Committed-window rate is not unique-delivery proof; report unique delivery as UNKNOWN unless independently established.

Use only bounded metric dimensions: `run`, `variant`, `component`, and the fixed `stage` enumeration. Never add row, publication, file, table-version, or other per-event IDs as metric labels. The stage names are `collect`, `build`, `sort`, `encode`, `upload`, and `commit`. Preserve sample counts and histogram bounds; missing, censored, or unmeasured values are null/UNKNOWN, never a success-shaped zero. Report requested and effective configuration separately.

`crates/arrowcol-dataflow-components/src/metrics.rs` defines the shared `PipelineStage` and `StageMetrics` recorder surface. The initial recorder is a compile-time stub; L4 binds it to the OTel instruments, and L1a/L1b/L2 use the same fixed stage API.

## Tunables and defaults

| Rule | Contract |
| --- | --- |
| Definition source | Typed tunable structures are in `crates/arrowcol-tunables`; table-property names, types, defaults and validators are authoritative in `crates/arrowcol-props/src/lib.rs`. Generated property docs come from that registry. |
| New knobs | Document name, type, default, bounds, consumer and effect. Enum parsing is strict and invalid values fail; do not silently fall back. Record requested and effective values independently. |
| Existing defaults | Lane merges must not change shipped defaults. Promote a winning default only in a separate final commit backed by sweep evidence. |
| Generated docs | After property-registry changes, regenerate with `npx nx run arrowcol-props:generate-docs`; S0 owns generated output and `Cargo.lock`. |

The `arrowcol-tunables` and `arrowcol-props` sources contain empty, delimited slots for each lane (`oat-evidence`, `oat-campaign`, `aks-custody`, `file-formation`, `commit-pipeline`, `maintenance`, `loadgen`, `telemetry`, `grafana`). Fill only the owning slot; S0 resolves mechanical registry-list conflicts.

Existing property-definition ownership:

| Owner | Existing property groups |
| --- | --- |
| L1a `file-formation` | `arrowcol.layout.*`, `arrowcol.writer.*`, and Parquet encoding |
| L1b `commit-pipeline` | Commit, Delta and exporter |
| L2 `maintenance` | Maintenance |
| L3 `loadgen` | Client and loadgen |
| L4 `telemetry` | Telemetry, OTel and receiver |
| E1 `oat-evidence` | OAT and runner |

## Evidence shapes

| Evidence | Required content |
| --- | --- |
| Cell/window | Run and variant identity; table/schema and source/build/image provenance; exact UTC window and sample counts; offered/admitted/committed rows/s; raw/wire/Parquet bytes; stage and admission-to-commit latency percentiles; pending rows/bytes/oldest age and trend; CPU/RSS/resource samples; effective tunables. |
| File distribution | Committed ingestion-file count- and byte-weighted physical-size distributions, rows/file, tiny-file fraction/age, codec and row-group/footer proof. Separate pre- and post-maintenance files. State whether active files were fully decoded/reconciled or only sampled. |
| Maintenance pass | Operation and scope; eligibility reads/bytes; planning, replay, sort, upload and write timings; input/output files and bytes; retries/collisions; active-file/tombstone/checkpoint debt and oldest due age; no-work log writes/commits. |
| Campaign custody | Run/resource identity and generation; one-writer/pod UID/image and ownership observations; scale request/provider receipts; heartbeat/fencing and cleanup receipts; terminal workload state and independently verified smallest-safe-idle state. |

State the latency boundary, sample size, uncertainty/censoring and evidence limitations with every result. QAP completion percentage, pending artifacts and observation freshness are downstream indicators, not proof of row completeness or per-row queryability latency.

## Frozen table

**FINAL (shared brief D6):** `sqlserver_availability_replica_states`, selected from the public-schema/generator estimate. `sqlserver_active_sessions` is retained for quick smoke runs only.

The 22-column estimate is 17 strings plus 36 fixed bytes (one INT32 and four timestamps), approximately 521 B/row of raw Arrow; this is a schema/generator estimate, not a measurement. `customer_id` is constant per client. The other 16 strings and keyed numeric/timestamp fields are nonconstant, with cardinalities capped at 1,024 under defaults; the schema has no booleans.

Reproducibility: generator default seed `20260923` (`ARROWCOL_CLIENT_SEED`). Schema DDL SHA-256 `932d58482285afd5bbf8e2f48b5deec0cb95dd717a62ad9ff53ffebe7275517c` covers the exact checked-in DDL block at lines 32-55 of `projects/loadtest/schemas/sqlserver.sql`; it is not an encoded-schema measurement.

L3's measured top-three Parquet comparison put the selected table within 5.1% of the widest candidate, below the 10% swap threshold; the coordinator has frozen it. Keep the 521 B figure labeled as a schema/generator estimate. Record exact IPC and Parquet bytes/row plus the frozen schema/profile with scored evidence when available.

## Ownership and merge protocol

| Lane | Owns |
| --- | --- |
| S0 integration | This contract, shared workspace manifests/lockfile/generated docs, rolling merges and the single draft PR |
| E1 `oat-evidence` | Tunable framework and OAT Rust evidence/config/runner |
| E2a `oat-campaign` | Campaign launch/verdict, sweep manifests, experiment docs and results tooling |
| E2b `aks-custody` | AKS custody/scaling/reconciliation, Bicep and QAP observer |
| L1a `file-formation` | Parquet layout/sizing/writer and file-batch configuration |
| L1b `commit-pipeline` | Exporter/shutdown and Delta staging/commit/checkpoint pipeline |
| L2 `maintenance` | Maintenance service, Delta maintenance implementation and its operations guide |
| L3 `loadgen` | Public DMV load generator and client |
| L4 `telemetry` | Collector receiver/envelope/metrics, Delta commit telemetry and OTel wiring |
| L6 `grafana` | Load-test Grafana dashboards and Compose observability wiring |

All lanes rebase onto the contract commit before depending on its interfaces, then push only their lane branch. The coordinator directs each integration; S0 merges one completed lane at a time with a merge commit (no squash), regenerates shared artifacts, runs targeted checks and pushes the integration branch. Keep edits within the ownership table; return semantic conflicts to the owning lane. No lane changes shipped defaults; default promotion is a separate, evidence-backed final commit.
