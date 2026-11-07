# Metrics and dashboards

[Architecture index](README.md) | [Load generator operations](../loadtest/README.md)

Metrics combine low-cardinality process observations with routing state. Publication IDs, lease UUIDs, customer IDs, and file paths are not metric labels.

| Group | Observations |
| --- | --- |
| Router | Decisions/failures, active streams, cached leases/events, maximum collector pressure |
| DFE process | Flight streams, channel occupancy, received batches/rows/bytes, downstream Ack/Nack, pending commits, Delta files/rows/bytes/failures |
| Source collector SDK | Actual exported values for the seven-family [metric contract](../../../projects/loadtest/src/metric-contract.ts), SDK version, and Dataflow resource |
| SQL routing store | Collector registration, pressure, snapshot age, routing owner, lease age, time to expiry |
| DFE durable buffer | `arrowcol_dataflow_durable_*` backlog and admitted/released/failed batches; replay/rebind/deferred batch counters after restart |
| Delta publication | Version collisions, safe retries, exhaustion, publication outcomes, and file/row/byte changes |
| Standalone maintenance | Plans, applies, no-ops, stale plans, retention decisions, and rewritten/deleted files |
| Tenant stream | Source cursor, lag/backlog, checkpoint and sink commits, replay, and scan/skipping observations |
| Native local Kusto/QAP | Committed source version, cached table version, health, accelerated rows, query correctness, convergence lag, and explicit degradation |
| Azure QAP evidence | Policy/state, accelerated bytes, capacity, query correctness, and touched-file reacceleration |

DFE advertises `best_effort` delivery by default and `durable_admission` when its durable buffer is enabled. Internal Ack/Nack and replay metrics are not a client receipt, publication query, or purge API.

## Dashboard separation

| Grafana folder | Contents |
| --- | --- |
| **Arrow Flight** (home dashboard) | Router, DFE throughput/pressure, Delta files, schema state, resources, logs, traces |
| **OTEL Collector Baseline** | Optional OTEL collector DFE engine, resources, logs, traces only |

The optional OTEL baseline is never mixed into delivery-receipt panels. For finite/continuous runs, retained results, Grafana access, validation, and chaos commands, use the [load generator guide](../loadtest/README.md).

## Evidence boundaries

Dashboard definitions alone are not evidence of live service instrumentation. Qualification must record actual exported series and Grafana/query results against the tested source revision. Keep tenant IDs, file paths, publication IDs, and checkpoint UUIDs out of metric labels.

The read-only QAP observer uses its own private CLI cache and isolates every helper invocation's provenance directory. Missing numeric observations remain unavailable, not healthy-looking zeroes. Completeness and freshness checks precede native-state acceptance. SDK value/version panels and native QAP panels are separate, and each required panel must return actual query-range data, delivered Grafana frames, and an individual screenshot.

Drain publications while admin endpoints are alive, inspect the live panels, then cleanly stop source, tenant and maintenance mutators before immutable snapshot/native validation. Keep Kusto, LGTM and Explorer available. A source-SDK baseline is not full-service qualification of tenant effects, maintenance or native QAP.

Diagnostic cause chains are redacted before logs and send JSONL byte-bound truncation. Redaction in a newer image does not retroactively qualify older raw runtime logs; preserve those privately rather than uploading them as sanitized evidence.

Local Kusto is a functional integration surface, not a performance benchmark. Its asynchronous cache visibility is separate from DFE publication. Bound convergence checks and require current `TableVersion`, healthy `IsHealthy`, and expected `AcceleratedRowCount` from `QueryAccelerationState`, plus cached row-multiset equality with both the independent oracle and uncached fallback. A 100% acceleration statistic, zero pending files, or a fresh update timestamp alone can report success while cached queries remain stale. Azure QAP stress must preserve sanitized policy/state/query observations and distinguish expected reacceleration of rewritten files from an unhealthy catalog, full reset, or uncached fallback. A skipped credential-gated cloud check is not a cloud pass.

Use the [loadtest CLI guide](../../../projects/loadtest/README.md) for current mode selection and evidence locations, and the [loadtest guide](../loadtest/README.md) for the signal path.

Sources: [DFE metrics/capabilities](../../../crates/arrowcol-dataflow-components/src/metrics.rs), [router metrics](../../../projects/arrowcol-router-rust/src/metrics.rs).
