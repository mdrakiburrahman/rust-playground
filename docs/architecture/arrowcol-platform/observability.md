# Metrics and dashboards

[Architecture index](README.md) | [Load generator operations](../loadtest/README.md)

Metrics combine low-cardinality process observations with SQL-derived durable
state. Receipt IDs and lease UUIDs are **not** metric labels.

| Group | Observations |
| --- | --- |
| Router | Decisions/failures, active streams, cached leases/events, maximum collector pressure |
| Direct process | Streams, decoder bytes, received batches/rows/bytes, control failures, reconciled receipts, incomplete shutdown groups |
| DFE | Channel occupancy, downstream Ack/Nack, pending commits, Delta files/rows/bytes/failures, delivery-guarantee truth |
| SQL receipts | State/count/rows/Arrow bytes, published totals, failures, creation-to-publication and flushing-to-publication latency |
| SQL collectors/tables | Pressure, snapshot age, routing owner, lease age, time to expiry |
| Native Quiver (Direct) | `arrowcol_direct_quiver_*`: disk used/capacity, backlog, persisted/published/rejected/deferred batches, batches recovered and receipts adopted after a restart, backpressure waits |
| Native Quiver (DFE) | `arrowcol_dataflow_durable_*` backlog and admitted/released/failed batches; `arrowcol_dataflow_{replayed,rebound}_batches_total` and `arrowcol_dataflow_replay_deferred_total` after a restart |
| Delta publication | Version collisions, safe retries, exhaustion, publication outcomes, and file/row/byte changes |
| Standalone maintenance | Plans, applies, no-ops, stale plans, retention decisions, and rewritten/deleted files |
| Tenant stream | Source cursor, lag/backlog, checkpoint and sink commits, replay, and scan/skipping observations |
| Native local Kusto/QAP | Committed source version, cached table version, health, accelerated rows, query correctness, convergence lag, and explicit degradation |
| Azure QAP evidence | Policy/state, accelerated bytes, capacity, query correctness, and touched-file reacceleration |

DFE advertises
`arrowcol_delivery_guarantee_info{delivery_guarantee="best_effort",...}` with
`receipt_polling="false"` and `at_least_once="false"`. With native Quiver it
reports `delivery_guarantee="durable_admission"` and `at_least_once="true"`.
Internal Ack/Nack metrics are not a per-delivery client purge API.

## Dashboard separation

| Grafana folder | Contents |
| --- | --- |
| **Arrow Flight** (home dashboard) | Router, direct receipts, direct-vs-DFE throughput/pressure, Delta files, schema state, resources, logs, traces |
| **OTEL Collector Baseline** | Optional OTEL collector DFE engine, resources, logs, traces only |

The optional OTEL baseline is never mixed into arbitrary-Arrow receipt/delivery
panels. For finite/continuous runs, retained results, Grafana access, validation,
and chaos commands, use the [load generator guide](../loadtest/README.md).

## Evidence boundaries

Dashboard definitions alone are not evidence of live service instrumentation.
Qualification must record actual exported series and Grafana/query results
against the tested source revision. Keep tenant IDs, file paths, receipt IDs,
publication IDs, and checkpoint UUIDs out of metric labels.

Local Kusto is a functional integration surface, not a performance benchmark.
Its asynchronous cache visibility is separate from Direct receipt publication.
With local `MaxAge=1min`, bound convergence checks to five minutes and require
current `TableVersion`, healthy `IsHealthy`, and expected `AcceleratedRowCount`
from `QueryAccelerationState`, plus cached row-multiset equality with both the
independent oracle and uncached fallback. A 100% acceleration statistic, zero
pending files, or a fresh update timestamp alone can report success while
cached queries remain stale. Azure QAP stress
must preserve sanitized policy/state/query observations and distinguish expected
reacceleration of rewritten files from an unhealthy catalog, full reset, or
uncached fallback. A skipped credential-gated cloud check is not a cloud pass.
Use the [loadtest CLI guide](../../../projects/loadtest/README.md) for current
mode selection and evidence locations, and the
[root diagram](../../../loadtest-architecture.html) for the full signal path.
Sources: [direct metrics](../../../projects/arrowcol-rust/src/metrics.rs),
[DFE metrics/capabilities](../../../crates/arrowcol-dataflow-components/src/metrics.rs),
[router metrics](../../../projects/arrowcol-router-rust/src/metrics.rs).
