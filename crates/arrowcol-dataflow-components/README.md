# Arrowcol DFE components

Cloud collectors require `ARROWCOL_RESET_GENERATION` and `ARROWCOL_DELTA_ROOT`.
Startup validates the configured UUID against ready storage authority before
starting DFE. The epoch binds the existing persisted `instance_key`, not the
publication app ID. A stale key is a retryable reconciliation diagnostic before
any current-lease fast path or lease rebinding; Quiver retains it, never ACKs or
relabels it under the new epoch. Reset execution remains separately disabled
until all publisher/reader barriers are proven.

The collector installs SDK0.31 OTLP metrics before Delta initialization. The DFE
embedding hooks preserve an unfiltered Kernel layer on every global/thread-local
subscriber (including Noop logging), and flush/shutdown metrics before DFE exits.
Standard OTLP endpoint/protocol variables and millisecond metric interval are
honored; the default interval is 2000 ms.
Its default `service.name` is `arrowcol-dataflow-rust`; `OTEL_SERVICE_NAME`
overrides it and `OTEL_RESOURCE_ATTRIBUTES` adds labels. SDK resource attributes
are `telemetry.sdk.name=opentelemetry`, `telemetry.sdk.language=rust`, and
`telemetry.sdk.version=0.31.0`.

Only a clean controller shutdown closes shared publisher sessions, once the
controller and its drained table owners have been dropped. Closure runs on the
main thread before metric flush. A refused close is logged, leaves its registry
member registered, and exits nonzero without retrying unregister. Startup or
controller errors retain the publisher registry for reconciliation.

`arrowcol-dataflow-components` links two external OTAP Dataflow Engine (DFE)
nodes:

- `receiver:arrow_flight`
- `exporter:arrow_deltalake`

The receiver uses `arrowcol-flight` for Arrow Flight transport, strict catalog
validation, nullable-append alignment, bounded decoding, Flight SQL DDL, and
graceful drain. Each validated `RecordBatch` is moved into
`PdataArrowRecordSet` as representation `arrowcol.flight@1.1`. No Arrow IPC,
protobuf, or value-buffer copy occurs. DFE fan-out clones share Arrow arrays.
The current pluggable-pdata API still requires an OTLP `SignalType`; Arrowcol
uses `Logs` only as an engine discriminator and advertises
`representation_local_not_otap`. The exporter never interprets the batch as an
OTAP logs layout.

The representation-local schema ID carries the logical table, source and target
catalog schema versions, routing lease ID, collector owner, fencing token, and
the admitting collector's stable `registration.instance_key` (added in version
1.1; other versions are rejected). The schema fingerprint is recomputed before
export. The generic Delta exporter accepts only this pluggable representation
and sends the original arbitrary batch to `arrow-deltalake`; it does not invoke
OTAP schema preparation.

Each admitted batch also carries a fresh server UUID (`publication_id`) and
original UTC ingestion milliseconds in this persisted envelope. The exporter
keeps grouped scheduling but writes one file per publication with transaction
`arrowcol.publication.<UUID>` version `0`. Quiver replay preserves that identity
and timestamp even if flush grouping changes; independent equal payloads have
different UUIDs and remain independent. Legacy durable envelopes missing these
fields are refused transiently for operator reconciliation, never silently
relabeled or acknowledged.

The correctness tradeoff is more, smaller Parquet files: one file per admitted
publication, not one file per grouped flush. The exporter stages the whole
group, submits unforced files, and forces only the final file of each bounded
worker-capacity wave, then joins all commit receipts before acknowledging any
batch. Independent transaction IDs can therefore share one Delta log commit
without coupling their replay identity. File-count metrics count those files,
not the number of flush groups.

For legacy pdata with either field absent, `stage` returns a transient error
before opening a table or writing a file. The exporter sends a retryable NACK
with the unchanged pdata and a diagnostic requesting operator reconciliation;
it sends neither an ACK nor a permanent/refused NACK. A durable buffer retains
and retries it according to its configured retention policy. Operators must
not enable destructive `drop_oldest`/expiry policies if retention is required.

`base_path` accepts an absolute local path or an
`abfss://container@account.dfs.core.windows.net/prefix` root. Set
`ARROWCOL_PARTITION_GRANULARITY=minute`, `hour`, or `day` to partition on core
`__arrowcol_ingestion` from the saved admission timestamp, not event time; the
unset default remains unpartitioned. Runtime collector images install Azure
CLI >=2.54 from Microsoft's signed Bookworm repository; mount authenticated
configuration for UID 10001 and point
`AZURE_CONFIG_DIR` at it instead of embedding credentials.

## Delivery semantics

`native_quiver` on `receiver:arrow_flight` selects the Flight completion mode.

With `native_quiver: false` (default), completion is **best-effort
admission**. A successful final `PutResult` means every batch entered its
bounded DFE route. It does not mean Delta publication completed.

With `native_quiver: true`, the topology must route the receiver into
`processor:durable_buffer`, ideally with `wal_flush_interval: 0s`. Completion
is **durable admission**: the receiver registers every batch's DFE
correlation ID with its stream and completes the stream only after the
durable buffer acknowledged each one following its write-ahead-log append.
The wait is bounded by `limits.admission_timeout`. Any rejection or the
timeout fails the stream with a retryable `UNAVAILABLE` error
(`capacity_exhausted` or `backpressure_timeout`); later batches of a rejected
stream fail fast. Acknowledged data survives a crash and is replayed, so
delivery remains at-least-once, while retries of the same stable publication
are transaction-deduplicated. Independent client resubmissions remain separate.

In both modes:

- no SQL delivery receipt is created;
- receipt polling is not supported; and
- downstream Ack/Nack feedback updates pressure and failure metrics.

| Field | Best effort | Durable admission |
| --- | --- | --- |
| `delivery_guarantee` | `best_effort` | `durable_admission` |
| `completion_mode` | `best_effort_admission` | `durable_admission` |
| `at_least_once` | `false` | `true` |
| `receipt_polling`, `sql_delivery_receipts` | `false` | `false` |
| `native_quiver` | `{"supported": true, "enabled": false}` | `{"supported": true, "enabled": true}` |

These statements are repeated by `/v1/capabilities`, `/readyz`
(`delivery_guarantee`), and the `arrowcol_delivery_guarantee_info` metric.

## Fencing, ownership, and replay

The Flight admission hook reads the authoritative routing lease and rejects
non-owners before emitting pdata. Delta publication uses one process-shared
table owner and a commit-boundary `PublicationFence`. The synchronous Delta
fence sends a bounded request to a dedicated routing worker, which reads the
authoritative lease and verifies lease ID, holder, fencing epoch, and expiry.
A failed commit poisons the table's shared commit worker, so the owner
retires and the next batch reopens the table under the current lease.

Before staging, the exporter binds every batch to the authoritative lease
(cached per table for at most one second; the commit fence re-checks it):

- the envelope matches the current lease held by this process: unchanged,
  including while that lease drains;
- otherwise, when the current lease is active, valid, and held by this
  process, and the batch was admitted by this process or by a predecessor
  with this process's instance key: the envelope's lease ID, holder, and
  fencing token are rebound to the current lease; and
- anything else (no lease, a lease held by another collector, or an
  unrelated or unknown instance key) is refused **transiently**, never
  permanently, so the durable buffer keeps the batch and retries it.

Routing transfers a dead predecessor's lease to its same-instance-key
successor, so replayed batches become publishable once the restarted process
holds the lease.

## Pressure

The pressure document counts active Flight streams, Ack-tracked DFE batches,
and pending Delta commits per table. This lets router drain/reassignment wait
for the old owner to empty before a new owner publishes.

With durable admission, global and per-table buffered bytes, rows, and
batches also include the durable backlog: batches the durable buffer
acknowledged to this process's receiver that the exporter has not yet
published or refused permanently. The exporter releases only batches whose
envelope names this process as the admitting holder; predecessor replays are
counted by `arrowcol_dataflow_replayed_batches_total` instead. The backlog
uses signed per-table counters that re-anchor at zero when a table's backlog
empties, so a release observed before its acknowledgement, or a replayed
layout measured slightly differently, never drifts. `quiver_state` is
`disabled` without durable admission, otherwise `blocked` when any backlog
dimension exceeds its `registration.buffered_*_limit`, `degraded` at 80% of a
limit, and `ready` below; `quiver_pressure_bps` is the highest of the three
utilizations. `dfe_state`, `dfe_pressure_bps`, and `oldest_buffer_age_ms`
keep describing in-memory DFE work only. Batches evicted by a Quiver
`drop_oldest` or `max_age` policy are not subtracted, so durable deployments
should use `size_cap_policy: backpressure` without `max_age`.

## Public integration APIs

- `DfeAdmission` implements `arrowcol_flight::BatchAdmission`;
  `with_durable_admission` enables durable completion.
- `PdataSender` is the bounded DFE send seam used by focused tests.
- `StoreLeaseResolver` implements authoritative admission fencing.
- `ArrowcolBatchEnvelope`, `encode_record_batch`, and `decode_record_set`
  define the representation contract.
- `GenericDeltaExporter` owns arbitrary Arrow-to-Delta publication.
- `RuntimeMetrics` supplies registration, rich pressure, capability, and
  Prometheus snapshots; `process_instance_id` and `process_instance_key`
  expose the process identity that replay rebinding uses.

## Validation

```bash
npx --no-install nx run arrowcol-dataflow-components:verify
```

The focused suite covers generated schemas, the `arrowcol.flight@1.1`
envelope, fingerprint validation, shared Arrow ownership, channel
backpressure, durable completion (ACK, NACK, timeout, abort), generic Delta
output, stale fencing, owner retirement, replay rebinding and deferral,
capability honesty in both modes, and Ack-tracked and durable-backlog
pressure.
