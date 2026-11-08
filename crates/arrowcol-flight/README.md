# arrowcol-flight

Reusable Arrow Flight 58.3-compatible ingress runtime shared by the direct and
OTAP dataflow collectors. The crate owns transport concerns and deliberately
does not own a listener, receipt store, direct buffer, Delta publisher, or DFE
pipeline.

## Ingress contract

`ArrowColFlightService` implements tonic's generated `FlightService` and can be
wrapped with `into_tonic_service()`. For collector `DoPut` streams it:

1. bounds and parses the generated SDK routing metadata through
   `arrowcol-protocol`;
2. rejects unvalidated internal identity claims, then calls the async
   `Authenticator`;
3. incrementally reads the first Arrow schema message and validates the
   descriptor path (`catalog/schema/table/vN`);
4. computes the canonical fingerprint of the received schema and asks
   `arrowcol-catalog::SchemaValidator` for the exact historical/current version
   plus its null-fill plan;
5. only then calls `BatchAdmission::begin`, so unknown or breaking schemas
   cannot create receipts;
6. incrementally decodes record batches into a bounded channel, retaining Arrow
   array ownership and adding only all-null arrays for compatible historical
   schemas;
7. drives `StreamLifecycle` with deadline, cancellation, drain, and response
   backpressure handling.

The service enforces metadata, schema, and protobuf message limits in addition
to tonic's decoding limit. `DrainController` stops new admission, waits for
active work, and force-cancels only after a caller-selected graceful timeout.

## Server implementation APIs

Direct collector adapters implement:

- `Authenticator`
- `BatchAdmission::begin`
- `StreamLifecycle::{push_batch, complete, abort}`
- `StreamCompletion::receipt(receipt_id)`

Direct implementations that create one receipt per incrementally admitted
record batch can emit each receipt through `PutResultEmitter` and finish with
`StreamCompletion::results_already_emitted()`. The runtime then does not append
a duplicate synthetic final receipt.

The DFE collector implements the same traits but completes with
`StreamCompletion::best_effort()`. Its JSON `PutResult` is explicitly labeled
`"delivery_guarantee":"best_effort"`; it is not presented as a durable
receipt.

When the DFE collector persists batches to an on-disk durable buffer (Quiver)
and waits for every batch to be persisted before completing, it completes with
`StreamCompletion::durable_admission()`. That `PutResult` uses
`"kind":"durable_admission"` and `"delivery_guarantee":"durable_admission"`.
The collector then owns delivery to Delta, including after a crash, but there
is still no receipt to poll.

Implementations can emit bounded progress acknowledgements through the supplied
`PutResultEmitter`. `PressureHooks` receives non-blocking stream/channel
transitions. `CapabilityReporter`, `PressureReporter`, `HealthReporter`, and
`ReadinessReporter` feed `AdminHandlers`, which returns framework-neutral HTTP
status/content-type/body parts for:

- `/healthz`
- `/readyz`
- `/v1/capabilities`
- `/v1/pressure`

`ShutdownCoordinator` is the implementation-neutral stop/drain contract;
`DrainController` is the provided race-safe implementation used by the runtime.

## Stable `PutResult` metadata

Completion metadata is compact versioned JSON. Direct metadata keeps
`receipt_id` at the top level, so generated Rust and TypeScript SDK
`parse_receipt_id` helpers remain compatible while newer clients can decode the
full `PutResultMetadata`:

```json
{
  "kind": "receipt",
  "wire_version": 1,
  "protocol_version": 1,
  "receipt_id": "c7f0d870-5870-4ec7-9de8-c7ef1f0ac158",
  "delivery_guarantee": "durable_receipt",
  "source_schema_version": 1,
  "target_schema_version": 2,
  "schema_fingerprint": "sha256:...",
  "summary": { "batches": 2, "rows": 200, "bytes": 4096 }
}
```

## Flight SQL DDL

When configured with `with_ddl_handler`, the same `DoPut` endpoint accepts only
Flight SQL `CommandStatementUpdate`. It invokes
`arrowcol_catalog::FlightSqlDdlHandler`, rejects transactions and body batches,
and returns the standard protobuf `DoPutUpdateResult { record_count: 0 }`.
Committed schema version, fingerprint, saga, identity, and disposition are
returned as response metadata plus `x-arrowcol-ddl-completion-bin`.

Routed CREATE TABLE / ADD COLUMNS requests carry the same table-routing metadata
as data uploads. The command descriptor selects DDL dispatch, and its parsed
table identity must match those headers before any catalog mutation. Direct DDL
without routing metadata remains supported. The claimed Arrow fingerprint is
validated on subsequent data uploads, not used as a DDL dispatch discriminator.

## Validation

```bash
npx --no-install nx run arrowcol-flight:verify
```
