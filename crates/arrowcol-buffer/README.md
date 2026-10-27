# arrowcol-buffer

Bounded in-memory buffering and durable publication coordination for direct
Arrow collectors.

## Design

- Queues are isolated by table, registered schema version/fingerprint, and
  table lease. `RecordBatch` values keep their existing reference-counted Arrow
  arrays; batches are never concatenated or deep-copied.
- Admission reserves global and per-table byte, row, and batch capacity before
  the manager retains a delivery. Capacity remains reserved through durable
  publication and receipt completion.
- A queue flushes on the first adaptive compressed-size target, Arrow-memory
  limit, row limit, batch limit, maximum age, schema boundary, lease boundary,
  explicit drain, or shutdown.
- `PublicationSink` / `PublicationSession` form the storage-neutral streaming
  worker interface. `DeltaPublicationSink` adapts the shared
  `arrow-deltalake::commit::BatchedTable`, preserving typed stale-fence and
  uncertain-commit failures.
- Each receipt moves to flushing before sink I/O and becomes
  published only after the sink reports a durable commit. A SQL failure after
  durable storage poisons the table and appears in `ShutdownReport` with the
  publication observation.
- A flush trigger schedules independent per-receipt files, even when several
  deliveries share a queue. The stable Delta transaction is
  `arrowcol.publication.<receipt UUID>` version `0`, never a payload hash or a
  flush UUID. Identical independent submissions remain independent; retries
  and crash replay of the same receipt cannot append its rows again.
  This favors correctness over large-file efficiency: the direct manager
  serializes per-receipt jobs for a table, so its independent receipts are not
  coalesced into one file or one commit. Adaptive thresholds still govern
  queued flush scheduling; file compaction belongs in table maintenance.
- UTC ingestion milliseconds are captured once before admission waits and
  passed explicitly to Delta. With `ARROWCOL_PARTITION_GRANULARITY` set to
  `minute`, `hour`, or `day`, core Delta stamps `__arrowcol_ingestion` from
  this timestamp, not event time. The unset default remains unpartitioned.
- `BufferManager::pressure` reports exact capacity held by queued and in-flight
  batches. Flush concurrency is bounded independently.
- `shutdown` closes admission, waits for active admissions, drains all queues,
  releases Arrow references, and reports incomplete work.

## Direct worker integration

Create one validated receipt per Flight delivery, then submit:

```text
Admission {
  receipt: ReceiptVersion,
  delivery: ValidatedDelivery,
  lease: TableLease,
  batches: Vec<RecordBatch>,
}
```

Use `admit_with_timeout` when the transport has a request deadline. Call
`drain` for an explicit flush while admission remains open. During process
shutdown, first put the SQL owner into draining state, call `shutdown`, resolve
or report any `IncompleteWork`, and then finish the owner drain.

## Quiver durable stage

`QuiverStage` puts a Quiver write-ahead log and segment store in front of the
buffer for direct collectors that must not lose acknowledged payloads:

- `persist` writes one accepted delivery (its record batch plus a versioned
  manifest with the receipt, original UTC ingestion stamp, owner, table lease,
  and schema identity) to Quiver
  as bundle metadata. With a zero WAL flush interval it returns only after the
  WAL fsync, so an acknowledgement implies durable local storage.
- A consumer reads finalized bundles back from disk, admits them through
  `BufferManager::admit_tracked`, and acknowledges a bundle only after its
  receipt is published (or Delta committed). Terminal receipt failures drop the
  bundle; unknown outcomes defer it for a retry with backoff.
- After a crash, replayed bundles are reconciled against SQL: published
  receipts are acknowledged without republishing, failed ones are dropped, and
  active receipts of a predecessor (or of an older lease) are adopted onto the
  current owner and lease through `ReceiptStore::adopt_receipts` before they are
  published. Receipts adopted while `flushing` reuse the original receipt
  transaction and ingestion timestamp, so a commit that landed just before a
  crash is skipped rather than duplicated. Legacy manifests without a stamp
  use the durable receipt's immutable creation time, never replay time.
- `check_admissible` rejects deliveries that could never fit the buffer before
  they are persisted. `drain` waits for a stable empty backlog; `snapshot`
  reports disk usage, backlog, and counters for pressure and metrics.

The bundle format is public: `DeliveryManifest` (`arrowcol.direct` version 1)
and `DeliveryBundle` put one record batch in `DELIVERY_SLOT`, and
`DELTA_SUBSCRIBER` names the consumer. [`quiver-cli`](../../bin/quiver-cli/README.md)
uses them to write and decode the same bundles.

`admit_tracked_at` lets durable stages preserve the original ingestion stamp
when admitting recovered deliveries whose
receipts are already `buffered` or `flushing` (`TrackedState`) and returns a
`DeliveryCompletion` that resolves to a `DeliveryOutcome`.

## Validation

```bash
npx --no-install nx run arrowcol-buffer:verify
```

Tests cover zero-copy array identity, capacity/backpressure, every flush
trigger, adaptive convergence, schema isolation, independent receipt publication,
receipt and sink failures, stale fencing, cancellation, explicit drain,
shutdown reporting, publishing persisted batches from disk, crash replay with
receipt adoption, and acknowledging already-published replays without
republishing.
