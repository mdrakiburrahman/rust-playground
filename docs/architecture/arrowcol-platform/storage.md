# Buffers, files, and Delta

[Architecture index](README.md) | [Delivery boundary](delivery-guarantees.md)

## DFE controls

[DFE config](../../../projects/arrowcol-dataflow-rust/config.yaml) bounds Flight decoding, pdata channels, buffered capacity, authority timeout, pending Delta files, and file batching. Healthy queues do not turn admission into durability. SQL Server stores router registrations, pressure, and routing/fencing leases only; it stores no catalog, delivery receipt, or Arrow payload.

## Dataflow Delta root

| Context | Dataflow |
| --- | --- |
| Host config | `ARROWCOL_DELTA_ROOT`, default `/onelake` |
| Benchmark host | `onelake/benchmark/<run-id>/dataflow` |
| Benchmark container | `/delta/dataflow` |

## Persistence limits

SQL stores routing/control metadata only; it has no delivery receipt or publication-status ledger. By default, DFE process loss loses in-memory Arrow arrays.

The DFE durable buffer is optional local-disk payload persistence. It is enabled with
`native_quiver: true` plus `processor:durable_buffer`. Each Quiver bundle carries bounded metadata,
so it preserves the table, schema, fingerprint, publication identity,
ingestion-time stamp, and original lease identity of
arbitrary Arrow. Pending batches then survive a process crash, and the restarted
collector replays them. See
[delivery guarantees](delivery-guarantees.md#admission-versus-publication).

| Durable-buffer directory | DFE |
| --- | --- |
| Config key | `processor:durable_buffer` `path` (per-core `core_N/`) |
| Benchmark host | `onelake/benchmark/latest/dataflow-state-1/quiver` |
| Benchmark container | `/quiver` |

[Quiver on disk](../../quiver/README.md) documents the DFE durable-buffer layout.

A Quiver directory must outlive the process and belong to one
`instance_key`. Disk usage is capped by the configured capacity, and a full
budget backpressures ingest.
In Azure mode, replay must also pass the original captured reset-generation
check before WAL admission or lease rebinding. A visible generation marker is
not sufficient after an uncertain fsync: its file and directory ancestry must
be durably repaired before admitting work.

## Publication and storage modes

The DFE collector appends through Delta Kernel; maintenance is a separate process, not
an unbounded background task in the collector. Local mode uses shared disk for functional integration and recovery.
Azure mode uses ADLS Gen2 with data publication before conditional transaction-log
publication. Local filesystem results do not prove cloud atomicity.

Publication retry identity is not client-payload deduplication. A stable
transaction identity protects retries and recovery of the same publication;
independent client requests still append independently. Version collisions may
be retried only when classified safe and bounded; metadata changes, stale
maintenance read sets, and uncertain outcomes require explicit reconciliation.
There is no cross-table transaction.

Every Azure writer process uses leased publisher admission pinned to its
captured `ARROWCOL_RESET_GENERATION`, before owned data, log or checkpoint I/O. Source collectors,
tenant sink/checkpoint, and maintenance all participate. The tenant sink and
checkpoint share one session. Clean close follows drain; pending or uncertain
physical operations retain their blocking registration rather than expiring
through a TTL or being forcibly released.

Partition stamps come from ArrowCol ingestion time, not an event timestamp in
the payload. Assign the UTC stamp once and retain it across retry and Quiver
replay. The loadtest uses minute partitions and explicit `customer_id` in all
13 schemas. Customer sorting is a file/row-group layout policy, not a promise of
global table ordering or end-to-end zero-copy.

The publication path retains physical batches within a bounded file buffer and
sorts the whole staged file once by customer before encoding Parquet. Production
defaults are 16,384 rows per row group and 2,048 rows per page. The focused
8,192-row, two-batch, 16-customer qualification explicitly uses 512-row groups
and 128-row pages; its one-of-16 matching-group result is not a production
tiny-receipt pruning claim. Independent publication files often contain only
one production-sized row group. DFE publications retain their transaction identity rather than coalescing unrelated
client requests to manufacture larger groups. The internal identity is not a client receipt.

## Maintenance and downstream ownership

| Consumer | Source behavior | Recovery boundary |
| --- | --- | --- |
| Standalone OPTIMIZE | Revalidate planned active files; replace them atomically with `dataChange=false` | A stale plan cannot overwrite another writer's work; a no-op leaves the version unchanged |
| Partition PURGE | Remove existing expired ingestion-time partitions with `dataChange=true` | Derive work from the active snapshot, not guessed directory names |
| VACUUM LITE | Physically delete eligible retained tombstones, without a new data commit | Seven-day retention floor by default; insufficient history fails visibly; arbitrary orphan discovery is not LITE |
| Kernel checkpoint | Materialize current state and preserve publication/query transaction IDs | Checkpoint cadence alone does not remove history or prove a storage plateau |
| Guarded log pruning | Prove checkpoint state and eligible history against durable vacuum progress and a retained floor | Keep the checkpoint and its commit JSON; only eligible earlier JSON may be removed; a stricter resume retention policy is rechecked before further deletion |
| Historical tenant fork | Capture a snapshot, then tail committed data changes; skip OPTIMIZE | Independently encoded sink files survive source purge; missing required source history is an error |
| Native Kusto external Delta/QAP | Follow the current committed active snapshot after asynchronous cache refresh | Replacements and removals must converge within the bounded correctness gate; cached queries must match the independent oracle and MaxAge-overridden query |

Tenant sink transactions bind query/batch identity to publication so recovery
after a sink commit cannot duplicate that batch. Spark-compatible metadata,
offset, and commit logs describe Rust-owned recovery; they do not imply a
Spark aggregation state store or bidirectional Spark job handoff.

Retention must cover the supported recovery/consumer lag. An append-only
historical fork is not a promise of unbounded source history: it retains rows
already copied, but cannot recover unread files after physical deletion.
Shorter-than-safe VACUUM retention is an explicit isolated-test opt-in, never
the ordinary operating default.
Local physical deletions are durably synced before vacuum progress is
acknowledged. Log pruning preserves replay transaction state, but it does not
reconstruct missing unread source commits: a consumer whose required cursor
falls below retained history must fail without advancing its offset.

See the [lakehouse loadtest guide](../loadtest/README.md)
for local/Azure identity, validation, Kusto, and observability boundaries.

Sources: [DFE receiver](../../../crates/arrowcol-dataflow-components/src/receiver.rs),
[DFE exporter](../../../crates/arrowcol-dataflow-components/src/exporter.rs),
[remote catalog](../../../crates/arrowcol-catalog-client/src/remote.rs).
