# Buffers, files, and Delta

[Architecture index](README.md) | [Delivery boundary](delivery-guarantees.md)

## Direct controls

Checked-in host values in [direct config](../../../projects/arrowcol-rust/config.yaml):

| Control | Value | Bound |
| --- | ---: | --- |
| `buffer.global_capacity.bytes` | 2 GiB | Retained and in-flight process Arrow bytes |
| `buffer.per_table_capacity.bytes` | 512 MiB | Retained and in-flight table Arrow bytes |
| `buffer.max_arrow_memory_per_flush` | 512 MiB | Logical Arrow rotation |
| `buffer.max_rows_per_flush` | 1,000,000 | Row rotation |
| `buffer.max_batches_per_flush` | 512 | Batch rotation |
| `buffer.max_age_ms` | 30,000 | Oldest-batch flush trigger |
| `buffer.max_concurrent_flushes` | 4 | Independent flushes |
| `buffer.adaptive_file.target_parquet_bytes` | 128 MiB | Compressed Parquet target |
| `buffer.adaptive_file.min_input_bytes` | 16 MiB | Lower input estimate clamp |
| `buffer.adaptive_file.max_input_bytes` | 1 GiB | Upper input estimate clamp |
| `delta.commit_batch_window_ms` | 10,000 | File collection window |
| `delta.max_files_per_commit` | 64 | File actions per commit |
| `delta.max_pending_files` | 256 | Queued and committing files |

The loadtest direct template keeps the 128 MiB compressed target, lowers the
cold-start minimum input clamp to 1 MiB, and shortens age/commit windows for
finite tests. These are configuration bounds, not unbounded defaults.

[DFE config](../../../projects/arrowcol-dataflow-rust/config.yaml) separately
bounds Flight decoding, pdata channels, buffered capacity, authority timeout,
and pending Delta files. Healthy queues do not turn admission into durability.

## Separate roots

| Context | Direct | DFE |
| --- | --- | --- |
| Host config | `projects/arrowcol-rust/.data/delta` | `ARROWCOL_DELTA_ROOT`, default `/onelake` |
| Direct container config | `/var/lib/arrowcol/delta` | Not applicable |
| Benchmark host | `onelake/benchmark/<run-id>/direct` | `onelake/benchmark/<run-id>/dataflow` |
| Benchmark container | `/delta/direct` | `/delta/dataflow` |

## Persistence limits

SQL stores receipt/control metadata, fixed-size hashes, bounded counts, errors,
and publication observations, **not pending ingest payloads**. By default,
collector loss loses in-memory Arrow arrays.

Native Quiver is optional local-disk payload persistence. It is enabled with
`buffer.native_quiver` for Direct, and with `native_quiver: true` plus
`processor:durable_buffer` for DFE. Each Quiver bundle carries bounded metadata,
so it preserves the table, schema, fingerprint, and lease identity of
arbitrary Arrow. Pending batches then survive a process crash, and the restarted
collector replays them. See
[delivery guarantees](delivery-guarantees.md#native-quiver-disk-persistence).

| Quiver directory | Direct | DFE |
| --- | --- | --- |
| Config key | `buffer.native_quiver.directory` | `processor:durable_buffer` `path` (per-core `core_N/`) |
| Benchmark host | `onelake/benchmark/latest/direct-state-1/quiver` | `onelake/benchmark/latest/dataflow-state-1/quiver` |
| Benchmark container | `/state/quiver` | `/quiver` |

[Quiver on disk](../../quiver/README.md) shows these files with `quiver-cli`.

A Quiver directory must outlive the process and belong to one
`instance_key`. Disk usage is capped by the configured capacity, and a full
budget backpressures ingest.

## Publication and storage modes

Collectors append through Delta Kernel; maintenance is a separate process, not
an unbounded background task in the collector. Direct and DFE retain independent
roots. Local mode uses shared disk for functional integration and recovery.
Azure mode uses ADLS Gen2 with data publication before conditional transaction-log
publication. Local filesystem results do not prove cloud atomicity.

Publication retry identity is not client-payload deduplication. A stable
transaction identity protects retries and recovery of the same publication;
independent client requests still append independently. Version collisions may
be retried only when classified safe and bounded; metadata changes, stale
maintenance read sets, and uncertain outcomes require explicit reconciliation.
There is no cross-table transaction.

Partition stamps come from ArrowCol ingestion time, not an event timestamp in
the payload. Assign the UTC stamp once and retain it across retry and Quiver
replay. The loadtest uses minute partitions and explicit `customer_id` in all
13 schemas. Customer sorting is a file/row-group layout policy, not a promise of
global table ordering or end-to-end zero-copy.

## Maintenance and downstream ownership

| Consumer | Source behavior | Recovery boundary |
| --- | --- | --- |
| Standalone OPTIMIZE | Revalidate planned active files; replace them atomically with `dataChange=false` | A stale plan cannot overwrite another writer's work; a no-op leaves the version unchanged |
| Partition PURGE | Remove existing expired ingestion-time partitions with `dataChange=true` | Derive work from the active snapshot, not guessed directory names |
| VACUUM LITE | Physically delete eligible retained tombstones, without a new data commit | Seven-day retention floor by default; insufficient history fails visibly; arbitrary orphan discovery is not LITE |
| Historical tenant fork | Capture a snapshot, then tail committed data changes; skip OPTIMIZE | Independently encoded sink files survive source purge; missing required source history is an error |
| Local Kusto bridge | Reconcile the current committed active snapshot | Replacements and removals change the visible generation; this is not historical-fork behavior |

Tenant sink transactions bind query/batch identity to publication so recovery
after a sink commit cannot duplicate that batch. Spark-compatible metadata,
offset, and commit logs describe Rust-owned recovery; they do not imply a
Spark aggregation state store or bidirectional Spark job handoff.

Retention must cover the supported recovery/consumer lag. An append-only
historical fork is not a promise of unbounded source history: it retains rows
already copied, but cannot recover unread files after physical deletion.
Shorter-than-safe VACUUM retention is an explicit isolated-test opt-in, never
the ordinary operating default.

See the [lakehouse loadtest architecture](../../../loadtest-architecture.html)
for local/Azure identity, validation, Kusto, and observability boundaries.

Sources: [buffer manager](../../../crates/arrowcol-buffer/src/manager.rs),
[direct sink](../../../projects/arrowcol-rust/src/delta_sink.rs),
[DFE exporter](../../../crates/arrowcol-dataflow-components/src/exporter.rs).
