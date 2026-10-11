# arrow-lake

Pure Arrow `RecordBatch` writers, independent of OTEL and Arrow Flight.
Parquet-only mode retains the input Arrow schema. Delta mode writes Parquet
and uses pinned Delta Kernel APIs to create, evolve, and commit Delta tables.
No delta-rs dependency is present in the writer; the loadtest reader uses it
independently to verify interoperability.

```rust
use arrow_lake::delta::DeltaTableWriter;

# async fn write(batch: arrow::array::RecordBatch) -> anyhow::Result<()> {
let mut writer = DeltaTableWriter::open("/tmp/my-table")?;
writer.write(&batch).await?;
writer.flush().await?;
writer.close().await?;
# Ok(())
# }
```

The SDK also exposes `ParquetFileWriter` and `DeltaTable::new_file` for callers
that own batching, file sizing, or visibility ordering. Data is streamed;
the writer does not collect a whole table in memory. Finish/flush/close must
be awaited. Dropping an unsubmitted writer does not commit, and unfinished files
are not visible through Delta snapshots. Use `default-features = false` for
Parquet-only consumers.

## Shared batched publication

`commit::BatchedTable` shares one local table owner across collector cores.
`new_file(...).await` obtains a streaming writer without synchronous Kernel
work on the caller's runtime. `DeltaFileWriter::stage().await` finalizes and
synchronizes Parquet without changing the Delta snapshot; submit that staged
file only when its dependencies have committed.

`submit(..., false).await` waits for bounded queue admission and returns a
receipt. **Await `receipt.wait()` for durable publication**, not merely
admission. Several files can receive the same committed version.

Defaults are a **10-second** collection window, **64 files per commit**, and
**256 queued plus committing files per table**. The window starts with the
oldest eligible file and is not extended by new arrivals. File-count limits
commit sooner; `flush_pending()` publishes admitted partial batches.
`start_draining()` bypasses collection delays for the owner's remaining
dependency waves. Standalone `DeltaTableWriter::flush`/`close` remain immediate.

Each table has its own bounded worker for synchronous Kernel metadata and log
operations, plus Kernel's background I/O executor. Tables do not share a global
commit lock; a short registry lock only resolves canonical ownership. Private
Arrow data and Parquet finalization remain parallel. A second independent
process cannot acquire the same table's writer lock. Shared handles must use
identical commit options.

Already submitted files may finish committing even if their receipt is dropped;
cancellation is not a rollback. A failed or uncertain commit poisons the
coordinator and fails subsequent work rather than replaying possibly published
files. Workers release ownership after their last client closes and submitted
work is drained.

## Representation and schema

Delta mode normalizes unsupported Arrow physical types explicitly:

| Arrow input | Delta representation |
| --- | --- |
| Unsigned 8/16/32-bit integers | Signed 16/32/64-bit integers |
| `UInt64` | `Decimal128(20,0)` |
| Fixed-width binary | Binary, preserving every byte |
| Dictionary encoding | Decoded logical values |
| Timestamp | UTC microsecond timestamp plus `<field>__unix_nanos` |
| Duration | Signed 64-bit count, with original unit metadata |

Original logical type information is stored in field metadata. Timestamp
companions retain exact epoch nanoseconds; negative epochs use floor rounding
for the microsecond representation. Scaling overflow and reserved companion
names fail. Struct and list/map-of-struct normalization preserves null masks.
Bare timestamp list elements or map keys/values must be wrapped in a struct
with an explicitly named timestamp field so its companion has a stable name.
Because Delta has no list-element or map-key/value field metadata, their
logical types are recorded on the enclosing field. Canonical element/key/value
names are used; dictionary key widths do not affect logical compatibility.

The canonical schema is persisted in Delta. New nullable top-level columns
are added automatically; older rows read null. Missing nullable columns in
new batches are null-filled. Nested additions, type changes, non-nullable
additions, missing required fields, and incompatible logical metadata fail.
Schema evolution is a separate metadata transaction before the data append:
a failed append can leave a new nullable column without new data.

## Storage and guarantees

The supported storage target is a local Linux filesystem. One `DeltaTable`
owns an exclusive writer lock per table; several files can share that owner.
`open_with_store` permits injecting an object store for local-file URLs,
including fault-injection tests. It does not promise cloud commit semantics.

Data files are closed and synchronized before the Kernel commit is published.
Kernel's filesystem committer atomically creates the versioned log; the SDK
synchronizes the commit and containing directories before reporting success.
Errors, conflicts, and uncertain commit outcomes are returned explicitly, not
retried blindly. Uncommitted files remain for diagnostics. Corrupt logs and
existing raw Parquet directories are not treated as missing tables.

There is no upsert, overwrite, compaction, catalog, cloud authentication,
cross-table transaction, or exactly-once delivery guarantee. Delta Kernel's
experimental APIs are pinned and isolated in this crate.

```bash
npx --no-install nx run arrow-lake:verify
```
