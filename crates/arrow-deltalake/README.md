# arrow-deltalake

Streaming Arrow `RecordBatch` -> Parquet / Delta. Local Linux and ADLS Gen2.
No OTEL or Flight dependency. Writes use pinned Delta Kernel 0.29.0;
delta-rs is only the independent loadtest reader.

## Data path

```text
                         Arrow RecordBatch
                                |
                  +-------------+--------------+
                  |                            |
             PARQUET ONLY                    DELTA
                  |                            |
         +--------v----------+        +--------v----------+
         | ParquetFileWriter |        | normalize + align |
         | input schema kept |        | canonical schema  |
         +--------+----------+        +--------+----------+
                  |                            |
             finish().await           DeltaFileWriter
                  |                            |
          closed + synced                 stage().await
                  |                            |
             .parquet file             private .parquet
                                               |
                                      submit -> Kernel commit
                                               |
                                          Delta snapshot
```

Parquet-only: `default-features = false`. Batches stream into files, not a
whole-table buffer.

```rust
use arrow_deltalake::delta::DeltaTableWriter;

async fn append(batch: &arrow::array::RecordBatch) -> anyhow::Result<()> {
    let mut writer = DeltaTableWriter::open("/tmp/my-table")?;
    writer.write(batch).await?;
    writer.close().await?;
    Ok(())
}
```

Standalone `flush` / `close` commits immediately; no collection window.

## Completion accounting and adaptive rotation

Every successful batch write is checked into per-file counters before it is
reported as accepted. `CompletedFile::observation()` returns a path-independent
`{ rows, input_bytes, parquet_bytes }` value; `StagedAppend::observation()` makes
the same counters available before Delta publication. Input bytes describe the
caller's logical Arrow slices (including nested validity and values), not
allocator capacity or Delta's normalized physical columns. Shared buffers count
once per logical reference, so the value remains useful as the denominator for
Parquet compression and file-size control.

`AdaptiveFileSizeEstimator` learns only from completed files. It owns no
`RecordBatch` and returns a bounded logical-input threshold for the caller's
next file:

```rust
use arrow_deltalake::sizing::{
    AdaptiveFileSizeConfig, AdaptiveFileSizeEstimator,
};

let mut sizing = AdaptiveFileSizeEstimator::new(
    AdaptiveFileSizeConfig::default(), // 128 MiB compressed target
)?;
let completed = writer.finish().await?;
let next_input_target = sizing.observe(completed.observation())?;
```

The configuration includes minimum/maximum input clamps, a finite cold-start
Parquet/input ratio, conservative EWMA weight, and ratio bounds. Zero counters,
NaN/infinite settings, inverted bounds, and unsafe divisions are rejected.
Callers can use `record_batch_logical_bytes`, or the streaming writers'
`input_bytes_written()`, to compare the current file with the returned target.

## Cross-core batching

```text
  Core A: write -> stage F1 --+
  Core B: write -> stage F2 --+   concurrent Parquet finalization
  Core C: write -> stage F3 --+
                             |
                             v
           +-----------------------------------------+
           | BatchedTable: one owner per table path  |
           | 256 queued + committing files maximum   |
           | full queue -> async backpressure        |
           +--------------------+--------------------+
                                |
                  64 eligible files OR 10 seconds
                  from the oldest file OR forced flush
                                |
                                v
           +-----------------------------------------+
           | Per-table worker: serial Kernel commits |
           | CREATE / ALTER / INSERT metadata + log  |
           | one INSERT version -> many file adds    |
           +--------------------+--------------------+
                                |
                 sync log + directories -> receipts
                                |
               F1, F2, F3 share the committed version

  Table X: [queue X] -> [worker X] -> _delta_log X
  Table Y: [queue Y] -> [worker Y] -> _delta_log Y
             independent workers, no global commit lock
```

`new_file(...).await` keeps synchronous Kernel work off collector cores.
Shared handles require identical commit options. One collector process owns
the filesystem lock; the last clients close and submitted work drains before
ownership is released.

## Visibility and completion

```text
  write -> stage -> submit -> commit + sync -> receipt.wait()
           private  queued     Delta-visible     SUCCESS

  stage != commit       queue admission != durable delivery
  drop before submit -> no commit
  drop after submit  -> may still commit; cancellation is not rollback
  terminal error     -> fail waiters + poison coordinator; no blind retry

  OTEL adapter's dependency gate (not a cross-table transaction):

  child:  stage -> submit -> commit -> receipt
                                         |
  parent: stage ------------ wait --------+-> submit -> commit

  flush_pending()  -> publish partial admitted batches now
  start_draining() -> bypass delay for every remaining dependency wave
```

Data files are synced before publication. Safe CAS collisions retry with jitter,
up to eight attempts and a ten-second retry-admission window. Stable transaction
identities reconcile uncertain success before any resubmission; an unreadable
outcome fails closed, and unidentified uncertain appends are never replayed.
Local reconciliation/deduplication synchronizes retained replay logs/checkpoints
and their directories before acknowledging: read-back visibility alone does not
repair a failed local durability operation.
Exhaustion and semantic/schema conflicts remain visible errors; orphan files
remain for inspection. The 10-second
collection window is not a visibility bound: dependency waves add latency.

## Publication fencing

`PublicationFence` is a synchronous, storage-agnostic hook for caller-owned
lease or generation state. Open with `DeltaTable::open_with_fence` or
`BatchedTable::open_with_fence`; after the append transaction and file actions
are prepared, the hook runs under the local commit lock immediately before the
versioned Delta log commit. The crate has no SQL/client dependency and performs
no fence retry.

A rejected batched transaction is never made visible. Every receipt in that
transaction receives `CommitReceiptError::PublicationFenced` through
`wait_typed()`; the coordinator is then poisoned just like other uncertain or
commit failures. Existing `wait()` remains available and wraps the typed error
in `anyhow::Error`.

## Shared tables, ADLS identity and reset fencing

Legacy local `open` retains its lifetime writer lock. `open_url` and
`open_uri_with_fence` permit independent maintenance/streaming processes while
Delta's conditional-create log publication arbitrates each commit. Independent
submissions remain independent; only the same application ID/version deduplicates.
Transaction retention must remain unset. Checkpoints retain transaction fences.

`object_store_for_url` supplies the same refreshable Azure CLI identity to
readers/checkpoint clients without opening a writer. Azure runtimes require `az`
2.54 or later, the authorized CLI configuration mounted privately, and
`AZURE_CONFIG_DIR` pointing at it. A token cache alone is not sufficient.
The provider invokes `az account get-access-token` with the Storage scope;
no credentials are embedded in jobs or table metadata. Child execution/output
has a five-second deadline, cancellation kills its dedicated Unix process group
(including the packaged shell wrapper's Python descendants), concurrent credential
admission is bounded to six seconds, and output is limited to 64 KiB.
Cached tokens expire at the earlier of 60 seconds or their expiry minus 30 seconds.
CLI stderr and token responses are never included in errors.
Explicit timeout reaps the direct child; outer-future cancellation retains Tokio's
child reaper. Container runtimes must supply an init/subreaper for orphaned
descendants. The provider never signals the caller's process group. Regression
tests cover both deadline expiry and outer cancellation with delayed descendant
work, not only delayed work in a wrapper shell.

Normal Azure open/upload/commit requires owned, ready checkpoint-container
metadata and the same captured reset generation. Bootstrap-only
`initialize_url_for_bootstrap` instead requires the owned, fenced generation,
normalizes its raw input schema, validates existing roots, and returns no writer.
`StorageAuthority::open/check` lets checkpoint services enforce the same authority
immediately before and after each mutation; failed authority forbids cleanup.
Core repeats authority after slow local staging/checkpoint preparation, and the
committer materializes at most 8 MiB of action arrays before its final check.

Azure HTTP attempts time out after ten seconds and opaque transport retries are
disabled, including for raw stores shared with checkpoint clients. Delta CAS
collisions still use their separate eight-attempt classified outer policy.
One credential acquisition plus one HTTP attempt allows at most 16 seconds of
configured asynchronous waiting after request preparation.
A metadata authority request adds up to six seconds of credential admission
and ten seconds of HTTP waiting. These are **not a hard whole-operation or
server-completion deadline**: CPU scheduling, runtime suspension, serialization,
and already-sent server requests are outside these client bounds. Reset grace
alone is not proof of fencing. Stop/quiesce services before destructive reset;
the stalled-auth tests specifically prove a cancelled CLI cannot later resume a
data/log publication. Real ADLS/reset qualification still requires authorized
subscription access.

## Telemetry

Install the process's global OpenTelemetry meter provider before any Delta work,
then attach `telemetry::kernel_metrics_reporter()` with Kernel's
`WithMetricsReporterLayer` to the global tracing subscriber. Apply log filters
only to the logging layer, not the Kernel reporting layer. The crate does not
install another subscriber/exporter or create placeholder observations.

The `arrow-deltalake` meter exports these actual instruments:

| Instrument | Unit | Bounded attributes |
|---|---|---|
| `delta.kernel.events` | counter | `event`, `outcome` |
| `delta.kernel.duration` | s, histogram | `event`, `phase` |
| `delta.kernel.values` | 1, histogram | `event`, `field` |
| `delta.kernel.bytes` | By, histogram | `event`, `field` |
| `delta.kernel.version` | 1, gauge | `event` |
| `delta.publication.events` | counter | `outcome` |
| `delta.publication.duration` | s, histogram | `outcome` |

Kernel event coverage includes log-segment and protocol loading, snapshot,
transaction prepare/committer/total timings and file counts/bytes, domain and
transaction lookup/cache results, CRC, JSON/Parquet reads, scan selection and
predicate/dedup/transform work, and storage listing/reading/copying. The match is
exhaustive for pinned Kernel 0.29. Publication outcomes include `attempt`,
`conflict`, `retryable_io`, `committed`, `deduplicated`, `reconciled`, `exhausted`
and `failure`; the duration records one terminal observation per publication.
No table URI, account, application UUID or correlation ID is a metric label.
Prometheus/OTLP exporters may normalize dots and append unit/type suffixes.

## Publication file layout

Collector `new_file_with_publication` files containing `customer_id` use the
shared whole-file sort buffer, not isolated per-batch sorting. All rows from one
receipt are sorted together before Parquet encoding; its stable transaction,
ingestion partition, normalized schema, row count and original logical input-byte
accounting are preserved. Legacy `new_file` stays streaming and unchanged.
Schemas without `customer_id` keep streaming unless sort columns are explicit.

| Environment option | Default |
|---|---|
| `ARROWCOL_LAYOUT_SORT_COLUMNS` | `customer_id` (comma-separated, ordered keys) |
| `ARROWCOL_LAYOUT_BLOOM_COLUMNS` | `customer_id` |
| `ARROWCOL_LAYOUT_MAX_ROWS` | 262144 |
| `ARROWCOL_LAYOUT_MAX_INPUT_BYTES` | 67108864 |
| `ARROWCOL_LAYOUT_ROW_GROUP_ROWS` | 16384 |
| `ARROWCOL_LAYOUT_PAGE_ROWS` | 2048 |

Policies enable row-group/page statistics, offset indexes and selected Bloom
filters. Qualification may explicitly set row groups to 512 and pages to 128
for an 8192-row/16-customer source; production defaults are not changed. The
regression exercises multiple interleaved batches through a real publication:
sorted output selects one of 16 groups by customer bounds, while an unsorted
control with identical encoding controls selects all 16.

Bounds apply to retained physical Arrow arrays; sorting/concatenation requires
additional proportional workspace. An oversized publication fails visibly before
commit and must be split upstream into independently identified publications;
it never silently commits a partial receipt. Explicit
`new_file_with_publication_and_layout` supports per-caller policies or `None`
for streaming without changing process environment.

## Arrow representation

```text
  Arrow input              Delta representation
  -----------------------  -------------------------------------------
  UInt8 / UInt16 / UInt32   Int16 / Int32 / Int64
  UInt64                   Decimal128(20,0)
  fixed-width binary       binary, every byte preserved
  dictionary               decoded values; key width is irrelevant
  timestamp                UTC microseconds + <field>__unix_nanos
  duration                 Int64 count + original-unit metadata
  struct / list / map      normalized children; null masks preserved

  timestamp -1 ns -> timestamp -1 us + companion -1 ns
                       floor rounding       exact value

  SCHEMA
    new nullable top-level column -> ALTER -> append
    absent nullable column        -> null-filled
    nested/type/logical changes   -> ERROR
    missing required/new required -> ERROR
```

Timestamp scaling overflow and reserved companion names fail. Bare timestamp
list/map elements need a named struct field. Container logical types live on
the enclosing field because Delta has no element/key/value field metadata.
Schema evolution is separate: a failed append may leave a new nullable column.

## Local table layout

```text
  /tmp/my-table/
  +-- .arrow-lake-writer.lock        exclusive writer ownership
  +-- part-<uuid>.parquet            staged or committed data
  +-- _delta_log/
      +-- 00000000000000000000.json  table metadata
      +-- 00000000000000000001.json  add(F1), add(F2), ...
      +-- ...                       snapshot-selected files only
```

The legacy lock filename and `arrow_lake.original_type` metadata key stay
stable across the crate rename. Corrupt logs/raw Parquet directories are not
adopted as missing tables. `open_with_store` supports local-URL fault injection,
not cloud commit semantics.

No upsert/overwrite, compaction, catalog/cloud authentication, cross-table
atomicity, or exactly-once guarantee.

```bash
npx --no-install nx run arrow-deltalake:verify
```
