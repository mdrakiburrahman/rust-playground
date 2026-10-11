# otel-deltalake-exporter

Registers `exporter:deltalake` in the rust-playground collector. The adapter
reuses otel-arrow's Parquet exporter preparation, ID generation, file
scheduling, age/row flushes, and child-before-parent completion. Its injected
file backend calls the pure-Arrow `arrow-lake` SDK; no Delta writer code lives
in otel-arrow and no delta-rs dependency enters the collector.

Each existing OTAP payload table becomes an independent Delta table.
`_otel_join_namespace` is persisted on every parent and child row. Join
`(namespace, id)` to `(namespace, parent_id)`, never numeric IDs alone:
the generator's namespace changes after restarts or ID rollover.

Only absolute local `storage.file.base_uri` paths are accepted. Delta
partitioning and cloud retry settings are rejected. File completion includes
the Delta commit, so children must commit before parent files become visible.
There is no atomic transaction across payload tables or exactly-once delivery.

The shared exporter's signal metrics remain enabled; its Parquet-specific
I/O metric set is disabled for the Delta backend rather than mislabeling
Delta commits as Parquet writes.

## Multi-core file and commit pipeline

Collector cores retain their own OTAP processing and streaming Parquet writers.
They finalize private files concurrently, then submit eligible files to one
shared `arrow-lake` commit worker per table. **One Delta data commit can publish
many files from different cores.** Other tables have independent workers.

Configure the adapter without adding Delta settings to the fork:

```yaml
storage:
  file:
    base_uri: /onelake
writer_options:
  target_rows_per_file: 1000
  flush_when_older_than: 2s
commit_options:
  batch_window: 10s
  max_files_per_commit: 64
  max_pending_files: 256
```

These are the sample/default commit settings. A full batch commits sooner.
The pending limit includes queued and committing files; each core also bounds
unfinished files per payload type. Cores continue ingesting while receipts are
pending, until backpressure closes pdata admission. Completion and control
messages continue to be polled.

Parquet finalization alone is not delivery: Delta readers see files only after
their commit. A parent is submitted only after its children have committed.
Consequently, the 10-second collection window is **not** a maximum visibility
latency; dependency waves can require several windows. Graceful shutdown forces
all remaining waves without those collection delays and fails on drain errors
or deadline expiry.

An OTLP HTTP success response or the inherited signal-buffering metrics do not
promise durable Delta delivery. File publication completion does; it includes
the durable commit result. Failed or uncertain commits are surfaced and not
retried automatically. Only one collector process may own a table, and all
instances sharing it must agree on commit settings.

```bash
npx --no-install nx run otel-deltalake-exporter:verify
```
