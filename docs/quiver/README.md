# Quiver on disk

The DFE collector can persist Arrow batches through the native
[`durable_buffer` processor](../../submodules/otel-arrow/rust/otap-dataflow/crates/core-nodes/src/processors/durable_buffer_processor/mod.rs)
and [Quiver](../../submodules/otel-arrow/rust/otap-dataflow/crates/quiver/README.md)
before forwarding them to the Delta exporter.

[Delivery flow](../architecture/arrowcol-platform/diagrams/delivery.drawio)

| Surface | Current contract |
| --- | --- |
| Default Flight completion | Best-effort admission; not durable storage or Delta publication |
| `--quiver` Flight completion | Durable-buffer persistence, with WAL fsync before acknowledgement |
| Delta publication | Requires independent observation of a committed Delta snapshot |
| Client delivery status | No durable delivery receipt IDs, receipt polling, or client receipt/purge API |
| Internal subscriber acknowledgement | Resolves a persisted bundle after downstream success; not a client delivery receipt |
| Recovery evidence | Persisted backlog, restart/replay observations, and independent Delta validation |
| Operational custody receipts | Reset, stop, publisher-release, provider custody, and validation evidence remain separate and required |

Durable admission does not prove later Delta publication. Permanent downstream
rejection, retention eviction, unavailable storage, and uncertain publication
outcomes must remain visible; an empty buffer alone is not publication proof.
Retries and replay do not deduplicate independent client submissions.

## Loadtest directories and commands

Use the [loadtest CLI guide](../../projects/loadtest/README.md#disk-persistence-with-quiver)
for current prerequisites, lifecycle, flags, and evidence locations.

```bash
npx nx run loadtest:run -- --quiver --measure 2m --crash-after 30s --validate
```

This command intentionally crashes and restarts the loadtest DFE collector.
It requires replay from preserved disk state and separate committed-Delta
validation; it is not a production reset or an unrestricted purge command.

| Path under the run's `dataflow-state-1/quiver/core_N/` | Purpose |
| --- | --- |
| `wal/quiver.wal`, rotated WAL files | Append-only persisted bundles |
| `wal/quiver.wal.cursor` | Position already represented in finalized segments |
| `segments/*.qseg` | Finalized Arrow data and bundle metadata available downstream |
| `quiver.sub.durable-buffer` | Internal subscriber progress |

Each DFE core owns an isolated Quiver directory. Paths and byte counts depend
on the run; historical Direct demo transcripts and screenshots are not evidence
for current DFE images. Use [`quiver-cli`](../../bin/quiver-cli/README.md) to
generate a catalog-typed local fixture or inspect DFE Quiver directories. Its
read-only commands can open loadtest storage; its writer commands only operate
on directories marked by `generate`.

## Local CLI inspection

[`quiver-cli`](../../bin/quiver-cli/README.md) can generate a catalog-typed
fixture and inspect existing loadtest DFE storage without changing it:

```bash
npx nx run quiver-cli:run -- generate --catalog-endpoint memory onelake/quiver-demo
npx nx run quiver-cli:run -- tree onelake/quiver-demo
npx nx run quiver-cli:run -- segments onelake/quiver-demo
npx nx run quiver-cli:run -- wal onelake/quiver-demo
```

Generation uses the current loadtest schema seed through the catalog's in-memory
store. Omit `--catalog-endpoint memory` to query the live Schema Catalog. The
read-only commands also accept a loadtest Quiver directory. Only use `consume`,
`crash`, or `recover` on a directory created by this CLI; its marker guard
rejects collector-owned storage.

## WAL durability and recovery

The loadtest's Quiver mode uses `wal_flush_interval: 0s`: each bundle is fsynced
before its upstream acknowledgement. The native processor's default interval
is 25 ms, which can leave acknowledged WAL appends vulnerable to a crash before
fsync. Do not generalize the loadtest's zero-interval guarantee to other configs.

On reopening preserved storage, Quiver recovers WAL entries not yet represented
in finalized segments. DFE replays retained bundles downstream and maintains
internal subscriber progress. A restart must preserve the state volume, original
publication identity, ingestion timestamp, and cloud reset generation; replay
must not bypass current routing/publication fences.

Downstream success resolves a bundle; permanent rejection can also resolve it
without publication. Transient failures retry with bounded backoff, subject to
configured retention/eviction policy. Fully resolved segments can be reclaimed,
and WAL reclamation follows its cursor/rotation rules. Therefore empty
`segments/`, cursor advancement, or subscriber progress alone does not prove
that every admitted row reached Delta.

## Qualification and limits

| Evidence | What it establishes |
| --- | --- |
| Durable admission | The configured buffer persistence boundary was crossed |
| Crash/restart and replay observations | Recovery behavior for that run's preserved disk state |
| `results/quiver-recovery.json` | Recorded outage, persisted backlog, and replay observations |
| Independent committed-Delta validation | Published rows, multiplicities, schema, and partition correctness |

The local WAL is not replicated storage: deleting the state volume or losing
the disk removes its recovery source. Permanent rejection and retention eviction
are data-loss conditions, not successful publication. Verify runtime metrics,
configuration, and independent Delta evidence rather than treating an
acknowledgement or historical file listing as an end-to-end guarantee.
