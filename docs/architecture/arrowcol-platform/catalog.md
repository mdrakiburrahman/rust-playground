# Catalog and schema admission

[SDK pipeline diagram](diagrams/sdk-generation.html) | [Architecture index](README.md)

| Surface | Contract |
| --- | --- |
| Flight SQL `DoPut` | Spark SQL `CommandStatementUpdate`; CREATE, nullable ADD, SET/UNSET TBLPROPERTIES, external DROP and CREATE DATABASE |
| Flight SQL queries | SHOW TABLES, SHOW TBLPROPERTIES and DESCRIBE TABLE; prepared query watermarks reject catalog changes before retrieval |
| gRPC metadata | `SchemaCatalog.GetTable` / atomic `GetSnapshot`; committed history, Arrow IPC, manifest and protobuf descriptors |
| Change events | `GetCurrentEventId`, `GetEvents(from)` and `Watch(from)`; CREATE_TABLE, ALTER_TABLE, DROP_TABLE, SET_PROPS |
| Table model | Catalog/database/table, provider, location, comment, partition columns and TBLPROPERTIES |
| Runtime | Standalone Rust catalog and Delta Kernel; no Spark process, JVM or Thrift |

```sql
CREATE TABLE telemetry.prod.events (
    id BIGINT NOT NULL,
    body STRING
) USING DELTA TBLPROPERTIES ('vendor.owner' = 'telemetry');

ALTER TABLE telemetry.prod.events ADD COLUMNS (source STRING);
```

The Rust [Spark SQL parser](../../../crates/arrowcol-sparksql/README.md) rejects
CTAS, arbitrary queries and breaking schema mutations. The catalog validates
registered properties through the shared [property registry](../../reference/TABLE_PROPERTIES.md).
Unrelated vendor properties remain intact in Delta `metadata.configuration`.

## Commit and recovery

| Stage | Safety invariant |
| --- | --- |
| Durable location index | Written before Delta CREATE, so restart can discover an unknown-success commit |
| Delta transaction | Schema, properties, comment and immutable catalog history commit in one optimistic transaction |
| Publication | Only a successfully read-back Delta head enters memory or emits an event |
| Restart | Read `_delta_log` first; older seed SQL cannot overwrite committed schema or properties |
| Concurrent DDL | Serialized catalog operations; a Delta version conflict requires fresh metadata and a rebuilt transaction |
| External DROP | Durable catalog tombstone; never deletes Delta data or its transaction log |

DFE publication groups actual batch values by the committed partition columns,
including typed and null values, and writes each key with a separate replay-safe
transaction identity. A partial commit followed by retry cannot skip remaining
keys or duplicate already committed rows. Partition directory values are escaped;
the Delta log retains their unescaped typed representations.

`catalog.schemaHistory` and `catalog.dropped` are reserved, non-tunable internal
configuration keys. CREATE/SET/UNSET attempts to overwrite them fail. Recovery
checks persisted history against the committed Delta schema, partitions,
properties and comment. Existing tables without catalog history are imported
only when their physical Arrow types map losslessly to Spark.

Delta Kernel 0.29 lacks an ALTER property setter. The catalog's supported
Committer extension augments generated metadata **before** the atomic write,
then validates the final metadata/protocol pair. It preserves unrelated actions
and optimistic concurrency; it never rewrites a committed log.

## Schema admission

| Incoming schema / cache state | Result |
| --- | --- |
| Exact claimed fingerprint and registered layout | Admit after identity/lease checks |
| Historical registered nullable-append prefix | Reuse source arrays and null-fill appended fields |
| Unregistered prefix, reordered/changed field or false claim | Clear Flight rejection before DFE admission |
| Warm coherent cache | No catalog RPC per ingestion stream; validate one immutable history snapshot |
| Epoch mismatch, retained-event gap or stream lag | Discard cached state and take a new atomic snapshot |
| Catalog outage or missed high watermark | Fail closed after bounded freshness; never use an unbounded stale cache |

The resume token is **epoch + event_id**. IDs increase within one process epoch;
restart creates a new epoch. Snapshot high watermark and replay/subscription
are race-safe. Clients periodically reconcile the server watermark and count
actual RPC starts separately from local hits, misses and invalidations.

Existing field identities, positions, protobuf numbers, types, nullability and
partition roles never change. New field numbers skip protobuf's reserved range.
Property-only revisions preserve earlier descriptor versions and do not issue
a Delta transaction when the effective properties are unchanged.

See [service setup](../../../projects/arrowcol-catalog-rust/README.md),
[SDK generation](sdk-generation.md#evolution-and-limits) and
[admission validation](../../../crates/arrowcol-catalog/src/validation.rs).
