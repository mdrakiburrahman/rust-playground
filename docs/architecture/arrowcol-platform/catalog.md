# Catalog and schema admission

[SDK pipeline diagram](diagrams/sdk-generation.html) | [Architecture index](README.md)

| Surface | Contract |
| --- | --- |
| Flight SQL `DoPut` | Spark SQL `CommandStatementUpdate`; CREATE, nullable ADD, SET/UNSET TBLPROPERTIES, external DROP and CREATE DATABASE; standard update result plus committed schema identity/version/fingerprint in `x-arrowcol-ddl-completion-bin` response headers for table-schema completions |
| Flight SQL queries | SHOW TABLES, SHOW TBLPROPERTIES and DESCRIBE TABLE; prepared query watermarks reject catalog changes before retrieval |
| gRPC metadata | `SchemaCatalog.GetTable` / atomic `GetSnapshot`; committed history, Arrow IPC, manifest and protobuf descriptors |
| Contract preparation/promotion | Read-only `PrepareTable` freezes an exact managed v1 candidate; `PromoteTable` checks all identity/schema/policy/artifact bytes and uses the existing Delta commit path |
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
including typed and null values. An explicitly unpartitioned catalog table remains
unpartitioned even when the process sets a legacy ingestion-partition default;
defaults apply only to new uncataloged tables. Each key uses a separate replay-safe
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

The [registration-last SDK workflow](sdk-generation.md#registration-last-contract-workflow)
does not publish prepared candidates into the catalogue. Promotion compares the
complete schema and metadata, not just its schema fingerprint; exact retries are
idempotent and conflicting candidates fail. Preparation is optimistic, not a
reservation. A build or SDK verification failure before promotion creates no table.

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

## Optional Unity Catalog and Hive registries: investigation

**Not implemented or started.** The demo's hosted simulation is still a second
`arrowcol-catalog-rust` service, not Unity Catalog or Hive. Both alternatives can
run locally, but neither endpoint implements Arrowcol's `SchemaCatalog` protocol.
Changing only the registry URL would not work.

| Choice | Local deployment | Integration surface | Main gap |
| --- | --- | --- | --- |
| OSS Unity Catalog | Separate server with persistent metadata database and shared warehouse; isolate from the existing demo | REST `/api/2.1/unity-catalog`, three-part namespaces, columns, properties and external Delta table locations | Native table ID is assigned during external registration; ordinary table metadata is not the complete immutable Arrowcol contract/history |
| Hive Metastore | Standalone `apache/hive` metastore with PostgreSQL, JDBC driver and persistent warehouse; HiveServer2 is unnecessary for registry access | Thrift, normally port 9083; `Table.sd.cols`, `partitionKeys`, `parameters`, location and catalog/database/table names | Thrift adapter, lossless type/constraint conversion and independent contract history are required |

Prefer Unity Catalog as the first adapter: its REST API and three-level namespace
are the smaller integration step. Hive remains a separate optional backend, not
a prerequisite for Unity Catalog. Use explicit opt-in Nx lifecycle targets,
pinned service images and isolated volumes. Keep development endpoints private;
the Unity Catalog single-user examples disable authorization. Do not copy their
ephemeral metadata setup into this persistent demo or reset the existing
`.temp/contract-builder` state.

### Identity, schema and preparation

The inspected Unity Catalog v0.7.0 API exposes `TableInfo.table_id` but does not
accept it in `CreateTable`; the external-table implementation allocates a UUID.
Its `ColumnInfo` exposes type, position and nullability, not Arrowcol field IDs
or protobuf numbers. Hive 4.1.0 exposes a read-only optional `Table.id`, while
`FieldSchema` contains name, type and comment. Hive nullability requires separate
constraint metadata; neither its column list nor column order is a lossless
Arrow schema. Fail unsupported conversions rather than infer IDs or weaken types.

Two superficially similar upstream features do not replace our contract:

| Upstream feature | Why it is not Arrowcol preparation/promotion |
| --- | --- |
| Unity Catalog managed staging tables | Allocate upstream state/storage before promotion; they are not read-only preparation and do not freeze or validate an Arrowcol SDK candidate |
| Hive `translate_table_dryrun` | Transforms a create request for client capabilities; it does not reserve a table or provide an exact-candidate commit token |
| Hive notification events or Delta log versions | Useful reconciliation inputs, but not interchangeable with immutable Arrowcol schema versions, field IDs, descriptors or atomic snapshot watermarks |

Use an Arrowcol-compatible Rust adapter with a durable canonical contract store.
The existing UI and workflow would select its endpoint; TypeScript/Nx would
orchestrate local services rather than reimplement catalogue semantics.
The adapter needs the existing preparation, promotion, table and snapshot
interfaces. Collector event/watch support also needs coherent adapter epochs
and snapshots; a native paginated listing is not an atomic snapshot.

Freeze the Arrowcol authority/table ID/version/field IDs and full schema/options
before generation. Keep the upstream table ID as a **separate registration
receipt**, populated after registration, rather than rewriting the already
verified SDK with an ID that did not exist at build time. Store immutable
canonical artifacts/history outside the native property's size limits, and put
a reserved contract pointer and candidate digest in UC properties/Hive
parameters. Reserve and validate that adapter metadata explicitly; do not make
arbitrary user properties trusted contract identities.

### Registration-last across an external service

| Phase | Proposed behavior |
| --- | --- |
| Prepare | Validate DDL and backend capabilities, read current namespace/location state, freeze the canonical candidate; no external registration or Delta CREATE |
| Generate and stage | Run the unchanged direct/protoc SDK checks and deterministic packaging against that candidate |
| Provision and register | After successful checks, initialize the exact Delta schema at an owned location through the existing Rust path, then register an external table and its contract marker |
| Read back and publish | Compare types, nullability/constraints, properties, partitions, location and marker; durably record native identity and only then publish the release |
| Unknown outcome | Retain candidate/archive and a durable phase journal; query by name and marker before retrying |
| Conflict | Reject a different existing table, including an unmarked table with an identical column list; never silently adopt or overwrite it |

This is a recoverable sequence, **not an atomic transaction spanning the native
catalogue, Delta storage and release directory**. Native registration alone does
not initialize a valid Delta transaction log. HMS may create directories or run
hooks, which is not Delta provisioning. A failure can leave initialized storage
without registration, or a registered table without a published release. Record
these separately and resume the same candidate; do not delete data as rollback.
Restrict writes to adapter-owned tables for the initial supported mode. Detect
out-of-band edits and fail closed; an adapter-local lock cannot fence independent
native-catalogue writers.

Before calling either adapter supported, prove new CREATE and historical rebuild,
source-to-native-to-source type/option fidelity, restart persistence, duplicate
and concurrent registration, a timeout immediately after successful native
registration, external drift, and SDK failure without registration. Existing
foreign tables require explicit import/adoption semantics, not an automatic
synthetic history.

### Investigation sources

| Source | Evidence |
| --- | --- |
| [Unity Catalog local server guide](https://docs.unitycatalog.io/how-to/run-local-server) | Local Compose options, persistent PostgreSQL alternative, shared paths and development authorization limits |
| [Unity Catalog v0.7.0 API](https://github.com/unitycatalog/unitycatalog/blob/v0.7.0/api/all.yaml) | Namespace/table endpoints, column/property shapes and managed staging API |
| [Unity Catalog v0.7.0 table repository](https://github.com/unitycatalog/unitycatalog/blob/v0.7.0/server/src/main/java/io/unitycatalog/server/persist/TableRepository.java) | External-table UUID allocation versus managed staging identity |
| [Apache Hive Docker setup](https://cwiki.apache.org/confluence/spaces/Hive/pages/282102281/Setting+Up+Hive+with+Docker) | Standalone metastore, PostgreSQL/JDBC setup and persistent volumes; examples use Hive 4.0.0 |
| [Hive 4.1.0 Thrift interface](https://github.com/apache/hive/blob/rel/release-4.1.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift) | Table/column/constraint models, create/read requests and notification APIs |
| [Hive 4.1.0 HMS handler](https://github.com/apache/hive/blob/rel/release-4.1.0/standalone-metastore/metastore-server/src/main/java/org/apache/hadoop/hive/metastore/HMSHandler.java) | `translate_table_dryrun` transforms metadata; it is not exact-candidate promotion |
