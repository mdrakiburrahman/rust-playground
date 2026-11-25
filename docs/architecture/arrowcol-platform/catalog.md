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

## Optional local Unity Catalog and Hive services

Unity and Hive can be independent registration-last build authorities. The
existing **Hosted simulation** still means a second Rust catalogue. Enable each
native connection below, restart `catalog-ui`, then select **Unity Catalog** or
**Hive Metastore** in any frontend. **Inspect connections** also browses native
metadata, including foreign tables that are deliberately not imported into the
contract workflow. A signed CES bridge adapts the native authority to collectors;
SDKs receive neither registry configuration nor a registry client.

Run these commands in the devcontainer with access to the local Docker socket.
The images are digest-pinned and service state is isolated in labelled Docker
volumes. **Development only: native authentication is disabled. Service ports
bind only to loopback; PostgreSQL has no published port and uses trust
authentication within the dedicated Docker network. Never publish these ports
or connect untrusted containers to that network.**

| Command | Result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=up --provider=unity` | Unity Catalog v0.7.0 REST at `http://localhost:18080/api/2.1/unity-catalog`; persistent H2 metadata |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=up --provider=hive` | Hive 4.1.0 Metastore at `thrift://localhost:19083`, persistent PostgreSQL 16 metadata and a separate warehouse volume |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=enable-build --provider=unity` | Configure independent native authority and CES bridge on `18819`; substitute `hive` for bridge `18820`. **May stop/recreate the owned service container to add its warehouse bind; metadata volumes are retained. Run while builds are idle, then restart `catalog-ui`.** |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=status --provider=unity` | Owned container identity, running state and configured build capability; substitute `hive` |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=unity --definition=schemas/events.sql --codegen=protoc` | Same verification and immutable packaging as Rust registries, followed by native external Delta registration; use `--registry=hive` and a `hive.database.table` name for Hive |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=inspect --provider=unity --catalog=unity --schema=default` | Native table listing; omit namespaces to list catalogues, or add `--table=NAME` for metadata |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=inspect --provider=hive --catalog=hive --schema=default` | Native table listing through HCat/Thrift, without HiveServer2; omit namespaces to list databases |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=verify --provider=unity` | Creates and retains an isolated namespace and external Parquet **metadata-only** example, restarts the service, checks native identity/schema persistence and saves evidence; substitute `hive` |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=stop --provider=hive` | Stops owned Hive/PostgreSQL containers without deleting volumes; substitute `unity` for Unity only |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues-e2e` | Inspect retained native definitions and connection capabilities in all three frontends |
| `npx --no-install nx run arrowcol-sdk-gen:native-contract-e2e` | Create through Lakehouse UI, verify native read-back and receipts, deterministic rebuild, duplicate rejection and absence of dual registration |
| `npx --no-install nx run arrowcol-sdk-gen:native-recovery-e2e` | With native builds idle, stop/restart each owned native service, check fail-closed CES reads during outage and unchanged schemas/receipts afterward; retains volumes and tables |

`verify` leaves `arrowcol_check_*` namespaces with `example_events(id, payload)`
for inspection. These examples contain **no data files, Delta log or generated
SDK** and are never registered in a Rust catalogue. Unity identity and metadata
are compared before/after restart; Hive's retained column types are checked
through its native metadata client. Evidence and lifecycle ownership live in
`.temp/contract-builder/native`. Preserve that directory and the Docker volumes;
the tool refuses foreign resources and offers no destructive reset command.

Unity uses synchronous H2 writes and runs Java directly under Docker's init
process so acknowledged metadata survives immediate graceful restart. Hive's
PostgreSQL JDBC driver is downloaded at a fixed version and SHA-256 checked.
Inspection invokes fixed commands through the local Docker Engine exec API,
waits for complete output and checks actual exit state; premature stream closure
is an explicit unknown-outcome error. The web API exposes only allowlisted
read-only namespace/table operations, not shell execution or native CREATE.
Hive namespace listings and unconfigured connections use HCat textual output,
including startup diagnostics. Enabled Hive table inspection uses structured
Rust Thrift reads, including native identity, storage and NOT NULL constraints.

| Choice | Local deployment | Integration surface | Identity |
| --- | --- | --- | --- |
| OSS Unity Catalog | Separate persistent H2 service and narrowly shared warehouse directory | REST `/api/2.1/unity-catalog`; native column type JSON, order, nullability, properties and external Delta location are compared | Native UUID is recorded after CREATE, separately from the frozen Arrowcol identity |
| Hive Metastore | PostgreSQL, JDBC and persistent warehouse; HiveServer2 is unnecessary | Generated Rust Hive 4 Thrift client, atomic `create_table_with_constraints`, `get_table_req` and `get_not_null_constraints` | Read-only `Table.id` identifies replacements; constraint metadata preserves NOT NULL |

The adapters are independently optional. Enabling one connects the verified
development container to the owned Docker network and mounts only that authority's
warehouse into its native service at the same absolute path. It does not expose
the whole workspace. Preserve `.temp/contract-builder` and all labelled volumes.

The initial build scope is **new unpartitioned immutable v1 tables with flat
primitive columns**, plus exact rebuilds. Nested types, user partitions, ALTER,
foreign-table adoption and ingestion-layout rewriting fail explicitly. Identifiers
must be simple lowercase names; Hive uses its `hive` catalogue. Hive maps UTC
timestamps to `timestamp with local time zone` and timezone-free timestamps to
`timestamp`; its internal ownership/provider/statistics parameter names are
reserved. No `version`, field-number or contract-URI property is required.
Hive is a metadata registry here, not a provisioned Hive SQL/Delta query engine.

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

The selected native catalogue remains the registration authority. Do not
dual-register in a second Rust catalogue. The optional native mode of
`arrowcol-catalog-rust` supplies the Nx preparation/promotion and collector CES
protocol without independently registering a Rust-authority table.
The compiler and generated SDK consume the neutral contract and do not select
or connect to a catalogue. A protocol bridge may expose `SchemaCatalog` to an
existing collector, but must resolve the selected native authority rather than
maintain an independently authoritative mirror. Every CES read validates native
identity and metadata against its receipt; watches revalidate every two seconds
and close with an error on drift/outage. Local epochs enumerate only receipted,
adapter-owned contracts, not an allegedly atomic listing of the entire native
catalogue. Existing collector cache-age limits still apply. Out-of-band updates
are rejected, not silently translated into new schema versions.

Freeze the Arrowcol authority/table ID/version/field IDs and full schema/options
before generation. Keep the upstream table ID as a **separate registration
receipt**, populated after registration, rather than rewriting the already
verified SDK with an ID that did not exist at build time. Keep immutable
schema/descriptor history and native registration receipts with SDK releases,
without requiring `version`, field numbers or a contract pointer in native
table properties. Catalogue properties remain optional documentation/downstream
options, mirroring DDL properties. Under strict nullable-tail-only evolution,
field numbers can be derived from stable sibling ordering, then checked against
the previous published descriptor. See
[catalogue metadata versus SDK versioning](sdk-generation.md#catalogue-metadata-versus-sdk-versioning).

### Registration-last across an external service

| Phase | Implemented behavior |
| --- | --- |
| Prepare | Validate DDL and backend capabilities, read current namespace/location state, freeze the canonical candidate; no external registration or Delta CREATE |
| Generate and stage | Run the unchanged direct/protoc SDK checks and deterministic packaging against that candidate |
| Provision and register | After successful checks, initialize the exact Delta schema at an owned location through the existing Rust path, then register an external table; no mandatory Arrowcol property |
| Read back and publish | Compare types, nullability/constraints, optional properties, partitions and location; durably record native identity and only then publish the release |
| Unknown outcome | Retain candidate/archive and a durable phase journal; use native idempotency/receipts where available, otherwise fail closed for explicit reconciliation |
| Conflict | Reject a different existing table; matching columns alone do not prove this workflow created it, so never silently adopt or overwrite it |

This is a recoverable sequence, **not an atomic transaction spanning the native
catalogue, Delta storage and release directory**. Native registration alone does
not initialize a valid Delta transaction log. HMS may create directories or run
hooks, which is not Delta provisioning. A failure can leave initialized storage
without registration, or a registered table without a published release. Record
these separately and resume the same candidate; do not delete data as rollback.
Restrict writes to adapter-owned tables for the initial supported mode. Detect
out-of-band edits and fail closed; an adapter-local lock cannot fence independent
native-catalogue writers.
Without a mandatory registration marker, a native API lacking idempotency or a
recoverable receipt may not distinguish an uncertain CREATE from somebody else's
identical table. Do not promise automatic recovery in that case; this is a native
adapter capability decision, not a reason to require version metadata on users'
tables.

Receipts and provisioning intents live in the bridge state directory's `native`
subdirectory, protected by an exclusive process lease and bound to the native
endpoint/provider. Native assigned IDs are also returned in table metadata and
saved as `registration-receipt.json` beside the run's staged archive. They do not
change the immutable archive. If a durable receipt exists, explicit resume reads
and verifies the same native identity before publication. A CREATE attempt without
a receipt is **not automatically replayed**, even if matching metadata exists.
Retain it for explicit reconciliation; there is no force-adopt endpoint.

Use `native-contract-e2e` for the real UI/build path,
`native-recovery-e2e` for outage/restart persistence, and
`CONTRACT_COLLECTOR_RUN=RUN_UUID nx run arrowcol-sdk-gen:collector-contract-e2e`
through `npx --no-install` for the packaged SDK-to-Delta proof. Both retain their
tables and evidence. The collector proof requires a fresh, empty successful
protoc run and never executes CREATE. Lifecycle `verify` remains the separate
metadata-only test described above.

### Investigation sources

| Source | Evidence |
| --- | --- |
| [Unity Catalog local server guide](https://docs.unitycatalog.io/how-to/run-local-server) | Local Compose options, persistent PostgreSQL alternative, shared paths and development authorization limits |
| [Unity Catalog v0.7.0 API](https://github.com/unitycatalog/unitycatalog/blob/v0.7.0/api/all.yaml) | Namespace/table endpoints, column/property shapes and managed staging API |
| [Unity Catalog v0.7.0 table repository](https://github.com/unitycatalog/unitycatalog/blob/v0.7.0/server/src/main/java/io/unitycatalog/server/persist/TableRepository.java) | External-table UUID allocation versus managed staging identity |
| [Apache Hive Docker setup](https://cwiki.apache.org/confluence/spaces/Hive/pages/282102281/Setting+Up+Hive+with+Docker) | Standalone metastore, PostgreSQL/JDBC setup and persistent volumes; examples use Hive 4.0.0 |
| [Hive 4.1.0 Thrift interface](https://github.com/apache/hive/blob/rel/release-4.1.0/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift) | Table/column/constraint models, create/read requests and notification APIs |
| [Hive 4.1.0 HMS handler](https://github.com/apache/hive/blob/rel/release-4.1.0/standalone-metastore/metastore-server/src/main/java/org/apache/hadoop/hive/metastore/HMSHandler.java) | `translate_table_dryrun` transforms metadata; it is not exact-candidate promotion |
