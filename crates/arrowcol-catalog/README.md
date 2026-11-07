# arrowcol-catalog

Production catalog and schema-control-plane library for Arrow collectors. It is
transport-embeddable: collector binaries can share the same
`FlightSqlDdlHandler` without this crate owning a server socket.

## Contracts

- Exactly one maintained-parser DDL statement:
  - `CREATE TABLE catalog.schema.table (...) [PARTITIONED BY (...)]`
  - `ALTER TABLE catalog.schema.table ADD COLUMNS (...)`
- Existing fields are immutable. New fields are nullable, tail-appended, and
  receive never-reused positions and protobuf field numbers.
- Drops, renames, type/nullability changes, `FIRST`/`AFTER`, partition changes,
  replacement, CTAS, defaults, constraints, storage clauses, and every other
  statement are rejected with `CatalogErrorCode` plus an
  `arrowcol_protocol::ErrorCode`.
- Incoming Arrow schemas are accepted only when their computed fingerprint and
  claim match one exact registered version. An older registered version receives
  an explicit source-copy/null-fill plan only when the current head is a
  nullable append-only superset.

The exact historical-version rule is what keeps generated clients compatible:
an SDK generated for version `N` can continue sending its exact schema after
nullable tail-appended version `N+1`; the collector adds only all-null arrays
for the new fields. An unregistered prefix, reordered field, changed physical
type, nullability change, or fingerprint mismatch is rejected before collector
admission.

The SQL type surface follows DataFusion 54's Arrow mappings for booleans,
signed/unsigned integers, real/double, strings, `BYTEA`, date, time,
`TIMESTAMP(0|3|6|9)`, explicit `DECIMAL(p,s)`, `ARRAY`, and `STRUCT`. `MAP`
uses the parser's `MAP(key, value)` spelling. Closed Arrow extensions required
by collector schemas include `FIXED_SIZE_BINARY(n)`, `LARGE_STRING`,
`STRING_VIEW`, `LARGE_BINARY`, `BINARY_VIEW`, `DATE64`, and explicit
time/duration unit names. Unsupported or lossy mappings fail closed.

## Runtime and compatibility

| Component | Responsibility |
| --- | --- |
| [arrowcol-catalog-rust](../../projects/arrowcol-catalog-rust/README.md) | Authoritative Delta-backed catalog service and schema-event stream |
| [arrowcol-catalog-client](../arrowcol-catalog-client/src/lib.rs) | Remote catalog access and bounded schema cache |
| This crate | DDL parsing, immutable schema contracts, validation, and compatibility traits |
| `MemoryCatalogStore` / `MemoryMetastore` | In-process compatibility fixtures; not production persistence |

SQL catalog persistence and the Java Hive/Thrift sidecar are retired. SQL Server
remains only as the router's routing store. The schema-change compatibility
control plane still reconciles recoverable reservations, descriptor artifacts,
and activation through its store/metastore traits.

## Validation

| Command | Expected result |
| --- | --- |
| `npx --no-install nx run arrowcol-catalog:verify` | Format, lint, build, tests, and API documentation pass |
