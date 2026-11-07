# Catalog-driven SDK generation

[Architecture index](README.md)

Spark SQL `.sql` DDL defines the table; the catalog owns immutable schema
versions and stable field identities. Generated SDKs use **Arrow Flight for
transport**. Protobuf is a schema contract, not an ingestion wire format.

| Catalog artifact | Contract |
| --- | --- |
| `manifest.json` | Canonical schema, identities, version, Arrow logical details and fingerprint |
| `schema.arrow` | Exact Arrow IPC schema; no row data |
| `descriptor.pb` | Proto2 `FileDescriptorSet` |
| `schema.proto` | Real source reconstructed deterministically from the persisted descriptor |

All scalar protobuf fields are optional, including SQL `NOT NULL` columns.
SQL/Arrow nullability remains authoritative for ingestion. Timestamp contract
values are `int64` UTC microseconds; decimals are base-10 strings. Collections
use repeated/message fields. Field numbers survive version changes and catalog
restart, are never reused, and skip the protobuf-reserved range 19000-19999.

| Generated surface | Responsibility |
| --- | --- |
| Rust `src/proto.rs` | Prost-generated contract types, alongside Arrow row builders |
| Rust and TypeScript row/builders | Construct the exact registered Arrow layout |
| Flight descriptor, fingerprint and routing helpers | Configure transport and send through Dataflow |
| `sdk-manifest.json` | Hashes of artifacts and generated files; package/version identity |

Generation cross-checks source, descriptor, Arrow schema and normalized manifest.
Unsupported/lossy types, identifier collisions, stale output, symlinks and unsafe
paths fail explicitly. `check` is read-only and requires byte-exact output.
Generated roots must remain under gitignored `.temp` or `target`; archive
historical clients before cleanup.

## Commands

Run inside the repository devcontainer.

| Command | Expected result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:generate` | Separate v1/v2 artifacts and Rust/TS SDKs |
| `npx --no-install nx run arrowcol-sdk-gen:check` | Deterministic generated output matches |
| `npx --no-install nx run arrowcol-sdk-gen:compatibility` | Catalog nullable-tail checks and real Buf `WIRE_JSON` both pass |
| `npx --no-install nx run arrowcol-sdk-gen:generated-test` | Generated Rust/Prost and TypeScript packages compile and execute |
| `npx --no-install nx run arrowcol-sdk-gen:package-artifact` | Deterministic SDK archive |

The fixtures hydrate CREATE followed by ALTER into distinct immutable versions;
v1 output is not regenerated as v2. CI compares their separate `schema.proto`
files using the pinned npm Buf CLI and root `buf.yaml`. Rust compatibility checks
remain stricter than protobuf: existing Arrow fields and partition roles cannot
change, and additions must be nullable trailing columns.

A historical SDK continues to send its exact registered old Arrow layout.
The collector reuses existing arrays and null-fills only new trailing fields.
Protobuf optionality does not weaken that validation.

See the [generator CLI guide](../../../crates/arrowcol-sdk-gen/README.md) for
artifact-based export, packaging and safety details.
