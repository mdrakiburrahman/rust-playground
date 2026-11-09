# arrowcol-sdk-gen

| Purpose | Result |
| --- | --- |
| Hydrate ordered Spark SQL `.sql` files through Flight SQL | Read only catalog-committed immutable schemas |
| Generate Rust/TypeScript SDKs | Exact Arrow builders and Dataflow Flight helpers; Rust prost contract types |
| Validate evolution | Nullable-tail Arrow policy plus real Buf `WIRE_JSON` through Nx |

Protobuf is a proto2 **schema contract**, not the ingestion wire. Scalar fields
are optional; timestamps are UTC microseconds (`int64`), decimals are strings.
SQL/Arrow nullability is still enforced. Arrow Flight remains the wire.

## Generate from the catalog

Requires a running catalog serving Flight SQL and SchemaCatalog RPCs.
**Generation replaces its output directory.** Outputs must be workspace-relative
under gitignored `.temp` or `target`; absolute paths, traversal and symlinks fail.

```bash
cargo run --locked -p arrowcol-sdk-gen -- generate \
  --catalog-endpoint http://127.0.0.1:8817 \
  --ddl schemas/events.sql --table telemetry.prod.events \
  --out .temp/sdk/events --export-artifacts .temp/catalog/events
```

Repeat `--ddl` in dependency order; each regular `.sql` file is one statement.
Use `--version 1` to generate an unchanged historical client after ALTER.
Use `--artifacts <directory>` instead of catalog options for offline generation.

| Input artifact | Validation |
| --- | --- |
| `manifest.json` | Rebuild schema invariants and fingerprint |
| `schema.arrow` | Exact canonical Arrow IPC schema |
| `descriptor.pb` | Exact canonical protobuf descriptor set |
| `schema.proto` | Exact real proto2 source |

## Devcontainer commands

| Nx command | Expected output |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:generate` | Separate `.temp/catalog{,-v2}` and `.temp/generated{,-v2}` |
| `npx --no-install nx run arrowcol-sdk-gen:check` | Both generated versions match byte-for-byte |
| `npx --no-install nx run arrowcol-sdk-gen:compatibility` | Distinct v1/v2 contracts pass catalog and Buf checks |
| `npx --no-install nx run arrowcol-sdk-gen:generated-test` | Both Rust/Prost and TypeScript versions compile and run |
| `npx --no-install nx run arrowcol-sdk-gen:package-artifact` | Separate deterministic v1/v2 archives |
| `npx --no-install nx run arrowcol-sdk-gen:verify` | Complete generator checks |

Generated output is gitignored, never committed. Archive historical clients
outside disposable roots before cleanup. A historical client writes its exact
registered Arrow schema; the collector only null-fills new trailing columns.
The manifest records table/version identity, package roots and all file hashes.

Detailed contracts and limitations:
[SDK generation](../../docs/architecture/arrowcol-platform/sdk-generation.md).
