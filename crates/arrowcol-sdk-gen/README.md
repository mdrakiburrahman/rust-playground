# arrowcol-sdk-gen

| Purpose | Result |
| --- | --- |
| Hydrate ordered Spark SQL `.sql` files through Flight SQL | Read only catalog-committed immutable schemas |
| Generate Rust/TypeScript SDKs | Exact Arrow builders and Dataflow Flight helpers; Rust prost contract types |
| Validate evolution | Nullable-tail Arrow policy plus real Buf `WIRE_JSON` through Nx |
| Build a new contract before registration | Prepare exact candidate, verify both SDKs, stage, promote, publish |

## Catalogue Explorer and contract builds

Run in the devcontainer; forward port `4173` to your machine for the UI.
The UI starts two independent persistent catalogue services, not mock APIs.

| Command | Result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:catalog-ui` | Explorer at `http://localhost:4173`; local registry `18817`, hosted simulation `18818` |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=local --definition=schemas/events.sql` | Prepare, build/test and package before registering the exact candidate |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=hosted --table=telemetry.prod.events --schema-version=1` | Rebuild a committed version without CREATE or promotion |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --resume-run=RUN_UUID` | Explicitly finish a failed promotion/publication using retained verified bytes |
| `npx --no-install nx run arrowcol-sdk-gen:catalog-ui-e2e` | Against a running UI: create in both registries, rebuild, reload, download and verify; keeps test tables and evidence |
| `npx --no-install nx run arrowcol-sdk-gen:contract-properties` | List optional properties, enum values and native defaults from compiled proto2 descriptors |
| `npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=schemas/events.sql` | Opt into real protoc generation; the website and default target remain direct |

CLI builds require the selected registry to be running. Configuration, run logs,
registry state/warehouses and immutable releases live under
`.temp/contract-builder`. **Do not clean this directory if you need its tables,
run history or artifacts.** Copy releases elsewhere before repository cleanup.
The UI offers downloads and copyable host paths. Set `CONTRACT_HOST_WORKSPACE`
to the host checkout path on first startup when running inside a container.
Browser checks require Playwright Chromium and its system libraries
(`npx --no-install playwright install --with-deps chromium --only-shell`).

See [workflow phases, configuration and recovery](../../docs/architecture/arrowcol-platform/sdk-generation.md#registration-last-contract-workflow).

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
| `options.proto` + `google/protobuf/descriptor.proto` | Portable custom options and their definitions; properties are recoverable from the descriptor alone |

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
