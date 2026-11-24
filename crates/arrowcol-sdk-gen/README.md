# arrowcol-sdk-gen

| Purpose | Result |
| --- | --- |
| Hydrate ordered Spark SQL `.sql` files through Flight SQL | Read only catalog-committed immutable schemas |
| Generate Rust/TypeScript SDKs | Exact Arrow builders and Dataflow Flight helpers; Rust prost contract types |
| Validate evolution | Nullable-tail Arrow policy plus real Buf `WIRE_JSON` through Nx |
| Build a new contract before registration | Prepare exact candidate, verify generated output, stage all packages, promote, publish |
| Typed protobuf streams for flat scalar contracts | Independent Rust/C++/C# models and reusable language SDK packages; descriptor-driven shared runtime |
| Neutral contract distribution | Standalone .proto/options/descriptors/Arrow manifest bundle, with no SDK dependency |

| Guide | Contents |
| --- | --- |
| [Localhost demo](../../docs/architecture/arrowcol-platform/localhost-demo.md) | UI, SDK build, editable five-minute emitters, optional node, regional Delta and local Kusto; Linux and Windows CMD commands |
| [SDK developer guide](../../docs/architecture/arrowcol-platform/sdk-developer-guide.md) | Separate SDK/model dependencies, generic Rust/C++/C# APIs, configuration and delivery guarantees |

## Catalogue Explorer and contract builds

Run in the devcontainer; forward port `4173` to your machine for the UI.
The UI starts two independent persistent catalogue services, not mock APIs.

| Command | Result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:catalog-ui` | Original explorer at `http://localhost:4173/`; separate Fabric-style prototype at `/fabric`; local registry `18817`, hosted simulation `18818` |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=local --definition=schemas/events.sql` | Prepare, build/test and package before registering the exact candidate |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=hosted --table=telemetry.prod.events --schema-version=1` | Rebuild a committed version without CREATE or promotion |
| `npx --no-install nx run arrowcol-sdk-gen:build-contract --resume-run=RUN_UUID` | Explicitly finish a failed promotion/publication using retained verified bytes |
| `npx --no-install nx run arrowcol-sdk-gen:catalog-ui-e2e` | Against a running UI: create in both registries, rebuild, reload, download and verify; keeps test tables and evidence |
| `npx --no-install nx run arrowcol-sdk-gen:catalog-ui-e2e:fabric` | Same real workflow through the separate Fabric-style UI with protoc, favorites and detail drawer |
| `CONTRACT_COLLECTOR_RUN=RUN_UUID npx --no-install nx run arrowcol-sdk-gen:collector-contract-e2e` | Writes three synthetic rows using a fresh protoc UI run's SDK, then checks committed Delta rows; requires the owned catalogue/UI running and an empty table |
| `npx --no-install nx run arrowcol-sdk-gen:contract-properties` | List optional properties, enum values and native defaults from compiled proto2 descriptors |
| `npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=schemas/events.sql` | Opt into real protoc generation; the original website/default target remain direct, while `/fabric` defaults to protoc |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=up --provider=unity` | Start persistent local Unity Catalog; substitute `hive` for Hive/PostgreSQL. **Unauthenticated local development only; never publish native ports.** |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=enable-build --provider=unity` | Enable native registration/SDK builds; substitute `hive`. **Run with builds idle: may recreate the owned container to add a shared warehouse, retaining its metadata volumes. Restart `catalog-ui` afterward.** |
| `npx --no-install nx run arrowcol-sdk-gen:native-contract-e2e` | Real Lakehouse CREATE, protoc artifact, native receipt, deterministic rebuild and duplicate rejection for both enabled native authorities |
| `npx --no-install nx run arrowcol-sdk-gen:native-recovery-e2e` | **Run with native builds idle:** stop/restart both native services, check fail-closed reads and retained schemas/receipts; preserves volumes |
| `npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=verify --provider=hive` | Retains a native metadata-only example and checks restart persistence; substitute `unity`. Not a Delta/SDK ingestion test |

CLI builds require the selected registry to be running. Configuration, run logs,
registry state/warehouses and immutable releases live under
`.temp/contract-builder`. **Do not clean this directory if you need its tables,
run history or artifacts.** Copy releases elsewhere before repository cleanup.
Click a workflow run for its live top-down pipeline, step timings, DDL/proto
inspection and logs. The UI offers independent neutral/model/SDK source downloads,
digests and copyable host paths; no public package feed is configured. Set `CONTRACT_HOST_WORKSPACE`
to the host checkout path on first startup when running inside a container.
Browser checks require Playwright Chromium and its system libraries
(`npx --no-install playwright install --with-deps chromium --only-shell`).
After `enable-build`, the main dropdown selects Unity or Hive for the same
create/build workflow. Before enabling, their entries open inspection only.
Native builds support new unpartitioned, flat-primitive v1 contracts and exact
rebuilds; Hive names start with `hive`. **Inspect connections** also browses
foreign native metadata without adopting it. SDKs remain registry-independent;
the collector's deployment selects the signed native CES bridge.
See [native setup, inspection and safety](../../docs/architecture/arrowcol-platform/catalog.md#optional-local-unity-catalog-and-hive-services).

| Frontend | Purpose |
| --- | --- |
| `/` | Original catalogue explorer |
| `/fabric` | Existing OneLake catalogue demo, unchanged navigation |
| `/lakehouse` | Separate Lakehouse Explorer: nested table tree, inline schema/properties/SDK tabs and compact build controls |

Open a Fabric table's **Open in Lakehouse Explorer** link to carry its catalogue and
table selection into the new view. The Lakehouse rail mirrors the reference's
surrounding navigation (including Learn and Real-Time), but only OneLake catalogue
and Explorer actions work; other rail items are non-interactive decoration.
The pane displays real schema metadata, not queried or fabricated data rows.
Run `npx --no-install nx run arrowcol-sdk-gen:catalog-ui-e2e:lakehouse` for the
new frontend's actual create/build/rebuild/download journey.

See [workflow phases, configuration and recovery](../../docs/architecture/arrowcol-platform/sdk-generation.md#registration-last-contract-workflow).
The [schema-to-SDK design](../../docs/architecture/arrowcol-platform/sdk-generation.md#design-schema-authoring-to-a-protoc-built-sdk)
explains each compilation step and the catalogue/collector boundary.

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
