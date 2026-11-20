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
| `options.proto` and `google/protobuf/descriptor.proto` | Imported custom option vocabulary and complete dependency sources |

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

```bash
cargo run --locked -p arrowcol-sdk-gen -- generate \
  --catalog-endpoint http://127.0.0.1:8817 \
  --ddl schemas/events.sql --table telemetry.prod.events \
  --out .temp/sdk/events --export-artifacts .temp/catalog/events
```

`--ddl` is repeatable and ordered; each regular `.sql` file is one Spark DDL
statement. The command connects to the running catalog, executes Flight SQL,
then validates the catalog's committed history and artifacts. `--version 1`
selects a historical contract without changing it to the current head.
`--artifacts` remains available for offline generation from validated exports.

| Command | Expected result |
| --- | --- |
| `npx --no-install nx run arrowcol-sdk-gen:generate` | Separate v1/v2 artifacts and Rust/TS SDKs |
| `npx --no-install nx run arrowcol-sdk-gen:check` | Deterministic generated output matches |
| `npx --no-install nx run arrowcol-sdk-gen:compatibility` | Catalog nullable-tail checks and real Buf `WIRE_JSON` both pass |
| `npx --no-install nx run arrowcol-sdk-gen:generated-test` | Generated Rust/Prost and TypeScript packages compile and execute |
| `npx --no-install nx run arrowcol-sdk-gen:package-artifact` | Deterministic SDK archive |

Offline fixtures materialize CREATE followed by ALTER into distinct immutable versions;
v1 output is not regenerated as v2. CI compares their separate `descriptor.pb`
descriptor sets (including imported options) using the pinned npm Buf CLI and root `buf.yaml`. Rust compatibility checks
remain stricter than protobuf: existing Arrow fields and partition roles cannot
change, and additions must be nullable trailing columns.

## Evolution and limits

A historical SDK continues to send its exact registered old Arrow layout.
The collector reuses existing arrays and null-fills only new trailing fields.
Protobuf optionality does not weaken that validation.

See the [generator CLI guide](../../../crates/arrowcol-sdk-gen/README.md) for
artifact-based export, packaging and safety details.

## Registration-last contract workflow

`arrowcol-sdk-gen:catalog-ui` is a local developer tool, not a hosted admin
service. It uses an original catalogue tree/detail/run layout; the reference
Databricks workspace required sign-in and was not inspected beyond its login page.
The primary form accepts catalogue/schema/table names, ordered columns, supported
types, nullability and table properties, then displays the generated DDL.
It serializes input only; the Rust parser and central property registry validate it.

| Phase | Durable result / invariant |
| --- | --- |
| Validate | Exactly one managed CREATE; reject ALTER, LOCATION, IF NOT EXISTS and invalid typed properties |
| Prepare | Read-only `PrepareTable` freezes table/field IDs, v1, complete schema, exact properties, comment, partitions and the selected authority's managed location |
| Generate | Direct backend uses the existing Rust renderer; the opt-in protoc backend invokes `protoc-gen-arrowcol-sdk` and reuses that same renderer |
| Verify | Canonical byte-exact check, TypeScript typecheck and generated tests, Rust generated-crate tests |
| Stage | Deterministic archive containing `contract/`, both SDKs in `sdk/`, and `release.json`; not exposed as a completed release |
| Promote | `PromoteTable` validates the entire candidate under the catalogue DDL lock and commits through the existing Delta optimistic transaction/reconciliation path |
| Publish | Read-back equality of the complete committed v1 contract; create-only content-addressed archive and download reference |

Preparation is not a reservation and publishes no table/event. Conflicting
concurrent creation fails promotion; schema fingerprint equality alone is never
enough. The full candidate includes IDs and backend properties even though the
Arrow schema fingerprint deliberately does not hash mutable policy or DDL text.
Exact-candidate promotion is idempotent, including after a lost response/restart.
The service's existing deterministic identity rules are retained, not replaced
with IDs invented by a temporary catalogue. Preparation refuses a managed location
with pre-existing Delta history.

Rebuilding uses an explicit committed table/version and never submits CREATE,
prepares a new candidate or promotes one. It generates and verifies against the
exported historical contract rather than a mutable head. Identical source,
authority and pinned artifacts yield identical archive bytes. A new generator or
changed policy version yields a distinct release digest; a different existing
file is never overwritten.

### Registry configuration and storage

First startup creates `.temp/contract-builder/config.json`.

| Field / path | Meaning |
| --- | --- |
| `registries[].id`, `label` | CLI selector and UI label; defaults `local`, `hosted` |
| `registries[].authority` | Persisted authority identity; distinct for each service, not a restart epoch |
| `registries[].endpoint` | Real Flight SQL/SchemaCatalog endpoint; defaults `http://127.0.0.1:18817` and `http://127.0.0.1:18818` |
| `registries[].mode` | `owned` starts an independent local service; `external` connects only, never provisions/stops it |
| `hostWorkspace` | Actual host checkout root for copyable paths; initialized from `CONTRACT_HOST_WORKSPACE` or the execution cwd |
| `registries/<authority-hash>/` | Independent persistent state, warehouse and service log |
| `runs/<UUID>/` | Request, state, Nx/build logs, candidate, staged archive and payload |
| `releases/<authority-hash>/<table-UUID>/vN/<SHA-256>/contract-sdk.tar.gz` | Immutable completed SDK + schema contract |

Stop the UI before editing configuration. An external endpoint must implement
the candidate RPCs for new tables; no fallback to registration-first is allowed.
Configure its actual stable authority identity, not an arbitrary new identity on
every restart. Endpoint configuration is not cloud deployment or an authentication
integration; credential forwarding and real cloud mutation are outside this MVP.
The workflow refuses a registry change between queueing and execution/promotion.

The UI binds to `127.0.0.1:4173` by default. `CONTRACT_UI_PORT` changes the port;
`CONTRACT_UI_BIND=0.0.0.0` is for an isolated devcontainer with a private forwarded
port only, never a public deployment. The backend checks local Host/Origin,
requires a session token and JSON for mutations, and exposes only structured
workflow requests, not shell commands. It refuses to adopt a process already
listening on an owned registry port. Shutdown drains admitted builds before
terminating only the registry children it started.
Owned loopback demo registries explicitly use `dev_only_allow_unsigned`; the
upstream deployed catalogue defaults to signed identity. This local setting
does not disable authentication on external catalogues.

### Failure and recovery

Runs persist queued/running/succeeded/failed state, phase, registration outcome,
timestamps, logs and release references across page reloads and server restarts.
Pre-promotion validation/generation/test failures leave the authoritative
catalogue unchanged. A promotion transport error is recorded as **unknown**, not
incorrectly declared unregistered. Publication failure after acknowledgement is
**registered but not published**; verified staged bytes remain available.

Inspect the run and catalogue, then explicitly retry only finalization:

```bash
npx --no-install nx run arrowcol-sdk-gen:build-contract --resume-run=RUN_UUID
```

This checks retained archive/candidate digests and promotes the exact candidate,
not replayed DDL, before create-only publication. Interrupted processes are never
automatically replayed. A damaged/active claim or server lock requires operator
inspection, not blind deletion. Existing failed pre-promotion runs can be replaced
with a corrected new request because they have not registered the table.

The release records authority, endpoint, table ID, schema version/fingerprint,
backend policy metadata and generator/workflow/dependency-lock digests.
`backendPolicy` is descriptive: no retention scheduler, collector policy engine,
header forwarding or schema evolution editor is added.

### Portable options and native defaults

The versioned [proto2 options definition](../../../crates/arrowcol-catalog/proto/options.proto)
is the authoritative extensible-property vocabulary. Rust compiles it and reads
the descriptor's optional labels, enum values and native declared defaults.
Neither the browser nor CLI maintains a second list of values/defaults.

| Surface | Convention |
| --- | --- |
| DDL key | `protobuf.` followed by the fully qualified custom option extension name |
| Demonstration | `protobuf.arrowcol.metadata.v1.ingest_tier` |
| Default | The declaration's native `[default = ...]`; materialized before candidate freezing |
| Unknown `protobuf.*` key or invalid enum | Reject; do not silently preserve it as an operational flag |
| Existing `arrowcol.*` / Delta keys | Continue using the central typed property registry |
| Other vendor metadata | Preserve verbatim in portable table metadata; inert unless a consumer understands it |
| Definition identity | SHA-256 of the complete compiled definitions descriptor set, including dependencies |

The optional-property dropdown comes from those descriptors, shows the declared
default and provides enum choices. The preview includes effective defaults;
explicit selections override them. The ingest-tier example is **metadata only**:
it does not implement routing tiers, retention, gRPC parameters or policy enforcement.

Preparation materializes defaults into canonical DDL and exact table metadata.
The form submits its definitions digest; a changed vocabulary is rejected before
preparation. Candidate descriptors, run state and release provenance freeze the
definitions. Finalization refuses different definitions rather than silently
reinterpreting a candidate. Restart the owned services after editing the options
definition; mixed server/generator versions fail artifact equality checks.

Generated `schema.proto` imports `options.proto` and carries selected values as
custom options. All dependency sources and the complete compiled `descriptor.pb`
are shipped in the contract and SDK descriptors. This is portable metadata, not
a JSON-only manifest feature. Inspect it without the JSON manifest:

```bash
cargo run --locked --quiet -p arrowcol-sdk-gen -- catalog metadata \
  --descriptor .temp/exported-contract/descriptor.pb
```

`catalog properties` (or the `contract-properties` Nx target) lists the compiled
vocabulary/defaults. Buf compatibility consumes `descriptor.pb#format=binpb`,
not an isolated source file lacking imports. Tests compile exported sources with
protoc/Buf and compare descriptor bytes, including native defaults and overrides.

## Optional protoc backend

The website and `arrowcol-sdk-gen:build-contract` default to direct generation.
`arrowcol-protoc:build-contract` explicitly selects `codegen=protoc`; it builds
the exporter, plugin and normal catalogue bridge without restarting services.
Both backends share validation, SDK tests, staging, promotion and publication.
Use `--schema-version` for historical builds through Nx, whose own `--version`
flag is not forwarded. Direct invocation of the TypeScript CLI also accepts the
legacy `--version` spelling.

| Compiler input | Contract |
| --- | --- |
| `--proto-entry=telemetry.proto` | Configurable basename, default `schema.proto`; traversal and reserved import names are rejected |
| `payload/compiler/` | Separate import root, bundled with the release; canonical `payload/contract/` is unchanged |
| `options.proto` | Existing native defaults, selected values and portable table properties |
| `arrowcol_codegen.proto` | Versioned compiler envelope and typed Arrowcol API binding |
| `compiler.catalog_manifest_json` | Existing canonical `artifact_manifest_version=1` catalogue IR, embedded as bytes in a protobuf option |
| `compiler.api` | Arrow Flight DoPut operation/path, descriptor path and resolved routing metadata |

The initial adapter deliberately reuses the public versioned JSON catalogue IR
rather than duplicating the full Arrow type model in a second protobuf schema.
That IR is **inside the compiler descriptor**, not an ambient sidecar path.
It preserves nullability, IDs, physical types, decimals, timezones and collection
details that cannot be recovered from protobuf row primitives alone. A consumer
only needs the exported compiler sources/imports and compatible plugin binaries.

The plugin reconstructs the validated `ArtifactBundle` and compares its canonical
row, property and dependency descriptors with the request, ignoring only source
locations and the extra compiler envelope. Changes to rows, option defaults, IR
or unsupported API mappings fail closed. Unknown plugin parameters, duplicate
files and multiple selected table files are rejected in this first version.
Custom-option bytes are retained rather than lost through `prost_types`.
The existing renderer is reused; there is no second SDK implementation.

`CodeGeneratorResponse` returns UTF-8 sources. Nx copies the original binary
`schema.arrow` and `descriptor.pb` into the SDK, then checks the entire SDK against
direct generation byte-for-byte. The release includes the compiler sources and
exporter/plugin/driver hashes plus the protoc version. Toolchain changes during
generation fail before promotion. Old releases are not rewritten; the optional
compiler inputs and provenance intentionally produce a different archive digest.

### Supported APIs and extension ownership

| API | Existing surface |
| --- | --- |
| Construct Arrow batches | Rust `record_batch`; TypeScript `recordBatch` |
| Build routing metadata | Rust `routing_metadata`; TypeScript `routingMetadata` |
| Identify table/version | Rust `flight_descriptor`; TypeScript `createDoPutPlan().descriptorPath` |
| Write telemetry | Rust `do_put`, plus streaming encoder/decorator helpers; TypeScript `createDoPutPlan` supplies a plan to a transport adapter |
| Catalogue control plane | `GetTable`, `GetSnapshot`, events/Watch, `PrepareTable` and `PromoteTable`; not data-plane write methods |

The supported write binding is `/arrow.flight.protocol.FlightService/DoPut`.
Its existing metadata keys are `x-arrowcol-protocol-version`,
`x-arrowcol-implementation-pool`, `x-arrowcol-catalog`, `x-arrowcol-schema`,
`x-arrowcol-table`, and `x-arrowcol-schema-fingerprint`. The implementation pool
currently supports `dataflow`. The plugin rejects alternate bindings rather than
generating an API that the current SDK does not implement. Gateway policy and
tier headers remain proposals; portable ingest-tier metadata is not forwarded.

Run a second trusted plugin against the same descriptors to generate wrappers
around the SDK. It does not need to implement Arrowcol request construction.
For example, after exporting a compiler contract and creating an output directory:

```bash
protoc -I .temp/compiler telemetry.proto \
  --plugin=protoc-gen-arrowcol-sdk=target/debug/protoc-gen-arrowcol-sdk \
  --plugin=protoc-gen-team=target/debug/protoc-gen-arrowcol-example \
  --arrowcol-sdk_out=.temp/extended-sdk \
  --team_out=.temp/extended-sdk
```

Both plugins receive the input schema independently; one plugin's output is not
automatically the next plugin's input. The example creates
`typescript/team.ts`, calling `recordBatch` and `createDoPutPlan`.
It composes with the generated SDK without insertion points. This standalone
command emits text, not a fully packaged canonical SDK: binary-copy and manifest
assembly remain Nx responsibilities. Extra team-generated files also require
their own validation/package step rather than bypassing the core SDK's exact
file-set check. The live `arrowcol-protoc:e2e` target typechecks and executes this
wrapper separately from the immutable base release.

Plugin binaries are executable build tools, not untrusted UI parameters.
This prototype uses fixed repository-owned paths. Configuring arbitrary external
option-definition files or additional production plugins is not yet supported;
definitions remain compiled and digest-pinned as in the direct backend.

## Design: schema authoring to a protoc-built SDK

This design separates authoring, build-time contract validation, registration
and runtime ingestion. The current implementation is a local demonstration,
not a deployed Fabric feature. The original catalogue frontend stays available;
a Fabric-style frontend can submit the same structured requests without owning
schema semantics or executing arbitrary commands.

### End-to-end compilation

```mermaid
flowchart TD
    author["📝 UI / CLI<br/>Columns, nullability, properties<br/>Validate inputs and preview DDL"]
    subgraph schemaRepo["Schema repository / contract build boundary"]
        ddl["📄 Spark SQL DDL<br/>CREATE TABLE + TBLPROPERTIES"]
        nx["⚙️ Nx build-contract<br/>Selected registry + protoc backend"]
        prepare["🔒 Rust validation + PrepareTable<br/>Freeze exact candidate; no registration"]
        contract["📦 Canonical contract<br/>Manifest + Arrow schema + proto descriptors"]
        proto["📜 Compiler entry .proto<br/>Row schema + options.proto + compiler IR/API options"]
        compiler["🛠️ protoc<br/>CodeGeneratorRequest → protoc-gen-arrowcol-sdk"]
        sdk["🧩 Rust + TypeScript SDK<br/>Arrow builders + Flight DoPut helpers"]
        verify["✅ Compile, test and compare<br/>Stage deterministic SDK archive"]
        promote["📚 Promote exact candidate<br/>Commit and read back registered schema"]
        release["🔗 Publish immutable release<br/>Download + host path + SHA-256"]
        ddl --> nx --> prepare --> contract --> proto --> compiler --> sdk --> verify --> promote --> release
    end
    author --> ddl
```

The boundary is the intended schema-repository ownership model. Today the UI
persists DDL as `runs/<run-id>/definition.sql` in the local build store; it does
**not** create a version-controlled schema file or commit to a separate schema
repository. CLI callers can supply an existing `.sql` file. Introducing a
reviewed schema-repository submission flow is a separate integration.

| Step | Input and implementation | Output and invariant |
| --- | --- | --- |
| Author | UI form or CLI definition; compiled property vocabulary supplies native defaults | Escaped DDL preview; ordered columns remain ordered; client validation is not authoritative |
| Admit build | Local backend enqueues a structured request; `contract.ts` executes the Nx workflow | Durable run ID, selected authority and explicit backend; no arbitrary-shell endpoint |
| Validate | `arrowcol-sdk-gen catalog validate` invokes Rust parsing/property validation | Exactly one supported managed CREATE; invalid or stale definitions fail before preparation |
| Freeze | Catalogue `PrepareTable` returns a candidate; no table, event or Delta log is created | Table UUID, v1, stable field/protobuf IDs, Arrow types/nullability, location and complete properties |
| Export canonical artifacts | Existing catalogue artifact code derives manifest, Arrow IPC schema, row proto and descriptors | Lossless canonical model and complete imports; schema fingerprint excludes mutable policy and DDL spelling |
| Export compiler input | `arrowcol-protoc --artifacts … --out … --entry telemetry.proto` | Separate compiler sources containing native property options plus the versioned compiler envelope |
| Invoke compiler | `protoc --arrowcol-sdk_out=…` resolves the explicitly pinned `protoc-gen-arrowcol-sdk` executable | Standard protobuf compiler request on stdin and response on stdout; no catalogue/network lookup inside the plugin |
| Interpret contract | Plugin reads extension-preserving descriptors, restores `ArtifactBundle` and compares row/options/dependencies/API binding | Changed or unsupported inputs fail; no silent inference of Arrow semantics from protobuf primitives |
| Render SDK | Plugin calls shared `render_sdk_files` | Rust/TS sources identical to the direct renderer; binary Arrow/descriptor files are copied separately by Nx |
| Verify and stage | Existing canonical checker, TypeScript checks/tests, generated Rust tests and deterministic packager | Archive digest and pinned toolchain provenance; SDK failure leaves the registry unchanged |
| Register and publish | `PromoteTable` checks the exact candidate, commits and verifies read-back; Nx publishes create-only | Immutable release tied to authority, table ID, schema version/fingerprint and release SHA-256 |

### What the compiler options actually mean

`options.proto` defines **contracts with downstream components**: portable table
properties, native defaults and selected values. The plugin-specific
`arrowcol_codegen.proto` adds an explicit API binding, not executable code.
Its `CompilerContract` contains:

| Field | Role |
| --- | --- |
| `format_version` | Version of the compiler-envelope protocol |
| `catalog_manifest_json` | Existing canonical, lossless versioned IR embedded inside the protobuf descriptor |
| `option_definitions_sha256` | Pins the complete compiled property vocabulary |
| `api.operation` / `api.grpc_path` | Supported operation and `/arrow.flight.protocol.FlightService/DoPut` |
| `api.descriptor_path` | Catalog, schema, table and `v<version>` |
| `api.metadata` | Resolved existing protocol, implementation-pool, namespace and fingerprint headers |

Embedding JSON IR is a deliberate prototype compatibility choice, not a fully
protobuf-native Arrow IR. Row `.proto` primitives alone cannot faithfully encode
Arrow nullability, decimal representation, timezones and stable identity.
The plugin checks the binding against the supported SDK implementation rather
than treating arbitrary strings as instructions to call arbitrary services.
Properties such as ingest tier remain metadata unless an implemented runtime
binding consumes them; gateway-policy/tier enforcement is not implied.

### Extension and runtime boundaries

```mermaid
flowchart TD
    descriptor["📜 Same compiler descriptors<br/>Schema IR + downstream options"]
    core["🛠️ Arrowcol-owned protoc plugin"]
    extension["🧰 Team-owned protoc plugin"]
    sdk["📦 Base Rust / TypeScript SDK"]
    wrapper["🧩 Team wrapper<br/>Calls base SDK public APIs"]
    batch["📊 Exact Arrow RecordBatch"]
    flight["📡 Arrow Flight DoPut<br/>Descriptor + routing metadata"]
    collector["⚙️ Configured collector endpoint<br/>Validate against registered schema"]
    sink["🗄️ Configured sink<br/>Verify committed/readable rows separately"]
    descriptor --> core --> sdk
    descriptor --> extension --> wrapper
    sdk --> wrapper --> batch --> flight --> collector --> sink
```

Both plugins read the same input descriptors independently. Importing a proto
does not run a plugin. A plugin that emits another `.proto` for compilation needs
a second protoc invocation; the tested wrapper example needs only one invocation
and no insertion points. Extension files are validated separately from the
canonical base SDK's exact file-set check.

Rust exposes an actual `do_put` client path plus batch/stream/request helpers.
TypeScript's `createDoPutPlan` supplies a batch, descriptor and headers to a
transport adapter; it is not itself a network sender. Collector validation and
downstream visibility require their own end-to-end proof. A successful build,
registered table or admitted Flight stream alone does not demonstrate that rows
have reached a durable sink.

### Registry adapter boundary

The current local and hosted-simulation choices are two independent Rust
catalogues. A native Unity Catalog REST endpoint or Hive Metastore Thrift
endpoint cannot be substituted for an Arrowcol `SchemaCatalog` address.
An adapter must preserve read-only preparation, immutable contract history and
exact-candidate promotion. External native identity belongs in a post-registration
receipt, not a rewrite of the verified SDK. See the
[registry investigation](catalog.md#optional-unity-catalog-and-hive-registries-investigation)
for persistence, registration recovery and capability limitations.

### Source map and acceptance

| Responsibility | Source |
| --- | --- |
| Form and DDL preview | `tools/libs/arrowcol-sdk-gen/ui/app.ts` |
| HTTP API and Nx dispatch | `tools/libs/arrowcol-sdk-gen/src/catalog-server.ts` |
| Build state machine | `tools/libs/arrowcol-sdk-gen/src/contract.ts` |
| Protoc orchestration | `tools/libs/arrowcol-sdk-gen/src/protoc.ts` |
| Compiler contract and plugin | `crates/arrowcol-protoc/proto/arrowcol_codegen.proto`, `crates/arrowcol-protoc/src/lib.rs` |
| Shared SDK renderer | `crates/arrowcol-sdk-gen/src/generate.rs`, `src/render/` |
| Real plugin-composition proof | `crates/arrowcol-protoc/tests/plugin.rs`, `tools/libs/arrowcol-sdk-gen/test/protoc-backend.e2e.ts` |

Acceptance requires actual UI submission, protoc execution, generated SDK checks,
registration-last failure cases, correct downloadable archive/hash and persistent
history after restart. A runtime extension of that demonstration additionally
requires sending known rows through the selected collector topology and reading
them from the configured sink, with admission distinguished from durable delivery.
