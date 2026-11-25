# Arrowcol protoc frontend

| Surface | Purpose |
| --- | --- |
| `arrowcol-protoc` | Export a self-contained, opt-in compiler contract from canonical catalogue artifacts |
| `protoc-gen-arrowcol-sdk` | Read standard protoc requests and reuse the existing Rust/TypeScript SDK renderer |
| `protoc-gen-arrowcol-example` | Example second plugin that generates a wrapper around the SDK's public API |
| Default generator / website | Remain on the existing direct backend; canonical catalogue artifacts are unchanged |

Run inside the devcontainer with the selected catalogue already running:

| Command | Expected result |
| --- | --- |
| `npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=schemas/events.sql` | Real protoc generation, SDK checks and packaging before exact-candidate registration |
| `npx --no-install nx run arrowcol-protoc:build-contract --registry=local --table=telemetry.prod.events --schema-version=1 --proto-entry=telemetry.proto` | Rebuild a committed version using the configured compiler entry filename |
| `npx --no-install nx run arrowcol-protoc:test` | Descriptor roundtrip, rejection cases, output parity and real two-plugin composition |
| `npx --no-install nx run arrowcol-protoc:e2e` | Against the running demo: create a hosted-simulation table, compile/execute a second plugin's wrapper and verify the download |

**Preserve `.temp/contract-builder`: it contains catalogue state and releases.**
The live check keeps its uniquely named table and evidence. Use `--schema-version`
through Nx because Nx consumes its own `--version` flag.

The initial API binding supports existing Arrow Flight `DoPut` and its six
`x-arrowcol-*` routing headers. It does not invent gateway policy, tier handling,
generic header forwarding or a new transport service.

See [compiler contract, plugin composition and limitations](../../docs/architecture/arrowcol-platform/sdk-generation.md#optional-protoc-backend).
