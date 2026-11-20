# Arrowcol SDK generation tool

TypeScript developer tooling around the single Rust generator pipeline. Rust
remains responsible for schema parsing, validation, canonicalization and promotion.

| Surface | Command / result |
| --- | --- |
| Catalogue Explorer | `npx --no-install nx run arrowcol-sdk-gen:catalog-ui`; local UI on port 4173 |
| New contract | `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=local --definition=schemas/events.sql` |
| Existing contract | `npx --no-install nx run arrowcol-sdk-gen:build-contract --registry=hosted --table=telemetry.prod.events --schema-version=1` |
| Optional protoc backend | `npx --no-install nx run arrowcol-protoc:build-contract --registry=local --definition=schemas/events.sql` |
| Tool checks | `npx --no-install nx run arrowcol-sdk-gen-tool:verify` |
| Live browser checks | `npx --no-install nx run arrowcol-sdk-gen:catalog-ui-e2e` against the running UI |
| Restart persistence | Run `npx --no-install tsx tools/libs/arrowcol-sdk-gen/test/catalog-restart.e2e.ts capture`, restart the UI, then rerun with `verify` |

The UI and CLI share the same non-cached Nx workflow. New-table SDK compilation,
tests and deterministic packaging finish before exact-candidate registration.
Each build invokes Cargo directly so an Nx cache hit cannot hide a missing generator.
**Preserve `.temp/contract-builder` across runs; cleaning it removes local tables,
history and artifacts.** See [configuration and recovery](../../../docs/architecture/arrowcol-platform/sdk-generation.md#registration-last-contract-workflow).

```bash
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts generate-fixture
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts generate \
  --catalog-endpoint http://127.0.0.1:8817 --table telemetry.prod.events \
  --ddl schemas/events.sql
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts check
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts compatibility
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts test
npx --no-install tsx tools/libs/arrowcol-sdk-gen/src/cli.ts package-artifact
```

`generate` connects to the catalog and hydrates ordered SQL files.
`generate-fixture` keeps offline v1/v2 fixtures and clients separate;
`compatibility` compares those versions with Rust policy and the pinned Buf CLI.

The `test` workflow copies both versions' generated packages into `target/` before invoking
Cargo, preventing validation-created lockfiles or build state from changing the
generated artifact directory.
