# Arrowcol SDK generation tool

Thin TypeScript workflow wrapper around the single Rust generator pipeline. It
does not parse or transform catalog schemas itself.

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
