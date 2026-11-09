# quiver-cli

Generate and inspect the DFE `durable_buffer` Quiver layout, including per-core
storage, QPA pluggable-Arrow bundle manifests, WAL entries, segments, and
subscriber acknowledgements. Read-only commands can inspect the loadtest's DFE
Quiver directories; mutating commands require a `quiver-cli.json` marker created
by `generate`.

| Task | Command |
| --- | --- |
| Generate the committed SQL Server fixture using the loadtest schema seed | `npx nx run quiver-cli:run -- generate --catalog-endpoint memory` |
| Generate with the live Schema Catalog | `npx nx run quiver-cli:run -- generate` |
| Inspect files and manifests | `npx nx run quiver-cli:run -- tree` |
| Run the lifecycle walkthrough | `npx nx run quiver-cli:verify` |

The default catalog endpoint is `http://127.0.0.1:8817`. The `memory` endpoint
loads the checked-in loadtest Spark SQL schema seed into the catalog's
`MemoryCatalogStore`; it is intended for local demos and tests. The default CSV
is [`sqlserver_wait_stats.csv`](data/sqlserver_wait_stats.csv), captured from
`sys.dm_os_wait_stats` and typed using the selected catalog schema.

See [Quiver on disk](../../docs/quiver/README.md) for the DFE format and loadtest
storage contract.
