# ArrowCol catalog

| Purpose | Runtime |
| --- | --- |
| Govern Arrow schemas, Delta metadata and SDK descriptors | Rust + Delta Kernel; no SQL catalog, JVM, Spark runtime or Thrift |
| Submit Spark SQL DDL | Flight SQL `CommandStatementUpdate`, port `8817` |
| Fetch schemas/artifacts and resume changes | gRPC `arrowcol.catalog.v1.SchemaCatalog` on the same endpoint |

| Command | Expected result |
| --- | --- |
| `cargo run -p arrowcol-catalog-rust -- --listen 0.0.0.0:8817 --warehouse-root /tmp/warehouse --state-dir /tmp/catalog` | Hydrate committed Delta tables; optionally add `--ddl-dir <seed-directory>` for single-statement `.sql` seeds |
| `cargo run -p arrowcol-catalog-rust -- --healthcheck` | Check the local CES endpoint |
| `npx nx run arrowcol-catalog-rust:verify` | Format, lint, build, tests and Rustdoc |
| `npx nx run arrowcol-catalog-rust:image-smoke` | Build and probe the standalone runtime image |

Point DFE receivers at the catalog, not a SQL schema store:

```yaml
catalog:
  type: remote
  endpoint: http://arrowcol-catalog:8817
  maximum_cache_age: 30s
```

Persist `--state-dir` and the warehouse across container replacement. Existing
Delta metadata wins over older SQL seeds. Explicit `LOCATION 'abfss://...'`
uses Azure CLI credentials and the existing sandbox publisher fencing.
**Infra-only provisioning is not write readiness:** complete the supported
`azure-infra:provision` bootstrap, including Delta initialization and
`complete_bootstrap`, before starting ordinary Azure catalog/DFE writers.
Do not invent or overwrite `ARROWCOL_RESET_GENERATION`.

See [catalog semantics and recovery](../../docs/architecture/arrowcol-platform/catalog.md)
and [container publishing](../../docs/containers/publishing.md).
