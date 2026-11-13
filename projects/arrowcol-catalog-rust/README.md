# ArrowCol catalog

| Purpose | Runtime |
| --- | --- |
| Govern Arrow schemas, Delta metadata and SDK descriptors | Rust + Delta Kernel; no SQL catalog, JVM, Spark runtime or Thrift |
| Submit Spark SQL DDL | Flight SQL `CommandStatementUpdate`, port `8817` |
| Fetch schemas/artifacts and resume changes | gRPC `arrowcol.catalog.v1.SchemaCatalog` on the same endpoint |

| Command | Expected result |
| --- | --- |
| `cargo run -p arrowcol-catalog-rust -- --listen 0.0.0.0:8817 --warehouse-root out/warehouse --state-dir out/catalog --ddl-identity-secret-file out/internal-hmac` | Hydrate committed Delta tables; the mounted internal HMAC key is required for catalog DDL; optionally add `--ddl-dir <seed-directory>` for single-statement `.sql` seeds |
| `--ddl-auth-mode dev_only_allow_unsigned` | Explicit local-development escape hatch for unsigned DDL; never use in a deployed runtime |
| `--ingestion-layout` | DFE warehouse mode: keep logical ingress schemas separate from normalized physical columns and the `__arrowcol_ingestion` partition; adopt only schema-compatible bootstrapped tables. |
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
uses workload identity in AKS (Azure CLI fallback only for operator/local runs)
and the existing sandbox publisher fencing. Catalog and DFE require data-container
Blob Data Contributor, plus checkpoint-container contributor for custody/fencing.
**Infra-only provisioning is not write readiness:** complete the supported
`azure-infra:provision` bootstrap, including Delta initialization and
`complete_bootstrap`, before starting ordinary Azure catalog/DFE writers.
Do not invent or overwrite `ARROWCOL_RESET_GENERATION`.

DFE deployments must enable `--ingestion-layout` and use the same warehouse root
as `ARROWCOL_DELTA_ROOT`. A no-history bootstrap table is not exposed as an ingress
schema: a matching logical CREATE first commits catalog history without rewriting
its physical schema. Incompatible layouts fail closed; these catalog semantics
and data-plane Flight SQL behavior are unchanged. See
[OAT setup](../../docs/experiments/arrowcol-oat/RUNTIME.md).

Flight SQL DDL verifies the router/loadgen signed internal-identity metadata with
the shared `internal-hmac` key. The default `signed_identity` mode fails startup
when `--ddl-identity-secret-file` (or `ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE`)
is not configured. This authenticates the DDL assertion, not the transport:
catalog RPC remains cleartext without TLS/mTLS. Keep it on the existing isolated
network path; TLS/mTLS is deferred and remains a follow-up before expanding
network trust.

On SIGTERM or Ctrl-C, the service ends CES watches, drains accepted Flight SQL
requests, then closes its process-shared publisher sessions through the guarded
shutdown protocol. A failed/uncertain drain or close is an error, not a clean
shutdown; retained publisher membership must not be adopted or removed manually.

See [catalog semantics and recovery](../../docs/architecture/arrowcol-platform/catalog.md)
and [container publishing](../../docs/containers/publishing.md).
