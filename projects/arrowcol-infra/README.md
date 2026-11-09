# Arrowcol local routing SQL Server

| Component | Purpose |
| --- | --- |
| SQL Server 2022 | Router registrations, pressure snapshots, and routing/fencing leases in the arrowcol database |
| Remote Schema Catalog | Authoritative schemas and schema events; runs separately as arrowcol-catalog-rust |
| Retired | SQL catalog persistence, Java Hive/Thrift sidecar, and client delivery-receipt storage/APIs |

Services use Compose DNS (sqlserver), project-scoped SQL volumes, and loopback-only host ports. SQL Server is retained only for router routing/fencing; it is not the Schema Catalog or a delivery ledger. Clients receive no durable receipt IDs and have no receipt polling or purge API. DFE completion is best-effort admission unless durable_buffer is enabled; durable admission confirms buffer persistence, not Delta publication.

## Bootstrap and run

| Prerequisite | Requirement |
| --- | --- |
| Tools | Docker Engine with Compose v2, OpenSSL, Bash |
| Platform | SQL Server's Linux image is amd64; arm64 requires emulation |
| Memory | Reserve memory for SQL Server and the separate catalog/collector services |

```bash
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:config-check
npx --no-install nx run arrowcol-infra:up
npx --no-install nx run arrowcol-infra:integration-verify
```

Bootstrap copies .env.example to ignored .env and creates three independent mode-0600 secret files under ignored secrets/. Existing values are preserved and never printed. **Rotate credentials with scripts/bootstrap-secrets.sh --force only after arrowcol-infra:reset.** Rotation is refused while the persistent SQL volume exists. Never reuse these development credentials elsewhere; .dockerignore excludes configuration and secrets from image contexts.

| Database | Principal | Local role |
| --- | --- | --- |
| arrowcol | arrowcol_app | db_datareader, db_datawriter for routing state |
| arrowcol | arrowcol_migrator | db_owner for migrations |
| master | sa | Local bootstrap/administration |

The wrapper reads the Docker secret immediately before launching SQL Server. File permissions are part of the local secret boundary. Production requires platform secret management, trusted TLS certificates, and separately scoped runtime/migration principals.

## Operations

| Command | Effect |
| --- | --- |
| npx --no-install nx run arrowcol-infra:logs | Follow project logs |
| npx --no-install nx run arrowcol-infra:down | Remove project containers/network; preserve SQL volume |
| npx --no-install nx run arrowcol-infra:reset | **Delete this project's SQL volume and routing state** |
| npx --no-install nx run arrowcol-infra:clean | **Delete project SQL volume and locally built images**; preserve secret files |

No command performs a global Docker prune. SQLSERVER_PORT defaults to 14333 and is configurable in .env. SQL readiness executes SELECT 1 through sqlcmd; Compose waits for SQL health and initialization jobs. Initialization fails on the first error; shutdown is bounded by STACK_STOP_SECONDS (default 30s).

## Migrations and verification

| Contract | Behavior |
| --- | --- |
| Migration application | Sorted V*.sql under an exclusive SQL application lock; transactional checksum ledger |
| Reapplication | Identical recorded checksum skips; changed applied file fails |
| Current state | Routing registrations, pressure history, and routing/fencing leases only; retirement migration removes SQL catalog and delivery tables |
| Payloads | No Arrow payload bytes in SQL |
| Integration verification | SQL health, routing tables/indexes/constraints, migration checksums, retired-table absence, then idempotent bootstrap/migration rerun |

Historical migration files remain immutable; they do not describe the current supported topology. Verification stops the stack on exit without deleting its durable volume.

## Reproducibility

SQL Server pins the amd64 manifest:
`mcr.microsoft.com/mssql/server:2022-CU27-ubuntu-22.04@sha256:4402d880dd4c34bfa7d8705e56a86cd6c88da80a1f6bbbe741f999e76264a090`.
Registry retention and host architecture/emulation remain external constraints.

See [DFE collector setup](../arrowcol-dataflow-rust/README.md) and [Azure custody/reset details](../../docs/operations/AZURE_SANDBOX.md).
