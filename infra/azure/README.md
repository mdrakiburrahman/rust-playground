# Azure sandbox

Declarative Bicep infrastructure with TypeScript orchestration. Run from the repository root inside the Linux devcontainer.

| Setup | Value |
| --- | --- |
| Private configuration | Copy `.env.example` to repo-bootstrapper's `sync/rust-playground/.env`; fill in the authorized subscription and required tags. |
| Authentication | `az login --use-device-code`; the identity needs deployment and role-assignment permissions. |
| Runtime | Repository Node.js, `npm ci`, Azure CLI and Linux `flock`. |
| Infrastructure | Resource group, ADX cluster/database, ADLS Gen2 containers, managed identity, RBAC, 26 external Delta tables and QAP. |
| Loadtest default | Local disk and Kusto; Azure is opt-in. |

Use `npx --no-install tsx infra/azure/bootstrap.ts <command>`:

| Command | Result |
| --- | --- |
| `config` | Generate private `.env.azure`; no Azure calls. |
| `preflight` | Check subscription and provisioning identity. |
| `provision --wait-seconds 600` | Create or reconcile infrastructure, Delta roots, external tables and QAP; rerunnable. |
| `status` | Check deployed infrastructure and Kusto capacity. |
| `reset` | Preview data-plane deletion; no mutation. |
| `reset --execute` | Delete owned Kusto databases/tables/policies and stored blobs, versions and snapshots. |
| `reset --resume` | Finish interrupted reset from its durable intent. |
| `recover` / `recover --execute` | Preview / recover abandoned publisher occupancy after stopping writers. |

| Task | Command |
| --- | --- |
| Azure benchmark | `npx nx run loadtest:run -- --mode azure --validate --measure 2m` |
| Archive an unsealed prior attempt | `npx --no-install tsx projects/loadtest/src/main.ts --discard-unsealed` |
| Recreate empty tables after reset | `npx --no-install tsx infra/azure/bootstrap.ts provision --wait-seconds 600` |
| Offline checks | `npx nx run azure-infra:verify` |

**Reset preserves the cluster, storage account, containers and managed identity.** Stop writers first; keep `.env*` private.

| Details | Reference |
| --- | --- |
| Configuration, custody, recovery and failure contracts | [Azure sandbox details](../../docs/operations/AZURE_SANDBOX.md) |
