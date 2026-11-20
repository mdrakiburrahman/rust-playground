# Remote storage experiment

This experiment measures Quiver group commits and recovery on shared storage, and measures Azure Blob commit primitives separately. Blob timings do not establish that Quiver has a Blob backend.

| Component | Purpose |
| --- | --- |
| `crate/` | Standalone Rust driver with a pinned Quiver dependency and lockfile |
| `src/` | TypeScript deployment, guest orchestration, and evidence collection |
| `infra/` | Dedicated two-zone Azure lab, private storage endpoints, managed identities, and outbound NAT |
| [Method](../../docs/experiments/t3-storage/README.md) | Metrics, fault protocol, and interpretation |

## Commands

Run checks in the repository devcontainer. Azure orchestration can run on a host with an existing Azure CLI login; the private configuration selects the CLI executable and argument prefix.

| Command | Expected result |
| --- | --- |
| `npx nx run t3-storage:verify` | Azure-free TypeScript checks, Rust formatting, Clippy, and tests |
| `npx nx run t3-storage:build` | Release driver in `target/storage-spike/release/` |
| `npx nx run t3-storage:run -- deploy --config <ignored-config.json>` | Dedicated lab and private inventory |
| `npx nx run t3-storage:run -- bootstrap --config <ignored-config.json>` | Verified driver and guest tools installed |
| `npx nx run t3-storage:run -- stop --config <ignored-config.json>` | Only inventory-owned VMs deallocated; resources retained |

> ⚠️ Bootstrap formats a blank data disk at the lab's provisioned LUN 0. Existing filesystems must already carry the experiment's filesystem label. Deployment and guest operations require a dedicated owner-suffixed resource group with matching ownership tags.

Keep configuration, inventories, credentials, raw output, and investigation reports in ignored paths. The runner rejects evidence output outside the checkout or outside Git ignore rules. It never uses the repository deployment stamp.

The pinned engine's existing Criterion fixture needs a benchmark-only budget correction for Phase A to pass. The [fixture provenance and reproduction constraints](../../docs/experiments/t3-storage/README.md#existing-benchmark-fixture) keep that change separate from the runtime pin.
