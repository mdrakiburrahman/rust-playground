# AKS QAP operations

The AKS mode runs the existing Azure QAP experiment runtime as a Kubernetes Job.
It does not deploy the unrelated SQL Server/Hive Compose stack and does not
replace the devcontainer mode.

## Router authentication

Edge HMAC works end-to-end in the proven local flow; using it on AKS requires matching mounted keys, updated pinned router and client images and a trusted TLS and authorization edge, not a different router mode. Catalog identity and TLS, and any QAP host allowlist, are open policy decisions; arbitrary credential-free HTTP(S) hosts remain allowed by existing QAP validation.

## Qualification and safety

| Requirement | Qualified value or guard |
| --- | --- |
| Azure target | The private stamp subscription, `arrow-collector-rg`, East US 2 or Canada Central; never the separate `rust-playground` resource group. Each region still requires live SKU, version, quota and network preflight. |
| Stamp | Integer `1..999999` or the quoted five-digit string `'00000'` in ignored `infra/azure/stamp.yaml`; `arrow-collector-<stamp>` must fit Kusto's provider-enforced 22-character name limit. |
| AKS baseline | 1.35.6; `Standard_D8ds_v6` system pool with 3 initial/3 minimum/3 maximum nodes, 250 pods per node, zones 1/2/3, Azure Linux/FIPS, Entra/Azure RBAC, OIDC/workload identity, Azure CNI overlay dual stack and API-server VNet integration. |
| Declared identity | One `arrow-collector` UAMI for control plane, kubelet, QAP ServiceAccount federation, ADX/storage and ACR. Identity-creating optional add-ons are omitted; tenant-enforced add-ons require separate live qualification below. |
| Publisher ownership | Generated `AZURE_STAMP` selects the same `stamp-<stamp>` authority in TypeScript and Rust, preserving `00000` when selected. Invalid stamps fail closed; an absent stamp retains the Rust legacy hashed-authority path only for older deployments. |
| Networking | Explicit IPv4/IPv6 VNet, node, API, pod, service and DNS ranges; preflight checks subscription VNets and connected peering ranges before deployment. |
| Legacy gate | Preflight blocks while old `project=arrow-collector` resources or additional UAMIs remain. Preserve Terraform state, Key Vault dependencies, role assignments, registry publishing consumers and every FIC subject before retiring the old stamp. |

No destructive replacement is authorized outside `arrow-collector-rg`. The
separate `rust-playground` resource group must remain untouched. Retiring old
resources requires either a completed dependency inventory or explicit
authorization to discard the exact sandbox and its state. Purge protection
cannot be bypassed; use fresh stamped names after an authorized reset.

### Tenant-enforced identities

The live target's mandatory Azure Policy and AzureSecurityLinuxAgent deployments
create identities in the AKS-managed resource group even though the template
omits identity-creating add-ons. This does **not** satisfy a literal one-UAMI
requirement. Inventory both the main group and the cluster's returned
`nodeResourceGroup`; successful template deployment is not proof of that count.

[AKS managed-identity documentation](https://learn.microsoft.com/en-us/azure/aks/managed-identity-overview)
lists bring-your-own identity for the Azure Policy add-on as unsupported. The
generic extension schema's `aksAssignedIdentity` input is not proof that a
particular security extension supports rebinding, and the provider rejects
updates while an extension is `Creating`. Preserve required agents and their
in-use identities. Do not disable add-ons, add policy exemptions or skip tags,
or delete their identities to claim compliance. A supported provider binding
or an explicitly approved platform-identity exception is required before
declaring the one-UAMI requirement complete.

Prepare the private stamp from `infra/azure/stamp.example.yaml`. Keep the
subscription UUID, real non-overlapping network ranges and any configured external
publisher FICs private. Optional `publisherFederatedCredentials` may be empty
(`[]`); the stamp's native AKS ServiceAccount federation remains required.
Preserve any existing in-use trusts; an empty optional list is not a request to
restore or add external trusts. Align the operator environment's subscription,
resource group and location with that stamp.

For a different region, use a fresh stamp and separate private stamp, generated
environment and table-manifest paths. Keep existing stamps untouched. Bootstrap
preserves the shared resource group's and UAMI's original locations and tags,
adds only the new stamp's service-account federation, and uses stamp-specific
deployment names. A region change does not relocate existing resources. The
schema-evolution mode's region boundary is unchanged.

```bash
npx --no-install tsx infra/azure/bootstrap.ts config --stamp-config infra/azure/stamp.yaml
npx --no-install tsx infra/azure/bootstrap.ts preflight --stamp-config infra/azure/stamp.yaml
npx --no-install tsx infra/azure/bootstrap.ts provision --stamp-config infra/azure/stamp.yaml
```

`provision` applies subnet Network Contributor, AKS control-plane/kubelet
identity use, AKS RBAC Cluster Admin for the operator, ACR push/pull, and the
QAP service-account federated credential automatically. The QAP pod receives
only generated resource settings and the reset-generation secret; it does not
mount the operator's Azure CLI cache or credentials.

In stamped mode, the shared runtime UAMI also has container-scoped Blob Data
Contributor on the stamp's data container for catalog and DFE writes, plus the
experiment, checkpoint and website containers. These runtime write grants are
not enabled in schema-evolution mode.
The stamped `$web` container carries the same ownership metadata as the private
containers and participates in the existing guarded inventory and reset path.
Schema-evolution mode does not enable static website hosting.

Stamped OAT aggregates have a separate private `experiments` container and a
container-scoped runtime writer grant. This is not the `experimentqap` fixture
container. Both it and `$web` carry stamp ownership metadata and participate in
the guarded inventory; isolated schema-evolution mode creates neither.

The private stamp can opt into `aks.networkDataplane: cilium` for enforced
NetworkPolicy; omission preserves the existing Azure data plane without policy
enforcement. Isolated OAT workloads require Cilium before deployment.
[Migrating an existing cluster](https://learn.microsoft.com/en-us/azure/aks/update-azure-cni#update-the-data-plane-to-azure-cni-powered-by-cilium)
reimages node pools and can interrupt networking. Qualify an empty disposable
stamp first, preserve its pool counts and CIDRs, and wait for every node and
Cilium agent to become Ready before testing allowed and denied pod traffic.

The read-only runtime mounts writable Parquet staging at `/app/.temp/qap-stage`,
inside the workspace as required by the fixture safety check. Native fixture
copies use the same bounded, renewing CLI-or-workload credential provider as
the other Rust storage paths. Completed or failed Jobs remain alive until their
artifacts are copied; cleanup then removes only that run's Job, ConfigMap and
generation Secret.

## Image and run

The image must be built for `linux/amd64`, pushed to the stamped ACR, and
referenced by digest. The image publisher reads the identity client ID from
`.env.azure` and subscription from the private stamp; it refuses to publish
unless the active Azure CLI principal is the `arrow-collector` UAMI.
For an operator-authenticated fresh clone, use `--federated`: it first verifies
that the active kubectl context is the stamped cluster, applies the existing
namespace/ServiceAccount templates, and obtains an AKS-issued assertion. The
publisher signs in to the shared UAMI in a temporary Azure CLI cache, verifies
that principal again, and removes the cache afterward. Operator credentials are
not replaced. First obtain the stamped cluster's kubeconfig with
`az aks get-credentials` and convert it with `kubelogin convert-kubeconfig -l azurecli`.

This native-federated ACR publisher is separate from repository GitHub Actions
image publication to GHCR, which uses `GITHUB_TOKEN` with `packages: write`, not
Azure OIDC. GHCR CI does not require external publisher FICs or Azure Actions
variables.

```bash
npx nx run azure-qap-experiment:image -- \
  --federated \
  --registry <stamped-acr> --env .env --azure-env .env.azure \
  --stamp-config infra/azure/stamp.yaml
cp infra/kubernetes/azure-qap.example.yaml infra/kubernetes/azure-qap.yaml
# Set image to <stamped-acr>.azurecr.io/azure-qap@sha256:<published-digest>.
npx nx run azure-qap-experiment:run -- --kubernetes \
  --kubernetes-config infra/kubernetes/azure-qap.yaml \
  --operations optimize,purge,vacuum,reload
```

The YAML config controls the pinned image, optional kubectl context, CPU/memory
requests and limits, and node selector. The namespace, ServiceAccount and UAMI
are injected from validated Azure output, not chosen independently in the YAML.
Kustomize renders a fresh overlay per run. Jobs have no automatic retry; the
runner streams logs and retrieves the report, JSONL evidence and KQL files
before deleting only its uniquely named Job and generated resources.

## Evidence and validation

New diagnostic routing can take
[up to 90 minutes to start](https://learn.microsoft.com/en-us/azure/azure-monitor/data-collection/diagnostic-settings#expected-latency-after-creating-a-diagnostic-setting).
Bootstrap readiness does not imply telemetry readiness. A cold-stamp run can
recover QAP and pass row parity while correctly failing the RELOAD positive
control because no read logs arrived. Preserve that failed result and its query
files; qualify routing before a new run instead of weakening evidence checks.
Copied reports use query paths relative to the directory containing `report.md`,
so their late-log commands remain usable after retrieval from a pod. A pending
zero-read count is not evidence that unchanged files were not re-read.

ADX does not guarantee a Kusto-specific user-agent: live QAP reads included
empty headers and a generic .NET SDK header. Neither is an identity proof.
Attribution uses the trial's `experimentqap/runs/<runId>/<trial>/` path and shared
principal, excluding every owned fixture/maintenance request by its enforced
run-specific marker and recorded client/server request IDs. Incomplete HTTP
receipts fail attribution. The warm-up evidence window begins after synchronous
fixture seeding, so native-copy source reads cannot contaminate it. Each trial
has separate receipts and paths, including when operations run in parallel.

| Stamp YAML setting | Evidence destination | Recheck |
| --- | --- | --- |
| `storageReadLogDestination: log-analytics` (omitted setting default) | `StorageBlobLogs` in the configured workspace | Saved `queries/*.kql`, including owned-request exclusions |
| `storageReadLogDestination: storage` | Azure Monitor NDJSON in separate `arrowreadlogs<stamp>` storage | Saved `queries/*.yaml` through `experiments/azure-qap/src/read-logs.ts` |

The storage destination is declarative, uses Entra-only read access for the
operator and shared UAMI, and is not the fixture account. It preserves raw
operation, time, principal, path, bytes and correlation IDs in `evidence.jsonl`;
HTTP receipts survive pod retrieval under `http-traces/`. The chosen destination
is explicit in the report: there is no silent fallback to another source.
Fixture cleanup/reset does not remove diagnostic evidence; deleting the sandbox
resource group does. Storage logs incur storage charges until deleted.

AKS requires `RELOAD`; missing reads of any unchanged file fail its positive
control. Preserve late-log waiting, query files and report output; never turn
missing telemetry into a zero-reread claim.

Offline verification: `npx nx run azure-infra:test`,
`npx nx run azure-qap-experiment:verify`,
`npx nx run devcontainer:verify`, and
`az bicep build --file infra/azure/main.bicep --stdout`.
