# AKS QAP operations

The AKS mode runs the existing Azure QAP experiment runtime as a Kubernetes Job.
It does not deploy the unrelated SQL Server/Hive Compose stack and does not
replace the devcontainer mode.

## Qualification and safety

| Requirement | Qualified value or guard |
| --- | --- |
| Azure target | The private stamp subscription, `arrow-collector-rg`, East US 2; never the separate `rust-playground` resource group. |
| Stamp | Positive integer in ignored `infra/azure/stamp.yaml`; deployment names are derived from it. |
| AKS baseline | 1.35.6; `Standard_D8s_v6` system pool with 3 initial/3 minimum/6 maximum nodes, 250 pods per node, zones 1/2/3, Azure Linux/FIPS, Entra/Azure RBAC, OIDC/workload identity, Azure CNI overlay dual stack and API-server VNet integration. |
| Identity | Exactly one `arrow-collector` UAMI for control plane, kubelet, QAP ServiceAccount federation, ADX/storage and ACR. Add-ons that create identities are omitted. |
| Networking | Explicit IPv4/IPv6 VNet, node, API, pod, service and DNS ranges; preflight checks subscription VNets and connected peering ranges before deployment. |
| Legacy gate | Preflight blocks while old `project=arrow-collector` resources or additional UAMIs remain. Preserve Terraform state, Key Vault dependencies, role assignments, registry publishing consumers and every FIC subject before retiring the old stamp. |

No destructive replacement is authorized outside `arrow-collector-rg`. The
separate `rust-playground` resource group must remain untouched. Do not delete
old resources or soft-deleted Key Vault content to bypass an incomplete
inventory. The current preflight reaches the legacy-resource gate only after
Azure version, SKU/quota, API-server feature and network checks succeed.

Prepare the private stamp from `infra/azure/stamp.example.yaml`. Keep the
subscription UUID, real non-overlapping network ranges and preserved publisher
FICs private. Align the operator environment's subscription, resource group and
location with that stamp.

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

## Image and run

The image must be built for `linux/amd64`, pushed to the stamped ACR, and
referenced by digest. The image publisher reads the identity client ID from
`.env.azure` and subscription from the private stamp; it refuses to publish
unless the active Azure CLI principal is the `arrow-collector` UAMI.

```bash
npx nx run azure-qap-experiment:image -- \
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

`StorageBlobLogs` attribution must be scoped to the run's
`experimentqap/runs/<runId>/` prefix, the shared principal, and a Kusto/ADX
`UserAgentHeader`. The identity alone cannot distinguish pod storage traffic
from ADX reads. The runtime excludes rows without that marker; AKS mode requires
`RELOAD`, and the run fails if the positive control does not read every
unchanged file. The exact ADX user-agent signature must be confirmed against
live Azure logs before interpreting a run; an unknown signature fails closed.
Preserve late-log waiting, query files and report output; never turn missing
telemetry into a zero-reread claim.

Offline verification: `npx nx run azure-infra:test`,
`npx nx run azure-qap-experiment:verify`,
`npx nx run devcontainer:verify`, and
`az bicep build --file infra/azure/main.bicep --stdout`.