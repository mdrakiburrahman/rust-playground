# AKS QAP operations

The AKS mode runs the existing Azure QAP experiment runtime as a Kubernetes Job.
It does not deploy the unrelated SQL Server/Hive Compose stack and does not
replace the devcontainer mode.

## Qualification and safety

| Requirement | Qualified value or guard |
| --- | --- |
| Azure target | The private stamp subscription, `arrow-collector-rg`, East US 2; never the separate `rust-playground` resource group. |
| Stamp | Integer `1..999999` in ignored `infra/azure/stamp.yaml`; `arrow-collector-<stamp>` must fit Kusto's provider-enforced 22-character name limit. |
| AKS baseline | 1.35.6; `Standard_D8s_v6` system pool with 3 initial/3 minimum/6 maximum nodes, 250 pods per node, zones 1/2/3, Azure Linux/FIPS, Entra/Azure RBAC, OIDC/workload identity, Azure CNI overlay dual stack and API-server VNet integration. |
| Declared identity | One `arrow-collector` UAMI for control plane, kubelet, QAP ServiceAccount federation, ADX/storage and ACR. Identity-creating optional add-ons are omitted; tenant-enforced add-ons require separate live qualification below. |
| Publisher ownership | Generated `AZURE_STAMP` selects the same `stamp-<integer>` authority in TypeScript and Rust. Invalid stamps fail closed; an absent stamp retains the Rust legacy hashed-authority path only for older deployments. |
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
