# AKS QAP operations

The AKS mode runs the existing Azure QAP experiment runtime as a Kubernetes Job.
It does not deploy the unrelated SQL Server/Hive Compose stack and does not
replace the devcontainer mode.

## Existing-stamp OAT setup

Use the ignored private YAML for the intended stamp; the tracked example/default may describe a different region or stamp. Pass the same path to setup, standalone preflight and the run:

```bash
STAMP_CONFIG=infra/azure/stamp.yaml
# For a new stamp only; auto uses Azure first and one eligible Cilium fallback.
npx --no-install arrowcol-oat aks deploy --stamp-config "$STAMP_CONFIG"
# Existing stamp, or after deploy:
npx --no-install arrowcol-oat aks setup --stamp-config "$STAMP_CONFIG"
npx --no-install arrowcol-oat --mode aks --preflight-only \
  --stamp-config "$STAMP_CONFIG" --config out/arrowcol-oat/oat-config.aks.json
npx --no-install arrowcol-oat --mode aks --stamp-config "$STAMP_CONFIG" \
  --config out/arrowcol-oat/oat-config.aks.json
```

Setup builds or reuses revision-pinned images, writes private runtime inputs under ignored `out/arrowcol-oat`, and runs preflight itself; continue only when it reports READY. Use `--stamp <five-digit-stamp>` if more than one live AKS cluster is present in the selected resource group. Do not use `--skip-preflight`. The separate read-only preflight command is optional confirmation, not a bypass.

Each OAT cell creates a namespace from the configured prefix plus its compact run UUID, applies a run ownership label, and cleans up only its own namespace and federated credential. The benchmark verdict checks for remaining run-scoped resources. Do not manually apply a shared static workload namespace.

`aks destroy --stamp-config "$STAMP_CONFIG"` only inventories resources. Adding `--confirm arrow-collector-rg` deletes the whole primary resource group and captured AKS-managed resource groups, including every stamp there; it is not per-run cleanup. Use that confirmation only for an intentional full teardown after reviewing the inventory and receipt.

## Router authentication

Edge HMAC is enabled on AKS with matching mounted keys and pinned router and client images; it authenticates requests to the router but does not provide TLS. Catalog Flight SQL and CES requests require signed internal identity by default. Assertions bind the RPC method and payload digest, carry a one-time nonce, and expire within 30 seconds; the replay cache is bounded at 65,536 requests per catalog replica and rejects new requests when full. The catalog and dataflow mount the `internal-hmac` key from `arrowcol-router-auth` at `/run/auth/internal-hmac`; generated direct OAT client Jobs mount that key at `/run/secrets/internal-hmac` and set `LOADTEST_INTERNAL_SECRET_FILE` to that path. `--ddl-auth-mode dev_only_allow_unsigned` bypasses catalog RPC identity checks and is for development only; do not use it on AKS. The separate data-plane Flight SQL service is unchanged. TLS/mTLS is deferred. NetworkPolicy objects are not assumed to be enforced on the default AKS dataplane, and no QAP host allowlist is added. The shared `qap_endpoint` validator checks URL shape only; AKS preflight pins it to HTTPS and the exact selected stamp and region Kusto host, with no credentials, query, fragment, or path beyond a slash. A destination allowlist beyond those checks remains a deferred policy decision.

## Qualification and safety

| Requirement | Qualified value or guard |
| --- | --- |
| Azure target | The private stamp subscription, `arrow-collector-rg`, East US 2 or Canada Central; never the separate `rust-playground` resource group. Each region still requires live SKU, version, quota and network preflight. |
| Stamp | Integer `1..999999` or the quoted five-digit string `'00000'` in ignored `infra/azure/stamp.yaml`; `arrow-collector-<stamp>` must fit Kusto's provider-enforced 22-character name limit. |
| AKS baseline | 1.35.6; `Standard_D8ds_v6` system pool with 3 initial/3 minimum/3 maximum nodes, 250 pods per node, zones 1/2/3, Azure Linux/FIPS, Entra/Azure RBAC, OIDC/workload identity, Azure CNI overlay dual stack and API-server VNet integration. |
| Template identity | One `arrow-collector` UAMI is declared in the primary resource group for control plane, kubelet, QAP ServiceAccount federation, ADX/storage and ACR. This is not a managed-resource-group or deployment-wide identity count; see tenant-enforced identities below. |
| Publisher ownership | Generated `AZURE_STAMP` selects the same `stamp-<stamp>` authority in TypeScript and Rust, preserving `00000` when selected. Invalid stamps fail closed; an absent stamp retains the Rust legacy hashed-authority path only for older deployments. |
| Networking | Explicit IPv4/IPv6 VNet, node, API, pod, service and DNS ranges; preflight checks subscription VNets and connected peering ranges before deployment. |
| Legacy gate | Preflight blocks unaccounted legacy `project=arrow-collector` resources or extra application UAMIs in the primary resource group. Preserve Terraform state, Key Vault dependencies, role assignments, registry publishers and every FIC subject before retiring old resources. Platform-managed identities in the AKS-managed group are separately inventoried. |

No destructive replacement is authorized outside `arrow-collector-rg`. The
separate `rust-playground` resource group must remain untouched. Retiring old
resources requires either a completed dependency inventory or explicit
authorization to discard the exact sandbox and its state. Purge protection
cannot be bypassed; use fresh stamped names after an authorized reset.

### Tenant-enforced identities

The deploy receipt verifies exactly one `arrow-collector` UAMI in the primary
resource group only; it does not inventory or certify managed-resource-group or
deployment-wide identity cardinality.

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
npx --no-install tsx infra/azure/bootstrap.ts provision --stamp-config infra/azure/stamp.yaml --preview-before-create
```

`provision` applies subnet Network Contributor, AKS control-plane/kubelet
identity use, AKS RBAC Cluster Admin for the operator, ACR push/pull, and the
QAP service-account federated credential automatically. The QAP pod receives
only generated resource settings and the reset-generation secret; it does not
mount the operator's Azure CLI cache or credentials.

`npx --no-install arrowcol-oat aks deploy` automatically runs a read-only ARM
what-if immediately before every create attempt, including the Azure dataplane
attempt and any allowed Cilium or convergence retry. Bootstrap reuses the same
resolved stamp, reset authority, resource-group/UAMI locations and tags, template
path, and private parameter file for each preview and its create. A failed,
timed-out, or unverified preview stops before creation and cannot trigger network
fallback. The preview pins Azure CLI to `--result-format FullResourcePayloads` and
`--no-pretty-print`. The validator rejects deletions, `Deploy` and unsupported
changes, resource IDs outside the selected subscription/resource group, and
resource types outside the templates; extension-resource role assignments and
diagnostic settings are
accepted only when both the parent and extension types are allowlisted.
Both definite and potential changes receive the same checks, in either the
observed flat CLI envelope or the REST `properties` envelope; ambiguous or
malformed envelopes fail closed. `NoEffect` is not a resource change type.
Diagnostics are read from the same envelope. Error, Warning (including skipped
nested deployments), unknown or missing levels, malformed diagnostics and
ambiguous placement stop before create. Info diagnostics print their level, code
and redacted message; absent, null or empty diagnostics are allowed.
A `Modify` is accepted only for the exact shared resource group or UAMI.
Their Bicep-managed projection is name, location and tags (checked against the
compiled templates in tests). Both snapshots must include matching resource
ID, type, name, API version and location; every non-tag field must remain
identical. Flat `path`/`propertyChangeType` deltas must account for every tag
change. Missing identity fields, tag removals, nested/array deltas, and all
other resource modifications fail closed except the proven role-reference no-op
below, including an in-place AKS pool
update. Nested children are unsupported, not silently discarded.
The direct `bootstrap.ts provision` command
requires `--preview-before-create` to enable this guard. The standalone
`aks deploy --what-if` command remains read-only.

For Cilium retries and idempotent reruns, a role-assignment `Modify` is accepted
only for an exact template-derived deterministic ID and role definition.
The only allowed delta is an unresolved `properties.principalId` reference to
the shared UAMI, optionally accompanied by `principalType: ServicePrincipal`
as `NoEffect`. One read-only UAMI lookup per preview must prove that the before
principal equals the live identity principal; snapshots must preserve every
other assignment property and scope. Missing proof or any other delta stops
before create. The template parity test also checks the runtime role list.

The sanitized Azure CLI 2.92.0 fixture is a projection of a read-only
provider capture: shared RG/UAMI `NoChange` snapshots, diagnostic-settings
`Create`, and a role-assignment
`Modify` with the unresolved principal reference described above.
Tag-only `Modify` and the REST `properties` wrapper are synthetic test cases;
those shapes remain unobserved in this capture. A successful ARM preview is
not authorization to deploy, and an unresolved expression alone is not proof
of a safe principal replacement.

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

`aks deploy` defaults to Azure networking and allows one Cilium retry only
after a proven AKS-creation failure. Use `--network azure` or `--network cilium`
to pin a dataplane; a pin disables fallback. A raw bootstrap deployment can
select `aks.networkDataplane: cilium` in the private stamp. Azure dataplane
NetworkPolicy objects are not enforced (`networkPolicy: none`): in-cluster
workloads can reach the passwordless dataflow Flight port, though there is no
public ingress. Cilium enforces the declared policies.
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

`aks setup` builds or reuses, publishes and verifies the five revision-pinned
OAT images (SQL Server, catalog, router, dataflow and client), then generates
private OAT runtime inputs under ignored `out/arrowcol-oat`. This is separate
from the standalone Azure QAP image used by the bounded Job below. Build that
`azure-qap` image for `linux/amd64`, publish it to the stamped ACR and reference
it by digest with the `azure-qap-experiment:image` command below. The Azure QAP
image publisher reads the identity client ID from `.env.azure` and subscription
from the private stamp; it refuses to publish unless the active Azure CLI
principal is the `arrow-collector` UAMI.

For an operator-authenticated fresh clone, use `--federated` with this Azure QAP
image command: it first verifies that the active kubectl context is the stamped
cluster, applies the existing namespace/ServiceAccount templates, and obtains an
AKS-issued assertion. The publisher signs in to the shared UAMI in a temporary
Azure CLI cache, verifies that principal again, and removes the cache afterward.
Operator credentials are not replaced. First obtain the stamped cluster's
kubeconfig with `az aks get-credentials` and convert it with
`kubelogin convert-kubeconfig -l azurecli`.

This native-federated Azure QAP ACR publisher is separate from repository GitHub
Actions image publication to GHCR, which uses `GITHUB_TOKEN` with
`packages: write`, not Azure OIDC. GHCR CI does not require external publisher
FICs or Azure Actions variables.

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
