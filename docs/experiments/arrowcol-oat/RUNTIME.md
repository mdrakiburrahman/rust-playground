# Isolated ArrowCol OAT runtime

| Component | Contract |
| --- | --- |
| `arrowcol` Deployment / `dataflow` container | Integrated Flight receiver → Delta exporter. One or two replicas, each with `--num-cores 1` or `2`; constant CPU request/limit of 2. |
| `arrowcol-router:50050` | ClusterIP only, h2c to private Flight `32010`, mandatory trusted-edge HMAC, SQL-serialized distinct table owners. Operator access by port-forward only. |
| `arrowcol-catalog:8817` | Persistent 16Gi state PVC, same stamped dataflow warehouse as DFE, `--ingestion-layout`. |
| `arrowcol-sql:1433` | Persistent 32Gi PVC; SQL2022 CU27, routing/fencing only. SA bootstrap then checksum-verified V1–V10 migrations; separate SA/app/migrator secrets. |
| Namespace and scheduling | Configurable namespace, default example `arrowcol-oat`; benchmark node labels `type=benchmark`, amd64. No changes to the Azure QAP Job overlay or system node pools. |
| Identity | Workload-identity-labeled catalog/DFE pods; optional `serviceAccount` defaults to `arrowcol-oat`. Stamp 89152 reuses namespace/service account `arrow-collector`. No Azure/GitHub CLI credential mounts. |

DFE explicitly enables generated ingestion partitions with
`ARROWCOL_PARTITION_GRANULARITY=hour`. Existing committed partition layout and
table properties remain authoritative; without this writer mode a logical
ingress batch lacks the physical `__arrowcol_ingestion` partition value.

## Prerequisites and authorization

Complete the selected stamp's **supported provision/bootstrap**, including Delta
initialization and `complete_bootstrap`, before starting writers. Select the
generated `.env.azure` and `.env.azure.tables.json` for that stamp, not a different
stamp's repository defaults. Never invent/reset a generation or recover/delete
publisher state to make a run proceed.

The runtime UAMI needs:

* **Storage Blob Data Contributor on the selected stamped data container**:
  catalog metadata commits and DFE Parquet/Delta writes under
  `dataflow/<catalog>/<schema>/<table>`. Reader is insufficient.
* Existing checkpoints-container contributor for generation/publisher fencing.
* Federation for the configured namespace/service account, audience
  `api://AzureADTokenExchange`. Stamp 89152 already authorizes
  `system:serviceaccount:arrow-collector:arrow-collector`; set both configuration
  fields to `arrow-collector`, without adding a FIC or changing the shared UAMI.

The operator needs an authorized Kubernetes context, image-pull readiness,
metrics API, a real QAP endpoint, and write access to approved `experiments`
and root `$web` destinations. Ensure at least one benchmark node is available;
this runtime never scales node pools or grants Azure roles.
The private fixture container `experimentqap` is not a substitute for the owned
`experiments` results container. Complete its supported provisioning/role
reconciliation before publishing OAT results.

**An enforcing Kubernetes network-policy provider is mandatory.** Azure CNI
Overlay alone is not proof of enforcement. The internal DFE receiver does not
authenticate public clients; only the HMAC-checking router may reach Flight.
The infrastructure owner must verify the cluster's policy provider before
deployment. Foundation installs default-deny ingress before any workload starts;
do not omit that phase or run this topology on a cluster ignoring NetworkPolicy.

## Build and rehearse

Run inside the repository devcontainer. These commands create local images only:

```bash
docker build -f projects/arrowcol-infra/docker/sqlserver/Dockerfile -t arrowcol-oat/sqlserver:local projects/arrowcol-infra
docker build -f projects/arrowcol-catalog-rust/Dockerfile -t arrowcol-oat/catalog:local .
docker build -f projects/arrowcol-dataflow-rust/Dockerfile -t arrowcol-oat/dataflow:local .
docker build -f projects/arrowcol-router-rust/Dockerfile -t arrowcol-oat/router:local .
docker build -f projects/arrowcol-oat/Dockerfile -t arrowcol-oat/client:local .
npx --no-install nx run arrowcol-oat:runtime-smoke
```

The smoke uses a unique internal Docker network, generated private files and
owned volumes; no cloud credentials or Kubernetes calls. It checks SQL migrations
twice, authenticated versus unsigned Flight, all four replica/core cases, one
Flight/admin listener per worker, physical Delta output, graceful DFE shutdown
and catalog restart. Its cleanup removes only its own containers/volumes/files.
It is functional evidence, **not an Azure performance benchmark**.

The client image is only for rehearsal. Build both host binaries for live OAT:

```bash
cargo build --locked --package collector-chain --bin arrowcol-dmv --package arrowcol-oat-rust --bin arrowcol-oat-rust
```

## Prepare manifests and private files

Copy `runtime-config.example.json` to an ignored directory under `out/arrowcol-oat`
and replace every placeholder with the selected stamp's actual values/digests.
For stamp 89152 set both `namespace` and `serviceAccount` to `arrow-collector`.
`deltaRoot` must be that stamp's bootstrapped **data container** plus `/dataflow`
(include the actual prefix if provision used one), not experiments or `$web`.
For stamp 89152 the verified root is
`abfss://arrowcol@arrowcollector89152.dfs.core.windows.net/dataflow`.
Use `subscription` from the selected `stamp.yaml` and `AZURE_UAMI_CLIENT_ID`
from its generated `.env.azure`; the generated environment has no subscription ID.
Do not copy passwords, tokens or the generation into runtime JSON.

Before publication, each image value may be the explicit marker `"unpublished"`
in this ignored draft. **The renderer rejects such drafts**; never substitute
Docker image IDs or invented manifest digests.

The identity-only phase is the sole exception: it renders only the namespace
and ServiceAccount, never a pod or secret. If they do not yet exist, the cluster
owner can establish the already-federated subject before image publication:

```bash
export KUBECONFIG="$PWD/target/arrowcol-oat/stamp-89152/kubeconfig"
npx --no-install tsx projects/arrowcol-oat/runtime.ts render \
  out/arrowcol-oat/runtime.json identity out/arrowcol-oat/identity.json
# Authorized cluster-owner action; creates/updates only namespace and ServiceAccount.
kubectl apply -f out/arrowcol-oat/identity.json
```

This does not create/change the Azure UAMI or FIC and does not start workloads.

Commit source changes first. The following authorized command pushes the four
local service images and atomically replaces draft image values with real
**registry manifest digests**; it does not apply workloads or identity resources:

```bash
export KUBECONFIG="$PWD/target/arrowcol-oat/stamp-89152/kubeconfig"
npx --no-install tsx projects/arrowcol-oat/publish-images.ts \
  out/arrowcol-oat/runtime.json --federated --registry arrowcollector89152 \
  --env .env --azure-env target/arrowcol-oat/stamp-89152/.env.azure \
  --stamp-config target/arrowcol-oat/stamp-89152/stamp.yaml
```

This reuses the QAP publisher's `withPublisherIdentity` helper: verify the stamped
cluster and **existing** ServiceAccount annotation, obtain a short-lived AKS
assertion, authenticate as the shared UAMI, then verify the principal before ACR
login. Azure and Docker credential caches are isolated under ignored `out/` and
removed afterward; operator credentials are not replaced. It never creates or
updates a FIC, namespace or ServiceAccount. If the already-authorized
ServiceAccount is absent or mismatched, stop and have its owner establish it.
Do not replace this command with operator `az acr login`.

After publication, prepare secret files and render manifests. These commands
perform no cluster actions:

```bash
mkdir -p out/arrowcol-oat
npx --no-install tsx projects/arrowcol-oat/runtime.ts prepare-secrets \
  target/arrowcol-oat/stamp-89152/.env.azure out/arrowcol-oat/secrets
for phase in foundation bootstrap workload; do
  npx --no-install tsx projects/arrowcol-oat/runtime.ts render \
    out/arrowcol-oat/runtime.json "$phase" "out/arrowcol-oat/$phase.json"
done
```

Secret generation reads the **captured real generation** and creates exclusive
0600 files in a 0700 directory on the devbox. Existing credentials are preserved;
generation changes or unsafe permissions fail. Do not print these files, put
values in shell history/job code, check them in, or rotate them with existing
SQL/catalog state.

## Operator deployment sequence

The following changes the live cluster and is deliberately separate from render.
Apply only after the selected stamp, role scope and federation are ready.
Use the approved context and namespace consistently:

```bash
export KUBECONFIG="$PWD/target/arrowcol-oat/stamp-89152/kubeconfig"
NAMESPACE="$(node -p 'JSON.parse(require("fs").readFileSync("out/arrowcol-oat/runtime.json","utf8")).namespace')"
kubectl apply -f out/arrowcol-oat/foundation.json
kubectl -n "$NAMESPACE" create secret generic arrowcol-sql-admin \
  --from-file=password=out/arrowcol-oat/secrets/sa-password --dry-run=client -o json | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic arrowcol-sql-runtime \
  --from-file=password=out/arrowcol-oat/secrets/app-password --dry-run=client -o json | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic arrowcol-sql-migration \
  --from-file=password=out/arrowcol-oat/secrets/migration-password --dry-run=client -o json | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic arrowcol-router-auth \
  --from-file=edge-hmac=out/arrowcol-oat/secrets/edge-hmac \
  --from-file=internal-hmac=out/arrowcol-oat/secrets/internal-hmac --dry-run=client -o json | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic arrowcol-generation \
  --from-file=generation=out/arrowcol-oat/secrets/generation --dry-run=client -o json | kubectl apply -f -
kubectl -n "$NAMESPACE" rollout status deployment/arrowcol-sql --timeout=600s
kubectl apply -f out/arrowcol-oat/bootstrap.json
kubectl -n "$NAMESPACE" wait --for=condition=complete job/arrowcol-migrate-v10 --timeout=600s
kubectl apply -f out/arrowcol-oat/workload.json
kubectl -n "$NAMESPACE" rollout status deployment/arrowcol-catalog --timeout=600s
kubectl -n "$NAMESPACE" rollout status deployment/arrowcol --timeout=600s
kubectl -n "$NAMESPACE" rollout status deployment/arrowcol-router --timeout=600s
kubectl -n "$NAMESPACE" port-forward service/arrowcol-router 50050:50050
```

Stop on failed migration/readiness; inspect logs without printing secret files.
Do not delete PVCs or reset publisher authority. Bootstrap/migrations run once as
a completed Job; runtime pods never mount the migrator or SA credentials.
Network policies deny pod ingress by default, permit only router→Flight,
DFE→catalog and runtime/migration→SQL. Kubelet probes and operator port-forward
remain available. No public router, external catalog, or direct public collector.

## Matrix and interpretation

Copy `oat-config.example.json` into the ignored run directory. Keep both
Deployment/container/selector pairs identical (`arrowcol`/`dataflow`/`app=arrowcol`),
set its namespace to the configured runtime namespace (`arrow-collector` on 89152),
use `http://127.0.0.1:50050`, the private edge-key path, the host DMV binary, actual
QAP endpoint/database and actual experiments/root `$web` destinations.
Read the QAP endpoint/database from the selected environment's `AZURE_KUSTO_URI`
and `AZURE_KUSTO_DATABASE`, not the experiment database or repository defaults.
The QAP database for stamp 89152 is **`arrowcol`**, not `experimentqap`.

The OAT **host process** must set `ARROWCOL_MODE=azure`; otherwise the loadgen
Kusto observer defaults to local mode and rejects the HTTPS endpoint. Its
`BoundedAzureCli` provider accepts `AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and
`AZURE_FEDERATED_TOKEN_FILE`. Use the existing `arrow-collector` ServiceAccount's
private assertion file prepared by the operator, with audience
`api://AzureADTokenExchange`; keep it mode 0600 and refresh before expiry.
Do not replace the operator's Azure CLI login used by kubelogin:

```bash
export ARROWCOL_MODE=azure
export AZURE_CLIENT_ID="$(node -p 'JSON.parse(require("fs").readFileSync("out/arrowcol-oat/runtime.json","utf8")).workloadIdentityClientId')"
SUBSCRIPTION="$(node -p 'JSON.parse(require("fs").readFileSync("out/arrowcol-oat/runtime.json","utf8")).subscriptionId')"
export AZURE_TENANT_ID="$(az account show --subscription "$SUBSCRIPTION" --query tenantId -o tsv)"
export AZURE_FEDERATED_TOKEN_FILE=/absolute/path/to/operator-prepared-private-assertion
cargo run --locked --package arrowcol-oat-rust
```

In another terminal submit with
`npx --no-install tsx projects/arrowcol-oat/launch.ts <private-oat-config.json>`.
The server captures/restores each unique Deployment once, applies replicas/core
args together, waits for rollout and stale 30-second leases, and counts each pod
once. Pod UID and advertised pod IP come from the downward API.

Each selected table receives independent HMAC-signed connections. The router's
`distinct_table_owners` setting gives separate active tables separate collectors;
one HTTP/2 connection alone cannot exercise multiple SO_REUSEPORT workers.
The benchmark router also limits each upstream H2 connection to one active
stream and uses zero idle timeout, preventing upstream multiplexing from
collapsing independent client connections onto one DFE worker. These settings
are fixed across the matrix and do not change the normal router defaults.
Keep client concurrency comfortably above the worker count (the example uses
16); SO_REUSEPORT distributes connections by kernel hashing, not round-robin.
`--num-cores` is authoritative because the rendered pipeline has no resource
policy override. Flight success is **best-effort admission**, not publication.
QAP observations and client timing must not be presented as a durability receipt.
Only measured sanitized aggregate Parquet is published; no raw payloads or
credentials belong in the anonymously readable `$web` container.
