# ArrowCol Azure sandbox

This is original, public infrastructure code. Private reference implementations
were used only to understand operational requirements, not copied.

Local disk and local Kusto remain the loadtest default. `--mode azure` (or
`ARROWCOL_MODE=azure`) switches storage and Kusto together. Azure writers and the
bootstrap use the existing signed-in Azure CLI identity; Kusto reads Delta using
the dedicated user-assigned managed identity. No account keys, client secrets,
SAS tokens, or credential files are required.

## Private configuration

Copy the root `.env.example` into repo-bootstrapper's private
`sync/rust-playground/.env`, fill in the authorized subscription, and let
repo-bootstrapper distribute it. Never commit or print the filled file.
On the controller the private source is
`C:\Users\mdrrahman\repo-bootstrapper\sync\rust-playground\.env`.

Run all following commands inside the repository Linux devcontainer:

```bash
python3 infra/azure/bootstrap.py config
python3 infra/azure/bootstrap.py preflight
python3 infra/azure/bootstrap.py provision
```

`.env.azure` is generated privately with mode `0600`; it contains deterministic
names, Kusto endpoint and canonical `abfss://` data/checkpoint roots. The runner
loads `.env`, then `.env.azure`, then explicit process-environment overrides.
Names derive from the private subscription and exact `rust-playground` resource
group, so independent devboxes agree without sharing resource identifiers in
commands or source. `config` does not contact Azure and is not deployment proof.

Optional private knobs include region, supported Standard Kusto SKU, QAP hot
days, identity/database/container names, and autoscaling bounds. Defaults are
`eastus2`, `Standard_E4ads_v5`, two to four instances, and seven QAP hot days.
The maximum is capped at eight. Actual region/SKU availability and quota must be
qualified in the authorized subscription; defaults are not a quota guarantee.
The script never falls back to another subscription.

## Dependency-ordered bootstrap

The subscription-scope Bicep creates the resource group, HNS storage account,
two private containers, dedicated identity, least-privilege storage assignments,
bounded-autoscaling ADX cluster, database and principals. The operator gets
Blob Data Contributor on this account only. The Kusto identity gets Blob Data
Reader on the data container only, not checkpoint write access. Storage shared
keys/public blobs are disabled. The disposable account disables soft deletion,
versioning, restore and change feed: do not use it for production data.

The reconciler refuses to adopt existing untagged/foreign resources. It preserves
checkpoint-container control metadata across Bicep redeployment. It waits for
Kusto access and storage RBAC, fences publication, runs the foundation's
`initialize-delta.sh`, and checks every declared root has a valid version-zero
Delta protocol and metadata before creating external tables. The initializer
must create only absent roots, never overwrite a live table.

KQL reconciliation enables exactly `ExternalTable,AutomatedFlows` managed-identity
usages, creates or alters inferred-schema Delta external tables, and enables QAP
with the identity's object ID. `QueryAcceleration` is not a managed-identity
usage. Only after initialization and all KQL succeed is publication unfenced.
Failure is visible and leaves the sandbox fenced, not falsely ready.

`provision --infra-only` deliberately omits Delta initialization/KQL/QAP. Its
output explicitly says that full bootstrap was **not verified**. It is useful
for diagnosing infrastructure access, not an acceptance shortcut.

## Cloud occupancy and reset

The test runner acquires shared occupancy before live tests and releases it only
after its owned writers/consumers stop successfully:

```bash
python3 infra/azure/bootstrap.py occupy --occupancy-id "$RUN_UUID"
python3 infra/azure/bootstrap.py release --occupancy-id "$RUN_UUID"
```

Only the exact UUID owner can release. Provision/reset/stop reject active
occupancy. Lifecycle authority is metadata on the preserved checkpoint
container, not a blob: `sandboxid`, `resetstate`, `resetgeneration`, `occupancy`,
and `occupancyid`. Updates use `If-Match` ETags. Writers fail closed unless
ownership matches and `resetstate=ready`, checking before open, upload and each
commit attempt. A restarted writer must not bypass a fenced sandbox.

```bash
# Preview only: no mutation.
python3 infra/azure/bootstrap.py reset
# Mutate only after the cloud test owner releases occupancy.
python3 infra/azure/bootstrap.py reset --execute
```

Execution verifies exact ownership, HNS, disabled retention, the exact database
allowlist, and the complete container allowlist/ownership. It persistently fences
remote writers, stops the local workspace-owned loadtest, waits 60 seconds for
bounded in-flight requests, inventories again, drops the owned database, and
deletes every owned blob, snapshot and version, including Delta/service
checkpoints. It refuses preexisting soft-deleted objects rather than claiming
hidden retained data is gone. Unknown resources cause failure, not deletion.

Cluster, identity, account and both containers survive. Verification requires
zero databases and literally zero blobs in both containers. Container control
metadata remains fenced without leaving a marker blob. A subsequent full
`provision` recreates the declared Delta/Kusto baseline and returns it to ready.
The foundation's explicit empty-root initializer is the only bootstrap path that
can create schema roots while fenced; normal publishers have no bypass.

After live tests release occupancy, the lifecycle owner stops (never deletes)
the cluster:

```bash
python3 infra/azure/bootstrap.py stop
python3 infra/azure/bootstrap.py stop --execute
```

The executing command waits until ARM reports `Stopped`. Reprovisioning starts a
stopped cluster and waits for real Kusto readiness.

## Verification and evidence

```bash
npx nx run azure-infra:verify
```

This runs offline Python safety tests and compiles Bicep with Azure CLI. It
needs no Azure identity and does not deploy. Live qualification is separate:
provision twice, inspect all external-table/QAP policies, exercise reset and full
rebootstrap, hand the ready sandbox to the cloud test owner, then stop after
release. Do not equate offline checks or `config` with live deployment success.

Operational logs redact private identifiers/endpoints. Do not publish raw ARM,
KQL, CLI token responses, generated configuration, or subscription details.
If the supplied subscription is absent from the existing identity, bootstrap
fails without trying another subscription; restore access to that authorized
subscription before repeating live qualification.

Public protocol references:

- [Delta external tables](https://learn.microsoft.com/en-us/kusto/management/external-tables-delta-lake)
- [Managed identity policy](https://learn.microsoft.com/en-us/kusto/management/managed-identity-policy)
- [QAP policy](https://learn.microsoft.com/en-us/kusto/management/alter-query-acceleration-policy-command)
