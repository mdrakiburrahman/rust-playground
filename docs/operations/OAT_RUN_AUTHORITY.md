# OAT run authority

The SQL and federation helpers below are source changes, not deployment or
execution authorization. Apply the current worker-specific clearance gates
before transferring or executing a draft. Earlier PR-head CI does not qualify
these changes.

## SQL initialization and immutable ownership

| Surface | Contract |
| --- | --- |
| Migration | V0011 follows the existing checksum ledger and transactional migration lock. Both platforms must complete it before starting routers. |
| Setup identity | Only the finite pin-init Job receives the existing `arrowcol_migrator` credential. Never mount it in the router, client fleet or collector. |
| Runtime identity | `arrowcol_app` can SELECT the three authority tables and EXECUTE only the `routing_run_pin_set_digest` function through its explicit new grant. Explicit object-level INSERT/UPDATE/DELETE DENYs override inherited `db_datawriter` membership; no schema-wide EXECUTE grant is added. |
| Expected set | One immutable run UUID, exact count, sorted-set digest and fixed UTC expiry. Insert the parent and the entire pending marker set in one transaction; insert all markers in one statement. |
| Owner choice | Trusted setup refreshes the routing view, applies existing collector ranking and claims a lease through SQL CAS. No request-supplied holder, lease UUID or fencing epoch is accepted. |
| Pin insertion | SQL verifies the returned live active lease and atomically changes the matching pending marker to bound. The setup caller must not perform a separate marker UPDATE. |
| Retry | Exact run UUID, expected keys, digest and expiry must agree. A bound marker without its owner row is a conflict, never permission to create a replacement. |
| Retention | Parent, marker and owner rows survive expiry as tombstones. SQL rejects owner replacement, deletion and marker rollback. Historical pins intentionally do not FK-reference rotating live leases. |
| Forwarding | Revalidate SQL authority at request admission, upstream selection and after connection acquisition before sending headers/body. Renewed lease metadata may refresh, but the selected process, endpoint and lease lineage cannot change. Streaming requests are never retried. |
| Forwarding expiry | After the final awaited authority/view read, sample fresh monotonic time and recheck the retained pin, lease, collector lifetime and pressure deadlines. I/O crossing any deadline fails closed; no additional pre-I/O timestamped query substitutes for this check. |
| Failure evidence | A lost pin, authority outage or unavailable pinned upstream emits `pinning_violation` routing failures. The OAT runner preserves failed-cell rows with `pinning_ok=false`; incomplete measurements are zero and QAP observation remains incomplete, never a successful throughput sample. |

Finite pin-init uses login `arrowcol_migrator` and database `arrowcol`, never
the app or SA credential. Local Compose keeps its existing dedicated
`arrowcol_migrator_password` secret at
`/run/secrets/arrowcol_migrator_password`. The AKS runtime uses its existing
`arrowcol-sql-migration` Secret, key `password`, mounted at `/run/migration`;
`ARROWCOL_MIGRATOR_PASSWORD_FILE=/run/migration/password`, host `arrowcol-sql`.
Both migration and finite pin-init must receive the same separately supplied
credential that initializes that SQL instance's migrator login. Missing input
is a readiness failure, not permission to generate a different password or
fall back to app credentials. The pin-init configuration is secret-free;
pass each platform's actual dedicated password-file path separately.

The finite setup calls
`RoutingEngine::initialize_run_table_pins(run_id, expected, expires_at, now)`
under the migrator identity. It refreshes/ranks collectors, claims live SQL
leases and inserts all expected pins; `initialize_run_table_set` alone is not
a completed setup. Construct its `RoutingPolicy` from the actual rendered
local/AKS router's `RoutingConfig::policy()` values, not separate defaults.
Keep pressure/warmup thresholds, lease/run-pin lifetimes, readiness freshness,
distinct-owner policy and run identity consistent. Runtime admission requires
the complete bound pin set, while runtime app credentials remain read-only
for this authority.

### Sorted-set digest v1

For each unique key, encode fields in order: implementation pool (`direct` or
`dataflow`), catalog, schema, table. Encode each unchanged string as UTF-8 bytes
and lowercase hex. Sort rows lexicographically by the **tuple of four hex
strings**, not enum order or the joined line. Join fields with `:` and append
LF to every row, including the last. Prepend `arrowcol-oat-pin-set-v1` and LF.
SHA-256 the resulting ASCII bytes and store lowercase 64-character hex.
No BOM, CR, spaces, NUL terminator, JSON escaping or Unicode normalization.
Reject duplicates and the empty set; compare run UUID and expiry separately.

For `(dataflow, a, b, c)`, the preimage is
`arrowcol-oat-pin-set-v1\n64617461666c6f77:61:62:63\n`, where each `\n` denotes
one LF byte. SQL's `routing_run_pin_set_digest` uses UTF-8 conversion and
binary hex ordering to implement the same contract.

## Run-scoped AKS federation

All commands use the existing private Azure/stamp configuration and run from
the repository's leased Linux devcontainer. The namespace and ServiceAccount
must already exist with the run's ownership labels and annotations.

| Command suffix for `npx --no-install tsx infra/azure/bootstrap.ts` | Result |
| --- | --- |
| `prepare-run-fic --run-id UUID --namespace NAME --service-account NAME --record PRIVATE_PATH` | Create one `arrowcol-run-<UUID>` FIC under the existing UAMI, with durable intent, original HTTP receipt and verified postcondition. |
| `probe-run-fic --run-id UUID --namespace NAME --service-account NAME --record PRIVATE_PATH --wait-seconds 300` | Validate the recorded identities and perform an exact-ServiceAccount token exchange before workloads start. |
| `cleanup-run-fic --run-id UUID --record PRIVATE_PATH` | **Removes only that run's recorded FIC, after its namespace is absent.** Does not remove the UAMI, native trust, other credentials or receipts. |

The namespace ends in `-<UUID without hyphens>`. Both its labels and annotations
must carry `arrowcol-oat/run-id` and `arrowcol-oat/owner-token`. Records retain
the namespace and ServiceAccount UIDs, ownership nonce, exact UAMI/client/tenant,
cluster server, issuer, subject and audience. The helper does not infer a
ServiceAccount from the stamp's QAP account.

The native trust is **`azure-qap-${clusterName}`**, as declared in Bicep.
An empty external publisher FIC list is permitted, but does not waive the
separate native issuer/subject/audience/UAMI checks.

### Shared mutation ledger

| Item | Behavior |
| --- | --- |
| Location | Existing stamp checkpoint container: `arrowcol-oat/federation/<sha256(lowercase full UAMI ARM ID)>/ledger.json`. Not `$web`; never publish this ledger or local custody files. |
| Creation | Supported Blob `If-None-Match: *`, with the first acquisition intent already present. No new account, container, identity or role. |
| Mutex | Infinite Blob lease for each short cross-service mutation transaction. Ledger updates require that lease and the captured Blob ETag. |
| Admission | Read the eligible full completion chain, acquire an infinite lease with `If-Match` for that exact Blob ETag, then reread/revalidate under the new lease before any ledger mutation. A stale conditional acquisition cannot overwrite the previous state. Initial creation includes its acquisition reservation. |
| ARM | One ordinary PUT or DELETE. No invented ARM If-Match/If-None-Match. Persist intent first; capture original HTTP status/body/correlation privately before accepting its postcondition. Creation requires status 201, never 200 adoption/update. |
| Capacity | Distinct fully proven ready runs may coexist within the 20-entry union of live credentials and recorded live authority. Native/external trusts also count. Missing proof, unresolved transactions and foreign/drifted run credentials block admission. Serialization covers short mutations, not a run's whole lifetime. |
| Completion | Verify the exact postcondition and persist the original receipt, then release only the acquired lease. Release-pending blocks successors until the conditional final commit exposes a fully validated receipt chain, not merely a completion flag. Both per-run completion tombstones are retained. |
| Failure | Retain custody and receipts; never issue a compensating release, break the lease, infer expiry, replay or automatically clean up an ambiguous operation. A lost release response leaves the durable release-pending barrier even if the service actually released the lease. |

Deterministic capacity, candidate-name and ownership preflight runs against
the eligible ledger snapshot **before** reservation or lease acquisition.
Repeat it under the conditionally acquired lease before writing an active
mutation or entering any ARM-write callback. A preflight refusal cannot
strand a healthy run's cleanup.

If that under-lease admission check returns a definite refusal, its separate
`admission-refusal` path records unchanged authority hashes and exact
acquisition/release receipts, without entering the resource-write callback.
The terminal refusal history confers no FIC ownership. This narrow pre-ARM
path is not a generic `finally` release: unknown reads, writes, ARM outcomes
or lease/release receipts still retain the barrier. A failed refusal release
must not be treated as completed admission.
The same preflight ordering covers cleanup ownership, live credential drift
and namespace absence. Private attempt files remain even for a terminal
refusal; admitting a different operation is not automatic replay of the
refused attempt.

The **only** automatic acknowledgement reconciliation is for the final
coordination-metadata commit. It may commit despite a lost response. The
caller preserves its private `completion-unknown.json` receipt and still
reports uncertainty. A successor may independently validate the committed
full record: same UAMI/run/owner generation, original intent and resource
response, actual postcondition response, exact lease acquisition and release
requests, and their original definitive HTTP receipts. No missing reference,
Boolean, elapsed time or caller assertion substitutes for those proofs.
Unknown ARM outcome, acquisition, lease custody or release still blocks.
The successor never replays the prior mutation, and later cleanup still
requires exact live identity/spec and owned-workload removal checks.

This serializes **cooperating OAT writers on this stamp's coordinator**.
A Blob lease does not fence ARM, and ARM offers no documented exact-version
FIC deletion. Portal/admin changes can race the check and mutation. Detected
drift or broken custody is a stop condition, not permission to repair or
overwrite. Other stamps or tools must not bypass this coordinator and claim
the same guarantee.

The probe obtains an audience-scoped TokenRequest and exchanges it in memory.
It validates issuer, subject, audience and ServiceAccount UID, retries only
the documented `AADSTS70021` propagation case, and bounds the propagation
window at 300 seconds. Assertions and access tokens are neither logged nor
written to disk. A successful exchange does not prove workload RBAC or
authorize OAT experiments independently of their execution gates.

## Run-scoped QAP bindings

These are the AKS caller contracts. The local platform uses its explicit
local Kustainer path, not ambient Azure authentication or this helper.
The helper handoff and CLI integration must be qualified together.

**Provider contract qualification:** one authorized, run-owned Delta probe
established the observed `TableType=Delta`, JSON-string `Properties`, and
dynamic `ConnectionStrings` shapes. The production helper accepts only the
qualified Delta property set (`Format=Parquet`, `AutoUpdateSchema=false` and
the observed compression, encoding, header, prefix and shortcut defaults) and
one `abfss://...;managed_identity=<principal-guid>` connection. The URI is
compared with the configured run path and ARM-derived UAMI **principal ID**;
the client ID is not a substitute. Create receipts require the matching table,
Delta type, owner `DocString` and qualified properties. The observed drop
receipt instead has the matching `TableName` and null `TableType`,
`DocString` and `Properties`; cleanup also independently verifies absence.
Unknown shapes and ownership mismatches fail closed. Storage reads use API
`2023-05-01`; Kusto reads use `2024-04-13`.

The same probe's empty-log count returned zero with a successful completion
record and the typed catalog-uninitialized warning
`Severity=3`, `SeverityName=Warning`, `StatusCode=-2133196383`. The parser
classifies only that exact warning shape, retains the separate successful
completion requirement, rejects `Severity<=2` and unknown or malformed
statuses, and reports acceleration as not applied/unproven. This qualifies
provider receipts and parsing only; it does **not** prove accelerated reads,
benchmark readiness or end-to-end dataflow. Cycle E1-E5 still requires real
data and positive accelerated-read evidence. The read-only query response is
parsed separately through its validated table-of-contents ordinals; mutation
receipts remain strict single-primary management results.
Read-only provider KQL may already have run before a later failure; it must
not be reported as `DELTA_LOGS_ABSENT` or `kql_attempted=false`. Genuine absent
Delta logs retain their separate retryable handling.

| Command suffix for `npx --no-install tsx infra/azure/bootstrap.ts` | Required custody |
| --- | --- |
| `bind-oat-qap --run-id UUID --manifest PATH --record PRIVATE_PATH --workload-record FIC_PATH` | The existing ready `RunFicRecord`, after exact-ServiceAccount preparation/probe. Its namespace UID, owner token and ARM-derived cluster server are bound into QAP ownership. |
| `verify-oat-qap --record PRIVATE_PATH` | The original immutable run manifest and owned binding receipts, not reconstructed names or assumed database state. |
| `cleanup-oat-qap --record PRIVATE_PATH --workload-stop-record STOP_PATH` | Positive exact-namespace deletion and final-absence evidence matched to creation-time ownership, plus current namespace absence. **Only owned bindings may be dropped; data roots, markers and databases remain.** |

The manifest is `{schema_version: 1, run_id, tables: [{external_table,
relative_path}]}`. Names start with `oat_<UUID without hyphens>_`; paths
belong below `<configured prefix>/runs/<UUID>/dataflow/`. A SQL fixture's
suffix is `telemetry/sqlserver/<table>`, not just `<table>`. The immutable
`<configured prefix>/runs/<UUID>/_arrowcol-oat-run.json` marker binds the
run owner and canonical manifest digest. Initialize the real Delta metadata
before binding; do not synthesize schema or adopt an existing name.

The runner's private stop receipt comes only after checking namespace UID
and ownership markers, successful owned deletion with `--wait=true`, and
an empty final lookup. It carries the run, namespace UID, owner and exact
AKS/server identity originally recorded for binding. Missing or mismatched
evidence is an error; no default stop attestation is allowed. Remove QAP
bindings before final FIC cleanup, preserving both private records on error.

Kusto create-only commands and Blob coordination serialize cooperating
writers; they do not supply Kusto CAS or fence unrelated administrators.
Unknown mutation receipts and changed live bindings must stop without
replay, rebinding or destructive repair.

## Direct immutable AKS run

The direct runner is a production entrypoint, not an OAT measurement engine or a readiness waiver. It requires the existing private bootstrap environment and ARM-hydrated stamp, the durable active/armed campaign and RunInventory, the exact ready run-FIC record, a private kubeconfig, and a private release file with six immutable image references. All private files must be regular files without group/other permissions; output directories must already exist with mode 0700. The explicit kubeconfig must agree with any configured process/environment value. No secrets are emitted.

Use the stock campaign custody CLI for setup; do not start the OAT measurement engine. The
following commands are **only** for an independently cleared live run. Keep the existing
authorized campaign heartbeat fresh throughout the run and cleanup. For a new campaign:

```bash
campaign() {
  npx --no-install tsx projects/arrowcol-oat/aks-campaign.ts "$@" \
    --env-file "$BOOTSTRAP_ENV" --stamp-config "$STAMP_CONFIG"
}
campaign start "$CAMPAIGN_ID" "$CAMPAIGN_OWNER_ID"
# Use the actual start receipt's ID/owner/token as CAMPAIGN_ID, CAMPAIGN_OWNER_ID,
# CAMPAIGN_FENCING_TOKEN in the private environment.
campaign publish-inventory < "$INITIAL_INVENTORY"
campaign capture-baseline "$CAMPAIGN_OWNER_ID" "$CAMPAIGN_FENCING_TOKEN"
campaign arm "$CAMPAIGN_OWNER_ID" "$CAMPAIGN_FENCING_TOKEN"
```

Publish the initial `not_authorized` inventory with planned duration and maintenance stop
budgets, no namespace UID, and no completed cleanup steps. Record the original plan from
`bootstrap.ts plan-run-fic` before creating the namespace. Follow the
[staged YAML runbook](../../infra/azure/arrowcollector-breakthrough.yaml) for setup and the
exact FIC token probe, leaving the schedule barrier closed. After the staged readiness checks,
publish `authorization_pending`, then separately publish `authorized` after actual operator
admission, using `campaign publish-inventory < "$INVENTORY_FILE"` each time. Bind the three
observed objects and their original UIDs: `apps/v1 Deployment/arrowcol` (`collector`),
`apps/v1 Deployment/delta-maintenance` (`maintenance`), and
`batch/v1 Job/direct-load-confirmwide` (`client`). Keep `qap: null`, no official OAT
measurement reference, and bind the actual namespace UID, original FIC and checkpoints
destination `_arrowcol-custody/direct/<run-id>/<namespace-uid>/evidence`.
The direct armer proves the live identities before release; stock `assert-ready` is for
QAP-bound OAT cells, not this direct run.

The release file has exactly this shape (all six references must be `repository@sha256:<64 lowercase hex>`; `revision` identifies the workload-image build source, not the successor controller commit):

```json
{
  "revision": "<40-hex-image-source-commit>",
  "schemaFingerprint": "<reviewed-schema-fingerprint>",
  "images": {
    "writer": "<repository>@sha256:<digest>",
    "client": "<repository>@sha256:<digest>",
    "maintenance": "<repository>@sha256:<digest>",
    "sql": "<repository>@sha256:<digest>",
    "router": "<repository>@sha256:<digest>",
    "catalog": "<repository>@sha256:<digest>"
  }
}
```

```bash
npx --no-install tsx infra/azure/benchmark-direct-cli.ts arm \
  --env-file "$BOOTSTRAP_ENV" --stamp-config "$STAMP_CONFIG" \
  --fic-record "$FIC_RECORD" --release-file "$DIRECT_RELEASE" \
  --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" --clients 1 \
  --baseline-directory "$BASELINE_DIR"
```

`--clients` is an explicit workload-shape expectation; the existing armer re-reads the live Job, Pods and lifetimes and refuses a mismatch. The direct armer checks the durable campaign fence and FIC, release digests, startup config, transport and clock evidence, then writes the original arm record conditionally and verifies exact off-node bytes/ETag before any barrier release. A lost or ambiguous write/release is an error, not permission to retry.

After the original measurement interval has ended, run the terminal path with the original
run ID and the same authority, private evidence directories, FIC, release, kubeconfig and context:

```bash
npx --no-install tsx infra/azure/benchmark-direct-cli.ts finish \
  --env-file "$BOOTSTRAP_ENV" --stamp-config "$STAMP_CONFIG" \
  --fic-record "$FIC_RECORD" --release-file "$DIRECT_RELEASE" \
  --kubeconfig "$KUBECONFIG" --context "$KUBE_CONTEXT" \
  --run-id "$RUN_ID" --terminal-directory "$TERMINAL_DIR" --delta-directory "$DELTA_DIR"
```

`finish` reloads the immutable arm record, proves original-run stop and drain, seals raw
logs and Delta evidence, deletes only the matching namespace UID, then removes only its
recorded FIC. It reconciles the ordered cleanup milestones through the existing cell-step
journal and CampaignStore compare-and-swap and returns `handoffEtag` only after durable
`RunInventory.cleanup.handoff=complete`. This is a per-cell handoff, not a campaign-wide
safe-idle or throughput qualification claim. Before another cell, complete the independent
publisher-generation, cluster-scoped StorageClass, data preservation, and safe-idle
checks in the staged runbook; never delete Delta data.

Never run two `finish` processes for the same cell. After an interruption inspect the
original durable journals and current fence before resuming the same explicit run ID.
A verified seal, positive UID-fenced namespace response and completed FIC transaction
allow reconciliation without redispatch. Intent-only or otherwise ambiguous effects,
including an unresolved FIC transaction, still block; never infer success from absence,
delete by name, or substitute empty gauges for drain evidence. The live run still requires
separate execution clearance and all YAML `NOT RUN-READY` gates.

## Qualification

Source-only review is not sufficient. After clearance, run the existing
`azure-infra:verify` target and the real SQL migration/routing tests in the
leased devcontainer. In particular, qualify SQL/Rust digest parity,
permission denials despite inherited roles, concurrent initialization,
expired tombstones, missing bound pins, lost lease/ARM/write receipts,
changed remote credentials and namespace-removal ordering. Require a fresh
integrated exact-head CI result; do not reuse an earlier head's success.
Include two concurrent ready runs and independent cleanup, capacity/name
refusals preserving previous cleanup, under-lease admission races, and the
real CLI subprocess cases that enter both dynamically imported helpers.

Provider references: [FIC create/update](https://learn.microsoft.com/en-us/rest/api/managedidentity/federated-identity-credentials/create-or-update?view=rest-managedidentity-2023-01-31),
[Blob lease](https://learn.microsoft.com/en-us/rest/api/storageservices/lease-blob),
[federation limits and propagation](https://learn.microsoft.com/en-us/entra/workload-id/workload-identity-federation-considerations).
