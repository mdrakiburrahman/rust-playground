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

**Provider qualification blocker:** the current helper deliberately raises
a nonretryable `Failure` before ownership writes, lease acquisition or mutation. Official
external-table output documents an opaque `Properties` value, not a
`ConnectionStrings` column or an exact connection/authentication schema.
Authorized observed Delta table-type, property and redaction evidence is
required before implementing that parser; desired configuration cannot serve
as observed provider proof. ARM reads explicitly use Storage `2023-05-01`
and Kusto `2024-04-13`. Unset/default QAP policy and mutation receipt shapes
also remain unqualified. The command contracts below are not a claim that
the current draft can bind or clean up QAP resources.
Read-only provider KQL may already have run before this failure; it must not
be reported as `DELTA_LOGS_ABSENT` or `kql_attempted=false`. Genuine absent
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
