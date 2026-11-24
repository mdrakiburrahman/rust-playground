# Perf69 AKS custody watchdog

The ACA observer was not deployable: subscription policy `Network_Injection` requires a VNet-integrated ACA environment. The existing stamp VNet has only the occupied `systempool` subnet and the AKS-delegated, occupied `apiserver-subnet`. Do not add a subnet outside `infra/azure/aks.bicep` or apply the policy skip tag; either would violate the stamp IaC/policy boundary.

Custody therefore runs as a watchdog on a dedicated, separately leased devbox, never on the campaign runner's box. It uses the host's Azure CLI sign-in and the Blob-backed fenced campaign record. Keep the devbox lease renewed (maximum 4 hours) for the entire armed campaign. The watchdog is observe-only until the campaign owner explicitly starts it; no scale-up is permitted until its cleanup qualification passes.

| Invariant | Required behavior |
| --- | --- |
| Heartbeat | Owner heartbeat every 60 seconds; fence after three missed beats |
| Lifetime | Six-hour hard maximum; heartbeats never extend expiry |
| Ownership | Compare-and-swap ETags plus monotonic fencing tokens reject stale owners |
| ARM operations | Poll and reconcile operation URLs only on `management.azure.com` and the selected subscription; uncertain operations remain unsettled |
| Safe idle (legacy stamps) | Fixed system pool (autoscale explicitly off) 2×D8ds_v6, falling back to baseline 3×D8ds_v6 if any pod is unschedulable or a PDB blocks drain; bench pools 0 nodes with autoscale 0..1; Kusto at least Standard_E4ads_v5 ×2, autoscale 2..4, auto-stop on, never stopped |
| Cleanup state machine | Fencing persists ordered, stable idempotency receipts; retries resume the same step, and completion requires every step to verify |
| Cleanup adapter | Source/mocked only. Default CLI/watchdog providers install exact discovery, bounded lifecycle/journal recovery, namespace/QAP/FIC custody, ARM rollback and independent Delta/non-run Kusto preservation. External rehearsal remains mandatory; keep custody unarmed. |

The helper APIs below expose the original QAP Blob custody without replacing its full ownership snapshot or positive workload-stop checks. E1 must persist the credential-free reference before load. `createDefaultCleanupProviders` installs concrete lifecycle/recovery and full-data observation by default; injection is optional for tests. External adapter rehearsal and independent watchdog lease renewal remain gates. Keep the campaign unarmed.

Selection retains the exact, case-sensitive subscription/cluster/location binding
as a SHA-256 fingerprint; no runtime override selects this topology.
For the reviewed Canada Central breakthrough topology, capacity-idle requires exactly
five pools: the existing system pool at 3×D8ds_v6 with autoscaling pinned at
3..3, and benchd64, benche64, supportd64 and arrowe192 at zero with autoscaling
0..1. Kusto must independently meet the same idle requirements. The campaign
baseline binds a separate ARM receipt to each pool. Fallback restoration for
this profile is observation-only; drift remains pending for separately authorized
reconciliation rather than writing to any pool or Kusto. Capacity-idle is not
proof of workload or data safety.

Before campaign load, exercise heartbeat expiry, stale-owner rejection, repeated cleanup, and a safe-idle no-op on the watchdog box. Confirm the final ARM state and preserve the evidence before handing the lease back. A cleanup operation that is pending or uncertain is never reported as complete.

## Selected configuration and campaign baseline

Every `aks-campaign.ts` invocation requires `--env-file <private-env> --stamp-config <stamp.yaml>`.
The separate watchdog requires the same flags:
`npx --no-install tsx projects/arrowcol-oat/aks-watchdog.ts --watch --env-file <private-env> --stamp-config <stamp.yaml>`
(or `--once`). Neither CLI guesses a stamp or accepts a missing selection.
The existing validated env/YAML loaders are shared with bootstrap; ambient variables retain bootstrap precedence.
`ARM_SUBSCRIPTION_ID` must equal the selected subscription; `CAMPAIGN_STORAGE_ACCOUNT` and
`CAMPAIGN_STATE_CONTAINER` must equal the selected account/checkpoints container. Producer identity environment
must match the start receipt. These inputs contain identities, not embedded credentials.

| Action (append both configuration flags) | Requirement/result |
| --- | --- |
| `start <campaign-uuid> [owner-uuid]` | Starts observe-only custody; retain its producer identity environment. |
| `publish-inventory` | Persist initial `not_authorized` inventory before provisioning. Advancing beyond initial preparation requires the matching campaign baseline, even while admission remains `not_authorized`. |
| `capture-baseline <owner-uuid> <fencing-token>` | First capture requires observe mode, null namespace UID, planned/unobserved resources/FIC/QAP and entirely unstarted cleanup with null deadlines/reasons. Freeze verified idle ARM capacity and non-run Kusto identity/count/policy before provisioning/arm/load; any inventory/version change during capture fails closed. |
| `arm <owner-uuid> <fencing-token>` | Requires matching durable campaign baseline; this is effect authorization, not a rehearsal/dry run. |
| `publish-inventory` with `authorized`, `assert-ready`, `capture-cell-baseline` | Independently require the matching campaign baseline; absence, drift or incomplete non-run Kusto provenance fails closed. |

`capture-baseline` returns one JSON line:
`{ok:true,receipt:{status:"captured",campaign_id,owner_id,fencing_token,blob,etag,captured_at_unix_ms}}`.
`blob` is the raw key `_arrowcol-custody/campaigns/<campaign-uuid>/baseline.json`, not a URL.
`etag` versions that immutable Blob, not inventory. `CampaignBaselineReceipt` is exported by
`aks-default-providers.ts`; errors use the existing machine-code envelope. Controller cleanup continues to
validate the baseline's original producer origin/fence, not the controller's newer fence; it never recaptures it.
An existing-proof retry is read-only and remains valid after provisioning, load authorization or controller
takeover using the current actor/fence; it does not require the advanced inventory to look initial again.

## Owned helper APIs

All commands below use `npx --no-install tsx infra/azure/bootstrap.ts` and the selected `--env-file <private-env> --stamp-config <stamp.yaml>`. The plan/reference/recovery commands are read-only against Azure; recovery only creates private local files.

| Command | Result |
| --- | --- |
| `plan-run-fic --run-id <cell-uuid> --namespace <run-ns> --service-account <sa>` | JSON `RunFicPlan`: `version`, `runId`, `namespace`, `serviceAccount`, `identityId`, `resourceId`, `clientId`, `tenantId`, `aksId`, `server`, `credentialName`, `issuer`, `subject`, `audience`. Resolves the configured UAMI through ARM before any run namespace or credential creation. |
| `prepare-run-fic --run-id <cell-uuid> --namespace <run-ns> --service-account <sa> --record <fic.json> --identity-plan <plan.json>` | Existing FIC preparation, additionally rejecting any planned identity drift before mutation; the resulting private record retains the same identity fields. **Creates run-scoped trust; campaign mutation authorization is still required.** |
| `oat-qap-reference --record <qap.json>` | JSON `OatQapReference`: `schema_version`, `account`, `container`, `blob`, `run_id`, `nonce`, `manifest_sha256`, `namespace_uid`, `namespace_owner_token`. Requires exact agreement with the original durable shared snapshot. |
| `recover-oat-qap --reference <reference.json> --record <recovered-qap.json>` | Fetches and validates the full original `RecordV1`, owner marker, exact run/environment and lease receipts. Never overwrites different local custody. |
| `recover-oat-qap-stop --reference <reference.json> --workload-stop-record <stop.json>` | Recovers the positive original-UID deletion proof and original-workload stop receipt; absence alone is insufficient. |
| `observe-custody-capacity --read-only` | Exact stamp AKS, system/benchmark pool and Kusto ARM GETs with original successful response hashes/request IDs. Reports `capacity_idle` or `capacity_not_idle`; missing/invalid observations fail nonzero as UNKNOWN. Always reports workload/data verification as `unknown`, never overall stamp safety. |

Identity-plan and reference inputs must be bounded private regular files, never symlinks. `--identity-plan` is limited to 64 KiB; QAP inputs retain the existing 4 MiB limit.

The watchdog imports `recover_oat_qap_record`, `delete_oat_qap_workload`, `recover_oat_workload_stop` and `cleanup_oat_qap` from `infra/azure/oat_qap.ts`. `delete_oat_qap_workload(azure, reference, recordPath, stopPath, authority)` verifies the original namespace UID and both ownership labels/annotations and uses Kubernetes `DeleteOptions.preconditions.uid`. The conditional, credential-free `_arrowcol-oat-workload-stop.json` Blob advances through `intent` (before DELETE), `responded` (typed affirmative response, before absence polling), then `complete` (positive response plus confirmed absence). Another host restores the journal into the same guarded helper and polls absence without resending DELETE. An intent without a positive response remains UNKNOWN even if the namespace later appears absent. Only `complete` can produce a recovered workload-stop receipt. `authority.assertCurrent()` must enforce the controller's current cleaning token, not the immutable producer token.

`cleanup_oat_qap(azure, recordPath, stopPath, authority)` reuses all existing full-snapshot, connection, schema, policy, environment and positive workload-stop checks. It can resume an affirmatively held original root lease only with controller authority; ordinary producer calls still require released custody. A controller can recover `cleaning` only when intent is null and every entry is fully evidenced `ready` or `dropped`; confirmed drops are not replayed. Recovery validates the original create/policy/drop receipts, including process loss after the final drop before `cleaned`. Acquisition/release ambiguity, incomplete bindings, unresolved mutation intents and drift remain explicit blockers; there is no lease break, inferred success, broad deletion or Delta-data deletion.

`createOwnedRunCleanupActions(store, azure, reference, privateWorkDirectory, setupPlan?)` in `aks-cleanup-executor.ts` binds the three exact namespace/QAP/FIC actions to these helpers. It rereads the current invocation actor/fence and immutable run origin at every helper gate, compares the full binding set, and retains private local evidence on errors. Producer cleanup stays active and requires a live heartbeat; controller cleanup requires the current cleaning fence. QAP cleanup always uses the complete original record, never loose inventory bindings. FIC recovery additionally requires the original completed shared ledger and creation proof; uncertain transactions remain blocked. Completed FIC cleanup rechecks exact-resource absence and never deletes a successor.

## Setup loss and exact discovery

Persist `fic.plan` before prepare, each exact workload intent before apply, `qap.reference` once available, and credential-free `evidence_destination` before capture. Attached plans/references/destinations are immutable. Namespace `owner_token` always remains the producer owner, never the controller's newer fencing token.

| State | Required proof |
| --- | --- |
| QAP not planned (`qap=null`) | Supply the original `RunFicPlan` as `setupPlan`; namespace/FIC cleanup does not require or fabricate a ready QAP/ServiceAccount record. |
| Namespace deletion before QAP | Exact original UID, namespace labels and annotations, and the conditional namespace-only journal `_arrowcol-custody/namespaces/<run-id>/<namespace-uid>.json`; `intent`/`responded`/`complete` retain the same no-uncertain-DELETE-replay rules. Invalid completion receipts are rejected before writing. |
| FIC created before runner loss | Recover its original affirmative shared-ledger creation proof and completed transaction; require the positive namespace-stop journal before deleting that exact credential. |
| Planned FIC never created | Exact authoritative credential GET plus no unresolved shared-ledger intent; persist the original exact absence receipt, not fabricated creation/removal success. |
| Workload UID known | Exact API/kind/name/namespace and persisted UID inside the exact owned namespace. |
| Workload UID unknown | Additionally require both labels **and** annotations `arrowcol-oat/run-id` and `arrowcol-oat/owner-token` to match the immutable run/origin before adoption. |
| QAP planned but no recoverable reference | UNKNOWN; no prefix discovery, inferred ownership, loose cleanup or success fallback. The original full custody reference/recovery boundary must be resolved. |

`discoverOwnedRunInventory` supports the explicitly inventoried Deployment (`apps/v1`), Job (`batch/v1`) and Pod (`v1`) objects only. It never lists broadly for adoption/deletion. Permission/API failure is not absence; terminal absence receipts retain their original identity/time.

## Per-cell effect CLI and refresh

Use `npx --no-install tsx projects/arrowcol-oat/aks-campaign.ts cleanup-step <selector> --env-file <private-env> --stamp-config <stamp.yaml>`. This **executes** the selected provider effect; a custody-enabled runner must not also perform the matching direct Rust cleanup. Legacy noncampaign cleanup is unchanged. An unavailable provider, nonzero exit or UNKNOWN never permits fallback direct cleanup.

| Exported `CellCleanupSelector` | Provider action |
| --- | --- |
| `stop_admission` | `stop-admission` |
| `bounded_drain` | `bounded-drain` |
| `evidence_capture` | `preserve-evidence` |
| `stop_maintenance` | `stop-maintenance` |
| `terminal_capture` | `record-terminal-state` |
| `remove_namespace` | `remove-run-namespace` |
| `remove_qap` | `remove-run-qap` |
| `remove_fic` | `remove-run-fic` |
| `handoff` | `cell-handoff`: verify prior per-cell receipts and all lifecycle/resource terminal proofs, then confirm the final cell CAS. No hidden deletion, capacity restore or campaign completion. |

`cleanup.stop_maintenance` is a required `InventoryStep`, initially `not_started` with null deadline/reason. Legacy inventories missing it fail closed. Running maintenance stop requires an absolute Unix-millisecond deadline; both executors persist its progress under the current actor/ETag. A completed milestone is monotonic and is required for handoff.

The table order is mandatory: maintenance stays on throughout load/drain; capture pre-stop evidence, then stop maintenance, then capture final logs/actual terminal receipts before namespace removal. The installed default maintenance provider reports completion only after verifying the exact workload stopped **and** recreation is disabled, not merely after API acceptance.

The exact `CELL_CLEANUP_SELECTORS`, `CellCleanupEnvelope`, `CellCleanupReceipt`, `ReadInventoryEnvelope` and `ReadInventoryResult` are exported by `aks-cell-cleanup.ts`. All stdin is bounded to 1 MiB.

```typescript
// cleanup-step input
{ schema_version: 1, inventory: RunInventory, expected_etag: string,
  actor: { mode: "producer" | "controller", owner_id: string, fencing_token: number } }
// cleanup-step success
{ ok: true, receipt: { status: "complete", step: CellCleanupSelector,
  campaign_id: string, owner_id: string, fencing_token: number,
  orchestration_run_id: string, cell_run_id: string, namespace_uid: string | null,
  actor: { mode: "producer" | "controller", owner_id: string, fencing_token: number },
  completed_at_unix_ms: number, etag: string } }
```

Receipt top-level owner/fence identify the immutable producer; nested `actor` identifies the invocation. Before effects, authority, exact durable inventory and expected ETag must match. Controllers additionally require the matching current outer running step/key (`handoff` is guarded by outer `verify-safe-idle`, not a call to campaign-terminal restoration). Normal producer cleanup changes only per-cell inventory and never transitions active to cleaning.

Conditional per-selector journals under `_arrowcol-custody/campaigns/<campaign>/cells/<cell>/` progress from `running` to `effect-confirmed` to `complete`. A provider's positive response is persisted before the confirmed post-effect inventory CAS. Losing the write/confirmation resumes that proof without executing again. An effect whose response is unknown requires an explicit positive recovery provider; an empty journal read, listing absence or refreshed inventory is not proof of completion. Default providers are installed, but missing identity, journal, physical-stop or preservation proof still fails nonzero. Handoff independently verifies data preservation after removals; it never restores capacity or ends the active campaign.

Saved-running recovery also requires a confirmed CAS against the supplied full inventory/version and pins that version through provider authority checks. Maintenance stop admits each workload only within the original absolute deadline; its authority carries the remaining allowance and rejects use after expiry. A slow/late first stop never authorizes a second stop, renews the deadline or produces a normal completion receipt. Confirmed partial provider evidence remains recoverable rather than being replayed.

Refresh using `aks-campaign.ts read-inventory --env-file <private-env> --stamp-config <stamp.yaml>`, not `status`. Strict stdin is:

```typescript
{ schema_version: 1, campaign_id: string, orchestration_run_id: string, cell_run_id: string,
  actor: { mode: "producer" | "controller", owner_id: string, fencing_token: number } }
```

Success is exactly `{ok:true,inventory,etag}` from one validated durable read. The configured campaign environment, live producer/current cleaning controller, immutable origin and exact requested run/cell must match. No writes or provider effects occur. Replace the runner cache after confirmed step success or a pre-effect stale-version refusal; do not reconstruct TypeScript deadlines. This read does not resolve UNKNOWN effects or authorize replay.

Both CLIs return one JSON error line, nonzero: `{ok:false,code:"campaign_fenced"|"stale_version"|"write_outcome_unknown"|"invalid_or_storage_error",message}`. Only a provably pre-effect version/CAS refusal is `stale_version`, permitting bounded refresh and reevaluation. Conflicts after journal/effect dispatch remain UNKNOWN, never refresh-and-replay.

**Controller-only terminal expiry:** `recordUnsettledDrain` admits only the current armed cleaning controller, positive admission stop, the immutable run/namespace UID and an already-expired, non-extendable drain deadline. It first durably retains a failed/uncertain drain with reason `drain_deadline_expired_unsettled`, then confirms a distinct `unsettled` outer receipt binding the original producer, current actor, deadline and actual observation time. Unknown Delta/Kusto row counts remain null. Restart reuses that receipt; unrelated failures, stale actors and unexpired deadlines cannot advance.

Only this validated expiry can pass the drain guard. Remaining evidence, maintenance stop, terminal capture, exact removals and full safe-idle verification remain mandatory and ordered. Final state is `complete_unsettled`, cleanup status `verified_with_unsettled`, and handoff stays **failed** with the expiry reason. It is not a successful drain, preservation measurement or qualified cell. Producer cleanup has no expiry shortcut; per-cell handoff cannot emit normal success for this path. The watchdog reports distinct terminal progress and exits with code 2 rather than spinning or reporting clean success.

## Pre-load preservation

`RunInventory.load_admission` is required: `not_authorized` -> `authorization_pending` -> `authorized`.
The first inventory must record `not_authorized`, planned resources and no namespace UID before provisioning.
Transitions cannot skip, regress or change after fencing. Only confirmed authorization permits client preparation,
preflight or start; QAP absence alone never proves no rows were admitted.

`aks-campaign.ts capture-cell-baseline --env-file <private-env> --stamp-config <stamp.yaml>` accepts bounded JSON stdin:

```typescript
{ schema_version: 1, campaign_id: string, orchestration_run_id: string, cell_run_id: string,
  expected_etag: string, hot_delta_relative_path: string,
  actor: { mode: "producer", owner_id: string, fencing_token: number } }
```

The explicit path must exactly match one persisted planned QAP binding, with a known namespace UID.
Creating a reference requires `not_authorized`; retry reads the immutable original rather than replacing it.
Success is `{ok:true,receipt}` with `status:"captured"`, the exact campaign/producer/run/cell/namespace/path,
`blob`, strong baseline `etag`, `delta_state:"table"|"observed_absent"`, `table_id`, `delta_version` and `rows`.
The baseline ETag is not the inventory ETag. Normal error codes above apply.

Real tables require fresh log/checkpoint replay, exact metadata/protocol/file identities and known add-stat row coverage.
Positive absence requires two complete successful exact-prefix listings with distinct storage request IDs.
An absent table has null identity/version/rows, never zero; permission errors and missing history are not absence.
Absence is not a throughput or row-delivery proof. The campaign's immutable capacity/non-run Kusto reference
must already exist. After physical writer stop, terminal capture freezes a new immutable reference before removal.
Final replay independently protects both that reference and the original baseline, rechecks prior-cell reference chains,
and observes preserved non-run Kusto identities/counts and healthy, non-growing QAP backlog. Both QAP samples
must match baseline enabledness; an enabled policy needs both comparable samples and a healthy final sample.
Missing comparison evidence is UNKNOWN, never safe idle. Run-QAP pre-DROP
observations remain historical; their post-removal count is null with reason `run_owned_qap_removed_by_design`.
Missing stats/history and PURGE without a separately trusted frozen retention policy remain UNKNOWN.

## Concrete lifecycle and terminal recovery

Exact Deployment/Job stops bind the original namespace/workload UID and current resourceVersion. Deployments
must have zero replicas, an observed generation and no live selected pods, with no matching HPA; Jobs must be
suspended with positive terminal/suspension state and no live owned pods. Selector/controller ownership is checked;
no broad workload discovery or deletion is used. Blocking Kubernetes commands consume the remaining persisted budget.
Sanitized log receipts retain exact pod/container identities, hashes, byte counts and allowlisted numerical samples;
unstructured text and unapproved fields are redacted. Actual observed container exits/OOM status are retained.
A missing terminal observation is explicit evidence loss, never a successful drain.
Physical-stop proof requires exact spec/status coverage for regular, init (including native sidecar) and ephemeral
containers, with every container terminal. Missing, duplicate or unrecognized coverage fails closed. Stop,
exact-state recovery and terminal Delta freeze share this proof.

Only a current controller with durable unsettled-drain proof and completed admission/maintenance stops can stop
residual collectors inside `terminal_capture`. This has its own persisted 120-second deadline, never resets or
extends the expired drain deadline, and never deletes resources. All residual workload plans are journaled before
effects; every PATCH uses UID/resourceVersion tests and a version-pinned authority check. An uncertain terminal
PATCH can recover only from positive exact physically stopped state, labelled as state recovery rather than an API
response; it is never blindly reissued. Late responses remain evidence, not successful budget completion.
Terminal Delta is frozen only after collector and maintenance writers are physically stopped with recreation disabled.

## Shutdown finalization

| Boundary | Behavior |
| --- | --- |
| Required order | Stop admission → bounded drain → evidence capture → maintenance stop → freeze → fenced terminal-summary inputs → attach final AKS Delta reconciliation → guarded create-only finalization → validation → namespace, QAP and FIC removal → handoff. The bounded pre-drain snapshot remains diagnostic. |
| Exit 0 | Requires the actual measured post-teardown reconciliation, settled zero ambiguity, clear teardown admissions and matching independently read frozen Delta row counts. A zero exit alone never authorizes release. |
| Exit 143 | Always unavailable; an exact marker permits only provisional physical stop/freeze. Release and handoff stay withheld. |
| Early failure | Stop/freeze checks still apply. Authoritatively missing pre-load Blob gives `no_baseline`, nullable missing references and an immutable unavailable record; never invented zero rows, namespace UID or terminal observations. Release stays withheld. |
| Missing measurements | An existing baseline with missing reconciliation rows is `reconciliation_unavailable`, not `no_baseline`. Storage/authentication errors are not missing-baseline observations. |
| Unknown write | Pre-read existing artifact; guarded canonical-equality readback only. Never repeat a conflicting or uncertain PUT. Finalization cannot settle or replay a running/UNKNOWN removal journal. |

The three CLI actions are `shutdown-finalization-inputs`, `shutdown-finalize` and
`shutdown-finalization-read`. Inputs/read require exactly
`schema_version:1,campaign_id,orchestration_run_id,cell_run_id,expected_etag,actor`.
Inputs return `schema_version,inventory_etag,terminal_capture,delta_before,delta_terminal,unavailable_reason,terminal_summaries`;
source references are `{blob,etag,sha256}` or explicit null, with canonical-JSON hashes.
Finalize additionally accepts those exact nullable references and
`reconciliation:{profile:"single_hot_table"|"other",proof:null|{status,post_teardown_verification,
expected_rows,observed_rows,settled_zero_ambiguity,teardown_admissions_clear}}`.
Proof is the authenticated producer's actual measured reconciliation, not a client-supplied match flag.
TS also checks frozen row-count differences; this is not positive exit-143 continuity proof.

Final/read receipts retain immutable origin, actor, creation inventory ETag, source references and verdict identities,
plus `status:"validated"|"unavailable"|"failed",reason_code,release_allowed,reconciliation` and the final artifact reference.
All fields remain present, including nulls. Finalization journal I/O has its own bounded 30-second action window;
it never extends or restarts physical stop/freeze deadlines. Read-only recovery preserves the creation ETag after later
inventory progress. Current actor/fence and inventory version are checked throughout.
Known non-143 failures retain only the existing validated controller unsettled-cleanup exception; never a successful cell.
No membership, follower or positive exit-143 API is installed.

### Exit-zero terminal summary

`terminal_summaries` is required-present and nullable, from the same fenced terminal-capture reference:
`[{pod_name,pod_uid,container,restart_count:0,exit_code:0,summary}] | null`.

| Contract | Requirement |
| --- | --- |
| Producer record | Exactly one `arrowcol_terminal_evidence` JSON record per original collector container, emitted only after drain/final acknowledgement. Summary keys: `schema_version:1,admitted,committed,failed,cancelled_ambiguous,pending,committed_rows,incomplete,commits`. |
| Units | Distinct publication IDs offered downstream, including rejected sends. `admitted = committed + failed + cancelled_ambiguous + pending`; every counter is a nonnegative JS-safe integer. |
| Positive accounting | `failed=cancelled_ambiguous=pending=incomplete=0`; `incomplete` is numeric `0\|1`. Unknown/replayed attribution, races, overflow and missing summaries are never fabricated as zero. |
| Commit entries | Exact `{table_id,version,rows,files,parquet_bytes}`; table ID is the existing lowercase SHA256 of the writer's normalized URL, not raw table data. Numeric totals and every unique version/row/file/byte tuple must match independently replayed frozen Delta. Entire-lifetime/pre-baseline or duplicate tuples refuse validation. |
| Source completeness | Full log read without `--tail`/`--limit-bytes`, below the established native 64 MiB log cap (`COLLECTOR_TERMINAL_LOG_MAX_BYTES`), receiver startup preceding terminal record, complete final line, and exact fenced pod UID/container with zero restarts observed before and after read. Missing/rotated/partial/duplicate/malformed or inconsistent evidence yields null. Existing custody-Blob/Delta history bounds still apply; oversized workloads are refused, not truncated. |
| Capture timing | Read original pods while draining, before deletion, and recheck at terminal observation. Persist only typed accounting, hashes and existing identity provenance, never raw logs or configuration. New stop journals mark `terminal_evidence_version:1`, preventing a missing new summary from falling back to legacy acceptance. |
| Table binding | Only the independently configured ABFSS account/container/root URL with the writer's trailing slash is supported. Scheme aliases or normalization uncertainty are not equated; unequal hashes withhold release. |

Delta JSON history reads use one named `DELTA_READ_CONCURRENCY=4` cap, with ordered replay after concurrent
fetches. One absolute 35-second read deadline covers listing, all batches and retries; terminal capture takes the
earlier persisted physical deadline. Finalization rereads share the stricter existing 30-second journal action deadline,
not a fresh budget per replay. Expiry is unavailable/UNKNOWN, never empty/zero evidence or a restarted budget.
CPU quota, QAP validation, mandatory process/commit capture and immutable journal fencing remain mandatory.

## Campaign-terminal providers

| API | Safety boundary |
| --- | --- |
| `captureCleanupBaseline(store, azure, owner, token)` | Live producer custody, before ARM mutations/load authorization; positive idle-capacity receipts and explicit non-run Kusto table identities/counts (or positively observed empty inventory), conditional immutable baseline Blob and matching write/readback ETag. Never replace a baseline with current capacity. |
| `createDefaultCleanupProviders(store, options)` | Installs concrete lifecycle, discovery, owned namespace/QAP/FIC actions, ARM restoration and independent full-data observation. Injection is optional for tests; no implicit success implementation. |
| `restoreSafeIdle` | Current controller only. Reconcile unresolved ARM receipts first; persist exact resource/target before each PUT/PATCH. One capacity dimension per invocation. An uncertain target is polled, never blindly reissued. Try two system nodes, retain the original three-node fallback after scheduling/PDB problems; preserve recognized writable configuration and reject unknown properties/SKU drift. Never stop Kusto. |
| `observeStampHealth` | Re-resolve the exact original ARM/server identity; require positive PodList/PDB resource versions, current PDB generation and typed readiness/scheduling observations. UNKNOWN is not healthy. |
| `verifySafeIdle` | Fresh capacity and pod observations **plus** timely run-bound independent preservation evidence and strong evidence ETag. Null run-QAP post-DROP counts are not converted to zero. A setup-only no-load disposition requires fenced `not_authorized`, no planned QAP and original exact namespace/FIC removal proofs. Capacity alone always leaves full safety UNKNOWN. |

The default watchdog remains unarmed until paired source review and external-adapter rehearsal pass. This source/mocked work performs no live cloud reads/mutations, load or scaling.
