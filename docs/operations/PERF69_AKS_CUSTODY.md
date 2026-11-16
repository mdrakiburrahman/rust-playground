# Perf69 AKS custody watchdog

The ACA observer was not deployable: subscription policy `Network_Injection` requires a VNet-integrated ACA environment. The existing stamp VNet has only the occupied `systempool` subnet and the AKS-delegated, occupied `apiserver-subnet`. Do not add a subnet outside `infra/azure/aks.bicep` or apply the policy skip tag; either would violate the stamp IaC/policy boundary.

Custody therefore runs as a watchdog on a dedicated, separately leased devbox, never on the campaign runner's box. It uses the host's Azure CLI sign-in and the Blob-backed fenced campaign record. Keep the devbox lease renewed (maximum 4 hours) for the entire armed campaign. The watchdog is observe-only until the campaign owner explicitly starts it; no scale-up is permitted until its cleanup qualification passes.

| Invariant | Required behavior |
| --- | --- |
| Heartbeat | Owner heartbeat every 60 seconds; fence after three missed beats |
| Lifetime | Six-hour hard maximum; heartbeats never extend expiry |
| Ownership | Compare-and-swap ETags plus monotonic fencing tokens reject stale owners |
| ARM operations | Poll and reconcile operation URLs only on `management.azure.com` and the selected subscription; uncertain operations remain unsettled |
| Safe idle | Fixed system pool (autoscale explicitly off) 2×D8ds_v6, falling back to baseline 3×D8ds_v6 if any pod is unschedulable or a PDB blocks drain; bench pools 0 nodes with autoscale 0..1; Kusto at least Standard_E4ads_v5 ×2, autoscale 2..4, auto-stop on, never stopped |
| Cleanup state machine | Fencing persists ordered, stable idempotency receipts; retries resume the same step, and completion requires every step to verify |
| Cleanup adapter | Source/mocked only. Exact discovery, namespace/QAP/FIC custody and ARM rollback providers exist; runner lifecycle, full data observations and external rehearsal remain required. The watchdog still fails closed without an installed executor; keep custody unarmed. |

The helper APIs below expose the original QAP Blob custody without replacing its full ownership snapshot or positive workload-stop checks. E1 must persist the credential-free reference before load. `createDefaultCleanupProviders` composes the independent providers with explicitly supplied runner lifecycle actions; it is not a qualified, unattended default watchdog. Concrete lifecycle/recovery providers, positive full-data observations, adapter rehearsal and independent watchdog lease renewal remain gates. Keep the campaign unarmed.

Before campaign load, exercise heartbeat expiry, stale-owner rejection, repeated cleanup, and a safe-idle no-op on the watchdog box. Confirm the final ARM state and preserve the evidence before handing the lease back. A cleanup operation that is pending or uncertain is never reported as complete.

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

Use `npx --no-install tsx projects/arrowcol-oat/aks-campaign.ts cleanup-step <selector>`. This **executes** the selected provider effect; a custody-enabled runner must not also perform the matching direct Rust cleanup. Legacy noncampaign cleanup is unchanged. An unavailable provider, nonzero exit or UNKNOWN never permits fallback direct cleanup.

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

The table order is mandatory: maintenance stays on throughout load/drain; capture pre-stop evidence, then stop maintenance, then capture final logs/actual terminal receipts before namespace removal. A maintenance provider may report completion only after verifying the exact workload stopped **and** recreation is disabled, not merely after API acceptance. The default CLI does not install this missing concrete lifecycle provider.

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

Conditional per-selector journals under `_arrowcol-custody/campaigns/<campaign>/cells/<cell>/` progress from `running` to `effect-confirmed` to `complete`. A provider's positive response is persisted before the confirmed post-effect inventory CAS. Losing the write/confirmation resumes that proof without executing again. An effect whose response is unknown requires an explicit positive recovery provider; an empty journal read, listing absence or refreshed inventory is not proof of completion. Lifecycle and unknown-outcome recovery providers are **not installed by the default CLI**; those selectors fail nonzero rather than return positive stubs.

Saved-running recovery also requires a confirmed CAS against the supplied full inventory/version and pins that version through provider authority checks. Maintenance stop admits each workload only within the original absolute deadline; its authority carries the remaining allowance and rejects use after expiry. A slow/late first stop never authorizes a second stop, renews the deadline or produces a normal completion receipt. Confirmed partial provider evidence remains recoverable rather than being replayed.

Refresh using `aks-campaign.ts read-inventory`, not `status`. Strict stdin is:

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

`aks-campaign.ts capture-cell-baseline` accepts bounded JSON stdin:

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
must already exist. Terminal/final preservation and concrete default-provider installation remain separate gates.

## Campaign-terminal providers

| API | Safety boundary |
| --- | --- |
| `captureCleanupBaseline(store, azure, owner, token)` | Live producer custody, before ARM mutations/load authorization; positive idle-capacity receipts and explicit non-run Kusto table identities/counts (or positively observed empty inventory), conditional immutable baseline Blob and matching write/readback ETag. Never replace a baseline with current capacity. |
| `createDefaultCleanupProviders(store, options)` | Concrete discovery and owned namespace/QAP/FIC actions, plus ARM restoration and full-safety composition. Requires supplied lifecycle providers; no implicit success implementation. |
| `restoreSafeIdle` | Current controller only. Reconcile unresolved ARM receipts first; persist exact resource/target before each PUT/PATCH. One capacity dimension per invocation. An uncertain target is polled, never blindly reissued. Try two system nodes, retain the original three-node fallback after scheduling/PDB problems; preserve recognized writable configuration and reject unknown properties/SKU drift. Never stop Kusto. |
| `observeStampHealth` | Re-resolve the exact original ARM/server identity; require positive PodList/PDB resource versions, current PDB generation and typed readiness/scheduling observations. UNKNOWN is not healthy. |
| `verifySafeIdle` | Fresh capacity and pod observations **plus** an explicit timely run-bound `FullDataSafetyReceipt` with evidence ETag, QAP health/backlog and preserved Delta/Kusto row counts. Capacity alone always leaves full safety UNKNOWN. |

The default watchdog remains unarmed until concrete lifecycle/full-data implementations and external-adapter rehearsal pass. This source/mocked work performs no live cloud reads/mutations, load or scaling.
