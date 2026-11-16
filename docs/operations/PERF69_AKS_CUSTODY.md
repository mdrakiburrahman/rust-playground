# Perf69 AKS custody watchdog

The ACA observer was not deployable: subscription policy `Network_Injection` requires a VNet-integrated ACA environment. The existing stamp VNet has only the occupied `systempool` subnet and the AKS-delegated, occupied `apiserver-subnet`. Do not add a subnet outside `infra/azure/aks.bicep` or apply the policy skip tag; either would violate the stamp IaC/policy boundary.

Custody therefore runs as a watchdog on a dedicated, separately leased devbox, never on the campaign runner's box. It uses the host's Azure CLI sign-in and the Blob-backed fenced campaign record. Keep the devbox lease renewed (maximum 4 hours) for the entire armed campaign. The watchdog is observe-only until the campaign owner explicitly starts it; no scale-up is permitted until its cleanup qualification passes.

| Invariant | Required behavior |
| --- | --- |
| Heartbeat | Owner heartbeat every 60 seconds; fence after three missed beats |
| Lifetime | Six-hour hard maximum; heartbeats never extend expiry |
| Ownership | Compare-and-swap ETags plus monotonic fencing tokens reject stale owners |
| ARM operations | Poll and reconcile operation URLs only on `management.azure.com` and the selected subscription; uncertain operations remain unsettled |
| Safe idle | System pool 2×D8ds_v6, falling back to baseline 3×D8ds_v6 if any pod is unschedulable or a PDB blocks drain; bench pools 0 nodes with autoscale 0..1; Kusto at least Standard_E4ads_v5 ×2, autoscale 2..4, auto-stop on, never stopped |
| Cleanup state machine | Fencing persists ordered, stable idempotency receipts; retries resume the same step, and completion requires every step to verify |
| Cleanup adapter | Not yet qualified. The CLI fails closed on a fenced campaign without an injected executor; keep custody unarmed and do not start scale/load until the external stop/drain/evidence/QAP/FIC/ARM adapter is wired and exercised |

The helper APIs below expose the original QAP Blob custody without replacing its full ownership snapshot or positive workload-stop checks. E1 must persist the returned credential-free reference before load. Concrete namespace/QAP/FIC actions exist, but no complete default external action adapter is wired yet: runner stop/drain/evidence contracts, full workload/data verification, adapter rehearsal and independent watchdog lease renewal remain gates. Keep the campaign unarmed.

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

`createOwnedRunCleanupActions(store, azure, reference, privateWorkDirectory)` in `aks-cleanup-executor.ts` binds the three exact namespace/QAP/FIC actions to these helpers. It rereads current cleaning campaign/actor/fence and immutable run origin at every helper gate, compares the full binding set, and retains private local evidence on errors. QAP cleanup always uses the complete original record, never individual loose inventory bindings. FIC recovery additionally requires the original completed shared ledger and creation proof; uncertain transactions remain blocked. Completed FIC cleanup rechecks exact-resource absence and never deletes a successor. Compose these actions with positive runner lifecycle and safe-idle providers; they do not supply those missing providers or authorize arming.
