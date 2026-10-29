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

The thin hook calls `arrowcol-loadgen initialize-azure --manifest` with the
private output path supplied as `ARROWCOL_AZURE_TABLE_MANIFEST`. The loadgen owns
the canonical schemas and initializes both Direct and Dataflow pools (26 roots).
The generated manifest contains `{name, relative_path}` bindings beneath the
configured data container, not credentials or account URLs. Kusto infers the
normalized schema, timestamp companions and ingestion partitions from Delta.
Isolated cloud runs can reconcile their own bindings with
`reconcile --tables <manifest> --occupancy-id <current-owner-UUID>` without
destroying another run's data. Absolute paths, traversal and connection-string
injection in manifests are rejected.

For tables first created by live generators, `reconcile --wait-seconds 120`
waits only for HTTP-404 missing declared version-zero logs (maximum 600 seconds).
No KQL runs until every declared log is valid. Exit `3` writes a single JSON
diagnostic to stderr:
`{"status":"not_ready","code":"DELTA_LOGS_ABSENT","retryable":true,"kql_attempted":false}`.
Authorization, ownership, malformed logs and other failures are not readiness
retries and exit `1`; the caller must surface them. Success exits `0`.

KQL reconciliation enables exactly `ExternalTable,AutomatedFlows` managed-identity
usages, creates or alters inferred-schema Delta external tables, and enables QAP
with the identity's object ID. `QueryAcceleration` is not a managed-identity
usage. Only after initialization and all KQL succeed is publication unfenced.
Failure is visible and leaves the sandbox fenced, not falsely ready.

`provision --infra-only` deliberately omits Delta initialization/KQL/QAP. Its
output explicitly says that full bootstrap was **not verified**. It is useful
for diagnosing infrastructure access, not an acceptance shortcut.

## Cloud occupancy and reset

**Data-plane reset is enabled for this disposable, single-operator sandbox.**
`reset` previews; `reset --execute` runs the fenced custody sequence below;
`reset --resume` continues an interrupted cleanup from its custody record.
It refuses unless occupancy is released, the publisher registry is empty and
authority is `ready-v1`/`fenced-v1`. Known limitation: safety relies on every
mutating process participating in the publisher registry; processes outside the
cooperative protocol (arbitrary OAuth clients, other devboxes' runners) must be
stopped by the operator first. Crashed, suspended, or uncertain publishers
leave blocking registrations, with no TTL or force-unregister shortcut.

Registry metadata alone cannot activate a new protocol safely: older
reset-aware binaries ignore unknown registry fields while accepting
legacy `resetstate=ready`. Activation therefore uses versioned `ready-v1`, which
those binaries already reject, so incompatible binaries fail closed rather than publish
without registering. Arbitrary OAuth clients and binaries predating reset
fencing are outside the cooperative protocol and must be stopped before cleanup.
An uncertain server write remains an uncertain registration even if a later
transaction lookup proves logical commit success. Opaque SDK write retries must
be disabled or tracked through definitive completion; logical reconciliation is
not proof that no delayed request can still mutate storage.

### Versioned control-plane implementation

`publisher_control.PublisherControl` implements the internal Python transitions;
it is deliberately not exposed as reset execution or protocol activation.
The exact enforced states are `ready-v1`, `fenced-v1` and `cleaning-v1`. Legacy literal
`ready`/`fenced` states are rejected by this implementation; old reset-aware
clients reject the new literals. Registry headers are
`publisherregistryversion=1` and compact JSON `publishers`, a map of canonical
publisher UUIDs to `{generation: <canonical UUID>, state: active|uncertain}`.
Capacity is 16 members and 4096 UTF-8 bytes for the registry, also enforcing the
8192-byte total metadata header budget. `occupancy` is required explicitly as
`released`, `testing` or `stopping`; `testing`/`stopping` also require a
canonical `occupancyid`. Absence never means released under v1.

Every transition reads and validates ownership, generation, registry and state,
then performs one lease-guarded compare-and-set preserving unrelated metadata.
Normal admission requires `ready-v1`; admission failure or an unknown response
never grants mutation permission or silently adopts a previous registration.
Fencing preserves the generation and member set so existing members can clean
drain; new admission is blocked. Rotation requires the same fenced generation,
released test occupancy and an explicitly empty registry.

Cleanup authority is persistent, not a snapshot. `drained()` is only a
read-only diagnostic; an empty fenced registry plus ETag is never deletion
authority. `claim_cleanup` takes `fenced-v1`, released occupancy and an empty
registry, then in one CAS sets `cleaning-v1` with the cleanup operator as the
sole member. Deletion starts only after that definite success and each step can
re-verify with `cleanup_operator`. While `cleaning-v1` holds, normal and
bootstrap admission, fencing, competing cleanup claims and member release are
rejected. `finish_cleanup` requires definitive deletion completion and, in one
CAS, removes the operator, rotates to a fresh generation and returns to
`fenced-v1` with an empty registry.

Bootstrap custody belongs to the single Rust child initializer that
`initialize-delta.sh` launches. The child admits its own fresh UUID under
`fenced-v1`, released occupancy and an empty registry; its registration makes
the registry non-empty, so a later cleanup claim fails. Cleanup and bootstrap
claims therefore serialize through one container ETag and cannot both mutate.
The Python parent never adopts the child's UUID. The child removes itself only
after definitive physical drain; on failure or unknown drain it stays registered
or uncertain and blocks the parent. The child never sets `ready-v1`. After a
clean child exit the parent admits its own fresh UUID through the same bootstrap
rule for the generation captured before the launch, before any manifest, Kusto
managed identity, external table or query acceleration I/O. If cleanup claimed
or rotated in the idle gap, that admission fails before any I/O. The parent
re-verifies with `bootstrap_owner` before each step, holds its member through all
hosted-state work, and `complete_bootstrap` removes that member and sets
`ready-v1` in one CAS only after every request completed definitively. An unknown
admin outcome keeps the member active or marks it uncertain, so it failsclosed. There is no destructor release, expiry, force clear, or automatic retry.
An uncertain member or operator cannot release even if logical transactions
reconcile. Unknown CAS responses return `UnknownTransition`; retain the
identity and reconcile authority read-only rather than claim success or adopt a
possibly applied registration.

#### Lease-guarded metadata compare-and-set

Set Container Metadata accepts only `If-Modified-Since` as a condition, so an
`If-Match` ETag is not a compare-and-set. Every protocol writer, Python and
Rust, uses this exact sequence instead:

1. Acquire a 15-second container lease with a fresh proposed lease
   ID. `409` or an ambiguous acquire is busy and writes nothing.
2. Read authority metadata under the lease and compare its content with the
   caller's earlier read. A mismatch releases and writes nothing.
3. Write ALL metadata with `x-ms-lease-id`. Success is applied; `412` is
   definitely not applied; any other failure is an unknown outcome.
4. Release the lease best-effort; expiry bounds an unreleased lease.

A container lease does not reject metadata writes that omit a lease ID, so the
proof covers only cooperating writers. All Python lifecycle writes (provision
fence, occupancy, stop and publisher transitions) use this path. Existing
checkpoint containers are never rewritten by Bicep, and arbitrary older tools
remain outside the proof and must be stopped. The lease serializes only the
short read-modify-write; it never expires or releases publisher membership.

#### Public v1 entrypoint wiring

`provision`, `reconcile`, `occupy`, `release` and `stop` use only v1 authority.
Legacy or unversioned metadata (`resetstate=ready`/`fenced`, or no
`publisherregistryversion`) is refused with no implicit migration.

- **New checkpoint container:** Bicep creates it once with `fenced-v1`, an empty
  registry, `occupancy=released` and a fresh random `resetgeneration`.
- **Existing container:** redeployment writes no container metadata, so all
  existing authority state is preserved.
- **Full bootstrap order:**
  1. The Rust initializer receives the captured generation as
     `ARROWCOL_EXPECTED_RESET_GENERATION`. It admits its own UUID, drains it,
     and unregisters it.
  2. The parent then admits a fresh bootstrap UUID before any cluster start or
     KQL. If cleanup or a stop hold won the gap, this admission fails before I/O.
  3. Cluster start and KQL reconciliation run under that member.
  4. `complete_bootstrap` atomically removes the member and publishes `ready-v1`.
  5. Provision exports `ARROWCOL_RESET_GENERATION` only after it re-reads
     `ready-v1` at the same generation.
- **Ready redeploy and released `reconcile`:** one leased transition claims
  exclusive operator custody, moving `ready-v1`, released occupancy and an empty
  registry to `fenced-v1` with exactly one operator member. Deploy, cluster start
  and KQL run under it. While it holds, `occupy` (needs `ready-v1`), admission,
  cleanup and stop all fail closed. `complete_bootstrap` returns to `ready-v1`.
  If `occupy` wins first, the claim fails before any I/O.
- **`reconcile` under test occupancy:** requires the captured generation
  (`--reset-generation` or `ARROWCOL_RESET_GENERATION`). It admits a normal member
  whose exact occupancy owner and generation are checked inside the admission
  transition, so a release/re-occupy by another owner fails it before I/O. The
  member then blocks that owner's release. Live collectors stay registered.
- **Failure handling:** `NotReady` (exit 3) cleanly releases the member. Any
  other failure, or an unknown start or admin outcome, marks the member uncertain.
  An uncertain member keeps blocking stop and cleanup until an operator reconciles it.

Arbitrary lease-less or old tools and concurrent first provisioning remain
outside the proof.

**Recovering a stranded sandbox.** Publisher UUIDs are per process, so a crashed
or killed run can leave `occupancy=testing` and active/uncertain members that no
live process will ever clean-release; `release`, `provision` and `reset` then
refuse. The explicit operator escape hatch is:

```bash
python3 infra/azure/bootstrap.py recover            # preview
python3 infra/azure/bootstrap.py recover --execute  # stop local writers, fence, rotate
python3 infra/azure/bootstrap.py reset --execute    # data written by the failed run is untrusted
python3 infra/azure/bootstrap.py provision          # back to ready-v1
```

`recover --execute` requires the generation-bound local stop receipt, waits the
60-second quiescence interval, then in one leased CAS sets `fenced-v1`, rotates
`resetgeneration`, clears the registry and releases occupancy. Rotation means a
zombie writer's membership and generation no longer match, so it is rejected
rather than silently readmitted. It refuses `cleaning-v1` (use
`reset --resume`) and a cluster `stopping` hold, before stopping anything. Stop
writers on every other devbox sharing the sandbox first.

The test runner acquires shared occupancy before live tests and releases it only
after its owned writers/consumers stop successfully:

```bash
python3 infra/azure/bootstrap.py occupy --occupancy-id "$RUN_UUID"
python3 infra/azure/bootstrap.py release --occupancy-id "$RUN_UUID" \
  --reset-generation "$ARROWCOL_RESET_GENERATION"
```

`occupy` requires `ready-v1`. `release` requires the same three conditions in
one leased transition, or occupancy is retained:

- the exact UUID owner;
- the generation captured at occupancy (`--reset-generation`, or
  `ARROWCOL_RESET_GENERATION` from the environment);
- an empty publisher registry. Any active or uncertain member fails release,
  with no force clear.

Provision and reset reject any non-released occupancy. `stop --execute` first
moves `released` to `stopping` with a fresh hold UUID in one lease-guarded
compare-and-set, before any ARM call. That same transition also requires an
empty registry and `ready-v1`/`fenced-v1`, not `cleaning-v1`, so any registered
bootstrap or normal member blocks stop. Every admission rejects `stopping`. A
competing `occupy`, bootstrap or reconcile either wins first, so stop fails
without calling ARM, or is rejected while the hold exists. The
hold is released only after this process's own definite ARM submission and an
observed `Stopped` state. An unknown compare-and-set, submission or wait
outcome, including an Azure CLI timeout (which kills the whole CLI process
group), leaves a blocking hold. A later `stop --execute` never replays or
releases an existing hold, because the old request may still land late; it
needs explicit operator reconciliation. A definite compare-and-set conflict
writes nothing and stays retryable. Lifecycle authority is metadata on the preserved checkpoint
container, not a blob: `sandboxid`, `resetstate`, `resetgeneration`, `publisherregistryversion`,
`publishers`, `occupancy` and `occupancyid`. Updates use the lease-guarded
compare-and-set above. Writers fail closed unless ownership matches and
`resetstate=ready-v1`, checking before open, upload and each
commit attempt. A restarted writer must not bypass a fenced sandbox.

Successful full provision and `occupy` export the exact ready container
`resetgeneration` as `ARROWCOL_RESET_GENERATION` in private mode-`0600`
`.env.azure`. `occupy` re-reads authority after its CAS before writing this
admission value. The runner must reload the file after occupancy and pass the
value to both collector pools; it must never derive it from a run/occupancy UUID.
`config` alone cannot export an authoritative generation because it performs no
Azure reads.

```bash
# Preview only: no mutation.
python3 infra/azure/bootstrap.py reset
# Fence, stop the local loadtest, quiesce 60s, delete databases and all blobs.
python3 infra/azure/bootstrap.py reset --execute
# Continue an interrupted cleanup from .env.azure.reset.json.
python3 infra/azure/bootstrap.py reset --resume
# Restore the declared baseline (ready-v1).
python3 infra/azure/bootstrap.py provision
```

Execution verifies exact ownership, HNS, disabled retention, the exact database
allowlist, and the complete container allowlist/ownership. It persistently fences
remote writers, stops the local workspace-owned loadtest, waits a 60-second
quiescence interval, inventories again, drops the owned database, and
deletes every owned blob, snapshot and version, including Delta/service
checkpoints. It refuses preexisting soft-deleted objects rather than claiming
hidden retained data is gone. Unknown resources cause failure, not deletion.

**The 60-second interval is not independently a fencing proof.** Safe destructive
reset additionally requires every integrated publisher to have a verified
whole-publication bound covering credential acquisition (including the Azure CLI
child process), retries, and outstanding storage writes. HTTP request timeouts
alone do not bound authentication: a publisher stalled in an unbounded CLI
refresh can otherwise resume after deletion. The core's bounded/cancellable
credential provider and stalled-auth regression close that specific auth-stall
case, but do not prove a hard whole-operation or server-completion deadline:
scheduling, serialization, and already-sent server effects remain relevant.
Do not execute reset against active or potentially suspended remote publishers.
Stop and establish quiescence for every publisher sharing the sandbox; the
local runner's stop receipt is evidence only for that workspace, not other
devboxes. The 60-second delay cannot replace that distributed lifecycle evidence.
Offline reset tests establish orchestration ordering, not that cross-process
publication deadline or live Azure safety.

Cluster, identity, account and both containers survive. Verification requires
zero databases and literally zero blobs and HNS paths (including empty
directories) in both containers. Container control
metadata remains fenced without leaving a marker blob. A subsequent full
`provision` recreates the declared Delta/Kusto baseline and returns it to ready.
The foundation's explicit empty-root initializer is the only bootstrap path that
can create schema roots while fenced; normal publishers have no bypass.

### Operator path

The following describes the operator sequence behind `reset --execute` and
`reset --resume`.

- **Running gate.** Reset never starts or stops the cluster. It refuses a
  non-`Running` cluster before fencing. If a released `stop` wins during
  quiescence (fenced, empty registry), the post-claim check observes the
  non-`Running` cluster and `abort_cleanup` returns `cleaning-v1` to `fenced-v1`
  at the same generation, before any deletion. Once cleanup owns the
  authority, `stop` is refused before any ARM call.
- **Generation-bound stop receipt.** The runner stop command receives the
  captured generation as `ARROWCOL_RESET_GENERATION`. It must print
  `{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true,"resetGeneration":G}`.
  A receipt without that exact lowercase generation is rejected. The receipt
  is workspace-local evidence. Registry closure remains the drain proof.
- **Custody record.** Before the claim CAS, the operator UUID, the captured
  generation, the precomputed successor generation and the sandbox are
  written to the gitignored, mode-`0600` `.env.azure.reset.json`.
  - An exclusive `flock` on `.env.azure.reset.json.lock` is held from the
    record read through publish, claim and cleanup. A second reset or resume
    process is refused.
  - The record is published create-if-absent with `link()`, never `replace()`.
    The file is fsynced, then the parent directory. A durability failure
    propagates before the claim CAS.
  - Removal requires byte-identical canonical content and fsyncs the parent.
    A loser never overwrites or removes a record it does not own.
  - A fresh sequence refuses while that record exists. Resume first re-fsyncs
    the parent directory (durability repair) before any CAS.
  - Resume first re-reads the authority.
  - If the successor generation is already present, the earlier finish
    applied, and resume only removes the record.
  - If the authority is fenced and empty at the recorded generation (the claim
    never applied), resume re-claims with the same operator UUID.
  - Otherwise resume requires `cleaning-v1` with exactly that one active
    operator.
  - Foreign, uncertain, extra or other-generation members are refused, never
    taken over or cleared.
  - An unknown claim, delete or finish keeps the record and custody.
- **Observe-first deletion.** Every step re-inventories the physical state. An
  applied blob DELETE whose response was lost is absent and is not reissued.
  An unapplied one is reissued for the same object.
  - Base blobs and HNS paths are deleted only with `If-Match` on the
    inventoried ETag. A stale in-flight request that lands late therefore
    gets `412` against any successor object at that path.
  - Snapshots and versions are addressed by immutable identity.
  - Objects without an ETag are refused before any deletion.
  - Unknown stop/start outcomes are never replayed by reset.
- **Database DELETE intent and provider receipt.** The ARM database DELETE has
  no precondition, so a stale request landing after rotation and reprovision
  could remove the successor database. Absence in a listing therefore never
  reconciles a submission.
  - Before submitting, the operator durably publishes a private
    `.env.azure.reset.database.json` intent (`submitting`) bound to its
    operator UUID and generation.
  - Only a definitive response advances it: `200`/`204`/`404` to `completed`,
    or `202` with an `Azure-AsyncOperation` URL to `accepted`. A
    `Location`-only response is treated as unknown.
  - The operation URL must be `https://management.azure.com/...`, with no
    user info, port, fragment, dot segments or unsafe characters. It is
    re-validated on every resume, before any token is fetched.
  - An accepted intent is polled read-only and becomes `completed` only on an
    explicit `Succeeded` status. A missing, malformed, non-string or
    unrecognized status (for example `Queued`) is unknown: the accepted
    intent and custody are retained, never cleared or resubmitted.
  - A transport failure, crash or other response leaves `submitting`. That
    custody is explicitly **unresumable**: no finish, rotation, rebootstrap or
    resubmission, and the record and `cleaning-v1` are retained for manual
    reconciliation. Timeouts are not claimed to prove anything.
  - Only an explicit provider `Failed` or `Canceled` removes the intent, so
    resume may resubmit once more under the same custody.
  - A `Deleting` database without this custody's intent, or an intent from
    another operator or generation, is refused and never adopted.
  - Blob-only cleanup stays resumable as before.

### Writer inventory (enablement gate input)

| Writer | Published source | Reset-relevant tests | Status |
| --- | --- | --- | --- |
| Rust core publisher session | `7307a94` (`publisher.rs`, `delta.rs`) | `bootstrap_is_exclusive_and_generation_pinned`, `stopping_occupancy_blocks_all_admission`, `stale_generation_refuses_write_and_cleanup`, `uncertain_outcomes_retain_member`, `azure_admission_requires_configured_generation`, `unknown_shared_admission_is_permanent` | Offline pass |
| `arrowcol-rust`, `arrowcol-dataflow-rust`, `delta-maintenance` binaries | `7307a94` (`close_shared` on exit) | Core `clean_drain_deregisters_only_self`, `inflight_upload_refuses_close_until_settled` | Offline pass |
| Tenant stream (sink and checkpoint) | `5dd2c85` | Shared admission across sink and checkpoint | Offline pass |
| Azure copy/rename | `7307a94` | `azure_server_side_copy_is_unreachable` | Unsupported before I/O |
| Collector replay | `8b82339`, `590a329` | Replay epoch fencing | Actual WAL drain **pending** |
| Loadgen `initialize-azure` child | `89d3eee`, `9c01c6d`, `d618bb7` | `initialize_azure_dispatch` (6 missing/non-canonical expected-generation cases rejected before any I/O); debug-only `initialize_control` over the core `test-control` seam: shared 26-table body with stale-G/cleaning/ready/stopping/live-member refusal before any blob, drain before manifest, unknown write retains member; parent child-env, `initialize-delta.sh` argv, CLI parse and Python manifest/log validation composed (no Kusto) | Component offline pass; the release child process, its Azure control/data wire and `run_tree` handling remain **unverified until live Azure qualification** |
| Python parent (bootstrap/reconcile/release/stop) | this directory | `test_bootstrap`, `test_publisher_control` | Offline pass |

Offline passes are orchestration evidence only. Public enablement still
requires the pending rows plus live qualification.

After live tests release occupancy, the lifecycle owner stops (never deletes)
the cluster:

```bash
python3 infra/azure/bootstrap.py stop
python3 infra/azure/bootstrap.py stop --execute
```

The executing command waits until ARM reports `Stopped`. Reprovisioning starts a
stopped cluster under a registered member and waits for real Kusto readiness.

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

The initializer is a cross-workstream contract, not an independent schema
implementation in this directory. The integrated loadgen/core revision must be
qualified before claiming full bootstrap. A missing/failed initializer never
produces a ready sandbox.

Public protocol references:

- [Delta external tables](https://learn.microsoft.com/en-us/kusto/management/external-tables-delta-lake)
- [Managed identity policy](https://learn.microsoft.com/en-us/kusto/management/managed-identity-policy)
- [QAP policy](https://learn.microsoft.com/en-us/kusto/management/alter-query-acceleration-policy-command)
