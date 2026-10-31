# Dedicated local PoC bridge v1

`live-v1.schema.json` is the unchanged closed packet contract. `live_contract.py`
validates it and supplies server-owned command-fence checks. `live_server.py`
implements the bounded authenticated HTTP transport, read-only by default; `live_runtime.py`
projects an explicitly configured dedicated Linux/Docker runtime into it.
These sources are not permission to connect live mutations. Transport tests use
real HTTP with synthetic observations; observer source tests use a fake Engine.
Neither is live deployment acceptance.

The bridge is a loopback-only, same-origin local service for one explicitly
registered dedicated Linux/Docker deployment. It serves the production frontend
and `/api/live/v1` on the same origin; the static simulation/recorded frontend
does not discover or connect a bridge automatically. No CORS wildcard, public
binding, arbitrary URL, path, shell, container name or credentials are accepted
from the browser. Actual container IDs, start identities, network/volume labels,
authority credentials and target-to-writer mappings are verified server-side.

## Transport

| Method and route | Request | Response |
| --- | --- | --- |
| `GET /api/live/v1/capabilities` | none | `capabilities` |
| `GET /api/live/v1/snapshot` | none | sole registered deployment's read-only `snapshot`, even before selection |
| `GET /api/live/v1/feed?after=<revision>` | canonical decimal cursor | `feed` |
| `POST /api/live/v1/selection` | `select` | `selection` |
| `POST /api/live/v1/arm` | `arm` | `armed` |
| `POST /api/live/v1/disarm` | `disarm` | `selection` with `armed:false` |
| `POST /api/live/v1/commands` | `command` | `operation` |
| `GET /api/live/v1/operations/<operation_id>` | bounded ID | `operation` |

Bodies are JSON. Requests are at most 32 KiB and depth 16; responses at most
1 MiB. Feed responses contain at most 64 revisioned full snapshot events;
clients apply each as a replacement, not a patch. An empty feed is allowed.
Only one bounded request is outstanding; polling has a 1-second minimum interval
and a 5-second transport timeout. The server independently enforces connection,
request rate, response-size and operation-ledger bounds.

Feed cursors are bridge-incarnation scoped and consecutive (+1 for each item);
`next` equals the last item's revision, or `after` when empty without a gap.
A missing retained revision,
regression, unknown cursor or deployment replacement returns `gap:true`,
`resnapshot:true`, `items:[]`. Clients stop controls, disarm and fetch a new
snapshot; they never fill gaps by simulation. Disconnection, a bridge restart,
resource replacement or stale observations invalidates arming.

Revisions, epochs, versions, generations and policy revisions are opaque strings
or decimal strings as specified by the schema; never convert counters to JS
Number. Writer generation is an opaque identity, not a counter. Snapshot age is a
server-local monotonic duration. The browser also expires displayed freshness
5 seconds after its own successful receive; it does not subtract wall clocks.
Unknown data remains null/unknown. Table IDs alias immutable TableKeys, not
schemas. Accepted receipts are not completed Delta publication.

## Authentication and browser boundary

The server generates a fresh local session secret and one-time bootstrap grant.
The operator opens a bridge-issued loopback URL with fragment
`#arrowcol-grant=<opaque>` (32-512 base64url characters);
the frontend consumes and removes the fragment immediately, then sends it once
to `POST /api/live/v1/session` with `{ "grant": "<opaque>" }`. The server consumes
it once, returns `{ "csrf": "<opaque>" }`, and sets a short-lived HttpOnly,
SameSite=Strict, host-only session cookie scoped to `/api/live/v1`. The cookie
uses Secure on HTTPS; the dedicated HTTP loopback exception is never public.
Session grants/CSRF are transport secrets, excluded from packet exports, logs,
URLs after bootstrap and localStorage. They are not runtime credentials.
The frontend does not generate an invented credential or expose authority/SQL/
Docker secrets. Bootstrap/session/logout transport bodies are not dataset packets.

Every endpoint requires the authenticated cookie after bootstrap. Every POST,
including bootstrap, requires exact configured Origin and rejects absent/null
Origin. After bootstrap every POST additionally requires `X-Arrowcol-CSRF`
matched to that server session. GET requires same-origin browser metadata;
cross-site requests are denied, not enabled by CORS. Host is exact allowlisted
loopback host/port, preventing DNS rebinding. No bearer query tokens.
`POST /api/live/v1/logout` clears the cookie, selection and arm.
Authenticated same-origin `GET /api/live/v1/session` returns `{ "csrf": "<opaque>" }`
for reload/resume, with the same browser-metadata checks and `Cache-Control:
no-store`; it never returns the cookie, bootstrap grant or runtime credentials.
It invalidates existing selection/arm and never selects or arms. Thus reload
starts read-only and does not require reusing a one-time grant.

No client is required to send `authenticated`, `qualified`, `connected`,
`supported`, a monotonic timestamp or an arm deadline. These are server-owned
facts. Recorded files can contain no authorizing session material, and importing
any dataset cannot select a deployment or arm a session.

## Selection, commands and reconciliation

Capabilities default to disconnected, unqualified and all operations unsupported.
The server operator registers only one dedicated deployment; the UI still must
explicitly select its exact ID/incarnation/current revision. Selection alone is
read-only. Arming requires a fresh snapshot, correctness qualification, selected
identity and explicit `confirm:"dedicated-local-poc"`; its maximum TTL is 30s.
Commands carry the server-issued selection/arm IDs, exact deployment and snapshot
revision. Targeted policy commands additionally carry exact resource incarnation,
collector/process/generation and policy revision. New policy ACKs are not
inferred from requested revisions.

`admission.enabled:true` means **block new streams**, not enable admission.
Exclusion, admission block, retirement and migration are separate operations.
Retirement never directly stops a collector; teardown requires exact retirement
approval, no owned/tentative/attached assignment, resolved completion/recovery
and ACTIVE successors. Approved deletion cannot be canceled or the execution
resource reused while deletion may still occur.

The schema lists bounded possible actions, not promises of support. Capabilities
enable only individually implemented and qualified operations. Initial limits
are 1-4 collectors, 1-3 gateways, up to 256 workload requests/30 seconds and
15-second maximum faults. Role-specific lower limits are enforced server-side.
Insufficient capacity rejects/blocks a change, never bypasses recovery.
Faults require a separate bounded rollback supervisor independent of the HTTP
request/session and a durable repair journal. Until that exists, faults remain
unsupported. Emergency reconciliation/repair may use a separately authenticated
local operator path, never a generic disarmed browser bypass.

Workload is unsupported unless capabilities list its enabled operation and an
exact `workload_presets` entry. Each server-owned preset fixes the immutable table
alias, schema variant, stream concurrency, rows/request and request/duration
limits. The client submits only the profile ID and bounded request/duration
counts, never a schema, SQL, arbitrary table or stream URL. A missing preset,
unqualified table/schema or excess bound is denied. Editing arbitrary schema,
stream or table definitions is unsupported in v1. A fixed evolved-schema preset
may exercise the qualified evolution path; it does not grant catalog authority.

Snapshots always include a bounded `metrics` array (possibly empty with missing
coverage explicit). Each sample names an exact listed resource incarnation and
process-local clock, offset, duration, closed metric/unit, decimal-string value
or null, method and evidence class. CPU is process/container counter delta in ns,
memory an end-of-interval gauge, IO counter deltas, and queue-age local elapsed
time. No CPU sample is per-request allocation. Throughput is derived over its
declared interval; unknown samples use `evidence:unavailable`, null and
`method:not-observed`. No wall-clock subtraction, percentile summation or
allocation of full shared work to every request is permitted. Empty telemetry
never means zero. Clock restart creates a new clock identity.

The client creates one operation ID before submitting. The server durably records
the normalized deployment/action and requested revision before any effect.
Same ID/same action returns the original ledger record even if response was lost;
same ID/different action returns conflict. Reconciliation GET is read-only and
does not require a live arm. After a timeout, query the same ID; never invent a
new operation or replay effects. Unknown operations return `unknown-operation`,
not success. Requested/pending/applied/completed/failed/unknown remain distinct;
applied does not imply receipt/Delta completion. Restart reloads the operation/
rollback journal, invalidates sessions/arms, and reconciles before reporting.

Errors use the closed `error` packet without raw exception text. HTTP 400 is
invalid, 401 unauthenticated, 403 Origin/CSRF/disarmed, 409 stale/wrong-target/
conflict, 413 limit, and 503 disconnected/unqualified/unsupported. A known command
failure is still observable in its durable `operation` record.

`live_ledger.py` implements bounded durable intent/operation records and
lost-response reconciliation without effect execution. It stores no session or
arm credentials. The read-only transport implements one-time bootstrap, bounded cookie sessions,
Origin/Host/browser-metadata/CSRF checks, reload selection invalidation,
read-only selection, consecutive full-snapshot feeds, operation reconciliation
and allowlisted static production assets. It binds only IPv4 loopback; a private
new bootstrap file holds the grant URL, never stdout or an export.
The dedicated runtime evidence directory is owner-only before setup creates
credentials; private configuration and raw runtime artifacts are never served
from the frontend root.
Startup requires explicit immutable `ARROWCOL_SQL_IMAGE` and
`ARROWCOL_RUNTIME_IMAGE` pins matching locally available qualified images.
It inspects them before creating a deployment and never implicitly pulls the
legacy harness defaults or substitutes another image.

The explicit observer verifies created container IDs and deployment labels.
Resource restart changes the public resource incarnation and resets CPU deltas;
unknown counters remain null. CPU and memory are container-scoped measurements
using observer-local monotonic intervals, never per-request allocation.
The metric process alias identifies the observed container cgroup, not the
collector writer process or an exclusive charge to that writer. Counter resets
produce unavailable CPU intervals, break continuity and retain dropped/error
coverage; valid memory gauges remain available. Failed observation attempts are
counted in the next successful snapshot while the old snapshot ages stale.
Approved retirement is still draining while its exact container is running;
approval alone is not teardown completion.
IO, queue and throughput observations remain explicitly missing.
A router-hosted authority is represented once
as its gateway resource, not duplicated into two CPU charges.

Without an explicit matching `--policy-proof`, all HTTP action capabilities and
arming remain unsupported, `qualified:false`. The independent backend SQL/Delta
gate alone never enables effects. The frontend client must keep its independent
mutation gate false until the corresponding real bridge/browser acceptance.

`live_executor.py` now supplies a serialized policy-effect core for exclusion
and admission only. An explicitly qualified instance enables only those HTTP
capabilities. It durably reserves
normalized intent before mutation, rechecks freshness/arming after preflight,
checks the exact execution resource and full writer, and uses the authority's
atomic expected policy revision. Other desired policy fields are preserved;
retiring/approved resources are rejected. Native operation identity is stable
across lost responses. Reopening its ledger and reconciling performs reads,
never automatic policy replay. `applied_revision` comes from a fresh matching
public snapshot, not an unrelated native policy counter; completion additionally
requires the exact-incarnation collector ACK. This is policy completion, not a
claim about receipt/Delta completion.

Dispatch has one global effect slot and one worker; another operation is rejected
before reservation, not silently queued. An authorized duplicate of the same
intent can read its existing record while that slot is occupied. HTTP submission
returns the durable state without waiting for SQL or ACKs. Reconciliation GET
schedules bounded reads only. Unexpected worker failure remains unknown and
disables new effects. The final effect check uses the current server session and
snapshot, not a copied arm: logout, disarm, expiry, replacement, gaps and reload
during preflight can prevent the effect. Resource identity checks do not wait
for optional CPU/memory telemetry. An observation revision may legitimately
advance between reads and selection/arm/command; a typed stale pre-effect denial
requires a fresh snapshot, not relaxed fencing or replay of uncertain work.

Separate direct real SQL/collector qualification binds the exact executor,
ledger, guard/schema sources, seven native binaries and immutable local images.
Actual HTTP qualification subsequently verified all four policy mutations and
collector ACKs, admission denial before receipt, exclusion preserving owned-table
streams and nine exact ordinary Delta rows. It deliberately discarded an HTTP
response, interrupted the native response after SQL mutation, killed the actual
bridge process and reconciled the original durable ID after process restart,
without a second policy mutation. That is API acceptance, not browser mutation
acceptance or receipt-feed qualification.

`live_deployment.py` saves an owner-only, no-overwrite, fsynced ownership manifest
and acquires an exclusive process lease. Explicit `--resume` requires the same
deployment/subnet/arm, exact created container IDs/start identities/images,
unchanged qualified local binaries, authority binding and matching policy proof
when effects are requested. It never discovers or adopts similarly named
containers. Credentials remain in the private role configuration. Deployment
identity survives restart; bridge identity, grants, sessions, selection and arm
do not. The durable ledger persists snapshot revision watermarks, including
compare-and-set publication, so applied/requested revisions do not regress after
restart. Fresh observation is still required before support or arming.

The Linux launcher requires `umask 022` or equivalent native bind-directory
permissions; the evidence root itself remains owner-only. Normal shutdown
closes the observer/executor/ledger and stops only the exact owned deployment.
`--retain-runtime-on-stop` is an explicit bounded restart facility, not the
default and not a fault rollback supervisor. Faults, repair, workload, replica
changes, retirement and migration remain unsupported. Independent bounded fault
rollback remain unfinished; neither
HTTP policy acceptance nor full-process restart qualifies those surfaces.

## Live acceptance and physical completion

`live_receipts.py` now projects the dedicated SQL authority's bounded accepted
prefix and final receipt rows using a fixed read-only query against the exact
owned SQL container. SQL credentials stay in private files/stdin; neither the
query nor a database/container identifier is supplied by the frontend. SQL
acceptance alone emits `completion:accepted`, even if the Delta log is already
visible but its final SQL receipt is not yet durable.

`completed` additionally requires exact proposal/final-receipt digest and
version agreement, immutable action-log bytes, data-file lengths/SHA-256 and
contiguous verified prefixes, plus an ordinary Delta reader reconstructing that
version with the exact accepted file set. The ordinary reader checks snapshot
metadata, not a full row multiset on every poll; real qualification separately
scans the exact marker multiset. Native unit IDs are preserved in
`receipt.operation_id`; they are not invented browser-command IDs.

Physical verification caches are reused only while device/inode/size/mtime/ctime
and expected content identities remain unchanged. A probe hashes at most
32 MiB, uses bounded SQL/reader processes and exposes at most 128 receipts in a
balanced per-table window. Omitted history retains explicit missing/drop
coverage. Reader errors, missing/corrupt physical evidence, budget exhaustion
and SQL failures never become completed or zero: unknowns and coverage gaps are
preserved. When the receipt component fails, prior receipt identities remain
visible as unknown while independent CPU/memory observation continues.

Real authenticated read-only qualification held publication before log install
and before final receipt persistence. Both phases remained accepted, not
completed. After release, four receipts, bounded live feeds and nine exact
ordinary Delta rows reconciled; the server exited 0 and all eight owned
containers stopped. This does not enable workload/fault controls or qualify
browser mutation acceptance.

## Independent fault-supervisor qualification

`live_faults.py` adds a private, separately journaled Linux supervisor. It is
**not wired to HTTP capabilities yet**. Its closed mapping permits only collector
or gateway pause, publisher pause as `publisher-interrupt`, and dedicated SQL
pause as `authority-loss`. Requested duration is bounded to 3,000 ms; this is
the fault deadline, not a claim that Docker rollback has zero latency or always
works when Docker itself is unavailable. Each native control call is bounded;
failed compensation remains `repair-required`, never successful recovery.

Intent is durable before dispatch. The child checks the exact saved container,
start identity, image and deployment ownership. Only after preflight does its
live parent send a one-use authorization byte; a dead/disarmed parent before
that handshake cannot start a fault. After admission the separate process owns
the monotonic deadline and compensates without the HTTP process. Reopening an
uncertain record only repairs; it never repeats pause. Preexisting pauses and
replaced incarnations are not adopted. The owner-only journal and exclusive
worker lease also support narrowly scoped local `--repair-only` recovery.

Real isolated direct-supervisor qualification exercised all three fault kinds,
actual bridge SIGKILL followed by independent rollback, and actual supervisor
SIGKILL followed by exact-resource emergency repair. Three existing ordinary
Delta rows survived, eight exact containers stopped and all workers exited.
This does not qualify HTTP dispatch, browser effects, in-flight workload
completion, or measured maximum physical pause duration. Those checks remain
required before advertising fault support.

## Server-owned workload helper qualification

`live_workload.py` supplies fixed `main`/baseline presets, not an arbitrary
schema/table/endpoint API. Small uses three rows/request, at most eight requests
and 10 seconds; elephant uses 65,536 rows/request, at most two requests and
15 seconds. Both have one stream. The nonnullable baseline schema, native
fingerprint and exact loopback gateway/container are checked before dispatch.
Credentials and native headers remain server-side.

Private workload records share the FULL-synchronous operation ledger and retain
logical request/attempt IDs, single-send decisions, native unit IDs, RPC outcomes
and final-receipt identities. A native unit is recorded as soon as received.
Existing jobs are never uploaded again; missing unit identity stays unknown.
Partial unsent work is cancelled rather than silently resumed. RPC final/SQL
acceptance is not job completion: all requested units need fresh matching
physically verified receipt observations. RPC timeout/error and eventual
completion remain separate facts in the private record.

Separate actual helper qualification completed both presets and an in-flight
request during real publisher pause, reconciled the reopened journal without
replay, and matched 65,548 ordinary Delta marker rows. It measured three
inclusive fault lifecycles on the same Linux-boot CLOCK_MONOTONIC, from
pre-pause dispatch through observed rollback: 0.681 s and 0.697 s for 500 ms
requested faults, and 2.235 s for the 2,000 ms publisher fault. All were below
the explicit requested-duration-plus-five-seconds qualification bound. These
are control lifecycles, not CPU or cross-process latency estimates. HTTP
workload/fault capabilities remain unsupported pending shared-executor and
guarded HTTP qualification; frontend mutation authority remains disabled.

## Shared HTTP executor checkpoint (not full qualification)

The shared `EffectExecutor` preserves bounded admission, durable operation IDs,
single-send workloads and separate requested/pending/completed outcomes. Normal
startup requires both a matching policy proof and an additional exact-source,
seven-binary effects proof; omitting it leaves faults/workloads unsupported.
The effects verifier accepts the existing native-helper proof shape or a
separately identified guarded-HTTP proof with five applied/repaired fault cases,
actual parent/worker loss checks, four physically completed workload units,
ordinary Delta reconciliation and exact stopped cleanup. Old proof hashes
cannot activate changed sources. There is no production qualification bypass.

Fault workers now attach only the selected resource saved in the immutable
manifest and private journal: deployment incarnation, closed role/fault mapping,
container ID/start/image/ownership and bounded intent must match. They never
execute native binaries or load endpoint credentials. The parent retains its
full deployment/source/binary verification and final live session, snapshot,
policy and resource fences. A one-use live-parent handshake is required before
initial pause only. Deadline rollback and `--repair-only` need the durable exact
resource identity, not a surviving parent, current UI revision or armed session.
No freshness window, observer cadence, duration cap or retry budget was relaxed.

HTTP-effects3 rejected four collector faults stale before application: measured
read-only full child attachment took 2.045 seconds against one-second snapshot
updates. Selected-resource startup subsequently measured 0.252 seconds in one
read-only probe; this is diagnostic startup cost, not a performance benchmark
or a maximum. HTTP-effects4 completed collector and SQL faults on their first
intents, plus the existing policies and fixed small/elephant workloads. Its
publisher pause applied and rolled back, but the fixture incorrectly treated
HTTP `pending` as already observed native `paused`; that operation remained
pending, not completed. The corrected fixture waits on the same journal intent
without replay. Eight exact containers stopped and the endpoint closed. Current
parent-loss regressions pass in source tests, but this failed HTTP attempt did
not reach the actual parent/worker-kill checks or final combined Delta audit.
It produced no complete current-source effects proof or browser qualification.

The subsequent source-identical HTTP-effects5 attempt completed the publisher
overlap and real preflight-parent-loss rejection, then failed a private fixture
PID lookup: Linux records child processes under their spawning executor thread,
not necessarily the main thread. HTTP-effects6 corrected only that lookup,
deduplicated the known bridge's task children, required a unique exact worker
script/project/operation argv match and used pidfd-bound signaling.

HTTP-effects6 passed four native policies and lost-response/crash reconciliation,
all three fault variants, preflight parent death without application, worker
SIGKILL/emergency repair, post-application owner SIGKILL/independent rollback,
same-ID no replay, four physical workload units and the exact 65,554-row ordinary
Delta multiset. Its five local monotonic fault lifecycles passed the unchanged
requested-duration-plus-five-seconds bound. Final bridge exit was 0; eight exact
containers stopped and the endpoint closed. The private bootstrap qualifier is
explicitly distinct from normal source-gated startup.

Normal `live_runtime.py` then accepted that current eleven-file/seven-binary
artifact without any evidence-gate override, completed a real authenticated
small workload and collector fault, and verified three exact ordinary Delta
rows plus reload/logout. The normal smoke's final retention assertion failed:
the baseline has seven running roles and an intentionally stopped eighth legacy
collector fixture, not eight running roles. Its failure and safe stopped cleanup
remain separate; normal source-gated effect success is not browser availability
or full live-game qualification. Preserve the exact baseline running-state map
for bridge-only restart, never start/adopt that stopped fixture.

## Corrected browser closeout and unadvertised next contract

The corrected normal8 smoke retained the exact seven-running/one-stopped
baseline and passed without a production evidence-gate override. The parent's
subsequent actual browser acceptance was observation-only: seven real stations,
exact selection, live CPU/memory and consecutive feeds, session/reload/logout,
no arm/commands or credential storage. Graceful exact-process teardown exited
0, all eight owned resources stopped and port 18786 closed. Preserve normal7's
failed assertion separately; read-only browser success is not mutation acceptance.
See HANDOFF.md for separate evidence identities and the retained observer error.

The implemented schema `ddb3f534b8dab2507d0293c9cd035a2c017bf38a1c124a9cdd8b01346c4c3cf3`
and semantics/46 synthetic fixtures are in session
`files/live-safety-streams-draft/`. They require nullable exact-probe replica
safety facts, nullable current receipt-probe health and caller workload streams
bounded by each preset's qualified maximum. They preserve common authority
fences, explicit diagnostic-only missing labels, unknown completion and
independent rollback. Older `5923cc7e...` packages and qualification artifacts
remain frozen historical evidence, not compatible consumers or authorization
for this source.

`live_inventory.py` now overlays the immutable birth manifest for observation,
attachment, selected fault attachment and exact cleanup. Creation intents,
one-use dispatch, nonce/config binding and lost-response reconciliation are
durable; unknown creation is never retried under a new identity. Binding alone
does not mean readiness. The stopped legacy fixture is birth evidence only,
never managed staff. An approved collector tombstone
cannot be silently reset or reused: initial 3-to-4-to-3 must be labeled finite,
with exhausted fresh identity explicit until a new identity/config path exists.
Gateway removal still needs authoritative admission stop and observed drain.
The current exact-source `collector-add-small` qualification enables only the
initial ADR collector increase from three to four and single-request small
workloads. It requires explicit `--creation-proof`; default startup is read-only.
It is mutually exclusive with policy/effects qualification. `replicas` accepts
exactly `role=collector, desired=4`: decrease, other roles and other desired
counts remain unsupported even though the generic packet schema admits them.
After slot4 is bound, replica availability becomes unsupported/exhausted; this
is not repeatable staff capacity or permission to reset credentials/tombstones.
Retirement, migration, repair, faults, admission/exclusion and elephant workload
are not granted by this proof.

Both the advertised workload operation maximum and its preset
`maximum_requests` are **1** in this scope. Server validation rejects
`requests>1` and `streams>1` before admission; values are never clamped.
The two singleton jobs in capacity qualification are not qualification of a
multi-request job or concurrent clients. The retained capacity3 case requested
two attempts, sent one, then left the second unsent after its original revision
became stale; the operation reconciled as cancelled. The multi-request workload
and eventual fresh collector identity/configuration path remain unfinished.

Required replica `identity_verified`, `running` and `paused` are nullable facts
from exact current private probes. Public resource aliases never establish
identity. Snapshot `receipt_observation_known` is true only after a successful
current receipt observation; healthy empty is explicit, not a missing-field
default. SQL state, current receipt existence and explicit JSON receipt array
share one bounded SERIALIZABLE observation transaction. Missing/changed fields,
orphan receipts, reader failures and stale probes keep health/completion
unknown and deny workload admission. Accepted SQL or RPC final alone is not
physical completion.

Common auth, Origin/CSRF, selected deployment/incarnation, exact latest revision,
arming, gaps and source gates still apply. Required safety/policy/writer facts
must be known. Only `cpu`, `memory`, `cpu-counter-gap`, `io-read`, `io-write`,
`queue-depth`, `queue-age` and `throughput` are diagnostic-only missing labels;
all remain visible. Any other missing fact denies. Workload additionally
requires receipt health exactly true and every advertised gateway verified,
running and unpaused; the server independently preflights its exact gateway.
Selected fault initial application still requires the live parent handshake;
durably exact deadline/emergency compensation does not require parent, bridge,
inventory or a surviving UI revision. The dynamic fault path is source-tested,
not a fault grant from the creation proof.

## Proposed admitted-workload lease: not implemented

This is a separate contract proposal, not a change to revision freshness,
authorization or the single-request capability above. Agreement and dedicated
regressions/native qualification must precede implementation.

Admission would require the exact latest revision, explicit current arming,
source/deployment/resource identities and authoritative preflights. A durable
bounded job lease would retain operation/request/attempt identities, the
qualified request/stream/time bounds, immutable TableKey/schema, gateway and
authority bindings, policy dependencies, and a versioned safety footprint.
Pure diagnostic samples/feed sequence advances would not themselves revoke
that already admitted lease. Safety changes, missing critical observations,
authority/incarnation/policy mismatch, explicit disarm/logout/expiry, bridge
loss and reconciliation gaps would forbid further sends. Each new attempt
would still recheck authoritative dependencies and current receipt health.
Already accepted attempts would reconcile exact receipts without replay;
unsent, cancelled and unknown attempts would remain distinct.

Cold assignment binding and permitted writer transitions need explicit
agreement: no inferred writer grant, silent resource replacement or implicit
lease renewal. A restarted bridge would reconcile durable attempts, not resume
unsent payloads automatically. Concurrent streams/clients require a separately
qualified bounded scheduler and durable inventory, not a larger advertised
maximum.

Required regression scenarios include diagnostic-only advancement during a
cold first RPC, explicit disarm and expiry between attempts, pause/policy/writer/
authority replacement, failed or racing receipt observation, lost response and
bridge death with no replay, requested/sent/completed/cancelled accounting, and
the actual advertised request/stream bounds. These scenarios are proposed;
no passing evidence for the lease is claimed.

## Normal browser blocker and review-only revision proposal

The source-gated opt-in capacity candidate has **not** passed live browser
mutation acceptance. Final actual selection/arm succeeded at revision737;
one command received HTTP200 `requested`, then same-ID reconciliation returned
durable `rejected/stale` with null applied revision. Native/private inspection
found one creation intent and one preflight cancellation, no dispatch/workload
and only the original three collectors. All eight exact normal2 resources were
subsequently stopped and its bridge exited0/endpoint closed. See HANDOFF for
separate proof identities; this does not supersede native capacity5 evidence.

The recorded cancellation locates rejection at the creator's final authorization
after required prepare, before one-use dispatch/effect admission. Current
`authorize_command` reports stale for either revision mismatch or observation
age beyond5s. Actual per-step monotonic durations, final revision and age were
not captured; assigning the delay to Docker, native view, file fsync or full
binary verification would be unsupported. Full source/binary checks are already
startup checks. Successful observation publishes advance the revision even
when only diagnostic values change. Reducing prepare cost can reduce that race,
but cannot eliminate it for submissions arbitrarily near a publish boundary.

The two source-only options have different guarantees:

| Option | Preparation and immediate fences | Limitation / review boundary |
| --- | --- | --- |
| A: move non-authorizing expensive preparation before fresh arm | Startup-verify immutable source/binary/config templates and bounded owned staging before a fresh selection/arm; keep exact native membership/eligibility/assignment and resource identity/running/paused checks immediately before effect, plus durable nonce/config binding and final authorization | No Docker creation, inventory slot admission or authorization grant from preparation; cached templates must reject changed ownership/content/config. Reduces critical-path cost, but final equality can still lose to an arbitrary diagnostic publish. Reusing private helper evidence must not remove live checks or fsync ordering |
| B: distinct safety revision and feed sequence | Keep preparation and exact immediate checks, but reject a changed safety footprint rather than an unchanged-safety diagnostic tick; independently enforce retained request observation age and current observation freshness | Requires an explicit new shared wire/schema contract, complete server-owned safety fields, unknown/ABA/continuity handling and source-proof invalidation. Neither inferred authority nor an old-revision fallback is allowed |

Option A may remain wire-compatible if limited to startup preparation; any
new preparation endpoint/token requires separate review. A preparation token
would not be a command, arm, inventory reservation or writer grant. Do not
prestage a new operation ID and later substitute/replay it. Nonce-specific
durable records still belong to the originally admitted command.

**Proposal only; not current wire semantics or authorization.** Prefer explicit
separate safety revision and diagnostic feed sequence, reviewed with the parent
before any implementation. The existing snapshot/feed sequence must continue
to advance, expose gaps/drops and drive bounded resnapshot. A safety revision
would be server-owned, scoped to an exact deployment/bridge incarnation and
persistently advanced whenever an agreed critical authorization fact changes,
becomes unknown or loses observation continuity. Unknown/unclassified changes
must invalidate it, not default to diagnostic-only.

The critical footprint must explicitly enumerate source qualification,
authority/writer/process/generation/epoch, exact resource identity and running/
paused/readiness, policy revision/admission/retirement, inventory slot/reservation
and relevant receipt-health dependencies. Action-specific relevance needs
agreement; this proposal does not silently exempt any current critical guard.
Diagnostic-only value changes may leave that safety revision stable only when
all critical facts remain freshly and independently known. A critical
known-to-unknown-to-known transition must advance it even if the final value
matches the original, preventing ABA. Restart, gap or observation failure must
disarm/invalidate; persisted numbers alone never establish continuity.

Proposed minimum exact footprint, to be approved before implementation:

| Surface | Server-owned fields / transitions that invalidate |
| --- | --- |
| Source and capability | Qualified actor/schema/proof/binary digests and scope; connected/qualified flags; supported operation/preset/bounds changes. A false or unknown qualification is denial, never a diagnostic change |
| Ownership and continuity | Exact `bridge.{id,incarnation}`, `deployment.{id,incarnation}`; owner/boot continuity; observation failure, staleness, transport/history gaps and unclassified drops. Missing critical labels and any previously unknown label deny |
| Resources and policies | Exact selected `resource.{id,incarnation}`, role/status, `identity_verified`, `running`, `paused`; writer `{collector,process,generation}`; `policy_revision`, `applied_policy_revision`, `excluded`, `block_streams`, `retiring`. Relevant additions/removals/replacements or known/null changes invalidate |
| Native authority / table safety | Independently verified authority incarnation/epoch/eligibility and exact native membership; immutable TableKey/schema binding, table `epoch`, `lifecycle`, full writer identity and `recovery_obligations`. Missing native authority fields cannot be reconstructed from CPU, role labels or container liveness; new producer-owned fields require explicit qualification |
| Finite capacity | Exact inventory deployment/baseline/revision and selected slot state, reservation, cancellation, dispatch, binding and uncertainty; scoped config identity/digest. The command's own permitted intent/dispatch transition requires an explicitly specified expected transition, not blanket revision suppression |
| Receipt safety | For workload, current `receipt_observation_known` must remain true; unknown/failure invalidates. Receipt count/presence, throughput and an accepted SQL row are not authority or writer grants. Receipt/digest/ordinary-reader completion remains independently reconciled, not inferred from safety revision |

The only diagnostic missing-label exemptions remain the existing exact eight
labels, not new broad category matching. CPU/memory/IO/queue/throughput values,
receipt presence/count and heartbeat timestamp changes cannot themselves grant
authority. A newly unclassified change or missing critical producer field denies.
Action-specific footprint narrowing and expected self-transitions are unresolved
review decisions, not permissions implemented by this table.

Selection, arm, initial reservation and final pre-effect authorization would
require the caller's exact latest safety revision, current explicit arm/session,
unchanged qualified sources and exact deployment/resource/policy identities.
Independent authoritative preflights and monotonic observation age5s/arm TTL30s
remain mandatory. A diagnostic tick cannot extend expiry, hide a gap, authorize
a new resource or renew a lease. Requested/applied safety revisions and observed
feed sequence need explicit wire fields and matching frontend/schema fixtures;
overloading today's `revision` or accepting missing fields as old semantics is
not proposed. Until the new contract is agreed and qualified, exact equality
against today's snapshot revision remains enforced and the consumer gate closed.

The submitted request must reference a retained server observation
`(bridge boot, feed sequence, safety revision, observed monotonic time)`.
Its original observation age stays at most5s through final pre-effect checks;
new diagnostic samples, selection/arm retries, reconciliation or a repeated
same-ID lookup cannot reset that deadline. Current critical observations must
also be fresh independently. Missing/evicted history or another boot denies.
Session TTL and explicit arm TTL30s remain separate, with disarm/logout/expiry
effective before any dispatch. This does not hold publication locks to hide
intervening observations or postpone invalidation.

Required review/qualification cases: diagnostic publish during each creator
preflight phase; resource/policy/authority or inventory change in the same
positions; critical unknown and ABA; aged observation without a sequence change;
gap/drop/observer failure/restart; disarm/expiry between reserve and dispatch;
old or missing revision fields; same-ID lost response and terminal cancellation
without replay; exactly one creation dispatch followed by actual registration/
ACTIVE/resource observation, then one-request physical Delta/receipt completion.
Add bounded local monotonic guard/phase evidence with requested/current safety
revision, feed sequence and observation age, preserving errors and unknowns.
Offline synthetic schedules must exercise boundary races before a separately
authorized single bounded runtime/browser checkpoint. No fresh runtime or
fence change is authorized by this proposal.

Private instrumentation proposal: at most64 operations and64KiB total retained
allowlisted evidence per bounded checkpoint, with explicit overflow/dropped
counts and no success claim from missing spans. Record original operation ID,
local process/clock identity and monotonic start/end for reservation, inventory
intent, native view, each of the three parallel exact resource probes, config/
hard-link/fsync preparation, final guard and dispatch boundary. Record private
stale subreason flags for revision mismatch versus original/current observation
age expiry, requested/current revisions, retained observation sequence/age,
arm/request remaining budget and each step's outcome. Preserve simultaneous
causes rather than choosing whichever guard executes first. No tokens, cookies,
CSRF, configs/credentials or arbitrary request bodies; no cross-clock wall-time
subtraction and no summing overlapping resource-probe spans as elapsed time.
Evidence collection is not permission to perform a Docker effect.

Deterministic unit regressions must separately assert age expiry with unchanged
revision, revision change while age remains fresh, both causes, unknown age,
future/foreign-clock observation and exact5s boundary. Synthetic phase barriers
should publish diagnostic versus critical changes between native view/resource
checks/config/final guard, exercise source/capability/scope and policy/epoch
changes, observer gaps and known-null-known ABA, and check no dispatch after
denial. For option B, a diagnostic publish must not reset original request age;
a request older than5s must fail even when the latest feed is fresh. Assertions
must distinguish initial HTTP denial, durable requested then preflight rejection,
pending/unknown effects and same-ID reconciliation. No unit/native pass for
these proposed cases is claimed.

Optimizing immutable startup-verified preparation can be reviewed independently,
but must preserve exact live authority/resource/config checks and durable
hard-link/no-overwrite/fsync intent/dispatch ordering. It is not proof of usable
human-paced authorization. This revision proposal does not implement or qualify
the separate multi-request/concurrent-client admitted-workload lease above.
