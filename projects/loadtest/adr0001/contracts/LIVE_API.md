# Dedicated local PoC bridge v1

`live-v1.schema.json` is the unchanged closed packet contract. `live_contract.py`
validates it and supplies server-owned command-fence checks. `live_server.py`
implements the bounded authenticated read-only HTTP transport; `live_runtime.py`
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
Receipts, IO, queue and throughput observations remain explicitly missing in
this first read-only projection. A router-hosted authority is represented once
as its gateway resource, not duplicated into two CPU charges.

All HTTP action capabilities and arming remain unsupported, `qualified:false`,
even after the independent backend SQL/Delta gate passes: that gate does not
qualify this bridge's executor. The frontend client must keep its independent
mutation gate false until the corresponding real bridge/end-to-end acceptance.

`live_executor.py` now supplies a serialized policy-effect core for exclusion
and admission only; it is not wired to HTTP capabilities. It durably reserves
normalized intent before mutation, rechecks freshness/arming after preflight,
checks the exact execution resource and full writer, and uses the authority's
atomic expected policy revision. Other desired policy fields are preserved;
retiring/approved resources are rejected. Native operation identity is stable
across lost responses. Reopening its ledger and reconciling performs reads,
never automatic policy replay. `applied_revision` comes from a fresh matching
public snapshot, not an unrelated native policy counter; completion additionally
requires the exact-incarnation collector ACK. This is policy completion, not a
claim about receipt/Delta completion.

Separate direct real SQL/collector qualifications verified both policy controls,
lost-response/ledger-reopen reconciliation, new-stream admission denial,
exclusion/admission separation and exact ordinary Delta marker rows. Those
proofs do not qualify authenticated HTTP effects, full bridge-process restart,
other operations or independent bounded fault rollback. Those surfaces remain
pending and disabled.
