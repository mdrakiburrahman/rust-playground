# Authenticated shared Delta runtime

This is an opt-in, finite, single-host runtime correctness implementation, not a
production HA system or an exactly-once delivery claim. It builds on
`PUBLICATION_GATE.md`; that document and its retained closeout describe the
earlier administrative publication fixture, not runtime authentication.
Repeated performance cohorts and the main HTML report use `delta_benchmark.py`
and `report.py --shared-delta COHORT`; they remain separate from this fault
qualification. See README.md for the finite methodology and evidence layout.

## Modes and topology

Omitting `Config.delta` preserves the original manifest/etcd collector and
controller. The ordinary Pingora CLI also remains unchanged. The original
Pingora source worktree is not used or modified by this integration.

Set `delta: {"publisher": "http://publisher:50061"}` to select the shared native
Flight/Parquet/Delta collector. Both new arms use **the same `adr-flight`
collector executable**, buffer bounds, dictionary normalization, schema
evolution, native Delta helpers, and completion protocol:

* ADR: `adr-flight` gateway → shared collector; independent `adr-controller`.
* Adapted baseline: existing `arrowcol-router-rust` executable, opted in with
  `ARROWCOL_DELTA_CONFIG` and `ARROWCOL_DELTA_PRIVATE`, → the identical collector.
  Pingora hosts placement/control in its own process.
* Both: the same separate `adr-publisher` role with the same resource limits,
  private preparation storage, final storage, and SQL receipt projection.

The private JSON configuration selects exactly one authority backend, `sql` or
`etcd`. SQL uses a dedicated database/login and password-file reference. etcd
uses the configured authenticated endpoint and `/delta/` namespace. There is no
dual write of ownership and no fallback from either authority to the other.
In etcd mode SQL stores only a non-authoritative final-completion projection;
etcd remains necessary for ownership, acceptance, recovery, and receipt lookup.

Collectors have only their own process token plus ingress/assertion verification
keys. They have neither SQL credentials nor etcd credentials. A collector can
write only its own staging mount; final Parquet and `_delta_log` mounts are
read-only. Controllers/routers have authority credentials, but no final-data
mount. Only the trusted publisher can promote immutable files and install logs.
`delta_runtime.py` creates and verifies this actual container/mount separation.
Runtime SQL uses a database-scoped `db_owner` login, not `sa`; least-privilege SQL
role design and production identity/mTLS are not qualified by this PoC.
Its password is independently generated; runtime roles do not receive the
administrative bootstrap password.

## Ownership and placement responsibilities

`delta_store` is a narrow typed state machine, not an etcd JSON API emulator.
Controller incarnation, member/process/local generation, expiry, ownership,
admission, reservations, operation identity, accepted versions and published
prefix are serialized by one authority transaction. SQL uses a locked deployment
row and **database time**. etcd uses a linearizable state/liveness snapshot, real
leased liveness keys, and CAS over the state and liveness observations.
Expired process incarnations cannot revive; a new controller generation fences
old grants and requires recovery. Watch delivery is a complete revisioned
snapshot, so consumers do not silently skip incremental-history gaps.

The adapted baseline retains router-hosted selection, sticky routing,
least-pressure policy, reservation-aware target ranking and migration
orchestration. It does **not** forward its placement decisions to ADR's
controller. Shared primitives enforce grants and publication safety.
Disk/cached grants are no longer ownership authority in this mode: SQL/etcd
grants are final. Its opt-in policy supports `least_pressure` only; the existing
legacy weighted/other policies are not silently substituted. ADR additionally
uses its assignment-count weighting. Router death also loses its control
process; its conservative restart may wait for expiry and fresh collector
processes. Do not attribute ADR's separate-controller availability to baseline.

### Data-only Pingora replicas

`ARROWCOL_DELTA_MODE=controller` is the opt-in primary's default. Exactly one
primary owns placement; another controller-mode process still fails controller
contention. For a second **Pingora** data-plane process set:

* `ARROWCOL_DELTA_CONFIG=/config.json`
* `ARROWCOL_DELTA_MODE=data_only`
* optionally `ARROWCOL_DELTA_LISTEN` (default `0.0.0.0:50051`)

Its configuration uses `controller` to address the primary router's private
control endpoint and carries the gateway token, authenticated catalog registry,
and edge/internal assertion keys. Set `operator_token`, `etcd_user`, and
`etcd_password` to empty strings, `etcd` to `[]`, and `collectors` to `{}`.
Do not provide `ARROWCOL_DELTA_PRIVATE`, `ARROWCOL_CONTROL_LISTEN`, authority
credentials/mounts, or final data/log write mounts. Provide its own writable
`/runtime` for Pingora's process files. The runtime rejects authority credentials
and control-listener/private configuration in data-only mode.

The replica uses the existing authenticated `Client` to resolve grants at the
primary, with the same bounded 300 ms sticky cache. It never opens an authority
store, claims a controller incarnation, selects an owner, or promotes itself.
Cached grants still undergo exact collector admission and publication fencing.
Primary-control or authority outages fail closed; this supports gateway
1→2→1 operation, **not** controller HA. Structured `delta_route` events report
cache hits and the routing role without logging credentials.

## Publication and client semantics

The native ingress signed identity/header protocol reaches the shared collector
behind both gateways. Admission and publication authenticate the exact catalog
table, member/process/local generation and epoch. Accepted authorization and
ownership transfer use the **same store transaction**; a cached lease check is
not a commit fence.

The trusted publisher binds a unit to its staging SHA-256, decodes bounded
Parquet, prepares genuine Delta actions privately, then accepts an exact
canonical action/content/version decision. Only accepted contiguous decisions
can be materialized. Final file/log installation uses complete-file, atomic
no-overwrite visibility and verifies exact content on idempotent retries.
Ordinary Delta readers require no custom manifest or catalog modification.

An accepted decision may finish after ownership transfer. A successor remains
Recovering until the accepted prefix is materialized and final receipts are
recorded. Unaccepted old-generation proposals cannot cross the transfer fence.
Final receipts follow physical materialization and SQL completion, never merely
accepted intent. In etcd mode the SQL projection is written before the etcd
published-prefix update; recovery reconciles that gap without a new decision.

The Flight result stream first emits `DELTA_OPERATION` with `final:false`.
This is only an operation identity, **not success**. `DELTA_COMMITTED` is final.
After disconnect or unknown outcome, query `delta_receipt` with that identity;
never replay payloads through a gateway. A log may already be visible while
SQL completion is unavailable; that state must not yield a final ACK. Gateways
do not maintain a receipt ledger.
Submitted bounded publisher work outlives a collector HTTP deadline; connection
loss is not an implicit abort. Staged reads use a bounded opened regular-file
descriptor and reject symlinks.

The Delta collector decodes one bounded batch per admitted transport stream
**before** waiting for its table's publication mailbox. Waiting before consuming
DATA can deadlock a multiplexed HTTP/2 connection: waiting streams occupy the
connection flow-control window while the active writer cannot finish receiving
its batch. Publication remains serialized, with unchanged stream/message/
decoded-memory bounds, deadlines, admission and commit fences. Both new arms
use this same scheduling; legacy manifest mode retains stream-wide serialization.
Concurrent streams can hold bounded decoded batches while waiting; memory
observations include those batches. There is no payload replay.

An empty metadata-reader result is not itself evidence of lost gRPC trailers:
the native client can surface a nonzero terminal gRPC status on writer close.
In particular a publisher HTTP deadline remains an unknown outcome, potentially
resolved by operation identity; it must not be relabeled a stream-final ACK.

## Bounds and conservative recovery

This deliberately finite implementation uses a deployment-wide serialized
snapshot (768 KiB), at most 32 tables, 1,024 retired processes, 128 retained
decisions/input identities per table, 16 pending/accepted-backlog units, four
global migration reservations and two reservations per target. Publication
has 16 slots and one mailbox per table; control has 64 request slots. Staging/data is bounded to 8 MiB per
file, decoded data to 32 MiB and 65,536 rows; the native Flight limits also
apply. The private preparation directory is capped at 128 attempts.
That cap is **global across tables**, not per-table: a successful input normally
consumes two slots (schema preparation, including a no-op, and data preparation).
The finite comparison reserves headroom with 56 offered inputs / at most 112
attempts; rejected pre-admission inputs consume fewer preparation slots.

There is no compaction/checkpoint garbage collection or indefinitely sustained
capacity claim. Preparation attempts, including no-op schema probes and
interrupted unaccepted attempts, consume the finite private-workspace bound.
An unaccepted failed admission may require authoritative source expiry/revoke
and migration rather than payload replay. SQL/etcd outages fail closed.
Accepted decisions have identity-based recovery; an unaccepted partial private
preparation is not a durability or delivery promise. These are explicit
operational limitations, not permission to relax the reader/fencing contract.

### Gateway topology limitation

The pre-repair binaries measured in cohort `shared20261026d` unconditionally
started an exclusive controller in every adapted Pingora process. Their second
router failed the live-controller claim. Preserve that failed scale evidence:
the later data-only repair does not retroactively qualify those measurements.
Never disable fencing or substitute an ADR gateway and label it Pingora.

## Reproducible correctness evidence

`delta_runtime.py --project <unique-name> --subnet <unused-CIDR> --arm
{adr,pingora} --backend {sql,etcd}` runs finite real native Flight correctness,
not performance. `--arm adr --backend etcd --legacy` omits the Delta option and
checks original manifest behavior. Each run verifies project/subnet isolation,
uses dynamic loopback ports and retained named resources, records hashes and
logs, then stops only its own containers. It never deletes networks or volumes.

The fault barriers are inert unless `ARROWCOL_DELTA_FAULTS` is explicitly set
for the task publisher. They operate in the actual authenticated publication
path; they are not fake collectors or a second publisher implementation.
`delta-contract` separately exercises real backend concurrent claims and
operation reconciliation. Rust domain tests cover generation/expiry, operation
collisions, reservation replacement, snapshot gaps and default compatibility.

Final qualification manifests and exact command outcomes are retained under
`target/delta-runtime/`; per-attempt events, frozen binaries, ordinary Delta
reads, uploads and stopped inventories are under
`target/publication-gate/publication-gate-runtime-*`. Failed/intermediate
attempts remain evidence and must not be represented as passing qualification.

`--gateway-repair` selects focused correctness: eight 65,536-row, half-closed
native uploads at concurrency four, exact ordinary-Delta reads, actual second
gateway cold assignments/traffic, authority and primary-control outages, and
scale-in of only the data-only replica. `--streaming-repro` is a negative
qualification for retained pre-repair binaries, not a passing-runtime gate.
Neither mode runs performance cohorts or edits benchmark/report evidence.
The eight large inputs use a fresh table with one preceding warm publication.
Scale-in traffic uses the separate cold-assignment table. An additional upload
against the longer main-table prefix records its original wire terminal and
separate identity reconciliation, without replay. Repeated verification and
finalization of the accepted prefix can still exceed the unchanged five-second
publisher HTTP deadline; such a status-14 RPC is not relabeled as a native final.
This is a bounded correctness workload, not qualification of arbitrary prefix
lengths or concurrency.
