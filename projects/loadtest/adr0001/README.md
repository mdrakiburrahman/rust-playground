# Explicit ADR 0001 same-host experiment

## ADR alignment and stage tracing

The in-progress SQL/Delta update targets ADR PR #9 revision
`1ba1e53577ba3acf3db94626bce8bb7af819a7d8` for both primary arms; historical
manifest/legacy modes remain regression surfaces, not feature-parity targets.
The cohorts documented below predate this update and do not qualify it.

The shared placement model uses fresh collector observations, conditional
reservations and conservative new-table resource estimates. CPU and memory are
normalized to the collector cgroup's configured limits. Throughput is normalized
to a configured 64 MiB/s experiment envelope, **not measured sustainable
capacity**; per-table cold costs are estimates, not allocated CPU measurements.
Unknown/stale capacity backpressures placement. Excluding future placement,
blocking new streams, and retiring a collector are distinct revisioned policies.
Retirement must retain successor-recovery obligations until recovered ACTIVE,
including replacement of a failed RECOVERING successor.

`ARROWCOL_TRACE=1` enables bounded per-service monotonic spans. Disabled spans
skip clock/context capture, UUID and JSON construction; trace-header propagation
remains part of both arms. An on/off comparison measures enabled span tracing
over that common baseline, not the cost of removing all instrumentation code.
Stages are inclusive elapsed observations, not exclusive CPU work. In
particular, outer request/materialization spans contain downstream waits.
Neither SQL engine CPU nor cross-clock one-way latency is inferred.

The closed-field [benchmark exchange v1](contracts/README.md) is shared with the
standalone factory visualization. Validate/import data without granting any
runtime control. `export_traces.py --cohort COHORT` projects allowlisted aliases,
request/attempt/unit/commit/receipt links, service spans and container resource
intervals; its stage report lists missing coverage. Overflow markers make the
dataset incomplete; their count is a lower bound on suppressed events.
The continuation passed 42 Rust tests, rebuilt seven shared binaries and
qualified the exercised SQL/Delta boundary in both arms; exact retained runs and
earlier failures are in [HANDOFF.md](HANDOFF.md). Fresh matched measurements are
still pending. Earlier report/cohort values below do not qualify the new source.

### Fresh measured export and separate report

After all four fresh tracing calibrations and six nine-phase runs complete,
export with `export_traces.py --cohort COHORT`, then call
`delta_report.generate_fresh("COHORT")` from this directory using the existing
Python environment. The fresh-report path verifies the alternating arm order,
calibration workload/order, shared binary hashes, ordinary Delta and SQL receipt
reconciliation, stopped resources and actual Docker CPU/memory/PID bounds.
It creates a new `target/delta-benchmark/COHORT/public` directory containing the
sanitized exchange, fresh report, stage report, calibration/provenance summary
and exact frozen source/binary manifest. It never overwrites the historical
main report or inherits its historical qualification claims.

Preserve the measured native tree while a cohort runs. Later postprocessor
fixes must be separately identified; the public summary records the actual
report/export postprocessor hashes in addition to the frozen measurement
manifest. Preserve blocked attempts and their original metadata; do not combine
pre-fix calibration with post-fix measurements. CPU counter-reset intervals are
unavailable with retained error/coverage counts, not zero or complete coverage.

## Historical comparison: shared SQL / Delta

The retained historical report compares **adapted Pingora/router-hosted placement versus ADR
gateway/separate controller**, using the identical qualified `adr-flight`
collector and `adr-publisher` binaries. SQL controls ownership and accepted
publication decisions in both arms; disk/cache is not a competing write
authority. This is not a pure proxy benchmark. See [DELTA_RUNTIME.md](DELTA_RUNTIME.md)
for publication, authentication, finite-state bounds and prior fault qualification.

The completed repaired cohort is `shared20261027f`: three fresh pairs, 27 matched
phase pairs and 2,850,816 independently reconciled rows per design. All 336 inputs
completed without payload replay; 192 needed operation-ID reconciliation after
nonfinal RPCs. Warm sample medians varied by repetition; this is not evidence of
a general proxy winner or production capacity. The incomplete `shared20261027e`
attempt is retained separately: its unsupported Flight readiness probe stopped
the harness after seven phases, with all 909,312 submitted rows visible. The
corrected probe checks the listener; actual two-gateway uploads establish service
functionality in the measured phase.

The report's **Where time goes** section separates client-observed time before
the admission ID, admission-ID-to-final-receipt time, and final-receipt-to-RPC-end
time. It does **not** claim pure routing or collector timing: the first interval
includes client/upload, routing/control, collector decode/queue/admission and the
identity reply; the second includes collector staging, publisher, SQL/Delta and
reply transit. Warm stacked bars use additive means; the appendix retains
per-run medians and per-request publication-time fractions. Nonfinal RPCs remain
separate, and post-completion reader scans are excluded. No new benchmark was
run for this breakdown. The preceding report and derived evidence are archived
under `target/delta-benchmark/shared20261027f/report-revisions/pre-latency-breakdown`,
with hashes in that cohort's `pre-latency-report-archive.json`.

The opening **Requirements** section describes target design scenarios, not
implemented guarantees. It separates remaining durability/HA/SLO decisions from
the current evidence matrix. The **Run guide** defines R1-R3 as fresh paired
repetitions of the same nine-phase workload, explains sequential arm order and
host-order confounding, and spells out hot-table completion versus RPC success.
**Duration & throughput** derives run workload time by summing phase timers and
divides completed rows by that sum (never averaging phase rates). These timers
include per-input completion polling and reader scans, but exclude transitions,
setup and final audit/teardown. The separate first-input-to-last-scan span includes
interphase transitions/control gaps; it is not full lifecycle wall time.
Per-phase seconds remain in the appendix. This presentation update did not rerun
the benchmark. Its preceding report/source/evidence revision is retained under
`target/delta-benchmark/shared20261027f/report-revisions/pre-scenario-guide`,
indexed by `pre-scenario-report-archive.json`.

Use the existing venv, without rebuilding the measured binaries:

```sh
export TMPDIR="$PWD/target/delta-runtime/scratch"
target/adr0001/venv/bin/python projects/loadtest/adr0001/delta_benchmark.py \
  --cohort NEW-UNIQUE-NAME --subnet-start UNUSED-OCTET
target/adr0001/venv/bin/python projects/loadtest/adr0001/report.py --shared-delta COHORT
target/adr0001/venv/bin/python projects/loadtest/adr0001/evidence.py --shared-delta COHORT
```

The finite harness checks subnet/name isolation, alternates arm order over three
fresh pairs, freezes actual executables, and retains raw inputs, independent
standard Delta scans, SQL final receipts, cgroup observations and scoped stopped
inventories under `target/delta-benchmark/COHORT` and the named
`target/publication-gate/publication-gate-bench-*` attempts. Never use the ordinary
destructive load-test lifecycle here. Evidence closeout additionally requires
saved browser validation matching the report hash.
`delta_browser.py --cohort COHORT` uses an isolated local Playwright Chromium
profile and a short-lived loopback server exposing only the report. It checks
desktop/mobile layout, collapsed/expanded appendix behavior, internal links,
exact chart data and rendered content-box widths, screenshots and console errors.
For the retained validation, Chromium and missing shared libraries were installed
only under `target/delta-benchmark/`; run with `PLAYWRIGHT_BROWSERS_PATH` pointing
to `browser-binaries` and `LD_LIBRARY_PATH` pointing to
`browser-libs/root/usr/lib/x86_64-linux-gnu` there. Those paths are local validation
artifacts, not repository defaults or production dependencies.
`--resume` continues the remaining arm order only when all retained attempts
have completed independent reconciliation; it never reruns or silently discards
a failed arm. Each arm retains its exact measured harness source. Cohort
`shared20261026d` retained its first completed measurement after a read-only
sqlcmd output-format repair; input, workload and topology methods and frozen
native binaries are verified identical across all six arms.

Each run uses 56 inputs, two tables and 112/128 **global** private preparation
attempts at most; no-op schema probes count. Cold/warm, bounded concurrency pressure,
elephant inputs, manual migration, collectors 3→4→3 and gateway topology changes
are distinct phases. An unknown RPC is reconciled using only its operation ID
(200 ms plus request-time polling, at most 45 seconds), never by replaying its
payload. Resolved completion is not a successful terminal RPC latency sample.
Both clients half-close their one-batch input before waiting for final completion.
Naturally occurring nonfinal RPCs are retained, not retried or discarded:
independent scans report submitted-but-absent rows separately and require every
final-receipt row/digest to reconcile. An unattributed publication or SQL receipt
fails closeout. Successful-only latency distributions are explicitly censored
when a phase contains nonfinal RPCs.
Reader observation is a standard scan after completion, not exact first visibility.
P95 is exploratory below 100 samples; P99 unavailable below 1,000 samples.

**Gateway scale-out:** the primary Pingora router owns placement/control; added
replicas use explicit `ARROWCOL_DELTA_MODE=data_only`, authenticated remote
resolution and no private authority credentials or mounts. ADR gateways share
the separate controller. A second controller-enabled Pingora still correctly
rejects live-controller contention. Neither arm establishes physical capacity
scaling or controller HA on one host. Collector 3→4→3 phases measure membership
transitions, not automatic redistribution of existing tables.

The pre-repair `shared20261026d` cohort remains failed/limited evidence: 20
Pingora elephant inputs (1,310,720 rows) never published, and gateway-two was
unmatched. Repair qualification isolated shared upstream HTTP/2 flow-control
starvation when a collector waited for its table mailbox before consuming a
batch. In opt-in Delta mode, bounded decoding now precedes serialized publication;
legacy scheduling, deadlines and size/concurrency limits are unchanged.
`target/delta-runtime/repair-20261026/closeout.json` records native red/green
large-upload and data-only gateway gates with SQL and etcd. Fresh paired runs use
the repaired binaries throughout; old failures are not relabeled as successes.

The previous report is copied into each cohort and retained in the collapsed
historical appendix, with original sink/batching/source attribution. The
original snapshot is carried forward from the selected cohort on later runs;
the current shared-path report is never relabeled as the original baseline.
The immediately preceding shared report is separately saved as
`preceding-shared-report.html`, with its cohort and digest in `preceding-cohort.json`.
The historical publication-gate executable was overwritten during runtime development; original
executable bytes are unavailable, although historical source hashes/logs remain.
New binary copies do not repair that historical provenance gap.

## Historical manifest / disk comparison (not the current shared path)

This **additional adapter**, `crates/arrowcol-authority`, leaves the overlaid
Pingora/disk-routing/SQL/Delta implementation intact. It is not an implementation
of an independently verified GitHub PR revision. The source input was the
user-supplied, proposed ADR. The private PRD and ADR are intentionally not copied
here. [Offline measured report](report.html).

## Boundary and trust

Two Rust process roles are `adr-controller` (logically distinct placement,
ownership and commit modules) and the native `adr-flight` gateway. The latter
also has an **explicit** `--collector ID` mode implementing the no-WAL direct
collector adjunct. It reuses Arrow Flight, the canonical catalog SDK schemas and
`arrowcol-protocol` fingerprints, not the old `BufferManager`, SQL receipts,
Delta sink or durable-backlog succession promises.

An authenticated, bounded single-tenant catalog maps the **existing catalog
table UUID** to descriptor names. Tenant/catalog scope is bound to the configured
edge HMAC principal; fingerprint/version is not ownership identity. External
requests use the original edge-signed metadata contract. Gateway assertions bind
the original signature and exact assignment with a separate internal HMAC.
Collectors require both. This benchmark is not a general multi-tenant IAM system.

Controller-only etcd credentials have prefix-scoped permission. Per-collector
credentials cannot impersonate another collector. The controller grants a unique
generation for each process incarnation. Durable process markers prevent
lease-expired generations from registering again. Control/member leases do not
delete durable epochs or table heads. Renewed heartbeats cannot reset an expired
generation; restart the affected process. A controller whose own lease expires
must restart and acquire a fresh control identity, never initialize a new cluster.

The reader-visible head is inside the same etcd transaction as the exact
control-key and live-member value/revision and complete prior ownership record.
That record includes epoch, lifecycle, admitted units and prior head. Immutable
Parquet and a parent/delta manifest are synchronized before requesting this
transaction. `TABLE_COMMITTED` follows confirmed publication only. Repeated
internal proposals resolve by exact commit/head identity in authoritative history;
finite client retries may instead append duplicates.

Readers traverse only verified committed head/parent references. Directory scans
are **not** a reader contract. Object names are table/process/generation/commit
bound, immutable-create only. History is linear in commits and capped at 100,000
loads; checkpoints/compaction and orphan GC are deferred. This is a bounded
benchmark, not a qualified indefinitely growing table format.

## Stream and migration limits

Gateways forward raw Flight messages through a four-message queue, with 16
concurrent streams, 2 MiB messages, a 1 MiB initial envelope, 5-second initial
and ownership-resolution bounds and a 30-second lifetime. They never replay
DoPut, decode arrays, persist payloads, or retain receipt outcomes. Assignment
misses coalesce behind a bounded directory lock. Snapshot revisions precede etcd
watch events through a controller request/response long-poll bridge (gateway
reconnects after 200 ms), not continuous gateway-facing streaming; old events are discarded and compaction/disconnect/uncertainty
causes a resnapshot. Cached routes never grant commit authority.

Collectors share one table mailbox across concurrent streams and replacement
runtimes. `Service::replace_runtime` takes the same lifecycle barrier as writer
admission, drains active streams and blocking encoding, permanently retires the
old runtime, then requests controller-generated session rotation. That transaction
compares the old exact session and all owned records, advances owned-table epochs
into RECOVERING and changes the live generation atomically (bounded to 32 owned
tables per local replacement). Thus even an uncertain old RPC cannot cross the
new head fence. The successor retains shared mailboxes and recovers before
activation. Failed replacement is fail-closed; it never revives the old runtime.
A live incarnation also excludes overlapping processes using the same credential.
Decoded batches/streams are bounded, and encoding runs off the async executor.
Admission is provisional. Drain closes new authority admissions; pending units
may finish, then a two-second bounded drain produces a greater-epoch RECOVERING
owner. Only the successor that reloads the exact post-cutover head can activate.
Operation identity, source, target, deadline and phase are persisted.

Placement uses actual cgroup CPU, process RSS and active-stream pressure, fresh
leased membership, centralized effective assignment counts and a per-table
500 ms post-activation cooldown. DRAINING operations count both source and reserved
target pressure; RECOVERING targets count once as owners. Persisted operation
records reconstruct reservation budgets after restart: at most four concurrent
migrations globally and two per target. Completion releases the reservation;
target expiry during drain cancels the move if the source remains live; failed
recovery targets are fenced and replaced. It is deliberately not an autoscaler.
An elephant table still has one owner. Draining eligibility is durable. Evacuate all owned tables before removing
a collector. Missing liveness is not a fence: the conditional table transition is.

The retained client returns a dict-compatible `UploadResult`. JSON contains only
evidence; an unresolved result's **`retry_handle`** owns the complete Arrow schema
and batches (including dictionaries), at most 8 MiB per finite unit. Call
`result.retry_handle.resume(endpoint, secret)` to replay through the same gateway.
Attempts are bounded to 1–8 and each RPC has a 15-second deadline. `run()` returns
all unresolved handles in `report.retry_handles`; callers must retain that object.
Serializing the report does not persist input and provides no client-death outbox.
Stream-open latency is the local `do_put` return, not server admission. ADR has
no provisional admission response, so admission latency is explicitly unavailable;
final receipt and terminal observations remain separate.

`focused.py --project adr0001-UNIQUE --baseline BASELINE_ROOT` runs narrowly scoped
reservation/restart, active DoPut gateway interruption/replay, same-live-gateway
compaction resnapshot, post-partition staging, and exact replaced-session fencing
proofs. It saves a separate source/binary/image manifest and stops only its own
project, preserving failed logs, state and volumes. It does not regenerate the
historical report or attribute new behavior to old cohort binaries.

## Reproduce

Run from this isolated worktree. Do not invoke host bootstrap, global Docker
cleanup, or remove volumes. Existing baseline source must remain hash-identical
to its recorded overlay manifest.

```sh
git submodule update --init submodules/otel-arrow
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo build -p arrowcol-authority --bins
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test -p arrowcol-authority
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo clippy -p arrowcol-authority --all-targets -- -D warnings
python3 -m venv target/adr0001/venv
target/adr0001/venv/bin/pip install -r projects/loadtest/adr0001/requirements.txt
target/adr0001/venv/bin/python -m unittest discover \
  -s projects/loadtest/adr0001 -p test_client.py
```

The baseline's unchanged `routing-benchmark/stack.py` prepares a **new**
`pingora-bench-*` project and generates SDKs in its ignored `target`. Its existing
verified runtime/SQL/Hive images can be tagged for that project. Run baseline
measurements with `benchmark.py baseline` and `scenarios.py baseline`; stop that
exact project before starting this adapter.

Generate a catalog with `benchmark.catalog(BASELINE_ROOT)`, save JSON under
`target/adr0001/`, and prepare a unique project:

```sh
PY=target/adr0001/venv/bin/python
$PY projects/loadtest/adr0001/stack.py prepare \
  --project adr0001-UNIQUE --catalog target/adr0001/catalog.json
$PY projects/loadtest/adr0001/stack.py start --root target/adr0001/adr0001-UNIQUE
```

`start` bootstraps a private, dedicated three-member etcd cluster and enables
authentication **before** application startup. Never rerun bootstrap on existing
state. Use Compose `start` instead. Public Flight ports are random loopback ports;
etcd/control have no published ports. Generated credentials are mode 0600 in
ignored task state, scoped separately for every application role.

First run a dedicated correctness project with two collectors/two gateways.
`fixtures.extra()` adds explicit shared-schema, evolution and dictionary fixtures;
`fixtures.prove()` and `faults.py` exercise real RPCs and durable heads. Stop this
project, then create a **fresh** catalog/etcd/object dataset for cold measurements.
Add collector3 before `scenarios.py adr`. It runs 3→4→3 collectors and 1→2→1
gateways. `benchmark.py` records retained client input, attempts, committed
receipts, terminal outcomes, independent rows, and whole-stack resource samples.
No-retry runs require exact marker counts; retry runs count duplicates and
record unconfirmed logical bytes and deterministic regeneration settings on
exhaustion, not live retained buffers or a durable outbox.

```sh
$PY projects/loadtest/adr0001/scenarios.py adr \
  --root target/adr0001/adr0001-UNIQUE --baseline BASELINE_ROOT
$PY projects/loadtest/adr0001/stack.py stop --root target/adr0001/adr0001-UNIQUE
```

Run the report generator after saving measurements. Raw evidence and the exact
final JSON manifest live in `target/adr0001/evidence/`. Failed attempts remain
visible. Baseline final publication observation includes SQL receipt polling;
new-design receipt latency is head-commit observation. They are not equivalent
latency definitions.

`mailbox.py --root ROOT --baseline BASELINE_ROOT` checks 18 overlapping streams
and exclusion of a second collector process. `limits.py` with the same arguments
checks the 16-stream limit, cancellation without publication, and oversize
message rejection. `repeats.py` creates two additional fresh cold projects per
architecture and captures bounded idle samples; its project names are fixed to
this recorded experiment, so use new names before repeating it. Preserve all
existing state. These programs consume the catalog prepared for their scenario.

Runtime preparation copies the built binaries into each project's read-only
`bin/` snapshot: replacing a Cargo binary must not silently change a running
bind mount. The final manifest compares legacy snapshots with the **legacy** validated
binaries, cohort b with its original instrumented build/run hashes, and focused
and final snapshots with the focused build hashes. They are distinct revisions.
It preserves the dated legacy source manifest rather than claiming it matches
current source, checks the reused baseline image/executables against prior audit
evidence, rehashes all 45 original overlay paths, and proves task containers are
stopped. The first proof snapshot and failed recovery remain developmental
evidence, not the final validated binaries.

```sh
$PY projects/loadtest/adr0001/report.py \
  > target/adr0001/evidence/report-generation-final.log
$PY projects/loadtest/adr0001/evidence.py \
  > target/adr0001/final-evidence-generation.log
```

Generate provenance last, after stopping the exact task projects. Keep its stdout
outside the evidence directory so it cannot hash its own changing output. The
offline HTML uses no external assets; browser verification was attempted but
the MCP request timed out, so only static HTML/metric validation is claimed.

## Interpretation and exclusions

### Final functional revision, October 26

`instrumented-obs20261026final2` is a new matched experiment after the focused
functional fixes, not a revalidation or pooling of cohort b. It uses the current
focused-validated binaries without rebuilding: persisted global/per-target
migration reservations and restart/release handling, runtime retirement/fencing,
and retained in-process retry handles. `focused-gaps20261026/manifest.json`
records seven passed Rust contracts, eight passed Python tests and ten passed
live proofs in `adr0001-focused20261026f`.

Both stacks run **sequentially**, baseline then ADR: cold, three warm,
pressure, elephant, migration/after-move, collectors 3→4→3, gateways 1→2→1:
11 matched phases × 4,608 = **50,688 generated/visible rows per design**.
All phases must reconcile with zero missing rows and unresolved streams; sample
validation remains unchanged. Periodic CPU/cgroup, cache, revisions and accounted
client/application backlog are captured with the same observer as cohort b.
Stream-open latency is separate from terminal/final observation. **ADR admission
latency is unavailable by protocol** (only final `TABLE_COMMITTED`); stream-open
is not an admission proxy.

`observe_runs.py` inspects Docker IPAM and host routes, then selects four unused
private /24 subnets for this exact baseline and ADR project. No global network,
volume cleanup or daemon bootstrap is performed. The first `obs20261026final`
attempt stopped before project preparation because the baseline requires /24,
not /28; its evidence/log is retained. The corrected unique run is `final2`.
All exact task containers are stopped afterward; named volumes/state remain.

```sh
$PY projects/loadtest/adr0001/observe_runs.py --run-id NEW_UNIQUE_ALPHANUMERIC_ID
$PY projects/loadtest/adr0001/analyze_observations.py \
  --cohort instrumented-obs20261026final2
$PY projects/loadtest/adr0001/coverage.py
$PY projects/loadtest/adr0001/report.py \
  > target/adr0001/evidence/report-generation-final.log
$PY projects/loadtest/adr0001/evidence.py \
  > target/adr0001/final-evidence-generation.log
```

The final HTML displays final2, historical b and legacy separately. The original
cohort-b final manifest is retained at
`instrumented-b-closeout/final-evidence.json`; incomplete a, legacy and focused
proofs/failed attempts remain untouched. The final current-source manifest
records reporting/observer edits separately from runtime build sources and
rehashes the 45 original baseline files.

Residual boundaries remain explicit: no single full-native Flight hot-replace
while encoding proof (barrier/replacement and live session/epoch fencing are
compositional), no packet-level ambiguous etcd transaction reply-loss injection,
and no exact consensus-event latency. Partition staging uses collector-exec
injection, not a paused native encoder. The coverage matrix describes the
implemented focused fixes without claiming the entire ADR complete.

### October 26 instrumented closeout

The legacy 13 matched pairs/11,520 rows per design remain separate from completed
`instrumented-obs20261026b`: **11 matched pairs, 4,608 rows each, 50,688
generated/independently visible rows per design, zero missing/unresolved**.
Do not treat aggregate generated rows from all reports as acknowledged/reconciled:
the old failed recovery and all failed attempt logs remain visible.
`instrumented-obs20261026a` was interrupted at baseline pressure after four
baseline reports, has sampling failures and no matched ADR arm; retain it as
incomplete and never pool it.

Offline regeneration (no containers are started):

```sh
$PY projects/loadtest/adr0001/analyze_observations.py \
  --cohort instrumented-obs20261026b
$PY projects/loadtest/adr0001/coverage.py
$PY projects/loadtest/adr0001/report.py \
  > target/adr0001/evidence/report-generation-final.log
$PY projects/loadtest/adr0001/evidence.py \
  > target/adr0001/final-evidence-generation.log
```

The HTML includes per-phase CPU/cgroup, client logical retention, application
backlog, cache/watch and requirement tables. `observation-summary.json` retains
machine-readable component/interval distributions. All units and sample counts
are explicit. Baseline/ADR have 351/240 observation cycles and zero sampling
errors; the 0.5-second wait follows each read cycle, so median start cadences
are 935.09/1,207.35 ms, not 2 Hz. CPU is counter usage divided by elapsed Engine
read time (one core = 100%), only for intervals wholly inside a phase; not
integrated phase CPU. Memory is sampled cgroup usage/anonymous bytes, not process
RSS or continuous maxima. Component sums combine staggered reads. Two missing
ADR component memory reads stay unknown; incomplete sums are excluded.

Client bytes count logical Arrow ownership, excluding native/IPC/allocator
buffers. Gateway byte guards exclude tonic/kernel buffers; writer/mailbox
gauges exclude unaccounted Parquet/native memory. High-water is exact only for
accounted ownership since process start (client high-water is per phase);
sampled current peaks and last samples are not exact peaks or shutdown proof.
Saved runtime logs omit collector4/gateway2 application observations; their
Docker measurements exist but their application gauges/cache are unavailable,
not zero. Cache resets include uncertainty/idle timeout, not just compaction.

Watch propagation covers 1,596 matched watch applications: median lower/upper
RPC-bracket bounds 169/186 ms, P95 297/312 ms. These bracket consensus timing
using same-host RPC timestamps with ±1 ms precision, not exact commit-event
latency. Snapshot/resolve applications are excluded. Asynchronous signed
revision distance is measured in **revisions**, not milliseconds; its observed
minimum −1 reflects staggered reads, not negative propagation time.

`requirements-evidence.json` and the HTML candidly describe a **bounded PoC,
not a complete ADR implementation**. Residual work includes live catalog/DDL
registration and general IAM, continuous gateway streaming watch,
checkpoints/compaction/orphan GC and production hot-reload orchestration.
The final revision implements reservation/migration budgets and runtime
retirement/barriers; focused live proofs cover active-stream gateway removal.
The matched-cohort gateway scale-in itself is between completed finite calls. Stale-owner and lost-reply
proofs have the narrower control/application injection boundaries documented in
the matrix. Cohort b used the prior functional revision; the final cohort above
measures the focused fixes.

`instrumented-checks/build-source.json` identifies cohort b's original Rust source;
saved build, four Rust contracts and strict Clippy passed. Closeout reran
format checking and six local Flight client tests. Run-time source hashes in
`instrumented-obs20261026b/run.json` remain immutable; reporting changes are
identified separately in the final current source manifest. The old manifest
is preserved at `prior-cohort/final-evidence.json`. Original baseline runtime
provenance remains the prior audited filesystem/four executables, not new Cargo
binaries. Final evidence enumerates all exact old and observation projects,
including `pingora-bench-adrobs20261026a/b` and `adr0001-obs20261026b`,
checks all containers stopped and named volumes preserved, and rehashes all
45 original baseline files. The final cohort adds a fresh live workload; no
commit, push, PR or global cleanup is part of this closeout.

Compare complete architectures, **not proxy implementations**: Delta/SQL/Hive
versus custom manifests/etcd, flush/batching, fsync, receipt polling, processes,
resource allocations and protocol all confound attribution. All three etcd
members share one host. This validates selected process/link/quorum faults, not
host/disk loss, backups, regional HA, production capacity or policy superiority.
There is no ingestion WAL or client-death guarantee. Source workloads and
in-process writer adapters assume non-Byzantine services; shared local storage
is not an adversarial WORM boundary. No generic DFE/Quiver integration is claimed.

## Reading and regenerating the visual report

`report.html` opens with a concise executive view: paired architecture/control
diagrams, final2-only statistics, zero-based latency bars for all 11 matched
phases, four insights, balanced trade-offs and a **proposed, untested** next
experiment. The warm headline is the **median of three phase P95s**, not a pooled
P95. Individual repetitions and per-phase stream sample counts remain visible.
All historical cohorts, detailed measurements, the 28-row requirement matrix,
fault evidence, caveats, provenance and primary documentation references are in
one initially collapsed appendix. Expand it before printing if those details
are wanted; the executive view prints without expanding evidence automatically.

The proposed first experiment is bounded per-table group-commit on the fenced
ADR, with routing, sink and final committed-receipt semantics unchanged. It is
not implemented or benchmarked here. The report specifies sweep controls,
correctness gates and measurement requirements; it makes no promised speedup,
proxy-only or production-capacity claim.

Run the existing `report.py` then `evidence.py` commands above, offline, without
rerunning containers. `report-metrics.json` retains its historical schema and
adds `executive_final` with raw-derived headline metrics and chart geometry.
`report-validation.json` checks geometry, labels, a 900-word executive prose
budget, balanced HTML and initially closed details. The only external URLs
allowed are three named primary-documentation anchors inside the appendix;
scripts, external runtime assets and CSS imports remain forbidden.
If browser tooling is available, verify desktop/mobile/print layout and record
`report-browser-validation.json` against the exact HTML SHA-256 before generating
final provenance. Screenshots and browser evidence belong under ignored
`target/adr0001/evidence/`, not the report's source directory.
