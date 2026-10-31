# Development transfer checkpoint (2026-10-30)

This is unfinished source saved for another machine, **not qualification, fresh
performance evidence, or PR readiness**. The transfer branch is
`dev/adr0001-routing-handoff-20261030`. The existing draft PR #55 remote branch
`feat/adr0001-routing-benchmarks` is intentionally not advanced by this checkpoint.
Its starting commit is `496b7b56f410bb512240bd6c9d956b32bbb641b0`.

## Approved scope and boundaries

Align both primary SQL/Delta comparison arms with
`microsoft/arrow-collector` ADR PR #9,
`docs/adr/0001-table-writer-ownership-and-routing.md`, pinned at
`1ba1e53577ba3acf3db94626bce8bb7af819a7d8`. Preserve older baseline/manifest
behavior without requiring full feature parity. Both primary arms must use the
same bounded cold-assignment recovery wait/retry policy. Gateways forward only:
no application receipt ledger or payload replay. Client/writer identity receipts,
SQL acceptance fencing, ordinary Delta visibility, recovery before ACTIVE, and
whole-table lifecycle ordering must be qualified at the real publication boundary.

The approved outcome includes fresh, finite, matched local benchmark runs,
ordinary Delta-reader and SQL reconciliation, per-request causal service timing,
and sanitized reproducible artifacts. No fresh runs have occurred for this update.
Historical report/cohort results are not evidence for the new code.

A separate standalone factory frontend lives in `microsoft/arrow-collector`.
Its offline MVP acceptance gate was explicitly passed during this continuation.
The local observation/control bridge has a draft packet schema and fail-closed
source guards, but no operational transport or executor. **Live mutations remain
disconnected until backend correctness qualification passes**. Its boundary is a dedicated local Docker
PoC, authenticated/origin-checked commands, exact target identity/incarnation and
policy revisions, arming/confirmation, idempotency, bounded real operations and
automatic fault recovery. No production/cloud administration or arbitrary Docker
proxy is authorized; the game must never hold database/Docker/signing credentials.

## Source included

- Shared placement/resource estimates, revisioned schema/stream admission,
  RECOVERING-first claims, retirement successor-recovery obligations, and bounded
  optimization drafts in `crates/arrowcol-authority`.
- Common recovery resolution policy for both gateways; opt-in Delta changes
  preserve legacy paths.
- Bounded task-local tracing, explicit trace/parent propagation, SQL and
  publication stages, result-aware error versus cancellation status.
- Lifecycle/schema/retirement regression tests; these Rust changes are uncompiled.
- Versioned benchmark JSON Schema, synthetic fixture, semantic validator,
  allowlisted exporter and regression tests in this directory.
- Draft real-runtime alignment scenario (`delta_runtime.py --adr-alignment`),
  exact-incarnation retirement checks, and benchmark source/binary manifesting.

Contract files are canonical; consumer copies must remain identical:

| File | SHA-256 |
| --- | --- |
| `contracts/benchmark-v1.schema.json` | `4e0384720d8e2ea7ef5dec9cc719aedbe3be782e83670476fa236234d3333c65` |
| `contracts/benchmark-v1.fixture.json` | `1d62d035a5bf2bc8cdea65794697d757b93a4ce61beceba091864f4549f2d7bf` |

## Portable setup and next commands

Use an isolated checkout on a Linux host with adequate disk and Docker access.
Do not copy the old host's virtualenv, target directory, credentials or runtime
configuration. Clone/fetch the transfer branch, then initialize the pinned
submodule with `git submodule update --init --recursive`. Its unchanged pin is
`1c83771289fab578bcfa2987d421ca76676d29af`; Windows requires long-path-aware Git.
The omitted long-path binary fixture was restored and matched its index object.
Cargo.lock is retained unchanged.

The workspace requires Rust 1.96 or compatible newer stable, a native C/C++
toolchain, CMake, Python with venv support, and a working local Linux Docker
engine. Further native prerequisites must be resolved from actual build errors;
protoc was previously absent but no missing-protoc build failure has yet occurred.
Install dependencies into isolated tooling/venvs, not global host configuration.

Example resume commands (not claims that they have passed on the new machine):

```sh
export CARGO_BUILD_JOBS=1
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_TARGET_DIR="$HOME/.cache/arrowcol-handoff-target"
# Ensure a real cmake executable is on PATH; do not reuse old temporary paths.
cargo fmt -p arrowcol-authority -p arrowcol-router-rust --check
nice -n 10 cargo check -p arrowcol-authority -p arrowcol-router-rust --tests
nice -n 10 cargo test -p arrowcol-authority --lib --test delta_contracts
python3 -m venv .venv-adr0001
.venv-adr0001/bin/python -m pip install -r projects/loadtest/adr0001/requirements.txt
cd projects/loadtest/adr0001
../../../.venv-adr0001/bin/python -B -m unittest test_exchange test_export_traces
../../../.venv-adr0001/bin/python -B contracts/acceptance/verify_benchmark_exchange.py \
  exchange.py contracts/benchmark-v1.fixture.json
```

Inspect `DELTA_RUNTIME.md`, `publication_gate.py`, and `delta_runtime.py` before
starting any containers. Runtime scripts currently copy native executables from
`target/debug`; if using an external Cargo target, explicitly copy the qualified
executables there before the harness freezes/hashes them. Check all required
binaries listed by `delta_benchmark.py`, Docker images, free disk, unused
project/network/subnet names, bind-mount semantics and configured resource limits.
Use unique dedicated deployments; never prune/delete unrelated resources.
Do not run the benchmark until real correctness/fault gates pass for both arms.

## Exact evidence and known failures

- The exchange validator's initial nine tests passed before later exporter work.
  An independent coordinating environment passed 19 contract acceptance cases
  with unchanged schema/fixture hashes, including duplicate keys, bounded reads,
  reference validation and asynchronous outliving work.
  The independent script, synthetic cross-language corpus, and hash-bound prior
  results are preserved unchanged under `contracts/acceptance/`. The corpus has
  17 document cases; duplicate-key and oversized-file cases are exercised by the
  script separately, yielding 19 checks. These are contract checks, not Rust or
  runtime evidence.
- `cargo fmt -p arrowcol-authority -p arrowcol-router-rust` passed, exit 0 in
  3.584 seconds on 2026-10-30. Additional source changes followed; this does not
  establish final-checkpoint formatting.
- `cargo check -p arrowcol-authority -p arrowcol-router-rust --tests`, jobs=1,
  lowered priority, native Linux target: **failed, exit 101**. `libz-ng-sys`
  could not execute the configured isolated CMake executable (ENOENT). Dependency
  compilation progressed, but there is no successful Rust source check/test.
- An independent environment passed the five exporter tests present at that
  moment in 1.943 seconds, including RPC-terminal 2 ms followed by reconciled
  receipt 200 ms, and same-process/different-clock causal links. Later source
  edits added gateway-stage coverage and a sixth mixed-category/outcome test;
  that final suite has not been run.
- One local exporter test invocation caught an indentation error, subsequently
  fixed; its retry timed out at 20 seconds during host contention. No timeout or
  wrapper exit is presented as a passing suite.
- An earlier mounted-worktree formatter stalled in `p9_client_rpc` and was stopped
  by exact owned PID. Native rustfmt startup later took 0.21 seconds versus
  5.09 seconds for one mounted-manifest read; after host recovery the targeted
  formatter completed normally. Do not assume memory starvation. Prefer native
  source/target storage if mounted I/O stalls; hash any isolated source snapshot.
- No new runtime qualification, fault run, instrumentation on/off calibration,
  fresh benchmark, real measured exchange bundle, bridge, or end-to-end live
  acceptance has been completed.

## Pending work, in order

1. Restore isolated native prerequisites and compile/check the final source.
   Review test placement and all new transitions; formatting/parsing alone is
   insufficient. Run targeted lifecycle, schema/admission, error/cancellation,
   optimization and legacy regression checks.
2. Investigate policies keyed by collector surviving Register replacement:
   eligibility/admission lookups do not consistently filter `policy.incarnation`.
   Define and qualify exact-incarnation policy behavior, including replacement,
   old acknowledgements, stale approval and retirement.
3. Qualify actual SQL/Delta acceptance/visibility fencing and recovery for both
   arms. Cover empty-deployment retirement, persisted schema policy on restart,
   missing revisioned admission rejection, retirement obligations through a failed
   RECOVERING successor and epoch-3 recovery before source teardown. The draft
   alignment scenario is not proof that all these cases run at the real boundary.
4. Finish causal/stage coverage and export validation, including asynchronous
   recovery and many-to-many batch/unit/schema/data mappings. Resource counters
   are process/container-scoped; table cold demand is estimated, not measured CPU
   allocation. Nested/parallel spans and client milestones overlap: never sum
   them as exclusive processing. TRACE=0 retains context/header overhead.
5. Freeze qualified source/binaries, run a bounded on/off tracing calibration and
   three alternating-order finite comparison pairs. Reconcile with ordinary Delta
   readers and SQL receipts; retain unknown outcomes/failures and small-sample
   tail limits. Update hardcoded historical report/evidence claims from real new
   manifests rather than relabel old results. Publish only allowlisted sanitized
   exports, reports and hashes, never raw target/runtime directories.
6. Only after explicit offline-MVP gate release, implement/qualify the guarded
   local bridge with the frontend owner. Preserve unsupported scaling/fault
   limitations rather than simulate successful live actions.

The original task remains incomplete. Resume implementation/qualification on the
new machine before deciding how to advance PR #55; never force-push or merge as
part of this transfer.

## Windows-host resume, 2026-10-30 (source work, not runtime qualification)

The starting checkpoint `fd9d4ca` and live Proposed ADR PR #9 head
`1ba1e53577ba3acf3db94626bce8bb7af819a7d8` were confirmed. Pinned submodules
were initialized recursively: otel-arrow `1c83771289fab578bcfa2987d421ca76676d29af`,
opamp-spec `18df1285f968a072912b7b3a832938824ac1acbc`, and opentelemetry-proto
`be5d58470429d0255ffdd49491f0815a3a63d6ef`.

The pending policy-incarnation concern is being repaired in source: uniform
exact-writer policy lookups, desired policy rebinding with a new revision and
cleared acknowledgement on replacement, old telemetry invalidation, and
writer/revision/operation-bound retirement approval. Approved deletion prevents
resource reuse without confirmed removal. Restart must preserve desired policy,
not silently reset admission. New regression cases cover both registration and
local-generation replacement, stale observations/approvals, retained recovery
obligations and approved-resource reuse denial. These Rust changes subsequently
passed source/test compilation; their behavior is not yet qualified.

Existing Ubuntu-24.04 WSL Python 3.12.3/jsonschema ran these lightweight checks
successfully from `projects/loadtest/adr0001` (no dependency installation):

- `python3 -B -m unittest test_exchange test_export_traces`: **passed**, 15 tests,
  including the final six exporter cases.
- `python3 -B contracts/acceptance/verify_benchmark_exchange.py exchange.py contracts/benchmark-v1.fixture.json`:
  **passed**, all 19 checks, input hashes unchanged.

Subsequent lightweight exporter repairs keep run/table-scoped logical units
across attempts, emit each physical table/version commit only once, reject
conflicting digests, and preserve failed/cancelled/timeout/rejected RPC terminals
separately from eventual receipt completion. Four added regressions expanded
`python3 -B -m unittest test_exchange test_export_traces` to **19 passing tests**;
the independent acceptance script still **passed all 19 checks** afterward.
The benchmark producer now records those explicit failure terminal categories.
No measured cohort was run or relabeled to support these source changes.

These are new source-contract checks only, not a measured benchmark bundle.
Schema/fixture hashes remain the canonical values above. Cargo, rustc, CMake
and a native C compiler were not found on the initial WSL PATH. After the parent
granted the heavy-job token, the exact repository-pinned devcontainer image was
restored successfully, without changing the shared WSL host:
`ghcr.io/mdrakiburrahman/rust-playground/devcontainer:7a6c321bf956de1f7b2f1e9154d51b8e042542e9b8d2a5646c21d3d9f8abe36a`,
resolved digest
`sha256:c3c7c0aabf082facf709fe2b5f7a2d5cd499a83364463492057268901cb6bede`.
A short, no-network, read-only container verified rustc/cargo 1.96.1, CMake
3.28.3, GCC 13.3.0, protoc 3.21.12 and rustfmt. Docker server 29.9.2 is reachable
with 32 CPUs/about 64 GiB configured; task runtime/SQL images are not restored yet.
The source was copied into task-local native WSL storage, excluding Git metadata,
target, node modules and credentials; 1,466 build-source hashes matched before
formatting, and the formatted changed files were resynchronized.

`cargo fmt -p arrowcol-authority -p arrowcol-router-rust -- --check` initially
reported formatting differences (including unfinished checkpoint test formatting).
After targeted formatting, the same command **passed**, exit 0, on the native
snapshot in the pinned isolated image. This is parsing/formatting, not a Rust
compile or correctness result.

After the parent's explicit offline MVP gate pass, the bounded native command
`cargo check --locked -p arrowcol-authority -p arrowcol-router-rust --tests`
**passed**, exit 0, in the Cargo-reported 4m20s. It used jobs=1, 2 CPUs, 8 GiB,
512 PIDs, an isolated build network, read-only source/root, dropped capabilities
and task-owned Cargo/target volumes. The prior CMake/libz-ng-sys blocker is
cleared. This compiles source/tests but does not execute correctness tests.
The exact 1,466-input source manifest, command/image/container/limit record and
full log are preserved in this session's `files/rust-check-source-sha256.json`,
`files/rust-check-result.json` and `files/rust-check.log`.

The check reported unused `Context` and optimizer `id` warnings. Subsequent
lightweight changes use `Context` for explicit Delta RSS validation, remove the
unused binding, and add an unknown-RSS regression; these later Rust changes
have been formatted but are not yet compiled or executed.

Client/report input-span timing, gate elapsed time and observer cadence now use
local monotonic intervals rather than wall-clock subtraction. Missing historical
client clock metadata yields an unavailable input span. Resource counter resets
and invalid clock intervals surface retained observation errors. Latest
`python3 -B -m unittest test_exchange test_export_traces test_observation`
**passed**, 24 tests in 1.310s; the correctly parameterized independent
acceptance command above **passed all 19 checks**, inputs unchanged. An initial
rerun omitted its two required positional arguments and failed argument parsing;
that invocation did not run acceptance checks.

Canonical schema, fixture and 17-document corpus now have per-file `-text`
attributes to preserve the already-shared physical bytes across checkouts.
The files currently use CRLF; recording those exact bytes produces only
line-ending changes against normalized Git blobs, not contract-semantic changes.
Schema/fixture SHA256 values above remain unchanged.

The heavy-job token was returned at the completed Rust-check boundary. The parent
currently holds it for bounded historical-import production-browser acceptance;
no backend heavy job is running. Await explicit return before Rust behavior
tests, runtime image restoration or benchmarks. The offline MVP gate is passed;
backend correctness is not passed. No real SQL/Delta qualification, calibration,
fresh matched cohort or operational live mutation bridge has occurred.

The next jobs=1 behavior command
`cargo test --locked -p arrowcol-authority --lib --test delta_contracts --test contracts`
initially failed four publication fixtures with read-only-filesystem errors;
after those fixtures honored `CARGO_TARGET_DIR`, a legacy manifest fixture exposed
the same assumption. Both failed attempts are retained as
`files/rust-tests-{1,2}*`. After the same target-directory correction for that
fixture, the full command **passed 37 tests** (10 unit, 7 legacy, 20 Delta) with
exit 0; exact source hashes/log/result are in `files/rust-tests-final*`.
This includes the latest RSS and incarnation-policy changes, but is not a real
SQL/Delta runtime pass. Shared native executable codegen is the next bounded job.

Finite retirement drivers now capture owned container ID plus start identity
before requesting approval, verify exact policy revision/operation/writer, and
stop only that unchanged ID. Both benchmark/runtime drivers use this guard.
Its six source regression cases initially could not import the runtime because
WSL Python lacks `pyarrow`; prerequisite restoration is authorized after that
actual failure. No runtime test success is claimed for that blocked invocation.

Frontend historical-import production acceptance subsequently passed 18 cases;
the parent explicitly regranted the heavy token. The live client contract is
`contracts/live-v1.schema.json` with `contracts/LIVE_API.md`. The separate
`python3 -B -m unittest test_live_contract` **passed 10 tests** for closed packet
shapes, Origin/auth/CSRF, freshness, arming, deployment/resource/writer/policy
fences and feed gaps. These pure guards perform no HTTP or runtime mutation.

Pinned Python restoration required an actual missing-`pyarrow` import failure,
then a failed `python3 -m venv` (missing ensurepip). `sudo -n true` was denied;
the authorized WSL root invocation successfully installed `python3.12-venv`
(including required Python patch-package dependencies), after which the exact
four `requirements.txt` pins installed into a task-local native virtualenv.
No global pip packages were replaced. All six exact-resource retirement tests
then passed with the runtime imports available.

Seven shared executables built successfully in 3m45s. A new task SQL image
`arrowcol-eec969a5/sqlserver:qualification` was built from the repository's pinned
SQL Server 2022 CU27 manifest, not an old benchmark image. Its isolated build
context normalized shell CRLF to LF; each original/transformed hash is retained
in `files/sql-build-context-transformations.json`. Runtime roles use the exact
restored Ubuntu devcontainer digest to match their native loader; no old runtime
image was relabeled. `ARROWCOL_SQL_IMAGE`/`ARROWCOL_RUNTIME_IMAGE` explicitly
select these images, with prior defaults preserved for existing callers.

Two real SQL/Delta ADR alignment attempts, unique suffixes `adr-align-1` and
`adr-align-2`, reached isolated SQL, non-sysadmin runtime SQL identity and
authenticated runtime readiness, then **failed cold ACTIVE recovery**. Publisher
workers aborted on stack overflow; all owned containers were stopped and raw
attempt evidence retained under native `target/publication-gate/`. Source
measurement found the unboxed publication-dispatch future required **1,537,232
bytes**. Boxing outer stage futures alone passed a dispatch-size regression but
still failed the second real run; it was not treated as a successful repair.
Nested Arrow writer write/finish futures are now boxed as well. Separate
dispatch/preparation future regressions enforce <=64 KiB state storage; the
latest command **passed 39 Rust tests** and rebuilt the same seven executables.
Exact source/binary hashes and output are in `files/rust-stack-repaired*` and
`files/runtime-binaries-stack-repaired-sha256.json`. A third real runtime attempt
subsequently failed as described below; the repair is not a real-boundary pass.

The heavy token is with the parent for a bounded live-client production-browser
source acceptance. Backend source work added strict bounded live metrics,
workload preset declarations, consecutive feed validation and a durable bounded
SQLite operation-intent ledger. The ledger records before any executor, preserves
requested/applied outcomes separately, rejects ID/action conflicts and supports
restart/lost-response reconciliation without replay. It contains no arm/session
secrets and invokes no runtime effects. All **48 Python tests** across exchange,
export, observation, retirement guards, live packets and ledger now pass.
HTTP/authentication/session transport, an actual observer/executor and independent
fault rollback are still unimplemented; all live capabilities remain unavailable.

The next staging guard expanded the executed Rust suite to **40 passing tests**
(13 unit, 7 legacy, 20 Delta); all seven executables rebuilt. The third uniquely
named SQL/Delta attempt `publication-gate-eec969a5-adr-align-3` again reached
authenticated runtime readiness but **failed cold recovery with publisher stack
overflow**. Its stopped containers and native run directory remain retained.
Passing future-state-size assertions did not establish bounded runtime poll stack.

Read-only disassembly of that exact failing executable found these individual
poll-frame reservations, before nested calls: publication dispatch `0x1b7218`
(1,798,680 bytes), background loop `0xfa1a8` (1,024,424), materialization
`0x123628` (1,193,512), activation `0x15268` (86,632). SQL connection polling
reserved only `0xb448` (46,152). The current source extracts/boxes background
accepted-prefix recovery and boxes a single wrapped future before constructing
the tracing helpers' async state, instead of moving generic futures into both
direct and task-local branches.
A regression bounds nested trace-wrapper storage to the wrapped future plus
4 KiB. **These newest changes have not been compiled or run**; no stack-limit
increase, recovery bypass or readiness relaxation was used.

Observer resource intervals now use observer-local monotonic receive timestamps
and an explicit origin/clock, not Docker wall timestamps. Historical samples
without that metadata remain unobserved. Reports distinguish missing counters
from observed zero and retain sample/error coverage; unmatched background spans
are explicitly dropped/unobserved rather than silently discarded.
The latest combined Python command passed **55 tests in 2.206s**. Independent
19-case acceptance was rerun after these timing/export edits: **all 19 passed**,
with canonical validator/schema/fixture hashes unchanged. The retained 40-test
log and native source/binary manifest are
`files/rust-stage-stack-40-tests.log` and
`files/rust-stage-stack-40-source-binaries-sha256.json`.

The parent currently holds the heavy token for a bounded full production-browser
rerun after a genuine paused-canvas resize failure. Backend work is lightweight
source/diagnosis only until explicit return. Offline and historical-import gates
are passed; the actual backend gate is still held. Live schema bytes remain
SHA256 `5923cc7e1fe59a91d344b7f9786de048947b4905a2d6a91558a259f548e32e20`.
No Pingora SQL/Delta qualification, tracing calibration, fresh matched pairs,
measured export bundle or operational bridge has completed. No backend commit
or push has occurred; the original draft PR branch is untouched.

An intermediate repair that pinned inside the async wrapper was actually tested
and **failed two new bounds**: materialization still required 153,872 bytes and
the task-local wrapper exceeded the wrapped-state-plus-4-KiB limit. Its output
and exact source are preserved in `files/rust-single-pin-failed*`; no binary
rebuild followed the failing test command. The eager-boxing repair replaces
that insufficient approach and is awaiting its own qualification.

Finite drivers now record each created container's exact returned ID and verify
its deployment ownership label before adopting it. Engine control/exec operations
and cleanup use recorded IDs, not recyclable names; retirement additionally
checks the exact running start identity. New SQL/etcd volumes require absent
names plus verified ownership labels, and new networks are ownership-labeled.
The expanded lightweight suite **passed 62 tests** before a final additional
registered-ID check in retirement capture. This is source-level isolation
coverage, not a substitute for inspecting the next actual deployment.

### Executed SQL/Delta qualification after tracing repair

The eager-boxing repair **passed 42 Rust tests** (15 unit, 7 legacy, 20 Delta)
and rebuilt all seven shared executables. Exact native disassembly reduced the
publication dispatch poll frame from 1,798,680 to 264,584 bytes and extracted
background recovery to 2,296 bytes. This was then qualified at the real boundary:

| Retained project suffix (`publication-gate-eec969a5-`) | Executed result |
| --- | --- |
| `adr-align-4`, `pingora-align-1` | Cold recovery, schema evolution, independent tables, exclusion/admission separation, recovered-successor retirement passed |
| `adr-faults-1`, `pingora-faults-1` | Competing claims, stale writers, failed targets, accepted-prefix SIGKILL recovery, lost replies, receipt/SQL outage and expired-controller restart passed |
| `native-faults-1` | Native publication gate passed: immutable install, paused cutover, hard failure before/after visible log, SQL durability, recovery and conflicting-prefix denial |
| `adr-policy-1`, `pingora-policy-1` | Extended alignment passed: approved-retirement cancellation denied; real collector restart retained desired controls, advanced incarnation/revision, ACKed current policy and rejected stale telemetry/policy/admission |
| `adr-gateway-1`, `pingora-gateway-1` | Eight 65,536-row inputs at concurrency four, actual second gateway, authority outage, primary-control loss and data-only scale-in passed |

These are separate finite correctness/fault runs, not benchmark phases.
Their event/command logs, ordinary Delta reads, exact source/binary hashes and
stopped inventories remain in native `target/publication-gate/<project>/`.
All owned containers stopped. Earlier failed deployments remain retained.
The source Python suite now **passed 65 tests**, including exact-ID ownership,
frozen-manifest/source verification and measured-provenance denial.

The exercised dedicated local SQL/Delta correctness boundary is qualified;
etcd and legacy runtime parity are not claimed. A real bridge still needs its
own auth/observer/executor and end-to-end acceptance; frontend mutations remain
disabled. No fresh benchmark claim is made by these correctness results.

The next fresh benchmark freezes exact dirty-snapshot source and seven common
binaries by hash, records the original checkout commit honestly through
`--revision-file`, checks source/binary stability before/after every deployment,
then executes four six-input tracing on/off calibrations followed by three
alternating-order matched pairs retaining all nine phases. Calibration/fault
runs are separate from finite comparison phases. Disabling tracing retains
context/header and wrapper allocation overhead. Historical evidence is not
relabelled; new measured exports require fresh provenance and reader/receipt
reconciliation.

The first fresh cohort `eec969a5-qualified-20261030` stopped during its first
calibration setup, before native workload, because generated SQL DNS labels
exceeded 63 characters. The failed cohort/deployment is retained, with no
measurement claim. Added preflight DNS/cohort bounds; the replacement cohort
uses the shorter new ID `eec969a5-q20261030` and fresh subnets. Shared native
binaries are unchanged.

### Retained measurement finalization failure and recovery

The second cohort completed all four real tracing calibration deployments and
the first Pingora comparison's nine phases. Finalization then rejected a CPU
counter reset observed around topology scale-in. An uncovered cleanup ordering
bug let that observer exception skip container shutdown. All nine exact recorded
owned container IDs were subsequently stopped; the original logs and attempt
metadata are retained. This is a blocked cohort, not a successful matched cohort
or a fresh export. Preserve its original attempt record before recording the
cleanup failure and manual recovery.

Cleanup now runs through nested `finally` blocks even if observation/log capture
fails. Benchmark-only partial-observation finalization retains sampling errors
and reports incomplete coverage; the observer's default remains strict.
Invalid per-container CPU intervals are unavailable, reset counter continuity,
and never create a zero or inflated CPU delta. Other valid counters and memory
samples remain available. Exports retain dropped/error counts and incomplete
coverage. SIGTERM raises an explicit interruption through normal cleanup rather
than bypassing it. These changes require a new frozen cohort, not resume or
relabeling of either failed cohort.

The current lightweight suite passed **88 tests** with cleanup and counter-gap
regressions. Independent exchange acceptance again passed **all 19 checks**;
canonical schema, fixture and validator hashes remain unchanged. No newer Rust
changes were made after the qualified 42-test/seven-binary build.

An authenticated, bounded loopback read-only transport and dedicated-runtime
observer now exist in `live_server.py` and `live_runtime.py`. Their real-socket
source tests use synthetic observations; no actual bridge deployment or frontend
end-to-end acceptance has occurred. All arming and mutation operations remain
unsupported and capabilities remain unqualified. The durable ledger is not yet
an effect executor. The parent's frozen gated frontend ZIP was verified and
staged without rebuilding; its mutation gate remains false. Do not export
bootstrap grants, cookies, CSRF values or runtime credentials.

The replacement cohort is `eec969a5-q20261030b`, using unused subnet octets
170 onward and the fixed frozen native harness. All four fresh calibrations
passed; comparison work is still in progress. Do not mix the previous cohort's
calibration into it. The native measurement tree remains unchanged while this
job runs.

Later Windows-only postprocessor/read-only observer refinements are separate
from that frozen tree. The fresh-report path verifies complete paired/calibration
workloads, receipt/multiset reconciliation, common binaries and actual stopped
container limits, and creates a new sanitized public package rather than
reusing historical report assertions. Export coverage counts each incomplete
attempt once even when both completion and stage observations are missing.
Actual postprocessor hashes are retained separately from the original frozen
manifest. The combined source suite passed **96 tests**; its log and exact
source snapshot are in `files/python-postfreeze-96-*`.

Read-only observation now retains unavailable counter-reset intervals and
dropped-sample provenance, reconnects after bounded Engine failures, and does
not label retirement approval as completed teardown while the container runs.
A separate native bridge source snapshot passed **42 bridge-only source
tests**, without changing the benchmark tree or starting a bridge deployment.
All browser actions remain unsupported/unqualified. The nine separate stopped
SQL/Delta correctness deployments were rechecked against their actual final
pass markers and stopped inventories; the sanitized persistent summary is
`files/runtime-sql-delta-qualification.json`.

### Fresh cohort finalization and bounded exchange

The newly frozen cohort `eec969a5-q20261030b` completed four new tracing
calibrations and all six nine-phase workloads: 336 inputs and 5,701,632 rows.
There were 203 streaming final RPCs and 133 eventual identity-resolved
completions. All six ordinary Delta multisets and SQL receipt versions/digests
reconcile. Earlier blocked cohorts and their four calibrations remain separate.

The outer 2,400-second deadline expired during the last deployment's final
container cleanup, after measurement. Driver exit 124 and the last raw
`blocked-cleanup` status are unchanged. All ten exact recorded final-deployment
container IDs were independently verified and stopped. The hash-bound
`measurement-audit.json`, `deadline-cleanup-recovery.json` and six
`post-stop-rescan-rN.json` files retain that recovery and independent post-stop
ordinary Delta verification. This is retained measurement evidence, not a clean
automated lifecycle pass.

The first full exchange projection failed the unchanged 32 MiB limit at
39,039,956 bytes. Shorter opaque aliases and removal of redundant attempt-to-span
containment links reduced it to 32,818,081 bytes without dropping any retained
span, interval, outcome or causal edge. Service membership remains explicit in
`trace_id`. The validated nonsynthetic exchange contains 66,670 spans and 11,771
resource records; counter-reset gaps, dropped/error provenance and
`driver-final-cleanup-interrupted` keep coverage incomplete.

The new public package is under the native cohort's `public` directory:
`benchmark-v1.json`, `measurement-report.html`, `stage-report.html`,
`provenance-calibration.json` and `frozen-source-binary-sha256.json`. The physical
exchange SHA-256 is
`0ebe91818607f86ddfcdad09382662d35ae6d8820b50a4f1367e12f93e03153c`;
the provenance/calibration SHA-256 is
`a9dd175e40e17c7d34ea8456c22c5f659143ba2203ae969c1f17472a2b4f56f6`.
The postprocessing source suite passed **97 tests** after this projection change.
Canonical schema, fixture and validator bytes remain unchanged. The native
measured source/seven binaries were not modified by later postprocessing.

No actual authenticated bridge is running yet. Read-only browser acceptance,
real receipt/Delta live observation, guarded effects and independent rollback
remain pending; frontend mutation gate and server qualification remain false.

### Actual read-only bridge and initial policy-effect core

The parent imported the real 32,818,081-byte exchange with the unchanged
production worker in about two seconds. Its subsequent consumer fix follows
normative `trace_id` membership instead of relying on redundant graph edges;
the separately pinned frontend is commit
`4b44d73cfb87b2109b863c11cef73fd118a82222`. The original `8183deb` ZIP remains
unchanged. The new production ZIP SHA-256 is
`8987e984666ab72674f31ccfe263e1b147ee244a99a638b62262526aa27ce694`;
served main JavaScript SHA-256 is
`55a0bcc19435b8f7c3c843576fe2df7bc7a2fc73f13b2f3240c3a21b1fd1be49`.

The first dedicated read-only startup (`live-ro1`) failed before creating any
containers because legacy image environment defaults were not supplied.
Its isolated network/volume and owner-only evidence remain retained. Startup
now requires explicit immutable locally available image pins before deployment
creation and never implicitly pulls those defaults. SQL content identity was
checked against the actual fresh measured inventory, not an older remembered
tag. The dedicated evidence root is owner-only before credential creation.

The replacement `publication-gate-eec969a5-live-ro2` actually served that exact
frontend at loopback port 18765. Real unauthenticated, Origin, Host and cross-site
denials passed. The parent's actual browser acceptance passed one bootstrap
POST after immediate fragment removal, fresh preselection discovery of seven
real resources/CPU/memory, explicit read-only selection, consecutive feeds,
reload session resume without inherited selection/arm, and logout. No arm or
command request occurred; missing receipt/IO/queue/throughput coverage remained
visible. Desktop/mobile captures exclude transport secrets.

After acceptance, exact-PID SIGTERM completed with exit 0. All eight originally
owned retained containers (seven runtime roles plus stopped bootstrap fixture)
were independently verified stopped, port 18765 closed, and only the consumed
private bootstrap URL file was removed. No prune or network/volume deletion.
Sanitized readiness/cleanup artifacts are `files/live-readonly-ro2-*.json`.
This is actual read-only acceptance, not full Phase2.

`live_executor.py` adds a separately staged, serialized admission/exclusion
effect core, not enabled by HTTP. It reserves durable normalized intent,
rechecks the arm/freshness before effects, fences exact resource/full writer and
native expected policy revision, preserves other desired flags, and separates
native application from matching public snapshot and collector ACK. Lost
responses and ledger reopen reconcile without replay. No arm/selection secrets
are stored in the ledger.

Two separate bounded direct SQL deployments (`live-policy1`, then final-guard
source `live-policy2`) passed actual policy mutations/ACKs, a deliberately lost
mutation response followed by ledger reopen/read-only reconciliation, blocked
new-stream admission before any receipt, exclusion without blocking an owned
table stream, stale policy/wrong-resource denial, and nine exact ordinary Delta
marker rows. The final separate policy source snapshot passed 52 source tests.
These direct effects do not qualify authenticated HTTP effects or complete
bridge-process restart. Receipt/Delta live feeds, remaining guarded operations,
durable restart integration and independent fault rollback remain pending.
Keep frontend mutation gate false and all HTTP operation capabilities disabled.

Both direct policy runs exited 0 and all eight exact originally owned containers
per deployment were independently inspected stopped. Sanitized qualification
artifacts are `files/live-policy1-qualification.json` and
`files/live-policy2-qualification.json`; the latter SHA-256 is
`7a5540b872b2cf4a3ec9c6759c5c6e016b319ad87c8ec1e92a896e6b6b97d4b9`.
The combined source command, adding `test_live_executor` to the earlier module
list, passed **107 tests** in 4.550 seconds. No bridge or native workload is left
running. The source remains an unfinished checkpoint: HTTP policy dispatch,
durable full-process restart and independent fault supervision must be qualified
before enabling the corresponding controls.

### Guarded HTTP policies and actual bridge-process restart

The policy executor now has a single bounded asynchronous slot, same-ID intent
inspection while busy, read-only reconciliation and explicit unknown/fail-stop
outcomes. The final pre-effect check consults the live server session, not a
copied arm; a disarm during blocked preflight prevents native mutation. Default
startup is still read-only. Only an explicit bounded direct qualification proof
matching the current core/schema, seven binaries and immutable images enables
admission/exclusion. All other operations remain unsupported.

New private deployment ownership manifests use exclusive hard-link publication,
file/directory fsync and an exclusive process lease. Explicit resume checks exact
container IDs, start identities, images, binary bytes and authority binding,
without recreating/adopting containers. Deployment identity and the ledger's
snapshot revision watermark persist; bridge identity and browser authority do
not. Rebinding after a crash permits TIME_WAIT reuse but never a second active
listener. The ordinary launcher stops exact owned resources; retaining them for
a bounded restart requires an explicit option.

The expanded source suite passed **139 tests** with:
`python -B -m unittest discover -p "test_*.py"`, including the Flight client
tests. Their old CPU fixture was updated to the already-qualified observer-local
monotonic clock contract: reset/duplicate/cross-clock intervals explicitly
raise, never return fabricated zero/complete coverage. No measured observer
source, cohort, canonical contract bytes or native binaries changed.

Historical attempts remain distinct:

- Direct `live-policy3` startup failed because an inherited private umask made
  the native bind directory untraversable by collector UID 21001; every created
  resource was verified nonrunning. The regular launcher now rejects that
  umask before runtime creation. A unit run under that umask also failed its
  then-unadapted startup fixture; it was not a runtime correctness result.
- `live-policy4` passed against its own earlier source snapshot. It is not the
  final core proof after the in-flight duplicate/revision-watermark changes.
- Frozen `bridge-eec969a5-policy-v5` passed 71 native-boundary tests and direct
  `live-policy5` passed four real policy mutations/ACKs, deliberate response
  loss and ledger reopen, admission/exclusion separation, exact nine Delta rows
  and all eight owned containers stopped. Its proof SHA-256 is
  `e8131f7d1f7f170cc2dddc0f9e10b9e4544edc096505f6460a2c06a4afc52fe3`.
- HTTP `live-http1` reached real SQL mutation and process crash but failed
  immediate listener rebinding; all eight resources stopped. HTTP `live-http2`
  failed the qualification driver's incorrect assumption that selection cannot
  receive a typed stale response during real observation; all eight stopped.
  Neither is relabeled as an end-to-end pass.

`bridge-eec969a5-http-v3` preserves the final sources separately from measurement.
The same HTTP source passed 74 native bridge tests. Its actual
`publication-gate-eec969a5-live-http3` qualification exited **0**:

- Actual authenticated HTTP enabled exactly admission/exclusion and rejected
  wrong-resource and CSRF requests before effects.
- The client discarded the command response. After the real SQL policy write,
  a qualification-only response interruption held the worker; exact-PID SIGKILL
  produced exit **-9**. The admission block rejected a real new Flight input
  before any receipt.
- A new real bridge process attached the same exact running deployment, issued
  new browser authority, rejected the old session and preserved a strictly
  advancing snapshot watermark. GET of the original operation completed without
  replay: native revision remained exactly 1.
- Three further HTTP policies completed with real collector ACKs. Existing
  owned-table ingestion remained possible during exclusion. Four total native
  mutations and nine exact ordinary Delta marker rows were independently read.
- Two genuine pre-effect stale outcomes (selection and arm) are retained in the
  proof; bounded resnapshot handling did not relax a fence or replay uncertain
  work.
- Final graceful bridge exit was **0**; all eight exact owned containers stopped
  and loopback port 18769 closed. No grants/credentials were exported and no
  frontend mutation gate changed.

Sanitized session artifacts are `files/live-http3-qualification.json`, physical
SHA-256 `6142f2770a9947ba2a0e87ea008a4607b265ce825d1ff477114d52c402b72d09`,
and `files/live-http3-source-sha256.json`, physical SHA-256
`ade16eb4f51d56eaf99a027d6df75f4274fed661b391286ae26708226a01389c`.
The executed scripts are separately preserved as
`qualify-policy-executor-v5.py`, `qualify-http-policy-v3.py` and
`qualify-http-policy-child-v1.py`. Each real job used an outer 600-second
timeout and a new isolated deployment; the frozen benchmark remained untouched.

This is not full Phase2. Actual browser mutation acceptance, bounded live
receipt/physical-Delta feeds, workload presets and independent bounded fault
rollback/repair remain unfinished. Replica, retirement and migration are also
unsupported; the native migration API still lacks the required atomic browser
expected-epoch fence. Keep frontend `mutationGate=false` until corresponding
server and real browser gates pass. No bridge/runtime is currently running.

### Actual live receipt and physical Delta feeds

The next separate source snapshot adds a fixed read-only SQL acceptance/final
receipt projection and bounded physical verification. Proposal digests, exact
action logs, Parquet byte lengths/hashes and contiguous prefixes must agree with
an ordinary Delta reader's version/file set before reporting completed. SQL
acceptance or merely reader-visible bytes without a final SQL receipt stays
accepted. The observer retains unknown receipt identities and coverage/drop
provenance on component failure; independent CPU/memory data is not replaced
with invented values. At most 128 receipts are exported, with explicit balanced
window truncation, and physical hashing is bounded to 32 MiB per probe.

The source suite passed **150 tests**; the separate receipt stage passed
**85 native bridge tests**. Real `live-receipts1` failed because `sqlcmd` forbids
combining `-h` and `-y 0`. Its default read-only observer correctly reported
unavailable receipts, not completion; all eight resources stopped. An isolated
network-none 128 MiB SQL-client probe confirmed the flag incompatibility without
credentials or starting a SQL service. Both failed artifacts remain retained.
The fixed query follows the existing benchmark's SQL JSON chunk handling.

`publication-gate-eec969a5-live-receipts2`, separately frozen under
`bridge-eec969a5-receipts-v2`, then passed actual authenticated observation:
two initial schema/data receipts completed; a new accepted input held before
log version 2 remained accepted with no physical log; another input held after
log version 3 installation but before its final SQL receipt also remained
accepted. Releasing each bounded qualification-only barrier produced the real
final input receipt and completed observation. Four receipt identities/digests,
bounded revisioned feeds and nine exact ordinary Delta marker rows reconciled.
Those barriers are not an enabled frontend fault operation or independent
rollback qualification.

The actual read-only server exited **0**, all eight exact owned containers
stopped and loopback port 18771 closed. Effect capabilities remained unsupported
throughout this proof, frontend mutationGate stayed false and other missing
IO/queue/throughput coverage remained incomplete. No benchmark binary/source,
cohort, contract or frontend package changed.

Sanitized artifacts are `files/live-receipts2-qualification.json`, physical
SHA-256 `bf668e32ca2fba42897cd0d1baa5f9cf1187955fc52c4ce1e190837f1eb058e9`,
and `files/live-receipts2-source-sha256.json`, physical SHA-256
`a23600984b429b20ed953626f672cf43a1671783b3307b44aa95b31734d16905`.
Executed proof scripts `qualify-live-receipts-v1.py` and
`qualify-live-receipts-v2.py` remain separate. Workload presets, independent
bounded fault rollback/repair and corresponding browser effect gates are still
unfinished; unqualified operations remain disabled.

### Independent native fault rollback, not HTTP fault support

The separate `live_faults.py` source adds a private SQLite FULL-synchronous
fault journal, exclusive worker lease and independent child process. The closed
role mapping permits collector/gateway pause, publisher pause/interruption and
dedicated SQL pause/authority loss. It accepts at most one unresolved fault and
256 durable IDs, with a 3,000 ms requested-duration bound. Final authorization
uses a one-use pipe handshake after child preflight: parent death/disarm before
authorization prevents pause; after admission, deadline rollback no longer
depends on the HTTP process. Resource ID/start/image/ownership are checked,
preexisting pauses are rejected, uncertain records only compensate, and failed
compensation remains explicitly repair-required. No browser credentials are
stored in the journal or sent to the child.

The full source suite passed **161 tests** and the separately frozen
`bridge-eec969a5-faults-v1` stage passed **96 native bridge tests**.
`publication-gate-eec969a5-live-faults1` then passed real direct-supervisor
qualification for collector pause, publisher interruption and SQL authority
loss; all three rolled back. It killed the actual supervisor process while
paused and repaired the same exact resource with `--repair-only`, then killed
the actual bridge parent and observed independent deadline rollback. Its
private qualification channel is not production HTTP authorization. Requested
duration is bounded; physical maximum pause time was not measured by this
first proof and is not claimed. In-flight workload/receipt behavior also still
requires combined qualification.

All three original ordinary Delta marker rows survived. The bridge SIGKILL
exit remains **-9**, not graceful exit; after rollback, exact cleanup stopped
all eight containers, port 18772 closed and zero exact fault workers remained.
Sanitized `files/live-faults1-qualification.json` has physical SHA-256
`f970ade0c0cc16e731dfee7d5645e6836cbeb71579139a5784f7afd02c8a4b32`;
`files/live-faults1-source-sha256.json` has physical SHA-256
`e23c775a9b0ac629d6f085bdcb6dcfd949e23a9389eb95b17ac1326e50b74106`.
The fixed proof's `max_pause_ms` denotes the configured requested-duration
limit, not a measured physical-duration result.

HTTP fault/repair/workload controls remain unsupported and frontend
mutationGate remains false. Shared contracts, measured sources/binaries/cohort
and frontend packages were untouched. Next integrate the helpers with the
shared bounded executor, qualify actual guarded dispatch/reconciliation,
measure rollback timing and retain in-flight receipt/Delta outcomes.

### Real finite preset workloads and measured direct fault lifecycles

`live_workload.py` now fixes two server-owned, single-stream `main`/baseline
presets: small is 3 rows/request, at most 8 requests and 10 seconds; elephant
is 65,536 rows/request, at most 2 requests and 15 seconds. Exact saved gateway
container/start/image, loopback binding, nonnullable schema and native
fingerprint are checked before upload. Browser table/schema/headers/endpoints
are not accepted. The existing FULL-synchronous operation ledger holds bounded
private jobs, logical request/attempt IDs, non-overlapping marker allocations,
single-send decisions, native unit identities, RPC/final/close outcomes.
Reopening or retrying a stored ID never sends payload again. Missing native
identity remains unknown; unsent partial work is cancelled. Only fresh matching
physically verified receipt observations can complete all requested units.
RPC errors/timeouts remain independently recorded even after completion.

The first separate `live-workload1` attempt failed its HTTP driver assertion
during 50 ms capability polling. The status/body were not retained, so its
precise error is unavailable; that polling can exceed the server's unchanged
12 requests/second/session limit. The second explicitly paced driver reached
real native preflight and exposed a genuine helper bug: its schema comparison
had assumed nullable `id`, whereas the qualified baseline is nonnullable.
Both unique failed stages/deployments remain retained, all eight exact
resources stopped, and no RPC was sent by the failing preflight. Added exact
native-schema/port/fingerprint regression fixtures; neither server bounds nor
native schemas were weakened.

The final source passed **171 full Python tests** and its independent frozen
`bridge-eec969a5-workload-v3` stage passed **106 native bridge tests**.
`publication-gate-eec969a5-live-workload3` then completed two small requests,
one elephant request and another small request during a real 2-second publisher
pause. All four native unit identities and physical receipts reconciled;
same-ID calls and a reopened journal caused no replay. Ordinary Delta scanning
matched the exact **65,548-row multiset**.

Fault compensation records now retain the observed repair timestamp and Linux
boot identity. Inclusive pre-pause-dispatch-through-observed-rollback intervals
were 680,887,020 ns (collector, requested 500 ms), 697,101,693 ns (SQL authority,
requested 500 ms), and 2,235,467,065 ns (publisher, requested 2,000 ms). These
same-kernel CLOCK_MONOTONIC control lifecycles each passed the explicitly tested
requested-duration-plus-five-seconds bound. They are not CPU measurements,
one-way transport estimates or wall-clock latency subtraction.

The default read-only bridge kept every effect capability unsupported. It
exited **0**, all eight exact containers stopped, the bounded supervisors exited
and port 18775 closed. This proof uses a trusted native helper channel, not
guarded HTTP dispatch, and does not replace the independently retained
SIGKILL/emergency-repair proof. Sanitized
`files/live-workload3-qualification.json` has physical SHA-256
`55e927e9c7fdfcf5fc1e5d91ac5cd394b6347fadd764d4f30ca1d234e4b8000f`;
`files/live-workload3-source-sha256.json` has physical SHA-256
`75f6f54cb0fc6ef1b2a8b345bbb25027acfaa61680fd49ec98e50ab780b565b2`.

Shared bounded executor integration, actual guarded HTTP workload/fault
dispatch/reconciliation and corresponding browser effect gates remain
unfinished. Frontend mutationGate stays false. Frozen measured source/binaries,
cohort, canonical contracts and frontend packages were untouched.

### User-directed benchmark priority; bridge checkpoint paused

The integrated policy/workload/fault admission source passed 182 full Python
tests. Its separately frozen workload-v5 prerequisite passed 117 native bridge
tests and actual four units/65,548 Delta rows, independent bridge/supervisor
SIGKILL rollback and exact stopped cleanup. All seven binary bindings now
verify; v4's incomplete five-binary activation binding remains retained.
Fresh direct policy-v6 passed four native policies/ACKs and nine Delta rows.

Guarded HTTP effects are not fully qualified. HTTP-effects1 completed its four
policy/restart checks then failed a driver status expectation: unsupported is
HTTP 503, not 409. HTTP-effects2 corrected that expectation and completed actual
small/elephant workloads, but its collector fault later reached an explicit
pre-dispatch `rejected/stale` result. The driver had inspected an earlier
nonterminal operation record; this is not proof of native fault application.
Both separate attempts are retained. Latest exact cleanup verified all eight
containers stopped and port 18780 closed. No runtime/bridge job remains owned
or active; unqualified browser effects stay disabled.

The user then explicitly prioritized local PoC benchmark numbers for Proposed
ADR0001 over further bridge/game work. That work is paused, not completed.
Read-only extraction of the frozen `eec969a5-q20261030b` inputs confirms two
actually written/visible TableKeys (`main`, `cas`) in all six runs; the third
configured key `dictionary` was unused. The elephant phase's one written key
also changes batch size/history and is not a table-count sweep. There is no
increasing-table-count series or isolated network-throughput/one-way latency
result in this delivered cohort.

The sanitized handoff is session `files/adr-local-poc-results.md`, with exact
per-run derived values in `files/adr-rough-measurements.json` (SHA256
`b6bbcedadf9e569c32b1e3a229b33ed507f318b1dccbd868b8a86196c4f3f90f`).
It separates finite end-to-end rows/s, censored RPC latency, eventual completion
and local inclusive service spans, preserves the 53,222 drops/cleanup caveat,
and proposes a bounded 1/2/8-table sweep without pretending a table-count CLI
already exists. Parent owns the ADR section update. Frozen benchmark evidence,
native binaries, canonical contracts and frontend packages remain unchanged.

### Narrow actual-TableKey sweep driver

The user subsequently authorized the proposed minimal measurement.
`table_count_sweep.py` now exists as a standalone driver; it imports only an
explicitly selected frozen native harness, not the later bridge modules.
It verifies the original manifest, four native harness source identities and
all seven frozen/host binary identities before execution. Six serial cases
use actual allowlisted TableKeys at cardinalities 1, 2 and 8, alternating arm
order. Each fresh deployment uses the same nonnullable int64 schema, 8 warm-up
requests then 24 timed requests, 4,096 rows/request, concurrency 2, three
collectors and one gateway. No topology changes or fault cases are included.
Tracing stays on; cgroup sampling is off in both arms. Generated native role
configurations retain their original file modes after prestart allowlist
replacement. No native source or executable is changed.

The first separate `eec-ts1` setup hit PermissionError on the generated
read-only collector configuration; both created resources stopped. Its failed
artifacts remain. The surgical owner-write/restore repair has a regression;
all four targeted driver tests pass. The newly frozen `table-sweep-v2` driver
SHA256 is `1a5aa2398ebf7443cd55fbfaeba2aad66977f93a3fae32dc9acb8f0679ae2d49`.
Its actual invocation is:

```sh
timeout --signal=TERM --kill-after=60s 1200 \
  "$POC_PYTHON" -B "$SWEEP_DRIVER" \
  --repo "$FROZEN_NATIVE_ROOT" --cohort eec-ts2 \
  --frozen-cohort eec969a5-q20261030b --subnet-start 207
```

`POC_PYTHON` is the existing qualified Python environment; `SWEEP_DRIVER` is
the separately frozen new script, not a file copied over the measured harness.
`FROZEN_NATIVE_ROOT` contains the exact retained dirty source/binaries and
original cohort. Reuse requires a new cohort and verified-unused subnet range,
not replay of the immutable `eec-ts2` directory. Current CLI help lists the
bounded options; there is no arbitrary payload, endpoint, credential or shell
input. Actual endpoints are loopback-exposed gRPC with a fresh client/channel
per request and an isolated Docker upstream.

Every case retains raw attempts privately, verifies exact per-unit SQL
receipt table/version/digest and ordinary Delta row multisets for every key,
and bounds preparation attempts at 64/128. Rows/s and logical payload MiB/s
include completion polling and standard-reader scans. Successful RPC elapsed,
physical-final-receipt observation, and post-completion reader observation
stay separate, with nonfinal RPC counts preserved. Gateway stage intervals
are local inclusive observations, never isolated network measurements.
Spreading a fixed number of inputs across more tables also shortens per-table
commit histories; this experiment cannot isolate that effect from routing.

The parent grants each case 180 seconds plus bounded termination cleanup;
the outer invocation has a 1,200-second deadline. Cleanup targets saved exact
IDs/start times/images/owner labels, stops only this case's resources, and
independently verifies stopped state before another case. Failed cases stop
the series rather than being silently replayed. JSON/CSV outputs are under
`target/table-count-sweep/COHORT`; private method argv/logs and deployment
directories must not be blindly published. Final sanitized results are a
separate handoff, not a relabeling of the nine-phase frozen cohort.

The `eec-ts2` sweep subsequently completed all six cases, overall exit 0.
Each case verified 32 data receipts plus one schema receipt per actual key
and 131,072 exact ordinary-reader rows; each of its 64 preparation attempts
remained below the unchanged 128 bound. All 45 exact saved container IDs
were independently verified stopped, with the seven native binaries still
byte-identical. Timed throughput and successful RPC / physical receipt P50s:

| Tables | Arm | Rows/s | Logical MiB/s | RPC P50 ms (n) | Physical receipt observed P50 ms (n) |
|---|---|---:|---:|---:|---:|
| 1 | Pingora | 1087.13 | 0.008294 | 6502.13 (20) | 6990.22 (24) |
| 1 | Flight ADR | 1089.15 | 0.008310 | 5851.87 (18) | 6943.35 (24) |
| 2 | Pingora | 2288.65 | 0.017461 | 3548.08 (24) | 3379.73 (24) |
| 2 | Flight ADR | 2352.81 | 0.017950 | 3640.31 (24) | 3432.68 (24) |
| 8 | Pingora | 3877.21 | 0.029581 | 2090.67 (24) | 1842.11 (24) |
| 8 | Flight ADR | 3962.83 | 0.030234 | 1862.45 (24) | 1640.82 (24) |

Four Pingora and six ADR one-table RPCs were nonfinal but all completed by
separate exact-unit receipt resolution, without payload replay. Unresolved
requests were zero; the censored RPC samples remain distinct from all-unit
receipt observations. All other timed cases had 24 streaming-final RPCs.
The fixed total uploads produce per-table Delta versions 32/16/4 at 1/2/8
tables, so accepted-prefix history and writer/SQL work confound throughput
scaling; this is not isolated network capacity.

Flight timed local assignment-resolve P50s were 0.03096/0.03521/0.03455 ms
(24 spans each), and upstream-connect 1.6667/1.7358/1.8219 ms (24 each).
Pingora assignment spans only cache-miss control resolution while Flight
wraps its resolve including cached lookup, so these differently bounded
spans cannot establish a direct transport-speed ratio. No one-way latency,
wire throughput, exclusive CPU or complete trace coverage is claimed.

The sanitized package is session `files/table-sweep-eec-ts2/`, with concise
method/results in `README.md`. `results.csv` (1,012 bytes) SHA256 is
`95c18bc33c210b1ff58dbbfc89bf1a8fc04fc26a28fbacc4e0c8a152cadc6015`;
`results.json` (159,904 bytes) is
`f760af74eea9906bfaab63b193e8438e3305789d18284064979d8f7e7630d239`;
`method.json` (18,705 bytes) is
`d40a11a1e6fe39e495dafa395baed7bfe708672cd371aec8bce42c03105ce95c`.
The included driver retains SHA256
`1a5aa2398ebf7443cd55fbfaeba2aad66977f93a3fae32dc9acb8f0679ae2d49`.
Parent owns Proposed ADR results integration. Bridge work remains paused,
frontend mutations false, old PR branch and original frozen cohorts untouched.

### User-directed live bridge resumption and selected fault preflight

After the completed table sweep, the user explicitly resumed the original live
game requirements. Safe staff capacity/retirement, bounded client concurrency,
fault/repair controls and truthful component observations remain pending
implementation/qualification, not permanently out of scope. Frontend owns the
scene/forms and its mutation gate remains false. The approved work reuses the
existing executor and seven frozen binaries; benchmark bytes are not relabeled.

HTTP-effects3 completed the four policy/restart checks and fixed small/elephant
workloads but rejected all four collector fault intents `stale` before native
application. Read-only diagnosis measured fresh-child import 0.428 s plus
full attachment/binary checks 1.617 s, total 2.045 s, against the unchanged
one-second snapshot revision cadence. All eight exact resources stopped.

`FaultResource` now binds only the selected saved resource for the independent
pause/unpause worker. The immutable manifest, deployment incarnation,
allowlisted role/fault/duration, exact ID/start/image/owner and journal intent
must match; other resource names are denied. Native executables/credentials
are unnecessary in this child. Full parent attachment and final live freshness,
session/policy/resource identity checks remain unchanged. Before application a
dead parent cannot issue the one-use grant; after application deadline rollback
and emergency repair do not require the parent or UI authority. Source
regressions cover both parent-loss boundaries; all 120 live-bridge tests passed
on the current separately staged sources and unchanged native harness.
Selected-resource read-only startup measured 0.252 s in one diagnostic probe.

One bounded HTTP-effects4 deployment on subnet 214 / port 18782 then completed
all four policies/restart, two small inputs plus one elephant input (three
physically completed native units / 65,542 workload rows), and both collector
pause and SQL authority loss on their first intents. The publisher pause also
applied and rolled back, but the driver asserted native `paused` immediately
after HTTP `pending`, before native pause observation. Its HTTP record remains
pending; no publisher completion, overlap/crash qualification or final combined
Delta multiset is claimed. The corrected private fixture only polls the same
saved intent to observed `paused`, with a two-second bound; no replay,
freshness/retry relaxation or additional capability was introduced.
This attempted fixture exited 1; all eight exact containers were independently
verified stopped and port 18782 closed. No complete HTTP-effects proof exists.

Current-source startup cannot reuse the old whole-file workload-v5 proof. The
new verifier supports a separately scoped guarded-HTTP proof with unchanged
source/seven-binary binding and explicit native/HTTP/cleanup checks. Private
operator qualification fixtures explicitly substitute the evidence admission
gate, never the actual HTTP/session/arming/freshness/identity/effect code. That
bootstrap is not normal production activation or browser acceptance. A complete
fresh artifact and subsequent normal nonfixture gated startup are still needed.
All historical helper proofs and failed stages stay immutable and separate.

At this boundary no backend job is running. The parent temporarily owns the
heavy token for observation-only frontend browser checks; after return, the
approved next bounded checkpoint is the corrected shared HTTP barrier and
actual preflight-parent loss, post-application parent loss, worker kill/repair,
same-ID reconciliation and final Delta audit. Do not start a broad helper cohort
or advertise pending staff/migration/repair/concurrency capabilities.

### Guarded HTTP-effects6 passed; normal source-gated effects observed

The next source-identical HTTP-effects5 attempt passed the corrected native
pause barrier, publisher-overlap completion and actual preflight-parent death
without application. Its fixture then looked for the fault child under only
the bridge's main-thread `/proc` task, although the executor thread spawned it.
No worker kill or post-application owner-kill pass was claimed for that attempt.
It exited 1; eight exact containers stopped and port 18783 closed. Its preserved
sanitized partial JSON SHA256 is
`56c778fc2823d8b7e760fd0513e9c73e2b3c327cd814c50c747bdd7cd775f7b7`.

HTTP-effects6 changed only the private worker-PID lookup: deduplicated children
from the known bridge's tasks, required one exact current script/project/op-ID
argv match, and used pidfd-bound SIGKILL with a second exact argv check. The
eleven source/schema bindings remain identical to effects5 and source checkpoint
`53a2010`; no Rust rebuild, contract, cadence, duration or retry changes occurred.
Its one bounded deployment on subnet 216 / port 18784 passed:

- Four native policies, interrupted HTTP/native response, actual bridge SIGKILL,
  exact restart, old-session rejection and durable same-ID reconciliation.
- Small two inputs, elephant one, and small one during publisher pause: four
  physical native units and the exact ordinary Delta **65,554-row multiset**.
- Collector pause, SQL authority loss and publisher interruption; actual parent
  death before grant prevented application; worker SIGKILL/exact emergency
  repair and parent SIGKILL after application independently rolled back.
- Same-ID queries never replayed payload; all five applied faults repaired on
  the same Linux boot within requested duration plus five seconds.

Inclusive dispatch-through-observed-repair intervals were 975.456 ms and
1,118.094 ms for the 500 ms collector/SQL faults, 3,562.329 ms for a 3,000 ms
publisher fault, 840.398 ms for early worker-kill/emergency repair, and
3,547.640 ms for a 3,000 ms owner-kill/deadline rollback. These are local
monotonic control lifecycles, not one-way transport, CPU or benchmark results.
Final bridge exit was 0; eight exact IDs stopped and port 18784 closed.
The raw private whole-source/seven-binary artifact SHA256 is
`34655447338e3fc2e23edbcc890629ed6ead0c6ca78992ad17456ae1c3048dd4`.
Its allowlisted `files/live-http-effects6-qualification.json` export SHA256 is
`a3ca173339d892b433f98e4c22a7aab0be971913ee3ce3a1015c860c835a75af`.
Private operator bootstrap admission is explicit; no production bypass was added.

Normal startup on new subnet 217 / port 18785 used **unmodified
`live_runtime.py`**, matching policy-v6 and the new HTTP-effects6 artifact,
without qualification override. It completed an authenticated small workload
and collector fault, same-ID no replay, exact three-row ordinary Delta audit,
reload/logout and graceful bridge exit 0. The enclosing smoke fixture then
failed an incorrect all-eight-running retention assertion: the qualified PoC
has seven running observed roles plus an intentionally stopped legacy collector
fixture. Its final cleanup independently stopped all eight and closed the port.
Normal source-gated effects did pass; whole smoke exit remains 1, not 0.
`files/live-normal7-partial.json` SHA256 is
`c20988219cfe6dabbaf98ad2f384ba20832059eead3a5fa04eef507419f483eb`.
The corrected private fixture captures and compares the exact baseline running
map; it never starts or adopts the stopped collector fixture.

At this boundary the parent holds the heavy token for a user-reported touchpad
camera fix/browser check. No backend process is running. The parent's playable
simulation preview at port 4175 is unrelated and must not be stopped. Latest
separately pinned observation-only frontend `fd83efb` ZIP/main hashes were
verified and staged privately without rebuild, but actual bridge-browser
availability remains pending the corrected normal restart and camera package.
Staff/retirement, bounded streams, atomic migration and HTTP repair remain
original pending requirements; full Phase2 is not complete. Missing live
IO/queue/throughput metrics remain unknown and frontend mutationGate is false.

### Normal8 browser closeout and coordinated fleet foundation

The corrected normal8 fixture used the same unmodified source-gated launcher
and seven frozen binaries on subnet 218 / port 18786. It compared the exact
seven-running/one-stopped birth map, completed actual authenticated workload and
collector fault, same-ID reconciliation, ordinary three-row Delta audit and
reload/logout, then exited 0. Its separate sanitized artifact
`files/live-normal8-qualification.json` SHA256 is
`99df5644a8ae1eb39fcd1624e3b82b7d2988a7c645aed3bd680eece895c3c5a2`.
Normal7's incorrect retention assertion and exit 1 remain preserved.

The parent subsequently passed the actual read-only browser against normal8
and the separately frozen `bf77762` touchpad package, main SHA256
`77379a2bfd36f2c959dec05b8148756acfc8fe02182d2e49526535cb624ed448`:
seven observed stations, exact read-only resource selection, real CPU/memory,
consecutive feeds, session/reload/logout, no stored secret, arm or command, and
no page errors. Parent-owned `normal8-readonly-acceptance.json`/screenshots are
browser evidence, not a mutation qualification.

After that browser closed, the backend matched its private owner-lease inode
to one exact PID/start-ticks/argv identity and sent pidfd SIGTERM. The bridge
exited 0; all eight exact saved containers were independently stopped and port
18786 closed. `files/live-normal8-cleanup.json` SHA256 is
`91a08cc54da5277b54f87439e8e77ba7d0074acf386a377fe97bedcac3791cda`.
One observer TimeoutError diagnostic is retained. No backend runtime remains;
the parent's intentionally playable simulation at 4175 is untouched.

The shared **unadvertised** contract draft lives in session
`files/live-safety-streams-draft/` with exact copies, `canonical.patch`,
reproducible fixture builder and `sha256.json`:

| Draft file | SHA256 |
| --- | --- |
| `live-v1.schema.json` | `ddb3f534b8dab2507d0293c9cd035a2c017bf38a1c124a9cdd8b01346c4c3cf3` |
| `LIVE_API.md` | `f29b909f705da35a7874405c47bb35bbce72043b9a6605eff87898ecd61ad5b7` |
| `live-v1.contract-fixtures.json` | `9886fd92ed7ee2def65d3f773e009399b6f06be1d394db1786d59dc2d54f67bf` |

Its 46 cases are synthetic structural/field-guard cases, not native authority.
Required nullable replica `identity_verified/running/paused` must come from
fresh exact private probes; snapshot `receipt_observation_known` is true for
a healthy empty probe and null after stale/failed/unavailable probes. Workload
requires receipt health exactly true before admission. Only the eight explicit
CPU/memory/IO/queue/throughput diagnostic labels can remain missing without
authorizing a safety bypass; critical/unrecognized missing facts still deny.
Required caller `streams` is 1..4 and cannot exceed a preset's qualified maximum.
Common auth/revision/arm/gap/source fences and independent rollback are unchanged.
Without a public gateway-binding descriptor the client conservatively checks
all advertised gateways, not "any healthy gateway".

The parent adopted that exact draft in frontend `8f354ef`, but retained global
coverage/mutation/pending-ID gates. Its new draft-only package must not be served
against the old backend contract. Repository/runtime schema remains unchanged
at `5923cc7e1fe59a91d344b7f9786de048947b4905a2d6a91558a259f548e32e20`.
Do not adopt/advertise the new fields until same-contract server implementation
and fresh source-bound qualifications are delivered.

`live_inventory.py` now implements a **private creation-intent foundation only**:
exclusive lease, bounded FULL-sync SQLite transactions, inventory-revision CAS,
same-ID collision/reopen protection, immutable birth manifest, separate managed
slots excluding the stopped legacy fixture, and unique deployment/creation-nonce
plus exact ID/name/image/StartedAt reconciliation. Missing/ambiguous/changed
resources never cause another create or adoption by name. Bindings are not
ACTIVE/readiness evidence; this module creates/stops nothing and grants no
retirement permission. It is not yet imported by the bridge, so existing
eleven-file native proof bytes remain unchanged.

Existing WSL venv, pinned image environment, `python -B` unittest selectors
`test_live_inventory test_live_deployment test_live_ledger`: 28 passed in
2.360 seconds, including 12 new synthetic inventory cases, transaction
interruption/concurrent CAS and actual fixture-child exit 37 after durable
intent. No Docker/native binary was invoked. These are source/persistence tests,
not a live scale-out/retirement proof.

Next remain authoritative creation/isolation/config/readiness adapters,
collector approval/ACK/ACTIVE-successor/obligation/capacity checks and exact
teardown, gateway admission/drain, safety/streams implementation and fresh
qualification. Approved native collector tombstones cannot be canceled/reused;
the current collector1..4 credential pool permits only a bounded initial
3-to-4-to-3 cycle, not generally repeatable staff scaling. Exhausted fresh
identity must remain explicit until a fresh-identity/config-registration path
is qualified. Atomic migration, HTTP repair, missing telemetry and full game
mutation acceptance remain unfinished original requirements.
