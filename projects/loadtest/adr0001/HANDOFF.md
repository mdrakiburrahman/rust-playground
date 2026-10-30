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
