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
Its offline MVP acceptance gate remains held. The later local observation/control
bridge in this repository is **not implemented and must not start until that
gate is explicitly released**. Its planned boundary is a dedicated local Docker
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
