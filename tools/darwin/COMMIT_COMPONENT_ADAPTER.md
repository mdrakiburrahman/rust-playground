# Commit component adapter

| Interface | Contract |
|---|---|
| `evaluateCommitComponent(host)` | Runs three alternating pairs serially and returns a component-only diagnostic; it never scores a candidate. |
| `host.control`, `host.candidate` | Trusted runner's pre-admitted revision, executable, fixture and entry-point hashes. Fixture and entry point must match. |
| `host.scratchRoot` | Existing canonical private directory, outside the candidate's control. Each cell receives a fresh result path. |
| `host.runAdmittedCell({arm, pair, resultPath, tablePath})` | Trusted runner selects the pinned executable for the arm in its existing isolation, invokes it with `--result resultPath --table-root tablePath`, waits for the entire process tree to settle, and returns the verified pin, exit code and `settled: true`. It must not use a candidate-selected binary, path or stdout. |
| `host.reconcileStoppedCell(tablePath)` | Trusted runner runs its separately admitted `delta-inspect summarize` oracle after settlement and passes its output to `reconcileStoppedCommitTable`. The oracle must be built and pinned independently of the candidate. |
| `reconcileStoppedCommitWithOracle(host, tablePath)` | Component-owned helper for that callback. It requests the admitted oracle with fixed arguments, 240-second deadline, 256 MiB stdout, 1 MiB stderr and 4 GiB memory bounds, rejects nonzero or unsettled execution, and reads only bounded regular JSON output. |
| Result | `valid: 0`, `combined_score: 0`, `accepted: false`; a same-pin A-A run additionally returns an explicitly diagnostic wall-time ratio. |

The host must enforce admission, executable identity and isolation independently;
this module is not a sandbox and its callback is trusted, not candidate input.
An exit-zero, settled cell, independently reconciled stopped table and bounded,
complete result file are mandatory. The fixture retains and syncs files only when
given a fresh empty table root; normal runs still clean up. The reconciler checks
the independent delta-rs snapshot and decoded Parquet row hashes against fixed
inputs, transaction identities, commit cohorts, Adds and physical file sizes.
It rejects extra commits or files and does not rely on the candidate's self-check.
The host must enforce a deadline and resource bounds for the oracle as well as
isolation and settlement for the candidate. A final table with no duplicate credit
cannot alone prove that replay was attempted; protected fixture execution is also
required. Candidate scoring stays disabled until real admission and settlement
are enforced by the runner.

For the oracle helper, `host.scratchRoot` and its fresh output directory must be
outside candidate write access. `host.runAdmittedOracle` must independently
select and verify the admitted executable; enforce all requested time, output
and memory bounds; and prove whole-process-tree settlement. The helper does not
provide a sandbox or independently verify those host guarantees. Timeouts,
cancellation, resource failures and unproved settlement fail closed; an
unsettled execution must be quarantined rather than followed by another cell.

The fixed synthetic case retains at most 16 MiB of Arrow input, not total process
memory. Its 2-second commit window dominates wall time; A-A calibration is not
a code-speedup, DFE ACK, Azure upload or end-to-end throughput measurement.

| Check | Command |
|---|---|
| Focused tests | `npx --no-install tsx --test tools/darwin/src/commit-component-adapter.test.ts tools/darwin/src/commit-component-oracle.test.ts` |
| Known pinned fixture A-A | Build the `81e0f384` fixture and `delta-inspect` in release mode, then run `NODE_OPTIONS=--max-old-space-size=4096 DARWIN_COMMIT_AA=1 npx --no-install tsx --test tools/darwin/src/commit-component-adapter.test.ts`. The opt-in diagnostic requires the exact recorded executable and source hashes; rebuilds with different toolchains may not match. It retains its scratch results and is not an isolation test. |
