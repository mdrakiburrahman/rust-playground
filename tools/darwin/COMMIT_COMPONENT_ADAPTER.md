# Commit component adapter

| Interface | Contract |
|---|---|
| `evaluateCommitComponent(host)` | Runs three alternating pairs serially and returns a component-only diagnostic; it never scores a candidate. |
| `host.control`, `host.candidate` | Trusted runner's pre-admitted revision, executable, fixture and entry-point hashes. Fixture and entry point must match. |
| `host.scratchRoot` | Existing canonical private directory, outside the candidate's control. Each cell receives a fresh result path. |
| `host.runAdmittedCell({arm, pair, resultPath, tablePath})` | Trusted runner selects the pinned executable for the arm in its existing isolation, invokes it with `--result resultPath --table-root tablePath`, waits for the entire process tree to settle, and returns the verified pin, exit code and `settled: true`. It must not use a candidate-selected binary, path or stdout. |
| `host.reconcileStoppedCell(tablePath)` | Trusted runner runs its separately admitted `delta-inspect summarize` oracle after settlement and passes its output to `reconcileStoppedCommitTable`. The oracle must be built and pinned independently of the candidate. |
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

The fixed synthetic case retains at most 16 MiB of Arrow input, not total process
memory. Its 2-second commit window dominates wall time; A-A calibration is not
a code-speedup, DFE ACK, Azure upload or end-to-end throughput measurement.

| Check | Command |
|---|---|
| Focused tests | `npx --no-install tsx --test tools/darwin/src/commit-component-adapter.test.ts` |
| Known pinned fixture A-A | Build the `6fa4c077` release fixture, then set `DARWIN_COMMIT_AA=1` for the focused test. The test verifies the exact known fixture and binary hashes and preserves its scratch results. This diagnostic is not an isolation test. |
