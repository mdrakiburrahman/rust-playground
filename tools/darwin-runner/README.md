# Darwin runner

The runner pins Darwin 0.3.0 and its Debian bookworm toolchain. Candidate containers use the same Rust/Node toolchain, no host workspace or credential mounts, and no network. Controller commands use a clean committed checkout; the ignored `.env` and encrypted token file are never mounted as workspace content.

| Command | Purpose |
|---|---|
| `tools/darwin-runner/run.sh version` | Build pinned images and print the Darwin version. |
| `tools/darwin-runner/run.sh dry-run tools/darwin-runner/toy.yaml` | Validate the tracked toy config and run Darwin's no-token dry-run. |
| `tools/darwin-runner/run.sh prepare [run-id]` | Create an age key volume and print its public recipient. |
| `tools/darwin-runner/run.sh evolve <config> <experiment> <run-id>` | Fail closed while native Docker-provider evolution is blocked. |
| `tools/darwin-runner/run.sh cleanup <run-id>` | Remove an unused encrypted input and private-key volume. |
| `npx --no-install tsx --test tools/darwin-runner/*.test.ts` | Run manifest, dispatch-contract, and deadline tests. |

## Native Darwin status

The pinned executable reports `darwin 0.3.0`, but its frozen bundle does not include `python_on_whales`. The campaign's Docker-backed config therefore fails before lane selection. No compatible checksum-pinned external runtime loader or complete official distribution is available in this branch. `evolve` is disabled; do not stage credentials or infer with this artifact. The minimum unblock is a compatible complete Darwin distribution that includes the native Docker provider and dependencies.

## Direct bridge contract

`dispatch-contract.ts` accepts only `{version: 1, lane: "compression-config", candidateCommit: "<full-sha>"}`. The trusted worker supplies the repository, producer commit, fixed config path, and complete config validator; candidate input cannot choose a path, command, environment, or evaluator. Validation rejects non-descendant commits and any changed path beyond the one trusted config before invoking the workload callback. The tests use committed fixture Git refs and an inert compression-only validator to prove distinct levels and fail-closed rejection. This is a direct bridge contract test, not Darwin evolve, OAT evaluation, or production worker integration.

`deadline-state.ts` stores a run's exact UTC deadline outside the repository and refuses a changed or expired deadline on resume. `deadline-runner.ts` pins this campaign to `2026-11-23T05:06:42.133Z`, bounds a child process by the remaining budget, and reserves up to 30 seconds for termination and container cleanup. The direct tests exercise persistence, expiry, cleanup reserve, and process termination with no token. Reuse the same run ID when resuming; these helpers do not establish native Darwin confinement.

`config-check.ts` is an offline strict check for the ten-lane root manifest and literal evaluator commands; it is not Darwin's production admission enforcement. The toy config is intentionally separate and has no inference or Docker evaluator.
