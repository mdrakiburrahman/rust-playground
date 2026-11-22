# Darwin runner

The runner pins Darwin 0.3.0 and its Debian bookworm toolchain. Candidate containers use the same Rust/Node toolchain, no host workspace or credential mounts, and no network. Controller commands use a clean committed checkout; the ignored `.env` and encrypted token file are never mounted as workspace content.

| Command | Purpose |
|---|---|
| `tools/darwin-runner/run.sh version` | Build pinned images and print the Darwin version. |
| `tools/darwin-runner/run.sh dry-run tools/darwin-runner/toy.yaml` | Validate the tracked toy config and run Darwin's no-token dry-run. |
| `tools/darwin-runner/run.sh bridge-test` | Exercise the trusted Unix-socket bridge between isolated, networkless containers using inert Git fixtures. |
| `tools/darwin-runner/run.sh prepare [run-id]` | Create an age key volume and print its public recipient. |
| `tools/darwin-runner/run.sh evolve <config> <experiment> <run-id>` | Fail closed until model, scorer, and evaluator boundaries are proven. |
| `tools/darwin-runner/run.sh cleanup <run-id>` | Remove an unused encrypted input and private-key volume. |
| `npx --no-install tsx --test tools/darwin-runner/*.test.ts` | Run manifest, dispatch-contract, and deadline tests. |

## Native Darwin status

The pinned executable reports `darwin 0.3.0`. Its CLI exposes `evolve` and `dry-run`, but no deterministic no-credential model stub; the sandbox reference confirms that evolution requires a Copilot token. The standalone runtime uses passthrough, which runs evaluator code with the controller's privileges. A Bookworm container alone does not provide an independent scorer or credential boundary, so `evolve` remains fail-closed. No credential or inference was used.

## Trusted Unix-socket bridge

`bridge-listener.ts` accepts only `{version: 1, lane: "compression-config", candidateCommit: "<full-sha>"}` over a Unix socket outside the trusted repository snapshot. Its trusted-side options fix the repo, producer commit, config path, validator, and callback. `bridge-test` runs that listener in a read-only Bookworm worker container and an unprivileged no-network client with only a read-only socket-volume mount; the client has no repo, Docker socket, host mounts, credentials, or egress. Two committed fixture refs reach the inert callback with levels 3 and 7; protected-file and invalid-level refs are rejected first. This is a bridge-only container test: it runs no Darwin evolve, model, OAT, or candidate source.

`dispatch-contract.ts` validates commit ancestry, exact changed paths, bounded config blobs, and the trusted validator before the callback. `bridge-fixture-worker.ts` is only the JSON fixture validator used by the container test; it is not the ArrowCol production validator. The actual passthrough evaluator is not integrated with this listener, and native `evolve` stays disabled until model credentials, scorer state, and workload launch are independently confined.

`deadline-state.ts` stores a run's exact UTC deadline outside the repository and refuses a changed or expired deadline on resume. `deadline-runner.ts` pins this campaign to `2026-11-23T05:06:42.133Z`, bounds a child process by the remaining budget, and reserves up to 30 seconds for termination and container cleanup. The direct tests exercise persistence, expiry, cleanup reserve, and process termination with no token. Reuse the same run ID when resuming; deadline enforcement is not part of the bridge-only test.

`config-check.ts` is an offline strict check for the ten-lane root manifest and literal evaluator commands; it is not Darwin's production admission enforcement. The toy config is intentionally separate and has no inference or Docker evaluator.
