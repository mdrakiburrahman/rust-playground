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

`new ContainerSettlementSupervisor(statePath).run(containerId, { timeoutMs, signal? })` starts and supervises a trusted, newly created Docker container and returns `{ containerId, outcome, exitCode, settlement, reason }`. It requires an unprivileged, networkless, mount-free private PID namespace and a retained container; a stopped container's independently inspected Docker state proves namespace settlement. Failure, timeout, cancellation, or unproved settlement retains both container and private state marker, blocking a restart with the same state path. The timeout is per invocation; the caller must separately enforce any inherited campaign deadline. This is not wired to scoring or component execution: fixture/oracle admission, safe output mounts, memory and output limits remain unimplemented.

## Trusted Unix-socket bridge

`bridge-listener.ts` accepts only `{version: 1, lane: "compression-config", candidateCommit: "<full-sha>"}` over a Unix socket outside the trusted repository snapshot. Its trusted-side options fix the repo, producer commit, config path, validator, and callback. `bridge-test` runs that listener in a read-only Bookworm worker container and an unprivileged no-network client with only a read-only socket-volume mount; the client has no repo, Docker socket, host mounts, credentials, or egress. Two committed fixture refs reach the inert callback with levels 3 and 7; protected-file and invalid-level refs are rejected first. This is a bridge-only container test: it runs no Darwin evolve, model, OAT, or candidate source.

`dispatch-contract.ts` validates commit ancestry, exact changed paths, bounded config blobs, and the trusted validator before the callback. `bridge-fixture-worker.ts` is only the JSON fixture validator used by the container test; it is not the ArrowCol production validator. The actual passthrough evaluator is not integrated with this listener, and native `evolve` stays disabled until model credentials, scorer state, and workload launch are independently confined.

`deadline-state.ts` stores a run's exact UTC deadline outside the repository and refuses a changed or expired deadline on resume. `deadline-runner.ts` pins this campaign to `2026-11-23T05:06:42.133Z`, bounds a child process by the remaining budget, and reserves up to 30 seconds for termination and container cleanup. The direct tests exercise persistence, expiry, cleanup reserve, and process termination with no token. Reuse the same run ID when resuming; deadline enforcement is not part of the bridge-only test.

`config-check.ts` is an offline strict check for the ten-lane root manifest and literal evaluator commands; it is not Darwin's production admission enforcement. The toy config is intentionally separate and has no inference or Docker evaluator.

`production-preflight.ts` exports `startArrowColCompressionPreflight` for the harness owner to supply the trusted one-arm callback. It checks commits against the `setup` snapshot, materializes only the committed candidate tree, runs the snapshot's production path policy and `checkCompressionCandidate`, and validates the OAT JSON with the pinned producer's Rust `--validate-config` binary before calling the callback. Its test uses a test-only inert callback and verifies an allowed level change plus protected-path, codec, and level rejection. This is a trusted preflight adapter test, not native Darwin evolution, OAT execution, or fitness evidence.

The verified no-token production proof used source snapshot `90ef040e17bda21d990ea15b2eb1cd824b65dc82`, pinned producer `efeec066ca04bba77e660b26cb0720976e7d5f28`, and validator SHA-256 `70945b0fe8c92701dacfbbf55bedf21a55175d783c05d9f1b8e7c5e692b293d0`.

The upstream `tools/darwin/src/main.ts setup` currently snapshots and emits modules but exits nonzero because archived-source TypeScript compilation cannot resolve `apache-arrow` and `parquet-wasm`. This proof used the emitted modules, a module-type marker, and the same pinned Cargo wrapper `setup` would write after a successful compile; clean setup completion still needs an upstream packaging fix.

After the integration branch's Darwin `setup` and `prepare` have produced a trusted home and validator, run the no-token proof with `DARWIN_PREFLIGHT_TEST_REPO=/path/to/clean/integration-worktree DARWIN_PREFLIGHT_TEST_HOME=/path/to/darwin-home env -u COPILOT_GITHUB_TOKEN npx --no-install tsx --test tools/darwin-runner/production-preflight.test.ts`. The integration worktree must contain the snapshot commit and pinned submodule objects; missing cached submodules fail closed without fetching.

`dispatchArrowColMaintenanceControl` is a separate explicit entry point. The harness may inject `dispatchMaintenanceControl` from `tools/darwin/src/maintenance-dispatch.ts` at `4546e3f88cc2521501ee34fedb1de47b10281864` through `ArrowColCompressionPreflightOptions.maintenanceControlAdapter`; the default compression path never calls it, and an absent adapter blocks maintenance dispatch. Verification tests only the missing-adapter rejection and never invoke the adapter or run its timed pairs.
