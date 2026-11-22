# Darwin runner

Darwin 0.3.0 runs in a pinned Debian bookworm controller beside the Ubuntu devcontainer. The controller and candidate worker use identical pinned Rust/Node toolchains. Each candidate gets a fresh container with only committed `git archive HEAD`, no host mounts, credentials, cloud config, controller home, inherited environment, or Docker socket.

| Command | Purpose |
|---|---|
| `tools/darwin-runner/run.sh version` | Build the pinned images and print the Darwin version. |
| `tools/darwin-runner/run.sh dry-run tools/darwin-runner/toy.yaml` | Validate the toy config and ask Darwin to dry-run it without a token. |
| `tools/darwin-runner/run.sh prepare` | Generate a per-run age identity in a Docker volume and print its run ID, public recipient, and SHA-256 fingerprint. |
| `tools/darwin-runner/run.sh evolve <tracked-config.yaml> <experiment> <run-id>` | Prove isolation, unwrap the run-scoped encrypted token for Darwin, and run the experiment. |
| `tools/darwin-runner/run.sh cleanup <run-id>` | Remove an unused run's encrypted workspace copy and private-key volume. |
| `env -u COPILOT_GITHUB_TOKEN /usr/local/bin/darwin-candidate-exec 'cargo test --locked'` | Required evaluator command form. |

After `prepare`, encrypt a one-line `COPILOT_GITHUB_TOKEN=<value>` file to the returned `age1...` recipient. Place only the ciphertext at `sync/rust-playground/.darwin-runner-<run-id>.env.age`; bootstrapper sync places it at the repository root. Do not stage plaintext in the shared sync folder, a command, or job history. The runner removes its workspace copy and destroys the key volume on completion or failed decryption; remove the source ciphertext from the sync folder after the run. The existing `.env` is neither read nor changed, and there is no fallback recipient.

Before decrypting anything, `evolve` checks that every evaluator command drops `COPILOT_GITHUB_TOKEN` before invoking the isolated executor. It then runs a fake-token probe using a separate temporary repository with a tracked sentinel `.env`. The worker must confirm the sentinel environment, `.env`, encrypted-file patterns, credentials, home and controller socket are absent. A failed probe stops before decryption or inference. The private key volume is mounted only into the controller and read-only; the decrypted one-line env file exists only in the controller's fresh tmpfs and is passed to Darwin with `--env-file`.

The pinned binary prints `darwin 0.3.0`. In the first no-token dry-run, the Debian controller started, and Darwin parsed the toy config, but dry-run failed with `ModuleNotFoundError: No module named 'darwin.prompts.default'`. The source repository is not readable with the authorized `mdrakiburrahman` GitHub identity, so this runner does not substitute an unverified binary or copy private source.

Once a trusted campaign config is available, use:

```bash
tools/darwin-runner/run.sh prepare
tools/darwin-runner/run.sh evolve tools/darwin/darwin.yaml <experiment-name> <run-id>
```

The prepare output contains only the public recipient and fingerprint. A successful inference run first prints `candidate isolation assertions passed`, `Isolation proof passed: ...`, and `Darwin evaluator isolation check passed (...)`; no credential value is emitted.
