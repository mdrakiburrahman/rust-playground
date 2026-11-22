# Darwin runner

Runs pinned Darwin 0.3.0 in a Debian bookworm controller beside the Ubuntu devcontainer. Darwin and candidate workers use the same pinned Rust and Node toolchains; each candidate gets a fresh container with only `git archive HEAD`, a sanitized environment, no host home or credentials, and no Docker socket. The candidate image is identical across A/B evaluations.

| Command | Purpose |
|---|---|
| `tools/darwin-runner/run.sh dry-run tools/darwin-runner/toy.yaml` | Parse and dry-run the toy config without inference or a token. |
| `tools/darwin-runner/run.sh evolve <tracked-config.yaml> <experiment>` | Run the isolation proof, then Darwin. Requires one `COPILOT_GITHUB_TOKEN` in the gitignored `.env`. |
| `env -u COPILOT_GITHUB_TOKEN /usr/local/bin/darwin-candidate-exec 'cargo test --locked'` | Evaluator command form; use it for every evaluator command. |

`evolve` refuses configs whose evaluator commands do not start with the isolated executor and token scrub. Before exposing the real token, it runs a fake-token probe that includes a tracked sentinel `.env`; the worker must report that its environment and filesystem contain no sentinel, credential path, or controller socket. Any failed proof stops the run before inference.

The Docker socket and the workspace (including `.env`) are mounted only into the controller. The real token is filtered into an ephemeral, mode-0600 env file and passed to the Darwin container; candidate archives are generated from the committed `HEAD` and exclude `.env*` and credential/config paths. The worker has no bind mounts, cloud configuration, GitHub credentials, inherited environment, or controller home.

Live inference command for the ArrowCol campaign:

```bash
tools/darwin-runner/run.sh evolve tools/darwin/darwin.yaml <experiment-name>
```

The command prints the Darwin result only after the fake-token isolation proof passes. It requires the repo-bootstrapper-injected `.env`; never put a real token in command arguments or job code. `darwin --version` in the controller prints `darwin 0.3.0`.

The dry run should print Darwin's parsed experiment summary and exit without starting an agent. The inference run should first print `candidate isolation assertions passed`, `Isolation proof passed: ...`, and `Darwin evaluator isolation check passed (...)`; no token value is emitted.
