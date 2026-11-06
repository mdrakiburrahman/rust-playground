# Devcontainer

The default configuration uses Docker Compose with the immutable image pinned
in `docker-compose.yml`. The source configuration adds
`docker-compose.local.yml`, builds `.devcontainer/Dockerfile`, and applies the
locked Dev Container Features from `source/devcontainer.json`.

Image publication is intentionally separate from the Compose lifecycle.
`devcontainer.build.json` is a Dockerfile-based configuration with the same
locked Features as the source configuration. The build script launches the
repository-pinned Dev Containers CLI with the current Node executable, so
`--platform linux/amd64` and optional `--push true` are used only with the
CLI configuration that supports them.

Common Nx targets:

- `nx run devcontainer:up` / `test`: use the published immutable image.
- `nx run devcontainer:up-source` / `test-source`: build from source.
- `nx run devcontainer:tag`: recompute the content hash and update the
  checked-in hash and Compose image reference.
- `nx run devcontainer:build`: build `devcontainer.build.json` for
  `linux/amd64`; Compose remains dedicated to interactive source up/test.
- `nx run devcontainer:publish`: publish a missing immutable image and update
  its branch alias with registry-level Buildx imagetools copying; `main` also
  updates `latest` without flattening OCI indexes or provenance attestations.
  Only exact `main` or `refs/heads/main` may use those reserved tags; every
  other branch is isolated under a deterministic `branch-` alias.
- `nx run devcontainer:verify`: type-check and test lifecycle scripts, then
  verify generated references.
- `nx run devcontainer:down -- --volumes`: remove only this workspace's
  discovered Compose project and its volumes.

Loadtest UIs (Grafana, kusto-explorer, Portainer) publish ports on the WSL
Docker host's loopback, which WSL relays to Windows `localhost`. Nothing listens
on those ports inside this container, so the configuration disables
`remote.forwardOnOpen` and `workbench.browser.enableRemoteProxy`. Otherwise VS
Code tunnels clicked `localhost` links into the container (usually on port+1,
because the WSL relay owns the original port) and the page never loads.

See
[headless devcontainer operations](../docs/devcontainer/headless-operations.md)
for exact lifecycle, capture, monitoring, cleanup, and regression commands.
See [container publishing](../docs/containers/publishing.md) for GHCR tag and
visibility behavior.

The devcontainer includes checksum-verified, version-pinned `kubectl`, `k9s`,
`kubelogin` and `kustomize`. The locked Azure CLI Dev Container Feature installs
the pinned `aks-preview` extension in both devcontainer configurations. Verify
the source configuration with `nx run devcontainer:test-source`; `nx run
devcontainer:build` builds the Dockerfile image but does not by itself prove
that the Dev Container Features were applied.
