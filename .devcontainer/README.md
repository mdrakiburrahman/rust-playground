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
- `nx run devcontainer:verify`: type-check and test lifecycle scripts, then
  verify generated references.
- `nx run devcontainer:down -- --volumes`: remove only this workspace's
  discovered Compose project and its volumes.

`initializeCommand` creates the host Azure and GitHub CLI configuration
directories and writes the ignored `.devcontainer/.env` used by Compose. The
hash is based on Git's tracked and non-ignored image-building inputs with
normalized line endings: `.dockerignore`, `Dockerfile`,
`devcontainer.build.json`, and `devcontainer-lock.json`. Compose lifecycle
files, published-image references, scripts, tests, project metadata,
documentation, and local `.env` state are excluded so the hash represents the
published image without recursion.

See
[headless devcontainer operations](../docs/devcontainer/headless-operations.md)
for exact lifecycle, capture, monitoring, cleanup, and regression commands.
See [container publishing](../docs/containers/publishing.md) for GHCR tag and
visibility behavior.
