# Container publishing

The repository publishes the devcontainer, five production Rust runtime
images, and the retained `hello-world` sample image to GHCR through Nx targets.
Run local publication from the source-built devcontainer so the pinned
toolchain, Docker Buildx support, GitHub CLI state, and repository scripts match
the development environment.

These are expected operations and tag policies. They do not assert that CI or
the publication workflows are currently green.

No image is published by a checkout, local build, image smoke, or documentation
change. This uncommitted workspace has not pushed an image. In the repository
release path, GHCR publication begins only when
`.github/workflows/publish-images.yml` runs on GitHub and verifies the remote
manifests.

## Production image names and local smoke

| Project | GHCR image | Local non-publishing smoke |
| --- | --- | --- |
| `otelcol-rust` | `ghcr.io/mdrakiburrahman/rust-playground/otelcol-rust` | `npx --no-install nx run otelcol-rust:image-smoke` |
| `arrowcol-rust` | `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-rust` | `npx --no-install nx run arrowcol-rust:image-smoke` |
| `arrowcol-dataflow-rust` | `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-dataflow-rust` | `npx --no-install nx run arrowcol-dataflow-rust:image-smoke` |
| `arrowcol-router-rust` | `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-router-rust` | `npx --no-install nx run arrowcol-router-rust:image-smoke` |
| `kusto-explorer` | `ghcr.io/mdrakiburrahman/rust-playground/kusto-explorer` | `npx --no-install nx run kusto-explorer:image-smoke` |

Each smoke target builds or reuses the local `linux/amd64` `:dev` image and
validates its embedded configuration and bounded startup/shutdown contract. It
does not log in to GHCR and does not push.

Published runtime tags are:

- `branch-<sanitized-branch>` for the selected branch;
- immutable `sha-<full-git-sha>`; and
- `latest` only when the workflow publishes `main`.

## Authenticate to GHCR

The devcontainer mounts the WSL host's `~/.config/gh` directory read/write.
For a fresh GitHub CLI login, request GitHub Packages write access:

```bash
gh auth login --hostname github.com --git-protocol https --web \
  --scopes write:packages
```

For an existing login, add the package write scope:

```bash
gh auth refresh --hostname github.com --scopes write:packages
```

Local publication requires the `write:packages` package scope.
`read:packages` supports package downloads but is not sufficient for a push.
Verify the selected account, then pass the token to Docker only through the
registry login target:

```bash
gh auth status
npx nx run tools-scripts:registry-login -- \
  --owner mdrakiburrahman \
  --environment local
```

For local execution, the target reads `gh auth token` and supplies it to
`docker login ghcr.io --password-stdin`. In GitHub Actions it uses
`GHCR_TOKEN`, then `GITHUB_TOKEN`. Tokens are never placed in command arguments,
printed by the helper, or written to repository files.

## Publish from the source-built devcontainer

From WSL, the exact local branch/SHA runtime publication flow for one image is:

```bash
npx nx run devcontainer:up-source
npx --no-install devcontainer exec \
  --workspace-folder . \
  --config .devcontainer/source/devcontainer.json \
  bash -lc 'npx nx run tools-scripts:registry-login -- --owner mdrakiburrahman --environment local && npx nx run otelcol-rust:publish'
```

The production runtime projects are:

- `otelcol-rust`
- `arrowcol-rust`
- `arrowcol-dataflow-rust`
- `arrowcol-router-rust`
- `kusto-explorer`

`hello-world` remains published as the repository sample. Every `publish`
target resolves the current branch and full Git object ID, then pushes:

- a mutable `branch-<sanitized-branch>` tag;
- an immutable `sha-<full-git-sha>` tag.

The `main` configuration adds `latest` while retaining both branch and SHA:

```bash
npx nx run otelcol-rust:publish:main
```

Use that configuration only for a `main` commit. The runtime workflow selects
it automatically for `main`.

## Devcontainer content hash and aliases

Recompute the devcontainer content hash and checked-in image reference with:

```bash
npx nx run devcontainer:tag
```

The hash covers Git-tracked and non-ignored image-building inputs under
`.devcontainer/`, with normalized line endings. It excludes lifecycle scripts,
tests, project metadata, documentation, local `.env` state, the generated hash
file, and the Compose file containing the resulting image reference.

Publish from an authenticated source-built workspace with:

```bash
npx nx run devcontainer:publish
```

The target:

1. checks for
   `ghcr.io/mdrakiburrahman/rust-playground/devcontainer:<content-hash>`;
2. builds and pushes that immutable `linux/amd64` image only when it is absent;
3. updates the sanitized current-branch alias when it differs;
4. updates `latest` as well when the branch alias is `main`.

The immutable content hash is the source of truth used by the default
devcontainer Compose file. `main` and `latest` are convenience aliases, not
replacements for the immutable reference.

## Verify remote manifests

Verify the exact immutable devcontainer manifest:

```bash
npx nx run tools-scripts:registry-manifest -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image devcontainer \
  --tag "$(cat .devcontainer/content-hash.txt)"
```

Inspect the runtime tag metadata and verify the immutable runtime manifest:

```bash
npx nx run tools-scripts:registry-tags -- \
  --branch "$(git branch --show-current)" \
  --sha "$(git rev-parse HEAD)" \
  --environment local

npx nx run tools-scripts:registry-manifest -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image otelcol-rust \
  --tag "sha-$(git rev-parse HEAD)"
```

For a `main` publication, also verify the mutable aliases:

```bash
npx nx run tools-scripts:registry-manifest -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image devcontainer \
  --tag latest

npx nx run tools-scripts:registry-manifest -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image otelcol-rust \
  --tag latest
```

The manifest helper uses `docker manifest inspect` and suppresses the manifest
body. A successful target confirms that the exact remote reference is
accessible.

## Make and verify packages public

GHCR packages can be private after their first push. The repository helper is
deliberately read-only because GitHub's REST Packages API does not expose a
visibility mutation:

```bash
npx nx run tools-scripts:registry-public -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image devcontainer

npx nx run tools-scripts:registry-public -- \
  --owner mdrakiburrahman \
  --repository rust-playground \
  --image otelcol-rust
```

A public package returns a `public` result. A private package exits with code
2 and returns `needs-ui-change` with the exact GitHub package settings URL.
Open that URL, change the package visibility to **Public** in the GitHub UI,
acknowledge GitHub's confirmation, and rerun the same target. Verify both the
visibility result and the required manifests before treating an image as
publicly available.

## Workflow triggers

`.github/workflows/publish-devcontainer.yml` runs on:

- a push to any branch that changes the devcontainer, its publishing workflow,
  root Node/Nx inputs, the Rust toolchain pin, or registry tooling, so a
  branch's content-hashed image exists before anyone bootstraps that branch;
- manual `workflow_dispatch`.

It logs in with the GitHub Actions token and runs `devcontainer:publish`, which
skips an image hash that already exists and updates only that branch's alias.
On `main` it also checks the `main` and `latest` manifests.

`.github/workflows/publish-images.yml` runs on:

- a push to `main` that changes the workflow, any published runtime project,
  shared crate, the pinned `otel-arrow` submodule, Cargo workspace inputs, root
  Node/Nx inputs, the Docker executor, or registry tooling;
- manual `workflow_dispatch`.

The workflow discovers the six source-built publication targets from the Nx
graph, then builds and smoke-tests each matrix entry before authentication. It
publishes branch and immutable SHA tags, adds `latest` on `main`, verifies the
deterministic local publication manifest, and verifies every remote tag.
Package visibility remains the explicit GitHub UI step described above.

The six-target matrix contains the five production images listed above plus
the retained `hello-world` sample image. The devcontainer is published by its
separate workflow.

## Image metadata and attestations

The five production runtime Dockerfiles set OCI title, description, source,
license, and revision labels. Local `:dev` builds use `revision=local`;
publication targets pass the full Git SHA.

The current `rust-playground-build:docker-build` executor does not expose
Buildx SBOM or provenance attestation options. The workflow therefore does not
emit SBOM/provenance attestations and does not request `id-token` permission.
Adding attestations later requires executor schema/argv coverage plus manifest
tests because attestations can change the published manifest shape. This is an
explicitly documented omission, not an implicit claim that attestations exist.

## No-secret rules

- Never commit `.devcontainer/.env`, `~/.azure`, `~/.config/gh`, Docker
  credentials, personal access tokens, device codes, passwords, or MFA
  material.
- Never pass a token as a command-line argument or Docker build argument.
- Use `tools-scripts:registry-login`; it keeps the token on standard input.
- Use only GitHub Actions encrypted tokens for workflow publication.
- Do not add private registries, package feeds, or organization-only service
  details to public manifests, project files, Dockerfiles, or documentation.
