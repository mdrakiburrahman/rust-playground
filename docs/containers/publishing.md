# Container publishing

The repository publishes the devcontainer and sample runtime image to GHCR
through Nx targets. Run local publication from the source-built devcontainer
so the pinned toolchain, Docker Buildx support, GitHub CLI state, and repository
scripts match the development environment.

These are expected operations and tag policies. They do not assert that CI or
the publication workflows are currently green.

## Authenticate to GHCR

The devcontainer mounts the WSL host's `~/.config/gh` directory read/write.
Authenticate GitHub CLI, verify the selected account, and pass the token to
Docker only through the registry login target:

```bash
gh auth login --hostname github.com --git-protocol https --web
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

From WSL, the exact local branch/SHA runtime publication flow is:

```bash
npx nx run devcontainer:up-source
npx --no-install devcontainer exec \
  --workspace-folder . \
  --config .devcontainer/source/devcontainer.json \
  bash -lc 'npx nx run tools-scripts:registry-login -- --owner mdrakiburrahman --environment local && npx nx run hello-world:publish'
```

`hello-world:publish` resolves the current branch and full Git object ID, then
pushes:

- a mutable sanitized branch tag;
- an immutable `sha-<full-git-sha>` tag.

The `main` configuration intentionally replaces that tag set with the
immutable SHA and `latest`:

```bash
npx nx run hello-world:publish:main
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
  --image hello-world \
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
  --image hello-world \
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
  --image hello-world
```

A public package returns a `public` result. A private package exits with code
2 and returns `needs-ui-change` with the exact GitHub package settings URL.
Open that URL, change the package visibility to **Public** in the GitHub UI,
acknowledge GitHub's confirmation, and rerun the same target. Verify both the
visibility result and the required manifests before treating an image as
publicly available.

## Workflow triggers

`.github/workflows/publish-devcontainer.yml` runs on:

- a push to `main` that changes the devcontainer, its publishing workflow,
  root Node/Nx inputs, the Rust toolchain pin, or registry tooling;
- manual `workflow_dispatch`.

It logs in with the GitHub Actions token, runs `devcontainer:publish`, and
checks the `main` and `latest` manifests.

`.github/workflows/publish-images.yml` runs on:

- a push to `main` that changes the workflow, `hello-world`, any shared crate,
  Cargo workspace inputs, root Node/Nx inputs, the Docker executor, or registry
  tooling;
- manual `workflow_dispatch`.

It smoke-tests the image, publishes SHA and `latest` for `main` (or branch and
SHA for a non-main manual context), and verifies the resulting manifests.
Package visibility remains the explicit GitHub UI step described above.

## No-secret rules

- Never commit `.devcontainer/.env`, `~/.azure`, `~/.config/gh`, Docker
  credentials, personal access tokens, device codes, passwords, or MFA
  material.
- Never pass a token as a command-line argument or Docker build argument.
- Use `tools-scripts:registry-login`; it keeps the token on standard input.
- Use only GitHub Actions encrypted tokens for workflow publication.
- Do not add private registries, package feeds, or organization-only service
  details to public manifests, project files, Dockerfiles, or documentation.
