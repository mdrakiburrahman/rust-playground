# Rust Playground

A Linux-first Rust learning monorepo organized with Cargo and Nx. It keeps
independent applications under `bin/`, reusable libraries under `crates/`, and
repository automation under `tools/`. The initial sample combines the
`greeting` library with the `hello-world` CLI and its Linux container image.

## Development environment

Development is Linux-first in WSL and uses the repository devcontainer:

- Ubuntu 24.04
- Rust 1.96.1, rustfmt, Clippy, cargo-make, mold, clang, and LLDB
- Node.js 24 and Nx 23
- Azure CLI, GitHub CLI, and Docker Buildx through Docker-outside-of-Docker

The source configuration builds `.devcontainer/Dockerfile` and applies the
locked Dev Container Features. The default configuration uses the immutable
GHCR image pinned in `.devcontainer/docker-compose.yml`.

The host bootstrap installs only shared developer-machine prerequisites.
Repository dependencies are installed by the devcontainer's post-create step.
The WSL host's `~/.azure` and `~/.config/gh` directories are mounted read/write
into the devcontainer. See
[headless devcontainer operations](docs/devcontainer/headless-operations.md)
for exact keep-running, monitoring, and targeted-cleanup commands.

## Quick start

After completing the one-time setup in [CONTRIBUTING.md](CONTRIBUTING.md),
install the pinned host CLI dependencies and build the workspace from source:

```bash
npm ci
npx nx run devcontainer:test-source
```

To use the checked-in immutable prebuilt image instead:

```bash
npm ci
npx nx run devcontainer:test
```

Both targets start the selected devcontainer and run every Nx `verify` target
inside it. Stop only this workspace's Compose project when finished:

```bash
npx nx run devcontainer:down -- --volumes
```

## Nx targets

Run repository targets inside the devcontainer:

```bash
npx nx run rust:format
npx nx run rust:format-check
npx nx run rust:lint
npx nx run rust:build
npx nx run rust:test
npx nx run rust:verify
npx nx run auth-automation:verify
npx nx run tools-scripts:verify
npx nx run rust-playground-build:verify
npx nx run devcontainer:verify
```

Run all verification targets serially with:

```bash
npx nx run-many -t verify --all --parallel=1
```

## Sample CLI and image

Run the sample application:

```bash
npx nx run hello-world:run -- --name Ferris
```

Build and smoke-test its local Linux amd64 image:

```bash
npx nx run hello-world:image-smoke
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:dev \
  --name Ferris
```

After the public `latest` tag has been published and verified, the equivalent
prebuilt invocation is:

```bash
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:latest \
  --name Ferris
```

## Project structure

- `bin/` - For independent binary crates. Nothing should depend on any crates
  in here. This is like `cmd` in the `go` project. Crates within this folder
  will often contain accessories to the corresponding source code like
  `Dockerfile`s.
- `crates/` - For library crates. Library crates may depend on each other and
  binary crates may also depend on them. This is just like `pkg` in the `go`
  project.
- `Cargo.toml` - This root level Cargo configuration defines the workspace and
  points to all other crates both bin and lib.
- `rust-toolchain.toml` - This pins the rust toolchain version that this repo
  uses.
- `tools/` - TypeScript CLIs, lifecycle helpers, and the local Nx Docker build
  plugin.
- `projects/otelcol-rust/` - Nx build/configuration and Docker image for the
  editable otel-arrow fork at `submodules/otel-arrow`.
- `projects/loadtest/` - Two-service Compose demo and content-verified Parquet
  E2E; generated telemetry lives in gitignored `onelake/`.
- `.devcontainer/` - Reproducible source-built and immutable prebuilt
  development environments.

See [Rust project layout](docs/rust/project-layout.md) and
[adding Rust projects](docs/rust/adding-projects.md).

## Local Rust telemetry collector

```bash
git submodule update --init --recursive
npx --no-install nx run loadtest:up
npx --no-install nx run loadtest:inspect
npx --no-install nx run loadtest:down
npx --no-install nx run loadtest:e2e
```

The sample sends logs, metrics, and traces to the Rust collector and writes
readable local Parquet. `loadtest:up` rebuilds both local Dockerfiles, including
uncommitted fork edits. See [loadtest](projects/loadtest/README.md) and
[the contributor workflow](CONTRIBUTING.md#rust-collector-and-local-telemetry-lake).

## Public GHCR images

The repository publishes these package locations. A tag should be treated as
available only after its manifest and public visibility have been verified:

| Image | Tags | Purpose |
| --- | --- | --- |
| `ghcr.io/mdrakiburrahman/rust-playground/devcontainer` | immutable content hash, branch alias, and `latest` from `main` | Development environment |
| `ghcr.io/mdrakiburrahman/rust-playground/hello-world` | sanitized branch, `sha-<full-git-sha>`, and `latest` from `main` | Sample runtime image |

See [container publishing](docs/containers/publishing.md) for login,
publication, visibility, and manifest verification. These documents describe
the expected operations; they do not assert that a workflow or publication
run is currently green.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
