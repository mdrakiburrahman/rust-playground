# otelcol-rust

An Nx-owned build of the fork's `otel-arrow-dfe` package (`df_engine` binary).
The binary is installed as `otelcol-rust` inside the Docker image.

```bash
git submodule update --init --recursive
npx --no-install nx run otelcol-rust:build
npx --no-install nx run otelcol-rust:config-check
npx --no-install nx run otelcol-rust:image-smoke
```

The deliberately small feature set is `otlp,parquet,crypto-ring`, with default
features disabled. The existing composition root registers supported nodes;
there is no duplicated collector entrypoint or Go component-parity promise.
Builds use the fork's own toolchain and Cargo.lock and read the local working
tree, including uncommitted changes. Nx build/image targets are uncached;
Docker still caches unchanged layers and Cargo artifacts.

`config.yaml` accepts OTLP HTTP/protobuf on 4318 and gRPC on 4317 and exports
all three signals to `/onelake`. The `loadtest` project supplies that bind mount.
Age-based flushes and graceful shutdown close small local Parquet files.
The exporter is experimental, not a production durability guarantee.

`otelcol-rust:run` is for a native collector invocation; its config expects a
writable `/onelake`. Prefer `loadtest:up` for the managed local mount and sample
traffic. Native build artifacts stay in root `target/otelcol-rust/`.

## Toolchain and build prerequisites

The collector uses the fork's pinned Rust toolchain independently of the root
workspace. Rustup installs it when first invoked inside the fork.
Native builds need `protoc`; the source devcontainer includes
`protobuf-compiler`. With an older prebuilt devcontainer, rebuild from source
or install that package before native collector builds. The collector
Dockerfile includes its own prerequisites.

## Editing the fork

Initialize the full source and nested proto submodules after cloning or
switching to this change:

```bash
git submodule update --init --recursive
```

CI checks out the committed gitlink SHA, not a moving branch tip. Ordinary
initialization leaves the submodule detached. Attach it to the exploration
branch before local development:

```bash
git -C submodules/otel-arrow switch dev/mdrrahman/explore
```

If that branch is not yet local, create it from the remote tracking branch:

```bash
git -C submodules/otel-arrow switch --track origin/dev/mdrrahman/explore
```

Native and image builds consume local source, including uncommitted edits;
no remote Cargo git dependency bypasses the submodule. After editing, rebuild
and restart the two-container demo with `npx --no-install nx run loadtest:up`.
See [loadtest](../loadtest/README.md) for observing and testing the result.

To incorporate remote changes without discarding local work:

```bash
git -C submodules/otel-arrow fetch origin
git -C submodules/otel-arrow switch dev/mdrrahman/explore
git -C submodules/otel-arrow merge --ff-only origin/dev/mdrrahman/explore
git submodule update --init --recursive
```

Push your collector commits to the fork first, then stage and commit
`submodules/otel-arrow` in rust-playground to pin the new SHA. Run
`npx --no-install nx run loadtest:e2e` before pushing the superproject.
Do not use `git submodule update --remote` in CI or reset a dirty submodule.
