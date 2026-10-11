# otelcol-rust

A root-workspace collector binary that links `otel-deltalake-exporter` and
calls the fork's shared `otel-arrow-dfe` startup entry point. The reusable
Arrow/Delta SDK and adapter live in rust-playground, not in the fork.

```bash
git submodule update --init --recursive
npx --no-install nx run otelcol-rust:build
npx --no-install nx run otelcol-rust:config-check
npx --no-install nx run otelcol-rust:image-smoke
```

The upstream feature set remains `otlp,parquet,crypto-ring`, with defaults
disabled. Parquet enables the shared OTAP preparation and file-backend hooks;
the external adapter registers `exporter:deltalake`. There is no duplicated
collector startup or Go component-parity promise. Builds use the root
workspace's toolchain and Cargo.lock and read local source, including
uncommitted fork changes. Nx build/image targets are uncached;
Docker still caches unchanged layers and Cargo artifacts.

`config.yaml` accepts OTLP HTTP/protobuf on 4318 and gRPC on 4317 and exports
all three signals to independent Delta payload tables beneath `/onelake`.
The `loadtest` project supplies that bind mount. Multiple collector cores write
Parquet concurrently and share batched Delta Kernel commits per table. The
native/image demo defaults to two cores; `LOADTEST_CORES` controls Compose.
`commit_options` defaults to a 10-second collection window, 64 files per commit,
and 256 queued/committing files per table. Age-based Parquet flushes stage private
files; parent publication waits for child commits. Graceful shutdown forces
partial batches and drains all dependency waves.
Native timestamps have exact nanosecond companions, and join namespaces
prevent false joins across cores and after restart. This is an experimental
append-only local writer, not a production or cross-table durability guarantee.

`otelcol-rust:run` is for a native collector invocation; its config expects a
writable `/onelake`. Prefer `loadtest:up` for the managed local mount and sample
traffic. Native build artifacts stay in root `target/`.

## Toolchain and build prerequisites

The root-owned collector uses the root's pinned Rust toolchain. Tests run
inside the fork still use its independently pinned toolchain.
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
