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

For source branch updates and gitlink publication, see
[CONTRIBUTING](../../CONTRIBUTING.md#rust-collector-and-local-telemetry-lake).
