# hello-world

`hello-world` is an independent binary crate under `bin/`. Nothing in the
workspace should depend on a `bin/` crate; applications instead compose shared
libraries such as `crates/greeting`.

Run the CLI from the workspace root:

```bash
npx nx run hello-world:run -- --name Ferris
```

The equivalent Cargo command is:

```bash
cargo run --locked --package hello-world -- --name Ferris
```

## Optional telemetry

The default remains one greeting on stdout and tracing diagnostics on stderr.
`--count` defaults to `1`; `--interval-ms` defaults to `1000`. Both must be
positive. `--repeat` instead runs until SIGINT or SIGTERM (and cannot be combined
with an explicit `--count`). `--run-id` is a nonblank marker, defaulting to `demo`;
use a unique value for each validation run.

Opt in to the OpenTelemetry Rust SDK's OTLP HTTP protobuf exporters:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://otelcol-rust:4318 \
  npx nx run hello-world:run -- --telemetry --repeat

OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  npx nx run hello-world:run -- \
  --telemetry --count 3 --interval-ms 100 --run-id marker
```

Each greeting exports a log with body `greeting generated`, a
`hello_world.greetings` counter measurement, and a `greeting` span. Every signal
has `name` and `run.id` attributes, with resource `service.name=hello-world`.
The log also carries the greeting span's trace context. The SDK appends
`/v1/logs`, `/v1/metrics`, and `/v1/traces` to the endpoint; signal-specific OTLP
environment variables are supported. No collector connection is made without
`--telemetry`.

Each greeting explicitly flushes all providers, and finite completion or
interrupt explicitly flushes and shuts them down. Export failures (including
background exports) produce a nonzero exit status instead of silently succeeding.
The interval is a delay after each greeting and its exports, not a fixed-rate
schedule. The SDK's OTLP timeout defaults to ten seconds and can be configured
with `OTEL_EXPORTER_OTLP_TIMEOUT` (milliseconds).

Build and smoke-test its local Linux amd64 image:

```bash
npx nx run hello-world:image-smoke
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:dev \
  --name Ferris
```

After publication and public manifest verification:

```bash
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:latest \
  --name Ferris
```

See [container publishing](../../docs/containers/publishing.md) for branch,
SHA, and `latest` behavior.
