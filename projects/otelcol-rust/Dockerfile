# syntax=docker/dockerfile:1.7

FROM --platform=linux/amd64 rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace/submodules/otel-arrow/rust/otap-dataflow
COPY submodules/otel-arrow /workspace/submodules/otel-arrow
RUN --mount=type=cache,id=otelcol-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=otelcol-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=otelcol-target,target=/workspace/target \
    CARGO_TARGET_DIR=/workspace/target cargo build --locked --release \
      --package otel-arrow-dfe --bin df_engine --no-default-features \
      --features otlp,parquet,crypto-ring \
    && install /workspace/target/release/df_engine /usr/local/bin/otelcol-rust \
    && strip /usr/local/bin/otelcol-rust

FROM --platform=linux/amd64 debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d -o app -g app /onelake
COPY --from=builder /usr/local/bin/otelcol-rust /usr/local/bin/otelcol-rust
COPY projects/otelcol-rust/config.yaml /etc/otelcol-rust/config.yaml
USER 10001:10001
EXPOSE 4317 4318 8080
HEALTHCHECK --interval=2s --timeout=2s --start-period=10s --retries=30 \
  CMD curl --fail --silent http://127.0.0.1:8080/api/v1/groups/default/pipelines/main/readyz || exit 1
ENTRYPOINT ["/usr/local/bin/otelcol-rust"]
CMD ["--num-cores", "1", "--http-admin-bind", "0.0.0.0:8080", "--config", "/etc/otelcol-rust/config.yaml"]
