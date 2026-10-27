# syntax=docker/dockerfile:1.7

FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml /workspace/
COPY crates /workspace/crates
COPY bin /workspace/bin
COPY projects /workspace/projects
COPY submodules/otel-arrow /workspace/submodules/otel-arrow
RUN --mount=type=cache,id=rust-playground-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rust-playground-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=rust-playground-target,target=/workspace/target,sharing=locked \
    CARGO_TARGET_DIR=/workspace/target cargo build --locked --release \
      --package arrowcol-rust \
    && install --mode=0555 /workspace/target/release/arrowcol-rust /usr/local/bin/arrowcol-rust \
    && strip /usr/local/bin/arrowcol-rust

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
LABEL org.opencontainers.image.title="arrowcol-rust" \
      org.opencontainers.image.description="Direct Arrow Flight collector with fenced Delta Lake publication" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}"
COPY infra/docker/install-azure-cli.sh /tmp/install-azure-cli.sh
RUN sh /tmp/install-azure-cli.sh && rm -f /tmp/install-azure-cli.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d --owner app --group app --mode 0700 /var/lib/arrowcol-azure \
    && install -d --owner root --group root --mode 0755 /etc/arrowcol-rust \
    && install -d --owner app --group app --mode 0750 /var/lib/arrowcol/delta
COPY --from=builder --chmod=0555 /usr/local/bin/arrowcol-rust /usr/local/bin/arrowcol-rust
COPY --chmod=0444 projects/arrowcol-rust/config.docker.yaml /etc/arrowcol-rust/config.yaml
USER 10001:10001
ENV AZURE_CONFIG_DIR=/var/lib/arrowcol-azure
WORKDIR /var/lib/arrowcol
STOPSIGNAL SIGTERM
EXPOSE 50051 8081
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
  CMD ["curl", "--fail", "--silent", "--show-error", "--max-time", "2", "http://127.0.0.1:8081/readyz"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arrowcol-rust"]
CMD ["--config", "/etc/arrowcol-rust/config.yaml"]
