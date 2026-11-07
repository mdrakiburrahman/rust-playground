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
      --package arrowcol-catalog-rust \
    && install --mode=0555 /workspace/target/release/arrowcol-catalog-rust /usr/local/bin/arrowcol-catalog-rust \
    && strip /usr/local/bin/arrowcol-catalog-rust

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
LABEL org.opencontainers.image.title="arrowcol-catalog-rust" \
      org.opencontainers.image.description="Rust schema catalog and CES validation service" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}"
COPY infra/docker/install-azure-cli.sh /usr/local/share/install-azure-cli.sh
RUN sed -i 's|/tmp/|/usr/local/share/|g' /usr/local/share/install-azure-cli.sh \
    && sh /usr/local/share/install-azure-cli.sh \
    && rm -f /usr/local/share/install-azure-cli.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d --owner app --group app --mode 0700 /var/lib/arrowcol-catalog-azure /data/catalog /data/warehouse
COPY --from=builder --chmod=0555 /usr/local/bin/arrowcol-catalog-rust /usr/local/bin/arrowcol-catalog-rust
USER 10001:10001
ENV AZURE_CONFIG_DIR=/var/lib/arrowcol-catalog-azure
STOPSIGNAL SIGTERM
EXPOSE 8817
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/arrowcol-catalog-rust", "--healthcheck"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arrowcol-catalog-rust"]
