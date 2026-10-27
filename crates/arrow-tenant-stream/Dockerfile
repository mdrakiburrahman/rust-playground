# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml /workspace/
COPY crates /workspace/crates
COPY bin /workspace/bin
COPY projects /workspace/projects
COPY submodules/otel-arrow /workspace/submodules/otel-arrow
RUN --mount=type=cache,id=rust-playground-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rust-playground-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=tenant-stream-target,target=/workspace/target,sharing=locked \
    cargo build --locked --release --package arrow-tenant-stream \
    && install --mode=0555 /workspace/target/release/arrow-tenant-stream /usr/local/bin/arrow-tenant-stream \
    && strip /usr/local/bin/arrow-tenant-stream

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
LABEL org.opencontainers.image.title="arrow-tenant-stream" \
      org.opencontainers.image.description="Checkpointed append-only historical Delta tenant forks" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}"
COPY --chmod=0444 infra/docker/install-azure-cli.sh /tmp/install-azure-cli.sh
RUN sh /tmp/install-azure-cli.sh \
    && rm -f /tmp/install-azure-cli.sh \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d --owner app --group app --mode 0750 /var/lib/tenant-stream \
    && install -d --owner app --group app --mode 0700 /var/lib/tenant-stream-azure
ENV AZURE_CONFIG_DIR=/var/lib/tenant-stream-azure
COPY --from=builder --chmod=0555 /usr/local/bin/arrow-tenant-stream /usr/local/bin/arrow-tenant-stream
USER 10001:10001
WORKDIR /var/lib/tenant-stream
STOPSIGNAL SIGTERM
EXPOSE 9094
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
  CMD ["curl", "--fail", "--silent", "--show-error", "--max-time", "2", "http://127.0.0.1:9094/metrics"]
ENTRYPOINT ["/usr/local/bin/arrow-tenant-stream"]
