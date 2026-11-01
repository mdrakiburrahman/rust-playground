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
      --package delta-maintenance \
    && install --mode=0555 /workspace/target/release/delta-maintenance /usr/local/bin/delta-maintenance \
    && strip /usr/local/bin/delta-maintenance

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
LABEL org.opencontainers.image.title="delta-maintenance" \
      org.opencontainers.image.description="Bounded Delta OPTIMIZE, PURGE and VACUUM LITE service on Delta Kernel" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}"
COPY infra/docker/install-azure-cli.sh /tmp/install-azure-cli.sh
RUN sh /tmp/install-azure-cli.sh && rm -f /tmp/install-azure-cli.sh
# The Azure CLI config dir is created EMPTY and outside any data directory;
# runners mount an isolated runtime directory over it. No auth is baked in.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d --owner app --group app --mode 0700 /var/lib/delta-maintenance-azure
COPY --from=builder --chmod=0555 /usr/local/bin/delta-maintenance /usr/local/bin/delta-maintenance
USER 10001:10001
ENV AZURE_CONFIG_DIR=/var/lib/delta-maintenance-azure
STOPSIGNAL SIGTERM
EXPOSE 9467
# Health is the real scheduled-mode Prometheus listener (default 127.0.0.1:9467);
# one-shot runs serve no listener and are never reported healthy. Override the
# URL when the service runs with a different --listen.
ENV DELTA_MAINTENANCE_HEALTH_URL=http://127.0.0.1:9467/metrics
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --show-error --noproxy '*' --max-time 3 --max-filesize 1048576 \
        --output /dev/null "$DELTA_MAINTENANCE_HEALTH_URL" || exit 1
# tini is PID 1 so orphaned az/Python descendants are reaped.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/delta-maintenance"]
CMD ["--help"]