# syntax=docker/dockerfile:1.7
ARG PROFILE_HASH=65f0a48e5711a4fb91141fc590b170e6b614798bb14e6f952dd440c3be547108
FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder
ARG PROFILE_HASH
ARG BUILD_ALLOCATOR=system
ARG BUILD_LTO=off
ARG BUILD_CODEGEN_UNITS=16
ARG BUILD_OPT_LEVEL=3
ARG BUILD_TARGET_CPU=generic
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
    --mount=type=cache,id=rust-playground-target-${PROFILE_HASH},target=/workspace/target-${PROFILE_HASH},sharing=locked \
    set -eu; \
    case "${BUILD_ALLOCATOR}" in system|mimalloc) ;; *) echo 'invalid BUILD_ALLOCATOR' >&2; exit 2 ;; esac; \
    case "${BUILD_CODEGEN_UNITS}" in ''|*[!0-9]*) echo 'invalid BUILD_CODEGEN_UNITS' >&2; exit 2 ;; esac; \
    [ "${BUILD_CODEGEN_UNITS}" -ge 1 ] && [ "${BUILD_CODEGEN_UNITS}" -le 256 ] || { echo 'invalid BUILD_CODEGEN_UNITS' >&2; exit 2; }; \
    case "${BUILD_OPT_LEVEL}" in 2|3) ;; *) echo 'invalid BUILD_OPT_LEVEL' >&2; exit 2 ;; esac; \
    case "${BUILD_TARGET_CPU}" in generic|x86-64-v2|x86-64-v3|x86-64-v4) ;; *) echo 'invalid BUILD_TARGET_CPU' >&2; exit 2 ;; esac; \
    profile_key="allocator=${BUILD_ALLOCATOR};lto=${BUILD_LTO};codegen_units=${BUILD_CODEGEN_UNITS};opt_level=${BUILD_OPT_LEVEL};target_cpu=${BUILD_TARGET_CPU}"; \
    actual_profile_hash=$(printf '%s' "$profile_key" | sha256sum | cut -d ' ' -f 1); \
    [ "$actual_profile_hash" = "${PROFILE_HASH}" ] || { echo 'PROFILE_HASH does not match build arguments' >&2; exit 2; }; \
    export CARGO_TARGET_DIR=/workspace/target-${PROFILE_HASH}; \
    export COLLECTOR_BUILD_PROFILE_HASH=${PROFILE_HASH} COLLECTOR_BUILD_ALLOCATOR=${BUILD_ALLOCATOR}; \
    export COLLECTOR_BUILD_LTO=${BUILD_LTO} COLLECTOR_BUILD_CODEGEN_UNITS=${BUILD_CODEGEN_UNITS}; \
    export COLLECTOR_BUILD_OPT_LEVEL=${BUILD_OPT_LEVEL} COLLECTOR_BUILD_TARGET_CPU=${BUILD_TARGET_CPU}; \
    export ARROWCOL_BUILD_LTO=${BUILD_LTO} ARROWCOL_BUILD_CODEGEN_UNITS=${BUILD_CODEGEN_UNITS}; \
    export ARROWCOL_BUILD_OPT_LEVEL=${BUILD_OPT_LEVEL} ARROWCOL_BUILD_TARGET_CPU=${BUILD_TARGET_CPU}; \
    features=''; \
    if [ "${BUILD_ALLOCATOR}" = mimalloc ]; then features='--features allocator-mimalloc'; fi; \
    if [ "${PROFILE_HASH}" != '65f0a48e5711a4fb91141fc590b170e6b614798bb14e6f952dd440c3be547108' ]; then \
      case "${BUILD_LTO}" in off) export CARGO_PROFILE_RELEASE_LTO=false ;; thin|fat) export CARGO_PROFILE_RELEASE_LTO=${BUILD_LTO} ;; *) echo 'invalid BUILD_LTO' >&2; exit 2 ;; esac; \
      export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=${BUILD_CODEGEN_UNITS}; \
      export CARGO_PROFILE_RELEASE_OPT_LEVEL=${BUILD_OPT_LEVEL}; \
      export RUSTFLAGS="${RUSTFLAGS:+${RUSTFLAGS} }-C target-cpu=${BUILD_TARGET_CPU}"; \
    fi; \
    cargo build --locked --release --package arrowcol-dataflow-rust ${features} \
    && install --mode=0555 ${CARGO_TARGET_DIR}/release/arrowcol-dataflow-rust /usr/local/bin/arrowcol-dataflow-rust \
    && strip /usr/local/bin/arrowcol-dataflow-rust

FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime
ARG VCS_REF=local
ARG PROFILE_HASH=65f0a48e5711a4fb91141fc590b170e6b614798bb14e6f952dd440c3be547108
LABEL org.opencontainers.image.title="arrowcol-dataflow-rust" \
      org.opencontainers.image.description="DFE-native Arrow Flight to Delta Lake collector" \
      org.opencontainers.image.source="https://github.com/mdrakiburrahman/rust-playground" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.profile="${PROFILE_HASH}"
COPY infra/docker/install-azure-cli.sh /usr/local/share/install-azure-cli.sh
RUN sed -i 's|/tmp/|/usr/local/share/|g' /usr/local/share/install-azure-cli.sh \
    && sh /usr/local/share/install-azure-cli.sh \
    && rm -f /usr/local/share/install-azure-cli.sh
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app \
    && install -d --owner app --group app --mode 0700 /var/lib/arrowcol-azure \
    && install -d --owner root --group root --mode 0755 /etc/arrowcol-dataflow-rust \
    && install -d --owner app --group app --mode 0750 /onelake \
    && install -d --owner root --group root --mode 0755 /var/lib/arrowcol \
    && install -d --owner app --group app --mode 0750 /var/lib/arrowcol/quiver
COPY --from=builder --chmod=0555 /usr/local/bin/arrowcol-dataflow-rust /usr/local/bin/arrowcol-dataflow-rust
COPY --chmod=0444 projects/arrowcol-dataflow-rust/config.yaml /etc/arrowcol-dataflow-rust/config.yaml
COPY --chmod=0444 projects/arrowcol-dataflow-rust/config.smoke.yaml /etc/arrowcol-dataflow-rust/config.smoke.yaml
COPY --chmod=0444 projects/arrowcol-dataflow-rust/config.quiver.yaml /etc/arrowcol-dataflow-rust/config.quiver.yaml
COPY --chmod=0444 projects/arrowcol-dataflow-rust/config.smoke-quiver.yaml /etc/arrowcol-dataflow-rust/config.smoke-quiver.yaml
USER 10001:10001
ENV AZURE_CONFIG_DIR=/var/lib/arrowcol-azure
# Inject ARROWCOL_DELTA_ROOT and ARROWCOL_RESET_GENERATION at runtime; never bake an epoch.
RUN az version >/dev/null \
    && /usr/local/bin/arrowcol-dataflow-rust --help >/dev/null \
    && find "$AZURE_CONFIG_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} + \
    && test -w "$AZURE_CONFIG_DIR" \
    && test "$(stat -c '%a' "$AZURE_CONFIG_DIR")" = 700
WORKDIR /onelake
STOPSIGNAL SIGTERM
EXPOSE 32010 32011
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
  CMD ["curl", "--fail", "--silent", "--show-error", "--max-time", "2", "http://127.0.0.1:32011/readyz"]
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arrowcol-dataflow-rust"]
CMD ["--num-cores", "1", "--config", "/etc/arrowcol-dataflow-rust/config.yaml"]
