# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler && rm -rf /var/lib/apt/lists/*
WORKDIR /workspace
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml /workspace/
COPY crates /workspace/crates
COPY bin /workspace/bin
COPY projects /workspace/projects
COPY submodules/otel-arrow /workspace/submodules/otel-arrow
RUN --mount=type=cache,id=rust-playground-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=rust-playground-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=rust-playground-target,target=/workspace/target,sharing=locked \
    cargo build --locked --release --package collector-chain --bin arrowcol-dmv \
    && install --mode=0555 target/release/arrowcol-dmv /usr/local/bin/arrowcol-dmv \
    && strip /usr/local/bin/arrowcol-dmv
FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/bin/arrowcol-dmv /usr/local/bin/arrowcol-dmv
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/arrowcol-dmv"]
