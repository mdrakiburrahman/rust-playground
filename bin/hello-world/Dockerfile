# syntax=docker/dockerfile:1.7

FROM --platform=linux/amd64 rust:1.96.1-bookworm@sha256:a339861ae23e9abb272cea45dfafde21760d2ce6577a70f8a926153677902663 AS builder

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates/greeting/Cargo.toml crates/greeting/README.md crates/greeting/
COPY crates/greeting/src crates/greeting/src
COPY bin/hello-world/Cargo.toml bin/hello-world/README.md bin/hello-world/
COPY bin/hello-world/src bin/hello-world/src
COPY projects/loadtest/reader/Cargo.toml projects/loadtest/reader/
COPY projects/loadtest/reader/src projects/loadtest/reader/src

RUN cargo build --locked --release --package hello-world \
    && strip target/release/hello-world

FROM --platform=linux/amd64 debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587 AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home --no-log-init --home-dir /nonexistent --shell /usr/sbin/nologin app

COPY --from=builder /workspace/target/release/hello-world /usr/local/bin/hello-world

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/hello-world"]
CMD ["--name", "World"]
