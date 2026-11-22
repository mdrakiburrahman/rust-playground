ARG DEBIAN_IMAGE=debian:bookworm-slim
FROM ${DEBIAN_IMAGE} AS toolchain

ARG DEBIAN_SNAPSHOT
ARG RUST_VERSION
ARG RUSTUP_INIT_SHA256
ARG NODE_VERSION
ARG NODE_SHA256
ARG AGE_PACKAGE

ENV CARGO_HOME=/opt/cargo \
    RUSTUP_HOME=/opt/rustup \
    PATH=/opt/cargo/bin:/opt/node/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

RUN rm -f /etc/apt/sources.list.d/debian.sources \
    && printf 'deb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s bookworm main\ndeb [check-valid-until=no] http://snapshot.debian.org/archive/debian/%s bookworm-updates main\ndeb [check-valid-until=no] http://snapshot.debian.org/archive/debian-security/%s bookworm-security main\n' \
        "$DEBIAN_SNAPSHOT" "$DEBIAN_SNAPSHOT" "$DEBIAN_SNAPSHOT" > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
        bash build-essential ca-certificates curl git libsqlite3-0 libssl-dev pkg-config xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN curl --fail --silent --show-error --location \
        https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init \
        -o /tmp/rustup-init \
    && printf '%s  %s\n' "$RUSTUP_INIT_SHA256" /tmp/rustup-init | sha256sum --check \
    && chmod 755 /tmp/rustup-init \
    && /tmp/rustup-init -y --no-modify-path --profile minimal --default-toolchain "$RUST_VERSION" \
    && rm /tmp/rustup-init \
    && rustc --version | grep -F "rustc $RUST_VERSION "

RUN curl --fail --silent --show-error --location \
        "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
        -o /tmp/node.tar.xz \
    && printf '%s  %s\n' "$NODE_SHA256" /tmp/node.tar.xz | sha256sum --check \
    && mkdir -p /opt/node \
    && tar -xJf /tmp/node.tar.xz --strip-components=1 -C /opt/node \
    && rm /tmp/node.tar.xz \
    && test "$(node --version)" = "v${NODE_VERSION}"

RUN mkdir -p /workspace && chown 10001:10001 /workspace
WORKDIR /workspace

FROM toolchain AS candidate
USER 10001:10001

FROM candidate AS controller
USER root
ARG DOCKER_CLI_PACKAGE
RUN apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends "$DOCKER_CLI_PACKAGE" "$AGE_PACKAGE" \
    && rm -rf /var/lib/apt/lists/*

COPY tools.lock.json install-tools.sh /opt/darwin-runner/
RUN chmod 755 /opt/darwin-runner/install-tools.sh \
    && /opt/darwin-runner/install-tools.sh \
    && install -m 755 /opt/darwin-runner/install-tools.sh /usr/local/bin/darwin-install-tools
COPY candidate-exec.sh /usr/local/bin/darwin-candidate-exec
COPY run-darwin.sh /usr/local/bin/darwin-runner-evolve
COPY dispatch-contract.ts bridge-listener.ts bridge-fixture-worker.ts /opt/darwin-runner/
RUN chmod 755 /usr/local/bin/darwin-candidate-exec /usr/local/bin/darwin-runner-evolve
