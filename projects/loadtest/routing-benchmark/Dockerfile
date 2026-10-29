FROM debian:bookworm-slim@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 \
    && rm -rf /var/lib/apt/lists/*
COPY bin/ /usr/local/bin/
COPY sdk-toolchain/ /opt/benchmark-rust/
ENV PATH="/opt/benchmark-rust/bin:${PATH}"
STOPSIGNAL SIGTERM
