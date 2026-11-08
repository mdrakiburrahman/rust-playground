FROM rust:1.96.1-bookworm AS rust-build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY . .
RUN cargo build --locked --release --package arrowcol-experiment --package delta-maintenance

FROM node:24.22.2-bookworm-slim

ENV NODE_ENV=production
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY infra/azure/ ./infra/azure/
COPY experiments/azure-qap/ ./experiments/azure-qap/
COPY tools/libs/experiment-kit/src/ ./tools/libs/experiment-kit/src/
COPY --from=rust-build /workspace/target/release/arrowcol-experiment ./target/release/arrowcol-experiment
COPY --from=rust-build /workspace/target/release/delta-maintenance ./target/release/delta-maintenance
USER 10001:10001
ENTRYPOINT ["node_modules/.bin/tsx", "experiments/azure-qap/src/job.ts"]