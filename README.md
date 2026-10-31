# Rust Playground

A Linux-first Rust learning monorepo organized with Cargo and Nx. It keeps
independent applications under `bin/`, reusable libraries under `crates/`, and
repository automation under `tools/`. The initial sample combines the
`greeting` library with the `hello-world` CLI and its Linux container image.

## Development environment

Development is Linux-first in WSL and uses the repository devcontainer:

- Ubuntu 24.04
- Rust 1.96.1, rustfmt, Clippy, cargo-make, mold, clang, and LLDB
- Node.js 24 and Nx 23
- Azure CLI, GitHub CLI, and Docker Buildx through Docker-outside-of-Docker

The source configuration builds `.devcontainer/Dockerfile` and applies the
locked Dev Container Features. The default configuration uses the immutable
GHCR image pinned in `.devcontainer/docker-compose.yml`.

The host bootstrap installs only shared developer-machine prerequisites.
Repository dependencies are installed by the devcontainer's post-create step.
The WSL host's `~/.azure` and `~/.config/gh` directories are mounted read/write
into the devcontainer. See
[headless devcontainer operations](docs/devcontainer/headless-operations.md)
for exact keep-running, monitoring, and targeted-cleanup commands.

## Quick start

After completing the one-time setup in [CONTRIBUTING.md](CONTRIBUTING.md),
install the pinned host CLI dependencies and build the workspace from source:

```bash
npm ci
npx nx run devcontainer:test-source
```

To use the checked-in immutable prebuilt image instead:

```bash
npm ci
npx nx run devcontainer:test
```

Both targets start the selected devcontainer and run every Nx `verify` target
inside it. Stop only this workspace's Compose project when finished:

```bash
npx nx run devcontainer:down -- --volumes
```

## Arrow collector platform

The repository contains a complete local Arrow Flight collection platform:

```text
Flight SQL DDL
      |
      v
SQL Server application catalog + Apache Hive Metastore
      |
      +--> immutable manifest.json / schema.arrow / descriptor.pb
      |         |
      |         +--> generated Rust and TypeScript SDKs
      |
generated clients
      |
      v
Pingora router -- one fenced owner per implementation pool and table
      |                                      |
      +--> Direct collector pool             +--> DFE collector pool
      |      SQL receipts                         best-effort admission
      |      bounded Arrow buffers                bounded DFE channels
      |                                      |
      +--> shared direct Delta root           +--> shared DFE Delta root
                         |
                         +--> independent validation and Grafana
```

The direct pool returns a server-generated receipt for each `RecordBatch`.
That initial Flight result is **not purge permission**. A client may purge its
source copy only after the receipt HTTP API reports `published` and
`purge_allowed: true`, after the fenced Delta commit is durable. The DFE pool
honestly reports only best-effort admission and has no receipt polling or
at-least-once claim. With native Quiver (`loadtest:run -- --quiver`), both pools
fsync every batch to local disk before acknowledging it and replay it after a
crash, so DFE completion becomes durable admission. [Quiver on disk](docs/quiver/README.md)
walks through those files with `quiver-cli`. See the
[platform architecture](docs/architecture/arrowcol-platform/README.md) for the full
catalog, router, receipt, fencing, buffering, and capability contracts.

### Lakehouse loadtest architecture

[![ArrowCol lakehouse loadtest: two pools, maintenance, tenant forks, Kusto, and LGTM](loadtest-architecture.png)](loadtest-architecture.html)

The [interactive architecture source](loadtest-architecture.html) follows
13 multi-customer schemas through both collector pools into independent Delta
roots, ingestion-time partitions, standalone maintenance, historical tenant
forks, and Kusto. Local mode pairs disk with the Kusto emulator. Native external
Delta/QAP is the default validation target; the optional compatibility bridge is
functional-only and never counts as native QAP evidence. Azure mode uses ADLS
Gen2 and native external-Delta query acceleration with a dedicated managed
identity. Independent validation and LGTM observe the same run.

The tenant fork preserves copied history after source purge; the local bridge
instead reconciles the current active snapshot. Stable publication identities
protect retries of the same publication, not duplicate independent client
requests. Local functional results are not Azure QAP performance evidence.
See the [loadtest guide](projects/loadtest/README.md) for the current CLI,
credential boundaries, lifecycle commands, and qualification artifacts.

### Experiments

Experiments isolate a behavior with small deterministic fixtures, independent
oracles and durable evidence; they do not run the lakehouse loadtest.

The [Azure QAP experiment](experiments/azure-qap/README.md) examines whether
append, OPTIMIZE, old-partition DELETE and physical VACUUM reload unchanged data.
See the [lessons learned, with diagrams](docs/experiments/azure-qap/README.md).

### Finite qualification quick start

Run from the repository root in the source devcontainer:

```bash
git submodule update --init --recursive
npm ci

# Bootstrap and verify the standalone SQL Server + Hive control plane.
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:integration-verify
npx --no-install nx run arrowcol-infra:down

# Verify deterministic catalog artifact and Rust/TypeScript SDK generation.
npx --no-install nx run arrowcol-sdk-gen:verify

# Run finite paired Arrow and Quiver crash/recovery qualification.
npx --no-install nx run loadtest:e2e
```

The Arrow phase registers
[`projects/loadtest/schemas/sqlserver.sql`](projects/loadtest/schemas/sqlserver.sql)
through Flight SQL, exports the immutable catalog artifacts, generates and
checks both SDK packages, starts the router and both collector pools, and
validates both Delta roots. The second E2E phase enables Quiver and kills and
restarts both collectors during traffic to exercise durable replay.

The current runner retains output beneath `onelake/benchmark/latest/` and prints
its exact output directory. A new start may reset only a sealed, validated local
attempt after verifying its private corpus and evidence archive. Failed,
interrupted, nonvalidated, Azure, or unmarked legacy attempts block reset and
preserve their original corpus. See the [loadtest lifecycle and evidence
guide](projects/loadtest/README.md#per-attempt-evidence-custody) for the reset
preconditions. Focused collector live E2Es retain their own evidence
under `projects/arrowcol-rust/.diagnostics/<run-id>/` and
`projects/arrowcol-dataflow-rust/.diagnostics/<run-id>/`.

### Continuous comparison and dashboards

```bash
npx --no-install nx run loadtest:run -- \
  --warmup 2m --concurrency 8 --batch-rows 1024
```

Omitting `--measure` runs generation until interrupted. The retained local stack
exposes Grafana at `http://127.0.0.1:3000` and Prometheus at
`http://127.0.0.1:9090`. Arrow Flight and optional OTEL baseline signals remain
separate. Use the canonical loadtest guide for current lifecycle and mode
options rather than the removed `benchmark-down`/`--run-id` interface.

See the [loadtest guide](projects/loadtest/README.md) for diagnostics and the
[load generator diagram and runbook](docs/architecture/loadtest/README.md) for
the generated-schema data path and exact run commands. See the
[delivery and verification matrix](docs/delivery-verification.md) for issue
coverage, exact qualification evidence, and intentional limitations.

## Nx targets

Run repository targets inside the devcontainer:

```bash
npx nx run rust:format
npx nx run rust:format-check
npx nx run rust:lint
npx nx run rust:build
npx nx run rust:test
npx nx run rust:verify
npx nx run auth-automation:verify
npx nx run tools-scripts:verify
npx nx run rust-playground-build:verify
npx nx run devcontainer:verify
```

`rust:lint` checks all first-party Markdown as well as Rust Clippy. To check or
autofix Markdown alone:

```bash
npx --no-install nx run tools-scripts:markdown-lint
npx --no-install nx run tools-scripts:markdown-lint-fix
```

Markdown checks include new files and hidden documentation directories, exclude
submodules and ignored/generated output, and allow long lines and inline HTML.
CI runs read-only lint and rejects tracked changes or unexpected untracked files
after verification. Check that gate explicitly on a committed checkout with:

```bash
npx --no-install nx run rust:fail-on-untracked-files
```

The gate is separate from local lint so uncommitted authoring remains supported.

Run all verification targets serially with:

```bash
npx nx run-many -t verify --all --parallel=1
```

## Parallel GitHub CI

CI discovers the complete Nx verification graph, then fans out independent
checks onto separate runners. Matrix concurrency is uncapped; dependencies,
not a global serial queue, determine when work starts.

```text
                           +--> Node checks --------------------+
  Nx task discovery -------+--> runtime image builds ---+        |
                           |                            |        |
                           +------------------------+   |        |
                                                    |   |        |
  locked source environment --> shared image -------+   |        |
                                  |                     |        |
                                  +--> Rust checks -----|--------+
                                  +--> Delta reader ----+        |
                                                       |        |
                                                       v        |
                                                artifact E2E ---+
                                                                |
                                               final CI gate <--+
```

Each Rust shard uses the same source-built tools, its own Cargo cache, and
only its Nx prerequisite closure. Same-project Rust checks share compiled
dependencies instead of duplicating them across runners. The source image is
cached by its exact build inputs, identity-verified, and transferred using
multithreaded Zstandard. Native CI uses mold without debug/incremental cache
bloat; application release builds are unchanged.

Runtime images use separate persistent BuildKit caches. E2E verifies the
exported image identities and reader executable before running; it does not
rebuild them or wait for unrelated checks. Every shard checks its worktree,
scoped cleanup always runs, and the final `Verify in source devcontainer`
check fails if any branch fails, is canceled, or is skipped. No runtime images
are published by CI.

## Cleaning temporary state

Every Nx project has an uncached `clean` target. The root `rust:clean` runs all
project cleanup targets, then cleans root-owned temporary state:

```bash
npx --no-install nx run rust:clean
npx --no-install nx run loadtest:clean
npx --no-install nx run greeting:clean
```

Cleanup removes only each project's `.temp/`, `.ci-logs/`, `logs/`, and
`coverage/`. `loadtest:clean` also removes the root `onelake/` directory,
including retained demo tables and E2E diagnostics. Missing state is a no-op;
directory symlinks are removed without following their targets.

Managed caches and build outputs are preserved: `node_modules/`, `.nx/`,
Cargo caches and `target/`, Docker images/volumes/build caches, `dist/`, and
`out/`. Configuration, credentials, and cached authentication profiles are
not removed.

These targets clean files only; they do not stop running processes or
containers. Stop the load test before cleaning its state.

## Sample CLI and image

Run the sample application:

```bash
npx nx run hello-world:run -- --name Ferris
```

Build and smoke-test its local Linux amd64 image:

```bash
npx nx run hello-world:image-smoke
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:dev \
  --name Ferris
```

After the public `latest` tag has been published and verified, the equivalent
prebuilt invocation is:

```bash
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:latest \
  --name Ferris
```

## Project structure

- `bin/` - For independent binary crates. Nothing should depend on any crates
  in here. This is like `cmd` in the `go` project. Crates within this folder
  will often contain accessories to the corresponding source code like
  `Dockerfile`s.
- `bin/quiver-cli/` - Writes real SQL Server rows to Quiver like the Direct
  collector and inspects any Quiver directory; see
  [Quiver on disk](docs/quiver/README.md).
- `crates/` - For library crates. Library crates may depend on each other and
  binary crates may also depend on them. This is just like `pkg` in the `go`
  project.
- `Cargo.toml` - This root level Cargo configuration defines the workspace and
  points to all other crates both bin and lib.
- `rust-toolchain.toml` - This pins the rust toolchain version that this repo
  uses.
- `tools/` - TypeScript CLIs, lifecycle helpers, and the local Nx Docker build
  plugin.
- `projects/otelcol-rust/` - Nx build/configuration and Docker image for the
  pinned, editable otel-arrow submodule at `submodules/otel-arrow`.
- `projects/loadtest/` - Two-service Compose demo and content-verified Delta
  E2E; generated telemetry lives in gitignored `onelake/`.
- `projects/kusto-explorer/` - Rust (Axum + Leptos) browser KQL workbench,
  wired into the loadtest against the local Kusto emulator.
- `.devcontainer/` - Reproducible source-built and immutable prebuilt
  development environments.

See [Rust project layout](docs/rust/project-layout.md) and
[adding Rust projects](docs/rust/adding-projects.md).

## Local Rust telemetry collector

```bash
git submodule update --init --recursive
npx --no-install nx run loadtest:run
```

See [loadtest](projects/loadtest/README.md) and
[collector development](projects/otelcol-rust/README.md#editing-the-submodule).

The [arrow-deltalake SDK](crates/arrow-deltalake/README.md) accepts pure Arrow
batches for Parquet-only or Delta writing. OTEL-specific routing and joins
live in the separate [Delta adapter](crates/otel-deltalake-exporter/README.md).
Writer-side Delta operations use Delta Kernel; delta-rs is confined to the
loadtest reader.

## Public GHCR images

The repository publishes these package locations. A tag should be treated as
available only after its manifest and public visibility have been verified:

| Image | Tags | Purpose |
| --- | --- | --- |
| `ghcr.io/mdrakiburrahman/rust-playground/devcontainer` | immutable content hash, branch alias, and `latest` from `main` | Development environment |
| `ghcr.io/mdrakiburrahman/rust-playground/otelcol-rust` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | OTLP-to-Delta collector |
| `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-rust` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | Direct Arrow Flight collector |
| `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-dataflow-rust` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | DFE-native Arrow Flight collector |
| `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-router-rust` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | Pingora Arrow Flight router |
| `ghcr.io/mdrakiburrahman/rust-playground/kusto-explorer` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | Browser KQL workbench for Kusto clusters |
| `ghcr.io/mdrakiburrahman/rust-playground/hello-world` | `branch-<sanitized-branch>`, `sha-<full-git-sha>`, and `latest` from `main` | Retained sample runtime image |

Local image qualification uses `:dev` and does not publish:

```bash
npx --no-install nx run otelcol-rust:image-smoke
npx --no-install nx run arrowcol-rust:image-smoke
npx --no-install nx run arrowcol-dataflow-rust:image-smoke
npx --no-install nx run arrowcol-router-rust:image-smoke
npx --no-install nx run kusto-explorer:image-smoke
```

No GHCR image is published merely by adding these targets, building locally,
or changing documentation. This uncommitted workspace has not published an
image. In the repository release path, publication begins only when
`.github/workflows/publish-images.yml` runs on GitHub and verifies the pushed
manifests.

See [container publishing](docs/containers/publishing.md) for login,
publication, visibility, and manifest verification. These documents describe
the expected operations; they do not assert that a workflow or publication
run is currently green.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT. See [LICENSE](LICENSE).
