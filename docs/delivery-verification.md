# Arrow collector delivery and verification

This document maps the requested GitHub issue numbers to concrete repository
evidence. It records delivered files and local verification only. It does not
claim that any GitHub issue is closed, that a pull request is merged, or that a
GHCR image has been pushed.

## Delivery matrix

| Issue | Delivered scope | Concrete files and features | Verification |
| --- | --- | --- | --- |
| #8 | Durable, schema-enforced direct Arrow collector | `projects/arrowcol-rust/`; `crates/arrowcol-catalog/`; `crates/arrowcol-flight/`; `crates/arrowcol-buffer/`; `crates/arrowcol-receipts/`; SQL/Hive migrations in `projects/arrowcol-infra/migrations/` | `arrowcol-rust:verify`, direct live E2E, and `loadtest:e2e` schema/receipt/crash/fencing probes |
| #11 | DFE-based arbitrary-Arrow collector | `projects/arrowcol-dataflow-rust/`; `crates/arrowcol-dataflow-components/`; representation-local Arrow envelope, bounded DFE admission, fenced Delta exporter, honest capability document | `arrowcol-dataflow-rust:verify`, component tests, DFE live E2E, and the dataflow phases of `loadtest:e2e` |
| #15 | Direct-versus-DFE benchmark and Grafana comparison | `projects/loadtest/src/`; `projects/loadtest/compose.yaml` | loadtest Rust/TypeScript tests, Compose validation, finite E2E, and continuous `run` workflow |
| #18 | GHCR-ready OTEL collector image | `projects/otelcol-rust/Dockerfile`; `projects/otelcol-rust/project.json`; `projects/otelcol-rust/scripts/image-smoke.sh`; `.github/workflows/publish-images.yml` | `otelcol-rust:image-smoke` and image-plan/registry workflow tests; no publication claimed from this workspace |
| #19 | GHCR-ready direct, DFE, and router images | Dockerfiles, Nx image/publish targets, and image-smoke scripts under `projects/arrowcol-rust/`, `projects/arrowcol-dataflow-rust/`, and `projects/arrowcol-router-rust/`; shared publication workflow | Three Arrow image-smoke targets and image-plan/manifest tests; no publication claimed from this workspace |
| #22 | Load generated from catalog-owned schemas | `projects/loadtest/schemas/sqlserver.sql`; `projects/loadtest/loadgen/src/sdk.rs`; `generator.rs`; `transport.rs`; `crates/arrowcol-sdk-gen/` | SDK generator verification, exact artifact regeneration, generated-package tests, schema-contract manifests, and finite loadtest validation |

## Historical qualification snapshot

The following qualification was recorded on October 12, 2026. It is historical
baseline evidence, not proof of the current lakehouse integration, Azure
publication, maintenance, tenant streaming, or QAP stress. Generated evidence is
local and gitignored.

| Command | Result |
| --- | --- |
| `npx --no-install nx run loadtest:e2e` | Passed. OTEL run `e2e-6b61eba5-7388-4904-90ea-f02a2d645100` validated 8 tables and 31 committed files. Arrow run `e2e-arrow-4a73dc58-4091-4eeb-a7c0-c09529129a38` reported `passed: true`, 13 direct tables, 13 DFE tables, 17 direct files, 52 DFE files, all six collectors receiving traffic, and 26 pool/table leases. |
| Arrow E2E probes within the same command | Passed: breaking schema rejected before receipt, two byte-identical direct deliveries received distinct published receipts, bounded in-flight observation equaled the configured limit of 2, and four DFE probe batches were admitted. |
| Arrow chaos phase within the same command | Passed: owner kill plus lease-expiry resend, greater-generation restart, stale publication fencing, graceful direct/DFE drain and restart, bounded SQL outage failure, and SQL recovery. |
| `npx --no-install nx run-many -t verify --projects=arrowcol-catalog,arrowcol-protocol,arrowcol-flight,arrowcol-buffer,arrowcol-receipts,arrowcol-routing,arrowcol-sdk-gen,arrowcol-dataflow-components,arrowcol-rust,arrowcol-dataflow-rust,arrowcol-router-rust,tools-scripts --parallel=4` | Passed in 1 minute 14 seconds: 12 requested projects, 69 dependency tasks, and 81 successful tasks; 4 tasks used valid cached output. |
| `npx --no-install nx run loadtest:test` | Passed: 6 Delta inspection tests, 7 Arrow load-generator tests, and 21 TypeScript tests. |
| `npx --no-install nx run loadtest:compose-check` | Passed. |
| `npx --no-install nx run-many -t image-smoke --projects=otelcol-rust,arrowcol-rust,arrowcol-dataflow-rust,arrowcol-router-rust --parallel=2` | Passed in 49.7 seconds: all four local `:dev` images built and all four smoke targets passed; no registry login or push occurred. |
| `python3 docs/validate.py` | Passed: 22 Markdown files, 21 local links/anchors, 111 Nx target references, 18 Cargo package references, 45 documented paths, and prohibited public-reference checks. |
| `git diff --check` | Passed. |

## Historical baseline limitations

These limits describe that recorded baseline. For the current publication and
consumer boundaries, see [storage](architecture/arrowcol-platform/storage.md),
[delivery guarantees](architecture/arrowcol-platform/delivery-guarantees.md),
and the [lakehouse loadtest](architecture/loadtest/README.md).

- **Local shared-volume semantics:** the benchmark's three direct collectors
  share one host directory, and the three DFE collectors share another. This
  validates local process ownership, failover, and fencing, not remote object
  store commit behavior.
- **One-owner hot-table limit:** routing assigns one owner for each
  pool/table. Additional replicas distribute different tables but do not
  concurrently write one hot table inside the same pool.
- **DFE best effort by default:** without native Quiver, successful Flight
  completion confirms bounded DFE admission only. It provides no SQL receipt,
  polling, purge signal, or at-least-once guarantee.
- **Native Quiver is local-disk durability:** with native Quiver, both pools
  fsync each `arrowcol.flight@1.1` or Direct receipt batch to a local Quiver WAL
  before acknowledging it. They replay it after a crash at least once, without
  deduplication. Losing the disk loses the pending batches.
  `loadtest:e2e` kills both collectors mid-load and validates every
  acknowledged row.
- **Publication status:** local image builds and smoke tests use `:dev`. No
  GHCR image is published until the GitHub publication workflow runs and
  verifies its remote manifests.
