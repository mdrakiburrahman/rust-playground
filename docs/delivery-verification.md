# Arrow collector delivery and verification

This document maps the requested GitHub issue numbers to concrete repository
evidence. It records delivered files and local verification only. It does not
claim that any GitHub issue is closed, that a pull request is merged, or that a
GHCR image has been pushed.

## Lakehouse integration scope

The current integration covers only the following five issues. The older matrix
below is retained as historical context, not as additional integration scope.

| Issue | Implementation surfaces | Qualification boundary |
| --- | --- | --- |
| [#50](https://github.com/mdrakiburrahman/rust-playground/issues/50) | `infra/azure/`: Bicep, provisioning, identity, generation authority, guarded operator commands | Offline policy and recovery checks are not a live Azure deployment or reset qualification |
| [#44](https://github.com/mdrakiburrahman/rust-playground/issues/44) | Native external Delta/QAP in `projects/loadtest/loadgen/`; optional `projects/kusto-bridge/` | Native active-snapshot semantics differ from the historical tenant fork; local emulator results are not Azure performance evidence |
| [#36](https://github.com/mdrakiburrahman/rust-playground/issues/36) | `crates/arrow-deltalake/`; `crates/delta-maintenance/` | Production uses Delta Kernel; checkpoints and eligible log pruning preserve publication/query transactions, not a whole-storage byte plateau |
| [#48](https://github.com/mdrakiburrahman/rust-playground/issues/48) | `projects/loadtest/`: lifecycle, generated clients, all-13-schema fixtures, independent reader, LGTM | Component checks, source-built images, live workload correctness, and measured stress are separate claims |
| [#47](https://github.com/mdrakiburrahman/rust-playground/issues/47) | `crates/arrow-tenant-stream/`; sorted customer layout in `crates/arrow-deltalake/` | Historical snapshot then logical tail, Spark-compatible recovery, and independent sink replay are distinct from native Kusto snapshot tracking |

The [root loadtest architecture](../loadtest-architecture.html) and its
[browser-rendered PNG](../loadtest-architecture.png) show the integrated
publication, consumer, authority, and evidence boundaries.

## Source-bound integration evidence

These are intermediate records, not a final-head green-CI or cloud qualification.
Raw runtime logs remain private. Exported evidence must select causal fields and
redact them before truncation; the receipt redactor is not a general-purpose
sanitizer for arbitrary startup logs or configuration dumps.

| Source and command | Observed result |
| --- | --- |
| `238c4a218bc6f4079515cf2f29bb082f74b7811c`: combined ten-package, all-feature Cargo tests | 332 passed, zero failed, four existing ignored tests |
| Same source: `npx --no-install nx run tools-scripts:verify` | 95 tests passed |
| Same source: `npx --no-install nx run loadtest:build --outputStyle=static` | All nine tasks succeeded, zero cache hits, 18 minutes 6 seconds; these are local images, not published registry references |
| Same source: `npx --no-install tsx projects/loadtest/src/main.ts --mode local --warmup 5s --measure 30s --batch-rows 1024 --validate` | Failed during Direct receipt drain: 658 receipts, 608 published, 50 failed, all on `telemetry.sqlserver.sqlserver_database_properties`; both collectors reported the sorted-file input-budget failure |
| Quiver canonical fixture and walkthrough at that source | Six tests passed; all 3,546 original CSV rows retain their original values after adding explicit synthetic customer labels |

The actual failure cause was `write Delta data file: batch exceeds sorted-file
input budget; slice it before retrying`, not an inferred UInt8 conversion error.
The source-built Direct, DFE, router, and Explorer image IDs and the three reader
bundle hashes were recorded with that failed run. A subsequent correction must
be rebuilt into both collector images and rerun with the required 1,024-row
workload; lowering that workload is not equivalent qualification.

The corrected collector source
`8c15aaf7ee75f7dd5c391c6b78bcc4e8de5c19b6` was rebuilt into both real
collector images and reran that same 1,024-row command. All 636 Direct receipts
published, including 49 for database properties. DFE recorded 636 best-effort
admissions, zero error outcomes, zero publication-failure warnings, and zero
occurrences of the previous budget cause. This does not substitute for final
independent DFE row validation: the overall run subsequently failed the live
Grafana SDK-metric gate and observer cleanup.

The preserved live LGTM data showed `delta_kernel_version_ratio` for both
collector service names, but no `delta_kernel_version`. The version gauge's
dimensionless unit `"1"` caused the Prometheus translation suffix. The focused
emitter correction removes that unit rather than weakening the required live
query. Its actual rebuilt end-to-end qualification remains separate from the
publication recovery above.

### Checkpoint/prune interoperability proof

The bounded native harness ran on clean native source
`191e3e5d8b8a15b58ec222a810f70207acd1c86c`, with maintenance source
`01d5259fa1b10c1733da27b7aff77ba6b04a09dd`:

```bash
DELTA_MAINTENANCE_REF=01d5259fa1b10c1733da27b7aff77ba6b04a09dd \
  projects/loadtest/loadgen/scripts/kusto-qap-it.sh prune
```

Both cycles passed after checkpoint, eligible pruning, physical tombstone
vacuum, and emulator restart. Source floors advanced from 5/4 to 7/6. Each table
retained its current `C.json` and checkpoint; eligible history strictly below
`C`, including the previous floor, was removed. Publication and query
transactions remained present. Cached, fallback, and independent exact rows
agreed at version/row-count pairs 5/50 and 4/110, then 7/50 and 6/130.

The emulator was pinned to
`mcr.microsoft.com/azuredataexplorer/kustainer-linux@sha256:51e2250da2faf292de903f871a073eb37d3f2641cf63381b183879d21ebb1013`.
The 300-second limit was **per phase**. Zero retention was restricted to marked
temporary test fixtures with `--unsafe-short-vacuum-for-tests`; the production
VACUUM default remains seven days. This is not the production global five-minute
all-table deadline, all-13-schema Flight qualification, Azure I/O, or a claim that
total storage bytes plateau.

### Live Azure limitation

The configured subscription returned ARM `SubscriptionNotFound`, including after
credential refresh. No alternate subscription was selected. Provisioning,
real Azure stress, physical reset, and cluster stop are not qualified by local,
offline, or skipped credential-gated checks. Public reset execution remains
disabled; preview is not execution approval.

## Historical delivery matrix

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
