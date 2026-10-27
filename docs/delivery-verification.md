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
| #50 | `infra/azure/`: Bicep, provisioning, identity, generation authority, guarded operator commands | Offline policy and recovery checks are not a live Azure deployment or reset qualification |
| #44 | Native external Delta/QAP in `projects/loadtest/loadgen/`; optional `projects/kusto-bridge/` | Native active-snapshot semantics differ from the historical tenant fork; local emulator results are not Azure performance evidence |
| #36 | `crates/arrow-deltalake/`; `crates/delta-maintenance/` | Production uses Delta Kernel; checkpoints and eligible log pruning preserve publication/query transactions, not a whole-storage byte plateau |
| #48 | `projects/loadtest/`: lifecycle, generated clients, all-13-schema fixtures, independent reader, LGTM | Component checks, source-built images, live workload correctness, and measured stress are separate claims |
| #47 | `crates/arrow-tenant-stream/`; sorted customer layout in `crates/arrow-deltalake/` | Historical snapshot then logical tail, Spark-compatible recovery, and independent sink replay are distinct from native Kusto snapshot tracking |

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
query. The subsequent independently source-built
`69b157416916d6abcc22b46c5510558a67406caf` run published all 1,656 Direct receipts
and passed that unchanged SDK gate: both collector service names supplied 112
plain-name version-gauge samples. This is actual focused emitter evidence,
separate from an overall pipeline or maintenance qualification.

That run's fixed-window driver failed before either pool's physical CPU fixture
was ready and completed zero maintenance cycles. Final all-13/native validation
was subsequently terminated at its 600-second outer command deadline. No final
independent oracle, native, UI, or overall pass is claimed. Readiness must be
established within the existing startup deadline before the immutable window;
validator-cost diagnosis must preserve the native global five-minute deadline.

Read-only diagnosis subsequently measured 318.4 seconds and 316.0 seconds in the
two serial, pre-Kusto snapshot summaries: that failed command had not reached
its native Kusto phase. Parallel reader correction
`6d8a88df4a1763b1db7bc2773eefc51ecf562bde` is paired with reviewed corrective
tail `ada67e4c07a071e771c99e8fd7f5fce9e4a908b1`: file summaries merge in
ordered two-worker batches, and sampled rows aggregate checked multiplicities
within the existing distinct-row guards rather than retaining every decoded
row. Source-isolated reader qualification remains pending; timing measurements
are not a native qualification.
The approved readiness changes require compactable CPU fixtures from both
pools and actual remaining generator input before starting the immutable
window. Raw reader stderr is retained only in private staging outside the
benchmark/onelake artifact trees; the CI uploader's private-log exclusion is
additional defense, not the privacy boundary.

### Current acceptance gaps

The separate-process append/maintenance exercise exposed 24 unlogged Parquet
files from newly staged replays of already published identities. VACUUM LITE
cannot reclaim files that were never logged as an Add or Remove. The focused
correction scopes reclamation to fresh, never-submitted stages of the same
table, preserves uncertain submissions, and reports cleanup failures using fixed
classes rather than paths or raw errors. Its process harness owns a Linux process
group and subreaper with checked bounded cleanup.

Actual reconciled candidate `ab38407754cd9d5904e1b81b34319219a9ba93e1`
passed all eight replay tests and the hung-child cleanup regression, but the
separate writer/maintenance process exercise **still failed**, reporting 24
unreferenced Parquet files. The commands ran from that candidate with no
concurrent target writer. Subsequent read-only source/blob and binary checks
found that the process binary lacked the reclaim code, although the replay test
binary contained it. Reusing one Cargo target across checkouts had retained a
stale default-feature dependency. The failed receipt is not retroactively a
pass: a source-bound, initially empty dedicated target, binary hashes and
expected-code checks, and an actual process pass are required. Future candidate,
helper, and image qualification must isolate build targets by source revision.
Neither replay-orphan freedom nor a whole-storage byte plateau is qualified by
the stale-artifact result.

A new, initially empty, source-specific target at
`6eb418175efba48cbf305e949858e8268c08304a` passed eight replay tests, three
canonical tests, both process tests, and the increased-retention prune test.
The actual separate writer/maintenance exercise completed six cycles, five
overlaps and 24 publications, with three collisions, a maximum of two attempts,
142 rows, 32 files, and **zero replay orphans**. Source and tracked status were
unchanged before and after the execution.

The executing writer/test binary had SHA-256
`e6756dbfa6695f6b6ffc21ce5b75d1120e20fc82cb1e36fef917cfae322c17c8`;
the service binary had SHA-256
`de2704124e4342295c84490a17ba64ccb3dca37201876c9a208832b02ab91fad`.
The writer contained the reclaim-code marker. An initial provenance guard
incorrectly required that append-only marker in the maintenance service too
and refused execution. The service never appends, so the linker discards that
code; its corrected guard instead checked three relevant retention/prune
markers from its own source. The failed guard receipt remains separate.
The corrected guard and first actual process execution passed without changing
production code or test assertions. This qualifies the bounded replay/process
scenario, not a whole-storage plateau or the unrelated aggregate reader failure.

Combined candidate `18bbe96ec14443919e2e54317c97c8e4e824f361` passed all
137 TypeScript tests with zero skips, TypeScript checking, full Rust format
checking, locked workspace metadata, and `tools-scripts:verify`.
Its real maintenance CLI contract used the verified service
binary above, with unchanged maintenance/library source and manifests.
The optional generator declaration binds warmup, measure, capture settling and
startup budgets to the persisted plan hash; old plans remain distinct.
The host helper must still be rebuilt for the new progress fields.
Selected host and mounted helper hashes are recorded without filesystem paths,
must match, and are rechecked after stack startup; they do not prove build
freshness or source correctness on their own. The optional initializer fixtures
use a nondefault core seam also gated by debug assertions, with only two
additional loadgen dev-dependency lock edges and no package-version changes.
Their library checks do not qualify the actual successful release CLI,
environment/manifest, and Python child chain or enable public reset execution.
These checks do not qualify combined-source Rust binaries, images, or live stress.

CI run `40459829984` at
`ef0a14d33fc1025af0bda401fc15d893231ad455` finished with 44 successful jobs and
two failed jobs. Its actual `loadtest:e2e` failed the first Grafana panel's
three-numeric-sample proof after all 15 required metric-range checks passed,
including the unsuffixed version gauge for both collector services. The final
verification job merely reflected that failed prerequisite. The failing frame
and request resolution were not retained, so their exact cause is unknown.
Bounded, allowlisted frame-shape diagnostics preserve the unchanged per-frame
threshold; a read-only query on an older preserved dataset is not a CI
reproduction or a fix.

The local aggregate `npx --no-install nx run rust:verify --outputStyle=static`
at the same source also failed, with return code 130. The underlying failed
target was `loadtest:test`: the actual reader CLI evidence-chunks regression
reported `Parquet error: Required field schema is missing`. Thirty subsequent
isolated iterations of the same reader tests passed; the aggregate failure's
cause remains unproven. This is not a settled-final-head verification pass.

The external GitGuardian check on integration source
`ef0a14d33fc1025af0bda401fc15d893231ad455` reported two occurrences of one
synthetic JWT-shaped redaction-test canary. Independent source inspection found
only a `canary` claim and an undersized, invalid HS256 signature; it is not an
authenticating credential. The check remains blocked pending classification by
an authenticated owner in the [GitGuardian dashboard](https://dashboard.gitguardian.com).
No check is suppressed or disabled, and no history is rewritten. This
classification does not establish that the overall integration CI is green.

### Actual collector source reads and tenant snapshot/restart

The ignored, private-fixture test at original source
`b21d95253a4ecb04385967977fefd6702656ee9a` read pristine collector-produced
CPU tables from both pools of frozen collector source
`69b157416916d6abcc22b46c5510558a67406caf`, SDK child
`e008d64e806ebccb5df1c8ab598c3de453d34fab`:

```bash
ARROWCOL_SOURCE_READ_FIXTURE='<private-fixture-root>' \
  cargo test --locked -p arrow-tenant-stream --test collector_source_skipping \
  actual_collector_cpu_sources_skip_bytes_and_row_groups_in_both_pools \
  -- --ignored --exact --nocapture
```

Independent execution passed in 45.84 seconds. Each pool had 127 files,
130,048 rows, and 254 row groups. The selected customer's 8,128 complete rows
matched both a full scan and an identically reinterleaved unsorted control.
Actual DataFusion bytes read were 51,816 for the sorted filtered scan, 107,696
for its full scan, and 216,154 for the unsorted filtered scan. The sorted scan
visited 127 row groups and pruned 127, versus 254 visited and zero pruned.
Page pruning reported 381 pages and 48,768 rows pruned.

All 127 files were opened: **no file-pruning improvement is claimed**. The
requested 128-row page setting is not a measured physical page-size result.
Whole-artifact/per-file hashes, pristine Add/Txn history with no Removes, and
the unchanged manifest bound this proof to original collector output.

That source also exposed a real tenant service failure: already canonical
timestamp fields and nanosecond companions were normalized a second time.
Original core correction `632b72e5af721d88959ec2bab9ea0b7fddbd00ac` provides an
explicit typed canonical write path; tenant wiring
`724ad75cb8ab6cb6a92d5eca5ddd94874698f044` uses it without dropping columns,
metadata, null masks, or timestamp precision. Raw reserved-name rejection stays
unchanged.

The corresponding ignored service test uses the same private-fixture variable
and test target, selecting
`actual_service_forks_frozen_collector_sources_and_restarts_idle_in_both_pools`.
Independent exact-source execution passed in 12.72 seconds with fixed binary
SHA-256 `3716ddaa6d616fa6ee33bf7cde26d8ac53ef011718d24bf268f055f1ff15a698`.
Both pools copied exactly 8,128 full rows into five partition files at sink
version one, with complete schema/multiset/partition, Spark checkpoint,
stable query transaction, and idle-restart checks. All 417 source file
hashes, sizes, and inventory entries remained unchanged.

Test-only followups `533dfb9b9df49741c7b48241f0ee086e44211a79` and
`7ab4966f11d6f4a9140c6f1423a5e05240db880b` add the ignored selector
`actual_service_live_append_optimize_purge_prune_and_restart_on_disposable_copies`.
Exact final assertions passed once in 41.69 seconds using the same fixed
service binary on private mutable copies. Both pools advanced from 8,128 to
8,192 rows after a separate canonical writer append, then to 8,256 after
restart and another append. The scenario exercised real OPTIMIZE and PURGE,
historical tenant-row retention, a durable committed cursor and clean stop,
marked-copy unsafe VACUUM, retained checkpoint/current `C.json`, pruning
strictly below the floor, and idle restart. Every planned obsolete vacuum
candidate was physically absent and the retained active file remained.
The Direct/DFE floors were 130/37. All 417 original source-file hashes, sizes
and inventory entries were unchanged.

The original orchestration envelope nevertheless returned one despite Cargo
returning zero and printing the passing assertions. A separate custody/receipt
check returned zero; the orchestration discrepancy remains unexplained.
The assertions are a bounded pass, **not a whole-job pass**.

These are explicit artifact-backed executions, **not ordinary CI execution of
the ignored fixtures**. They qualify frozen-source reads, snapshot/idle restart,
and the disposable-copy joined lifecycle scenario, not the failed collector
fixed window, production native global deadline, all-schema service execution,
final combined-source E2E, or Azure.

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
