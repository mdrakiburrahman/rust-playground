# loadtest

Every synthetic table has a required `customer_id VARCHAR` first column. The
deterministic input interleaves 16 customers (`customer-0000` through
`customer-0015`) rather than pre-sorting the input. Direct and Dataflow receive
the same immutable encoded batches, including retries. The independent reader
returns complete row multisets with multiplicities for Kusto validation, without
sampling or truncation. Native validation packs disjoint whole ingestion
partitions into chunks of at most 256 distinct complete rows. It fails on a
changed snapshot, a single partition over 256 distinct rows, or more than 65,536
distinct rows across a table.

[![Architecture](../../loadtest-architecture.png)](../../loadtest-architecture.html)

The independent delta-rs oracle reconstructs partition columns from committed
`Add.partitionValues`, not directory names, and excludes uncommitted Parquet
files. `delta-inspect evidence <root> --table catalog.schema.table` returns a
captured snapshot plus its bounded row multiset. Local roots and explicit-table
`abfss://` roots are supported; cloud reads use the existing Azure CLI identity
and fail visibly without credentials. Deletion vectors are explicitly rejected.

`delta-inspect evidence-chunks <root> --table catalog.schema.table` instead
returns `{snapshot, corpus}`, with each table's corpus containing
`partition_column`, exact `expected_rows`, `partition_rows`, and
`chunks: [{partition_values, rows: [[complete_row, multiplicity]]}]`. Partition
strings are the exact committed values; chunks never overlap, and their union
equals the keys of `partition_rows`. Unpartitioned tables use one unfiltered
chunk. The legacy `evidence` command keeps its original 256-distinct-row limit.
Native cached and fallback validation checks every chunk and exact partition
counts under the unchanged fixed five-minute cached deadline; timeout is failure.

Terminal `receipts.jsonl` entries retain an optional sanitized `error_message`
alongside `error_code`; missing legacy fields remain readable. Failed admissions
in `sent-batches.jsonl` use the same client export boundary. Causal text is
preserved while credentials, private URLs/paths/identifiers, known private
environment values, and control characters are removed, with a 1,024-character
Unicode-safe bound. These artifacts are written before stack cleanup.

## Fixed-window local functional rehearsal

`--validate --stress-plan <file>` runs real OPTIMIZE, checkpoint, and VACUUM LITE
jobs against `sqlserver_cpu_utilization` in both pools while paired input runs.
The other twelve tables in each pool are append-only controls. It retains the
ordinary all-thirteen-table final source/native checks and Explorer assertions.
This is a **local functional rehearsal**, not Azure stress/performance evidence.
Azure rejects this option before occupancy or workspace reset until a complete
ADLS physical-inventory adapter is available.

The version-one plan requires every field below. The source batch must also
match `--batch-rows`; layout overrides apply to both actual source collectors.
The current deterministic generator repeats sixteen complete payload rows,
so 8,192 physical rows do not imply 8,192 distinct oracle rows. The live reader
still enforces the unchanged per-partition and whole-corpus guards.

```json
{
  "version": 1,
  "durationSeconds": 180,
  "waveIntervalSeconds": 60,
  "operationTimeoutSeconds": 40,
  "startupTimeoutSeconds": 300,
  "sampleTimeoutSeconds": 10,
  "fixture": {"batchRows": 8192, "rowGroupRows": 512, "pageRows": 128},
  "limits": {
    "minimumCycles": 3,
    "minimumSamples": 4,
    "minimumDriverRowsPerSecond": 100,
    "maximumBacklog": 1000,
    "maximumRssBytes": 4294967296,
    "maximumPhysicalBytes": 10737418240,
    "maximumRelativeTrend": 0.1,
    "maximumRelativeVariation": 0.2
  }
}
```

These are example limits, not demonstrated capacity or a successful run.
The immutable window starts only after BOTH actual CPU tables have committed
nonempty source files, a physically measured multi-rowgroup file, and at least
two small files in the newest eligible closed partition. Readiness mirrors the
service's minute-granularity OPTIMIZE rules: the newest partition is protected
even when closed, `latest-partitions=1`, and the target/small-file sizes are
128/64 MiB. A declared warmup such as `--warmup 120s` can form this fixture;
warmup and readiness must fit the original absolute startup deadline.
`stress-readiness.json` records actual readiness time, snapshots, layout hashes,
and the generator lifetime. Before starting, the remaining measured input must
cover the entire immutable window plus the final sample budget; insufficient
input fails before a window is created. Neither deadline is extended.
`driver-progress.json` is atomically replaced every
second with run identity, phase start/duration/monotonic remaining milliseconds,
actual scheduled logical rows, send-policy failures,
and terminal receipt failures. Scheduled input is a pace counter, **not ACK
accounting or a correctness oracle**. Per-wave source snapshots, structured
operation reports, complete measured-root inventories, raw numeric resource
evidence, samples, and the one verdict are retained in `results/stress-*`.
Source Parquet footer captures must prove multiple row groups before the first
wave. The first OPTIMIZE in each pool must actually reduce file count through
replacement; a no-op cannot qualify that fixture. Complete-row oracle limits
remain 256 distinct rows per partition and 65,536 per table, with no truncation.

Samples retain their real acquisition timestamps. The predeclared sample budget
is also the maximum endpoint lateness; an early final sample cannot shorten the
window. Input cessation, unavailable counters, operation/sample timeout,
unexpected restart, OOM, exhausted source delivery, or unstable measured
resources cannot pass. The controller never extends the window or retries until
green. Docker CLI termination is not container termination: owned maintenance
containers require the current runtime's ownership token and Compose identity
before cleanup by immutable container ID. Stopped failures remain available for
private Docker-log diagnosis; unverified containers are never stopped.
Raw independent-reader stderr is retained only in an owned mode-0700 gitignored
scratch directory outside `results`, with mode-0600 files. The public result
tree retains fixed failure classes, not raw stderr or private diagnostic paths;
a `.private.log` filename alone is not an artifact-publication boundary.
Only the latest capture's final 64 KiB of stderr is retained, independently of
the complete, untruncated JSON row oracle. This private custody lasts until the
next ownership-checked load-generator scratch reset; preserve a needed failure
privately before starting another run. Its bytes remain in staging accounting,
without exporting its path.

New stress plans may bind a complete `generator` declaration containing
`warmupSeconds`, `measureSeconds`, and `captureSettleSeconds`. CLI warmup and
capture-settle values must match; an omitted CLI measure uses the declared
measure, while an explicit mismatch fails before reset. This declaration is
part of the immutable plan, not a revision of an earlier run's evidence.
`stress-input-budget.json` records the full warmup-plus-measure lifetime and
separate startup, window, and final-sample budgets. A long declared lifetime
never substitutes for the actual remaining-input checks before window start.

Provenance records sizes and SHA-256 identities for the selected host loadgen,
SDK generator and reader, plus the two mounted helper copies. Mounted copies
must match the selected host artifacts; the identities are checked again after
stack startup. Helper filesystem paths are not exported. These hashes bind
actual artifacts, not a claim that a build was fresh or source-correct; cold
source-specific build and source pre/post receipts remain required.

Physical accounting includes all files under the declared source, tenant,
checkpoint, WAL, router-state, and generator-staging roots: JSON history,
checkpoints, uncommitted/orphan Parquet, and staging are not omitted. Logical
file bytes and allocated blocks are separate; hard links are deduplicated.
SQL/Kusto volumes and Docker layers are **not** included, and collector RSS
covers only the two source processes. There is no whole-system physical plateau
claim: checkpoint appId growth and retained JSON can legitimately fail the
declared stability limits. Seven-day VACUUM may be a measured no-op on fresh
data, not reclamation evidence. PURGE, short-retention/prune-log, tenant replay,
and their post-maintenance exact oracle gates remain separate pending scenarios.

```bash
npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

This starts a fresh SQL Server, Hive, router, Direct collector, Dataflow
collector, Kusto emulator, [kusto-explorer](../kusto-explorer/README.md) query
UI, and monitoring stack, then sends the same Arrow batches to both collectors
across all 13 tables. Kusto provides live functional inspection of committed
output. Native Delta external-table/QAP validation is the default target;
the compatibility bridge is optional and is never Azure QAP evidence.

**A start replaces only a previously sealed, validated local attempt.** Its
corpus and evidence must be verified in the private per-attempt archive before
SQL, Hive and Kusto volumes or ephemeral tables/results can be deleted.
Failed, interrupted, nonvalidated, Azure or unmarked legacy attempts block
reset and retain their original data. Other Docker projects and workspace
files are left alone.

The test runs until **Ctrl+C**. Stopping the generator leaves the stack running
so you can inspect the charts and output; that unsealed attempt blocks the next
start rather than allowing its evidence to be silently reset.

## Fenced Azure schema bootstrap

Infrastructure provisioning invokes:

```bash
cargo run --locked --quiet --package arrowcol-loadgen -- initialize-azure \
  --manifest "$ARROWCOL_AZURE_TABLE_MANIFEST"
```

Private environment inputs are `ARROWCOL_AZURE_TABLE_ROOT`,
`AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_CONTAINER`, and
`AZURE_STORAGE_CHECKPOINT_CONTAINER`. The root must be a credential-free
`abfss://` URL inside that account/container. The storage authority must remain
owned and **fenced** during initialization; this command cannot acquire a data
writer or bypass the reset fence.

The canonical registry initializes both pools/all 13 schemas with required
`__arrowcol_ingestion STRING` partitions. Core normalization runs exactly once,
including timestamp companions. Existing compatible roots are preserved.
Only after all 26 roots succeed is the `{name, relative_path}` JSON manifest
written; paths such as `direct/telemetry/sqlserver/sqlserver_active_sessions`
are relative to `ARROWCOL_AZURE_TABLE_ROOT`, including any private isolated-run
prefix. Initialization does not insert data or prove Azure/QAP readiness.

## Options

| Option | Meaning | Default |
| --- | --- | --- |
| `--mode` | Select `local` disk/emulator or `azure` ADLS/Azure Kusto together | `ARROWCOL_MODE`, otherwise `local` |
| `--warmup` | Warmup before measurement | `30s` |
| `--concurrency` | Concurrent sending lanes | `4` |
| `--batch-rows` | Rows per Arrow batch | `8192` |
| `--measure` | Stop after a finite measurement, such as `5m` | Until Ctrl+C |
| `--validate` | Check Delta snapshots and native Kusto data after the generator finishes | Off |
| `--final-compaction` / `--no-final-compaction` | Enable or explicitly opt out of post-release local compaction; explicit Azure enablement fails before startup | On for validated local runs; unchanged/off for Azure |
| `--quiver` | Both collectors persist every batch to [Quiver](#disk-persistence-with-quiver) on local disk before acknowledging it, then publish to Delta from disk | Off |
| `--crash-after` | With `--quiver`, `kill -9` both collectors this long after they start persisting, restart them, and require replay from disk | Off |

Azure mode reads private `.env`, then `.env.azure`, with explicit inherited
environment taking precedence. It requires the configured owned ADLS root,
account, data/checkpoint containers, Kusto URI and database; it never falls back
to another subscription or to the emulator. Each run uses isolated roots below
the configured private prefix. The runner acquires occupancy before cloud work
and stops its owned writers before releasing it. Missing authorization is a
failure, not successful skipped qualification. Real Azure QAP qualification is
separate from local emulator functionality and remains unverified until a live
authorized run produces the required observations.

Azure collector images must include the shared pinned CLI installer from
`infra/docker/install-azure-cli.sh`. At runtime the runner makes a separate
writable Azure CLI config copy for each collector, loadgen, infra helper, tenant
stream and maintenance service, below the ignored
`onelake/benchmark/latest/azure-cli/` directory. Directories are mode `0700`,
files `0600`; collectors use the invoking UID/GID. The host cache is never
mounted into a collector, and config/credentials never enter an image or its
build context. Copying config is not proof of valid cloud authorization:
token refresh and owned storage access must still succeed.

To stop only this workspace's loadtest without deleting its data:

```bash
npx --no-install tsx projects/loadtest/src/main.ts --stop
```

Successful stop emits one JSON receipt with `stopped:true`,
`scope:"workspace-owned-loadtest"` and `dataPreserved:true`. This local stop is
not a distributed fencing guarantee: Azure reset must also enforce the owned
checkpoint-container metadata fence and occupancy CAS.

### Qualification evidence

Correct query counts alone do not prove an accelerated index is ready.
Qualification must record native QAP cache state/statistics and compare both
cached and deliberately fresh fallback queries with the complete independent
Delta row multiset. `MaxAge` limits acceptable index staleness; it is not a
refresh-cadence guarantee. OPTIMIZE rewrites require reacceleration, which is not
by itself proof of a whole-table reset.
See the public [query acceleration policy](https://learn.microsoft.com/en-us/kusto/management/query-acceleration-policy)
and [troubleshooting guide](https://learn.microsoft.com/en-us/kusto/management/query-acceleration-troubleshooting-guide)
for supported table features, cache limits and unhealthy-catalog behavior.

For isolated Azure runs, the runner writes the same canonical 26-table manifest
without mutating storage (`arrowcol-loadgen azure-manifest --manifest PATH`),
then reconciles external-table bindings while generation is active. The
infrastructure helper has a fixed 120-second absent-log wait and distinguishes
not-ready logs from authentication, ownership or malformed-log failures; there
is no general retry-until-green loop. Occupancy exports the actual ready
`ARROWCOL_RESET_GENERATION`, and the runner reloads that private environment
before starting either collector. A run UUID is not a storage generation.
Cloud cleanup stops all owned services, permits a bounded five-minute drain,
and refuses occupancy release after a forced, OOM or unverifiable mutator exit.
This exit check is not proof of registry closure: the infrastructure release
must independently require an empty same-generation publisher registry. Unknown
drains retain occupancy and registrations; no force-clear is attempted.
The same captured post-occupancy environment is used for both benchmark and
release, even if a private env file changes later. Reconciliation owns a Linux
process group, so its bounded deadline/interrupt also drains Azure CLI wrapper
descendants. Any failed reconciliation retains occupancy for authoritative
reconciliation; process exit alone is not proof of mutation completion.

The optional `tenant` and `maintenance` Compose profiles use the real
`arrow-tenant-stream` and `delta-maintenance` images, both included in
`loadtest:build` (and therefore the E2E image closure). Both collectors and both
services use `init: true` to reap CLI descendants. Tenant sources are read-only;
its writable sink and checkpoint use separate directories. Azure replaces
source, sink, checkpoint and maintenance roots and mounts a separate private
CLI config for each service. Profiles are not started by ordinary baseline runs.

Local maintenance sees disposable loadtest roots under `/tmp/arrowcol-stress`.
This mount alone does **not** authorize short-retention VACUUM: the table must
also have the core's explicit disposable-test marker and the invocation must
opt in. The profile defaults to real OPTIMIZE only. Normal and Azure VACUUM keep
the seven-day retention floor; a fresh cloud run without aged tombstones cannot
claim physical-cleanup or steady-state evidence. Source layout qualification
sets `ARROWCOL_LAYOUT_ROW_GROUP_ROWS=512` and `ARROWCOL_LAYOUT_PAGE_ROWS=128`
on the actual collectors; ordinary defaults remain 16384/2048.

The fixed-window stress evaluator requires both operation cycles and export
samples at the same declared maintenance-wave phase. It rejects missing
telemetry, counter resets/jumps, driver stalls, unexpected restarts/OOM,
exhausted retries, resource ceilings and unstable trend/variation. Independent
operation evidence must show net file reduction with unchanged row multisets
for OPTIMIZE, and actual physical reclamation without removing active files for
VACUUM. These evaluators do not turn an unexecuted or timed-out scenario into a
pass; local component tests do not establish Azure QAP behavior.

Both collectors use two CPUs and matching buffer limits: 128 MiB overall,
32 MiB per table, and 32 MiB per flush. SQL Server defaults to a 16 GiB memory
budget. The Kusto container has a separate 4 GiB memory limit. Keep host memory
available for the other services.

To change the SQL budget (in MiB):

```bash
SQLSERVER_MEMORY_LIMIT_MB=8192 npx nx run loadtest:run -- --warmup 2m --concurrency 8 --batch-rows 1024
```

## Disk persistence with Quiver

```bash
npx nx run loadtest:run -- --quiver --measure 2m --crash-after 30s --validate
```

`--quiver` puts [Quiver](https://github.com/mdrakiburrahman/otel-arrow/tree/main/rust/otap-dataflow/crates/quiver),
the OpenTelemetry Arrow write-ahead log and segment store, in front of Delta in
both collectors. A batch is written and fsynced to disk **before** the client
gets its acknowledgement, and Delta publication reads it back from disk:

- **Direct** persists each batch with its receipt identity under
  `direct-state-1/quiver/`. Its receipt is still the delivery contract. The
  Quiver copy is released only after the receipt reaches `published`.
- **Dataflow** routes batches through the DFE `durable_buffer` processor under
  `dataflow-state-1/quiver/core_N/`. The Flight stream completes with
  `durable_admission`, replacing `best_effort`, only after every batch is on
  disk. Dataflow still has no SQL receipts.

To look inside these directories, see [Quiver on disk](../../docs/quiver/README.md#8-the-benchmarks-directories).

An fsync per acknowledgement costs throughput; that is the trade-off on show.
Watch the **Quiver** panels in Grafana for disk usage, backlog, and persist and
drain rates.

`--crash-after` sends `SIGKILL` to both collectors mid-load, so no drain or
flush runs. It then restarts them on the same disk.

- The restarted Direct process is a new generation. It adopts its predecessor's
  receipts and publishes the persisted batches under their original receipt IDs.
- Dataflow replays its write-ahead log and rebinds the replayed batches to its
  new table leases.

The load generator keeps retrying through the outage for up to five minutes.
`results/quiver-recovery.json` records the outage, the backlog on disk at the
moment of the kill, and the replay counts. The run fails unless both collectors
replay batches from disk.

With `--quiver`, `--validate` requires **every acknowledged row** in Delta for
both pools. Without it, Dataflow admission is best effort, so validation only
requires a published batch per Dataflow table. Independent source-client submissions may duplicate under the at-least-once
request contract. Replaying the **same publication identity** must instead be
idempotent. Exact snapshot row multisets and stable publication IDs, rather than
ACK counts alone, are the correctness evidence.

## Per-attempt evidence custody

`reader-build` waits for the existing loadtest lint and test targets before
installing the static helpers. This also applies to standalone reader/build
requests and the shared Rust build target: Cargo tests must finish writing
their normal binaries before static installation and E2E helper staging.
It also waits for the existing SDK generator lint, build, test, compatibility
and package-artifact targets. Their generation/check/package chain invokes
`cargo run` against the same host generator binary, so standalone reader/build
requests now include that SDK generation and generated-package validation cost.

A successful **validated local** attempt now stops its owned router only after
the actual Grafana, native validation and Explorer gates have finished, all
generator/helpers have returned, observer/sampler shutdown and diagnostics have
completed, and the original source/maintenance/tenant mutators are reverified
as cleanly stopped. The router is not kept live after qualified success.
Other live writable mounts into the corpus are refused; read-only Kusto/bridge
source mounts are permitted. No measurement window, query or evidence threshold
changes.

Before that attempt can be reset, its corpus (including tenant/checkpoint/WAL
state), results/receipts/Grafana evidence, source/lockfile provenance, seven
source-bound image identities and three selected helper executables are copied
and content-verified under the workspace's private
`.temp/loadtest-attempt-evidence/<attempt-uuid>/`. This path is outside **all**
benchmark reset directories. Only the exact run's reader stderr diagnostic
files are included from loadgen scratch; secret scratch, environment and
credential files are never archived. The archive is private (0700 directories,
0600 files), contains potentially sensitive diagnostics and must **not** be
committed or uploaded wholesale as a public artifact.

The seven local image targets now pass `VCS_REF={gitSha}` and require a clean
Git worktree, matching the existing publication metadata policy. Tags, Dockerfile
contents, build contexts and runtime behavior are unchanged. Archived OCI
revision labels and inspected image/container IDs identify those builds, not a
cryptographic Docker-context attestation. Git cleanliness does not cover ignored
files that Docker includes or prove absence of source mutation during a build.
The existing `target/images/*-load.txt` files are tag lists, not full build
receipts or context digests.

The bounded copy uses 1 MiB stream buffers, at most 100,000 inventory entries,
16 GiB of files, a 256 MiB manifest and 300 seconds of post-attempt work. Files
and directories are synchronized before an exclusive atomic seal is published.
At seal time every selected original and helper identity is checked before and
after copying. Before reset, all private archived bytes and permissions are
reverified, and the original corpus must still be unchanged. Rebuilding an
external helper or changing an external lockfile/diagnostic does not invalidate
its already sealed copy; those archived bytes remain immutable.
Symlinks and special files are refused, except for metadata about the one
configured, stopped router upgrade socket: it is neither opened, copied nor
deleted and is **not restorable**. Kusto's named volume/native cache is **not**
exported or claimed to be a consistent database image; the preserved native
evidence consists of the original validation/export receipts and source corpus.

Failures, interrupted starts, nonvalidated or Azure attempts remain **unsealed**:
their original roots are retained and the next reset refuses before any process
signal, Docker mutation or deletion. There is no automatic failure archive or
unsafe retry. A missing, partial, altered or stale seal likewise blocks reset;
the original attempt and any private partial archive remain for diagnosis.
Existing legacy roots without an attempt marker are never adopted or silently
deleted. Consecutive required Nx E2Es therefore preserve the first qualified
attempt synchronously before the second can reset its ephemeral runtime.

## Watch and inspect

- **Grafana:** <http://localhost:3000/d/arrowcol> (`admin` / `admin`).
- **SQL Server:** `localhost,14333`, username `sa`. The local password is printed
  at startup and stored in `projects/arrowcol-infra/secrets/sqlserver-sa-password.txt`.
- **Portainer:** <https://localhost:9444>, username `admin`, the SQL Server password.
  The local certificate is self-signed.
- **Kusto:** <http://localhost:8080>, database `loadtest`, no authentication.
  This is an HTTP query endpoint, not a browser UI.
- **kusto-explorer:** <http://localhost:8090>, a browser KQL workbench with the
  `loadtest` database preselected. Its server reaches Kusto at
  `http://kusto:8080` over the Compose network.
- **Logs:** `docker logs -f rust-playground-loadtest-latest-dataflow-1-1`
  (use `direct-1-1` for the Direct collector).

Use `SQLSERVER_PORT`, `GRAFANA_PORT`, `PORTAINER_PORT`, `KUSTO_PORT`, or
`KUSTO_EXPLORER_PORT` to override host ports. Published ports bind to host
loopback only. Inside the devcontainer,
the runner uses the emulator's allocated `10.89.x.50:8080` address instead of
`localhost`; the host connection remains `http://localhost:${KUSTO_PORT:-8080}`.
Do not share startup logs containing local-development passwords.

Output is always under `onelake/benchmark/latest/`:

```text
direct/                   Direct Delta tables
dataflow/                 Dataflow Delta tables
results/                  Metrics, manifests, diagnostics, and validation output
direct-state-1/quiver/    Direct Quiver WAL and segments (--quiver)
dataflow-state-1/quiver/  Dataflow Quiver WAL and segments (--quiver)
```

The runner prints the absolute Delta paths at startup. It samples memory,
threads, readiness, and row counters while the generator is running.

## Live Kusto inspection

The emulator requires Linux/amd64 Docker, a CPU supporting SSE4.2/AVX2, and
sufficient host memory. Its first image download is over 1 GB compressed. The
Compose service pins an immutable digest of Microsoft's documented
`mcr.microsoft.com/azuredataexplorer/kustainer-linux:latest` image and leaves its
startup command unchanged. It configures `ACCEPT_EULA=Y`; running this target
therefore accepts the [Microsoft emulator license terms][emulator-install].
The emulator supports HTTP only, without Microsoft Entra authentication.

The database is `loadtest`. Each of the 13 SQL table names appears in both pools:
`direct_<sqlserver_table>` and `dataflow_<sqlserver_table>`, for 26 native Delta external
tables. For example:

```kusto
external_table('direct_sqlserver_wait_stats')
| take 5
```

Committed Parquet files become queryable incrementally while generation runs.
Tables may be pending until their first Delta commit; readiness of the cluster
does not establish cache eligibility. Native QAP uses `MaxAge=00:01:00`;
validation waits within its declared timeout for actual eligible cache statistics
and checks both the cached query and the one-second fallback against the oracle.
The native cached gate keeps a fixed five-minute deadline; the benchmark's
overall validation budget is eight minutes. The separate clean-mutator stop
budget remains five minutes and is not extended by cache convergence.
The optional bridge is not the default and is not accelerated-cache evidence.
Timestamp `__unix_nanos` companion columns retain the original
integer nanoseconds; Kusto `datetime` alone is not nanosecond-exact.

To inspect an external table from the Docker host:

```bash
curl --fail-with-body --silent --show-error \
  "http://localhost:${KUSTO_PORT:-8080}/v2/rest/query" \
  -H 'Content-Type: application/json' \
  -d '{"db":"loadtest","csl":"external_table(\"direct_sqlserver_wait_stats\") | count"}'
```

For an interactive view, open kusto-explorer at <http://localhost:8090>. It lists
every table in its connections tree; run the same query there with **F5** and
chart it with `| render columnchart`. The browser talks only to kusto-explorer,
whose server queries Kusto through the Compose service name. No Azure
subscription or cloud credentials are required.

### Failures, validation, and retained data

Kusto starts and is probed separately from the mandatory SQL/Hive/collector
startup. The readiness probe is `POST /v1/rest/mgmt` with
`{"csl":".show cluster"}`, without requiring the database to exist yet.
An image, startup, readiness, or synchronization failure does **not** stop
ordinary collector load generation. The runner prints an explicit
`Kusto DEGRADED` outcome rather than claiming the full integration succeeded;
an ordinary run may still exit successfully.

`--validate` and `loadtest:e2e` require real post-run KQL correctness assertions
for all 26 tables and an in-run live-query observation. They fail if Kusto is
unavailable, incomplete, inconsistent, or unverifiable. Counts are compared with
each pool's captured Delta snapshot, not with acknowledgements or an assumption
that the two pools have equal physical row multiplicities.

They also require the kusto-explorer UI gate. After the KQL assertions pass, the
runner starts a Playwright browser container (Compose profile `ui-gate`) on the
stack's network and drives `http://kusto-explorer:8080` like a user. The gate
checks the configured cluster is `http://kusto:8080`, the connections tree lists
all 26 tables, and the per-table row counts exported from the UI match
`kusto-validation.json`. It also runs a management command and a column chart,
and fails on any browser console error. In ordinary runs, a kusto-explorer
startup failure is only a warning.

Validation first drains publication and collects live Grafana health/plot
evidence while mutators remain running. It then cleanly stops **all** source,
maintenance and tenant mutators and records run-bound drain receipts. Validated
local runs then execute `publisher-release`, followed by `final-compaction`,
before capturing the final immutable Delta versions and running the unchanged
cached/fallback correctness gate. `--no-final-compaction` passes the explicit
`--skip-final-compaction` request to the helper; skipped evidence is not a
successful compaction. The helper owns the compaction plan, bounded full-row
before/after comparison, final inventory and cooperative 300-second phase budget.
The runner's existing 600-second hard process backstop is unchanged; neither
deadline promises preemption of synchronous work or a guaranteed diagnostics reserve.

`publisher-release.json` is actual post-exit SQL evidence, not a local publisher
registry or reader-lease claim. The run's `routing.json` now includes its run ID.
Release requires exactly one same-run publisher ID/key in each pool, each
durably stopped in `routing_collectors`, with no other ready/draining generation,
even if its heartbeat expired. Historical stopped/failed rows are allowed.
Routing leases have only active/draining states and no clean-stop release path:
they may remain unexpired, but every holder must be a same-run stopped publisher.
Expiry is recorded as evidence, not used as release proof. The direct publisher
must additionally have a matching `collector_instances` receipt-owner row with
state stopped and a nonnull stop timestamp, zero unreleased receipt table leases,
and zero accepted/buffered/flushing receipts. Historical outstanding receipts
also fail admission. Dataflow has no receipt-owner requirement: its proof is
routing stopped plus owned clean exit zero. Missing required rows fail closed.

`publisher-admission.json` binds that SQL report to the stack-start SQL container
ID/start time/image, same-run root, SDK binary digest and exact cleanly stopped
writer containers. Ownership and generations are re-inspected around the SQL
read. SQL release is never attempted before clean writer exit, and a release
failure prevents final compaction and validation. Azure does not enter these
local-only phases by default; `--mode azure --final-compaction` is explicitly
unsupported and fails before effects.
The read-only validator, Kusto and Explorer remain available; their lifetime
does not authorize occupancy release. Ordinary non-validation runs retain the
live stack. Stopped-mutator receipts are not distributed registry-close proof.

The baseline Grafana gate is explicitly scoped to `source-sdk`: every one of
the seven Kernel/publication families must have actual samples from **each**
collector's OTLP resource, plus healthy live scrapes and individual source
counter/byte/duration/value/version panel screenshots paired with their exact datasource
queries. A baseline pass is not full service telemetry qualification.
All fourteen Delta operations time-series panels set Grafana's panel query
minimum interval to `2s`, matching the OTLP metric export cadence and the gate's
direct range queries. Without this override the LGTM datasource's `60s` default
can undersample short-lived maintenance series in the actual rendered plot.
The instant inventory table is unchanged; every returned time-series frame must
still contain at least three actual numeric samples in the same 300-second window.
`service-qualification` additionally requires real maintenance removals and
physical deletion, tenant skipping/checkpoint metrics and native QAP per pool;
bridge metrics are required only when enabled. Measured zero backlog/retries are
valid, but missing metrics never become manufactured zeroes.

Validation runs a read-only native QAP observer every ten seconds, with one
30-second authentication/query deadline and a separate 40-second owned-process
budget. At most two tables are observed concurrently; policy, statistics and
details remain serial within each table, and capacity is requested once after
all tables. Both lanes are polled by one parent task so response-artifact appends
cannot interleave; expiration drops all local request futures, but does not
prove immediate server-side query cancellation. This is a concurrency bound,
not a requests-per-second guarantee. Sanitized all-26-table observations and export receipts are retained in
`results/qap-observations/` and `results/qap-observer.jsonl`. It sends per-pool
completion, pending files, cached bytes, unhealthy catalogs and last-update age
to LGTM through OTLP. Missing measurements suppress their aggregate, not become
zeroes; qualification filters incomplete observations and observations older
than 45 seconds. These are state/lag observations, **not correctness or reset
proof**. Azure observation uses a seventh isolated writable private CLI cache.
The observer finishes before cleanup/release; observation/export failure fails
qualification and conservatively retains Azure occupancy for inspection.
Each helper attempt also writes `observation-diagnostic.json` with a fixed
`completed`, `deadline_exceeded` or `helper_failed` code, elapsed milliseconds
and completed-response count (including HTTP failures, excluding unfinished
requests). Failed observer receipts identify the phase and sequence and include
checked numeric child exit/signal and timeout outcomes when available; missing
or invalid diagnostics remain explicit unknowns. These diagnostic fields never
copy stderr, endpoints, queries or table labels. A supervisor backstop timeout
is separate from a reported child timeout. No diagnostic receipt retroactively
classifies failures from older helpers that discarded their error output.

The optional maintenance profile also requests checkpoints every 100 commits
(`LOADTEST_CHECKPOINT_EVERY_COMMITS` overrides this). Checkpoints bound replay
work, **not physical history**: they do not delete commit JSON or old
checkpoints. Until a separately verified retention/pruning implementation and
real fixed-window whole-tree measurements exist, no physical-size plateau or
production steady-state qualification is claimed.

`--stop` binds its stdout receipt's `resetGeneration` to the canonical lowercase
`ARROWCOL_RESET_GENERATION` supplied by its caller, without rereading private
environment files. Without a generation, ordinary local stop retains its
original receipt and cannot satisfy an infra reset request. A stop receipt is
not distributed registry-drain proof and does not enable reset.

Run the focused verification suite with:

```bash
npx nx run loadtest:verify
```

Its functional E2E uses 1,024-row batches while retaining all 13 schemas and all
26 external-table assertions. It runs twice in sequence: the baseline, then
`--quiver --crash-after 15s`. The second run resets the first only after its
validated corpus and evidence are privately sealed. The ephemeral `latest`
results are from the Quiver crash run; both successful attempts have separate
private archives. Ordinary `loadtest:run` workload defaults are unchanged.

Inspect these files under `onelake/benchmark/latest/results/`:

| Artifact | Meaning |
| --- | --- |
| `kusto-startup.json` | Independent, sanitized emulator start/probe outcome |
| `kusto-status.json` | Rust synchronization state, live visibility, file count, and any error |
| `kusto-queries.jsonl` | Executed KQL and query results, including live observations |
| `kusto-validation.json` | Final cached/fallback external-table correctness and QAP observations |
| `kusto-explorer-validation.json` | UI gate checks, screenshots, and any browser console errors |
| `kusto-explorer-export.json` | Per-table row counts exported through the UI |
| `kusto-explorer-*.png` | UI screenshots in the dark and light themes, or `kusto-explorer-failure.png` |
| `provenance.json` | Image identities and sanitized effective resource configuration |
| `quiver-recovery.json` | `--crash-after` outage, on-disk backlog at the kill, and replay counts per collector |

Startup diagnostics remain separate if synchronization subsequently recovers.
Missing or malformed final synchronization status is degraded, never assumed
healthy. Save evidence outside `onelake/benchmark/` before another run resets it.

The running emulator and native data remain available after finite runs and
Ctrl+C. Its owned `kusto-data` volume is mounted at `/kustodata`; the persistent
database uses `/kustodata/dbs/loadtest/{md,data}`. Only the Direct and Dataflow
output roots are exposed at `/delta/direct` and `/delta/dataflow`, both read-only.
The next loadtest start removes the owned volume only after checking its project,
name, workspace identity and the previous attempt's verified artifact seal;
it does not clean up other Docker projects. The seal is not a native volume backup.

Override the emulator memory limit and published port when needed:

```bash
KUSTO_MEMORY_LIMIT=4g KUSTO_PORT=18080 npx nx run loadtest:run -- --measure 5m --validate
```

### Functional inspection only

[Microsoft's emulator license prohibits benchmark tests][emulator-overview].
Do not measure or publish Kusto emulator throughput or latency; it is not a
production Azure Data Explorer performance proxy. This integration exercises
native local-file Delta/QAP query functionality only, not cloud performance.
Cache work consumes shared host resources, so collector
measurements with this stack must not be described as unaffected by Kusto.
Its configured resource limits are retained in provenance.

[emulator-install]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto-emulator-install
[emulator-overview]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto-emulator-overview
