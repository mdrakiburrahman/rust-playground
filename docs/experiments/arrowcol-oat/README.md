# ArrowCol OAT benchmark results

| File | Purpose |
| --- | --- |
| `index.html` | Static, Excel-like heatmap viewer using Hyparquet in the browser, with a live numbered architecture diagram and config-to-stack mapping. |
| `architecture.svg` | Full-width layered architecture displayed on the live page, with embedded official icons and numbered parts 1–6. |
| `results.parquet` | Sanitized, compact benchmark summary published by the OAT server. |
| `oat-config.example.json` | Validated configuration template with no credentials. |
| `architecture.drawio` / `architecture.drawio.png` | Editable source and rendered companion for the same layered architecture. |
| [Runtime setup](RUNTIME.md) | Isolated internal-only Kubernetes stack, image digests, workload identity, SQL bootstrap, secret files and local rehearsal. |
| [E1-E5 input preparation](EXPERIMENTS.md) | Offline binding of private AKS settings and reviewed pilot windows into five comparable 32-table runs. |

The viewer's six numbered stack parts match the diagram badges. Their numbers appear beside every OAT JSON field and Parquet result column that they affect. Preserve these IDs and titles when updating the diagram, configuration reference, or heatmap.

## Parquet contract

Schema version 2 keeps every existing column and adds run/platform and pinning metadata. `started_at` is an Arrow `timestamp[ms, tz=UTC]`; nullable fields are written as null when unavailable, never as synthetic zeroes.

The failure-evidence amendment makes `rows`, `duration_ms` and
`rows_per_second` nullable without changing their names, order or logical types.
Uncompleted measurements are null; completed measurements and historical zero
values remain values, never retroactively inferred as missing. The producer
accepts the exact prior 31-column v2 schema (including field/schema metadata)
only by widening these three fields from required to nullable during append.
All other schema differences are rejected; the output retains `schema_version: 2`.
This is an explicit nullability amendment, not the unchanged original strict
schema. Readers must accept null measurements, including failed-cell records.

Completed earlier matrix cells are published even when a later cell or cleanup
fails, and the overall run still fails. Failed cells retain their run/table
identity and fixed failure categories in `tunables_json`; incomplete cell
measurements remain null. Cleanup failures after completed measurement retain
those values and set `cleanup_failed: true`. Original diagnostic errors remain
in the overall failed-run error chain/receipt, not the public Parquet mirror:
arbitrary command output must not be published as sanitized benchmark data.
`status` is `succeeded`, `failed` or `pinning_violation`. A failed ownership
observation is unverified (`failed`, `pinning_ok: false`), not proof of an
ownership violation; only observed invariant loss or an explicit router
pinning rejection warrants `pinning_violation`. The viewer labels these states
separately and continues displaying null metrics as a dash, not zero.

| Column | Type | Nullability | Meaning |
| --- | --- | --- | --- |
| `schema_version` | 32-bit signed integer | Required | Contract version; currently `2`. |
| `mode` | UTF-8 string | Required | `local` or `aks`. |
| `stamp` | UTF-8 string | Nullable | Deployment stamp, when available. |
| `host` | UTF-8 string | Required | Host running the OAT workload. |
| `experiment` | UTF-8 string | Required | Named tunable combination. |
| `started_at` | Timestamp (milliseconds, UTC) | Required | Experiment start time. |
| `measure_seconds` | 64-bit float | Required | Steady-state measurement window. |
| `arrowcols` | 32-bit signed integer | Required | ArrowCol replica count. |
| `clients` | 32-bit signed integer | Required | Client fleet size. |
| `tables` | 32-bit signed integer | Required | Number of selected tables. |
| `table_owner` | UTF-8 string | Nullable | Stable ArrowCol owner for this table. |
| `pinning_ok` | Boolean | Required | True only when the owner remains valid and unchanged for the full experiment. |
| `status` | UTF-8 string | Required | Result status; pinning violations use `pinning_violation`. |
| `git_sha` | UTF-8 string | Required | Owned-server build revision from a clean, unchanged Git checkout; `unknown` for uncommitted source. External servers retain their own provenance. |
| `tunables_json` | UTF-8 string | Required | JSON object containing the experiment tunables. |

The producer retains these original result columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | UTF-8 string | Benchmark run identifier. |
| `replicas` | 64-bit unsigned integer | ArrowCol replica count. |
| `dfe_worker_concurrency` | 64-bit unsigned integer | DFE `--num-cores` setting. |
| `table` | UTF-8 string | DMV table used as the table-scoped routing key. |
| `rows` | Nullable 64-bit unsigned integer | Rows sent by the standalone DMV client; null if no complete measurement. |
| `duration_ms` | Nullable 64-bit unsigned integer | Shared fleet measurement duration; excludes preflight and warm-up; null if unmeasured. |
| `rows_per_second` | Nullable 64-bit float | Total table rows completed during the shared window divided by its duration; null if unmeasured. |
| `cpu_millicores` | Nullable 64-bit float | Unique collector/DFE pod CPU from `kubectl top`; integrated pods are counted once. |
| `memory_bytes` | Nullable 64-bit unsigned integer | Unique collector/DFE pod memory from `kubectl top`; integrated pods are counted once. |
| `qap_observation_complete` | 64-bit unsigned integer | 1 only when all expected QAP tables and required fields were measured; otherwise 0. |
| `qap_observed_tables` | 64-bit unsigned integer | Number of expected QAP tables observed. |
| `qap_completion_percent` | Nullable 64-bit float | Minimum reported completion percentage across the selected tables. |
| `qap_pending_artifacts` | Nullable 64-bit unsigned integer | Sum of pending artifacts across selected tables. |
| `qap_cached_bytes` | Nullable 64-bit unsigned integer | Sum of QAP `HotSize` bytes across selected tables. |
| `qap_unhealthy_catalogs` | Nullable 64-bit unsigned integer | Sum of unhealthy catalog counts across selected tables. |
| `qap_lag_seconds` | Nullable 64-bit float | Maximum reported QAP lag across selected tables. |

The viewer reads `results.parquet` from the same origin using HTTP range requests; it does not need CORS configuration. Azure Storage static website hosting on the HNS-enabled account serves `$web` anonymously, even when account-level blob public access is disabled. The AKS defaults target `abfss://experiments@arrowcollector<stamp>.dfs.core.windows.net/arrowcol-oat/results.parquet` and the root `results.parquet` in that stamp's `$web`; explicit destinations may override them. `aks setup` publishes the checked-in viewer to `$web/index.html` through the federated `arrow-collector` identity so the static-site root can load the same-origin results. The producer uses conditional ETag updates, deduplicates every write by `(run_id, table)` (last row wins), and verifies the website copy matches the authoritative results object before success. Local writers serialize read/merge/rename through a cross-process file lock with a 15-second timeout. Empty results files preserve the full schema and accept later appends. Publish only approved nonsensitive summary metrics there—never credentials, raw payloads, or sensitive identifiers.

## Running the OAT matrix

Complete [runtime setup](RUNTIME.md) first for AKS runs. The OAT executable starts its local Rust API server automatically; Rust/Cargo and the npm workspace dependencies must be available. The supported AKS topology uses **one integrated `arrowcol` Deployment**, container `dataflow`, for both receiver and exporter. Set both deployment/container/selector pairs identically. The server applies replicas and `--num-cores` together, observes and restores each unique Deployment once, and counts each pod once. It writes one compact Parquet file to both configured ADLS Gen2 destinations. The web destination must be the root of `$web` (for example, `abfss://$web@account.dfs.core.windows.net/results.parquet`).

Run the local OAT from the repository root with `npx arrowcol-oat --config docs/experiments/arrowcol-oat/oat-config.example.json`. For bridged Docker hosts, isolated smoke settings and opt-in stopped Delta exports, see [local runtime and snapshots](RUNTIME.md#local-smoke-and-stopped-delta-snapshots). The executable starts the Rust API server, builds the local Compose images, creates run-scoped local credentials and state, submits the configuration, polls the run, then stops the stack and API server. It then serves the local results page on loopback with HTTP range support and keeps the command open until Ctrl+C. Compose assigns ephemeral host ports for the local router and Kusto; the sample endpoint values are placeholders. Rust/Cargo, Docker Compose, and npm workspace dependencies must be available. The included sample writes both result files under `/tmp`; set ADLS Gen2 URLs when publishing results to storage. Add `--server <URL>` to use an already-running OAT API server instead.

### CLI options and local cleanup

Run `npx --no-install arrowcol-oat --help` for the supported options. Unknown options fail before startup with exit code 2. Failed or rejected runs exit without opening the viewer; use `--viewer-only` to inspect any previously published results, including partial failure evidence. If the local ownership marker cannot be written (for example, disk full), the CLI cancels the accepted run and waits for cleanup before reporting the write failure. In local mode, SIGINT, SIGTERM and SIGHUP request cancellation and wait for the local Compose cleanup; a second signal forces a fast exit. If only the CLI process is killed, the next local run checks its ownership marker and asks the still-running OAT server to cancel and clean up.

If the OAT server is no longer running, remove one abandoned local cell with `npx --no-install arrowcol-oat --cleanup-local <cell-run-uuid> --config <config-path>`. Get the cell UUID from its directory under `platform.compose_state_root`; relative state roots resolve from the repository root, matching the owned Rust server, even when invoking the CLI from another directory. This command is local-only and removes only that cell's Compose resources and state; it does not operate on AKS.

For a functional demo, use `oat-config.quick.json`: one table, one replica/worker cell, no warm-up and a one-second measurement, with results under `/tmp/arrowcol-oat-quick`; builds, stack startup and the 60-second schedule lead still apply, and bridged devcontainers require the local host settings above. After a successful run, reopen the results without rerunning the benchmark with `npx --no-install arrowcol-oat --mode local --viewer-only`. This serves the checked-in quick configuration's existing `website_url` results file on loopback without starting Rust, Docker Compose, a benchmark run, or Azure operations; it reports an error if the configured result file is missing.

AKS runs require a working Kubernetes context and metrics API, `dmv_client_image`, a namespace secret named by `edge_secret_name` with an `edge-hmac` key, and the **real provisioned QAP endpoint**. An optional `platform.kustomize_overlay` points to a local Kustomize directory for pod sizing; the rendered overlay is restricted to the configured ArrowCol/DFE Deployments, which the runner snapshots and restores. Each client is a Kubernetes Job pinned to `arrow-collector/role=client` with its matching NoSchedule toleration; Jobs use topology spread across `kubernetes.azure.com/agentpool`. ArrowCol, DFE, router and catalog runtime pods select the `arrowcol` role pool and tolerate its matching taint. AKS requires a five-digit `stamp`; if result destinations are omitted, the producer uses that stamp's `experiments` container and `$web`. Local runs use the Compose stack and local client/ArrowCol/Kusto endpoints; only the configured Parquet results sink may use Azure. Set local `qap_database` to `null` to use Kustainer's default `loadtest` database. Each DDL/upload is HMAC-signed; each concurrent upload uses a separate connection so SO_REUSEPORT can distribute work across DFE cores. Client timing reports steady-window throughput rather than admission latency.

Each matrix cell initializes and binds its complete immutable SQL run-table pin set before launching clients, through the existing routing engine's collector refresh/ranking and SQL lease-CAS path. The finite setup process is authenticated as `arrowcol_migrator`, never with router/app credentials, and receives only the routing-selection policy projected from that platform's exact rendered router config. Local Compose uses its dedicated migrator password file; AKS mounts the existing `arrowcol-sql-migration` Secret only into finite SQL migration and pin-init Jobs at `/run/migration/password`, never into router, client, or collector pods. The runtime bootstrap and pin setup must use the same migrator password source.

Set `clients_per_table` from 1 to 4 to multiply the client fleet (maximum 128 client jobs/containers); each table remains a single routing target and can still be owned by one ArrowCol. `client_ready_timeout_seconds` bounds client image startup, DMV preflight, and whole-fleet readiness (30–1,800 seconds; default 600). Each client schedule wait is separately bounded to readiness + `qap_timeout_seconds` + 60 seconds for final schedule delivery; QAP preparation itself is bounded by `qap_timeout_seconds` (1–300 seconds). A QAP deadline failure is returned without retry; if a helper was interrupted, its outcome may be indeterminate and remains subject to the existing ownership-record rules. The parent permits at most 60 seconds from schedule arming to the common start, and fails if any client misses that window rather than accepting a partial fleet. AKS Job `activeDeadlineSeconds` covers the composed startup budget, warm-up, measurement, and a 300-second completion margin. Pin initialization is separately bounded to 180 seconds on either platform. Because immutable run-pin expiry starts before pin initialization, `run_pin_ttl_ms` must cover that setup bound plus the complete client Job lifetime; it is never extended to compensate for delayed preparation. The shared measurement timestamp is chosen only after every client is ready and QAP setup has completed. `dmv_concurrency` is bounded to 64 per client, `rows_per_table` to 1,000,000 per upload, and one matrix to 25 combinations. Set `pilot_mode: true` with a `measure_seconds` value that is a 30-second multiple from 60–600; warm-up plus measurement may not exceed ten minutes. The pilot emits per-table rows/s for each full 30-second window and reports the earliest cumulative horizon whose observed window rates vary by no more than 5%. Use those windows to select the shortest stable measurement duration for subsequent comparison runs.

Each matrix cell receives a canonical UUID and an isolated platform stack, run-scoped immutable table pins and router configuration. `run_pin_ttl_ms` defaults to 3,600,000 ms and is bounded to 86,400,000 ms; validation requires it to cover the 180-second pin-setup bound, configured readiness and QAP preparation, schedule delivery, workload, and Job cleanup budgets. The owner lease TTL is one hour so it cannot expire during a valid run.

For AKS, copy the example and set `platform.kind` to `aks`, remove the local Compose settings, and configure the AKS endpoints, secret path, context, namespace, and result destinations described above. Local configurations use the configured `compose_project` as a prefix and append the unique OAT run ID, isolating the network, containers, and volumes; `compose_state_root` contains generated local credentials and bind-mounted state for that run.

The server never creates, deletes, or scales AKS node pools. Run `cargo run --locked --package arrowcol-oat-rust -- --validate-config docs/experiments/arrowcol-oat/oat-config.example.json` before submission to validate the shape without changing cluster state; placeholders still need replacing. The `az://container/object` URL form is also accepted when Azure account authentication is already configured for the process.

## Mode-based quick bug bash

From the repository root, the local mode selects `oat-config.quick.json`, detects the Docker bridge hosts when needed, runs one quick matrix cell, publishes the local results, and keeps the loopback website open:

```bash
npx --no-install arrowcol-oat --mode local
```

Reopen the existing local results website without rebuilding Rust, starting Docker, or rerunning the benchmark:

```bash
npx --no-install arrowcol-oat --mode local --viewer-only
```

The viewer reports an error if the configured Parquet results file is missing. It serves only on loopback with HTTP range support.

AKS mode selects `oat-config.aks.quick.json`. Run its read-only preflight explicitly with:

```bash
npx --no-install arrowcol-oat --mode aks --preflight-only
```

A full AKS benchmark invocation is:

```bash
npx --no-install arrowcol-oat --mode aks
```

It repeats the preflight and refuses to submit unless every readiness gate passes. The AKS quick stack is **not staged or qualified**: required workload resources and immutable images are absent, enforcing network policy and behavioral proof are missing, current workload metrics are empty, and QAP is not qualified. The checked-in AKS config references runtime/bootstrap files not present in the checkout and leaves the database and client image unset; do not treat a preflight result as permission to provision resources. There is no AKS website-only mode.
