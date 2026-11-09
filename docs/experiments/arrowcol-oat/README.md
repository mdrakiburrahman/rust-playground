# ArrowCol OAT benchmark results

| File | Purpose |
| --- | --- |
| `index.html` | Static, Excel-like heatmap viewer using Hyparquet in the browser, with a live numbered architecture diagram and config-to-stack mapping. |
| `architecture.svg` | Full-width layered architecture displayed on the live page, with embedded official icons and numbered parts 1–6. |
| `results.parquet` | Sanitized, compact benchmark summary published by the OAT server. |
| `oat-config.example.json` | Validated configuration template with no credentials. |
| `architecture.drawio` / `architecture.drawio.png` | Editable source and rendered companion for the same layered architecture. |
| [Runtime setup](RUNTIME.md) | Isolated internal-only Kubernetes stack, image digests, workload identity, SQL bootstrap, secret files and local rehearsal. |

The viewer's six numbered stack parts match the diagram badges. Their numbers appear beside every OAT JSON field and Parquet result column that they affect. Preserve these IDs and titles when updating the diagram, configuration reference, or heatmap.

## Parquet contract

Schema version 2 keeps every existing column and adds run/platform and pinning metadata. `started_at` is an Arrow `timestamp[ms, tz=UTC]`; nullable fields are written as null when unavailable, never as synthetic zeroes.

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
| `git_sha` | UTF-8 string | Required | Source revision used for the run, or `unknown` when unavailable. |
| `tunables_json` | UTF-8 string | Required | JSON object containing the experiment tunables. |

The producer retains these original result columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | UTF-8 string | Benchmark run identifier. |
| `replicas` | 64-bit unsigned integer | ArrowCol replica count. |
| `dfe_worker_concurrency` | 64-bit unsigned integer | DFE `--num-cores` setting. |
| `table` | UTF-8 string | DMV table used as the table-scoped routing key. |
| `rows` | 64-bit unsigned integer | Rows sent by the standalone DMV client. |
| `duration_ms` | 64-bit unsigned integer | Per-table client duration, including connection, registration, and upload. |
| `rows_per_second` | 64-bit float | Per-table throughput derived from the client's duration. |
| `cpu_millicores` | Nullable 64-bit float | Unique collector/DFE pod CPU from `kubectl top`; integrated pods are counted once. |
| `memory_bytes` | Nullable 64-bit unsigned integer | Unique collector/DFE pod memory from `kubectl top`; integrated pods are counted once. |
| `qap_observation_complete` | 64-bit unsigned integer | 1 only when all expected QAP tables and required fields were measured; otherwise 0. |
| `qap_observed_tables` | 64-bit unsigned integer | Number of expected QAP tables observed. |
| `qap_completion_percent` | Nullable 64-bit float | Minimum reported completion percentage across the selected tables. |
| `qap_pending_artifacts` | Nullable 64-bit unsigned integer | Sum of pending artifacts across selected tables. |
| `qap_cached_bytes` | Nullable 64-bit unsigned integer | Sum of QAP `HotSize` bytes across selected tables. |
| `qap_unhealthy_catalogs` | Nullable 64-bit unsigned integer | Sum of unhealthy catalog counts across selected tables. |
| `qap_lag_seconds` | Nullable 64-bit float | Maximum reported QAP lag across selected tables. |

The viewer reads `results.parquet` from the same origin using HTTP range requests; it does not need CORS configuration. Azure Storage static website hosting on the HNS-enabled account serves `$web` anonymously, even when account-level blob public access is disabled. The AKS defaults target `abfss://experiments@arrowcollector<stamp>.dfs.core.windows.net/arrowcol-oat/results.parquet` and the root `results.parquet` in that stamp's `$web`; explicit destinations may override them. The producer uses conditional ETag updates, deduplicates retries by `(run_id, table)`, and verifies the website copy matches the authoritative results object before success. Publish only approved nonsensitive summary metrics there—never credentials, raw payloads, or sensitive identifiers.

## Running the OAT matrix

Complete [runtime setup](RUNTIME.md) first for AKS runs. The OAT executable starts its local Rust API server automatically; Rust/Cargo and the npm workspace dependencies must be available. The supported AKS topology uses **one integrated `arrowcol` Deployment**, container `dataflow`, for both receiver and exporter. Set both deployment/container/selector pairs identically. The server applies replicas and `--num-cores` together, observes and restores each unique Deployment once, and counts each pod once. It writes one compact Parquet file to both configured ADLS Gen2 destinations. The web destination must be the root of `$web` (for example, `abfss://$web@account.dfs.core.windows.net/results.parquet`).

Run the local OAT from the repository root with `npx arrowcol-oat --config docs/experiments/arrowcol-oat/oat-config.example.json`. For bridged Docker hosts, isolated smoke settings and opt-in stopped Delta exports, see [local runtime and snapshots](RUNTIME.md#local-smoke-and-stopped-delta-snapshots). The executable starts the Rust API server, builds the local Compose images, creates run-scoped local credentials and state, submits the configuration, polls the run, then stops the stack and API server. It then serves the local results page on loopback with HTTP range support and keeps the command open until Ctrl+C. Compose assigns ephemeral host ports for the local router and Kusto; the sample endpoint values are placeholders. Rust/Cargo, Docker Compose, and npm workspace dependencies must be available. The included sample writes both result files under `/tmp`; set ADLS Gen2 URLs when publishing results to storage. Add `--server <URL>` to use an already-running OAT API server instead.

AKS runs require a working Kubernetes context and metrics API, `dmv_client_image`, a namespace secret named by `edge_secret_name` with an `edge-hmac` key, and the **real provisioned QAP endpoint**. An optional `platform.kustomize_overlay` points to a local Kustomize directory for pod sizing; the rendered overlay is restricted to the configured ArrowCol/DFE Deployments, which the runner snapshots and restores. Each client is a Kubernetes Job pinned to `arrow-collector/role=client` with its matching NoSchedule toleration; Jobs use topology spread across `kubernetes.azure.com/agentpool`. ArrowCol, DFE, router and catalog runtime pods select the `arrowcol` role pool and tolerate its matching taint. AKS requires a five-digit `stamp`; if result destinations are omitted, the producer uses that stamp's `experiments` container and `$web`. Local runs use the Compose stack and local client/ArrowCol/Kusto endpoints; only the configured Parquet results sink may use Azure. Set local `qap_database` to `null` to use Kustainer's default `loadtest` database. Each DDL/upload is HMAC-signed; each concurrent upload uses a separate connection so SO_REUSEPORT can distribute work across DFE cores. Client timing reports steady-window throughput rather than admission latency.

Each matrix cell initializes and binds its complete immutable SQL run-table pin set before launching clients, through the existing routing engine's collector refresh/ranking and SQL lease-CAS path. The finite setup process is authenticated as `arrowcol_migrator`, never with router/app credentials, and receives only the routing-selection policy projected from that platform's exact rendered router config. Local Compose uses its dedicated migrator password file; AKS mounts the existing `arrowcol-sql-migration` Secret only into finite SQL migration and pin-init Jobs at `/run/migration/password`, never into router, client, or collector pods. The runtime bootstrap and pin setup must use the same migrator password source.

Set `clients_per_table` from 1 to 4 to multiply the client fleet (maximum 128 client jobs/containers); each table remains a single routing target and can still be owned by one ArrowCol. `client_ready_timeout_seconds` bounds client image startup, DMV preflight, and whole-fleet readiness (30–1,800 seconds; default 600). Each client schedule wait is separately bounded to readiness + `qap_timeout_seconds` + 60 seconds for final schedule delivery; QAP preparation itself is bounded by `qap_timeout_seconds` (1–300 seconds). A QAP deadline failure is returned without retry; if a helper was interrupted, its outcome may be indeterminate and remains subject to the existing ownership-record rules. The parent permits at most 60 seconds from schedule arming to the common start, and fails if any client misses that window rather than accepting a partial fleet. AKS Job `activeDeadlineSeconds` covers the composed startup budget, warm-up, measurement, and a 300-second completion margin. Pin initialization is separately bounded to 180 seconds on either platform. Because immutable run-pin expiry starts before pin initialization, `run_pin_ttl_ms` must cover that setup bound plus the complete client Job lifetime; it is never extended to compensate for delayed preparation. The shared measurement timestamp is chosen only after every client is ready and QAP setup has completed. `dmv_concurrency` is bounded to 64 per client, `rows_per_table` to 1,000,000 per upload, and one matrix to 25 combinations. Set `pilot_mode: true` with a `measure_seconds` value that is a 30-second multiple from 60–600; warm-up plus measurement may not exceed ten minutes. The pilot emits per-table rows/s for each full 30-second window and reports the earliest cumulative horizon whose observed window rates vary by no more than 5%. Use those windows to select the shortest stable measurement duration for subsequent comparison runs.

Each matrix cell receives a canonical UUID and an isolated platform stack, run-scoped immutable table pins and router configuration. `run_pin_ttl_ms` defaults to 3,600,000 ms and is bounded to 86,400,000 ms; validation requires it to cover the 180-second pin-setup bound, configured readiness and QAP preparation, schedule delivery, workload, and Job cleanup budgets. The owner lease TTL is one hour so it cannot expire during a valid run.

For AKS, copy the example and set `platform.kind` to `aks`, remove the local Compose settings, and configure the AKS endpoints, secret path, context, namespace, and result destinations described above. Local configurations use the configured `compose_project` as a prefix and append the unique OAT run ID, isolating the network, containers, and volumes; `compose_state_root` contains generated local credentials and bind-mounted state for that run.

The server never creates, deletes, or scales AKS node pools. Run `cargo run --locked --package arrowcol-oat-rust -- --validate-config docs/experiments/arrowcol-oat/oat-config.example.json` before submission to validate the shape without changing cluster state; placeholders still need replacing. The `az://container/object` URL form is also accepted when Azure account authentication is already configured for the process.
