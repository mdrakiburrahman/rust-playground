# ArrowCol OAT benchmark results

| File | Purpose |
| --- | --- |
| `index.html` | Static, Excel-like heatmap viewer using Hyparquet in the browser. |
| `results.parquet` | Sanitized, compact benchmark summary published by the OAT server. |
| `oat-config.example.json` | Validated configuration template with no credentials. |
| `architecture.drawio` / `.png` | Editable architecture diagram showing each tunable and its component. |

## Parquet contract

The viewer reads these columns from `results.parquet`:

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | UTF-8 string | Benchmark run identifier. |
| `replicas` | 64-bit unsigned integer | ArrowCol replica count. |
| `dfe_worker_concurrency` | 64-bit unsigned integer | DFE `--num-cores` setting. |
| `table` | UTF-8 string | DMV table used as the table-scoped routing key. |
| `rows` | 64-bit unsigned integer | Rows sent by the standalone DMV client. |
| `duration_ms` | 64-bit unsigned integer | Per-table client duration, including connection, registration, and upload. |
| `rows_per_second` | 64-bit float | Per-table throughput derived from the client's duration. |
| `cpu_millicores` | Nullable 64-bit float | ArrowCol + DFE CPU from `kubectl top`, when available. |
| `memory_bytes` | Nullable 64-bit unsigned integer | ArrowCol + DFE memory from `kubectl top`, when available. |
| `qap_observation_complete` | 64-bit unsigned integer | 1 only when all expected QAP tables and required fields were measured; otherwise 0. |
| `qap_observed_tables` | 64-bit unsigned integer | Number of expected QAP tables observed. |
| `qap_completion_percent` | Nullable 64-bit float | Minimum reported completion percentage across the selected tables. |
| `qap_pending_artifacts` | Nullable 64-bit unsigned integer | Sum of pending artifacts across selected tables. |
| `qap_cached_bytes` | Nullable 64-bit unsigned integer | Sum of QAP `HotSize` bytes across selected tables. |
| `qap_unhealthy_catalogs` | Nullable 64-bit unsigned integer | Sum of unhealthy catalog counts across selected tables. |
| `qap_lag_seconds` | Nullable 64-bit float | Maximum reported QAP lag across selected tables. |

The viewer reads `results.parquet` from the same origin using HTTP range requests; it does not need CORS configuration. Azure Storage static website hosting on the HNS-enabled account serves `$web` anonymously, even when account-level blob public access is disabled. Publish only approved nonsensitive summary metrics there—never credentials, raw payloads, or sensitive identifiers.

## Running the OAT matrix

Build the standalone DMV client with `cargo build --locked --package collector-chain --bin arrowcol-dmv`, then start the Rust OAT server with `cargo run --locked --package arrowcol-oat-rust`. It binds to `127.0.0.1:8088`. The server applies and observes the two named Kubernetes Deployments, sets DFE worker concurrency through its supported `--num-cores` argument, restores both original Deployment specifications after the matrix, gathers `kubectl top` metrics, and writes one compact Parquet file to both configured ADLS Gen2 destinations. The web destination must be the root of `$web` (for example, `abfss://$web@account.dfs.core.windows.net/results.parquet`).

Submit and poll a strict OAT JSON configuration with `npx --no-install tsx projects/arrowcol-oat/launch.ts docs/experiments/arrowcol-oat/oat-config.example.json`. Copy the example and supply actual deployment names, selectors, endpoint URLs, and storage paths. Set exactly the initial replica values `1` and `2`, DFE worker concurrency values `1` and `2`, and at least two distinct DMV table names from `projects/loadtest/schemas/sqlserver.sql`. For each matrix case, one client process per selected table runs concurrently; the router uses the table identity as its stable routing key and keeps one valid SQL-fenced lease owner for that table. Enable `distinct_table_owners: true` in the router policy so separate active tables are assigned to different ArrowCol collectors, including across router replicas; SQL application locks serialize competing claims. The policy and current table-to-collector assignments are observable through `arrowcol_router_distinct_table_owners` and `arrowcol_router_table_owners` metrics.

The server requires a working Kubernetes context and metrics API. It does not create, delete, or scale AKS node pools. Run `cargo run --locked --package arrowcol-oat-rust -- --validate-config docs/experiments/arrowcol-oat/oat-config.example.json` before submission to validate the input without changing cluster state. The `az://container/object` URL form is also accepted when Azure account authentication is already configured for the process; account-specific `abfs://` and `abfss://` URLs are shown in the example.
