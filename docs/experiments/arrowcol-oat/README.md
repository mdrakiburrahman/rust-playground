# ArrowCol OAT benchmark results

| File | Purpose |
| --- | --- |
| `index.html` | Static, Excel-like heatmap viewer using Hyparquet in the browser. |
| `results.parquet` | Sanitized, compact benchmark summary published by the OAT server. |
| `architecture.drawio` / `.png` | Editable architecture diagram showing each tunable and its component. |

## Parquet contract

The viewer reads these columns from `results.parquet`:

| Column | Type | Meaning |
| --- | --- | --- |
| `run_id` | UTF-8 string | Benchmark run identifier. |
| `replicas` | 32-bit integer | ArrowCol replica count. |
| `dfe_worker_concurrency` | 32-bit integer | DFE worker concurrency setting. |
| `table` | UTF-8 string | SQL table pinned to this ArrowCol result. |
| `rows` | 64-bit integer | Rows read by the DMV client. |
| `duration_ms` | 64-bit integer | End-to-end measurement duration in milliseconds. |
| `rows_per_second` | 64-bit float | Observed throughput. |
| `cpu_millicores` | Nullable 64-bit float | Collected CPU metric, when available. |
| `memory_bytes` | Nullable 64-bit integer | Collected memory metric, when available. |

The viewer reads `results.parquet` from the same origin and uses HTTP range reads. It does not need CORS configuration. Azure Storage static website hosting serves `$web` anonymously; publish only approved nonsensitive summary metrics and never credentials, raw payloads, or sensitive identifiers.
## Running the OAT matrix

Start the Rust OAT server in the repository root with `cargo run --locked --package arrowcol-oat-rust --bin arrowcol-oat-server`. It binds to `127.0.0.1:8088` by default. The server applies and observes the two named Kubernetes Deployments, sets DFE worker concurrency through its supported `--num-cores` argument, restores both original Deployment specifications after the matrix, gathers `kubectl top` metrics, and writes one compact Parquet file to both configured ADLS Gen2 destinations. The web destination must be inside `$web`; only sanitized benchmark summaries belong there.

Submit and poll a strict OAT JSON configuration with `npx --no-install tsx projects/arrowcol-oat/launch.ts <oat-config.json>`. Configure exactly the initial replica values `1` and `2`, two distinct positive DFE concurrency values, and at least two distinct table names. Provide separate ADLS object URLs for the experiment container and `$web`, plus the ArrowCol endpoint, deployment/container names, pod selectors, and built `arrowcol-dmv` binary path. The DMV client starts one process per table assigned to the active replica count, using a distinct table identity for each process.

The server requires a working Kubernetes context and metrics API. It does not create, delete, or scale AKS node pools. Run `cargo run --locked --package arrowcol-oat-rust --bin arrowcol-oat-server -- --validate-config <oat-config.json>` before submission to validate the input without changing cluster state.
