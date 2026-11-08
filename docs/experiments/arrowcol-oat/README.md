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