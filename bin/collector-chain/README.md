# Collector-chain client

One simulated SQL Server client for the
[collector-chain experiment](../../experiments/collector-chain/README.md).

| Step | What it sends |
| --- | --- |
| Client ID | A fresh UUID per process, or `--client-id` to reuse one. |
| DMV Arrow | Registers every table in the SQL Server DMV catalog and puts `--rows` rows per table over Arrow Flight to `--arrow-endpoint`. `customer_id` holds the client ID. |
| OTLP | Sends `--signals` logs, spans and counter adds over OTLP/HTTP to `--otlp-endpoint`, with resource attribute `client.id`. |
| Output | One JSON object per round: `client_id`, `dmv_tables`, `dmv_rows_per_table`, `otlp_signals`. Nonzero exit if any send fails. |
| `--interval <s>` | Repeats the round every `<s>` seconds until stopped. Failed rounds go to stderr and the next round retries. |

```bash
cargo build --release -p collector-chain
target/release/collector-chain --arrow-endpoint http://<arrowcol-middle>:32310 --otlp-endpoint http://<otapcol-middle>:4318
```

## OAT DMV blast client

`arrowcol-dmv` is the OAT sending path. It prepares and IPC-encodes a bounded
deterministic corpus once, registers schemas once, and reuses persistent channels.
Every flag has an `ARROWCOL_CLIENT_<UPPER_SNAKE_NAME>` environment equivalent;
explicit CLI values win. Run `arrowcol-dmv --help` for the complete reference.

| Controls | Behavior |
| --- | --- |
| `--corpus-rows`, `--corpus-bytes` | Mutually exclusive; rows per selected table or an approximate decoded byte target across selected tables. Zero rows infers `--rows`. |
| `--batch-rows`, `--max-memory-bytes` | Batch rows default to `--rows`; the 512 MiB corpus reservation includes conservative encoding scratch and allocator slack, not transport/runtime RSS. Oversized requests fail before allocation/connection. |
| `--workers`, `--runtime-threads` | Zero uses all available cores. Every worker continuously replenishes `--streams-per-worker` independent slots without a cross-worker round barrier. Zero streams inherits `--concurrency`. |
| `--data-mode shared\|independent` | Shared immutable `Arc`-owned encoded corpus, or a separately seeded corpus and cursor per worker. Independent mode multiplies the memory reservation. |
| `--string-width`, `--cardinality`, `--null-fraction`, `--skew`, `--entropy`, `--seed` | Fixed-width strings (zero uses field/key formatting), value cardinality, nullable-field null probability, hot-key probability, variable string fraction, deterministic seed. `customer_id` always retains the client ID. |
| `--interval-us`, `--max-rows` | Per-stream-slot cooldown; zero never sleeps. Row limit bounds whole-batch attempts across preflight, warm-up and measurement; zero is unlimited and a remainder smaller than a batch is not sent. |
| `--connections-per-worker` | Persistent independent channels; stream slots round-robin across them. |
| `--ipc-compression`, `--dictionary-handling` | IPC `none`, `lz4_frame`, `zstd`; Flight dictionary `hydrate`/`resend`. The DMV fixture has no dictionary fields, so the latter does not dictionary-encode strings. |
| `--grpc-send-compression`, `--grpc-accept-compression` | `none`, `gzip`, `zstd`; sending compressed gRPC requires receiver support. This codec runs per send, separately from precomputed IPC. |
| Transport flags | Message/frame/header limits, HTTP/2 windows/adaptation, keepalive, TCP settings, endpoint concurrency/buffering, per-connection rate limit and timeouts are exposed in `--help`. |
| `--connect-retries`, `--retry-backoff-ms` | Only retry connection establishment. Ambiguous `DoPut` uploads are never automatically retried; doing so could duplicate admitted rows. |
| `--duration-secs`, `--warmup-secs` | Zero duration uploads the corpus once. A positive duration requires a shared future `--run-start-unix-ms` or OAT-armed `--schedule-file`; pilot mode preserves 30-second windows. |

`OAT_CLIENT_EFFECTIVE_CONFIG` emits non-secret resolved tuning JSON. No payloads,
endpoint, identity, key or file paths are included. `OAT_CLIENT_METRICS` emits
versioned JSON at preparation, measurement start, completion and failures.
Per-worker attempted/completed/error/cancelled rows and protobuf bytes and
power-of-two-upper-bound latency percentiles are process-cumulative. Preparation
reports decoded/encoded corpus size, generation/encoding wall time and Linux
caller-thread CPU time (null on other platforms); steady-state generation is
absent, not claimed to be saturated.

`measurement_window` counts each successfully observed admission completion
inside the half-open shared Unix interval, including warm-up requests completing
inside it and excluding requests completing afterward. Workers stop starting
requests at the end and drain outstanding requests under the configured timeout.
Snapshot deltas and cumulative latency are **not** exact measurement-window
statistics. Admission completion is **not** proof of Delta commit or visibility.
