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
