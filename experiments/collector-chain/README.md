# Collector-chain experiment

Local-only proof of collector-to-collector chaining. Every client sends DMV Arrow and OTEL
logs, metrics and traces through a middle collector into one batching gateway per chain.
Each gateway writes Delta Lake to local disk, and Kustainer reads the Delta tables natively.
Design and diagram: [docs/experiments/collector-chain](../../docs/experiments/collector-chain/README.md).

```bash
npx nx run collector-chain-experiment:run
npx nx run collector-chain-experiment:run -- --clients 6 --keep
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--clients <n>` | 3 | Concurrent clients (1-12). Client `i` uses middle `i % 3 + 1`. |
| `--rows <n>` | 100 | Rows per DMV table per client. |
| `--signals <n>` | 10 | Logs, spans and counter adds per client. |
| `--out <dir>` | `.temp/experiments/collector-chain/<runId>` | Report, client output and Delta tables. |
| `--keep` | off | Leave the stack running and print the teardown commands. |

| Stage | Pass condition |
| --- | --- |
| `stack` | `docker compose up --wait`: 3 ArrowCol middles, 1 ArrowCol gateway, 3 OTAPCol middles, 1 OTAPCol gateway, Kustainer. |
| `clients` | Every `collector-chain` client exits 0 and prints its unique client ID. |
| `flush` | Every DMV table and every OTAP signal table has a Delta commit with the expected rows. |
| `kusto` | `kind=delta` external tables over the gateway output return the expected rows for every client ID, and no unknown IDs. |
| `batching` | Each gateway table holds every client's data in fewer Delta commits than there are clients. |

| Exit code | Meaning |
| --- | --- |
| 0 | All checks passed. |
| 1 | A check failed; the report in the output directory shows which. |
| 2 | The run errored (stack, client or timeout). |
