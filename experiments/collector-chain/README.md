# Collector-chain experiment

Local-only proof of collector-to-collector chaining. Every client sends DMV Arrow and OTEL
logs, metrics and traces through a middle collector into one batching gateway per chain.
Each gateway writes Delta Lake to local disk, and Kustainer reads the Delta tables natively.
Design and diagram: [docs/experiments/collector-chain](../../docs/experiments/collector-chain/README.md).

```bash
npx nx run collector-chain-experiment:run
npx nx run collector-chain-experiment:run -- --clients 6 --keep
npx nx run collector-chain-experiment:run -- --stream
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--clients <n>` | 3 | Concurrent clients (1-12). Client `i` uses middle `i % 3 + 1`. |
| `--rows <n>` | 100 | Rows per DMV table per client. |
| `--signals <n>` | 10 | Logs, spans and counter adds per client. |
| `--out <dir>` | `.temp/experiments/collector-chain/<runId>` | Report, client output and Delta tables. |
| `--keep` | off | Leave the stack running, publish Kusto on `127.0.0.1:<kusto-port>` and print the teardown command. |
| `--stream` | off | `--keep`, and keep the same clients sending a round every `--interval` seconds. |
| `--interval <s>` | 10 | Seconds between streaming rounds (1-3600). Needs `--stream`. |
| `--kusto-port <n>` | 8080 | Host port for Kusto with `--keep` or `--stream`. |

| Stage | Pass condition |
| --- | --- |
| `stack` | `docker compose up --wait`: 3 ArrowCol middles, 1 ArrowCol gateway, 3 OTAPCol middles, 1 OTAPCol gateway, Kustainer. |
| `clients` | Every `collector-chain` client exits 0 and prints its unique client ID. |
| `flush` | Every DMV table and every OTAP signal table has a Delta commit with the expected rows. |
| `kusto` | `kind=delta` external tables over the gateway output return the expected rows for every client ID, and no unknown IDs. |
| `batching` | Each gateway table holds every client's data in fewer Delta commits than there are clients. |
| `stream` | With `--stream` only: the clients restart with the same IDs as detached processes, and a second round reaches both gateways' Delta tables. |

| Exit code | Meaning |
| --- | --- |
| 0 | All checks passed. |
| 1 | A check failed; the report in the output directory shows which. |
| 2 | The run errored (stack, client or timeout). |

## Explore in Kusto Explorer

With `--keep` or `--stream`, connect to `http://localhost:8080` on the Docker host (for WSL Docker,
from Windows), database `NetDefaultDB`. From inside the devcontainer, use the Kusto container IP
that the run prints. Kustainer has no HTTPS or Entra auth: in Kusto Explorer, enable
**Tools > Options > Connections > Allow unsafe connections**, then remove `AAD Federated Security=True`
from the connection string ([Microsoft docs](https://learn.microsoft.com/azure/data-explorer/kusto-emulator-install#connect-to-the-emulator)).
External tables read the Delta log at query time, so streamed rounds show up on the next query.

```kusto
.show external tables
external_table('otap_logs') | summarize count()
```

The run prints the exact teardown command. It stops the streaming clients and removes the stack.
A long stream writes many small Delta commits with no compaction, so queries slow down after hours.
