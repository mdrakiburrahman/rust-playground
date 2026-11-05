# Collector-chain experiment

Local-only proof of collector-to-collector chaining. Every client sends DMV Arrow and OTEL
logs, metrics and traces through a middle collector into one batching gateway per chain.
Each gateway writes Delta Lake to local disk, and Kustainer reads the Delta tables natively.
Design and diagram: [docs/experiments/collector-chain](../../docs/experiments/collector-chain/README.md).

```bash
npx nx run collector-chain-experiment:run
npx nx run collector-chain-experiment:run -- --clients 6 --keep
npx nx run collector-chain-experiment:run -- --stream
npx nx run collector-chain-experiment:down
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--clients <n>` | 3 | Concurrent clients (1-12). Client `i` uses middle `i % 3 + 1`. |
| `--rows <n>` | 100 | Rows per DMV table per client. |
| `--signals <n>` | 10 | Logs, spans and counter adds per client. |
| `--out <dir>` | `.temp/experiments/collector-chain/<runId>` | Report, client output and Delta tables. |
| `--keep` | off | Leave the stack running, publish Kusto and the kusto-explorer browser UI on `127.0.0.1`, and print the teardown command. Replaces an earlier run that publishes the same ports. |
| `--stream` | off | `--keep`, and keep the same clients sending a round every `--interval` seconds. |
| `--interval <s>` | 10 | Seconds between streaming rounds (1-3600). Needs `--stream`. |
| `--kusto-port <n>` | 8080 | Host port for Kusto with `--keep` or `--stream`. |
| `--ui-port <n>` | 8090 | Host port for kusto-explorer with `--keep` or `--stream`. |
| `--down` | off | Stop the clients and remove every stack that `--keep` or `--stream` left behind, then exit. Takes no other flag; the `down` target runs it without the image and cargo builds. |

| Stage | Pass condition |
| --- | --- |
| `stack` | `docker compose up --wait`: 3 ArrowCol middles, 1 ArrowCol gateway, 3 OTAPCol middles, 1 OTAPCol gateway, Kustainer. |
| `clients` | Every `collector-chain` client exits 0 and prints its unique client ID. |
| `flush` | Every DMV table and every OTAP signal table has a Delta commit with the expected rows. |
| `kusto` | `kind=delta` external tables over the gateway output return the expected rows for every client ID, and no unknown IDs. |
| `batching` | Each gateway table holds every client's data in fewer Delta commits than there are clients. |
| `explorer` | With `--keep` or `--stream` only: kusto-explorer serves its page, reaches Kusto, and lists every external table the `kusto` stage created. |
| `stream` | With `--stream` only: the clients restart with the same IDs as detached processes, and a second round reaches both gateways' Delta tables. |

| Exit code | Meaning |
| --- | --- |
| 0 | All checks passed. |
| 1 | A check failed; the report in the output directory shows which. |
| 2 | The run errored (stack, client or timeout). |

## Explore the data

With `--keep` or `--stream`, open `http://localhost:8090` in a browser on the Docker host (for WSL
Docker, on Windows). This is the repo's [kusto-explorer](../../projects/kusto-explorer/README.md),
with the `Collector chain` connection and the `NetDefaultDB` database preselected.
`compose.keep.yaml` pins its published image, so `--keep` needs no local UI build. From inside the
devcontainer, use the container IPs that the run prints.

Kusto itself is on `http://localhost:8080` for Kusto Explorer and other clients. Kustainer has no
HTTPS or Entra auth: in Kusto Explorer, enable **Tools > Options > Connections > Allow unsafe connections**,
then remove `AAD Federated Security=True` from the connection string
([Microsoft docs](https://learn.microsoft.com/azure/data-explorer/kusto-emulator-install#connect-to-the-emulator)).
External tables read the Delta log at query time, so streamed rounds show up on the next query.

```kusto
.show external tables
external_table('otap_logs') | summarize count()
```

The run prints the teardown command, `npx nx run collector-chain-experiment:down`. It stops the
streaming clients and removes every stack that `--keep` or `--stream` left behind. To restart, just
run `--stream` again: it replaces the earlier stack that holds the same host ports, and stops that
stack's clients first. A container that is not part of this experiment is never touched; if it holds
a port, the run stops with its name, and `--kusto-port` and `--ui-port` choose other ports.
A long stream writes many small Delta commits with no compaction, so queries slow down after hours.
