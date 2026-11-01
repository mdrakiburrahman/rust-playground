# Azure QAP demo

A quick, tunable demo of how Kusto Query Acceleration (QAP) reacts to Delta
maintenance on an external table, and how to get the evidence out of Kusto and
Azure Storage logs yourself.

For each operation the run seeds a tiny Hive-partitioned Delta table, accelerates
it with QAP, runs the real maintenance (or a known reload), watches QAP state, and
then counts which **unchanged** Parquet files Kusto's managed identity read again
(`StorageBlobLogs`). Every phase prints, in plain English, what it is doing and the
exact KQL it runs, so you can copy the queries and explore while it runs.

Background, findings and how to read the output:
[docs/experiments/azure-qap/README.md](../../docs/experiments/azure-qap/README.md).

## Run

Requires the devcontainer, a private `.env`, Azure sign-in and the provisioned
sandbox (`npx nx run azure-infra:provision`). The run never deploys or deletes
Azure resources: it uses the sandbox's `experimentqap` container, Kusto database
and Log Analytics workspace.

```bash
npx nx run azure-qap-experiment:run                # quick default: ~5 min + bounded log wait
npx nx run azure-qap-experiment:run -- --help
npx nx run azure-qap-experiment:run -- --operations optimize,reload --tail 90s
npx nx run azure-qap-experiment:run -- --files 16 --file-mib 32 --repetitions 2 --tail 10m --budget 1h
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--operations <list>` | `optimize,purge,vacuum,reload` | Trials to run; `reload` drops and recreates the table (positive control) |
| `--files <n>` | `2` | Files per multi-file partition role (2-128) |
| `--file-mib <n>` | `1` | MiB per seeded file (1-32) |
| `--repetitions <n>` | `1` | Trials per operation (1-10) |
| `--parallel <n>` | `4` | Concurrent trials (1-8) |
| `--tail <dur>` | `2m` | Watch window after the mutation once QAP is caught up; restarts if QAP regresses |
| `--log-wait <dur>` | `5m` | Max wait for `StorageBlobLogs`; `0` skips it (not allowed with `reload`) |
| `--budget <dur>` | `15m` | Hard cap for all trials; must cover `ceil(trials / parallel) x tail` |
| `--env <file>` | `.env` | Private env file |
| `--out <dir>` | `.temp/experiments/azure-qap/<runId>` | Report directory |

Defaults live in [`scenario.json`](scenario.json); flags override them.
Durations accept `90s`, `2m`, `1h`, `1h30m` or bare seconds.

## What a trial does

1. **Seed**: `arrowcol-experiment seed-scale` writes the table under
   `experimentqap/runs/<runId>/<trial>/` using server-side blob copies of one
   seed Parquet file. The VACUUM trial is seeded with tombstones already in its
   Delta history, so VACUUM only deletes files that were unreferenced before QAP
   was enabled.
2. **Accelerate**: creates external table `qap_<runId>_<trial>` in the
   `experimentqap` database, enables QAP and waits until it is caught up.
3. **Mutate**: runs the real `delta-maintenance` OPTIMIZE, PURGE or VACUUM, or
   drops and recreates the table for `reload`.
4. **Tail**: watches QAP state and extents; if QAP looks caught up and then
   regresses, the window restarts (bounded by `--budget`).
5. **Log wait**: polls Log Analytics for reads by the Kusto managed identity under
   the trial's prefix until late logs settle (bounded by `--log-wait`).
6. **Report**: prints a table per trial and writes `report.md`, `report.json`,
   `evidence.jsonl` and `queries/*.kql` under `--out`, plus the queries to
   re-check late logs.

A trial ends as `complete`, `late-logs-pending`, `budget-exceeded` or `failed`.

## Cleanup and exit codes

Cleanup always runs, including on failure and Ctrl+C: the run's Kusto tables are
dropped and its fixture directory `runs/<runId>/` is deleted. If cleanup itself
fails, the run prints the commands to finish it by hand.

| Exit code | Meaning |
| --- | --- |
| `0` | Report written; pending late logs alone are not a failure |
| `1` | Error, failed trial, Delta/oracle mismatch, failed cleanup, or an included `reload` that was not seen re-reading every unchanged file (for any reason, including pending logs or budget) |
| `2` | Invalid flags or `scenario.json` |
| `130` | Interrupted (cleanup still ran) |

Nx reports any nonzero exit as `1`; read `report.json` for the details.

## Develop

```bash
npx nx run azure-qap-experiment:lint
npx nx run azure-qap-experiment:test
```
