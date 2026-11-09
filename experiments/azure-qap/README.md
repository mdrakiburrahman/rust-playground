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
| `--kubernetes` | off | Run the full experiment as an AKS Job instead of in this devcontainer |
| `--kubernetes-config <yaml>` | `infra/kubernetes/azure-qap.yaml` | Kustomize image digest, pod resources, node selector and optional context; requires `--kubernetes` |
| `--env <file>` | `.env` | Private env file |
| `--out <dir>` | `.temp/experiments/azure-qap/<runId>` | Report directory |

Defaults live in [`scenario.json`](scenario.json); flags override them.
Durations accept `90s`, `2m`, `1h`, `1h30m` or bare seconds.

### Run on AKS

The devcontainer remains the default. To run the same driver and Rust binaries
inside a Kubernetes Job, publish a Linux image using the shared `arrow-collector`
UAMI, copy the Kustomize sample, and set its image to the returned immutable
`@sha256:` digest:

```bash
npx nx run azure-qap-experiment:image -- --registry <stamped-acr> \
  --azure-env .env.azure --stamp-config infra/azure/stamp.yaml
cp infra/kubernetes/azure-qap.example.yaml infra/kubernetes/azure-qap.yaml
# Set image to <stamped-acr>.azurecr.io/azure-qap@sha256:<published-digest>.
npx nx run azure-qap-experiment:run -- --kubernetes \
  --kubernetes-config infra/kubernetes/azure-qap.yaml \
  --operations optimize,purge,vacuum,reload
```

The YAML controls only the digest-pinned image, resource requests/limits, node
selector and optional context. Namespace, ServiceAccount, UAMI and Azure
resource settings come from validated stamp output. Commit the source first: the
publisher rejects a dirty worktree so each image digest identifies a revision.
AKS runs must include `reload`; evidence uses the shared principal and trial
path, excluding every owned read through HTTP markers and request receipts.
No Kusto-specific user-agent is assumed. The Job does not retry
automatically; the runner streams logs and retrieves the report/evidence/query
files before cleaning its uniquely named Job. See [Kubernetes runtime
configuration](../../infra/kubernetes/README.md) and [AKS QAP
operations](../../docs/operations/AKS_QAP.md).

## Clean AKS rehearsal

**Destructive:** reset deletes the entire `arrow-collector-rg` sandbox in the
explicit subscription. Stop writers and verify the subscription before running it.
It never deletes other resource groups; it waits only for AKS-managed groups
captured from that sandbox's inventory.

| Step | Command or action |
| --- | --- |
| Reset | `npx --no-install tsx tools/scripts/src/qap-sandbox-reset.ts --subscription "$SUBSCRIPTION" --confirm-reset arrow-collector-rg --receipt "$RESET_RECEIPT"` |
| Bootstrap | Preserve the reset receipt outside the reset boundary, release the old lease, obtain a new lease and Full-bootstrap the exact branch using the existing devbox helpers. Do not copy generated `.env.azure`; sync only the approved private `.env` and `infra/azure/stamp.yaml`. |
| Run | `npx --no-install tsx tools/scripts/src/qap-rehearsal.ts run --subscription "$SUBSCRIPTION" --expected-sha "$COMMIT_SHA" --evidence-dir "$EVIDENCE_DIR"` |
| Collect existing run | `npx --no-install tsx tools/scripts/src/qap-rehearsal.ts collect --subscription "$SUBSCRIPTION" --evidence-dir "$EVIDENCE_DIR"` |

Use a new absolute receipt path and a new absolute evidence directory outside the
checkout, backed by storage that survives Full bootstrap. If using a devbox-local
path, download the reset receipt separately and verify `evidence.tar` and
`receipt.json` against `SHA256SUMS` before reset, bootstrap or lease release. Do not include private sync files in
the archive. The run requires a clean expected commit, private sync files and no
preexisting generated `.env.azure` or report directories. It provisions twice,
publishes the digest, runs both modes and collects; it never retries a failed
stage. Child output is suppressed to avoid exposing credentials; the receipt
records the failed stage and exit status. The collect command rechecks all eight
saved queries, validates completed reports and RELOAD, checks the exact owned AKS
objects are absent, and archives the evidence. For a pre-sidecar run, add `--legacy-cleanup-inventory`: it fails if any
`azure-qap-*` Job, Pod, ConfigMap or Secret exists in the namespace, including
objects from other runs. Alternatively use `--job-name <exact-owned-job>` from
a trusted run record; never guess a Job name. Run and collect require Linux in
the devcontainer; sandbox reset also supports the Windows host. Both commands also have `tools-scripts:qap-sandbox-reset`
and `tools-scripts:qap-rehearsal` Nx targets.

## What a trial does

1. **Seed**: `qap-fixture seed-scale` writes the table under
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
5. **Log wait**: polls the stamp's explicit Azure Monitor destination (Log
   Analytics or separate diagnostic storage) for shared-principal reads under
   the trial prefix. Complete fixture/maintenance HTTP receipts exclude owned
   reads; missing receipts fail attribution. Late logs settle within `--log-wait`.
6. **Report**: prints a table per trial and writes `report.md`, `report.json`,
   `evidence.jsonl`, `http-traces/` and saved queries under `--out`: `.kql` for
   Log Analytics or `.yaml` for storage export. The report includes recheck commands.

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
