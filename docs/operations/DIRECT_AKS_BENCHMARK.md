# Direct AKS benchmark

| Item | Contract |
| --- | --- |
| Entry point | `npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run` |
| Workload | Pinned-digest images; 60-second warmup, 600-second timed load, and drain. Images were built from `683081d9`. |
| Isolation | Fresh UUID, owned namespace and run-scoped FIC; the run-scoped Delta data is retained. |
| Measurement | Reconcile timestamped Delta `Add` actions against the final active snapshot; report committed rows/s and Delta versions. |
| Cleanup | Remove only the run's UID-verified namespace and FIC; scale each empty workload pool to zero, then restore autoscaling min 0/max 1. |

## Prerequisites

Run from the repository root in the `perf/arrowcol-throughput` devcontainer. The approved private bootstrap environment file defaults to `.env` and must include `AZURE_UAMI_CLIENT_ID` for the selected stamp. The private stamp YAML defaults to `infra/azure/stamp.yaml`; provide `--env-file` and `--stamp-config` when their paths differ. Never print or publish either file's contents. The entrypoint verifies the selected Azure subscription, cluster, node-pool autoscaler settings, and zero starting workload nodes. The existing pinned images and `benche64`/`benchd64` pools must be available; images are downloaded from the stamped ACR, not built locally.

## Run

```bash
npm ci
npx --no-install nx run arrowcol-oat:typecheck
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run
```

The command prints the new run ID and ends with a `PASS` line containing committed rows/s and the report path. Exit code `0` means the Delta Adds reconciled with the active snapshot, the timed window contained committed rows, the registration receipt proved the one-table run, the namespace and run FIC were removed, and both workload pools returned to zero. Private reports and the retained FIC receipt are under `out/arrowcol-oat/direct-benchmark/<run-uuid-without-hyphens>/`.

Each invocation creates a fresh run ID. After a successful run, repeating with its ID is a no-op refusal before any Kubernetes apply because its Delta root already exists; never delete or reuse that Delta data:

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run \
  --run-id <previous-run-uuid>
```

## Cleanup

Cleanup is automatic on success and failure after owned resources are created. The entrypoint deletes only the namespace whose labels, annotations, and UID still match this run and removes only its run-scoped FIC after namespace absence. For each non-zero workload pool, it verifies that no non-DaemonSet pods remain, temporarily disables autoscaling, scales the pool to zero, and restores autoscaling min 0/max 1; already-empty pools are left unchanged. It never removes Delta data, the shared identity, the stamp, other namespaces, or other FICs. If ownership verification, namespace deletion, FIC cleanup, a remaining workload pod, scale-down, or autoscaler restoration fails, the command exits nonzero and preserves the private receipt/report for recovery; do not broaden cleanup to shared resources.
