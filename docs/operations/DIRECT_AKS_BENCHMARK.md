# Direct AKS benchmark

| Item | Contract |
| --- | --- |
| Entry points | `...aks-direct-benchmark.ts check` (no workload), `...aks-direct-benchmark.ts run --mode smoke` (60 s), and `...aks-direct-benchmark.ts run` (600 s) |
| Workload | Pinned-digest images; 60-second warmup, then a 60-second smoke or 600-second full timed load and drain. The registration image was built from `8f2cc94f`; all other images were built from `683081d9`. |
| Isolation | Fresh UUID, owned namespace and run-scoped FIC; the run-scoped Delta data is retained. |
| Measurement | Reconcile timestamped Delta `Add` actions against the final active snapshot; report committed rows/s and Delta versions. |
| Cleanup | Remove only the run's UID-verified namespace and FIC; scale each empty workload pool to zero, then restore autoscaling min 0/max 1. |

## Prerequisites

Run from the repository root in the `perf/arrowcol-throughput` devcontainer. The approved private bootstrap environment file defaults to `.env` and must include `AZURE_UAMI_CLIENT_ID` for the selected stamp. The private stamp YAML defaults to `infra/azure/stamp.yaml`; provide `--env-file` and `--stamp-config` when their paths differ. Never print or publish either file's contents. The entrypoint verifies the selected Azure subscription, cluster, node-pool autoscaler settings, and zero starting workload nodes. The existing pinned images and `benche64`/`benchd64` pools must be available; images are downloaded from the stamped ACR, not built locally.

## Check and smoke

The check validates required private configuration without printing values, renders a fake run ID, and verifies sentinel, ownership, digest-pinned image, and stage-label invariants. It attempts `kubectl apply --dry-run=server`; when the stamp is unreachable it uses `--dry-run=client`. Neither mode creates resources or launches a workload.

The smoke mode follows the same ordered AKS stages with a 60-second timed window. It still creates a fresh namespace and run FIC, retains run-scoped Delta data, and performs the documented cleanup.

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts check --env-file .env --stamp-config .temp/stamp-68590.yaml
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run --mode smoke --env-file .env --stamp-config .temp/stamp-68590.yaml
```

## Run

`run` defaults to the full 600-second window. Each stage has a bounded timeout and identifies the failing stage. Cleanup runs in `finally`, independently attempts namespace deletion, run-FIC removal after namespace absence, both workload-pool restorations to zero with autoscaling min 0/max 1, and local secret removal.

```bash
npm ci
npx --no-install nx run arrowcol-oat:typecheck
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run
```

The command prints the new run ID and ends with a `PASS` line containing committed rows/s and the report path. Exit code `0` means the Delta Adds reconciled with the active snapshot, the timed window contained committed rows, the registration receipt proved the one-table run, the namespace and run FIC were removed, and both workload pools returned to zero. Private reports and the retained FIC receipt are under `out/arrowcol-oat/direct-benchmark/<run-uuid-without-hyphens>/`.

Each invocation creates a fresh run ID. Repeating a run ID is refused before any Kubernetes apply: the same devcontainer checks the local run directory first, while a fresh local directory checks the remote Delta root. Never delete or reuse that data:

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run \
  --run-id <previous-run-uuid>
```

## Cleanup

Cleanup is automatic on success and failure after owned resources are created. The entrypoint deletes only the namespace whose labels, annotations, and UID still match this run and removes only its run-scoped FIC after namespace absence. For each non-zero workload pool, it waits up to 25 minutes for any non-DaemonSet pods, including completed pods, to leave or for the autoscaler to drain the pool naturally. Only when no such pods remain does it temporarily disable autoscaling, scale the pool to zero, and restore autoscaling min 0/max 1; already-empty pools are left unchanged. It never removes Delta data, the shared identity, the stamp, other namespaces, or other FICs. If ownership verification, namespace deletion, FIC cleanup, a remaining workload pod, scale-down, or autoscaler restoration fails, the command exits nonzero and preserves the private receipt/report for recovery; do not broaden cleanup to shared resources.

## Offline scaled-profile contract

The checked-in runner and manifest remain the default one-writer/one-client path. `projects/arrowcol-oat/aks-scaled-profile.ts` is an offline integration contract only; it is not exposed as a live runner option and does not authorize or verify an AKS run.

| Guard | Scaled-profile requirement |
| --- | --- |
| Clients | Render 1–8 separate Jobs with distinct run-scoped client IDs, unchanged shared generation arguments, and required hostname anti-affinity. All use the existing shared start schedule; the schedule is not advanced until every client is ready, and any failed client aborts. |
| Pool | Snapshot `benchd64` autoscaler settings only when autoscaling is enabled, min is 0, and both configured and observed node counts are 0. Temporarily cap max at the client count; restore the exact original min/max only after the pool drains and the active settings have not drifted. |
| Workload identity | Hash the effective `diagnostic-replay` / `diagnostic` / IPC `none` profile, client count, and every generator argument except per-client IDs. The existing diagnostic parity context includes batch/corpus 65,536; the historical 528,220 rows/s result is comparable only with the same profile and tunables. Any realistic profile must be labeled separately. |
| Images and observation | Bind all exact source image references (including distinct digests sharing one repository) to candidate digests and the same source commit/config hash. Runtime evidence must retain each client pod UID-to-node/image-ID mapping, distinct node placement, and collector pod UID-to-node/image-ID mapping. |

These helpers have only been exercised with local fixtures. Candidate image builds/attestations, live pool mutation/restoration, pod observation, quota/SKU availability, and end-to-end scaled execution remain unverified; no Azure or AKS writes or image pushes were performed.
