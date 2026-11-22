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

## Opt-in scaled holdout

The default runner and source manifest retain the historical one-writer/one-client behavior. The same entrypoint now accepts `--profile scaled`; this path has been verified offline with mock commands, not with a live deployment or load. These commands do not authorize cloud execution.

| Guard | Scaled-profile requirement |
| --- | --- |
| Clients | `--clients 1..8` (default 1): separate run-owned one-completion Jobs and required hostname anti-affinity. One-client replay preserves payload ID `direct-e64-confirmwide` and pod ordinal 0. Multi-client IDs are stable, unique, equal-width `direct-e64-client-0001` through `0008`, with ordinals 0..N-1; they are namespace-isolated, never run-UUID-expanded. Every client must become ready before one common schedule is patched and finish the same 60-second warmup/600-second window. Missing/failed clients abort; smoke is not accepted. |
| Collector | `--collector-shape e64` (default): E64ds_v6, 48 runtime/request/limit cores, 384 GiB; `e192`: E192ids_v6, 176 cores, 1536 GiB. Core-allocation policy and RSS admission limit are rendered for the shape. All collector/support stages target a fresh run-owned pool with a run-ID taint, declared by `infra/azure/scaled-collector.bicep`; existing `arrowe192` and `benche64` are not modified. |
| Client pool | Snapshot `benchd64` only when autoscaling is enabled, min is 0, and configured/observed nodes are 0. Temporarily cap max at the client count. After namespace deletion, drain and restore the exact original settings; drift is a nonzero failure, never overwritten. Partial startup also takes the owned cleanup path. |
| Workload provenance | Resolve and explicitly pin known client defaults, CLI flags and literal env. Conflicting/unknown env, envFrom, commands and args are rejected. The workload SHA-256 binds sorted effective generator configuration, client count and ordered payload-ID/pod-ordinal pairs. Job/run names remain unique but do not change diagnostic inputs. Historical data-identity eligibility requires the exact single-client ID and ordinal; another ID or multiple clients is non-parity. The rendered-manifest SHA-256 separately binds collector/support configuration and resources. Realistic or compressed profiles are separately labeled. |
| Candidate images | JSON binds each exact original image reference to a candidate digest, requested commit and workload hash. This is **structural binding, not build proof**. Before any workload/pool write, `run` requires `cosign verify-attestation` using `--provenance-key` from an independently trusted builder. Signed SLSA v0.2 claims must bind the exact image repository/digest, primary `invocation.configSource` repository/commit, and publisher role/Dockerfile/context; a matching unrelated material is insufficient. No historical-image fallback exists. |
| Runtime evidence | Validate actual pod UID, node, image ID, args, resources, client env and collector ConfigMap against the render; verify distinct client nodes, actual VM SKUs and allocatable CPU/memory. Retain each client's full-window result and private transcript. The observer starts before scheduling and stops before Delta replay/failure cleanup. `node_observation` schema v2 separates target-pod procfs/cgroup-v2 aggregates from optional read-only Kubelet host totals/capacity. Reads have 5-second abortable bounds; stop kills owned reads. Coverage and reset/unavailable reasons accompany samples; missing data remains null. |

Prepare an offline render and binding template (zero digests are placeholders, not deployable candidate images). The output paths must not already exist:

```bash
mkdir -p out/arrowcol-oat
CANDIDATE_COMMIT=$(git rev-parse HEAD)
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts render \
  --profile scaled --clients 4 --collector-shape e192 \
  --candidate-commit "$CANDIDATE_COMMIT" --output out/arrowcol-oat/scaled-holdout.yaml \
  --env-file .env --stamp-config .temp/stamp-68590.yaml
```

This writes `scaled-holdout.yaml.images.json` with the workload hash and every source-image slot. Later, an authorized image build must populate its candidate digests and publish trusted attestations. To re-render those bindings offline, add `--image-manifest <populated-json>` and choose a new output path.

**The existing `publish-images.ts` does not produce attestations.** Its revision labels and ACR digests cannot unlock this gate. The future signed contract identifies `urn:arrowcol:oat:publish-images:buildLocalImages:v1`, with entry point `projects/arrowcol-oat/publish-images.ts#buildLocalImages` and the exact role build definition exported by that module. No attestation-producing builder or independent signer trust chain has been demonstrated; a blanket signature over caller-supplied mappings is not an authorized substitute.

Before cleanup, each client retains `<job>.measurement.json` and its private pod log, bound to the observed pod UID and global `[start,end)`. Final-window admission rows, configured-IPC FlightData protobuf bytes (not NIC or gRPC/HTTP2 framing), error/retry categories and per-subwindow latency quantiles/sample counts are preserved when present. Aggregate p99 is never averaged. Missing byte/latency/stream details have null values and reasons; process-cumulative counters are not substituted. The explicit eight workers by two streams gives **16 configured independent send slots**, not `--concurrency 32`. Active slot occupancy, individual completed stream row counts and message splitting are not exposed by the current receipt, so they remain unmeasured rather than inferred as fixed 65,536-row sends. Parity retains `native_quiver: false`: the client ACK is bounded best-effort DFE admission, not a Delta commit. Storage commit duration and send-to-commit p50/p99 remain null with reasons without actual duration evidence and a send-to-version join. Admission evidence never replaces Delta reconciliation.

**Live execution requires separate authorization, serial stamp custody, candidate builds and a trusted public key.** The following reference command provisions a run-owned collector pool and temporarily changes the empty client pool; do not execute it as part of offline preparation:

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run \
  --profile scaled --clients 4 --collector-shape e192 \
  --candidate-commit "$CANDIDATE_COMMIT" \
  --image-manifest out/arrowcol-oat/scaled-holdout.yaml.images.json \
  --provenance-key "${TRUSTED_BUILD_PUBLIC_KEY:?Set the independently trusted builder public-key path}" \
  --env-file .env --stamp-config .temp/stamp-68590.yaml
```

Read-only discovery on 2026-11-22 found no Canada Central SKU restrictions for the three shapes above, with 1800 E-family, 1776 D-family and 3800 regional vCPUs available. The runner rechecks SKU/family/regional quota before any scaled mutation; this is not a capacity reservation or proof of schedulability. Offline CLI tests exercise success, missing/failed clients, partial collector/startup failure, source-provenance rejection, pool drift and default compatibility. Actual signed candidate images, live pool restoration, observer access and end-to-end scaled throughput remain unverified. No Azure/AKS write, image push or load was performed for this work.

The historical 528,220 rows/s context is Diagnostic replay, IPC None, batch/corpus 65,536. Compare code revisions only at identical effective tuning **and hardware/client counts**; a larger collector or additional clients are not a code speedup. Label realistic assessments separately.
