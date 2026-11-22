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
| Candidate images | JSON binds each exact original image reference to a candidate digest, requested commit and workload hash. This is **structural binding, not build proof**. Before any workload/pool write, `run` verifies an independently anchored `--provenance-bundle` or the existing cosign path. The new producer signs actual completed outputs, exact source commit/tree and recursive gitlinks, and the publisher's role/Dockerfile/context. Manifest/index and platform-child digests are distinct; runtime evidence must match the signed chain. No historical-image fallback exists. |
| Runtime evidence | Validate actual pod UID, node, image ID, args, resources, client env and collector ConfigMap against the render; verify distinct client nodes, actual VM SKUs and allocatable CPU/memory. Retain each client's full-window result and private transcript. The observer starts before scheduling and stops before Delta replay/failure cleanup. `node_observation` schema v2 separates target-pod procfs/cgroup-v2 aggregates from optional read-only Kubelet host totals/capacity. Reads have 5-second abortable bounds; stop kills owned reads. Coverage and reset/unavailable reasons accompany samples; missing data remains null. |
| Collector promotion | Optional `--collector-promotion JSON` applies only qualified leaf coordinates under `collector.batching`, `collector.commit`, `collector.runtime` or `parquet_writer.defaults`. The existing Rust typed registry validates against historical component context and projects env; the existing consumer-startup adapter checks actual readback before scheduling and after completion. Unmapped coordinates, invalid enums, explicit flag/env conflicts, table-writer policy or readback drift fail closed. Client profile, table/endpoint/data identities and pod/node limits are never copied from the local winner. |

Prepare an offline render and binding template (zero digests are placeholders, not deployable candidate images). The output paths must not already exist:

```bash
mkdir -p out/arrowcol-oat
CANDIDATE_COMMIT=$(git rev-parse HEAD)
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts render \
  --profile scaled --clients 4 --collector-shape e192 \
  --candidate-commit "$CANDIDATE_COMMIT" --output out/arrowcol-oat/scaled-holdout.yaml \
  --env-file .env --stamp-config .temp/stamp-68590.yaml
```

This writes `scaled-holdout.yaml.images.json` with the client input hash and every source-image slot, plus `.collector.json` binding expected collector args/env/resources/pipeline independently. The opt-in producer can populate image digests from completed builds. To re-render those bindings offline, add `--image-manifest <populated-json>` and choose a new output path.

### Qualified collector delta

The promotion file has exactly `schema_version: 1`, `qualified_source_commit`, `qualified_config_sha256`, `qualified_config_json` and `won_coordinates`. The config JSON is the exact retained full OAT configuration string from the qualified campaign; its SHA-256 covers those UTF-8 bytes, preserving the qualified digest rather than reserializing numbers. Coordinates are relative to its `tunables`, for example `["collector.batching.pipeline_depth"]`. Its source must match `--candidate-commit`. This is a binding input, **not a qualification verdict**; the campaign must qualify that source/config and those coordinates independently.

Only selected leaves are projected. For example, historical `coalesce_publications: true` supplies validation context for a selected pipeline depth without importing another profile's batching settings. Selected batching/commit leaves are updated in the historical exporter YAML and projected env. Runtime/writer leaves use the existing Rust env adapters. The client input hash does not change. Omitting the flag leaves the historical/default render unchanged.

```bash
cargo build --locked --package arrowcol-oat-rust --bin arrowcol-collector-contract
ARROWCOL_COLLECTOR_CONTRACT_PROOF=1 npx --no-install tsx --test projects/arrowcol-oat/aks-scaled-cli.test.ts
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts render \
  --profile scaled --clients 1 --collector-shape e64 \
  --candidate-commit "$CANDIDATE_COMMIT" --collector-promotion "$QUALIFIED_COLLECTOR_DELTA" \
  --output "$EVIDENCE/promoted.yaml" --env-file .env --stamp-config .temp/stamp-68590.yaml
```

The historical control has pod/CLI cores 48 but embedded DFE core-allocation count 32; scaled E64 renders that count as 48 and changes the receiver RSS limit. Singleton diagnostic input parity therefore does **not** certify full historical collector-config parity. These control values and the expected collector settings are retained separately. `ARROWCOL_COLLECTOR_CPU_WORKERS` is a separate process-wide pool, not the DFE allocation or node vCPU count. Consumer CPU-worker readback is reported independently when available; DFE startup currently emits its drain deadline, not its resolved core count, so process-core readback must remain unavailable rather than inferred from pod limits.

Promoted runs retain `collector-promotion-startup.json` and `collector-promotion-finished.json` before cleanup, binding checked public consumer settings to the collector pod UID and expected-settings hash. No raw local winner profile is copied into the report or manifest. The contract command performs only local file parsing/projection; these tests and renders perform no Azure/AKS writes.

**The existing publisher/default run remain unchanged.** The separate `aks-candidate-build.ts` producer reuses `imageBuilds`, fetches exact candidate Git objects from the policy repository and pinned submodule origins into isolated stores, and builds immutable archives without ignored files, export substitutions or signing material. It binds the completed BuildKit digest to hash-verified registry manifest/index, platform manifest and config bytes, then signs DSSE/SLSA statements outside build steps. It does not sign caller labels, tags, blobs or supplied digest mappings. The producer ID is `urn:arrowcol:oat:immutable-candidate-builder:v1`.

| Offline proof | Result/scope |
| --- | --- |
| `ARROWCOL_LOCAL_BUILD_PROOF=1 npx --no-install tsx --test projects/arrowcol-oat/aks-candidate-build.test.ts` | Six actual tiny builds using the publisher's role/path registry, owned loopback registry and disposable Ed25519 signer; signature/digest and negative verification. These are **fixture images, not Darwin images**, and fixture scope cannot authorize AKS. Owned containers, anonymous volumes and signer files are removed. |
| Default test suite | Immutable archives, pinned submodules, dirty/mismatched source refusal, external-key isolation, manifest-vs-index binding, wrong-key/tampered-payload rejection and actual CLI integration. No cloud execution. |

**Remaining external trust binding:** an operator must approve/pin the trusted producer tool revision separately from candidate source, protect an Ed25519 signing key so candidate build steps cannot access it, and independently supply its public-key fingerprint/PEM outside the candidate checkout. A key or approval shipped by a candidate is not a trust anchor. Production federation/ACR permission and normal campaign custody/approval are still required; this repository creates no production key, identity or approval.

**Application-code binding is also a live gate:** source plus a completed OCI digest does not prove a changed executable was rebuilt when Cargo target cache mounts are reused. Actual application publication requires independently validated per-arm BuildKit/Cargo isolation (a fresh builder with no shared writable target state is sufficient) and a retained installed-executable digest for the changed role. `--no-cache` alone does not clear cache mounts. The tiny fixture proof does not establish this application-build prerequisite.

After those gates only, the following command performs **ACR writes and production signing**. Run the approved tool installation, not code controlled by the candidate; keep outputs and keys outside candidate source. Production registry transport/federation has not been exercised by the local fixture proof:

```bash
npx --no-install tsx "$TRUSTED_TOOLS/projects/arrowcol-oat/aks-candidate-build.ts" \
  --source-root "$CANDIDATE_SOURCE" --revision "$CANDIDATE_COMMIT" \
  --publish --federated --registry arrowcollector68590 \
  --env "$PUBLISHER_ENV" --azure-env "$GENERATED_AZURE_ENV" \
  --stamp-config "$STAMP_CONFIG" --signing-key "$APPROVED_BUILD_SIGNING_KEY" \
  --output "$EVIDENCE/build-bundle.json" \
  --image-template "$EVIDENCE/scaled-holdout.yaml.images.json" \
  --image-binding "$EVIDENCE/candidate-images.json"
```

Without `--publish`, only loopback registries are allowed. `--image-template` is structural workload binding, not signed source evidence; its image digests are filled solely from actual producer outputs, and the runner independently recomputes its workload hash.

Before cleanup, each client retains `<job>.measurement.json` and its private pod log, bound to the observed pod UID and global `[start,end)`. Final-window admission rows, configured-IPC FlightData protobuf bytes (not NIC or gRPC/HTTP2 framing), error/retry categories and per-subwindow latency quantiles/sample counts are preserved when present. Aggregate p99 is never averaged. Missing byte/latency/stream details have null values and reasons; process-cumulative counters are not substituted. The explicit eight workers by two streams gives **16 configured independent send slots**, not `--concurrency 32`. Active slot occupancy, individual completed stream row counts and message splitting are not exposed by the current receipt, so they remain unmeasured rather than inferred as fixed 65,536-row sends. Parity retains `native_quiver: false`: the client ACK is bounded best-effort DFE admission, not a Delta commit. Storage commit duration and send-to-commit p50/p99 remain null with reasons without actual duration evidence and a send-to-version join. Admission evidence never replaces Delta reconciliation.

The headline's historical `readDeltaAddWindow` comparison against `commitInfo.timestamp` is unchanged. Separately, `collector-completion.log` and `collector-window.json` retain captured `arrowcol_data_commit` receipts whose `finished_at_unix_ms` is in `[start,end)`, joined by table ID/version to the same replay's Add rows/files/bytes. Both version sets and receipt-only/Delta-only versions are reported; missing receipt versions are distinguished from observed clock-boundary differences. No per-version cloud reread is performed, and neither receipt clock nor admission counts replace the Delta headline.

**Live execution requires separate authorization, serial stamp custody, candidate builds and a trusted public key.** The following reference command provisions a run-owned collector pool and temporarily changes the empty client pool; do not execute it as part of offline preparation:

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run \
  --profile scaled --clients 4 --collector-shape e192 \
  --candidate-commit "$CANDIDATE_COMMIT" \
  --image-manifest "$EVIDENCE/candidate-images.json" \
  --provenance-bundle "$EVIDENCE/build-bundle.json" \
  --provenance-key "${TRUSTED_BUILD_PUBLIC_KEY:?Set the independently trusted builder public-key path}" \
  --env-file .env --stamp-config .temp/stamp-68590.yaml
```

Read-only discovery on 2026-11-22 found no Canada Central SKU restrictions for the three shapes above, with 1800 E-family, 1776 D-family and 3800 regional vCPUs available. The runner rechecks SKU/family/regional quota before any scaled mutation; this is not a capacity reservation or proof of schedulability. Offline CLI tests exercise success, missing/failed clients, partial collector/startup failure, source-provenance rejection, pool drift and default compatibility. Actual signed candidate images, live pool restoration, observer access and end-to-end scaled throughput remain unverified. Only disposable fixture images were pushed to an owned loopback registry; no Azure/AKS/ACR write or load was performed for this work.

The historical 528,220 rows/s context is Diagnostic replay, IPC None, batch/corpus 65,536. Compare code revisions only at identical effective tuning **and hardware/client counts**; a larger collector or additional clients are not a code speedup. Label realistic assessments separately.
