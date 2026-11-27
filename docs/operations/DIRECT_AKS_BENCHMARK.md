# Direct AKS benchmark

One collector, one table and one load client on stamp `68590`, using the AKS defaults below. This is the route behind the ~10M canonical durable Delta rows/s result.

| Item | Contract |
| --- | --- |
| Entry points | `...aks-direct-benchmark.ts check` (no workload), `...aks-direct-benchmark.ts run --mode smoke` (60 s), and `...aks-direct-benchmark.ts run` (600 s) |
| Workload | [Pinned manifest](../../experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml) with digest-pinned images from the stamp ACR ([Images](#images)); 60 s warm-up, then a 60 s smoke or 600 s full timed load, then drain |
| Isolation | Fresh UUID, owned namespace and run-scoped FIC; run-scoped Delta data is retained |
| Measurement | Canonical durable Delta rows/s: rows in `dataChange` Adds of ingestion commits whose writer-reported finish time is in `[measurement_start, measurement_end)`, divided by the window. Every Delta version must join exactly one successful `arrowcol_data_commit` writer record; missing or conflicting records fail the run. |
| Cleanup | Remove only the run's UID-verified namespace and FIC; scale each empty workload pool to zero, then restore autoscaling min 0/max 1 |
| Duration | Plan about 30 minutes per full run. The load and Delta reconciliation stage is bounded at 45 minutes. |
| Optional ADX observation | [`run --qap`](AKS_QAP.md#direct-aks-ingestion-latency) measures acceleration separately without rebuilding or retuning the producer. |

## AKS defaults

| Component | Default |
| --- | --- |
| Placement | Collector stack on `benche64` (Standard_E64ds_v6); client Job on `benchd64` (Standard_D64ds_v6) |
| Collector | One pod, 48 CPU / 384 GiB, 48 pipeline cores; dataflow image built with mimalloc, fat LTO, 1 codegen unit, opt-level 3, `x86-64-v4` |
| Batching | 32 GiB / 200M rows global, 30 GiB / 150M rows per table, 4 GiB or 4,194,304 rows per flush, 32 concurrent flushes, pipeline depth 16, 4 staged files at once |
| Commit | Eager trigger, batched cached transaction lookup, checkpoint-aware snapshot cache, async checkpoint every 20 versions, 256 files per commit |
| Upload | Buffered multipart, streaming staging, 6 concurrent requests, 8 MiB buffers, upload limit 16 |
| Router | 16 threads, 1,024 HTTP/2 streams, 8 MiB stream / 64 MiB connection windows |
| Maintenance | Checkpoint only, every 60 s, applied |
| Client | 8 workers × 2 streams × 12 runtime threads, zero interval, 524,288-row batches, 1,048,576-row corpus, zstd Arrow IPC, 32-60 CPU / 72-160 GiB |

## Prerequisites

| Requirement | Detail |
| --- | --- |
| Checkout | Any `rust-playground` devcontainer at the revision under test, from the repository root |
| Tools | Node/npm, `az`, `kubectl` and `kubelogin` with `--disable-environment-override` |
| Operator | Sign in inside the devcontainer. The operator needs stamp and identity metadata reads, access to the AKS context and benchmark storage, rights to manage the run namespace and workloads, create/remove the run FIC, and scale the two workload pools and restore autoscaling. The workload identity client ID is not an operator login. |
| Stamp | Existing stamp; `benche64` and `benchd64` at zero nodes with autoscaling min 0/max 1; one run at a time |
| Private env file | `.temp/aks-direct-68590.env` with only `AZURE_SUBSCRIPTION_ID` and `AZURE_UAMI_CLIENT_ID` (identifiers, not secrets). Hydrate it below; no full `.env`, token or cache is needed, and an existing `.env` is never touched. |
| Private stamp YAML | `.temp/stamp-68590.yaml`, hydrated below from the stamp deployment record |

Never print or publish the private env file, stamp YAML or reports. In a dedicated Bash shell at the repository root, hydrate both owner-only files from read-only Azure metadata. Existing files are never overwritten; the checks reject a symlink, a file owned by another user or a file with group or other permissions.

```bash
set -euo pipefail
umask 077
export SUB=68fa0545-661c-4152-93f2-fc8ae1c231b8 STAMP_NUM=68590
export ENV_FILE=".temp/aks-direct-${STAMP_NUM}.env" STAMP_CONFIG=".temp/stamp-${STAMP_NUM}.yaml"
mkdir -p .temp
test -e "$ENV_FILE" || (
  tmp="$(mktemp "$ENV_FILE.XXXXXXXX")" || exit 1
  trap 'rm -f -- "$tmp"' EXIT
  client="$(az identity show --subscription "$SUB" --resource-group arrow-collector-rg --name arrow-collector \
    --query clientId --output tsv --only-show-errors)" || exit 1
  [[ "$client" =~ ^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$ ]] || exit 1
  printf 'AZURE_SUBSCRIPTION_ID=%s\nAZURE_UAMI_CLIENT_ID=%s\n' "$SUB" "$client" > "$tmp" || exit 1
  mv -n -- "$tmp" "$ENV_FILE"
) || { echo 'Private env hydration failed; stopping.' >&2; exit 1; }
test -e "$STAMP_CONFIG" || (
  tmp="$(mktemp "$STAMP_CONFIG.XXXXXXXX")" || exit 1
  trap 'rm -f -- "$tmp"' EXIT
  az deployment group show \
    --subscription "$SUB" \
    --resource-group arrow-collector-rg \
    --name "arrowcol-sandbox-stamp-${STAMP_NUM}" \
    --query "{
      subscription:'$SUB',
      stamp:'${STAMP_NUM}',
      resourceGroup:'arrow-collector-rg',
      location:properties.parameters.location.value,
      aks:properties.parameters.aksConfig.value,
      storageReadLogDestination:properties.parameters.readLogStorageAccountName.value && 'storage' || 'log-analytics',
      publisherFederatedCredentials:properties.parameters.publisherFederatedCredentials.value
    }" \
    --output yaml --only-show-errors > "$tmp" || exit 1
  test -s "$tmp" && mv -n -- "$tmp" "$STAMP_CONFIG"
) || { echo 'Stamp YAML hydration failed; stopping.' >&2; exit 1; }
for file in "$ENV_FILE" "$STAMP_CONFIG"; do
  [[ -f "$file" && ! -L "$file" && -O "$file" && -s "$file" ]] &&
    [[ "$(stat -c %a -- "$file")" =~ ^[46]00$ ]] ||
    { echo 'Private input must be a nonempty, owner-only regular file; stopping.' >&2; exit 1; }
done
echo 'private inputs ready'
```

## Check and smoke

`check` validates the private inputs without printing them, renders a fake run ID and verifies sentinel, ownership, digest-pinned image and stage-label invariants. It updates the local Kubernetes context and tries `kubectl apply --dry-run=server`; when server access cannot be confirmed it falls back to `--dry-run=client`. Neither creates cluster resources. Stop before smoke unless the final line ends `kubectl apply --dry-run=server.` A server dry run does not verify FIC or node-pool write access.

Smoke runs every AKS stage with a 60 s timed window, including a fresh namespace and run FIC, retained Delta data and cleanup. Do not score it.

```bash
npm ci
npx --no-install nx run arrowcol-oat:typecheck
check_output="$(npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts check \
  --env-file "$ENV_FILE" --stamp-config "$STAMP_CONFIG")"
printf '%s\n' "$check_output"
[[ "${check_output##*$'\n'}" == 'PASS private config and rendered manifest invariants; kubectl apply --dry-run=server.' ]] ||
  { echo 'No server dry-run confirmation; stop before scaling the pools.' >&2; exit 1; }
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run --mode smoke --env-file "$ENV_FILE" --stamp-config "$STAMP_CONFIG"
```

## Run

`run` defaults to the full 600 s window. Every stage has a bounded timeout and names the failing stage.

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run --env-file "$ENV_FILE" --stamp-config "$STAMP_CONFIG"
```

| Output | Meaning |
| --- | --- |
| `PASS <rate> canonical durable rows/s; <rows> rows in 600 seconds; Delta v<first>-v<last>; commit-timestamp window <rate> rows/s; report <path>` | Exit code `0`: Delta Adds reconciled with the active snapshot, every version joined one writer record, the registration receipt proved the one-table run, the namespace and run FIC were removed and both workload pools returned to zero |
| `report.json` | `canonical_rows_per_second` is the headline. `committed_rows_per_second` buckets by Delta commit timestamp and is diagnostic only. Also records `manifest_sha256`, `runbook_sha256` (this entry point), placement and cleanup flags. |
| `writer-commits.json` | The Delta Add commits and writer `arrowcol_data_commit` records behind the canonical join |
| `pod-placement.json`, `run-fic.json` | Pod-to-pool placement and the retained run-FIC receipt |

Private outputs are under `out/arrowcol-oat/direct-benchmark/<run-uuid-without-hyphens>/`. A functional `PASS` is not a throughput target; compare `canonical_rows_per_second` with the expected range.

Each invocation creates a fresh run ID. Repeating a run ID is refused before any Kubernetes apply: the same devcontainer checks the local run directory first, while a fresh local directory checks the remote Delta root. Never delete or reuse that data:

```bash
npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run \
  --env-file "$ENV_FILE" --stamp-config "$STAMP_CONFIG" --run-id <previous-run-uuid>
```

## Images

The manifest pins the six image digests from both 600 s confirmations. The dataflow digest `sha256:2951ebc62cd6ebe5c528a1388cdf184b3286706e7d9c59a01cec98a97b4c889e` has the registry tag `oat-d629e5705344f32d7b56d1f116c18b4c56836a9b-pcc540c67621826f74a21a3cbf5051d7443a22dfff1ab6dee75a3f7e2a296e3ca`; the five companion digests have the tag `oat-a14d852b4dd177f283fc226449bb2b61d507a856`. These pins reproduce the campaign binaries, not later source changes. A run against them cannot qualify a new build.

The run pulls the manifest's pinned images from the stamp ACR; it never builds. To re-pin after changing collector, router, catalog, SQL, maintenance or client code, publish all six images from a clean checkout. Setup builds the dataflow image with the AKS collector build profile above, tags it `oat-<revision>-p<profile-hash>`, and reuses images already published for the same revision. The fat-LTO, single-codegen-unit dataflow build is slow and memory-heavy; use a large devbox.

```bash
npx --no-install arrowcol-oat aks setup --stamp "$STAMP_NUM" --stamp-config "$STAMP_CONFIG" --maintenance --skip-preflight
```

Pin the `images` digests from private `out/arrowcol-oat/runtime.json` and the load-client `dmv_client_image` from `out/arrowcol-oat/oat-config.aks.json` into every matching manifest image line. Setup also applies the publisher identity, mints a one-hour host assertion and uploads the results viewer.

## Cleanup

Cleanup is automatic on success and failure after owned resources are created. The entrypoint deletes only the namespace whose labels, annotations, and UID still match this run and removes only its run-scoped FIC after namespace absence. For each non-zero workload pool, it waits up to 25 minutes for any non-DaemonSet pods, including completed pods, to leave or for the autoscaler to drain the pool naturally. Only when no such pods remain does it temporarily disable autoscaling, scale the pool to zero, and restore autoscaling min 0/max 1; already-empty pools are left unchanged. If a pod appears while autoscaling is being disabled, the runner skips scaling, restores autoscaling, and resumes the same bounded wait. It never removes Delta data, the shared identity, the stamp, other namespaces, or other FICs. If ownership verification, namespace deletion, FIC cleanup, a remaining workload pod, scale-down, or autoscaler restoration fails, the command exits nonzero and preserves the private receipt/report for recovery; do not broaden cleanup to shared resources.
