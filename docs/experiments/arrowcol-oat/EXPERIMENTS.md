# E1-E5 comparable AKS inputs

| Run | ArrowCols | Tables | Clients/table | DFE concurrency |
| --- | --- | --- | --- | --- |
| E1 | 4 | All 32 canonical DMV schemas | Baseline: 1 | Baseline |
| E2 | 8 | Same 32 | Same | Same |
| E3 | 16 | Same 32 | Same | Same |
| E4 | 16 | Same 32 | 4 (128 clients total) | Same |
| E5 | 16 | Same 32 | 1 | Explicitly approved higher value |

This is source/config preparation, not evidence that a pilot or any experiment
ran. Earlier parse-only drafts with 19 `TODO` table slots and fixed 30/120-second
timings are historical receipts, not executable inputs. The binder reads the
now-approved canonical fixture rather than copying or inventing table names.

## Capacity prerequisites

The example's user pools can autoscale from zero to one node; per-node capacity
is the maximum scale-up headroom. Azure CNI Overlay supports at most 250 pods
per node and assigns each node a /24 pod address block ([Azure CNI Overlay planning](https://learn.microsoft.com/azure/aks/azure-cni-overlay#ip-address-planning)).
Reserve 32 pod slots per node for DaemonSets; with maxPods 250 this leaves 218
workload slots. The minimum maxPods values for the largest planned workloads
are 160 for the client pool (128 E4 Jobs + 32 reserved) and 52 for the ArrowCol
pool (20 workload pods + 32 reserved).

| Run | Client-pool Job pods | ArrowCol-pool peak pods |
| --- | ---: | ---: |
| Default AKS quick run | 1 | 5 |
| 32-table pilot | 32 | 5 |
| E1 | 32 | 8 |
| E2 | 32 | 12 |
| E3 | 32 | 20 |
| E4 | 128 | 20 |
| E5 | 32 | 20 |

Each client Job requests 250m CPU and 256Mi memory. E4 therefore requests
32 CPU and 32Gi but configures 2 CPU and 2Gi limits per Job; its aggregate
limits are 256 CPU and 256Gi on a 64-vCPU node, making CPU throttling a
measurement-validity risk. The ArrowCol pool's peak comprises SQL Server
(1 CPU/3Gi), catalog (500m/1Gi), router (500m/512Mi), the configured Dataflow
replicas (2 CPU/4Gi each), and one transient pin-init Job (100m/128Mi). The
32-slot DaemonSet reserve is a conservative capacity allowance, not a measured
count for a particular node image. During the pilot, retain per-container CPU
usage, CFS throttled seconds, throttled periods divided by total periods, and
node CPU utilization aligned to each 30-second measurement window.

## Inputs and remaining binding

| Evidence stage | Meaning |
| --- | --- |
| Collector completion | All clients completed their uploads and measurement interval. Result `tunables_json.collector_completion` preserves each client's actual best-effort/durable admission counts and completion timestamp from completed upload calls across preflight, warm-up and measurement. Partial or cancelled upload calls are not counted; `admission_accounting_complete=false` explicitly marks incomplete accounting when warm-up cancels an upload. Missing reports are explicitly unavailable. |
| Delta delivery | `tunables_json.delta_delivery` remains `not_established`. Durable admission acknowledges local WAL persistence, not Delta commit/flush or ADX visibility; no delivery barrier is implied. |
| QAP catch-up | After client completion, sample read-only QAP metadata until every expected table is healthy, 100% complete and has zero pending files, or the existing single `qap_timeout_seconds` observation budget expires. Keep the last complete snapshot on deadline; incomplete QAP still fails the unchanged verdict. |

Historical query responses remain in `out/arrowcol-oat/qap/<cell-run-id>/kusto-queries.jsonl`.
Catch-up also retains timestamped sanitized samples in `qap-observations.jsonl`
and its terminal outcome in `catch-up-diagnostic.json`. The separate QAP setup
budget is unchanged. Unix QAP helpers run in an owned process group; cancellation
stops local `npx`/`tsx` descendants, not just the launcher. This does not undo
provider requests: timed-out setup remains indeterminate and is not retried.
Policy `MaxAge` is a freshness cutoff, not a deadline
guaranteeing that caching has completed.

| Input | Source / rule |
| --- | --- |
| Private baseline | Generated `out/arrowcol-oat/oat-config.aks.json`, its runtime JSON and bootstrap environment file. Do not copy secrets into tracked files. |
| Data and load | Baseline `rows_per_table`, `dmv_concurrency`, one client/table, exactly one baseline DFE setting. Retain all other generated settings. |
| Images, storage, pod sizing | Preserve runtime file, immutable image digests, destinations and optional overlay unchanged across all five configs. Freeze the checkout, runtime and overlay contents after the pilot; paths alone do not freeze their contents. |
| QAP database | Read from the private baseline; no hardcoded database name or null fallback. |
| Warm-up and measurement | **Unbound until live pilot review.** No duration defaults in the binder. |
| E5 concurrency | **Unbound until explicitly selected.** Must exceed baseline and remain in 1-256. |

At the separately authorized pilot stage, use the existing OAT launcher and
`pilot_mode: true` with all 32 tables and the same frozen workload/runtime
settings. Pilot measurements must consist of complete 30-second windows, at
least two windows and at most ten minutes including warm-up. Retain all raw
`pilot_table=... window=...` and `stable_from_seconds=...` output, configs,
source/image hashes, orchestration/cell run IDs and failures.

Compare candidate warm-up/steady durations and choose the shortest qualifying
pair only after inspecting the **entire** observation for every table. The
range divided by mean must be strictly below 5%; an early stable prefix cannot
qualify a later drift. Select a shared pair that covers all five comparison
settings; if any setting fails its qualification, revise/re-pilot before
freezing the common pair. A printed shortest horizon is not proof that a
different warm-up was tested. Do not treat untested candidates as measured.

The final binding step, after that review and explicit run authorization, is:

```bash
# Repository root, devcontainer. These three values come from the approved pilot record.
: "${WARMUP_SECONDS:?bind the measured warm-up}"
: "${MEASURE_SECONDS:?bind the measured steady duration}"
: "${HIGHER_DFE:?bind the approved E5 concurrency}"
: "${STAMP_CONFIG:?set the private stamp YAML selected for deployment}"
npx --no-install tsx projects/arrowcol-oat/prepare-experiments.ts \
  --base out/arrowcol-oat/oat-config.aks.json \
  --stamp-config "$STAMP_CONFIG" \
  --out out/arrowcol-oat/e1-e5-approved \
  --warmup-seconds "$WARMUP_SECONDS" --measure-seconds "$MEASURE_SECONDS" \
  --higher-dfe "$HIGHER_DFE"
```

This command is offline: no Azure, Kubernetes, image publication or benchmark
operation. It reuses the OAT config loader and runtime validator, requires
existing private runtime/bootstrap files and digest-pinned images, and writes
five mode-0600 JSON configs into a new private output directory. It refuses to
overwrite existing output. Keep the original pilot records unchanged; this
command binds reviewed values, **does not independently certify pilot evidence**
and does not replace the Rust runner's complete config validation. Pass the same
private stamp YAML selected for deployment using `--stamp-config "$STAMP_CONFIG"`.
The binder checks that its stamp ID
matches the generated baseline before using its pool limits; when omitted, it uses
`infra/azure/stamp.yaml` and still requires that stamp to match. Never copy or
overwrite a private stamp file just to satisfy this check.

Before any authorized execution, archive hashes of the five configs, source,
runtime, images and overlay beside the pilot decision. Compare E4/E5 to E3:
only `clients_per_table` / `dfe_worker_concurrency`, respectively, may differ.
Do not change rows, DMV concurrency, pod resources or windows between runs.
Launch each config separately with the existing command:
`npx nx run arrowcol-oat:cli -- --mode aks --stamp-config "$STAMP_CONFIG" --config out/arrowcol-oat/e1-e5-approved/E1.config.json`
(substitute E2-E5). **Launching is a live mutating operation and requires
separate authorization; preparation does not grant it.** Each launch retains
the runner's fresh cell identity/scoped namespace and cleanup behavior; the
common result/mirror locations intentionally aggregate the comparison.

Retain launched run IDs and exact expected owned-server Git SHA with each
Parquet/verdict receipt. QAP completion is observation evidence, not ADX
row-count proof. Keep failures and cleanup receipts; do not rewrite unknown
historical provenance or claim five experiments ran from these inputs.
