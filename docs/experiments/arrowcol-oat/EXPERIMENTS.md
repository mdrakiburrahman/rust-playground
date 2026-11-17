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

## Local launcher deadline and recovery

`--deadline-seconds <1..600>` is opt-in and valid only for a local run with `--no-viewer`; omitting it preserves the existing launcher behavior. The monotonic deadline begins when `launch.ts` starts and includes setup, server build, the run, report processing and owned-server shutdown. For example:

```sh
npx --no-install tsx projects/arrowcol-oat/launch.ts --mode local --config out/arrowcol-oat/oat-config.local.json --no-viewer --deadline-seconds 600
```

The launcher does not hard-kill itself or the OAT server. If the deadline expires while `POST /runs` is in flight, it waits for that request's existing 30-second bound; a late accepted response is recorded and only that exact UUID is canceled. A lost acceptance response is recorded as `UNKNOWN` with no fabricated ID, is never retried or searched for, and blocks the next deadline-enabled cell.

Each invocation atomically records its original monotonic deadline, run identity, overrun, cancellation attempts and cleanup state under `out/arrowcol-oat/deadline-receipts/`. Once the run ID is known, cancellation targets that ID and polls its status to terminal using the existing ten-minute cleanup allowance. A cancel request is retried only after a fresh status read confirms that same run is still active. On a later invocation, known-ID receipts are reconciled before a new cell; missing identity or unresolved cleanup remains `UNSETTLED` and blocks the cell. An overrun is always a failed launcher result, even if the run reaches `succeeded`.

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
| QAP catch-up | After client completion, sample read-only QAP metadata until every expected table is healthy, 100% complete and has zero pending files, or `qap_timeout_seconds` expires (strict integer 1–300 s; existing local quick 60 s unchanged). Local deadline-incomplete results retain raw completion/pending observations but publish derived Kusto/QAP metrics as null with an explanation, without changing Delta commit rates. AKS/default qualification remains strict. |
| Snapshot coverage | Exact run/table-bound Kusto row count and table version are compared with the hash-referenced stopped Delta manifest independently of acceleration. A match proves snapshot count/version coverage only; unique payload delivery, per-row queryability and delivery latency remain UNKNOWN. |
| Maintenance outcomes | The retained process-log baseline precedes client admission and covers warm-up, measurement and drain. Typed `stale_plan` records are retries, not operation failures; per-operation retry/error counts and raw exit codes remain separate. Exit 1 cannot qualify even after retries recover. Success requires typed `report.outcome` and `report.interrupted`; missing/malformed completion or error fields remain UNKNOWN. Protected-partition backlog is a separate measured debt vector, never added twice to eligible debt or zero-filled when absent. |
| Live overload ceiling | Guard defaults on: stop active clients at >=90% of a readback-confirmed hard cap, or after two complete 30-second zero-ingestion-commit windows with outstanding work, during warm-up or measurement. Continue normal drain/maintenance/custody-safe cleanup. `evidence_json.overload_stop.classification=above_ceiling` is the ladder/step stop signal; retain the structured reason and do not advance to higher load. Unknown protection instead remains unavailable/nonqualifying, not a measured ceiling. |
| Guard prerequisites | The memory-only guard currently requires exactly one run-owned collector, private cgroup-v2 accounting with a finite container memory cap, matching collector RSS/buffer cap readback and complete ingestion-only telemetry. No WAL/Quiver enablement is required. Missing/mismatched readback blocks admission; incomplete live coverage blocks qualification. Multi-collector comparisons are unavailable under this guard, not implicitly protected. See [guard settings and accounting](TUNABLES.md#in-flight-overload-guard). |

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
