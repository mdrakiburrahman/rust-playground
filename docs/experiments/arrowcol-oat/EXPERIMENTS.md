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

When `clients_per_table_sweep` is set, the Data Flow verdict requires one result per table for every declared replica, DFE concurrency, and client-count combination. The rows for a combination must share its cell run UUID, and different combinations must have distinct UUIDs. Other tunables are fixed within one orchestration.

The `scale_out_single_table` profile reports ranked commit-window throughput only when Delta-log reconciliation is `matched`. Missing, unavailable, or mismatched reconciliation produces a failed, unranked result; raw commit-window and client observations remain diagnostic. Downstream QAP availability is reported separately.

## Collector shutdown drain

For a workload that continues publishing after SIGTERM, set
`tunables.collector.shutdown_drain_timeout_seconds` (for example `240`) and
`collector_stop_grace_seconds` to at least twice that value plus 60 (for
example `540`). Both local Compose and AKS use the same guard: the DFE drains
producer pipelines before system observability, and the outer stop grace
also reserves time for terminal routing writes. Omitted drain budgets stay at
60 seconds. Compare the effective engine setting and the actual drain outcome
in the cell evidence; configuring a larger budget is not proof of a clean drain.

## Local launcher deadline and recovery

`--deadline-seconds <1..600>` is opt-in and valid only for a local run with `--no-viewer`; omitting it preserves the existing launcher behavior. Omit it for local `measure_seconds` windows that cannot finish within 600 seconds. The monotonic deadline begins when `launch.ts` starts and includes setup, server build, the run, report processing and owned-server shutdown. For example:

```sh
npx --no-install tsx projects/arrowcol-oat/launch.ts --mode local --config out/arrowcol-oat/oat-config.local.json --no-viewer --deadline-seconds 600
```

The launcher does not hard-kill itself or the OAT server. If the deadline expires while `POST /runs` is in flight, it waits for that request's existing 30-second bound; a late accepted response is recorded and only that exact UUID is canceled. A lost acceptance response is recorded as `UNKNOWN` with no fabricated ID, is never retried or searched for, and blocks the next deadline-enabled cell.

Every benchmark invocation durably records `submitted_config_body` (the exact post-mode-rewrite string), its lowercase SHA-256 `submitted_config_sha256`, and the original file's `source_config_sha256` under `out/arrowcol-oat/deadline-receipts/<invocation_id>.json` before POST. Omitting the deadline leaves its timer/deadline fields absent or null; it does not omit the binding receipt. POST and GET must attest the same raw submitted-byte hash. Missing/mismatched attestation remains UNKNOWN, including after safe cleanup.

Submit AKS benchmarks from the primary checkout that owns the existing receipt ledger, or from a linked worktree of that checkout, at the reviewed revision. A linked worktree builds and reports its own revision and uses its own setup state, but every AKS receipt read and write, unsettled-run check and `--recover-failed-aks` disposition uses the primary checkout's existing `out/arrowcol-oat/deadline-receipts` ledger; its own local ledger must also be settled. From a linked worktree both ledgers are checked read-only: an unsettled receipt in either one, including a local run, refuses the launch without persisting an overrun, cancelling that run or reconciling its receipt. The launcher refuses the submission unless Git and the file system prove the worktree is registered with a non-bare primary checkout owned by the same user and that ledger already exists as a real directory. Do not select another clone, copied ledger or symlinked directory to bypass an unsettled ledger.

Every AKS launch holds an exclusive `flock` on `out/arrowcol-oat/deadline-receipts.aks-submitter.lock` beside the canonical ledger from before its ledger scan until the launcher exits, so two launchers that share the ledger cannot both pass the scan before either publishes a receipt. The lock helper runs in its own session, so a terminal interrupt does not release it while the launcher lives, and the launcher rechecks the lock before it publishes creation intent and again before `POST /runs`. A launcher that cannot take or keep the lock, or finds `flock` missing, refuses to start. Launchers using other checkouts or hosts are not serialized by this lock; keep one timed AKS experiment per stamp.

### One-time settled operator handover

For an existing stamp whose operator must move hosts, **retire the previous operators first** and independently review one complete history bundle. This is an explicit, exclusive-campaign operating procedure, **not an IAM fence or cross-host service**. A local lock cannot revoke another operator's cloud access. Do not copy a live ledger, invent missing producer metadata, omit failed attempts, or treat an empty replacement ledger as continuity.

| Step | Required evidence or command |
|---|---|
| Read-only source export | Under the independently cleared source lease, census **every** canonical and linked ledger entry twice; preserve original bytes privately as base64 and their SHA-256. Stop on any changed inventory, sidecar/unsupported entry, active/unknown admission, incomplete terminal proof or wrong stamp. Never restore receipts to export them: restoration can change expired deadline fields. |
| Source retirement | Preserve the owned-launcher/job census, release receipts for **all** predecessor leases including the export lease, and proof that obsolete renewals are gone. Do not stop pre-existing processes or switch the primary checkout. |
| Destination | An independently cleared, owned clone with an empty runtime ledger. Bind its `hostname`, canonical `workspace`, `ledger`, `git_common_dir`, Unix `uid`, `lease_id` and `client_id`. A linked worktree of the held primary is not a handover destination. |
| Fresh approval | Refresh retirement and existing-stamp safe-idle observations within ten minutes of import, bind their original evidence bytes/hashes, and independently approve the **entire bundle SHA-256**. Refresh external premises again before a live run; an old archive is history, not fresh cloud custody. |
| Import once | `npx --no-install tsx projects/arrowcol-oat/launch.ts --import-aks-history /private/handover.bundle REVIEWED_SHA256` |
| Admission | Add `--aks-history-sha256 REVIEWED_SHA256` to the normal README-hydrated `--mode aks` launch. All normal source/image, preflight, deadline, cleanup and single-submitter checks still apply. |

Bundle schema (`version: 1`, `kind: "aks-settled-history"`):

| Field | Contract |
|---|---|
| `stamp`, `subscription_id` | Exact existing stamp and subscription UUID. A receipt that originally lacks a subscription retains those exact bytes; the reviewed envelope and safe-idle witness supply that binding. |
| `source` | `host`, `operator`, `observed_at_unix_ms`, all `lease_ids`, `export_lease_id`, complete absolute `ledgers`, `files`, `before_sha256`, `after_sha256`. Each file is `{ledger,name,sha256,content_base64}`; name must be its original invocation UUID plus `.json`. For test-generated no-submission receipts, retain their original bytes and add a `provenance` annotation to the file entry identifying the test source and original gate job/time window. An annotation is context, not proof of settlement. |
| Inventory hashes | SHA-256 of compact `JSON.stringify` over `{ledger,name,sha256}` entries, sorted by `ledger + "/" + name` with `localeCompare(..., "en")`. Both census hashes and `approval.source_inventory_sha256` must equal it. Full-file hashes bind original bytes, not normalized JSON. The reviewer must check completeness against the actual source census; an internally consistent supplied list alone cannot prove it. |
| `destination` | Exact identity fields listed above. Import checks the local canonical Git and filesystem owner identity, not merely a caller-supplied path. |
| `approval` | `observed_at_unix_ms`, `exclusive_campaign_ownership: true`, `source_inventory_sha256`, `retirement`, `safe_idle`. This is a digest-pinned operator attestation, not a cryptographic signature or automatic verification of hub/cloud responses. |
| `approval.retirement` | `observed_at_unix_ms`, zero `owned_launchers`, `owned_jobs`, `renewals`; `leases` containing `{lease_id,client_id,state:"released",released_at_unix_ms}` for all source leases; `evidence`. Immediately before releasing the export lease, independently enumerate **all** source directories again. Supply `final_census:{observed_at_unix_ms,ledgers,inventory,sha256}` with the same directory list, including empty directories, the sorted `{ledger,name,sha256}` entries and their inventory digest. A new, removed or changed entry, even if settled, invalidates the export; take a new double census and review it rather than omitting the change. Release must follow final census within ten minutes and precede the retirement observation. |
| `approval.safe_idle` | `observed_at_unix_ms`, matching `stamp`/`subscription_id`, zero `run_namespaces`/`run_fics`, complete `benchmark_pools:[{name,count:0,autoscaling,autoscale_min:0,autoscale_max}]`, `evidence`. Preserve the existing finite autoscale ceiling and enabled state, rather than changing the stamp to fit a fixture. Observe only authorized stamp resources; preserve Kusto and unrelated resources. |
| Evidence | Each `evidence` is `{content_base64,sha256}` preserving the reviewed original readback bytes. Do not include credentials. Keep the entire private bundle out of Git. |

Only confirmed no-submission attempts (no run/server identity or cancellation) and bound terminal AKS receipts with original terminal observation and cleanup confirmation are supported. A failed terminal receipt additionally requires `aks_cleanup_confirmed: true`. Unsupported legacy dispositions require a separately reviewed path; this importer never executes sidecars or recovery.

Import holds the destination's normal submitter lock, refuses nonempty ledgers and existing imports, durably claims the expected digest in `deadline-receipts.history-required`, then atomically installs the private `deadline-receipts.settled-history` archive without changing receipt bytes, original identities, outcomes or deadlines. Both files are outside the runtime ledger; history never becomes runnable work. Admission verifies the required digest and archive and rejects historical invocation/run IDs appearing in runtime receipts. It refuses missing, changed or wrong-destination history; installed or partially imported history cannot be used without its explicit digest. Normal local mode is unchanged.

If import is interrupted or its result is ambiguous, **do not retry blindly**. Read back the required-digest marker, exact archive digest and destination under the owning lease. A partial temporary file grants no continuity; the required-digest marker prevents a partial import from becoming a blank-ledger admission, and a completed archive remains installed across process death. Preserve evidence and resolve the outcome before proceeding. Never delete either marker to bypass this refusal. No import command calls a cloud API, cancels a run, extends an old deadline or adopts existing resources.

The receipt also records the original deadline, accepted canonical run identity, overrun, cancellation intent and cleanup state. For an owned local server, it binds `server_pid`, `server_start_ticks` and `server_executable` before the optional observer callback. After a validated terminal HTTP response, it stores the complete response and observation time in `terminal_observation` before shutdown. A consumer must match the receipt path, immutable run/server/config identity and original deadline; only a `succeeded` response with an in-window `finished_at_unix_ms` can authorize normal measurement. `failed` or `cancelled` proves termination only, never success. Legacy receipts remain readable for existing reconciliation, but cannot authenticate this terminal fallback. Cancellation is a durably recorded **once-only POST** for that exact run; an inconclusive response permits only bounded GET reconciliation, never another cancellation POST. Its first ten-minute cleanup deadline is persisted and reused on recovery, not renewed. Every response must name the exact accepted run. Only `succeeded` or `cancelled` confirms cleanup; `failed`, missing identity or binding uncertainty remains `UNSETTLED` and blocks another invocation. An overrun is always a failed launcher result, including expiry during scoring, final receipt persistence or owned-server shutdown. An unresolved run keeps its owned recovery server.

The existing local marker at `<compose_state_root>/.arrowcol-oat-owner-<serverPid>.json` preserves `runId`, `ownerPid`, `ownerStartTicks`, `serverPid`, `serverStartTicks`, `serverUrl`, `serverExecutable` and the same three binding fields. The optional programmatic `main` observer receives a frozen `{invocationId, runId, serverUrl, serverPid, serverStartTicks, serverExecutable}` snapshot and the receipt path, directly from launcher-owned memory, only after the actual accepted run's receipt and marker are durable and its attestation matches. Immediately after marker persistence, emission rechecks the current in-memory accepted run/server identity, active non-error status, binding and absence of cancellation, expiry or unsettled cleanup. Initial terminal, external-server and unknown/mismatched runs emit nothing; ineligible owned runs produce a supplementary diagnostic. Observer throws/rejections are supplementary diagnostics only; the observer is not awaited and cannot authorize admission, scoring or cleanup. No production supervisor CLI is enabled by this seam.

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
| Collector completion | All clients completed their uploads and measurement interval. Result `tunables_json.collector_completion` preserves each client's actual best-effort/durable admission counts, explicit `diagnostic_discard_streams` count (never admitted) and completion timestamp from completed upload calls across preflight, warm-up and measurement. Partial or cancelled upload calls are not counted; `admission_accounting_complete=false` explicitly marks incomplete accounting when warm-up cancels an upload or diagnostic discards bypass admission; a report claiming complete accounting with diagnostic discards, or omitting `diagnostic_discard_streams`, is rejected. Missing reports are explicitly unavailable. |
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
