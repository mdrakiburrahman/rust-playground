# Isolated ArrowCol OAT runtime

| Component | Contract |
| --- | --- |
| `arrowcol` Deployment / `dataflow` container | Integrated Flight receiver → Delta exporter. One or two replicas, each with `--num-cores 1` or `2`; constant CPU request/limit of 2. |
| `arrowcol-router:50050` | ClusterIP only, h2c to private Flight `32010`, mandatory trusted-edge HMAC, SQL-serialized distinct table owners. Operator access by port-forward only. |
| `arrowcol-catalog:8817` | Persistent 16Gi state PVC, same stamped dataflow warehouse as DFE, `--ingestion-layout`; catalog Flight SQL DDL requires signed internal identity by default. |
| `arrowcol-sql:1433` | Persistent 32Gi PVC; SQL2022 CU27, routing/fencing only. SA bootstrap then checksum-verified migrations through V0011; separate SA/app/migrator secrets. |
| Namespace and scheduling | Every AKS cell gets a namespace derived from the configured prefix plus its compact run UUID. The runtime selects `arrow-collector/role=arrowcol` nodes (amd64) and tolerates that role's `NoSchedule` taint. |
| Identity | `aks setup` prepares the shared `arrow-collector` ServiceAccount and workload identity. OAT cells use run-scoped namespaces and clean up only their owned namespace/FIC; no Azure/GitHub CLI credential mounts. |

OAT router configurations explicitly select `auth.authenticator: dev_only_allow_all`. Missing, legacy `allow_all` and unknown modes fail startup. Every non-public request still needs a valid edge HMAC; the shared development principal is not production caller authorization. Rebuild router and signing-client images together because `x-arrowcol-oat-run-id` is now signed, including its absence.

Catalog Flight SQL and CES requests require signed internal identity by default. Each assertion binds the RPC method and payload digest, carries a one-time nonce, and expires within 30 seconds; the replay cache is bounded at 65,536 requests per catalog replica and rejects new requests when full. AKS catalog and dataflow pods mount the `internal-hmac` key from `arrowcol-router-auth` at `/run/auth/internal-hmac`; generated direct OAT client Jobs mount the same key at `/run/secrets/internal-hmac` and set `LOADTEST_INTERNAL_SECRET_FILE` to that path. `--ddl-auth-mode dev_only_allow_unsigned` bypasses catalog RPC identity checks and is for development only; do not use it in AKS. The separate data-plane Flight SQL service is unchanged. TLS/mTLS is deferred, no NetworkPolicy enforcement is assumed, and no QAP host allowlist is added.

DFE explicitly enables generated ingestion partitions with
`ARROWCOL_PARTITION_GRANULARITY=hour`. Existing committed partition layout and
table properties remain authoritative; without this writer mode a logical
ingress batch lacks the physical `__arrowcol_ingestion` partition value.

Runtime pods disable Kubernetes service-link variables: the generated
`ARROWCOL_SQL_PORT=tcp://...` would otherwise override the numeric SQL setting.
The pinned SQL executable requires `NET_BIND_SERVICE` in its capability set;
only that container receives it, while remaining non-root with privilege
escalation disabled. Other containers continue to drop all capabilities.

Collector CPU pressure sampling is disabled by default and preserves the existing zero gauge. With `ARROWCOL_COLLECTOR_CPU_PRESSURE=process`, each pressure-publication tick samples process CPU-time against wall time multiplied by the effective cgroup v2 quota or available parallelism; the first tick is zero and readings clamp at 10000 basis points. OAT evidence reports `not_sampled` with a null gauge when sampling is off. CPU-gauge exclusion only affects router admission when process sampling is enabled.

## Prerequisites and authorization

For AKS prerequisites, continue below. Local runs use the following independent path.

## Local smoke and stopped Delta snapshots

| Setting / boundary | Contract |
| --- | --- |
| `platform.local.bind_host` | Optional Docker host IP for the SQL/catalog/router/Kusto/Grafana ephemeral port bindings; default `127.0.0.1`. For a bridged devcontainer, explicitly choose the daemon's reachable private bridge address. No gateway is guessed. |
| `platform.local.endpoint_host` | Optional bare reachable DNS/IP host (IPv6 accepted), without scheme/port/credentials. Overrides only discovered host, never its dynamic port. Otherwise the actual binding/active Docker context determines the endpoint; remote daemon loopback and ambiguous wildcard bindings fail closed. |
| Docker filesystem | Unix runner and daemon must see the **same absolute generated-state path**. A no-network, read-only nonce bind probe verifies actual bytes before SQL starts. Remote path remapping is unsupported, not silently approximated. |
| Local inputs | New per-cell project/network/SQL and Kusto volumes, private app/migrator/HMAC files; no retained SQL volume or Azure environment file is used. Compose auto `.env` loading is disabled. Images/build cache and host resources remain shared. |
| `platform.local.snapshot_root` | Optional absolute directory, outside generated state and not its ancestor. Opt-in export; default `null` retains automatic cleanup without exporting. Never point it at public website storage: snapshots contain real payloads. |
| Stop/export/cleanup | Stop owned ingress first, then stop and wait for `delta-maintenance`, record its sanitized outcome, and only then SIGTERM collectors; stop catalog and dependencies afterward. `collector_stop_grace_seconds` (30..=3600, default 300) is applied to local collectors and AKS `terminationGracePeriodSeconds` and is read back before measurement. Other local stop phases retain a 300 s ceiling; the overall local deadline includes collector grace, all three other phase ceilings, and 120 s margin. Each collector records SIGTERM-to-exit seconds, committed rows/commits completed after SIGTERM, Quiver bundles drained/force-dropped, log completeness, and exit code. A zero drained count derived only from matched shutdown start/complete records is labelled `inferred`, not `observed`. Maintenance uses the retained pre-client baseline through drain: typed `stale_plan` retries and sanitized operation failures are separate. Raw exit 1 is preserved but never alone establishes `maintenance_operation_failure`; missing records or an unproved exit cause remain UNKNOWN. Exit 0 and intentional SIGTERM/143 for a previously running writer remain accepted; exit 2, 137, OOM and every other nonzero exit still fail. An AKS terminated state missed before pod deletion yields `exit_outcome.status=unavailable` and a null exit code, never an assumed success. Stop failure retains owned resources for recovery rather than attempting destructive cleanup. In snapshot mode, copy/verify/sync snapshot and provenance before owned stack/volume/state deletion. |
| Local screen evidence | After stopped-snapshot export, local OAT reads `<snapshot_root>/<run_id>/<table>/table/_delta_log`, selects collector receipts by `finished_at` in `[start,end)`, matches window totals and each receipted version's data-change Adds, and checks unreceipted in-span versions. Only a reconciled result populates `evidence_json.durable_commits.delta_log_reconciliation.csv` and `screen_v1`; mismatches fail the row, while unavailable evidence leaves screen metrics null with a reason. |
| Commit-rate resolution | Commit windows and reconciled local `screen_v1` report `window_commit_count`, `max_commit_rows`, `rate_resolution_rows_per_second` (largest commit divided by window seconds), and `slope_rows_per_second` (least-squares cumulative committed rows against finish time). No commits leaves the maximum, resolution and slope null; fewer than three commits or identical finish times leaves slope null. Unavailable evidence has all four fields null. The slope orders equal finish times by Delta version. |
| Snapshot meaning | Last committed stopped data, **not** proof every admitted row was flushed. No measurement, pinning, unknown-count or QAP acceptance gate is relaxed. |

Client quiescence is capped at 86,400,000 ms and must be at least the request
timeout and strictly less than collector stop grace. The client stop grace is
the quiesce timeout rounded up to seconds plus 10 seconds. Separately, campaign
cleanup bounds its collector drain step to 120 seconds; increasing the pod's
termination grace does not increase that custody deadline.

Local teardown includes the maintenance Compose profile only when maintenance
was enabled for the cell; other profiles remain excluded.
After a successful measurement, `local_teardown` identifies the cell and its
completed schedule while cleanup is running. It is not terminal success or proof
of drain.

Collector routing-store SQL remains unpooled; unlike the router's per-request
authoritative OAT pin reads, its heartbeat/pressure operations are periodic.
Collector pool tunables are a follow-up, not part of the router pooling evidence.
Router SQL pooling can reduce login overhead, but pool acquisition and SQL
latency can still delay requests. A larger future-skew allowance also accepts
more genuine clock skew, while a longer background refresh interval risks
staler cache and readiness data; neither removes per-request authority reads.
See [router tunables](TUNABLES.md#router-sql-pooling-and-log-volume).

AKS starts SQL and completes migration before applying workload ConfigMaps,
Services, NetworkPolicies and the catalog Deployment. Catalog rollout must be
ready before collector/router Deployments are applied; maintenance remains at
zero replicas until table pins are established. Rollout permits startup crashes
below four restarts, fails immediately on image-pull/container-config errors,
and is bounded by `aks_rollout_timeout_seconds`, including stalled API calls.
Failures capture bounded current/previous logs and descriptions for every
workload role, namespace pods/services/endpoints and events before cleanup.
Scale-out keeps exactly one collector/router and initializes the fresh canonical
physical table in pin-init before maintenance readback and client admission.
An independent finite client-image registration Job then sends the production
DMV `CREATE TABLE IF NOT EXISTS` through the router with the client edge key.
Signed catalog snapshot readback must prove exactly that canonical schema at
the run-scoped location. Only the existing client signing keys are mounted:
no migrator password or storage identity. The controller verifies the receipt,
foreground-deletes the Job and confirms its UID-selected Pods are absent before
starting maintenance; the effective-policy/stop-budget readback gate is unchanged.
Deletion is preconditioned on the recorded Job UID. Logs use its controller-UID
selector, with Pod ownership and identity checked before and after the read.
One controller-side 180-second deadline includes resource creation, all RPCs and
cleanup; late completion fails admission. The registration ConfigMap remains
until normal run-namespace cleanup.
Pin-init uses the collector's Azure ownership environment from the validated
run runtime config; missing subscription/stamp or a mismatched Delta account
fails before Job creation. Its read-only root retains a bounded writable `/tmp`.
Failed initialization remains failed. Cleanup can confirm initializer termination
without a success receipt only with a recorded Job UID, `Failed=True`,
absent-or-integer-zero `active`/`ready`/`terminating`, no uncounted terminated
pods, and terminal kubelet states with container IDs for every
init/application container in every same-controller pod. The distinct
`initializer_terminated_without_receipt` evidence never claims table initialization;
timeouts, missing kubelet statuses and ambiguous/API failures remain unconfirmed.
It does not send a collector startup-table selector: the strict exporter
configuration supports `authority_timeout`, not table selection. Requested
exporter tunables survive initial injection and reconfiguration; malformed,
duplicate or conflicting overrides fail admission.
Teardown logs per-Deployment stop, namespace deletion/absence and QAP/FIC cleanup
durations; a failed cell's total elapsed time alone cannot identify its slow step.

Functional smoke only (not E1-E5 or Kustainer performance evidence), from the
repository root in the devcontainer. Rust, Node workspace dependencies and Docker
Compose are required; image pulls/builds can require registry access. On a
bridged runner first set `OAT_DOCKER_BIND_HOST` and `OAT_DOCKER_ENDPOINT_HOST` to the
verified private daemon binding/reachable host; omit them for an ordinary
same-host runner. No Azure credentials or publication are required:

```bash
mkdir -p out
export OAT_SMOKE_ROOT="$(mktemp -d "$PWD/out/arrowcol-oat-smoke-XXXXXXXX")"
node --input-type=module -e '
import fs from "node:fs";
import { pathToFileURL } from "node:url";
const root = process.env.OAT_SMOKE_ROOT;
const config = JSON.parse(fs.readFileSync("docs/experiments/arrowcol-oat/oat-config.example.json", "utf8"));
Object.assign(config.platform, {
  compose_project: "oat-smoke", compose_state_root: `${root}/state`,
  local: { bind_host: process.env.OAT_DOCKER_BIND_HOST ?? null,
    endpoint_host: process.env.OAT_DOCKER_ENDPOINT_HOST ?? null, snapshot_root: `${root}/snapshots`,
    collector_memory_limit_bytes: config.platform.local.collector_memory_limit_bytes }
});
Object.assign(config, {
  tables: ["sqlserver_active_sessions"], replica_counts: [1], dfe_worker_concurrency: [1],
  clients_per_table: 1, dmv_concurrency: 1, rows_per_table: 1000,
  warmup_seconds: 0, measure_seconds: 1, pilot_mode: false, qap_timeout_seconds: 60,
  stamp: null, qap_database: null,
  result_url: pathToFileURL(`${root}/results.parquet`).href,
  website_url: pathToFileURL(`${root}/site/results.parquet`).href
});
fs.writeFileSync(`${root}/config.json`, JSON.stringify(config), { flag: "wx" });
'
npx --no-install arrowcol-oat --config "$OAT_SMOKE_ROOT/config.json" --mode local 2>&1 | tee "$OAT_SMOKE_ROOT/run.log"
```

The overload guard is enabled by default and needs a finite collector cap, so the
recipe keeps the example's `platform.local.collector_memory_limit_bytes` (4 GiB);
without it the run fails before stack services start. `run.log` keeps the full
console transcript beside the results.

The one-second measurement still follows image/stack startup, trusted immutable
pin initialization, whole-fleet preflight/QAP setup and the schedule lead.
The viewer stays open after completion until Ctrl+C. A successful smoke
must have real positive committed rows in the exported dataset, not merely an
upload acknowledgement. Parquet summary schema v2 remains unchanged: missing QAP
measurements stay null; a completed pinning violation is published as failed
pinning, while preparation/client/cleanup errors can abort before Parquet exists.

| Scheduling setting / evidence | Contract |
| --- | --- |
| Top-level `schedule_lead_seconds` | Integer `1..=1800`, default `180` when omitted. This is the common lead from schedule creation to warm-up start and the **same original absolute timeout** for arming and full-fleet acknowledgment. Short leads are unqualified controls, not guaranteed deliverable schedules. |
| Client compatibility | `client_ready_timeout_seconds + qap_timeout_seconds + schedule_lead_seconds` must fit the pinned `arrowcol-dmv --schedule-timeout-secs` ceiling of `2160`; invalid totals fail before effects. Client Job lifetime, minimum pin TTL and planned cell duration use that same selected lead with their unchanged margins. `run_pin_ttl_ms` must still cover the resulting minimum. |
| Requested/effective proof | `tunables_json.schedule_lead_seconds` records the selected input (including its omitted-field default). `evidence_json.client_schedule` records requested/effective lead, actual assigned absolute phase boundaries, full-fleet acknowledgment time and derived lifetime budgets. It remains null without verified acknowledgment; missing/late delivery fails, is cancelled and is never replayed or relabelled ready. |
| Scope | No existing C3 JSON/hash is changed. Reducing the lead does not shorten warm-up/measurement, maintenance, reconciliation, collector drain or the agreed stability/600-second pilot protocol, and does not qualify a shorter lead for that protocol. |

| Retained output | Evidence / limits |
| --- | --- |
| `snapshots/<cell-run-UUID>/stopped.json` and `complete.json` | Stopped container identities/outcomes and completion manifest with per-table manifest SHA256. An absent completion marker means incomplete export, not permission to clean source data. |
| `snapshots/<cell-run-UUID>/sqlserver_active_sessions/table/` | Unchanged genuine Delta JSON log and payload bytes; only the empty writer lock file is omitted. Checkpoint Parquet sidecars and `_last_checkpoint` are preserved byte-for-byte; checkpoints do not bridge gaps in the required contiguous JSON history from version zero. Symlinks/nonregular files, deletion vectors, external paths, history gaps or missing historical Add payloads are rejected. |
| Sibling `manifest.json` | Version 1 `schema_version`, `run_id`, `table`, `latestVersion`, positive `rowCount`, original `schemaString`, `files[{path,size,sha256}]`, stopped `writers` and semantics. Paths are relative to `table/`; active rows are independently decoded and checked against Parquet footer counts, never inferred from Add statistics. |
| `results.parquet`, `site/results.parquet` | Local summary/mirror outside generated state. Not the Delta dataset and not a provider qualification receipt. |
| `out/arrowcol-oat/admission-diagnostics/<run-id>/<cell-id>.json` | Private controller-side receipt for successful or failed cells that captured admission evidence. It retains the selected collector identity, a bounded allowlisted excerpt and digest of the last received pressure body, and the first rejected startup values when available. A different post-client mismatch is recorded separately. It survives workload cleanup and does not qualify a cell or prove that a failed identity fence passed. |

For a separately authorized later provider probe, select exactly one completed
table directory and bind its raw manifest hash, run ID and table identity. Replay
the complete version-zero-through-latest history and verify every inventoried
byte plus all historical/active Add references before copying. Do not fabricate
schemas, edit logs, race-copy live writers, or treat this export as Azure
publication authority.

## AKS prerequisites and authorization

Create a private stamp file from `infra/azure/stamp.example.yaml`, fill in the authorized subscription and non-overlapping network ranges, and sign in with `az login --use-device-code`. Keep the same private path for every command:

```bash
STAMP_CONFIG=infra/azure/stamp.yaml
# First deployment only; skip this for an existing stamp.
npx --no-install arrowcol-oat aks deploy --stamp-config "$STAMP_CONFIG"
```

Deployment defaults to automatic networking: Azure first, with one Cilium retry only after a proven AKS-creation failure. Preflight or ARM preview failures do not trigger fallback. Pass `--network azure` or `--network cilium` to pin the choice; a pinned deployment has no network fallback. Azure's default dataplane sets `networkPolicy` to `none`, so declared Kubernetes NetworkPolicies are **not enforced**. In-cluster workloads can reach the passwordless DFE Flight port, although there is no public ingress. Pin Cilium when policy enforcement is required; the automatic fallback is not a security-policy upgrade.

For a new or existing deployed stamp, run setup, then the OAT benchmark:

```bash
npx --no-install arrowcol-oat aks setup --stamp-config "$STAMP_CONFIG"
npx --no-install arrowcol-oat --mode aks --stamp-config "$STAMP_CONFIG" \
  --config out/arrowcol-oat/oat-config.aks.json
```

If multiple live stamps are present in the selected resource group, also pass `--stamp <five-digit-stamp>` to setup. Setup prepares revision-pinned images and private inputs under ignored `out/arrowcol-oat` (including `runtime.json`, `oat-config.aks.json`, `bootstrap.env`, `aks.env`, secrets and the publisher identity manifest), then runs preflight. Continue only when setup reports READY. Correct a failed gate and rerun setup; do not skip preflight or copy generated files between stamps.

The template declares one `arrow-collector` UAMI in the primary resource group. That is not a count of all identities: required Azure platform extensions can create identities in the AKS-managed resource group. Inventory those separately; see [identity qualification](../../operations/AKS_QAP.md#tenant-enforced-identities). The runtime identity needs Blob Data Contributor on the selected data container and checkpoints access for publisher fencing; setup establishes the host `arrow-collector` namespace/ServiceAccount federation. OAT cells use a unique namespace derived from the configured prefix plus their compact run UUID, select `arrow-collector/role=arrowcol` nodes, and clean only their owned namespace and run-scoped FIC. The final verdict checks that run-scoped namespaces and FICs are gone.

The generated QAP database is `experimentqap`, distinct from the main `arrowcol` database and from the OAT results destination. The operator also needs the selected cluster context, image-pull readiness, metrics API access, and write access to the configured `experiments` and root `$web` destinations. The fixture container is not the results container.

`aks destroy --stamp-config "$STAMP_CONFIG"` only prints a read-only inventory. Adding `--confirm arrow-collector-rg` deletes the entire `arrow-collector-rg` and its captured AKS-managed resource groups, including every stamp there; it is not per-run cleanup. OAT benchmark cleanup is automatic and run-scoped.

## Heterogeneous AKS fleet

The [full-fleet template](oat-config.aks.full-fleet.json) is a sizing input, not a
direct Rust API request. Start from the selected stamp's private single-table
scale-out OAT configuration, retain its pinned images and identity bindings, and copy the
template's `platform.fleet` block. Set `collector_pool` to the explicitly selected
largest pool; the legacy collector pool is not implicitly selected or relabelled.

| Setting | Resolution |
| --- | --- |
| `collector.*` | `auto` derives DFE, runtime, blocking and encoder/writer CPU workers from the selected Ready node's available whole cores, respecting existing consumer maxima. Positive numeric overrides remain supported. |
| `client.*` | `auto` resolves independently for every Ready client node: worker/runtime threads, connection/stream counts, rows and corpus/memory budgets. |
| Client concurrency | Remove top-level `dmv_concurrency` and `tunables.client.streams_per_worker` from the base. Fleet sizing uses `streams_per_worker` per worker; alternatively set it to `0` and supply `client.dmv_concurrency` as `auto` or 1–64. Both omitted at the resolved client level fall back to 4 streams per worker. |
| Concurrency evidence | Per-client `concurrency_readback` verifies workers, streams per worker, source and their configured slot product against the consumer. The resource evidence key `requested_concurrency_per_process` is a legacy alias of `requested_concurrency_per_worker`, not a process-wide slot count. |
| Pod budget | Observed node allocatable minus existing nonterminal pod requests, including DaemonSets, minus reserved runtime/support-service room. Collector reservation includes SQL, router, catalog, maintenance and setup jobs; clients retain additional headroom. |
| Placement | One integrated collector pod plus existing support topology on the explicit collector pool/node. One DMV pod per client node, pinned by pool and hostname with the role selector/taint toleration retained. Only observed `amd64` nodes are admitted for the existing images. |
| Evidence | Per-client and per-pool completed/admitted rows/s, node/pool/SKU allocation and collector/client allocatable CPU/memory totals. Client completion rates are neither attempted/offered rates nor collector-committed rates. The ranking remains exactly `ranking_metrics.committed_rows_per_second`, reconciled from collector commits before shutdown. Missing ranking evidence stays null. |

For offline reproduction from a fleet orchestration observation, prepare a
single numeric cell without performing cloud operations:

```bash
npx --no-install tsx projects/arrowcol-oat/prepare-experiments.ts \
  --base out/arrowcol-oat/full-fleet-input.json \
  --fleet-observation out/arrowcol-oat/fleet-observation.json \
  --stamp-config "$STAMP_CONFIG" --out out/arrowcol-oat/resolved-fleet
```

The observation contains `nodes` (Ready-node identity, architecture, allocatable
CPU/memory and existing requested CPU/memory), sanitized `pools` outcomes and
`images: [{ "image": "<exact digest-pinned reference>", "architectures": ["amd64"] }]`
read from registry metadata for every runtime, client and maintenance image.
Preparation rejects unknown sizing fields, malformed sentinels, duplicate nodes,
incompatible architectures and insufficient budgets. It emits
`full-fleet.config.json`, with exactly one replica/core cell and no client-count
sweep. Do not reuse stale observations or submit unresolved `auto` fields directly
to the Rust API. No collector, router or DMV image rebuild is required.

Preparation requires image observations matching every pinned reference and
selected node architecture; an `amd64` node selector alone is not image proof.

### Custodied fleet execution and kill

| Requirement | Contract |
| --- | --- |
| Reviewed stamp | Complete System/User pool membership must match the reviewed stamp. Select one collector explicitly; all reviewed client pools participate. Nonselected User pools remain unowned at settled zero with their approved enabled autoscaler/minimum/maximum policy; unknown or missing pools prevent verified cleanup. |
| Private custody environment | `--env-file` supplies `ARM_SUBSCRIPTION_ID`, `CAMPAIGN_STORAGE_ACCOUNT` and `CAMPAIGN_STATE_CONTAINER`, exactly matching the selected stamp's subscription, existing account and checkpoints container. Existing operator credentials are reused. |
| Fresh observations | The production adapter observes ARM identities/policies, Ready nodes, requested resources and digest-bound Linux/amd64 image metadata. Preflight rechecks exact collector/client pool/SKU/node identities and remaining budgets. Supplied offline JSON is not production acceptance evidence. |
| Ownership | A versioned journal in the existing checkpoints container, journal-blob lease and legacy observe-only campaign claim exclude competing custody. Every selected pool must begin at zero with no physical nodes. Intent and ETag are persisted before conditional ARM dispatch. Recovery requires the retained owner UUID; implicit takeover is forbidden. Legacy campaign qualification is unchanged. |
| Scaling and idle | ARM operations serialize where required while Ready waits overlap. Run counts pin autoscaler minimum/maximum to the approved count. Cleanup restores zero, enabled autoscaling, minimum zero and each approved original maximum, then verifies zero selected nodes. Unknown outcomes stop rather than replay. |

Use the reviewed `sku-pools.ts` output's `userPools` in the private stamp,
and select its `collector.name` (with `collector.vmSize` matching that pool),
not the first `arrowcol` role. After copying the sizing block into the selected
stamp's private config, retain the owner UUID and run:

```bash
OWNER_ID="$(node -p "require('node:crypto').randomUUID()")"
COLLECTOR_POOL="$(node -p "JSON.parse(require('node:fs').readFileSync(process.argv[1], 'utf8')).collector.name" out/arrowcol-oat/reviewed-pools.json)"
npx --no-install arrowcol-oat aks fleet run \
  --env-file out/arrowcol-oat/custody.env --stamp-config "$STAMP_CONFIG" \
  --collector-pool "$COLLECTOR_POOL" --owner-id "$OWNER_ID" \
  --config out/arrowcol-oat/full-fleet-input.json --out "out/arrowcol-oat/fleet-$OWNER_ID"
```

Default scale/run/cleanup budgets are 7200/7200/3600 seconds and must fit six
hours together. The output retains observed fleet/image metadata, resolved
numeric input and a fleet result; native OAT publication remains the only source
of committed ranking evidence. A failed native launch retains active custody
even after successful pool teardown; use the recovery command to independently
verify idle and release it.

**Destructive recovery:** the following kill entry point resumes only the exact
durably owned selected user pools after process death. It removes their compute,
never system or unowned pools. It does not claim a graceful drain, a successful
benchmark, native run-resource cleanup or a resolved launcher disposition; retain
and reconcile those original receipts before another benchmark.

```bash
npx --no-install arrowcol-oat aks fleet kill \
  --env-file out/arrowcol-oat/custody.env --stamp-config "$STAMP_CONFIG" \
  --collector-pool "$COLLECTOR_POOL" --owner-id "$OWNER_ID"
```

## Build and rehearse

Run inside the repository devcontainer. These commands create local images only:

```bash
docker build -f projects/arrowcol-infra/docker/sqlserver/Dockerfile -t arrowcol-oat/sqlserver:local projects/arrowcol-infra
docker build -f projects/arrowcol-catalog-rust/Dockerfile -t arrowcol-oat/catalog:local .
docker build -f projects/arrowcol-dataflow-rust/Dockerfile -t arrowcol-oat/dataflow:local .
docker build -f projects/arrowcol-router-rust/Dockerfile -t arrowcol-oat/router:local .
docker build -f projects/arrowcol-oat/Dockerfile -t arrowcol-oat/client:local .
npx --no-install nx run arrowcol-oat:runtime-smoke
```

The smoke uses a unique internal Docker network, generated private files and
owned volumes; no cloud credentials or Kubernetes calls. It checks SQL migrations
twice, authenticated versus unsigned Flight, all four replica/core cases, one
Flight/admin listener per worker, physical Delta output, graceful DFE shutdown
and catalog restart. Its cleanup removes only its own containers/volumes/files.
It is functional evidence, **not an Azure performance benchmark**.

The client image is only for rehearsal. Build both host binaries for live OAT:

```bash
cargo build --locked --package collector-chain --bin arrowcol-dmv --package arrowcol-oat-rust --bin arrowcol-oat-rust
```

## Generated private files

The normal `aks setup --stamp-config "$STAMP_CONFIG"` path creates the selected stamp's private runtime configuration, OAT configuration, bootstrap environment, temporary AKS environment, secret files and publisher identity manifest under ignored `out/arrowcol-oat`. Use the generated `oat-config.aks.json` for the run; do not hand-copy passwords, tokens or the reset generation into JSON or move these files between stamps.

For manual renderer troubleshooting only, `runtime-config.example.json` may be copied into an ignored directory under `out/arrowcol-oat`. Its namespace is a prefix, not a shared workload namespace; the selected stamp supplies its data-container `deltaRoot`, workload identity and QAP endpoint/database (`experimentqap`). The normal CLI setup already creates and validates these inputs and the publisher ServiceAccount, so manual rendering and `kubectl apply` are not part of the standard OAT run.

## Normal OAT run and cleanup

The normal `--mode aks` path runs preflight, creates isolated per-cell namespaces, executes the benchmark, retrieves results, and validates owned cleanup. A namespace is `<configured-prefix>-<compact-run-UUID>` (at most 63 characters) and carries the `arrowcol-oat/run-id` ownership label. Cleanup targets only that run's namespace and federated credential; the final verdict fails if either remains. Do not apply a shared static workload namespace as a substitute.

```bash
npx --no-install arrowcol-oat --mode aks --stamp-config "$STAMP_CONFIG" \
  --config out/arrowcol-oat/oat-config.aks.json
```

The report's QAP endpoint/database are observation context, not proof of ADX row counts or ingestion parity. Stop on a failed setup or preflight gate rather than bypassing preflight or deleting run-owned resources manually.

## Advanced direct collector inspection

The commands in this section are for direct runtime diagnosis; the normal OAT benchmark is the `--mode aks` command above.

The generated AKS configuration supplies the selected stamp's QAP endpoint and
`experimentqap` database, workload identity, result destinations, and run-scoped
namespace prefix. The persistent DFE Deployment uses the
`arrow-collector/role=arrowcol` node selector and matching toleration; a user
pool must have that configured role. The QAP experiment's fixture database is
not the loadtest database `arrowcol`.

The OAT Rust runner selects its Kusto observation mode from the platform: the
Azure platform uses `ObservationMode::Azure`, while the local platform uses
`ObservationMode::Local`. No global `ARROWCOL_MODE` setting is required for this
selection. The Azure platform's `BoundedAzureCli` provider accepts
`AZURE_CLIENT_ID`, `AZURE_TENANT_ID` and `AZURE_FEDERATED_TOKEN_FILE`. Use the
existing `arrow-collector` ServiceAccount's private assertion file prepared by
the operator, with audience `api://AzureADTokenExchange`; keep it mode 0600 and
refresh before expiry.
Do not replace the operator's Azure CLI login used by kubelogin. First isolate
the **selected dedicated Canada stamp kubeconfig** from publisher environment
variables. Otherwise kubelogin combines its explicit subscription with the
publisher's `AZURE_TENANT_ID`, which Azure CLI rejects. The
`--disable-environment-override` flag must appear in the stored exec arguments;
`convert-kubeconfig` may leave an already-converted entry unchanged.

```bash
SUBSCRIPTION="$(node -p 'JSON.parse(require("fs").readFileSync("out/arrowcol-oat/runtime.json","utf8")).subscriptionId')"
KUBE_USER="$(kubectl config view --minify -o jsonpath='{.users[0].name}')"
kubectl config set-credentials "$KUBE_USER" \
  --exec-command=kubelogin --exec-api-version=client.authentication.k8s.io/v1beta1 \
  --exec-arg=get-token --exec-arg=--login --exec-arg=azurecli \
  --exec-arg=--server-id --exec-arg=6dae42f8-4368-4678-94ff-3960e28e3630 \
  --exec-arg=--subscription --exec-arg="$SUBSCRIPTION" \
  --exec-arg=--disable-environment-override
export AZURE_CLIENT_ID="$(node -p 'JSON.parse(require("fs").readFileSync("out/arrowcol-oat/runtime.json","utf8")).workloadIdentityClientId')"
export AZURE_TENANT_ID="$(az account show --subscription "$SUBSCRIPTION" --query tenantId -o tsv)"
export AZURE_FEDERATED_TOKEN_FILE=/absolute/path/to/operator-prepared-private-assertion
cargo run --locked --package arrowcol-oat-rust
```

In another terminal submit with
`npx --no-install tsx projects/arrowcol-oat/launch.ts <private-oat-config.json>`.
The server captures/restores each unique Deployment once, applies replicas/core
args together, waits for rollout and stale 30-second leases, and counts each pod
once. Metrics-server can lag a new rollout: sampling waits up to 120 seconds
for every current pod, rejects stale/partial samples, and fails rather than
substituting zero metrics. Pod UID and advertised pod IP come from the downward API.

Each selected table uses independent HMAC-signed connections. The OAT router
sets `distinct_table_owners=false`, so different tables retain stable ownership
while sharing collectors. This supports the 32-table matrix with 4 to 16
collectors; one HTTP/2 connection alone cannot exercise multiple SO_REUSEPORT
workers.
The benchmark router also limits each upstream H2 connection to one active
stream and uses zero idle timeout, preventing upstream multiplexing from
collapsing independent client connections onto one DFE worker. These settings
are fixed across the matrix and do not change the normal router defaults.
Keep client concurrency comfortably above the worker count (the example uses
16); SO_REUSEPORT distributes connections by kernel hashing, not round-robin.

In OAT mode, the router reads the authoritative SQL run-table pin for every
ingest request and again during revalidation; do not cache or weaken this check.
Therefore measured OAT router latency includes those SQL round trips, while the
production cached-lease routing path does not. `router.sql_pool_max_size` changes
connection reuse, not pin-read semantics. Per-cell `evidence_json.router_sql`
contains opened-connection and operation counters plus SQL operation latency
histogram deltas for the measurement window.
`--num-cores` is authoritative because the rendered pipeline has no resource
policy override. Flight success is **best-effort admission**, not publication.
QAP observations and client timing must not be presented as a durability receipt.
Only measured sanitized aggregate Parquet is published; no raw payloads or
credentials belong in the anonymously readable `$web` container.
