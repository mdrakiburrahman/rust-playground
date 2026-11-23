# Offline Realistic delivery equality

| Input/output | Contract |
| --- | --- |
| `snapshot.json` | `{ "table": "sqlserver_availability_replica_states", "rewrite_commit_count": 0, "manifest_paths": ["/run/completed-manifest.json"], "active_parquet_paths": ["/run/active-ingestion.parquet"] }`; paths must come from the trusted stopped-snapshot reconciler. |
| Terminal manifests | One per client (1–1,024), complete identity-bearing Realistic v1, all prepared batches ACKed exactly once, with no retry, ambiguity, error or truncation. The extractor preserves the exact manifest slices embedded in the OAT evidence; these are not claimed to be the original producer log bytes. |
| Exact equality | Complete unique run/incarnation/worker/sequence/row identities, with worker IDs 0–1,023. Missing, foreign, duplicate, rewritten or aliased input fails. Numeric u64 tokens are parsed exactly, not rounded through JavaScript numbers. |
| Scope | Whole active ingestion snapshot including warmup and drain; producer first-send cohorts are not Delta commit-window membership. Physical Add/history reconciliation and business payload checks are separate. |
| One-shot v1 | Complete ACKed `unscheduled` ranges with null bounds use a separate cohort; no window is invented. Scheduled ranges require matching client windows. |
| Memory | Default bitmaps <=512 MiB, <=65,536 ranges and <=64 MiB manifest sources. The reader projects only `endpoint_url`, one file and 8,192-row batch at a time; Parquet buffers are additional. |
| Frozen artifacts | Pin reader SHA256 and framed SHA256 of both TypeScript modules. Original manifest and snapshot bytes are hashed; file and artifact identities are checked before and after verification. |
| Result | Compact counts and hashes; nonzero exit on failure. This tool does not discover files, read Delta history or qualify throughput. |
| OAT integration | Every report must use the same inspected image and business corpus; every client readback is checked and retained. Cumulative completed rows are checked and summed against stopped receipts. Evidence is bounded to 16 MiB, each extracted manifest to 1 MiB. |
| Diagnostic-only Realistic | `readReconciledOatResult` takes `null` compression, identity tools as its eighth argument, and `{ diagnosticOnlyRealistic: true, expectedClientConfig: { workers: 14, streams_per_worker: 2 }, decodeFile }` as its ninth, with parity omitted. Complete startup and pre-admission writer readbacks must contain no overrides; the latter is bound to the collector image and precedes admission. The pinned baseline's `LayoutPolicy::writer_properties` sets uncompressed Parquet; all active file footers must agree. |
| Diagnostic identity | The output is marked `kind: diagnostic-only`, and identity artifacts are written beneath `outputDir/diagnostic-only/`; `identity-result.json` carries the same marker. Embedded manifest fragments and full OAT evidence remain byte-exact and unmodified. Whole-snapshot delivery equality is separate from half-open commit receipts and physical Delta/Parquet reconciliation; neither is a parity qualification. Missing identity evidence or tools fail closed. |

```bash
cargo build --locked --release --package collector-chain --bin delivery-identity-reader
VERIFIER_SHA=$(npx --no-install tsx tools/darwin/src/delivery-identity-cli.ts --artifact-hash)
DECODER_SHA=$(sha256sum target/release/delivery-identity-reader | cut -d' ' -f1)
npx --no-install tsx tools/darwin/src/delivery-identity-cli.ts \
  --input "$SNAPSHOT_JSON" --decoder target/release/delivery-identity-reader \
  --expected-decoder-sha256 "$DECODER_SHA" --expected-verifier-sha256 "$VERIFIER_SHA"
```

Integration can call `verifyDeliveryIdentitySet(fullManifests,
AsyncIterable<string>, { table, rewriteCommitCount: 0, maxBitmapBytes,
maxRanges })` while separately binding original evidence JSON bytes.
`verifyActiveParquetIdentitySet` takes the CLI input and frozen artifact hashes.
Decoder children are stopped and reaped on early failure or interruption.

## Isolated diagnostic entrypoint

| Item | Contract |
| --- | --- |
| First arm | `npx --no-install tsx tools/darwin/src/realistic-diagnostic.ts REPO HOME RUN_ROOT SNAPSHOT_DIR OUTPUT_DIR PINS_JSON BIND_IP ENDPOINT_IP COLLECTOR_CPUS CLIENT_CPUS STACK_BYTES --establish-reference` |
| Matched arm | `npx --no-install tsx tools/darwin/src/realistic-diagnostic.ts REPO HOME RUN_ROOT SNAPSHOT_DIR OUTPUT_DIR PINS_JSON BIND_IP ENDPOINT_IP COLLECTOR_CPUS CLIENT_CPUS STACK_BYTES --comparison-profile PROFILE_PATH PROFILE_SHA256 REFERENCE_RECEIPT_PATH` |
| Pin inputs | The JSON pins exact `harness_revision`, controller `source_revision` and `startup_revision`, `collector_source_revision`, `seed_sha256`, `controller_sha256`, `diagnostic_gate_sha256`, `footer_sha256`, `corpus_sha256`, the six `images` role IDs, and `identity_tools` hashes (`node_sha256`, `tsx_sha256`, `decoder_sha256`, `extractor_sha256`, `verifier_sha256`). The controller revision must descend from the merged harness and startup revisions. The collector source is independently fixed to `ad83c6376086f020a7d4e8cffd66994eedb537fe`; its immutable image label must match. |
| Image build identity | Build pinned local images with Compose project `diagnostic-stable-images`, matching the diagnostic runner. Runtime Compose projects are separately scoped by the OAT run ID. |
| Safety | This launches a run-owned Compose stack and removes its generated state after a successful stopped snapshot. Choose fresh `diagnostic-` paths; failures retain recovery state. The diagnostic baseline and every arm set both collector cgroup and pressure RSS limits to 64 GiB. Before launch and immediately before scheduling, the cap must leave at least 5% of `floor(0.75 × (MemAvailable − 5 GiB))` unused. Client startup accounting, pinned corpus, the single run-owned client roster and physically disjoint configured/effective CPU sets are checked at readiness and freshly rechecked immediately before the final memory check and schedule. |
| Matched CPU plan | On the 32-logical-CPU host, candidate collector masks `0,1,2,3,4,5,6,7`, `0` through `15`, and `0` through `23` reserve client CPUs `24` through `29` and support CPUs `30,31`. These are logical CPU numbers; lower strata leave the rest unused. A 32-logical collector cannot isolate the client and is unavailable for this comparison. The receipt retains actual client CPU samples and completed-window evidence; mean CPU alone does not establish client headroom. |
| Matched corpus | First-arm mode explicitly establishes a reference only after its receipt is reconciled. A matched arm requires the SHA-256 of a frozen comparison profile and its referenced completed receipt, with identical source, harness, seed, image and corpus pins. It copies both exact inputs and checks the same logical client's reservation at readiness, immediately before scheduling, and after the run. `--compare-reservations FIRST_RECEIPT SECOND_RECEIPT` remains a post-run comparison helper. |
| Results | This is a fixed-image collector CPU-scaling diagnostic, not a main-vs-candidate qualification or verdict. Whole-snapshot delivery equality includes warmup and drain; reconciled durable throughput uses deduplicated commits finishing in `[start,end)`, not client cohort membership. |

The Kustainer startup and QAP fallback are unchanged. A comparison binds the observed project-labelled Kusto container state and immutable image ID, not a tag or a presumption of availability. The pre-schedule proof also checks the collector’s effective cgroup cap. After an OAT invocation, `diagnostic-memory.json` records its lifetime cgroup v2 peak (not process RSS or a measured-window-only peak) and OAT overload-guard outcome, including on failed invocations. The OAT guard uses `cgroup_memory`, not process RSS. Stop and terminal snapshots include Docker state (including exited collectors), cgroup `memory.events`, and separate current anon/file bytes. An OOM kill is memory-limited; any max event excludes CPU attribution. A memory-cap stop is memory-limited. Missing, ambiguous or incomplete observations preclude CPU attribution; a zero-progress stop is not evidence of memory engagement.

### Project CPU admission and measurement

The treatment changes **both the collector cpuset and engine worker count**, not affinity alone. The historical seed must still specify 32; the derived config sets `dfe_worker_concurrency` and `collector.runtime.cpu_workers` to the mask size (8, 16, or 24). OAT supplies that count on first startup as well as scale-up. Before scheduling, the live collector argv (`--num-cores`), startup runtime readback, and effective cpuset must agree with the config. The receipt records the mask and both counts in `collector_treatment`.

The launch arguments are unchanged. Pass the ordered collector CPU list for the arm and client list `24,25,26,27,28,29`. Support is fixed at `30,31`; support services share those CPUs. The runner resolves services from its benchmark, OAT and maintenance Compose profiles with the generated run overlay. Only `dataflow-1` and `oat-client` are exceptions to the support default. The three role sets must be online and physically disjoint; this is **not host isolation**.

| Receipt field | Meaning |
| --- | --- |
| `pre_schedule_admission.cpu_isolation.project_readback` | Resolved service→cpuset map and every extant project-labelled container (including exited one-offs), ID, service, state and inspected cpuset. Missing, empty, unknown or mismatched assignments reject scheduling. Removed one-offs no longer have container counters. |
| `project_cpu_window.start/end` | Scheduled boundaries, actual host read times and per-container Engine read times (Unix ms), cumulative CPU nanoseconds, and host per-CPU ticks. Engine one-shot stats need not echo an ID: each read targets the full container ID after verifying its identity, Compose labels and running lifetime by inspect. Any returned ID must match. Rust validates and forwards the original one-line JSON without reserializing decimal tokens. The controller also enables `serde_json/float_roundtrip` for parsed values. Revalidation requires exact source integers, identities and recomputed CPU metrics; no numeric tolerance applies. Saturation and eligibility are recomputed without tolerance. One-shot reads aim 150 ms ahead of each boundary; timestamps outside ±1 s or discontinuous evidence reject the result. Containers already exited at both observations contribute zero, explicitly labelled, not a fabricated counter. |
| `project_cpu_window.per_container/group_cores/router_cores` | CPU seconds divided by actual elapsed seconds. Router is shown separately **and** included once in support. `host_busy_per_cpu` is busy fraction, excluding idle and iowait, not double-counting guest ticks. |
| `project_cpu_window.non_collector_busy_on_collector_cpus` | Host busy cores on the collector set minus pinned collector cores. This is an estimate, **not** attribution to individual processes. Kernel, dockerd, containerd, docker-proxy, devcontainer and other unpinned work can contribute. Negative estimates are retained and marked invalid. |
| `project_cpu_window.limited_by`, `collector_cpu_evidence` | A client or support group averaging **≥90%** of its logical CPU set marks the arm limited by that group (possibly both), not collector-CPU evidence. Apply the same criterion to pressure-probe comparisons; mean CPU below threshold alone does not establish headroom. A limited arm or invalid residual cannot establish a reference profile. |

### Diagnostic client profile correction

Use a fresh reference for the **width-63** profile. Do not compare it to a receipt from the earlier implicit Realistic width of 96. This changes generated string lengths, not the default generator, batch size, estimator, or admission policy.

The startup validator at `9f8d543c` and diagnostic gate at `b67fadd3` used 152,920,064 B; `8f94395c` carried it into matched accounting. None records a producing configuration or measurement, and the client source did not change from `b67fadd3` to `b12f7f06`. For the fixed 22-column replica-states table, customer `direct-e64-confirmwide`, and 65,536 rows per batch/corpus, the four-copy estimate is `4 × 65,536 × identity_row_bytes + 22 × 4,096`. Five width-dependent fields remain after substituting the logical endpoint identity, so `identity_row_bytes = 389 + 5 × string_width` for explicit positive widths. The old gate implies 583 B/row, or width 38.8: no integer width yields it.

| Explicit width | Measured reservation per live batch (B) | Admitted / configured |
| ---: | ---: | ---: |
| 38 | 151,871,488 | 28 / 28 |
| 39 | 153,182,208 | 28 / 28 |
| **63** | **184,639,488** | **28 / 28** |
| 64 | 185,950,208 | 27 / 28 |
| 96 (previous default) | 227,893,248 | 22 / 28 |

Width 63 is the closest admissible width to 96 with the row count, batch boundaries, 14 × 2 topology, seed 20260923 and 5 GiB cap unchanged. Two independent pending-schedule registrations produced logical corpus SHA-256 `40de89ded1a9dd7784e761ecbaa690ca48d9f2d3cf66ed4d5d935e882ebbf90c` and zero decoded rows. Each reserved 172,580,864 B for the corpus plus 5,169,905,664 B for 28 live batches, or 5,342,486,528 B total. The corpus reservation is still observed and compared across arms, not hard-coded into admission.

## Table-only storage intervention

**Not implemented or measured.** The current exporter reads `state/<run_id>/dataflow/telemetry/sqlserver/<table>` after stopping the writers. Changing a single container mount, relocating all of `compose_state_root`, or using a symlink would not establish a valid table-only comparison.

A candidate implementation would create a bounded, run-owned tmpfs mount at that exact table path **before** the stack starts, with no change to catalog, router, control or other state. It must prove the same mount and source identity in the runner/exporter and Docker daemon namespaces, verify the mounted table seen by catalog, collector, Kusto and maintenance (including read-only flags), and record filesystem type, mount ID, device, inode, capacity and source/target paths before scheduling and after stop. It must retain the mount and stopped data on failure, export and verify the stopped snapshot before any unmount, then unmount only the verified run-owned mount before state cleanup. Reject unsupported mount propagation, symlinks, mismatched identities or ambiguous cleanup. Tmpfs occupancy consumes host memory; budget its maximum separately while retaining the unchanged pre-schedule 75% check. No storage effect or valid arm is implied by this design.

### Named, matched diagnostic comparisons

Omit `--arm` for the original fixed-image profile. For a named series, append
`--arm candidate-baseline` to `--establish-reference`; each treatment instead uses
`--comparison-profile PROFILE_PATH PROFILE_SHA256 REFERENCE_RECEIPT_PATH --arm NAME`.
Use the same candidate pins file (identical bytes), host boot, trusted runtime,
stack size and run parent throughout. Use the mask matching the chosen row.

| Arm | Exact change from candidate baseline |
|---|---|
| `candidate-baseline` | Collector CPUs 0–7, 8 engine/runtime workers, 12 simultaneous flushes, 128 files/commit |
| `software-main` | Only the source/image pair below; software comparison, **not** single-resource causality |
| `cpu-16`, `cpu-24` | CPUs 0–15 or 0–23; engine `--num-cores`, DFE concurrency and runtime CPU workers move together to 16 or 24 |
| `writer-flush-6` | `batching.max_concurrent_flushes` 12 → 6 (exporter flush-slot semaphore, not writer threads) |
| `commit-files-64` | `commit.max_files_per_commit` 128 → 64 |

Candidate source `ad83c6376086f020a7d4e8cffd66994eedb537fe` requires image
`sha256:66c9d807973182594d6244a93f412a2b2c1da8b3bed30cfa528610ae50d67e68`;
main source `3630d3e4301bc3ba8f9f69a308d077cebee6d12a` requires image
`sha256:23a418cf48ab1765b6ab7b1a1bd647ee2486357db37865027b60df1ca4ee4c49`.
Rebuilding an image under the same revision label is not a substitute.
Before scheduling, OAT checks live process settings and the project service roster,
reverses just the manifest's exact treatment and compares the resulting digest with
the bound baseline. Missing readback or a second setting change rejects the arm.
Existing memory, saturation and durability gates still apply; a terminal read after
client failure records inspect evidence even when the exited cgroup cannot be read.
