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
| Safety | This launches a run-owned Compose stack and removes its generated state after a successful stopped snapshot. Choose fresh `diagnostic-` paths; failures retain recovery state. Admission is unchanged: both initial and immediate pre-schedule 75% host-memory checks must pass. Client startup accounting, pinned corpus, the single run-owned client roster and physically disjoint configured/effective CPU sets are checked at readiness and freshly rechecked immediately before the final memory check and schedule. |
| Matched CPU plan | On the 32-logical-CPU host, candidate collector masks `0,1,2,3,4,5,6,7`, `0` through `15`, and `0` through `23` reserve client CPUs `24` through `29` and support CPUs `30,31`. These are logical CPU numbers; lower strata leave the rest unused. A 32-logical collector cannot isolate the client and is unavailable for this comparison. The receipt retains actual client CPU samples and completed-window evidence; mean CPU alone does not establish client headroom. |
| Matched corpus | First-arm mode explicitly establishes a reference only after its receipt is reconciled. A matched arm requires the SHA-256 of a frozen comparison profile and its referenced completed receipt, with identical source, harness, seed, image and corpus pins. It copies both exact inputs and checks the same logical client's reservation at readiness, immediately before scheduling, and after the run. `--compare-reservations FIRST_RECEIPT SECOND_RECEIPT` remains a post-run comparison helper. |
| Results | This is a fixed-image collector CPU-scaling diagnostic, not a main-vs-candidate qualification or verdict. Whole-snapshot delivery equality includes warmup and drain; reconciled durable throughput uses deduplicated commits finishing in `[start,end)`, not client cohort membership. |

## Table-only storage intervention

**Not implemented or measured.** The current exporter reads `state/<run_id>/dataflow/telemetry/sqlserver/<table>` after stopping the writers. Changing a single container mount, relocating all of `compose_state_root`, or using a symlink would not establish a valid table-only comparison.

A candidate implementation would create a bounded, run-owned tmpfs mount at that exact table path **before** the stack starts, with no change to catalog, router, control or other state. It must prove the same mount and source identity in the runner/exporter and Docker daemon namespaces, verify the mounted table seen by catalog, collector, Kusto and maintenance (including read-only flags), and record filesystem type, mount ID, device, inode, capacity and source/target paths before scheduling and after stop. It must retain the mount and stopped data on failure, export and verify the stopped snapshot before any unmount, then unmount only the verified run-owned mount before state cleanup. Reject unsupported mount propagation, symlinks, mismatched identities or ambiguous cleanup. Tmpfs occupancy consumes host memory; budget its maximum separately while retaining the unchanged pre-schedule 75% check. No storage effect or valid arm is implied by this design.
