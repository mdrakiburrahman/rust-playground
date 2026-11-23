# ArrowCol Darwin evaluator

## Versioned diagnostic recovery transition

The reviewed pre-load cleanup exception is specific to invocation
`89fdefa5-855d-46ea-93ea-481fd77cbd2e` and the original frozen producer
`86eb8ebe07d876c925e027c0c656a69917269f22`. Prepare a separate trusted
snapshot at `.darwin/pressure-diagnostic-v2`; do not change the original
freeze or move, copy, or link its receipt ledger. The v2 runtime checks the
original freeze and uses its canonical ledger for both the launch gate and
new receipts. The reviewed sidecar and original evidence are revalidated.

Use `parity --diagnostic-prelaunch-only` to execute the same parity-to-launcher
gate without starting a workload. It builds the v2 controller and writes preflight files;
it does not qualify a sample. A missing or changed proof fails closed.

The current implementation supports a config-only compression smoke replay and
individual local historical-parity arms. It
does not yet qualify candidates, run Darwin, promote changes, or evaluate code
lanes. A failed or unsupported lane never scores as a winner.

The experimental `qualify` driver runs a guarded Realistic smoke, optionally
followed by A/A and main-versus-candidate AB/BA/AB local series. Tier-0 and
Tier-1 results are not yet bound, so even a passing Tier-2 series remains
`valid: 0` and cannot promote a candidate. Live end-to-end validation is still
required before relying on this path.

| Qualification step | Command |
| --- | --- |
| Fresh trusted home | `npx --no-install tsx tools/darwin/src/main.ts setup --home "$PWD/.darwin/qualification"` |
| Identity-bearing smoke | `.darwin/qualification/trusted/bin/harness qualify --roles <roles.json> --bind-host <docker-bridge-ip> --endpoint-host <docker-bridge-ip> --controller-stack-bytes 33554432` |
| Experimental codec smoke | `.darwin/qualification/trusted/bin/harness qualify-codec-smoke --candidate "$PWD" --roles <roles.json> --bind-host <docker-bridge-ip> --endpoint-host <docker-bridge-ip> --controller-stack-bytes 33554432` |
| Smoke then full local series | Add `--series` to the command in a fresh trusted home. |

`roles.json` must contain `baseline` and `candidate` objects with `imageId`
(`sha256:` image ID) and full `revision`, `observerImageId`, and a
`nonCollectorImages` object with immutable image IDs for `catalog`, `client`,
`maintenance`, `router`, and `sqlserver`. Build and retain provenance for
both collector images; labels alone do not prove source provenance. The two
collector roles must differ, while all other roles remain fixed. The generated
partition shortcut is not requested. This profile uses 14 client workers and
a 16 GiB client budget, not the historical 1 GiB client profile.

The codec smoke copies only `compression` and `compression_level` from the
allowlisted candidate into the guarded qualification profile; it does not
change the baseline or run a comparison. It is not yet live-validated. The
Darwin runner and its existing pair evaluator remain Zstd-level-only.
For this smoke the two collector roles may be identical; the code trial still
requires distinct roles.

| Candidate codec | Allowed level |
| --- | --- |
| `uncompressed`, `snappy`, `lz4`, `lz4_raw` | `null` or omitted |
| `gzip` | `null` or 0–9 |
| `brotli` | `null` or 0–11 |
| `zstd` | `null` or 1–22 |

`null` selects the writer's codec-specific default. Footer metadata proves the
codec, not its level; the requested level must also match both startup and
pre-admission readbacks.

The driver writes an exclusive original 24-hour deadline and retains failed
locks for manual owned-cell recovery; it does not resume interrupted series or
automatically retry. Do not remove the lock or start another series until
owned resources and the original deadline have been accounted for. Preserve
the raw active snapshot, manifests, toolchain hashes and receipts before
releasing the host. A smoke by itself is not a qualification result.

| Step | Command |
| --- | --- |
| Snapshot committed source | `npx --no-install tsx tools/darwin/src/main.ts setup --home "$PWD/.darwin/compression" --producer <full-producer-sha>` |
| Preflight | `.darwin/compression/trusted/bin/harness preflight --experiment compression-config` |
| Prepare and identify release controller | `.darwin/compression/trusted/bin/harness prepare --controller-stack-bytes 33554432` |
| Diagnostic pair | `.darwin/compression/trusted/bin/harness evaluate --experiment compression-config --smoke --bind-host <docker-bridge-ip> --endpoint-host <docker-bridge-ip> --controller-stack-bytes 33554432` |
| Diagnostic baseline only | Add `--baseline-only` to the diagnostic command with the frozen seed unchanged. |
| Path-only stub | `.darwin/compression/trusted/bin/harness evaluate --experiment compression-config --stub` |

Use an initialized repository, prepared Node and Cargo dependencies, and a
reachable Docker daemon. The pinned producer's tracked `Cargo.lock` freezes
dependencies; `--locked` does not itself populate an offline cache. Set a
reachable daemon bridge IP as described in
[OAT runtime](../arrowcol-oat/RUNTIME.md). Before evaluating, change only the
Zstandard level in `experiments/darwin/candidate.oat.json`. Level 3 is the
frozen baseline; 1–22 are valid. Both runs use the same controller stack size,
release executable and workload image identities.

The equivalent Taskfile entry points are `darwin:setup`, `darwin:prepare`,
`darwin:preflight`, and `darwin:evaluate`. Supply `DARWIN_PRODUCER` for setup,
and `DARWIN_BIND_HOST` and `DARWIN_ENDPOINT_HOST` for evaluation. The optional
`DARWIN_HOME` defaults to `.darwin/compression`.

The realistic profile and seed are distinct from the historical diagnostic,
IPC-none AKS client-parity seed. A single 150-second pair is diagnostic, not a
qualified winner or a 600-second AKS result. The evaluator selects original
commit receipts in the half-open window, independently checks Delta versions,
active Adds and decoded Parquet, and reports latency sample counts. A candidate
must meet its own frozen baseline's 10% bytes-per-row guard. The separate
historical comparable AKS headline guard is 38.17 B/row.

| Local profile | Purpose |
| --- | --- |
| `candidate.oat.json` | V9 Realistic seed; its unique per-row identity is high entropy. |
| `historical-parity.oat.json` | Diagnostic client with historical AKS batching, commit and CPU-worker settings, on the same hourly-partitioned table. The compression replay does not yet support this profile. |

| Parity step | Command |
| --- | --- |
| Snapshot current committed producer | `npx --no-install tsx tools/darwin/src/main.ts setup --home "$PWD/.darwin/parity"` |
| One ladder arm | `.darwin/parity/trusted/bin/harness parity --workers <1\|2\|4\|8\|16> --bind-host <docker-bridge-ip> --endpoint-host <docker-bridge-ip> --controller-stack-bytes 33554432` |
| Substitute a control collector | Append `--collector-image-id sha256:<digest> --collector-source-revision <full-commit>` to the parity command, using a separate trusted home. |
| Collector-only CPU stratum | Append `--collector-cpus 0,1,2,3` to a new trusted home; choose CPUs available to the host. |
| Bounded table diagnostic | Append `--profile bounded-table-2500m-diagnostic` in a separate trusted home. |
| Generated partition candidate | Append `--generated-partition-fast-path true` or `false` in a separate trusted home. |

Build the control image from a clean checkout of the recorded commit and retain
the build log and source tree identity. An image revision label alone does not
prove provenance. Compare controls only when the non-collector image IDs,
observer, corpus, settings and host match; an individual arm is not a
qualified comparison.
The bounded profile changes only per-table buffered-byte capacity from 4 GB to
2.5 GB; the 90% global guard and 86 GiB memory cap remain unchanged. The
capacity plus one 1 GiB flush is below the guard, but concurrent in-flight
work may still trigger it. Do not mix this stratum with historical parity;
compare main and candidate with the same profile.
The partition fast path is opt-in and requires a collector that reports support;
current main does not. Label a main-versus-enabled comparison as tuned-system,
and compare the candidate with the switch off/on separately.
The CPU option applies only to the collector before startup. The run refuses
admission unless its configured and effective cgroup CPU sets match the
requested list. It does not reserve CPUs from clients; record their placement
and use disjoint allocations where possible. A causal CPU-scaling claim also
requires noncompeting physical-core/SMT allocations, effective ancestor
constraints, actual collector utilization and bounded queues; otherwise the
shared-placement run is diagnostic only. Hold the candidate, configuration,
and clients fixed while changing only the collector allocation.

Each arm builds the trusted controller and footer inspector, checks host
headroom immediately before launch, verifies collector and client readback
before arming, and checks table readback and stopped active-file footers.
Parity additionally requires process-lifetime evidence of zero retried or
ambiguous sends. Flight acknowledgements are admission evidence, not durable
commits or proof of unique delivery. Missing counters invalidate the arm.
An individual arm is diagnostic and cannot qualify a winner. Preserve each
receipt and compare identical image and corpus identities before interpreting
a ladder or A/A. Parity builds use a stable Compose build project and pin each
role to its immutable image ID. The first reconciled arm records the image and
corpus identity; subsequent arms refuse an image mismatch before starting.
A failed arm retains its lock until owned-cell recovery.
Results retain timestamped router ownership samples and the start time of
ordered shutdown; missing timestamps carry an explicit reason.

Confirm Realistic codec, dictionary and compression winners on historical
parity at AKS-like file sizes (approximately 2 million rows/file) before
promotion. The parity collector's 4.29 GB queue budget is not headroom inside
its container limit. The current 86 GiB pin rounds down 75% of observed host
available memory after the 1 GiB client budget; it is not the historical AKS
384 GiB allocation. Recheck host availability and the effective cap before
every arm; refuse a run if the pin exceeds that bound. The 90% overload guard
remains enabled. Pressure registration uses the same local RSS limit and the
historical queue limits. Pipeline depth 8 causes the exporter to use
`force_commit=false`. The sorted-layout limits are 2 million rows, 1 GiB and
262,144 rows per group; table-effective readback must confirm them. Those
limits do not guarantee files reach 2 million rows.
On Unix, the local guard verifies the private cgroup directly at admission,
then samples Docker's one-shot cgroup accounting with a fresh timestamp and
container identity/cap checks before and after each sample. A missing or
stale sample still fails closed at the unchanged three-second deadline.
Label measurements **local one-box scale**, not AKS-equivalent; record host
memory and vCPUs, chosen cap and sampled collector peak RSS, using null when
unmeasured.

| Deliberate local deviation | Historical AKS |
| --- | --- |
| Collector and clients share 32 host vCPUs; collector uses 32 workers. | Collector requests and limits 48 CPUs. |
| Collector cap 86 GiB; pressure RSS limit equals the local cap. | Collector limit 384 GiB; pressure RSS limit 320 GiB. |
| Local storage and one-box scheduling. | Separate AKS resources and remote storage. |
| 60-second warmup and 150-second measurement. | 60-second warmup and 600-second headline window. |
| Runtime worker, blocking, upload and task limits are explicitly pinned by the local profile. | Historical manifest uses runtime defaults. |

The historical writer policy is source-derived from the deployed config and
`3630d3e4`: no Parquet override, uncompressed layout output, dictionary and
page statistics. Historical footer receipt: null. Each parity run must prove
the codec, dictionary pages and page statistics in stopped-table footers.

Resource memory is a sampled container value, not peak process RSS; unavailable
RSS and downstream throughput are reported as null with reasons.

The client uses the fixed business customer ID `direct-e64-confirmwide` while
delivery and row identities remain unique per run. Its manifest hashes the
generated Arrow IPC business corpus with `endpoint_url` excluded; both arms
must report the same hash. This contract applies to the Realistic seed only;
Diagnostic parity hashes all business columns but cannot reconcile unique
delivery identities.
Realistic compression replay requires the producer to match the trusted
snapshot revision and fails unless the complete stopped active ingestion
snapshot matches the terminal delivery manifest. Each arm retains
`identity-result.json`, the evidence and manifest bytes, and the snapshot
input, with pinned Node, tsx, extractor, verifier and reader hashes. This is
whole-snapshot equality, not proof of commit-window membership; see
[the identity contract](PHYSICS_OBSERVATION.md).
Client process CPU is reported separately from
collector samples, with its own sampling interval or an unavailable reason.
The hourly ingestion-partitioned table is checked against catalog metadata
before the clients are armed. Each pre-load Delta log is hashed and must have
no Adds or Removes. Later metadata-only versions need no receipt; every ingestion
Add version, including post-window drain, must match a final stopped collector
receipt and independently decoded file counts, rows and bytes. Removes and
non-ingestion Adds fail the fresh-table, no-rewrite check. Window selection uses
receipt completion times, not Delta commitInfo timestamps.
The stopped collector's complete log extends the pre-stop ledger through
shutdown drain. Overlapping receipts and the measured window must agree.
Gaps produce a null unattributed-row count, an explicit reason and a nonzero
evaluator exit.

On failure, retain the exact receipt, ownership marker, result and run log; use
`--cleanup-local <cell-uuid>` for that owned cell. For a failed single-cell
local run with bound overload evidence, including an unranked `pinning_violation`, run
`--recover-failed-local <invocation-id> <preserved-ownership-marker-path>` from
the receipt-owning runtime after verified cleanup. It appends a disposition
without changing the failed receipt; each subsequent launch rechecks its
evidence and same-host resource absence. Missing evidence, active writers,
resources or a changed host boot block recovery. Do not retry until cleanup is
confirmed. Fresh HOME and restricted paths prevent accidental credential
inheritance; they are not a security sandbox. Run candidate workloads only in
throwaway environments and never pass the Darwin model token to builds or runs.
