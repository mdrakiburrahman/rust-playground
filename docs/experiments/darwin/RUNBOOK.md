# ArrowCol Darwin evaluator

The current implementation supports a config-only compression smoke replay. It
does not yet qualify candidates, run Darwin, promote changes, or evaluate code
lanes. A failed or unsupported lane never scores as a winner.

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
| `historical-parity.oat.json` | Diagnostic client with historical AKS batching, commit and CPU-worker settings, on the same hourly-partitioned table. Config validation is not a runtime readback. The compression replay does not yet support this profile. |

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
Diagnostic parity requires the pending profile-aware digest and extractor.
Client process CPU is reported separately from
collector samples, with its own sampling interval or an unavailable reason.
The hourly ingestion-partitioned table is checked against catalog metadata
before the clients are armed. Each pre-load Delta log is hashed and must have
no Adds or Removes; all later versions need cumulative collector receipts.
The stopped collector's complete log extends the pre-stop ledger through
shutdown drain. Overlapping receipts and the measured window must agree.
Gaps produce a null unattributed-row count, an explicit reason and a nonzero
evaluator exit.

On failure, retain the exact run log and use the documented
`--cleanup-local <cell-uuid>` recovery for that owned cell. Do not retry until
the OAT owner has exited and cleanup is confirmed. Fresh HOME and restricted
paths prevent accidental credential inheritance; they are not a security
sandbox. Run candidate workloads only in throwaway environments and never pass
the Darwin model token to builds or runs.
