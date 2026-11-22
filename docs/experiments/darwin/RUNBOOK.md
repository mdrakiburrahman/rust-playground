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

Resource memory is a sampled container value, not peak process RSS; unavailable
RSS and downstream throughput are reported as null with reasons.

The client uses the fixed business customer ID `direct-e64-confirmwide` while
delivery and row identities remain unique per run. Its manifest hashes the
generated Arrow IPC business corpus with `endpoint_url` excluded; both arms
must report the same hash. Client process CPU is reported separately from
collector samples, with its own sampling interval or an unavailable reason.
The hourly ingestion-partitioned table is checked against catalog metadata
before the clients are armed. Each pre-load Delta log is hashed and must have
no Adds or Removes; all later versions need cumulative collector receipts.
Gaps produce a null unattributed-row count, an explicit reason and a nonzero
evaluator exit.

On failure, retain the exact run log and use the documented
`--cleanup-local <cell-uuid>` recovery for that owned cell. Do not retry until
the OAT owner has exited and cleanup is confirmed. Fresh HOME and restricted
paths prevent accidental credential inheritance; they are not a security
sandbox. Run candidate workloads only in throwaway environments and never pass
the Darwin model token to builds or runs.
