# Collector resource observation

| Tool | Contract |
|---|---|
| `tools/darwin/src/physics-observer.ts` | Read-only Linux observer of one explicit collector PID and executable SHA256. `--cpu-list` verifies an already configured run-scoped allocation; it never changes affinity, quota or host settings. |
| `tools/darwin/src/null-flight-probe.ts` | Bounded single-table realistic client capacity probe against a separate loopback-only Flight decode/discard process. Clients and sink must have disjoint CPU allocations. |
| Durable headline | Only independently reconciled trusted OAT Delta receipts; neither observer counters nor discard-sink acknowledgements are durable rows. |
| Qualification | Same host, source/binary pins and fixed business corpus; only the suspected collector resource changes. Do not pool different hosts or substitute screening for A/A and AB/BA/AB. |

## Collector-only CPU scaling

Apply the collector cpuset at container creation through the trusted harness.
Leave client CPU allocations, corpus, binaries, storage, buffering and codec
settings unchanged. Use only CPUs within the available allocation, reserving
disjoint client capacity; logical CPU IDs are not necessarily physical cores.
The unchanged parity control must not run the observer or inherit its caps.

```bash
npx --no-install tsx tools/darwin/src/physics-observer.ts \
  --pid "$COLLECTOR_PID" --executable-sha256 "$COLLECTOR_SHA256" \
  --cpu-list 0-1 --start-unix-ms "$MEASUREMENT_START_UNIX_MS" \
  --duration-seconds 150 --output "$NEW_RUN_DIRECTORY" \
  --metrics-url "$COLLECTOR_METRICS_URL" --profile
```

Run the observer in a PID/mount namespace that can see the collector. Start it
before the shared measurement timestamp and wait for `ready.json`. Bad target
identity, effective affinity, missed schedule or process replacement fails the
capture. Thread churn is keyed by TID and thread start ticks; vanished/reused
threads are unavailable rather than attributed to a different thread. Output
directories must be new. Profiler children are owned, stopped and reaped with
bounded cleanup; no live collector thread is ever modified.

| Evidence | Scope and limitation |
|---|---|
| `metadata.json` | Host, executable hash, PID start ticks, original/effective affinity, observer affinity, cgroup placement and mount visibility. |
| `samples.ndjson` | Raw wall-clock sample intervals closed after all reads, monotonic timestamps, per-thread CPU/runqueue `schedstat`, RSS/status, process I/O, cgroup quota/throttling/memory/I/O and collector metrics. |
| `/proc/stat`, `/proc/diskstats` | Observer-visible system-wide counters, not collector-attributed disk bytes. Count block sectors at 512 bytes; avoid double-counting partitions and parent devices. |
| Collector `/proc/PID/net/dev` | Network-namespace device bytes, not per-process or Flight protobuf bytes. Loopback and other namespace traffic must remain distinguishable. |
| `pidstat`, `iostat`, `perf.data` | Opt-in raw profiler output. Missing binaries, denied PMU access or failed captures are null with their reason in `completion.json`. |
| Queue/flush/commit waits | Raw collector metrics when an explicit reachable metrics URL is supplied. Without it these measurements are unavailable, not zero. |
| Memory bandwidth / hardware ceilings | Null without a calibrated measurable counter. RSS, queue growth, cpuset size and arithmetic byte budgets do not prove saturation. |

Join raw timestamps to the trusted reconciled commit window and its complete
30-second buckets. Report collector quota/throttling separately from affinity.
For each 2/4/8/... CPU stratum, retain durable rows/s, durable rows/s per allocated
CPU, client offered/admission rates and bytes, queue/slot occupancy, flush/commit
waits, RSS and actual disk/network counters. Increasing a collector cap must
increase *durable* throughput under spare client load before calling it a
resource limit. Missing measurements and observer overhead remain limitations.

## Isolated client capacity probe

Build release binaries from the exact source revision, then use a fixed JSON
object containing the normal strictly parsed client settings. At minimum use
`table: "sqlserver_availability_replica_states"` and `data_profile: "realistic"`.
Corpus/business SHA256 and fresh run/incarnation/worker/sequence/row identities
remain mandatory. The probe owns endpoint, schedule, duration and warmup.

```bash
cargo build --locked --release --package collector-chain \
  --bin arrowcol-dmv --bin flight-null-sink
npx --no-install tsx tools/darwin/src/null-flight-probe.ts \
  --source-revision "$(git rev-parse HEAD)" \
  --client-bin target/release/arrowcol-dmv \
  --sink-bin target/release/flight-null-sink \
  --client-cpus 0-7 --sink-cpus 8-15 \
  --client-config "$FIXED_CLIENT_CONFIG" \
  --duration-seconds 30 --warmup-seconds 5 --output "$NEW_RUN_DIRECTORY"
```

The sink only binds loopback and terminates after a bounded lifetime. Output
contains exact settings, binary/config hashes, logs, terminal replay manifest,
business corpus hash and separate send-start offered rows/s and admission
completion rows/s. Offered rows count each actual `do_put` attempt (including
retries) in the half-open shared interval; they are not lifetime attempts,
decoded rows or durability. A slow decoder/sink can still cap this probe; it
does not prove a collector hardware ceiling. Use physical-core topology and
effective quotas when interpreting disjoint logical CPU allocations.

| Focused check | Command |
|---|---|
| Observer/probe safety | `npx --no-install tsx --test tools/darwin/src/physics-observer.test.ts tools/darwin/src/null-flight-probe.test.ts` |
| Type checking | `npx --no-install tsc --noEmit --allowImportingTsExtensions -p tsconfig.json` |
| Client identity, accounting and replay gates | `cargo test --locked --package collector-chain` |
