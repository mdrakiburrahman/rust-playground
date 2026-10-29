# Finite Pingora routing benchmarks

See the self-contained [architecture and benchmark report](report.html) for the
technology stack, measured policy comparisons, migration/scaling results and limitations.

This **separate, state-preserving** local harness reuses loadtest's SQL Server,
catalog migrations, Hive metastore, Direct collectors, SQL receipts and Delta
publication. Do not use `nx run loadtest:run` to resume it: that existing command
has different, destructive lifecycle semantics.

Router 1 is both the Pingora ingress and sole disk-backed routing authority.
Three Direct Flight collectors start initially. The optional second router is
data-only and polls router 1; it never opens routing storage or chooses owners.
There is no controller sidecar, leader election, failover or automatic rebalancer.
The private bearer-authenticated API is not published to the host. Ingestion
still requires edge HMAC, router-issued internal identity and an authoritative
per-RPC routing epoch check at the collector. SQL publication fences are separate.

## Build and start

Run from the repository root with the repository's pinned submodules available
and Docker already running. Nothing here starts or installs a host Docker daemon.
Builds can be large; two Cargo jobs are the recommended initial bound.

```bash
git submodule update --init --recursive
CARGO_BUILD_JOBS=2 cargo build --locked \
  -p arrowcol-rust -p arrowcol-router-rust -p arrowcol-loadgen -p arrowcol-sdk-gen
mkdir -p target/routing-benchmark/bin
for binary in arrowcol-rust arrowcol-router-rust arrowcol-loadgen arrowcol-sdk-gen; do
  install -m 755 "target/debug/$binary" "target/routing-benchmark/bin/$binary"
  strip "target/routing-benchmark/bin/$binary"
done
RUSTFMT=$(rustup which rustfmt)
RUSTLIB=$(dirname "$RUSTFMT")/../lib
mkdir -p target/routing-benchmark/sdk-toolchain/{bin,lib}
install -m 755 "$RUSTFMT" target/routing-benchmark/sdk-toolchain/bin/rustfmt
cp "$RUSTLIB"/librustc_driver-*.so "$RUSTLIB"/libLLVM.so.*-rust-* \
  target/routing-benchmark/sdk-toolchain/lib/
```

Use the existing Python environment, or install `requirements.txt` into a
project-local environment. The PyArrow/native Flight implementation is required;
HTTP or a stubbed receiver is not equivalent. Rust native dependencies can
require CMake; follow a missing-dependency diagnostic rather than bootstrapping
unrelated host tooling.

```bash
python3 projects/loadtest/routing-benchmark/stack.py prepare \
  --project pingora-bench-example --subnet 10.198.37.0/24 --policy least_pressure
ROOT=target/routing-benchmark/pingora-bench-example
python3 projects/loadtest/routing-benchmark/stack.py up --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py schemas --root "$ROOT"
python3 projects/loadtest/routing-benchmark/experiment.py --root "$ROOT" --scenario all
```

Choose an unused private `/24` and unique `pingora-bench-*` project. All Docker
commands are explicitly project-scoped. Host ports are random and bound to
`127.0.0.1`; collectors and the private authority have no host ports. The runtime
image context excludes credentials/state and includes the four binaries plus
the pinned toolchain's existing rustfmt and its two compiler shared libraries.
The canonical SDK generator requires edition-2024 rustfmt; no host daemon or
alternate SDK generation path is installed.
SQL is capped at 3 GiB/2 CPUs, Hive at 1 GiB/1 CPU, each collector/router at
1 GiB/1 CPU. These are limits, not reserved physical capacity.

`up` waits for private authority liveness and collector readiness without a
collector/controller readiness cycle. `schemas` uses the existing native Rust
catalog/SDK generator flow; it does not hand-author schemas or fingerprints.
Scale-out uses `--no-deps` so adding a data process cannot recreate the controller.
Pingora's 65-second request grace precedes runtime teardown, independently of
its runtime-shutdown timeout. It covers the pilot's bounded stream lifetimes;
an overrun remains an honest drain failure, not permission to replay DoPut.

## Scenarios and matched policy runs

`experiment.py --scenario` accepts:

- `cold-warm-pressure`: cold and sticky warm tables, a real Locust native-thread
  pilot repeated three times (`--repetitions 1..3`), then a bounded ~75%-duty CPU worker **inside** collector 1's cgroup and
  concurrent new-table placement. Observe actual telemetry, not synthetic
  pressure injected into the routing store.
- `migration`: start a finite paced source stream, observe admitted work,
  introduce bounded source pressure, and request a controller-selected target.
  Existing work must finish and publish before the source releases its SQL
  fence; the target's greater routing epoch remains fenced until SQL activation.
- `scaling`: collectors 3→4→3, new assignments to collector 4 and controlled
  migration of an existing table into it under bounded old-pool CPU pressure,
  followed by evacuation before its removal; data routers 1→2→1, new connections through
  both routers and graceful removal while a finite stream is admitted.
  Data-router scaling must preserve table ownership and epochs.
- `restart`: restart the controller, require fresh collector telemetry and
  unchanged established ownership, then verify existing-table publication.
- `data-routers`: repeat only the two-router connection/drain experiment against
  existing owned tables; unlike cold-placement scenarios, it is safe to resume.
- `all`: the above in order, using disjoint cold-table subsets.

Cold-placement scenarios require a **fresh** authority/dataset. Repeating them
against existing ownership measures sticky routing, not cold selection.
For matched comparison, stop the first stack, prepare a different project/subnet
with `--policy weighted_rendezvous`, build/start/prepare it and run the same
`cold-warm-pressure` scenario. Stable collector keys, schemas, bounds and pressure
commands match; process UUIDs, timing and physical host contention do not. Keep
both evidence directories. Do not claim policy superiority from a tiny pilot,
or added physical capacity from more containers on one host.

Individual lifecycle operations are available:

```bash
python3 projects/loadtest/routing-benchmark/stack.py view --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py add-collector --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py migrate --root "$ROOT" \
  --table telemetry.sqlserver.sqlserver_cpu_utilization
python3 projects/loadtest/routing-benchmark/stack.py remove-collector --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py add-router --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py remove-router --root "$ROOT"
python3 projects/loadtest/routing-benchmark/stack.py stop --root "$ROOT"
```

Migration requires fresh telemetry, a sufficiently better eligible target and
cooldown, unless evacuating a deliberately retiring source. It can honestly
time out. Never stop a source after an unresolved move, force a new epoch, delete
the authority snapshot, or treat an unknown SQL release as success. Collector 4
is stopped only after all its ownership and fresh aggregate work are zero.
`stop` preserves state, secrets, results and project volumes. There is no reset,
global cleanup or implicit removal command. `configure` regenerates static
configuration without replacing task secrets or routing snapshots; do not run
it concurrently with workload execution.
Preservation is not automatic collector-process recovery: established ownership
retains its process/fence lineage, so restarting all processes may safely block.
The restart scenario keeps collectors alive and restarts only the controller.

## Native workload contract and evidence

`workload.py SETTINGS.json` and `locustfile.py` share the same finite worker.
The experiment writes reproducible settings, per-stream JSONL and a report for
each phase under `$ROOT/evidence/`. Locust uses bounded native OS threads, not
the assumption that blocking PyArrow Flight cooperates with gevent. Set
`LOCUST_SKIP_MONKEY_PATCH=1` when invoking Locust directly; constrain users to
the configured worker count. Connection/stream rotation, deadlines, batch bytes,
duration, stream count and total rows are bounded. There is no DoPut replay.

Reports separate generated/attempted/write-completed work, terminal RPC outcome,
admission receipts, receipt publication and independently scanned Delta output.
Schema identity/fingerprint and admitted row counts are checked. Publication
requires `published` plus `purge_allowed`; one Delta commit can cover multiple
receipts, so its aggregate count is not compared as a per-receipt row count.
Run/stream/batch/row markers reconcile exact visible row multisets. Missing,
duplicate/unconfirmed output, unknown RPC outcomes and publication failures
remain visible. This is **not an exactly-once guarantee**.
The publication deadline bounds the entire receipt set, including each HTTP
request, rather than restarting a full timeout for every pending receipt.
Run markers use a fixed-length random token independent of the scenario label,
so matching policy workloads have matching generated Arrow byte counts.
Controller logs include authenticated control-operation execution latency
(including authority locking/persistence, excluding HTTP decoding/encoding and
network time), plus snapshot bytes and write/sync/replace/parent-sync latency.
Finite scenarios also stream Docker CPU/memory samples for explicit task-owned
container IDs, refreshing the sampler when the topology changes and stopping it
on exit. Pilot measurements do not establish a safe production router capacity.
Project stop explicitly includes every Compose profile, so scaled-out collectors
and data routers are stopped too; it never deletes their state or volumes.
Independent output scans disable threaded scanning and batch prefetch, with at
most one current fragment. Scanner objects are released before the finite worker
process exits, avoiding native background reads racing interpreter shutdown.

Evidence includes authority/pressure views, Docker stats, assignment/epoch
snapshots, terminal latency percentiles, generation and reconciled throughput,
load-generator lifetime CPU/peak RSS and router/controller logs. Snapshot commit
bytes/latency and claim/read events are logged separately. Consider generator
saturation, SQL/Hive/Delta costs and host contention before interpreting results.
Topology captures also retain bounded public router metrics, including each
running router's active-stream gauge and latency/failure observations.
Placement-decision logs retain the actual eligible pressure snapshots and pending
reservations at selection time; the later claim event establishes durable commit.

Focused validation:

```bash
python3 -m unittest discover -s projects/loadtest/routing-benchmark -p 'test_*.py' -v
CARGO_BUILD_JOBS=2 cargo test --locked \
  -p arrowcol-routing -p arrowcol-rust -p arrowcol-router-rust -p arrowcol-loadgen
```

After the cold-placement experiments, opt into five real-stack checks
(valid publication, bad edge HMAC, unsupported protocol, wrong wire schema, and
bounded draining with authoritative rejection of new RPCs):

```bash
ARROWCOL_BENCHMARK_LIVE_ROOT="$ROOT" python3 -m unittest discover \
  -s projects/loadtest/routing-benchmark -p 'test_live.py' -v
```

These are tests, not throughput samples. They write separate evidence and never
replay a rejected upload. Without the explicit live-root variable they are skipped.
The live drain check observes an admitted source RPC, deliberately times out a
short migration observer without forcing ownership, rejects a new RPC while
draining, and then reconciles the admitted publication and completed higher epoch.
Observers reuse the exact acknowledged migration record instead of issuing a
second move. Early Pingora rejections can surface as native stream resets rather
than descriptive Flight errors; negative auth/metadata checks also require the
corresponding existing router failure counter to increase, with no admission receipt.

`stack.py rust-smoke --root "$ROOT"` additionally runs the existing Rust native
Flight path on one already-prepared table: one lane, eight rows per batch,
one second of generation, one attempt per logical batch and a bounded publication
drain. Run it **after** cold-placement timing with the other writers quiescent;
it checks receipt identities and the independently visible Delta row-count
increase, and writes separate manifests under
unique `$ROOT/prepared/rust-native-smoke-*` directories. Startup capability
probes are scoped to the selected scenarios, so this direct-only smoke does not
require a nonexistent Dataflow service. Its legacy SQL-routing export is not
evidence of disk ownership; the controller API/logs remain authoritative.
