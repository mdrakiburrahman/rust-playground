# Arrowcol Dataflow Rust collector

This binary links the DFE-native Arrowcol components and calls the fork's
standard startup entry point. It ships two declarative pipelines:

```text
config.yaml         Arrow Flight -> bounded DFE channel -> generic Delta exporter
config.quiver.yaml  Arrow Flight -> processor:durable_buffer (Quiver) -> generic Delta exporter
```

`config.yaml` completes Flight streams with best-effort admission.
`config.quiver.yaml` sets `native_quiver: true` and completes a stream only
after the durable buffer persisted every batch (see
[Delivery guarantee](#delivery-guarantee)).

The process is pinned to one DFE core in both YAML policy and the documented
command. Delta table ownership is process-shared, but a single pipeline core
also keeps Flight registration and admin lifecycle unambiguous.

## Build and validate

```bash
npx --no-install nx run arrowcol-dataflow-rust:build
npx --no-install nx run arrowcol-dataflow-rust:config-check
npx --no-install nx run arrowcol-dataflow-rust:smoke
npx --no-install nx run arrowcol-dataflow-rust:image-smoke
npx --no-install nx run arrowcol-dataflow-rust:verify
```

`config-check` validates the Docker-ready SQL/Hive configurations
(`config.yaml`, `config.quiver.yaml`) and both memory-backed smoke
configurations, and proves that a STANDARD-only processor
(`processor:batch` in `config.incompatible-otap.yaml`) is rejected by DFE
wiring contracts when it would receive Arrowcol pluggable Arrow pdata.
`smoke` starts the memory-backed best-effort (`config.smoke.yaml`) and
durable (`config.smoke-quiver.yaml`) topologies, verifies every admin endpoint
and capability claim, sends `SIGTERM`, and requires bounded graceful shutdown.
Both scripts use `$CARGO_TARGET_DIR` when it is set.
After ingress enters `draining`, the main-thread shutdown hook waits for the
controller's final publication and shared publisher close before writing the
terminal routing state. Only a clean drain and close with no unpublished identity or retired worker
becomes `stopped`; otherwise it becomes `failed`. A refused close or failed
terminal write is logged and exits nonzero. The receiver never declares the
process stopped while its exporters are still draining.

## Local infrastructure

Start the repository SQL Server and Hive Metastore:

```bash
npx --no-install nx run arrowcol-infra:bootstrap
npx --no-install nx run arrowcol-infra:up
```

Then run the collector with host-facing settings:

```bash
ARROWCOL_SQL_HOST=127.0.0.1 \
ARROWCOL_SQL_PORT=14333 \
ARROWCOL_SQL_PASSWORD_FILE=projects/arrowcol-infra/secrets/arrowcol-app-password.txt \
ARROWCOL_HIVE_HOST=127.0.0.1 \
ARROWCOL_HIVE_PORT=9083 \
ARROWCOL_DELTA_ROOT="$PWD/onelake/arrowcol-dataflow" \
npx --no-install nx run arrowcol-dataflow-rust:run
```

For the durable topology, add a writable Quiver directory and use `run-quiver`:

```bash
ARROWCOL_QUIVER_PATH="$PWD/onelake/arrowcol-dataflow-quiver" \
ARROWCOL_SQL_HOST=127.0.0.1 \
ARROWCOL_SQL_PORT=14333 \
ARROWCOL_SQL_PASSWORD_FILE=projects/arrowcol-infra/secrets/arrowcol-app-password.txt \
ARROWCOL_HIVE_HOST=127.0.0.1 \
ARROWCOL_HIVE_PORT=9083 \
ARROWCOL_DELTA_ROOT="$PWD/onelake/arrowcol-dataflow" \
npx --no-install nx run arrowcol-dataflow-rust:run-quiver
```

The live SQL/Hive/Delta qualification is:

```bash
npx --no-install nx run arrowcol-dataflow-rust:live-e2e
```

The script owns one bounded Compose window, connects the workspace container
to the infrastructure network when necessary, and always disconnects and
stops the stack. Logs and Compose evidence are retained under ignored
`.diagnostics/` run directories.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `0.0.0.0:32010` | Arrow Flight `DoPut` and strict Flight SQL DDL |
| `/healthz` | Process health |
| `/readyz` | Registration/catalog/admission readiness and delivery guarantee |
| `/v1/capabilities` | Honest delivery and feature metadata |
| `/v1/pressure` | Rich router pressure plus occupancy provenance |
| `/metrics` | Prometheus text metrics |

Admin HTTP listens on port `32011` by default.

## Delivery guarantee

| | `config.yaml` (`native_quiver: false`) | `config.quiver.yaml` (`native_quiver: true`) |
| --- | --- | --- |
| `delivery_guarantee` | `best_effort` | `durable_admission` |
| `completion_mode` | `best_effort_admission` | `durable_admission` |
| Successful `PutResult` means | every batch entered the bounded DFE route | the durable buffer appended and fsynced every batch |
| `at_least_once` | `false` | `true` (duplicates are possible) |
| SQL receipts / receipt polling | none | none |
| `native_quiver` capability | `{"supported": true, "enabled": false}` | `{"supported": true, "enabled": true}` |

Best-effort completion confirms bounded DFE admission, not Delta publication.
Delta failures after admission are visible only through DFE Nacks, pressure,
and metrics.

Durable admission waits, bounded by `limits.admission_timeout`, until the
durable buffer acknowledged every batch of the stream. Its acknowledgement
follows the write-ahead-log append, and `wal_flush_interval: 0s` makes it
follow an fsync, so an acknowledged stream survives a crash. A rejected batch
or the timeout fails the stream with a retryable `UNAVAILABLE` error; the
client must resend it, and batches persisted before the failure are still
delivered. A restart replays every unpublished batch, so delivery is
at-least-once rather than exactly-once.

## Delta and schema behavior

`arrowcol-catalog` performs exact schema lookup before the first batch. Current
schemas pass through unchanged; historical nullable-append schemas reuse
source arrays and add only null arrays. The pdata envelope
(`arrowcol.flight@1.1`) preserves logical table, source/target catalog schema
versions, lease lineage, owner, fencing epoch, and the admitting collector's
`registration.instance_key`. The exporter writes the original arbitrary Arrow
batch through `arrow-deltalake`, which performs only its generic lossless
Delta physical mapping.

One bounded `BatchedTable` owner exists per logical table in the process.
Every commit rechecks the authoritative routing lease through the dedicated
fence worker. A failed commit retires that owner, so the next batch reopens
the table under the current lease. Shutdown stops Flight admission, drains
active streams while still processing DFE acknowledgements, forces pending
Delta batches, publishes final pressure, and then relinquishes registration.

## Native durable buffer

`config.quiver.yaml` places `processor:durable_buffer` between the receiver
and the exporter. The buffer persists `arrowcol.flight@1.1` record sets
unchanged and replays unacknowledged ones after a restart. Its settings:

| Setting | Value | Purpose |
| --- | --- | --- |
| `path` | `${ARROWCOL_QUIVER_PATH:-/var/lib/arrowcol/quiver}` | Per-core WAL and segment storage; must persist across restarts |
| `wal_flush_interval` | `0s` | Fsync before every acknowledgement |
| `retention_size_cap` | `${ARROWCOL_QUIVER_CAPACITY:-4 GiB}` | Disk budget; each core needs at least 192 MiB |
| `size_cap_policy` | `backpressure` | Refuse new data instead of dropping old data |
| `max_segment_open_duration` | `1s` | Upper bound on the delay before replay-visible segments |
| `poll_interval` | `100ms` | Downstream forwarding cadence |
| `initial_retry_interval`, `max_retry_interval` | `250ms`, `2s` | Backoff for transiently refused batches |

Restart recovery depends on a stable identity: run the restarted process with
the same `registration.instance_key` (`ARROWCOL_INSTANCE_KEY`) and the same
Quiver volume. Routing hands a dead predecessor's leases to its
same-instance-key successor. Until then, replayed batches are refused
transiently and retried; afterwards the exporter rebinds them to the
successor's lease and publishes them. A batch the exporter refuses
permanently (for example a malformed envelope) is dropped by the buffer.

`/v1/pressure` adds the durable backlog (batches the buffer acknowledged to
this process that are not yet published or permanently refused) to the
global and per-table buffered counters and reports it as `quiver_state` and
`quiver_pressure_bps` against the `registration.buffered_*_limit` values.
`native_quiver_buffered_{bytes,rows,batches}` expose the backlog alone, and
the `arrowcol_dataflow_*` Prometheus metrics count durable admissions,
releases, admission failures, predecessor replays, rebinds, and deferrals.
Predecessor replays are not part of the restarted process's backlog.

## Container

`Dockerfile` is ready for local/CI builds and expects:

- `/run/secrets/arrowcol-app-password`;
- SQL Server DNS `sqlserver:1433`;
- Hive Metastore DNS `hive-metastore:9083`;
- writable `/onelake`; and
- for `config.quiver.yaml`, a writable `/var/lib/arrowcol/quiver` that is a
  persistent volume reattached to the restarted container.

The pinned `linux/amd64` runtime runs as UID/GID `10001`, keeps all four
embedded configs read-only, creates `/var/lib/arrowcol/quiver` writable by
UID `10001`, and exposes production readiness on port `32011`. Select the
durable topology with
`--config /etc/arrowcol-dataflow-rust/config.quiver.yaml`.
`image-smoke` validates every embedded YAML/capability graph, starts the
best-effort and durable memory topologies without credentials, and checks
readiness, the honest capability documents, and the Quiver WAL in the
default path. `publish` is clean-worktree guarded and writes its exact tag
manifest under `target/images/`.
