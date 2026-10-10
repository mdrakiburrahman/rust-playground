# loadtest

Two Docker Compose services: the locally built Rust collector and the existing
hello-world app emitting OTLP logs, metrics, and traces. No external backend or
cloud credentials are needed. Nothing is exposed on host ports.

```bash
npx --no-install nx run loadtest:up
npx --no-install nx run loadtest:logs
npx --no-install nx run loadtest:inspect
npx --no-install nx run loadtest:down
```

The demo greets once per interval until stopped. The root `onelake/demo/`
bind mount is gitignored and preserved when containers stop. Collector writes
use your non-root UID/GID, not privileged containers or checkout-wide chmod.
Re-running `up` rebuilds local source and recreates changed services.

## Architecture and what to observe

```text
[1] Local source                         [2] Nx + Docker Compose
    bin/hello-world/ -------------------> builds hello-world image
    submodules/otel-arrow/ -------------> builds otelcol-rust image
    (including uncommitted fork edits)            |
                                                  v
    +-------------------- private Compose network -------------------+
    |                                                                |
    | [3] hello-world             OTLP HTTP/protobuf                 |
    |     greeting log     --+                                       |
    |     greetings counter +----> [4] otelcol-rust :4318            |
    |     greeting span    --+          OTLP receiver                |
    |                                  -> Parquet exporter           |
    +------------------------------------------|---------------------+
                                               | flush + bind mount
                                               v
    [5] Local, gitignored onelake/
        demo/                     continuous run; retained after down
        e2e/e2e-<uuid>/            isolated finite test output
          logs/ + log_attrs/      greeting body + run.id
          univariate_metrics/     hello_world.greetings
          number_data_points/     counter values + joined attributes
          spans/ + span_attrs/    greeting spans + correlated IDs
                         |
                         v
    [6] loadtest:inspect / loadtest:e2e
        schemas, row counts, sample rows / content assertions + PASS
```

1. Edit the app or collector fork. Local edits enter the build without first
   publishing a package; see [collector development](../otelcol-rust/README.md).
2. `loadtest:up` builds both Dockerfiles and waits for collector readiness.
3. `loadtest:logs` shows repeated `Hello, World!` greetings and collector
   listener startup. The app exports a log, counter datapoint, and span.
4. The receiver accepts all three signals; the exporter closes Parquet files
   on its age threshold and during graceful shutdown.
5. Browse `onelake/demo/` for files, or a fresh `onelake/e2e/e2e-<uuid>/`
   after E2E. This is a **local folder**, not Microsoft Fabric OneLake.
6. `loadtest:inspect` shows schemas, counts, and two sample rows per table.
   `loadtest:e2e` must print `PASS` for marked logs, metric datapoints, and
   correlated traces; it does not treat file existence as successful delivery.

## E2E acceptance

```bash
npx --no-install nx run loadtest:e2e
```

The target builds both images and the Rust Parquet inspection helper. It creates
a fresh Compose project and `onelake/e2e/e2e-<uuid>/` directory, waits for
collector readiness, generates three greetings, flushes the SDKs, stops the
collector gracefully, and reads the closed Parquet files.

Acceptance requires three marked logs and spans, valid correlated trace/span
IDs, the expected greeting log body, and a marked counter datapoint joined
to `hello_world.greetings` that reaches three. Old files cannot satisfy a
new run. A failed app, missing signal, invalid/corrupt file, unsuccessful
collector shutdown, or timeout fails the target. Scoped Compose resources are
always removed; output and `compose.log` remain for inspection.

On successful runs, `inspection.json` contains the full finite dataset.
To inspect a specific run:

```bash
ONELAKE_INSPECT_PATH=onelake/e2e/e2e-<uuid> \
  npx --no-install nx run loadtest:inspect
```

Inspection prints schemas, row counts, and two sample rows per table.
Continuous demo data accumulates; stop the demo before inspecting a stable
snapshot. Delete only specific unwanted run directories when finished.

## Parquet layout

The experimental exporter writes OTAP payload tables, not three flattened
JSON-like tables:

| Signal  | Main tables                                | Associated attributes |
| ------- | ------------------------------------------ | --------------------- |
| Logs    | `logs`                                     | `log_attrs`           |
| Metrics | `univariate_metrics`, `number_data_points` | `number_dp_attrs`     |
| Traces  | `spans`                                    | `span_attrs`          |

Metric datapoints reference metric IDs; attribute rows reference their record's
ID with `parent_id`. `resource_attrs` and `scope_attrs` provide additional
metadata. Trace/span IDs correlate the greeting logs with their spans.

The Rust helper depends only on the Parquet reader and JSON serialization, not
on Python/DuckDB installations or a downloaded inspection image.
For custom Docker-outside-Docker setups, set `LOADTEST_HOST_WORKSPACE` to the
daemon-visible checkout path. Standard devcontainers use their own mount
metadata automatically.
