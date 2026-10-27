# kusto-bridge

Sidecar for the local Kusto emulator that keeps one Kusto table per Delta table equal to the
table's latest committed snapshot. It exists because the emulator accepts query acceleration
policies over local Delta external tables, but its accelerated cache did not refresh after
appends, `OPTIMIZE` rewrites, or removals in local probes. Plain (non-accelerated) Delta external
tables stayed correct and are kept as the correctness baseline.

> Status: service skeleton. The HTTP surface, configuration, metrics, and container healthcheck
> are in place; the Delta snapshot reconciler lands in the next slice.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `KUSTO_BRIDGE_KUSTO_ENDPOINT` | required | Emulator origin, for example `http://10.89.0.50:8080`. |
| `KUSTO_BRIDGE_DATABASE` | `loadtest` | Target database. |
| `KUSTO_BRIDGE_SOURCES` | required | `pool=/path` pairs, comma separated. Paths must be mounted at the same location in the emulator. |
| `KUSTO_BRIDGE_STATE_DIR` | `/state` | Restart checkpoint directory. |
| `KUSTO_BRIDGE_POLL_INTERVAL` | `2s` | Delta log poll interval. |
| `KUSTO_BRIDGE_LISTEN` | `0.0.0.0:9464` | HTTP listen address. |
| `KUSTO_BRIDGE_REQUEST_TIMEOUT` | `120s` | Per-request Kusto timeout. |
| `KUSTO_BRIDGE_RESYNC_ON_IDENTITY_CHANGE` | `false` | Rebuild instead of degrading when a table's identity or schema changes. |

## HTTP

- `GET /healthz` returns `ok` while the process serves requests.
- `GET /status` returns per-table `source_version`, `applied_version`, `lag_versions`,
  `visibility_lag_seconds`, `active_files`, `pending_files`, `state`, and `reason`.
- `POST /refresh?wait=60` triggers a pass and waits for it, returning `/status`.
- `GET /metrics` exposes `kusto_bridge_*` Prometheus series labelled only by `pool`, `outcome`,
  `reason`, or `operation`.

The container healthcheck runs `kusto-bridge healthcheck`; the image does not ship curl.
