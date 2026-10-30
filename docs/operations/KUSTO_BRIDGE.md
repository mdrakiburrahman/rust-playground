# kusto-bridge

Experimental, non-default sidecar for the local Kusto emulator. It is **not** used by the loadtest
and is not needed: the emulator's native query acceleration over local Delta external tables
converged to the committed snapshot (appends, `OPTIMIZE`, partitioned purge deletes, `VACUUM`)
with `MaxAge=00:01:00` in local probes, so native external `kind=delta` tables with that policy
are the default (see `projects/loadtest/loadgen/src/kusto.rs`).

> Status: service skeleton only (HTTP surface, configuration, metrics, healthcheck). It performs
> no Delta reconciliation and must not be relied on for correctness. It is kept so a sidecar can
> be revisited if native acceleration regresses.

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
