# Kusto bridge

| Item | Status |
| --- | --- |
| Default loadtest | Native Kusto emulator QAP over external Delta tables; no bridge needed. |
| This service | Experimental HTTP/configuration/metrics skeleton; **no Delta reconciliation**. |
| Correctness use | Do not use the bridge as a substitute for native QAP. |
| Health | `GET /healthz`; container probe: `kusto-bridge healthcheck`. |
| Diagnostics | `GET /status`, `GET /metrics`, `POST /refresh?wait=60`. |
| Configuration | [Variables and HTTP contract](../../docs/operations/KUSTO_BRIDGE.md) |
