# Load-test observability

| File | Purpose |
| --- | --- |
| `throughput-dashboard.json` | Provisioned single-writer OAT verdict, committed throughput, bottlenecks, maintenance, QAP and telemetry overview |
| `arrowcol.json`, `delta-operations.json` | Existing collector and operational detail dashboards |
| `prometheus.yaml` | Scrapes dataflow, router, maintenance and tenant-stream metrics; cAdvisor observes Compose services |
| `capture.ts` | Captures any milestone immediately; comparisons require equal-length measurement windows |

Grafana is provisioned by the `lgtm` service in `projects/loadtest/compose.yaml`, which uses the digest-pinned `grafana/otel-lgtm` image.
The capture runner needs Playwright Chromium and its Linux libraries: `npx --no-install playwright install --with-deps chromium` (run where system packages can be installed).

```bash
GRAFANA_URL=http://127.0.0.1:3000 npx --no-install tsx projects/loadtest/observability/capture.ts \
  --baseline-run=baseline-run --baseline-variant=default \
  --baseline-from=2026-11-15T10:00:00Z --baseline-to=2026-11-15T10:05:00Z \
  --output-dir=/tmp/perf69-grafana
```

Replace the example labels and timestamps with actual OAT runs. Supply any of `--baseline-*`, `--bottleneck-*`, or `--best-*`; each selected milestone requires `run`, `variant`, `from` and `to`. For comparisons, provide two or three complete groups with equal-duration windows at their actual timestamps. `variant` is the emitted telemetry label, not the screenshot's presentation name; `--component` defaults to all services.

Supply windows with retained telemetry; the script never generates sample data. It waits for each panel's completed query frames and rendered content, or a visible no-data/unavailable state; missing responses, query errors and readiness timeouts fail capture. No-data panels are not proof of a healthy service. Set `GRAFANA_USER` and `GRAFANA_PASSWORD` only in the environment when the local Grafana login is enabled. Screenshots default to the OS temp directory and should be staged outside the repository.

The current QAP observer exports pending file count, accelerated percentage, unhealthy count and age derived from `LastUpdatedDateTime`. Pending size and native `NotHealthyReason` are not currently exported; those panels remain visibly unavailable rather than treating missing values as zero. Refresh age is not row latency, and accelerated percentage is not row completeness.
