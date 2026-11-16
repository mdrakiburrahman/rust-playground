# Load-test observability

| File | Purpose |
| --- | --- |
| `throughput-dashboard.json` | Provisioned single-writer OAT verdict, committed throughput, bottlenecks, maintenance, QAP and telemetry overview |
| `arrowcol.json`, `delta-operations.json` | Existing collector and operational detail dashboards |
| `prometheus.yaml` | Scrapes dataflow, router, maintenance and tenant-stream metrics; cAdvisor observes Compose services |
| `capture.ts` | Captures baseline, diagnosed-bottleneck and best-candidate screenshots over equal-length measurement windows |

Grafana is provisioned by the `lgtm` service in `projects/loadtest/compose.yaml`, which uses the digest-pinned `grafana/otel-lgtm` image.
The capture script needs the Playwright Chromium runtime: `npx --no-install playwright install chromium`.

```bash
GRAFANA_URL=http://127.0.0.1:3000 npx --no-install tsx projects/loadtest/observability/capture.ts \
  --baseline-run=baseline-run --baseline-from=2026-11-15T10:00:00Z --baseline-to=2026-11-15T10:05:00Z \
  --bottleneck-run=diagnosed-run --bottleneck-from=2026-11-15T12:00:00Z --bottleneck-to=2026-11-15T12:05:00Z \
  --best-run=best-run --best-from=2026-11-15T14:00:00Z --best-to=2026-11-15T14:05:00Z \
  --output-dir=/tmp/perf69-grafana
```

Replace the example IDs and timestamps with actual OAT runs. The three captures use equal-length windows at each run's timestamps and select their fixed `run`, `variant` and `component` labels. Supply windows with retained telemetry; the script never generates sample data. Set `GRAFANA_USER` and `GRAFANA_PASSWORD` only in the environment when the local Grafana login is enabled. Screenshots default to the OS temp directory and should be staged outside the repository.

The current QAP observer exports pending file count, accelerated percentage, unhealthy count and age derived from `LastUpdatedDateTime`. Pending size and native `NotHealthyReason` are not currently exported; those panels remain visibly unavailable rather than treating missing values as zero. Refresh age is not row latency, and accelerated percentage is not row completeness.
