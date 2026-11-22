import assert from "node:assert/strict";
import { test } from "node:test";
import { adaptOatEvidence } from "./metrics-adapter.js";

function evidence() {
  return {
    durable_commits: {
      output_throughput: {
        measurement_start_unix_ms: 1000, measurement_end_unix_ms: 151000,
        window_seconds: 150, rows: 1500, parquet_bytes: 3000,
        rows_per_second: 10, bytes_per_row: 2,
        commit_latency_seconds: { p50: 0.1, p99: 0.5, samples: 5 },
      },
      complete_30_second_commit_windows: Array.from({ length: 5 }, (_, i) => ({
        measurement_start_unix_ms: 1000 + i * 30000,
        measurement_end_unix_ms: 31000 + i * 30000,
        window_seconds: 30, rows: 300, parquet_bytes: 600,
      })),
    },
    resources: { samples: [
      { sampled_at_unix_ms: 1001, metrics: { cpu_millicores: 100, memory_bytes: 200 } },
      { sampled_at_unix_ms: 150000, metrics: { cpu_millicores: 300, memory_bytes: 100 } },
    ] },
    downstream_status: "unavailable",
    downstream_comment: "Kustainer stopped",
  };
}

test("extracts commit-window rather than admission throughput", () => {
  const result = adaptOatEvidence({ ...evidence(), rows_per_second: 9000 });
  assert.equal(result.durableRowsPerSecond, 10);
  assert.equal(result.memoryBytesMaxObserved, 200);
  assert.equal(result.cpuMillicoresMean, 200);
  assert.equal(result.downstreamUnavailableReason, "Kustainer stopped");
});

test("rejects incomplete or inconsistent windows and unknown durable counts", () => {
  const gap = evidence();
  gap.durable_commits.complete_30_second_commit_windows[2].measurement_start_unix_ms++;
  assert.throws(() => adaptOatEvidence(gap), /bucket/);
  const missing = evidence();
  missing.durable_commits.complete_30_second_commit_windows.pop();
  assert.throws(() => adaptOatEvidence(missing), /buckets/);
  const inconsistent = evidence();
  inconsistent.durable_commits.output_throughput.rows_per_second = 100;
  assert.throws(() => adaptOatEvidence(inconsistent), /inconsistent/);
  const short = evidence();
  short.durable_commits.output_throughput.measurement_end_unix_ms--;
  assert.throws(() => adaptOatEvidence(short), /shorter/);
  const mismatchedBucket = evidence();
  mismatchedBucket.durable_commits.complete_30_second_commit_windows[1].rows++;
  assert.throws(() => adaptOatEvidence(mismatchedBucket), /bucket totals/);
});
