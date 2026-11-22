import assert from "node:assert/strict";
import { test } from "node:test";
import { adaptOatEvidence, extractTimedReceipts } from "./metrics-adapter.js";

function evidence() {
  return {
    durable_commits: {
      output_throughput: {
        scope: "new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort",
        clock: "collector_system_unix_ms_shared_schedule_clock_skew_not_measured",
        warmup_backlog: "included_when_commit_finishes_in_window",
        rewrite_commits: "excluded_by_ingestion_only_producer_capability",
        measurement_start_unix_ms: 1000, measurement_end_unix_ms: 151000,
        window_seconds: 150, rows: 1500, files: 5, commits: 5, parquet_bytes: 3000,
        rows_per_second: 10, bytes_per_row: 2,
        commit_latency_seconds: { p50: 0.1, p99: 0.5, samples: 5, scope: "commit_retry_and_sync_not_encoding_or_admission" },
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
  assert.equal(result.resourceSampleCount, 2);
  assert.equal(result.rssBytesMaxObserved, null);
  assert.match(result.rssUnavailableReason, /do not expose/);
  assert.equal(result.commitLatencySeconds.samples, 5);
  assert.equal(result.downstreamRowsPerSecond, null);
  assert.equal(result.downstreamUnavailableReason, "Kustainer stopped");
  assert.throws(() => adaptOatEvidence({ ...evidence(), downstream_comment: null }), /no reason/);
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

test("selects half-open timed version receipts and rejects missing or conflicting evidence", () => {
  const raw = evidence();
  const measurement = adaptOatEvidence(raw);
  const receipts = Array.from({ length: 7 }, (_, i) => ({
    table_id: "one-id", table_name: "one", delta_version: i, rows: 300, files: 1,
    input_bytes: 900, parquet_bytes: 600, min_ingested_at_ms: 0, max_ingested_at_ms: 100,
    finished_at_unix_ms: [999, 1000, 31000, 61000, 91000, 121000, 151000][i],
    elapsed_seconds: 0.1,
  }));
  const withReceipts = (list: unknown[]) => ({
    ...raw,
    durable_commits: { ...raw.durable_commits, run_cumulative_output: {
      scope: "all_new_ingestion_commits_from_this_cell_including_warmup", rows: 2100, commits: 7, commits_by_version: list,
    } },
  });
  assert.deepEqual(extractTimedReceipts(withReceipts(receipts), "one", measurement).map(r => r.version), [1, 2, 3, 4, 5]);
  assert.equal(extractTimedReceipts(withReceipts([...receipts, receipts[1]]), "one", measurement).length, 5);
  assert.throws(() => extractTimedReceipts(withReceipts(receipts.slice(1)), "one", measurement), /receipt/);
  assert.throws(() => extractTimedReceipts(withReceipts([...receipts, { ...receipts[1], rows: 301 }]), "one", measurement), /conflicting/);
  const missingTime = receipts.map((receipt, index) => index === 1 ? { ...receipt, finished_at_unix_ms: undefined } : receipt);
  assert.throws(() => extractTimedReceipts(withReceipts(missingTime), "one", measurement), /finish time/);
});
