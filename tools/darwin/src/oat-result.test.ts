import assert from "node:assert/strict";
import { test } from "node:test";
import { extractClientEvidence, extractStoppedReceipts } from "./oat-result.js";
import type { OatMeasurement } from "./metrics-adapter.js";

test("requires matching business corpus evidence and reports client CPU separately", () => {
  const hash = "a".repeat(64);
  const report = { phase_counters: {
    preparation: { payload_corpus_sha256: hash },
    payload_manifest: { payload_corpus_sha256: hash, payload_corpus_hash_scope: "arrow-ipc-v1-excluding-endpoint_url", identity_status: "identity_bearing", complete: true },
    measurement_window: { client_resources: { cpu_core_equivalents: 2.5, start_sample_unix_ms: 100, end_sample_unix_ms: 200 } },
  } };
  assert.equal(extractClientEvidence(report).payloadCorpusSha256, hash);
  assert.equal(extractClientEvidence(report).cpu.coreEquivalents, 2.5);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = "b".repeat(64);
  assert.throws(() => extractClientEvidence(report), /corpus evidence/);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = hash;
  report.phase_counters.measurement_window.client_resources.cpu_core_equivalents = -1;
  assert.throws(() => extractClientEvidence(report), /CPU evidence/);
});

test("extends the frozen window with complete stopped collector receipts", () => {
  const measurement: OatMeasurement = {
    startUnixMs: 1000, endUnixMs: 151000, rows: 3, parquetBytes: 60, files: 1, commits: 1, completeBuckets: 5,
    durableRowsPerSecond: 3 / 150, parquetBytesPerRow: 20,
    commitLatencySeconds: { p50: 0.1, p99: 0.1, samples: 1 },
    cpuMillicoresMean: null, memoryBytesMaxObserved: null, rssBytesMaxObserved: null,
    rssUnavailableReason: "unavailable", resourceSampleCount: 0,
    downstreamRowsPerSecond: null, downstreamUnavailableReason: "unavailable",
  };
  const receipt = {
    table_id: "one-id", table_name: "one", delta_version: 2, rows: 3, files: 1,
    input_bytes: 100, parquet_bytes: 60, min_ingested_at_ms: 0, max_ingested_at_ms: 100,
    finished_at_unix_ms: 1000, elapsed_seconds: 0.1,
  };
  const drain = { ...receipt, delta_version: 3, rows: 5, finished_at_unix_ms: 151000 };
  const cumulative = (items: typeof receipt[]) => ({
    scope: "all_new_ingestion_commits_from_this_cell_including_warmup",
    rows: items.reduce((sum, item) => sum + item.rows, 0), commits: items.length, commits_by_version: items,
  });
  const evidence = { durable_commits: { run_cumulative_output: cumulative([receipt]) } };
  const stopped = (items: typeof receipt[]) => ({
    schema_version: 1, run_id: "run", writers: [{
      service: "dataflow-1", was_running: true, oom_killed: false, exit_code: 0,
      finished_at: "2026-01-01T00:00:00Z",
      stopped_commit_output: { source: "complete_stopped_collector_logs", tables: { one: cumulative(items) } },
    }],
  });
  const read = (snapshot: unknown) => extractStoppedReceipts(evidence, snapshot, "run", "one", measurement);
  assert.deepEqual(read(stopped([receipt, drain])).all.map(item => item.version), [2, 3]);
  assert.deepEqual(read(stopped([receipt, drain])).selected.map(item => item.version), [2]);
  assert.throws(() => read(stopped([{ ...receipt, elapsed_seconds: 0.2 }, drain])), /disagree/);
  assert.throws(() => read(stopped([drain])), /receipt/);
  const incomplete = stopped([receipt, drain]);
  incomplete.writers[0]!.stopped_commit_output.source = "incomplete";
  assert.throws(() => read(incomplete), /complete stopped/);
  assert.throws(() => extractStoppedReceipts(evidence, stopped([receipt, drain]), "wrong", "one", measurement), /identity/);
});
