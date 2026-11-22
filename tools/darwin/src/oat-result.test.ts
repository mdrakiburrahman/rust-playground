import assert from "node:assert/strict";
import { test } from "node:test";
import { assertParityProcessRevision, extractClientEvidence, extractStoppedReceipts, readReconciledOatResult, type ClientDataProfile } from "./oat-result.js";
import type { OatMeasurement } from "./metrics-adapter.js";

function reportFor(profile: ClientDataProfile) {
  const hash = "a".repeat(64);
  const scope = profile === "diagnostic" ? "arrow-logical-v2-all-columns" : "arrow-logical-v2-excluding-endpoint_url";
  const identityEligible = profile === "realistic";
  const preparation: Record<string, unknown> = {
    data_profile: profile, steady_state_generation: identityEligible,
    payload_corpus_sha256: hash, payload_corpus_hash_version: 2, payload_corpus_hash_scope: scope,
  };
  const payloadManifest: Record<string, unknown> = {
    data_profile: profile, payload_corpus_sha256: hash, payload_corpus_hash_version: 2,
    payload_corpus_hash_scope: scope, identity_status: identityEligible ? "identity_bearing" : "diagnostic_only",
    eligible_for_identity_reconciliation: identityEligible, complete: identityEligible, truncated: false,
  };
  const clientResources: Record<string, unknown> = {
    cpu_core_equivalents: 2.5, start_sample_unix_ms: 100, end_sample_unix_ms: 200,
  };
  return { phase_counters: { preparation, payload_manifest: payloadManifest, measurement_window: { client_resources: clientResources } } };
}

test("a control revision applies only to the collector", () => {
  const collector = { component: "collector", source_revision: "main" };
  const router = { component: "router", source_revision: "observer" };
  assert.doesNotThrow(() => assertParityProcessRevision(collector, "observer", "main"));
  assert.doesNotThrow(() => assertParityProcessRevision(router, "observer", "main"));
  assert.throws(() => assertParityProcessRevision(collector, "observer"), /source revision/);
  assert.throws(() => assertParityProcessRevision({ ...router, source_revision: "main" }, "observer", "main"), /source revision/);
});

test("refuses a diagnostic result without the physical parity gate", async () => {
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", null, "diagnostic"),
    /requires a physical footer inspector/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "diagnostic", {
    config: {}, decodeFile: async () => 0,
  }), /requires a physical footer inspector/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", null, "realistic"),
    /requires its Zstd level/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "realistic"),
    /requires full delivery identity reconciliation/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "realistic", {
    config: {}, decodeFile: async () => 0,
  }), /explicitly qualified parity gate/);
});

test("accepts matching Realistic evidence and reports client CPU separately", () => {
  const report = reportFor("realistic");
  const evidence = extractClientEvidence(report, "realistic");
  assert.equal(evidence.profile, "realistic");
  assert.equal(evidence.identityEligibleForReconciliation, true);
  assert.equal(evidence.payloadCorpusSha256, "a".repeat(64));
  assert.equal(evidence.cpu.coreEquivalents, 2.5);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = "b".repeat(64);
  assert.throws(() => extractClientEvidence(report, "realistic"), /corpus evidence/);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = "a".repeat(64);
  report.phase_counters.measurement_window.client_resources.cpu_core_equivalents = -1;
  assert.throws(() => extractClientEvidence(report, "realistic"), /CPU evidence/);
});

test("accepts honest Diagnostic evidence without claiming unique delivery", () => {
  const report = reportFor("diagnostic");
  assert.equal(report.phase_counters.payload_manifest.complete, false);
  const evidence = extractClientEvidence(report, "diagnostic");
  assert.equal(evidence.profile, "diagnostic");
  assert.equal(evidence.identityEligibleForReconciliation, false);
  assert.equal(evidence.cpu.coreEquivalents, 2.5);
});

test("rejects profile, hash contract, unavailable, and truncated evidence", () => {
  const mismatchedProfile = reportFor("realistic");
  assert.throws(() => extractClientEvidence(mismatchedProfile, "diagnostic"), /corpus evidence/);

  const invalidEvidence = [
    (report: ReturnType<typeof reportFor>) => { delete report.phase_counters.preparation.payload_corpus_sha256; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.payload_corpus_hash_version = 1; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.preparation.payload_corpus_hash_scope = "wrong-scope"; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.truncated = true; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.eligible_for_identity_reconciliation = false; },
  ];
  for (const corrupt of invalidEvidence) {
    const report = reportFor("realistic");
    corrupt(report);
    assert.throws(() => extractClientEvidence(report, "realistic"), /corpus evidence/);
  }
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
