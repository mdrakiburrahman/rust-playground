import assert from "node:assert/strict";
import { test } from "node:test";
import { extractClientEvidence } from "./oat-result.js";

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
