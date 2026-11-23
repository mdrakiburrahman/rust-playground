import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { CLIENT_MEMORY, COLLECTOR_MEMORY, diagnosticPaths, realisticDiagnosticConfig } from "./realistic-diagnostic-config.js";
import { diagnosticInputBytes, diagnosticLaunchEnv, diagnosticPins, requireMatchingCorpusReservation, validateDiagnosticCpuProof, validateDiagnosticProof, validateHostAdmission, verifyDiagnosticHook } from "./realistic-diagnostic.js";
import type { OatMeasurement, TimedReceipt } from "./metrics-adapter.js";

test("requires explicit full harness and collector provenance pins", () => {
  const hash = "a".repeat(64);
  const input = {
    collector_source_revision: "b".repeat(40), harness_revision: "c".repeat(40),
    source_revision: "d".repeat(40), startup_revision: "e".repeat(40),
    controller_sha256: hash, corpus_sha256: hash, diagnostic_gate_sha256: hash,
    footer_sha256: hash, seed_sha256: hash,
    identity_tools: { decoder_sha256: hash, extractor_sha256: hash, node_sha256: hash,
      tsx_sha256: hash, verifier_sha256: hash },
    images: Object.fromEntries(["catalog", "client", "collector", "maintenance", "router", "sqlserver"]
      .map(role => [role, `sha256:${hash}`])),
  };
  const parsed = diagnosticPins(input);
  assert.equal(parsed.harness, input.harness_revision);
  assert.equal(parsed.collectorSource, input.collector_source_revision);
  assert.equal(parsed.source, input.source_revision);
  const withoutHarness = Object.fromEntries(Object.entries(input).filter(([key]) => key !== "harness_revision"));
  assert.throws(() => diagnosticPins(withoutHarness));
  assert.throws(() => diagnosticPins({ ...input, harness_revision: "31ce130" }));
});

test("sums reconciled original bytes within the half-open commit window", () => {
  const measurement = { startUnixMs: 1000, endUnixMs: 3000 } as OatMeasurement;
  const evidence = { durable_commits: { output_throughput: { input_bytes: 300, input_bytes_per_second: 150 } } };
  const receipt = (version: number, inputBytes: number, finishedAtUnixMs: number) =>
    ({ tableId: "table", version, inputBytes, finishedAtUnixMs } as TimedReceipt);
  const selected = [receipt(1, 100, 1000), receipt(2, 200, 2999)];
  assert.deepEqual(diagnosticInputBytes(evidence, selected, measurement), { bytes: 300, bytesPerSecond: 150 });
  assert.throws(() => diagnosticInputBytes(evidence, [selected[0]!, receipt(2, 200, 3000)], measurement));
  assert.throws(() => diagnosticInputBytes(evidence, [selected[0]!, receipt(2, 200, 999)], measurement));
  assert.throws(() => diagnosticInputBytes(evidence, [selected[0]!, selected[0]!], measurement));
  assert.throws(() => diagnosticInputBytes(evidence, [receipt(1, Number.MAX_SAFE_INTEGER, 1000), receipt(2, 1, 1001)], measurement));
  assert.throws(() => diagnosticInputBytes({ durable_commits: { output_throughput: { input_bytes: 300, input_bytes_per_second: 151 } } }, selected, measurement));
});

async function seed() {
  return JSON.parse(await readFile(new URL("../../../experiments/darwin/historical-parity.oat.json", import.meta.url), "utf8"));
}

test("derives realistic diagnostic from an unchanged historical seed", async () => {
  const original = await seed();
  const copy = structuredClone(original);
  const config = realisticDiagnosticConfig(original) as any;
  assert.deepEqual(original, copy);
  assert.equal(config.tunables.client.data_profile, "realistic");
  assert.equal(config.tunables.client.seed, 20260923);
  assert.equal(config.tunables.client.workers, 14);
  assert.equal(config.tunables.client.streams_per_worker, 2);
  assert.equal(config.tunables.client.max_memory_bytes, CLIENT_MEMORY);
  assert.equal(config.tunables.client.stream_retry_max_attempts, 1);
  assert.equal(config.tunables.transport.connect_retries, 0);
  assert.equal(config.platform.local.collector_memory_limit_bytes, COLLECTOR_MEMORY);
  assert.deepEqual(config.tunables.collector, original.tunables.collector);
  assert.deepEqual(config.overload_guard, original.overload_guard);
  assert.deepEqual(config.tables, ["sqlserver_availability_replica_states"]);
  assert.deepEqual(config.replica_counts, [1]);
});

test("refuses drift in pressure, guard and collector runtime", async () => {
  for (const mutate of [
    (s: any) => { s.tunables.collector.pressure.buffered_rows_limit--; },
    (s: any) => { s.tunables.collector.commit.max_pending_files--; },
    (s: any) => { s.tunables.collector.runtime.cpu_workers--; },
    (s: any) => { s.overload_guard.zero_progress_windows = 3; },
    (s: any) => { s.platform.local.collector_memory_limit_bytes--; },
  ]) {
    const changed = await seed();
    mutate(changed);
    assert.throws(() => realisticDiagnosticConfig(changed));
  }
});

test("requires distinct, normalized run-owned diagnostic paths", () => {
  const root = "/runs/diagnostic-001";
  assert.equal(diagnosticPaths(root, `${root}/diagnostic-snapshots`, `${root}/diagnostic-output`).root, root);
  assert.throws(() => diagnosticPaths(root, `${root}/diagnostic-snapshots`, `${root}/diagnostic-snapshots`));
  assert.throws(() => diagnosticPaths(root, `${root}/diagnostic-snapshots`, "/runs/diagnostic-output"));
  assert.throws(() => diagnosticPaths(root, `${root}/diagnostic-snapshots`, `${root}/../diagnostic-output`));
  assert.throws(() => diagnosticPaths(root, `${root}/diagnostic-snapshots`, `${root}/state`));
});

test("requires a single diagnostic hook between readiness and parity precheck", () => {
  const include = 'include!("runner_realistic_diagnostic_admission.rs");';
  const ready = "let ready_clients = loop {";
  const hook = "diagnostic_admission_or_cancel(config, platform, &handles, &ready_clients).await?;";
  const memoryHook = "diagnostic_memory_admission_or_cancel(config, platform, &handles).await?;";
  const precheck = 'let darwin_precheck = std::env::var("OAT_DARWIN_PARITY_PRECHECK");';
  const schedule = "let schedule_deadline = tokio::time::Instant::now();";
  assert.doesNotThrow(() => verifyDiagnosticHook(`${include}\n${ready}\n${hook}\n${precheck}\n${memoryHook}\n${schedule}`));
  assert.throws(() => verifyDiagnosticHook(`${include}\n${ready}\n${precheck}\n${memoryHook}\n${schedule}`));
  assert.throws(() => verifyDiagnosticHook(`${include}\n${ready}\n${precheck}\n${hook}\n${memoryHook}\n${schedule}`));
  assert.throws(() => verifyDiagnosticHook(`${include}\n${ready}\n${hook}\n${hook}\n${precheck}\n${memoryHook}\n${schedule}`));
  assert.throws(() => verifyDiagnosticHook(`${include}\n${ready}\n// ${hook}\n${precheck}\n${memoryHook}\n${schedule}`));
  assert.throws(() => verifyDiagnosticHook(`${include}\n${ready}\n${hook}\n${precheck}\n${memoryHook}\nlet other = 1;\n${schedule}`));
});

test("requires a fresh pre-schedule host memory admission", () => {
  const available = 150_000_000 * 1024;
  const host = { kind: "diagnostic-only", phase: "immediately_before_schedule", mem_total_bytes: 200_000_000 * 1024,
    mem_available_bytes: available, client_memory_budget_bytes: CLIENT_MEMORY, collector_memory_limit_bytes: COLLECTOR_MEMORY,
    collector_limit_bound_bytes: Math.floor((available - CLIENT_MEMORY) * 0.75), observed_at_unix_ms: 1000 };
  const line = `[oat] diagnostic host admission: ${JSON.stringify(host)}\n`;
  const schedule = { effective: { schedule_created_at_unix_ms: 1001 } };
  assert.deepEqual(validateHostAdmission(line, schedule), host);
  assert.throws(() => validateHostAdmission(line + line, schedule));
  assert.throws(() => validateHostAdmission(line, { effective: { schedule_created_at_unix_ms: 999 } }));
  assert.throws(() => validateHostAdmission(line.replace(`"mem_available_bytes":${available}`, '"mem_available_bytes":1'), schedule));
});

test("requires a single matching pre-schedule diagnostic proof", () => {
  const corpus = "a".repeat(64);
  const clientId = "oat-00000000-0000-4000-8000-000000000001-sqlserver_availability_replica_states-c1";
  const startup = { schema_version: 1, capture_point: "client_ready_before_arm",
    clients: [{ client_id: clientId, configured_live_slots: 28, admitted_live_slots: 28,
      live_identity_batch_bytes: 152_920_064, live_identity_batch_reservation_bytes: 28 * 152_920_064,
      corpus_memory_reservation_bytes: 100, memory_reservation_bytes: 100 + 28 * 152_920_064,
      max_memory_bytes: CLIENT_MEMORY }] };
  const proof = { kind: "diagnostic-only", phase: "before_schedule", payload_corpus_sha256: corpus,
    data_profile: "realistic", client_mapping: { table: "sqlserver_availability_replica_states", client_ordinal: 1, client_id: clientId },
    configured_live_slots: 28, admitted_live_slots: 28, live_identity_batch_bytes: 152_920_064,
    corpus_memory_reservation_bytes: 100, memory_reservation_bytes: 100 + 28 * 152_920_064,
    observed_at_unix_ms: 1000, expected_client_ids: [clientId], startup_accounting: startup };
  const line = `[oat] diagnostic admission: ${JSON.stringify(proof)}\n`;
  const schedule = { effective: { schedule_created_at_unix_ms: 1001 } };
  assert.deepEqual(validateDiagnosticProof(line, corpus, schedule), proof);
  assert.throws(() => validateDiagnosticProof("", corpus, schedule));
  assert.throws(() => validateDiagnosticProof(line + line, corpus, schedule));
  assert.throws(() => validateDiagnosticProof(line, "b".repeat(64), schedule));
  assert.throws(() => validateDiagnosticProof(line, corpus, { effective: { schedule_created_at_unix_ms: 999 } }));
  assert.throws(() => validateDiagnosticProof(line.replace('"admitted_live_slots":28', '"admitted_live_slots":27'), corpus, schedule));
  const wrongStartup = { ...proof, startup_accounting: { ...startup, clients: [{ ...startup.clients[0], live_identity_batch_bytes: 0 }] } };
  assert.throws(() => validateDiagnosticProof(`[oat] diagnostic admission: ${JSON.stringify(wrongStartup)}\n`, corpus, schedule));
});

test("compares observed corpus reservations across diagnostic arms", () => {
  const receipt = (bytes: unknown, run: string) => {
    const clientId = `oat-00000000-0000-4000-8000-${run}-sqlserver_availability_replica_states-c1`;
    return { kind: "diagnostic-only", qualification: "not-evaluated", seed_sha256: "b".repeat(64),
    diagnostic_profile: { data_profile: "realistic", seed: 20260923, workers: 14, streams_per_worker: 2,
      runtime_threads: 14, connections_per_worker: 2, max_memory_bytes: CLIENT_MEMORY },
    diagnostic_admission: { kind: "diagnostic-only", phase: "before_schedule",
      data_profile: "realistic", payload_corpus_sha256: "a".repeat(64), expected_client_ids: [clientId],
      client_mapping: { table: "sqlserver_availability_replica_states", client_ordinal: 1, client_id: clientId },
      startup_accounting: { schema_version: 1, capture_point: "client_ready_before_arm",
        clients: [{ client_id: clientId, configured_live_slots: 28, admitted_live_slots: 28,
          live_identity_batch_bytes: 152_920_064, live_identity_batch_reservation_bytes: 28 * 152_920_064,
          corpus_memory_reservation_bytes: bytes,
          memory_reservation_bytes: typeof bytes === "number" ? bytes + 28 * 152_920_064 : bytes,
          max_memory_bytes: CLIENT_MEMORY }] } } };
  };
  const a = "000000000001";
  const b = "000000000002";
  assert.equal(requireMatchingCorpusReservation(receipt(123, a), receipt(123, b)), 123);
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), receipt(123, a)));
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), receipt(124, b)));
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), receipt(undefined, b)));
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), receipt(0, b)));
  const wrongProfile = receipt(123, b);
  wrongProfile.diagnostic_admission.data_profile = "diagnostic";
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), wrongProfile));
  const wrongWorkers = receipt(123, b);
  wrongWorkers.diagnostic_profile.workers = 8;
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), wrongWorkers));
  const wrongMapping = receipt(123, b);
  wrongMapping.diagnostic_admission.client_mapping.client_ordinal = 2;
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), wrongMapping));
});

test("requires configured, effective and physically disjoint CPU allocations", () => {
  const proof = { cpu_isolation: {
    collector: { container_id: "a".repeat(64), requested: [0, 1], configured: "0-1", effective: "0-1" },
    client: { container_id: "b".repeat(64), requested: [2, 3], configured: "2-3", effective: "2-3" },
    thread_siblings: [[0, [0, 1]], [1, [0, 1]], [2, [2, 3]], [3, [2, 3]]],
  } };
  assert.doesNotThrow(() => validateDiagnosticCpuProof(proof, [0, 1], [2, 3]));
  const wrongEffective = structuredClone(proof);
  wrongEffective.cpu_isolation.client.effective = "2";
  assert.throws(() => validateDiagnosticCpuProof(wrongEffective, [0, 1], [2, 3]));
  const sharedCore = structuredClone(proof);
  sharedCore.cpu_isolation.thread_siblings[0] = [0, [0, 2]];
  assert.throws(() => validateDiagnosticCpuProof(sharedCore, [0, 1], [2, 3]));
});

test("launch environment passes the required pin and parity precheck without inheriting overrides", () => {
  const pin = "a".repeat(64);
  const env = diagnosticLaunchEnv("/run/home", 8 * 1024 * 1024, "/run/tools", pin, [8, 9]);
  assert.equal(env.OAT_DIAGNOSTIC_CORPUS_SHA256, pin);
  assert.equal(env.OAT_DARWIN_PARITY_PRECHECK, "1");
  assert.equal(env.OAT_DIAGNOSTIC_CLIENT_CPUS, "8,9");
  assert.throws(() => diagnosticLaunchEnv("/run/home", 8 * 1024 * 1024, "/run/tools", "bad", [8, 9]));
});
