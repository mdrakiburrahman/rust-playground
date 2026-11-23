import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fitsDiagnosticBound, memoryOutcome } from "./diagnostic-memory.js";
import { CLIENT_MEMORY, COLLECTOR_MEMORY, diagnosticPaths, realisticDiagnosticConfig } from "./realistic-diagnostic-config.js";
import {
  diagnosticInputBytes, diagnosticLaunchEnv, diagnosticPins, freezeDiagnosticComparisonProfile,
  requireMatchingCorpusReservation, validateDiagnosticCpuProof, validateDiagnosticProof,
  validateDiagnosticReferenceProof, validateDiagnosticRecheck, validateFrozenComparisonProfile,
  verifyComparisonProfileReference,
  validateHostAdmission, verifyDiagnosticHook,
} from "./realistic-diagnostic.js";
import type { OatMeasurement, TimedReceipt } from "./metrics-adapter.js";

type AdmissionProofFixture = {
  [key: string]: unknown;
  corpus_memory_reservation_bytes: number;
  expected_client_ids: string[];
  client_mapping: { table: string; client_ordinal: number; client_id: string };
  startup_accounting: { clients: Array<{ [key: string]: unknown; client_id: string }> };
  reference: unknown;
};

function diagnosticPinsFixture() {
  const hash = "a".repeat(64);
  return diagnosticPins({
    harness_revision: "c".repeat(40),
    source_revision: "d".repeat(40),
    startup_revision: "e".repeat(40),
    collector_source_revision: "b".repeat(40),
    controller_sha256: "f".repeat(64),
    corpus_sha256: "a".repeat(64),
    diagnostic_gate_sha256: "1".repeat(64),
    footer_sha256: "2".repeat(64),
    seed_sha256: "3".repeat(64),
    identity_tools: {
      decoder_sha256: hash, extractor_sha256: hash, node_sha256: hash, tsx_sha256: hash, verifier_sha256: hash,
    },
    images: Object.fromEntries(["catalog", "client", "collector", "maintenance", "router", "sqlserver"]
      .map(role => [role, `sha256:${hash}`])),
  });
}

function firstArmReceipt(expected: ReturnType<typeof diagnosticPins>): {
  [key: string]: unknown;
  diagnostic_admission: AdmissionProofFixture;
  pre_schedule_admission: AdmissionProofFixture;
} {
  const runId = "00000000-0000-4000-8000-000000000001";
  const clientId = `oat-${runId}-sqlserver_availability_replica_states-c1`;
  const startup = {
    schema_version: 1,
    capture_point: "client_ready_before_arm",
    clients: [{
      client_id: clientId,
      configured_live_slots: 28,
      admitted_live_slots: 28,
      live_identity_batch_bytes: 184_639_488,
      live_identity_batch_reservation_bytes: 28 * 184_639_488,
      corpus_memory_reservation_bytes: 100,
      memory_reservation_bytes: 100 + 28 * 184_639_488,
      max_memory_bytes: CLIENT_MEMORY,
    }],
  };
  const k = {state:"running",image:`sha256:${"d".repeat(64)}`};
  const proof = () => ({
    collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: k,
    kind: "diagnostic-only",
    phase: "before_schedule",
    data_profile: "realistic",
    payload_corpus_sha256: expected.corpus,
    configured_live_slots: 28,
    admitted_live_slots: 28,
    live_identity_batch_bytes: 184_639_488,
    corpus_memory_reservation_bytes: 100,
    memory_reservation_bytes: 100 + 28 * 184_639_488,
    expected_client_ids: [clientId],
    client_mapping: { table: "sqlserver_availability_replica_states", client_ordinal: 1, client_id: clientId },
    startup_accounting: startup,
    reference: { mode: "first" },
    observed_at_unix_ms: 1000,
  });
  return {
    kind: "diagnostic-only",
    qualification: "not-evaluated",
    collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: k,
    reference_establishment: "explicit-first-arm", collector_cpu_evidence: true,
    snapshot_reconciled: true,
    run_id: runId,
    source_revision: expected.source,
    harness_revision: expected.harness,
    collector_source_revision: expected.collectorSource,
    startup_revision: expected.startup,
    diagnostic_gate_sha256: expected.gate,
    controller_sha256: expected.controller,
    footer_sha256: expected.footer,
    seed_sha256: expected.seed,
    corpus_sha256: expected.corpus,
    images: expected.images,
    diagnostic_profile: {
      data_profile: "realistic",
      string_width: 63,
      seed: 20260923,
      workers: 14,
      streams_per_worker: 2,
      runtime_threads: 14,
      connections_per_worker: 2,
      max_memory_bytes: CLIENT_MEMORY,
    },
    client_schedule: { effective: { schedule_created_at_unix_ms: 1001 } },
    diagnostic_admission: proof(),
    pre_schedule_admission: proof(),
  };
}

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

test("freezes a non-circular first-arm profile and binds its per-client reservation to the receipt", () => {
  const expected = diagnosticPinsFixture();
  const receiptBytes = Buffer.from(JSON.stringify(firstArmReceipt(expected)));
  const receiptSha256 = createHash("sha256").update(receiptBytes).digest("hex");
  const profile = freezeDiagnosticComparisonProfile(receiptBytes, receiptSha256, expected);
  assert.equal(profile.kind, "diagnostic-comparison-profile");
  assert.equal(profile.qualification, "not-evaluated");
  assert.equal(profile.reference_receipt_sha256, receiptSha256);
  assert.deepEqual(profile.clients, [{
    logical_client_id: "sqlserver_availability_replica_states:1",
    corpus_memory_reservation_bytes: 100,
  }]);

  const profileBytes = Buffer.from(JSON.stringify(profile));
  const profileSha256 = createHash("sha256").update(profileBytes).digest("hex");
  assert.deepEqual(validateFrozenComparisonProfile(profileBytes, profileSha256, expected), profile);
  assert.deepEqual(verifyComparisonProfileReference(profileBytes, profileSha256, receiptBytes, expected), profile);

  const staleReceipt = { ...firstArmReceipt(expected), footer_sha256: "6".repeat(64) };
  const staleReceiptBytes = Buffer.from(JSON.stringify(staleReceipt));
  assert.throws(() => freezeDiagnosticComparisonProfile(
    staleReceiptBytes, createHash("sha256").update(staleReceiptBytes).digest("hex"), expected,
  ));
  const changedReservation = {
    ...profile,
    clients: [{ logical_client_id: "sqlserver_availability_replica_states:1", corpus_memory_reservation_bytes: 101 }],
  };
  const changedBytes = Buffer.from(JSON.stringify(changedReservation));
  assert.throws(() => verifyComparisonProfileReference(
    changedBytes, createHash("sha256").update(changedBytes).digest("hex"), receiptBytes, expected,
  ));
  const extraClient = {
    ...profile,
    clients: [...profile.clients, { logical_client_id: "unexpected:2", corpus_memory_reservation_bytes: 100 }],
  };
  const extraBytes = Buffer.from(JSON.stringify(extraClient));
  assert.throws(() => validateFrozenComparisonProfile(
    extraBytes, createHash("sha256").update(extraBytes).digest("hex"), expected,
  ));

  const matchedReference = {
    mode: "matched" as const,
    kind: "diagnostic-comparison-profile" as const,
    qualification: "not-evaluated" as const,
    profile_sha256: profileSha256,
    receipt_sha256: receiptSha256,
    run_id: profile.reference_run_id,
    corpus_sha256: profile.corpus_sha256, collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: profile.kustainer,
    clients: profile.clients,
  };
  const later = firstArmReceipt(expected);
  const laterId = "00000000-0000-4000-8000-000000000002";
  const laterClientId = `oat-${laterId}-sqlserver_availability_replica_states-c1`;
  for (const proof of [later.diagnostic_admission, later.pre_schedule_admission]) {
    proof.expected_client_ids = [laterClientId];
    proof.client_mapping.client_id = laterClientId;
    proof.startup_accounting.clients[0]!.client_id = laterClientId;
    proof.reference = matchedReference;
  }
  assert.doesNotThrow(() => validateDiagnosticReferenceProof(
    [later.diagnostic_admission, later.pre_schedule_admission], matchedReference,
  ));
  const mismatch = structuredClone(later.diagnostic_admission);
  mismatch.corpus_memory_reservation_bytes = 101;
  mismatch.startup_accounting.clients[0]!.corpus_memory_reservation_bytes = 101;
  assert.throws(() => validateDiagnosticReferenceProof([mismatch], matchedReference));
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
  const cpus = Array.from({ length: 8 }, (_, i) => i);
  const config = realisticDiagnosticConfig(original, cpus) as any;
  assert.deepEqual(original, copy);
  assert.equal(config.tunables.client.data_profile, "realistic");
  assert.equal(config.tunables.client.string_width, 63);
  assert.equal(config.tunables.client.seed, 20260923);
  assert.equal(config.tunables.client.workers, 14);
  assert.equal(config.tunables.client.streams_per_worker, 2);
  assert.equal(config.tunables.client.max_memory_bytes, CLIENT_MEMORY);
  assert.equal(config.tunables.client.stream_retry_max_attempts, 1);
  assert.equal(config.tunables.transport.connect_retries, 0);
  assert.equal(config.platform.local.collector_memory_limit_bytes, COLLECTOR_MEMORY);
  const second = realisticDiagnosticConfig(await seed(), cpus);
  assert.deepEqual(config.platform, second.platform);
  assert.equal(config.platform.compose_project, "diagnostic-stable");
  for (const n of [8, 16, 24]) {
    const mask = Array.from({ length: n }, (_, i) => i);
    const arm = realisticDiagnosticConfig(original, mask) as any;
    assert.deepEqual(arm.platform.local.collector_cpus, mask);
    assert.deepEqual(arm.dfe_worker_concurrency, [n]);
    const expectedCollector = structuredClone(original.tunables.collector);
    expectedCollector.runtime.cpu_workers = n;
    expectedCollector.pressure.rss_limit_bytes = COLLECTOR_MEMORY;
    assert.deepEqual(arm.tunables.collector, expectedCollector);
  }
  assert.throws(() => realisticDiagnosticConfig(original, [...cpus, 8]));
  assert.deepEqual(config.overload_guard, {...original.overload_guard, memory_signal: "cgroup_memory"});
  assert.deepEqual(config.tables, ["sqlserver_availability_replica_states"]);
  assert.deepEqual(config.replica_counts, [1]);
});

test("refuses drift in pressure, guard and collector runtime", async () => {
  for (const mutate of [
    (s: any) => { s.tunables.collector.pressure.buffered_rows_limit--; },
    (s: any) => { s.tunables.collector.commit.max_pending_files--; },
    (s: any) => { s.tunables.collector.runtime.cpu_workers--; },
    (s: any) => { s.dfe_worker_concurrency = [8]; },
    (s: any) => { s.overload_guard.zero_progress_windows = 3; },
    (s: any) => { s.platform.local.collector_memory_limit_bytes--; },
  ]) {
    const changed = await seed();
    mutate(changed);
    assert.throws(() => realisticDiagnosticConfig(changed, [0,1,2,3,4,5,6,7]));
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
  const hook = "let _ = diagnostic_admission_or_cancel(config, platform, &handles, &ready_clients, project_run_id, false).await?;";
  const recheck = "let schedule_admission = diagnostic_recheck_or_cancel(config, platform, &handles, project_run_id).await?;";
  const memoryHook = "diagnostic_memory_admission_or_cancel(config, platform, &handles).await?;";
  const scheduleGate = "schedule_after_diagnostic_admission(";
  const armCallback = "|| platform.arm_clients(config, project_run_id, run_start_unix_ms)";
  const precheck = 'let darwin_precheck = std::env::var("OAT_DARWIN_PARITY_PRECHECK");';
  const schedule = "let schedule_deadline = tokio::time::Instant::now();";
  const source = `${include}\n${ready}\n${hook}\n${precheck}\n${recheck}\n${memoryHook}\n${schedule}\n${scheduleGate}\n${armCallback}`;
  assert.doesNotThrow(() => verifyDiagnosticHook(source));
  assert.throws(() => verifyDiagnosticHook(source.replace(`${scheduleGate}\n${armCallback}`, armCallback)));
  assert.throws(() => verifyDiagnosticHook(source.replace(recheck, "")));
  assert.throws(() => verifyDiagnosticHook(source.replace(memoryHook, "")));
  assert.throws(() => verifyDiagnosticHook(source.replace(hook, `// ${hook}`)));
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
      live_identity_batch_bytes: 184_639_488, live_identity_batch_reservation_bytes: 28 * 184_639_488,
      corpus_memory_reservation_bytes: 100, memory_reservation_bytes: 100 + 28 * 184_639_488,
      max_memory_bytes: CLIENT_MEMORY }] };
  const proof = { kind: "diagnostic-only", phase: "before_schedule", payload_corpus_sha256: corpus,
    data_profile: "realistic", client_mapping: { table: "sqlserver_availability_replica_states", client_ordinal: 1, client_id: clientId },
    configured_live_slots: 28, admitted_live_slots: 28, live_identity_batch_bytes: 184_639_488,
    corpus_memory_reservation_bytes: 100, memory_reservation_bytes: 100 + 28 * 184_639_488,
    observed_at_unix_ms: 1000, expected_client_ids: [clientId], startup_accounting: startup };
  const line = `[oat] diagnostic admission: ${JSON.stringify(proof)}\n`;
  const schedule = { effective: { schedule_created_at_unix_ms: 1001 } };
  assert.deepEqual(validateDiagnosticProof(line, corpus, schedule), proof);
  const recheckLine = line.replace("diagnostic admission:", "diagnostic pre-schedule admission:");
  assert.deepEqual(validateDiagnosticProof(recheckLine, corpus, schedule, true), proof);
  assert.throws(() => validateDiagnosticProof(line, corpus, schedule, true));
  assert.throws(() => validateDiagnosticProof("", corpus, schedule));
  assert.throws(() => validateDiagnosticProof(line + line, corpus, schedule));
  assert.throws(() => validateDiagnosticProof(line, "b".repeat(64), schedule));
  assert.throws(() => validateDiagnosticProof(line, corpus, { effective: { schedule_created_at_unix_ms: 999 } }));
  assert.throws(() => validateDiagnosticProof(line.replace('"admitted_live_slots":28', '"admitted_live_slots":27'), corpus, schedule));
  const wrongStartup = { ...proof, startup_accounting: { ...startup, clients: [{ ...startup.clients[0], live_identity_batch_bytes: 0 }] } };
  assert.throws(() => validateDiagnosticProof(`[oat] diagnostic admission: ${JSON.stringify(wrongStartup)}\n`, corpus, schedule));
});

test("requires consistent live recheck before host memory readback", () => {
  const first = { startup_accounting: { clients: [{ id: "client", corpus: 3 }] }, client_mapping: { id: "client" },
    cpu_isolation: { cpus: [1, 2] }, observed_at_unix_ms: 10 };
  const second = { ...first, observed_at_unix_ms: 11 };
  const host = { observed_at_unix_ms: 12 };
  assert.doesNotThrow(() => validateDiagnosticRecheck(first, second, host));
  assert.throws(() => validateDiagnosticRecheck(first, { ...second, startup_accounting: {} }, host));
  assert.throws(() => validateDiagnosticRecheck(first, { ...second, cpu_isolation: {} }, host));
  assert.throws(() => validateDiagnosticRecheck(first, { ...second, observed_at_unix_ms: 9 }, host));
  assert.throws(() => validateDiagnosticRecheck(first, second, { observed_at_unix_ms: 10 }));
});

test("compares observed corpus reservations across diagnostic arms", () => {
  const receipt = (bytes: unknown, run: string) => {
    const clientId = `oat-00000000-0000-4000-8000-${run}-sqlserver_availability_replica_states-c1`;
    return { kind: "diagnostic-only", qualification: "not-evaluated", seed_sha256: "b".repeat(64),
    diagnostic_profile: { data_profile: "realistic", string_width: 63, seed: 20260923, workers: 14, streams_per_worker: 2,
      runtime_threads: 14, connections_per_worker: 2, max_memory_bytes: CLIENT_MEMORY },
    diagnostic_admission: { kind: "diagnostic-only", phase: "before_schedule",
      data_profile: "realistic", payload_corpus_sha256: "a".repeat(64), expected_client_ids: [clientId],
      client_mapping: { table: "sqlserver_availability_replica_states", client_ordinal: 1, client_id: clientId },
      startup_accounting: { schema_version: 1, capture_point: "client_ready_before_arm",
        clients: [{ client_id: clientId, configured_live_slots: 28, admitted_live_slots: 28,
          live_identity_batch_bytes: 184_639_488, live_identity_batch_reservation_bytes: 28 * 184_639_488,
          corpus_memory_reservation_bytes: bytes,
          memory_reservation_bytes: typeof bytes === "number" ? bytes + 28 * 184_639_488 : bytes,
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
  const legacyWidth = receipt(123, b);
  Reflect.deleteProperty(legacyWidth.diagnostic_profile, "string_width");
  assert.throws(() => requireMatchingCorpusReservation(receipt(123, a), legacyWidth));
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
  const reference = { mode: "first" as const };
  const env = diagnosticLaunchEnv("/run/home", 8 * 1024 * 1024, "/run/tools", pin, [8, 9], reference);
  assert.equal(env.OAT_DIAGNOSTIC_CORPUS_SHA256, pin);
  assert.equal(env.OAT_DARWIN_PARITY_PRECHECK, "1");
  assert.equal(env.OAT_DIAGNOSTIC_CLIENT_CPUS, "8,9");
  assert.equal(env.OAT_DIAGNOSTIC_REFERENCE, JSON.stringify(reference));
  assert.throws(() => diagnosticLaunchEnv("/run/home", 8 * 1024 * 1024, "/run/tools", "bad", [8, 9], reference));
});

test("reserves a full 5% of the post-client admission bound", () => {
  const threshold = Math.ceil(COLLECTOR_MEMORY * 20 / 19);
  assert.equal(fitsDiagnosticBound(threshold), true);
  assert.equal(fitsDiagnosticBound(threshold - 1), false);
});

test("attributes only a complete guard and an identity-bound cgroup peak", () => {
  const id = "a".repeat(64);
  const admission = "[oat] diagnostic pre-schedule admission: " + JSON.stringify({cpu_isolation:{collector:{container_id:id}}});
  const cg={collector_id:id,ps_all_ids:[id],observed_at_unix_ms:100,docker:{id,status:"running",oom_killed:false,exit_code:0,restart_count:0},events:{max:0,oom:0,oom_kill:0},stat:{anon:2,file:3},limit_bytes:COLLECTOR_MEMORY,peak_bytes:123};
  const sample = {cgroup:cg,memory_signal:"cgroup_memory", guard_cap:COLLECTOR_MEMORY,guard_status:"complete",stop:null};
  const outcome = (overrides: Record<string, unknown>) => memoryOutcome(admission + "\n[oat] diagnostic stop memory: " + JSON.stringify(cg) + "\n[oat] diagnostic terminal memory: " + JSON.stringify({...sample,...overrides}));
  assert.equal(outcome({}).permits_cpu_attribution, true);
  assert.equal(outcome({cgroup:{...sample.cgroup,collector_id:"b".repeat(64)}}).permits_cpu_attribution, false);
  assert.equal(outcome({guard_status:"unavailable"}).guard_engaged, null);
  assert.equal(outcome({guard_status:"triggered",stop:{reason:"hard_cap_fraction",metric:"collector_rss_bytes"}}).memory_limited, true);
  assert.equal(outcome({guard_status:"triggered",stop:{reason:"zero_ingestion_commit_progress",metric:"new_ingestion_commits"}}).memory_limited, null);
  assert.equal(outcome({guard_status:"triggered",stop:{reason:"hard_cap_fraction",metric:"collector_cgroup_memory_bytes"}}).memory_limited,true);
  assert.equal(outcome({cgroup:{...cg,events:{...cg.events,max:1}}}).permits_cpu_attribution,false);
  assert.equal(outcome({cgroup:{...cg,events:{...cg.events,oom_kill:1}}}).memory_limited,true);
  assert.equal(outcome({cgroup:{...cg,docker:{...cg.docker,oom_killed:true},stat:null,events:null}}).memory_limited,true);
  assert.equal(outcome({cgroup:{...cg,stat:{anon:1}}}).memory_limited,null);
  assert.equal(outcome({memory_signal:"process_rss"}).permits_cpu_attribution,false);
  assert.equal(memoryOutcome("").permits_cpu_attribution, false);
});
