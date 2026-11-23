import { execFileSync, spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, open, readFile, realpath, rmdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm";
import { STARTUP_ACCOUNTING_EXPECTED, requireMatchedStartupAccounting, requireStartupAccounting } from "../../../projects/arrowcol-oat/startup-accounting.js";
import { fitsDiagnosticBound, memoryOutcome } from "./diagnostic-memory.js";
import { cpuWindowReceipt } from "../../../projects/arrowcol-oat/diagnostic-cpu.js";
import * as oatResult from "./oat-result.js";
import { parseManifestJson } from "./delivery-identity.js";
import { extractClientEvidence, extractStoppedReceipts, readReconciledOatResult } from "./oat-result.js";
import type { OatMeasurement, TimedReceipt } from "./metrics-adapter.js";
import { assertParityEvidence, inspectParityHost } from "./parity-preflight.js";
import { buildFooterInspector, parseCollectorCpus, parityCpuAllocation, parityImageIds } from "./parity-replay.js";
import { cleanEnv, hasPinnedSubmodules, localReplayOptions, prepareIdentityTools, prepareReplayRuntime, sha256 } from "./replay.js";
import type { SnapshotManifest } from "./snapshot.js";
import { CLIENT_MEMORY, COLLECTOR_MEMORY, COLLECTOR_SOURCE_BASE, DIAGNOSTIC_STRING_WIDTH, diagnosticPaths, realisticDiagnosticConfig } from "./realistic-diagnostic-config.js";

type JsonObject = Record<string, unknown>;
const DIAGNOSTIC_WORKLOAD_PROFILE = Object.freeze({
  data_profile: "realistic", string_width: DIAGNOSTIC_STRING_WIDTH, seed: 20260923, workers: 14, streams_per_worker: 2,
  runtime_threads: 14, connections_per_worker: 2, max_memory_bytes: CLIENT_MEMORY,
});
const DIAGNOSTIC_LOGICAL_CLIENT = "sqlserver_availability_replica_states:1";
type DiagnosticClientReservation = { logical_client_id: string; corpus_memory_reservation_bytes: number };
type FrozenComparisonProfile = {
  schema_version: 1;
  kind: "diagnostic-comparison-profile";
  qualification: "not-evaluated";
  reference_receipt_sha256: string;
  reference_run_id: string;
  source_revision: string;
  harness_revision: string;
  collector_source_revision: string;
  startup_revision: string;
  diagnostic_gate_sha256: string;
  controller_sha256: string;
  footer_sha256: string;
  seed_sha256: string;
  corpus_sha256: string;
  images: unknown;
  collector_memory_limit_bytes: number; kustainer: Kustainer;
  diagnostic_profile: typeof DIAGNOSTIC_WORKLOAD_PROFILE;
  clients: DiagnosticClientReservation[];
};
type Kustainer = { state: string; image: string };
function kustainer(raw: unknown): Kustainer {
  const k = object(raw, "Kustainer observation");
  if ((k.state !== "running" && k.state !== "exited") || typeof k.image !== "string" || !/^sha256:[0-9a-f]{64}$/.test(k.image)) throw new Error("invalid Kustainer state or image");
  return {state:k.state, image:k.image};
}
type DiagnosticReference =
  | { mode: "first" }
  | { mode: "matched"; kind: "diagnostic-comparison-profile"; qualification: "not-evaluated";
      profile_sha256: string; receipt_sha256: string; run_id: string; corpus_sha256: string;
      collector_memory_limit_bytes: number; kustainer: Kustainer; clients: DiagnosticClientReservation[] };
type DiagnosticReferenceInput =
  | { mode: "establish" }
  | { mode: "compare"; profilePath: string; profileSha256: string; receiptPath: string };
function object(value: unknown, name: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as JsonObject;
}
function string(value: unknown, pattern: RegExp, name: string): string {
  if (typeof value !== "string" || !pattern.test(value)) throw new Error(`invalid ${name}`);
  return value;
}
export function diagnosticPins(raw: unknown) {
  const p = object(raw, "pins");
  if (Object.keys(p).sort().join(",") !== "collector_source_revision,controller_sha256,corpus_sha256,diagnostic_gate_sha256,footer_sha256,harness_revision,identity_tools,images,seed_sha256,source_revision,startup_revision") throw new Error("invalid pins fields");
  const images = object(p.images, "images");
  const identityTools = object(p.identity_tools, "identity tools");
  const identityFields = ["decoder_sha256", "extractor_sha256", "node_sha256", "tsx_sha256", "verifier_sha256"];
  if (Object.keys(identityTools).sort().join(",") !== identityFields.join(",")) throw new Error("invalid identity tool pins");
  for (const field of identityFields) string(identityTools[field], /^[a-f0-9]{64}$/, `identity tool ${field}`);
  const roles = ["catalog", "client", "collector", "maintenance", "router", "sqlserver"];
  if (Object.keys(images).sort().join(",") !== roles.join(",")) throw new Error("invalid image roles");
  for (const role of roles) string(images[role], /^sha256:[a-f0-9]{64}$/, `image ${role}`);
  return {
    source: string(p.source_revision, /^[a-f0-9]{40}$/, "source revision"),
    harness: string(p.harness_revision, /^[a-f0-9]{40}$/, "merged harness revision"),
    seed: string(p.seed_sha256, /^[a-f0-9]{64}$/, "seed hash"),
    controller: string(p.controller_sha256, /^[a-f0-9]{64}$/, "controller hash"),
    corpus: string(p.corpus_sha256, /^[a-f0-9]{64}$/, "corpus hash"),
    gate: string(p.diagnostic_gate_sha256, /^[a-f0-9]{64}$/, "diagnostic gate hash"),
    footer: string(p.footer_sha256, /^[a-f0-9]{64}$/, "footer inspector hash"),
    startup: string(p.startup_revision, /^[a-f0-9]{40}$/, "startup integration revision"),
    collectorSource: string(p.collector_source_revision, /^[a-f0-9]{40}$/, "collector source revision"),
    images, identityTools,
  };
}
function git(runtime: string, env: NodeJS.ProcessEnv, ...args: string[]) {
  return execFileSync("git", ["-C", runtime, ...args], { encoding: "utf8", env }).trimEnd();
}

function diagnosticClientMapping(proof: JsonObject): { clientId: string; logicalId: string; runId: string } {
  const mapping = object(proof.client_mapping, "diagnostic client mapping");
  if (Object.keys(mapping).sort().join(",") !== "client_id,client_ordinal,table"
    || mapping.table !== "sqlserver_availability_replica_states" || mapping.client_ordinal !== 1
    || !Array.isArray(proof.expected_client_ids) || proof.expected_client_ids.length !== 1
    || typeof mapping.client_id !== "string" || proof.expected_client_ids[0] !== mapping.client_id
    || proof.data_profile !== "realistic") throw new Error("diagnostic client mapping is missing or ambiguous");
  const match = /^oat-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})-sqlserver_availability_replica_states-c1$/.exec(mapping.client_id);
  if (!match) throw new Error("diagnostic client identity is not run-scoped");
  return { clientId: mapping.client_id, logicalId: `${mapping.table}:${mapping.client_ordinal}`, runId: match[1]! };
}

function referenceClients(proof: JsonObject): DiagnosticClientReservation[] {
  const mapping = diagnosticClientMapping(proof);
  const accounting = requireStartupAccounting(proof.startup_accounting, [mapping.clientId], "before_arm");
  if (accounting.clients.length !== 1) throw new Error("comparison profile client roster is incomplete or contains extra clients");
  const client = accounting.clients[0]!;
  if (!Number.isSafeInteger(client.corpus_memory_reservation_bytes) || client.corpus_memory_reservation_bytes <= 0) {
    throw new Error("comparison profile corpus reservation is missing or invalid");
  }
  return [{ logical_client_id: mapping.logicalId, corpus_memory_reservation_bytes: client.corpus_memory_reservation_bytes }];
}

export function freezeDiagnosticComparisonProfile(
  receiptBytes: Buffer,
  receiptSha256: string,
  expected: ReturnType<typeof diagnosticPins>,
): FrozenComparisonProfile {
  string(receiptSha256, /^[a-f0-9]{64}$/, "reference receipt hash");
  if (!receiptBytes.length || receiptBytes.length > 16 * 1024 * 1024
    || createHash("sha256").update(receiptBytes).digest("hex") !== receiptSha256) {
    throw new Error("reference receipt hash differs");
  }
  const text = new TextDecoder("utf-8", { fatal: true }).decode(receiptBytes);
  parseManifestJson(text);
  const receipt = object(JSON.parse(text), "reference receipt");
  const initial = object(receipt.diagnostic_admission, "reference admission");
  const recheck = object(receipt.pre_schedule_admission, "reference pre-schedule admission");
  const schedule = object(receipt.client_schedule, "reference schedule");
  if (receipt.kind !== "diagnostic-only" || receipt.qualification !== "not-evaluated"
    || receipt.collector_memory_limit_bytes !== COLLECTOR_MEMORY || initial.collector_memory_limit_bytes !== COLLECTOR_MEMORY
    || recheck.collector_memory_limit_bytes !== COLLECTOR_MEMORY || !isDeepStrictEqual(kustainer(receipt.kustainer), initial.kustainer)
    || !isDeepStrictEqual(receipt.kustainer, recheck.kustainer) || receipt.collector_cpu_evidence !== true || receipt.reference_establishment !== "explicit-first-arm" || receipt.snapshot_reconciled !== true
    || receipt.source_revision !== expected.source || receipt.harness_revision !== expected.harness
    || receipt.collector_source_revision !== expected.collectorSource || receipt.startup_revision !== expected.startup
    || receipt.diagnostic_gate_sha256 !== expected.gate || receipt.controller_sha256 !== expected.controller
    || receipt.seed_sha256 !== expected.seed || receipt.corpus_sha256 !== expected.corpus
    || receipt.footer_sha256 !== expected.footer
    || !isDeepStrictEqual(receipt.images, expected.images)
    || !isDeepStrictEqual(receipt.diagnostic_profile, DIAGNOSTIC_WORKLOAD_PROFILE)
    || !isDeepStrictEqual(initial.reference, { mode: "first" })
    || !isDeepStrictEqual(recheck.reference, { mode: "first" })) {
    throw new Error("reference receipt is not a completed, explicit first diagnostic arm");
  }
  const initialProof = validateDiagnosticProof(
    `[oat] diagnostic admission: ${JSON.stringify(initial)}\n`, expected.corpus, schedule,
  );
  const recheckProof = validateDiagnosticProof(
    `[oat] diagnostic pre-schedule admission: ${JSON.stringify(recheck)}\n`, expected.corpus, schedule, true,
  );
  if (!isDeepStrictEqual(initialProof.startup_accounting, recheckProof.startup_accounting)
    || !isDeepStrictEqual(initialProof.client_mapping, recheckProof.client_mapping)) {
    throw new Error("reference arm startup roster changed before scheduling");
  }
  const clients = referenceClients(initialProof);
  if (!isDeepStrictEqual(clients, referenceClients(recheckProof))
    || clients.length !== 1 || clients[0]!.logical_client_id !== DIAGNOSTIC_LOGICAL_CLIENT) {
    throw new Error("reference arm logical-client roster is invalid");
  }
  const mapping = diagnosticClientMapping(initialProof);
  if (mapping.runId !== receipt.run_id) throw new Error("reference receipt run identity differs");
  return {
    schema_version: 1,
    kind: "diagnostic-comparison-profile",
    qualification: "not-evaluated",
    reference_receipt_sha256: receiptSha256,
    reference_run_id: mapping.runId,
    source_revision: expected.source,
    harness_revision: expected.harness,
    collector_source_revision: expected.collectorSource,
    startup_revision: expected.startup,
    diagnostic_gate_sha256: expected.gate,
    controller_sha256: expected.controller,
    footer_sha256: expected.footer,
    seed_sha256: expected.seed,
    corpus_sha256: expected.corpus,
    images: expected.images, collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: kustainer(receipt.kustainer),
    diagnostic_profile: DIAGNOSTIC_WORKLOAD_PROFILE,
    clients,
  };
}

export function validateFrozenComparisonProfile(
  bytes: Buffer,
  sha256: string,
  expected: ReturnType<typeof diagnosticPins>,
): FrozenComparisonProfile {
  string(sha256, /^[a-f0-9]{64}$/, "comparison profile hash");
  if (!bytes.length || bytes.length > 65_536 || createHash("sha256").update(bytes).digest("hex") !== sha256) {
    throw new Error("comparison profile hash differs");
  }
  const text = new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  parseManifestJson(text);
  const profile = object(JSON.parse(text), "comparison profile");
  const fields = [
    "clients", "collector_memory_limit_bytes", "collector_source_revision", "controller_sha256", "corpus_sha256",
    "diagnostic_gate_sha256", "diagnostic_profile", "footer_sha256", "harness_revision",
    "images", "kind", "kustainer", "qualification", "reference_receipt_sha256", "reference_run_id",
    "schema_version", "seed_sha256", "source_revision", "startup_revision",
  ];
  if (Object.keys(profile).sort().join(",") !== fields.join(",")
    || profile.schema_version !== 1 || profile.kind !== "diagnostic-comparison-profile"
    || profile.qualification !== "not-evaluated"
    || profile.collector_memory_limit_bytes !== COLLECTOR_MEMORY || profile.source_revision !== expected.source || profile.harness_revision !== expected.harness
    || profile.collector_source_revision !== expected.collectorSource || profile.startup_revision !== expected.startup
    || profile.diagnostic_gate_sha256 !== expected.gate || profile.controller_sha256 !== expected.controller
    || profile.footer_sha256 !== expected.footer || profile.seed_sha256 !== expected.seed
    || profile.corpus_sha256 !== expected.corpus || !isDeepStrictEqual(profile.images, expected.images)
    || !isDeepStrictEqual(profile.diagnostic_profile, DIAGNOSTIC_WORKLOAD_PROFILE)) {
    throw new Error("comparison profile is stale or differs from pinned diagnostic inputs");
  }
  string(profile.reference_receipt_sha256, /^[a-f0-9]{64}$/, "reference receipt hash");
  string(profile.reference_run_id, /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/, "reference run ID");
  if (!Array.isArray(profile.clients) || profile.clients.length !== 1) {
    throw new Error("comparison profile client roster is missing or contains extra clients");
  }
  const clients = profile.clients.map((value, index) => {
    const client = object(value, `comparison profile client ${index}`);
    if (Object.keys(client).sort().join(",") !== "corpus_memory_reservation_bytes,logical_client_id"
      || client.logical_client_id !== DIAGNOSTIC_LOGICAL_CLIENT
      || !Number.isSafeInteger(client.corpus_memory_reservation_bytes)
      || Number(client.corpus_memory_reservation_bytes) <= 0
      || Number(client.corpus_memory_reservation_bytes) + 28 * STARTUP_ACCOUNTING_EXPECTED.live_identity_batch_bytes > CLIENT_MEMORY) {
      throw new Error("comparison profile client reservation is missing or invalid");
    }
    return {
      logical_client_id: DIAGNOSTIC_LOGICAL_CLIENT,
      corpus_memory_reservation_bytes: Number(client.corpus_memory_reservation_bytes),
    };
  });
  return {
    schema_version: 1,
    kind: "diagnostic-comparison-profile",
    qualification: "not-evaluated",
    reference_receipt_sha256: String(profile.reference_receipt_sha256),
    reference_run_id: String(profile.reference_run_id),
    source_revision: expected.source,
    harness_revision: expected.harness,
    collector_source_revision: expected.collectorSource,
    startup_revision: expected.startup,
    diagnostic_gate_sha256: expected.gate,
    controller_sha256: expected.controller,
    footer_sha256: expected.footer,
    seed_sha256: expected.seed,
    corpus_sha256: expected.corpus,
    images: profile.images, collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: kustainer(profile.kustainer),
    diagnostic_profile: DIAGNOSTIC_WORKLOAD_PROFILE,
    clients,
  };
}

export function verifyComparisonProfileReference(
  profileBytes: Buffer,
  profileSha256: string,
  receiptBytes: Buffer,
  expected: ReturnType<typeof diagnosticPins>,
): FrozenComparisonProfile {
  const profile = validateFrozenComparisonProfile(profileBytes, profileSha256, expected);
  const receiptSha256 = createHash("sha256").update(receiptBytes).digest("hex");
  if (receiptSha256 !== profile.reference_receipt_sha256) {
    throw new Error("comparison profile does not bind the supplied reference receipt");
  }
  const derived = freezeDiagnosticComparisonProfile(receiptBytes, receiptSha256, expected);
  if (!isDeepStrictEqual(profile, derived)) throw new Error("comparison profile differs from its verified reference receipt");
  return profile;
}

function diagnosticReference(profile: FrozenComparisonProfile, profileSha256: string): DiagnosticReference {
  return {
    mode: "matched",
    kind: "diagnostic-comparison-profile",
    qualification: "not-evaluated",
    profile_sha256: profileSha256,
    receipt_sha256: profile.reference_receipt_sha256,
    run_id: profile.reference_run_id,
    corpus_sha256: profile.corpus_sha256, collector_memory_limit_bytes: profile.collector_memory_limit_bytes, kustainer: profile.kustainer,
    clients: profile.clients,
  };
}

export function validateDiagnosticReferenceProof(
  proofs: readonly JsonObject[],
  reference: DiagnosticReference,
): void {
  if (reference.mode === "first") {
    if (proofs.some(proof => !isDeepStrictEqual(proof.reference, { mode: "first" }))) {
      throw new Error("first-arm reference establishment is not explicit");
    }
    return;
  }
  for (const proof of proofs) {
    if (proof.collector_memory_limit_bytes !== reference.collector_memory_limit_bytes || !isDeepStrictEqual(proof.kustainer, reference.kustainer) || !isDeepStrictEqual(proof.reference, reference)) throw new Error("diagnostic admission did not use the frozen comparison profile");
    const clients = referenceClients(proof);
    if (!isDeepStrictEqual(clients, reference.clients)) {
      throw new Error("post-run client reservations differ from the frozen comparison profile");
    }
  }
}

export function verifyDiagnosticHook(source: string): void {
  const include = 'include!("runner_realistic_diagnostic_admission.rs");';
  const hook = "let _ = diagnostic_admission_or_cancel(config, platform, &handles, &ready_clients, project_run_id, false).await?;";
  const recheckHook = "let schedule_admission = diagnostic_recheck_or_cancel(config, platform, &handles, project_run_id).await?;";
  const memoryHook = "diagnostic_memory_admission_or_cancel(config, platform, &handles).await?;";
  const scheduleGate = "schedule_after_diagnostic_admission(";
  const armCallback = "|| platform.arm_clients(config, project_run_id, run_start_unix_ms)";
  const lines = source.split(/\r?\n/).map(line => line.trim());
  const nonempty = lines.filter(Boolean);
  const precheckLine = nonempty.findIndex(line => line === 'let darwin_precheck = std::env::var("OAT_DARWIN_PARITY_PRECHECK");');
  const schedule = nonempty.findIndex((line, index) => index > precheckLine && line.startsWith("let schedule_deadline ="));
  const ready = source.indexOf("let ready_clients = loop {");
  const precheck = source.indexOf('let darwin_precheck = std::env::var("OAT_DARWIN_PARITY_PRECHECK");');
  const scheduleGateIndex = source.indexOf(scheduleGate);
  const armCallbackIndex = source.indexOf(armCallback);
  if (lines.filter(line => line === include).length !== 1 || lines.filter(line => line === hook).length !== 1
    || lines.filter(line => line === recheckHook).length !== 1 || lines.filter(line => line === memoryHook).length !== 1
    || precheckLine < 0 || schedule < 0 || ready < 0 || precheck < 0
    || source.indexOf(hook) <= ready || source.indexOf(hook) >= precheck
    || source.indexOf(recheckHook) <= precheck || source.indexOf(memoryHook) <= source.indexOf(recheckHook)
    || source.indexOf(memoryHook) >= source.indexOf("let schedule_deadline =")
    || scheduleGateIndex < source.indexOf("let schedule_deadline =") || armCallbackIndex < scheduleGateIndex) {
    throw new Error("diagnostic admission hook is not wired immediately after client readiness");
  }
}

export function validateHostAdmission(log: string, schedule: unknown): JsonObject {
  const prefix = "[oat] diagnostic host admission: ";
  const lines = log.split("\n").filter(line => line.startsWith(prefix));
  if (lines.length !== 1) throw new Error("diagnostic host admission missing or repeated");
  const proof = object(JSON.parse(lines[0]!.slice(prefix.length)), "diagnostic host admission");
  const effective = object(object(schedule, "schedule").effective, "effective schedule");
  const available = Number(proof.mem_available_bytes);
  if (proof.kind !== "diagnostic-only" || proof.phase !== "immediately_before_schedule"
    || !Number.isSafeInteger(proof.mem_total_bytes) || !Number.isSafeInteger(proof.mem_available_bytes)
    || available <= CLIENT_MEMORY || Number(proof.mem_total_bytes) < available
    || proof.client_memory_budget_bytes !== CLIENT_MEMORY || proof.collector_memory_limit_bytes !== COLLECTOR_MEMORY
    || proof.collector_limit_bound_bytes !== Math.floor((available - CLIENT_MEMORY) * 0.75)
    || !fitsDiagnosticBound(Number(proof.collector_limit_bound_bytes))
    || !Number.isSafeInteger(proof.observed_at_unix_ms) || !Number.isSafeInteger(effective.schedule_created_at_unix_ms)
    || Number(proof.observed_at_unix_ms) > Number(effective.schedule_created_at_unix_ms)) {
    throw new Error("diagnostic host memory admission differs from the pre-schedule contract");
  }
  return proof;
}

export function diagnosticLaunchEnv(
  home: string, stackBytes: number, toolDir: string, corpusSha256: string, clientCpus: number[],
  reference: DiagnosticReference,
): NodeJS.ProcessEnv {
  string(corpusSha256, /^[a-f0-9]{64}$/, "corpus hash");
  if (!isDeepStrictEqual(parseCollectorCpus(clientCpus.join(",")), clientCpus)) throw new Error("invalid client CPUs");
  const env = cleanEnv(home, stackBytes);
  env.OAT_DARWIN_PARITY_PRECHECK = "1";
  env.OAT_DIAGNOSTIC_CORPUS_SHA256 = corpusSha256;
  env.OAT_DIAGNOSTIC_CLIENT_CPUS = clientCpus.join(",");
  env.OAT_DIAGNOSTIC_REFERENCE = JSON.stringify(reference);
  env.PATH = `${toolDir}:${env.PATH}`;
  return env;
}

async function runDiagnosticOat(runtime: string, configPath: string, home: string, toolDir: string,
  logPath: string, stackBytes: number, corpusSha256: string, clientCpus: number[],
  reference: DiagnosticReference): Promise<void> {
  const log = await open(logPath, "wx");
  try {
    const child = spawn(process.execPath, ["--import", "tsx", "projects/arrowcol-oat/cli.ts",
      "--config", configPath, "--mode", "local", "--no-viewer"], {
      cwd: runtime, env: diagnosticLaunchEnv(home, stackBytes, toolDir, corpusSha256, clientCpus, reference),
      stdio: ["ignore", log.fd, log.fd],
    });
    let timedOut = false;
    let forceTimer: NodeJS.Timeout | undefined;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGTERM");
      forceTimer = setTimeout(() => child.kill("SIGKILL"), 5000);
    }, 6 * 60 * 60 * 1000);
    try {
      await new Promise<void>((resolve, reject) => {
        child.once("error", reject);
        child.once("exit", (status, signal) => {
          if (status === 0 && !timedOut) resolve();
          else reject(new Error(`diagnostic OAT failed (${timedOut ? "deadline" : signal ?? status}); inspect ${logPath} and owned-cell recovery before retrying`));
        });
      });
    } finally { clearTimeout(timer); if (forceTimer) clearTimeout(forceTimer); }
  } finally { await log.close(); }
}

export function validateDiagnosticProof(log: string, corpus: string, schedule: unknown, preSchedule = false): JsonObject {
  const prefix = preSchedule ? "[oat] diagnostic pre-schedule admission: " : "[oat] diagnostic admission: ";
  const lines = log.split("\n").filter(line => line.startsWith(prefix));
  if (lines.length !== 1) throw new Error("diagnostic admission proof missing or repeated");
  const proof = object(JSON.parse(lines[0]!.slice(prefix.length)), "diagnostic admission proof");
  const effective = object(object(schedule, "schedule").effective, "effective schedule");
  if (proof.kind !== "diagnostic-only" || proof.phase !== "before_schedule"
    || proof.payload_corpus_sha256 !== corpus || proof.configured_live_slots !== 28
    || proof.admitted_live_slots !== 28 || proof.live_identity_batch_bytes !== STARTUP_ACCOUNTING_EXPECTED.live_identity_batch_bytes
    || !Number.isSafeInteger(proof.corpus_memory_reservation_bytes) || Number(proof.corpus_memory_reservation_bytes) <= 0
    || !Number.isSafeInteger(proof.memory_reservation_bytes)
    || Number(proof.memory_reservation_bytes) !== Number(proof.corpus_memory_reservation_bytes) + 28 * STARTUP_ACCOUNTING_EXPECTED.live_identity_batch_bytes
    || Number(proof.memory_reservation_bytes) > CLIENT_MEMORY
    || !Number.isSafeInteger(proof.observed_at_unix_ms) || !Number.isSafeInteger(effective.schedule_created_at_unix_ms)
    || Number(proof.observed_at_unix_ms) > Number(effective.schedule_created_at_unix_ms)) {
    throw new Error("diagnostic admission proof differs from the pre-schedule contract");
  }
  const mapping = diagnosticClientMapping(proof);
  const accounting = requireStartupAccounting(proof.startup_accounting, [mapping.clientId], "before_arm");
  if (accounting.clients.length !== 1 || accounting.clients[0]!.corpus_memory_reservation_bytes !== proof.corpus_memory_reservation_bytes
    || accounting.clients[0]!.memory_reservation_bytes !== proof.memory_reservation_bytes) {
    throw new Error("diagnostic startup accounting differs from admission proof");
  }
  return proof;
}

export function validateDiagnosticRecheck(initial: JsonObject, recheck: JsonObject, host: JsonObject): void {
  if (!isDeepStrictEqual(initial.startup_accounting, recheck.startup_accounting)
    || !isDeepStrictEqual(initial.client_mapping, recheck.client_mapping)
    || !isDeepStrictEqual(initial.cpu_isolation, recheck.cpu_isolation)
    || Number(recheck.observed_at_unix_ms) < Number(initial.observed_at_unix_ms)
    || Number(host.observed_at_unix_ms) < Number(recheck.observed_at_unix_ms)) {
    throw new Error("diagnostic pre-schedule client or CPU readback changed or is out of order");
  }
}

function linuxCpuSet(value: unknown): number[] {
  if (typeof value !== "string" || !value.trim()) throw new Error("CPU readback missing");
  const result: number[] = [];
  for (const part of value.trim().split(",")) {
    const match = /^(0|[1-9]\d*)(?:-(0|[1-9]\d*))?$/.exec(part);
    if (!match) throw new Error("invalid CPU readback");
    const start = Number(match[1]);
    const end = Number(match[2] ?? match[1]);
    if (start > end || end > 4095) throw new Error("invalid CPU readback range");
    for (let cpu = start; cpu <= end; cpu++) result.push(cpu);
  }
  if (new Set(result).size !== result.length || result.length > 128) throw new Error("duplicate or oversized CPU readback");
  return result.sort((a, b) => a - b);
}

export function validateDiagnosticCpuProof(proof: JsonObject, collector: number[], client: number[]): void {
  const cpu = object(proof.cpu_isolation, "diagnostic CPU isolation");
  for (const [role, requested] of [["collector", collector], ["client", client]] as const) {
    const observed = object(cpu[role], `${role} CPU isolation`);
    if (typeof observed.container_id !== "string" || !/^[a-f0-9]{64}$/.test(observed.container_id)
      || !isDeepStrictEqual(observed.requested, requested)
      || !isDeepStrictEqual(linuxCpuSet(observed.configured), requested)
      || !isDeepStrictEqual(linuxCpuSet(observed.effective), requested)) throw new Error(`${role} CPU readback differs`);
  }
  const all = [...collector, ...client].sort((a, b) => a - b);
  if (new Set(all).size !== all.length || !Array.isArray(cpu.thread_siblings)
    || cpu.thread_siblings.length !== all.length) throw new Error("CPU topology is incomplete");
  const seen = new Set<number>();
  for (const pair of cpu.thread_siblings) {
    if (!Array.isArray(pair) || pair.length !== 2 || !Number.isSafeInteger(pair[0])
      || !all.includes(pair[0]) || seen.has(pair[0]) || !Array.isArray(pair[1])
      || !pair[1].every((cpu: unknown) => Number.isSafeInteger(cpu) && Number(cpu) >= 0 && Number(cpu) <= 4095)
      || !pair[1].includes(pair[0]) || new Set(pair[1]).size !== pair[1].length
      || pair[1].some((sibling: number) => collector.includes(pair[0]) ? client.includes(sibling) : collector.includes(sibling))) {
      throw new Error("CPU physical sibling isolation is unavailable");
    }
    seen.add(pair[0]);
  }
}

export function requireMatchingCorpusReservation(first: unknown, second: unknown): number {
  const arm = (receipt: unknown, armId: string) => {
    const r = object(receipt, "diagnostic receipt");
    const proof = object(r.diagnostic_admission, "diagnostic admission");
    if (r.kind !== "diagnostic-only" || r.qualification !== "not-evaluated"
      || !isDeepStrictEqual(r.diagnostic_profile, DIAGNOSTIC_WORKLOAD_PROFILE)
      || proof.kind !== "diagnostic-only" || proof.phase !== "before_schedule"
      || typeof r.seed_sha256 !== "string" || !/^[a-f0-9]{64}$/.test(r.seed_sha256)
      || typeof proof.payload_corpus_sha256 !== "string" || !/^[a-f0-9]{64}$/.test(proof.payload_corpus_sha256)) {
      throw new Error("invalid diagnostic corpus reservation evidence");
    }
    const mapping = diagnosticClientMapping(proof);
    const validated = requireStartupAccounting(proof.startup_accounting, [mapping.clientId], "before_arm");
    const oatClientId = `${mapping.runId}:${mapping.logicalId}`;
    const evidence = {
      schema_version: 1, capture_point: "client_ready_before_arm",
      clients: validated.clients.map(client => ({ ...client, client_id: oatClientId })),
    };
    return { mapping, corpus: proof.payload_corpus_sha256, seed: r.seed_sha256,
      arm: { arm_id: armId, expected_client_ids: [oatClientId], evidence } };
  };
  const left = arm(first, "first");
  const right = arm(second, "second");
  if (left.mapping.runId === right.mapping.runId || left.mapping.logicalId !== right.mapping.logicalId
    || left.corpus !== right.corpus || left.seed !== right.seed) {
    throw new Error("diagnostic arms have ambiguous identities or different workload profiles");
  }
  const compared = requireMatchedStartupAccounting([left.arm, right.arm], "before_arm");
  return compared[0]!.evidence.clients[0]!.corpus_memory_reservation_bytes;
}

export function diagnosticInputBytes(
  evidence: unknown, selected: readonly TimedReceipt[], measurement: OatMeasurement,
): { bytes: number; bytesPerSecond: number } {
  const commits = object(object(evidence, "evidence").durable_commits, "durable commits");
  const output = object(commits.output_throughput, "commit-window output");
  if (!Number.isSafeInteger(measurement.startUnixMs) || !Number.isSafeInteger(measurement.endUnixMs)
    || measurement.endUnixMs <= measurement.startUnixMs) throw new Error("invalid measurement window");
  let bytes = 0;
  const seen = new Set<string>();
  for (const receipt of selected) {
    if (!Number.isSafeInteger(receipt.inputBytes) || receipt.inputBytes < 0
      || !Number.isSafeInteger(receipt.finishedAtUnixMs)
      || receipt.finishedAtUnixMs < measurement.startUnixMs || receipt.finishedAtUnixMs >= measurement.endUnixMs) {
      throw new Error("invalid original-byte receipt");
    }
    const key = `${receipt.tableId}\0${receipt.version}`;
    if (seen.has(key)) throw new Error("duplicate original-byte receipt");
    seen.add(key);
    bytes += receipt.inputBytes;
    if (!Number.isSafeInteger(bytes)) throw new Error("original-byte receipt total overflow");
  }
  const seconds = (measurement.endUnixMs - measurement.startUnixMs) / 1000;
  const bytesPerSecond = bytes / seconds;
  if (!Number.isFinite(seconds) || seconds <= 0 || !Number.isFinite(bytesPerSecond)
    || output.input_bytes !== bytes || typeof output.input_bytes_per_second !== "number"
    || !Number.isFinite(output.input_bytes_per_second)
    || Math.abs(output.input_bytes_per_second - bytesPerSecond) > 1e-8 * Math.max(1, bytesPerSecond)) {
    throw new Error("original-byte receipts differ from the commit-window evidence");
  }
  return { bytes, bytesPerSecond };
}

export async function runRealisticDiagnostic(options: {
  repo: string; home: string; root: string; snapshot: string; output: string; pinsFile: string;
  bindHost: string; endpointHost: string; collectorCpus: string; clientCpus: string; stackBytes: number;
  reference: DiagnosticReferenceInput;
}) {
  const paths = diagnosticPaths(options.root, options.snapshot, options.output);
  if (!/^[0-9a-fA-F:.]+$/.test(options.bindHost) || !/^[0-9a-fA-F:.]+$/.test(options.endpointHost)) {
    throw new Error("bind and endpoint hosts must be explicit IP addresses");
  }
  const cpus = parseCollectorCpus(options.collectorCpus);
  const clientCpus = parseCollectorCpus(options.clientCpus);
  if (!isDeepStrictEqual(clientCpus, [24,25,26,27,28,29]) || cpus.some(cpu => clientCpus.includes(cpu) || cpu === 30 || cpu === 31)) throw new Error("invalid diagnostic collector, client or support CPU mask");
  const expected = diagnosticPins(JSON.parse(await readFile(options.pinsFile, "utf8")));
  let reference: DiagnosticReference;
  let comparisonProfile: FrozenComparisonProfile | undefined;
  let comparisonProfileBytes: Buffer | undefined;
  let comparisonProfilePath: string | undefined;
  let comparisonProfileSha256: string | undefined;
  let referenceReceiptBytes: Buffer | undefined;
  let referenceReceiptPath: string | undefined;
  if (options.reference.mode === "establish") {
    reference = { mode: "first" };
  } else {
    comparisonProfilePath = await realpath(string(options.reference.profilePath, /^\/.+/, "comparison profile path"));
    comparisonProfileSha256 = options.reference.profileSha256;
    comparisonProfileBytes = await readFile(comparisonProfilePath);
    referenceReceiptPath = await realpath(string(options.reference.receiptPath, /^\/.+/, "reference receipt path"));
    referenceReceiptBytes = await readFile(referenceReceiptPath);
    comparisonProfile = verifyComparisonProfileReference(
      comparisonProfileBytes, options.reference.profileSha256, referenceReceiptBytes, expected,
    );
    reference = diagnosticReference(comparisonProfile, options.reference.profileSha256);
  }
  if (expected.collectorSource !== COLLECTOR_SOURCE_BASE) throw new Error("diagnostic collector must be built from the frozen collector source");
  if (Reflect.get(oatResult, "DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION") !== 1) {
    throw new Error("trusted diagnostic-realistic snapshot reconciliation is unavailable; no measurement may be scheduled");
  }
  if (Reflect.get(oatResult, "DIAGNOSTIC_REALISTIC_DEFAULT_WRITER_VERSION") !== 1) {
    throw new Error("diagnostic reconciliation of the unchanged default writer is unavailable; no measurement may be scheduled");
  }
  const trusted = join(options.home, "trusted");
  const repo = await realpath(options.repo);
  const originEnv = cleanEnv(join(trusted, "prepare-home"), options.stackBytes);
  if (await realpath(fileURLToPath(import.meta.url)) !== join(repo, "tools", "darwin", "src", "realistic-diagnostic.ts")
    || git(repo, originEnv, "rev-parse", "--show-toplevel") !== repo
    || git(repo, originEnv, "rev-parse", "HEAD") !== expected.source
    || git(repo, originEnv, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
    || !hasPinnedSubmodules(repo, originEnv)) throw new Error("diagnostic launcher is not in the clean pinned source");
  const manifest = object(JSON.parse(await readFile(join(trusted, "manifest.json"), "utf8")), "manifest");
  const producer = object(JSON.parse(await readFile(join(trusted, "producer.json"), "utf8")), "producer");
  if (manifest.commit !== expected.source || producer.revision !== expected.source) throw new Error("source pin differs from trusted snapshot");
  execFileSync("git", ["-C", options.repo, "merge-base", "--is-ancestor", expected.harness, expected.source]);
  const collectorImage = execFileSync("docker", [
    "image", "inspect", "--format", '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}',
    String(expected.images.collector),
  ], { encoding: "utf8" }).trim();
  if (collectorImage !== `${expected.images.collector} ${expected.collectorSource}`) {
    throw new Error("frozen collector image identity or source revision differs");
  }
  execFileSync("git", ["-C", options.repo, "merge-base", "--is-ancestor", expected.startup, expected.source]);
  execFileSync("git", ["-C", options.repo, "diff", "--quiet", expected.harness, expected.source, "--",
    "experiments/darwin/historical-parity.oat.json", "tools/darwin/src/parity-replay.ts",
    "tools/darwin/src/parity-preflight.ts", "tools/darwin/src/qualification-config.ts"]);
  const seedPath = join(trusted, "src", "experiments", "darwin", "historical-parity.oat.json");
  const files = object(manifest.files, "manifest files");
  if (object(files["experiments/darwin/historical-parity.oat.json"], "seed manifest").sha256 !== expected.seed
    || await sha256(seedPath) !== expected.seed) throw new Error("seed pin differs from trusted snapshot");
  const config = realisticDiagnosticConfig(JSON.parse(await readFile(seedPath, "utf8")), cpus);
  const lock = join(options.home, "data", "evaluating");
  await mkdir(dirname(lock), { recursive: true });
  await mkdir(lock);
  let completed = false;
  try {
    const { runtime, binaryHash } = await prepareReplayRuntime(options.repo, options.home, options.stackBytes);
    if (binaryHash !== expected.controller) throw new Error("controller differs from frozen binary pin");
    const footer = await buildFooterInspector(runtime, join(trusted, "prepare-home"), options.stackBytes);
    if (footer.sha256 !== expected.footer) throw new Error("footer inspector differs from frozen binary pin");
    const identityTools = await prepareIdentityTools(options.home, runtime, manifest as unknown as SnapshotManifest, options.stackBytes);
    if (!isDeepStrictEqual({
      decoder_sha256: identityTools.decoderSha256, extractor_sha256: identityTools.extractorSha256,
      node_sha256: identityTools.nodeSha256, tsx_sha256: identityTools.tsxSha256,
      verifier_sha256: identityTools.verifierArtifactSha256,
    }, expected.identityTools)) throw new Error("identity verifier differs from frozen tool pins");
    const env = cleanEnv(join(trusted, "prepare-home"), options.stackBytes);
    if (git(runtime, env, "rev-parse", "HEAD") !== expected.source || !hasPinnedSubmodules(runtime, env)) throw new Error("runtime source pin changed");
    for (const name of ["realistic-diagnostic.ts", "realistic-diagnostic-config.ts"]) {
      if (await sha256(fileURLToPath(new URL(name, import.meta.url))) !== await sha256(join(runtime, "tools", "darwin", "src", name))) {
        throw new Error("diagnostic launcher differs from the pinned runtime source");
      }
    }
    const oatSource = join(runtime, "projects", "arrowcol-oat-rust", "src");
    verifyDiagnosticHook(await readFile(join(oatSource, "runner.rs"), "utf8"));
    if (await sha256(join(oatSource, "runner_realistic_diagnostic_admission.rs")) !== expected.gate) {
      throw new Error("diagnostic admission implementation differs from its pin");
    }
    if (await realpath(dirname(paths.root)) !== dirname(paths.root)) throw new Error("run parent must not contain symlinks");
    await mkdir(paths.root);
    await Promise.all([mkdir(paths.home), mkdir(paths.snapshot), mkdir(paths.output)]);
    const comparisonProfileCopy = join(paths.output, "diagnostic-comparison-profile.json");
    const referenceReceiptCopy = join(paths.output, "diagnostic-reference-receipt.json");
    if (comparisonProfileBytes && comparisonProfile && referenceReceiptBytes && reference.mode === "matched") {
      await writeFile(comparisonProfileCopy, comparisonProfileBytes, { flag: "wx", mode: 0o400 });
        await writeFile(referenceReceiptCopy, referenceReceiptBytes, { flag: "wx", mode: 0o400 });
        if (await sha256(comparisonProfileCopy) !== reference.profile_sha256) {
        throw new Error("copied comparison profile changed");
      }
    }
    const identityOutput = join(paths.output, "diagnostic-identity-evidence");
    await mkdir(identityOutput);
    const platform = object(config.platform, "platform");
    platform.compose_state_root = paths.state;
    if (platform.compose_override_file !== "projects/loadtest/compose.oat.yaml") throw new Error("unexpected base Compose override");
    const baseOverride = await readFile(join(runtime, "projects", "loadtest", "compose.oat.yaml"), "utf8");
    if (!baseOverride.startsWith("services:\n") || /^  oat-client:/m.test(baseOverride)) throw new Error("unexpected base Compose override structure");
    const clientOverridePath = join(paths.root, "diagnostic-client-cpus.compose.yaml");
    await writeFile(clientOverridePath, `services:\n  oat-client:\n    cpuset: "${clientCpus.join(",")}"\n${baseOverride.slice("services:\n".length)}`, { flag: "wx", mode: 0o400 });
    const clientOverrideHash = await sha256(clientOverridePath);
    platform.compose_override_file = clientOverridePath;
    platform.local = localReplayOptions(platform.local, options.bindHost, options.endpointHost, paths.snapshot);
    const local = object(platform.local, "local");
    local.expected_image_ids = expected.images;
    local.collector_image_override = { image_id: expected.images.collector, source_revision: expected.collectorSource };
    config.result_url = pathToFileURL(join(paths.output, "diagnostic-results.parquet")).href;
    config.website_url = pathToFileURL(join(paths.output, "diagnostic-site", "results.parquet")).href;
    const configPath = join(paths.output, "diagnostic-config.json");
    await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`, { flag: "wx", mode: 0o400 });
    const configHash = await sha256(configPath);
    const host = await inspectParityHost(CLIENT_MEMORY, COLLECTOR_MEMORY);
    if (!fitsDiagnosticBound(host.collectorLimitBoundBytes)) throw new Error("diagnostic memory bound lacks 5% headroom");
    await writeFile(join(paths.output, "diagnostic-host.json"), `${JSON.stringify(host, null, 2)}\n`, { flag: "wx" });
    const logPath = join(paths.output, "diagnostic-run.log");
    try { await runDiagnosticOat(
      runtime, configPath, paths.home, join(trusted, "bin"), logPath,
      options.stackBytes, expected.corpus, clientCpus, reference,
    ); } finally {
      const result = memoryOutcome(await readFile(logPath, "utf8").catch(() => ""));
      await writeFile(join(paths.output, "diagnostic-memory.json"), JSON.stringify(result, null, 2) + "\n", { flag: "wx" });
    }
    if (await sha256(configPath) !== configHash || await sha256(clientOverridePath) !== clientOverrideHash || await sha256(seedPath) !== expected.seed
      || await sha256(join(runtime, "target", "release", "arrowcol-oat-rust")) !== expected.controller
      || git(runtime, env, "rev-parse", "HEAD") !== expected.source
      || git(runtime, env, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
      || !hasPinnedSubmodules(runtime, env)
      || git(repo, originEnv, "rev-parse", "HEAD") !== expected.source
      || git(repo, originEnv, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
      || !hasPinnedSubmodules(repo, originEnv)) throw new Error("pinned run inputs changed");
    if (reference.mode === "matched" && comparisonProfileBytes && comparisonProfilePath
      && (comparisonProfileSha256 === undefined
        || await sha256(comparisonProfilePath) !== comparisonProfileSha256
        || await sha256(comparisonProfileCopy) !== comparisonProfileSha256
        || !referenceReceiptPath || !referenceReceiptBytes
        || await sha256(referenceReceiptPath) !== reference.receipt_sha256
        || await sha256(referenceReceiptCopy) !== reference.receipt_sha256)) {
      throw new Error("frozen comparison profile changed during the diagnostic arm");
    }
    const log = await readFile(logPath, "utf8");
    const memory = memoryOutcome(log);
    const prefix = "[oat] immutable local images: ";
    const imageLines = log.split("\n").filter(line => line.startsWith(prefix));
    if (imageLines.length !== 1 || !isDeepStrictEqual(object(JSON.parse(imageLines[0]!.slice(prefix.length)), "admitted images"),
      expected.images)) throw new Error("admitted images differ from pins");
    const allocation = parityCpuAllocation(log, cpus);
    const bytes = await readFile(join(paths.output, "diagnostic-results.parquet"));
    if (!bytes.length || bytes.length > 16 * 1024 * 1024) throw new Error("invalid OAT result size");
    const arrow = tableFromIPC(readParquet(bytes).intoIPCStream());
    if (arrow.numRows !== 1) throw new Error("expected a single diagnostic OAT row");
    const row = Object.fromEntries(arrow.schema.fields.map(field => [field.name, arrow.toArray()[0][field.name]]));
    if (row.status !== "succeeded" || row.git_sha !== expected.source || row.table !== "sqlserver_availability_replica_states" || row.mode !== "local"
      || Number(row.replicas) !== 1 || typeof row.evidence_json !== "string") throw new Error("diagnostic result identity differs");
    const evidence = object(JSON.parse(row.evidence_json), "evidence");
    const admission = validateDiagnosticProof(log, expected.corpus, evidence.client_schedule);
    const preScheduleAdmission = validateDiagnosticProof(log, expected.corpus, evidence.client_schedule, true);
    validateDiagnosticReferenceProof([admission, preScheduleAdmission], reference);
    const hostAdmission = validateHostAdmission(log, evidence.client_schedule);
    validateDiagnosticCpuProof(admission, cpus, clientCpus);
    validateDiagnosticCpuProof(preScheduleAdmission, cpus, clientCpus);
    validateDiagnosticRecheck(admission, preScheduleAdmission, hostAdmission);
    const cpuPrefix = "[oat] diagnostic CPU window: ";
    const windows = log.split("\n").filter(line => line.startsWith(cpuPrefix));
    if (windows.length !== 1) throw new Error("expected exactly one diagnostic CPU window");
    const cpuWindow = cpuWindowReceipt(object(preScheduleAdmission.cpu_isolation, "CPU isolation").project_readback,
      JSON.parse(windows[0]!.slice(cpuPrefix.length)), object(object(evidence.client_schedule, "schedule").effective, "effective schedule"));
    if (!Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) throw new Error("client evidence missing");
    const report = object(evidence.client_phase_reports[0], "client report");
    const executionImage = object(report.execution_image, "client execution image");
    if (executionImage.source !== "docker_container_inspect" || executionImage.image_id !== expected.images.client) {
      throw new Error("executed client image differs from the frozen image");
    }
    if (!Array.isArray(evidence.processes)) throw new Error("collector process evidence missing");
    const collectors = evidence.processes.map(process => object(process, "process evidence"))
      .filter(process => process.component === "collector");
    if (collectors.length !== 1 || collectors[0]!.log_complete !== true
      || collectors[0]!.image_id !== expected.images.collector || collectors[0]!.source_revision !== expected.collectorSource) {
      throw new Error("executed collector image or source differs from the frozen base");
    }
    const client = extractClientEvidence(evidence.client_phase_reports[0], "realistic");
    if (client.payloadCorpusSha256 !== expected.corpus) throw new Error("corpus differs from pin");
    assertParityEvidence(config, evidence);
    const engineCores = object(preScheduleAdmission.cpu_isolation, "CPU isolation").engine_num_cores;
    const parityCollector = object(object(evidence.parity_admission_readback, "pre-arm parity").collector, "collector startup");
    const cpuWorkers = object(parityCollector.runtime, "collector runtime").cpu_workers;
    if (engineCores !== cpus.length || cpuWorkers !== cpus.length || Number(row.dfe_worker_concurrency) !== cpus.length) {
      throw new Error("collector worker/core readback differs from diagnostic CPU stratum");
    }
    const decodeFile = async (path: string, active: boolean): Promise<number> => {
      if (await sha256(footer.path) !== expected.footer) throw new Error("footer inspector changed");
      const output = execFileSync(footer.path, [active ? "--active" : "--decode", path], {
        env: cleanEnv(paths.home, options.stackBytes), timeout: 600_000, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
      }).trim();
      if (await sha256(footer.path) !== expected.footer || !/^(0|[1-9][0-9]*)$/.test(output)
        || !Number.isSafeInteger(Number(output))) throw new Error("footer inspector returned an invalid count");
      return Number(output);
    };
    const resultPath = join(paths.output, "diagnostic-results.parquet");
    if (typeof row.run_id !== "string" || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(row.run_id)) {
      throw new Error("diagnostic cell identity missing");
    }
    const stoppedPath = join(paths.snapshot, row.run_id, "stopped.json");
    const stoppedHash = await sha256(stoppedPath);
    const resultHash = createHash("sha256").update(bytes).digest("hex");
    const diagnosticOptions = {
      diagnosticOnlyRealistic: true as const, expectedClientConfig: { workers: 14, streams_per_worker: 2 },
      decodeFile, collectorRevision: expected.collectorSource,
    };
    let reconciled: Awaited<ReturnType<typeof readReconciledOatResult>>;
    try {
      reconciled = await readReconciledOatResult(resultPath, paths.snapshot,
        "sqlserver_availability_replica_states", expected.source, null, "realistic", undefined,
        { tools: identityTools, outputDir: identityOutput, collectorRevision: expected.collectorSource }, diagnosticOptions);
    } catch (error) {
      await writeFile(join(paths.output, "diagnostic-reconciliation-unavailable.json"), JSON.stringify({
        kind: "diagnostic-only", durable_rate: null,
        reason: "trusted realistic snapshot reconciliation did not complete",
      }) + "\n", { flag: "wx" });
      throw new Error(`trusted realistic snapshot reconciliation unavailable; no durable rate: ${error instanceof Error ? error.message : "unknown failure"}`);
    }
    const wholeRunIdentity = object(reconciled.deliveryIdentity, "whole-snapshot delivery identity");
    if (reconciled.kind !== "diagnostic-only" || reconciled.snapshotReconciled !== true || reconciled.diagnosticOnlyRealistic !== true
      || reconciled.diagnosticRealisticReconciliationVersion !== 1 || reconciled.parityTelemetry !== null
      || wholeRunIdentity.kind !== "diagnostic-only" || wholeRunIdentity.verified !== true
      || wholeRunIdentity.scope !== "exact_whole_snapshot_delivery_identity_set_equality"
      || reconciled.cellRunId !== row.run_id || reconciled.clientEvidence.payloadCorpusSha256 !== expected.corpus
      || !isDeepStrictEqual(parityImageIds(log, reconciled.images), expected.images)
      || await sha256(resultPath) !== resultHash || await sha256(stoppedPath) !== stoppedHash) {
      throw new Error("diagnostic reconciler returned incompatible or changed evidence");
    }
    const stopped = JSON.parse(await readFile(stoppedPath, "utf8"));
    const receipts = extractStoppedReceipts(evidence, stopped, reconciled.cellRunId,
      "telemetry.sqlserver.sqlserver_availability_replica_states", reconciled.measurement);
    const originalBytes = diagnosticInputBytes(evidence, receipts.selected, reconciled.measurement);
    if (await sha256(stoppedPath) !== stoppedHash) throw new Error("stopped evidence changed during receipt extraction");
    const receipt: JsonObject = {
      kind: "diagnostic-only", qualification: "not-evaluated", source_revision: expected.source,
      run_id: row.run_id,
      reference_establishment: reference.mode === "first" ? "explicit-first-arm" : null,
      diagnostic_reference: reference,
      comparison_profile_sha256: reference.mode === "matched" ? reference.profile_sha256 : null,
      reference_receipt_sha256: reference.mode === "matched" ? reference.receipt_sha256 : null,
      reference_run_id: reference.mode === "matched" ? reference.run_id : null,
      diagnostic_profile: DIAGNOSTIC_WORKLOAD_PROFILE,
      seed_sha256: expected.seed, controller_sha256: expected.controller, corpus_sha256: expected.corpus,
      images: expected.images, startup_revision: expected.startup, diagnostic_gate_sha256: expected.gate,
      harness_revision: expected.harness, collector_source_revision: expected.collectorSource,
      config_sha256: configHash, diagnostic_admission: admission, collector_cpu_allocation: allocation,
      pre_schedule_admission: preScheduleAdmission, pre_schedule_host_admission: hostAdmission,
      client_schedule: evidence.client_schedule,
      footer_sha256: expected.footer,
      durable_rows_per_second: reconciled.measurement.durableRowsPerSecond,
      committed_original_bytes_per_second: originalBytes.bytesPerSecond,
      committed_original_bytes: originalBytes.bytes,
      original_byte_scope: "collector_reported_input_bytes_from_reconciled_commit_receipts",
      whole_snapshot_identity: wholeRunIdentity,
      measurement_window: { start_unix_ms: reconciled.measurement.startUnixMs, end_unix_ms: reconciled.measurement.endUnixMs },
      collector_treatment: { kind: "cpuset_and_engine_workers", cpus, engine_num_cores: engineCores, runtime_cpu_workers: cpuWorkers },
      project_cpu_window: cpuWindow,
      collector_cpu_evidence: cpuWindow.collector_cpu_evidence && memory.permits_cpu_attribution,
      collector_memory: memory, collector_memory_limit_bytes: COLLECTOR_MEMORY, kustainer: kustainer(preScheduleAdmission.kustainer),
      client_resource_observation: {
        requested_logical_cpus: clientCpus, cpu: client.cpu,
        measurement_window: object(report.phase_counters, "client counters").measurement_window,
        headroom: "not_established_from_mean_cpu_alone",
      },
      snapshot_reconciled: true,
      client_compose_override_sha256: clientOverrideHash,
      paths, host,
    };
    const receiptBytes = Buffer.from(`${JSON.stringify(receipt, null, 2)}\n`);
    const receiptPath = join(paths.output, "diagnostic-receipt.json");
    await writeFile(receiptPath, receiptBytes, { flag: "wx" });
    let establishedProfile: { path: string; sha256: string } | undefined;
    if (reference.mode === "first" && receipt.collector_cpu_evidence === true) {
      const profile = freezeDiagnosticComparisonProfile(
        receiptBytes, createHash("sha256").update(receiptBytes).digest("hex"), expected,
      );
      const profileBytes = Buffer.from(`${JSON.stringify(profile, null, 2)}\n`);
      const profilePath = join(paths.output, "diagnostic-comparison-profile.json");
      await writeFile(profilePath, profileBytes, { flag: "wx", mode: 0o400 });
      establishedProfile = {
        path: profilePath,
        sha256: createHash("sha256").update(profileBytes).digest("hex"),
      };
    } else if (comparisonProfile) {
      validateDiagnosticReferenceProof([admission, preScheduleAdmission], reference);
    }
    completed = true;
    return { receipt, comparisonProfile: establishedProfile };
  } finally {
    if (completed) await rmdir(lock);
  }
}

async function main() {
  if (process.argv[2] === "--compare-reservations" && process.argv.length === 5) {
    if (await realpath(process.argv[3]!) === await realpath(process.argv[4]!)) throw new Error("comparison requires two distinct receipts");
    const first = await readFile(process.argv[3]!, "utf8");
    const second = await readFile(process.argv[4]!, "utf8");
    const bytes = requireMatchingCorpusReservation(JSON.parse(first), JSON.parse(second));
    console.log(JSON.stringify({ kind: "diagnostic-only", corpus_memory_reservation_bytes: bytes }));
    return;
  }
  const [repo, home, root, snapshot, output, pinsFile, bindHost, endpointHost, collectorCpus, clientCpus, stack, ...referenceArgs] = process.argv.slice(2);
  if (!repo || !home || !root || !snapshot || !output || !pinsFile || !bindHost || !endpointHost || !collectorCpus || !clientCpus || !stack) {
    throw new Error("usage: realistic-diagnostic.ts REPO HOME RUN_ROOT SNAPSHOT_DIR OUTPUT_DIR PINS_JSON BIND_IP ENDPOINT_IP COLLECTOR_CPUS CLIENT_CPUS STACK_BYTES (--establish-reference | --comparison-profile PROFILE_PATH PROFILE_SHA256 REFERENCE_RECEIPT_PATH); or --compare-reservations FIRST_RECEIPT SECOND_RECEIPT");
  }
  let reference: DiagnosticReferenceInput;
  if (referenceArgs.length === 1 && referenceArgs[0] === "--establish-reference") {
    reference = { mode: "establish" };
  } else if (referenceArgs.length === 4 && referenceArgs[0] === "--comparison-profile") {
    reference = {
      mode: "compare",
      profilePath: referenceArgs[1]!,
      profileSha256: string(referenceArgs[2], /^[a-f0-9]{64}$/, "comparison profile hash"),
      receiptPath: referenceArgs[3]!,
    };
  } else {
    throw new Error("explicit first-reference establishment or frozen comparison profile is required");
  }
  const result = await runRealisticDiagnostic({
    repo, home, root, snapshot, output, pinsFile, bindHost, endpointHost,
    collectorCpus, clientCpus, stackBytes: Number(stack), reference,
  });
  console.log(JSON.stringify({
    kind: "diagnostic-only", qualification: "not-evaluated",
    receipt: result.receipt, comparison_profile: result.comparisonProfile ?? null,
  }));
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
