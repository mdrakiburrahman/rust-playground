import { readFile } from "node:fs/promises";
import { availableParallelism, cpus } from "node:os";
import { isDeepStrictEqual } from "node:util";

type JsonObject = Record<string, unknown>;

function object(value: unknown, name: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${name} must be an object`);
  }
  return value as JsonObject;
}

export type ParityProfile = "historical-parity-diagnostic" | "bounded-table-2500m-diagnostic";

export function parseParityProfile(value: string): ParityProfile {
  if (value !== "historical-parity-diagnostic" && value !== "bounded-table-2500m-diagnostic") {
    throw new Error("unsupported parity profile");
  }
  return value;
}

export function parityLadderConfig(
  seed: unknown, workers: number, profile: ParityProfile = "historical-parity-diagnostic",
): JsonObject {
  if (![1, 2, 4, 8, 16].includes(workers)) throw new Error("unsupported parity ladder worker count");
  const config = object(structuredClone(seed), "parity seed");
  const tunables = object(config.tunables, "tunables");
  if (tunables.parquet_writer != null) throw new Error("historical parity must not override the writer");
  const client = object(tunables.client, "client");
  if (client.data_profile !== "diagnostic" || client.payload_customer_id !== "direct-e64-confirmwide"
    || client.workers !== 8 || client.streams_per_worker !== 2
    || !Array.isArray(config.replica_counts) || config.replica_counts.length !== 1 || config.replica_counts[0] !== 1
    || !Array.isArray(config.tables) || config.tables.length !== 1
    || config.tables[0] !== "sqlserver_availability_replica_states") {
    throw new Error("invalid frozen parity seed");
  }
  client.workers = workers;
  if (profile === "bounded-table-2500m-diagnostic") {
    const collector = object(tunables.collector, "collector");
    const batching = object(collector.batching, "batching");
    const global = object(batching.global_capacity, "global capacity");
    const table = object(batching.per_table_capacity, "table capacity");
    const guard = object(config.overload_guard, "overload guard");
    if (global.bytes !== 4_290_000_000 || table.bytes !== 4_000_000_000
      || batching.max_arrow_memory_per_flush !== 1_073_741_824
      || guard.enabled !== true || guard.cap_fraction !== 0.9) {
      throw new Error("bounded profile requires the unchanged historical guard and capacity seed");
    }
    table.bytes = 2_500_000_000;
  } else if (profile !== "historical-parity-diagnostic") {
    throw new Error("unsupported parity profile");
  }
  return config;
}

export function assertParityCollectorReadback(seed: unknown, actual: unknown): void {
  const config = object(seed, "parity seed");
  const expected = object(object(config.tunables, "tunables").collector, "collector");
  const effective = object(actual, "collector readback");
  for (const section of ["batching", "commit", "pressure", "limits"] as const) {
    const requested = object(expected[section], `collector.${section}`);
    const observed = object(effective[section], `readback.${section}`);
    for (const [key, value] of Object.entries(requested)) {
      if (!isDeepStrictEqual(observed[key], value)) throw new Error(`collector readback differs at ${section}.${key}`);
    }
  }
  const runtime = object(expected.runtime, "collector.runtime");
  const actualRuntime = object(effective.runtime, "readback.runtime");
  for (const [key, value] of Object.entries(runtime)) {
    const observedKey = key === "workers" ? "runtime_workers" : key;
    if (!isDeepStrictEqual(actualRuntime[observedKey], value)) {
      throw new Error(`collector readback differs at runtime.${key}`);
    }
  }
  if (effective.cpu_pressure !== expected.cpu_pressure) throw new Error("collector readback differs at cpu_pressure");
  const layout = object(expected.layout, "collector.layout");
  const process = object(object(effective.layout, "readback.layout").process_policy, "readback.layout.process_policy");
  for (const [key, value] of Object.entries(layout)) {
    if (!isDeepStrictEqual(process[key], value)) throw new Error(`collector readback differs at layout.${key}`);
  }
}

export function assertParityTableLayout(seed: unknown, policies: unknown): void {
  const config = object(seed, "parity seed");
  const expected = object(object(object(config.tunables, "tunables").collector, "collector").layout, "collector.layout");
  if (!Array.isArray(policies)) throw new Error("table layout evidence is unavailable");
  const matches = policies.map((policy) => object(policy, "table layout"))
    .filter((policy) => policy.table_name === "telemetry.sqlserver.sqlserver_availability_replica_states");
  if (matches.length !== 1 || matches[0]!.scope !== "table_effective_at_file_construction_not_full_run_history") {
    throw new Error("canonical table layout evidence is unavailable");
  }
  const actual = object(matches[0]!.config, "table layout config");
  for (const [key, value] of Object.entries(expected)) {
    if (!isDeepStrictEqual(actual[key], value)) throw new Error(`table layout differs at ${key}`);
  }
}

export function assertParityEvidence(seed: unknown, input: unknown): void {
  const config = object(seed, "parity seed");
  const local = object(object(config.platform, "platform").local, "local");
  const evidence = object(input, "parity evidence");
  if (!Array.isArray(evidence.processes)) throw new Error("collector process evidence is unavailable");
  const collectors = evidence.processes.map((value) => object(value, "process"))
    .filter((process) => process.component === "collector");
  if (collectors.length !== 1 || collectors[0]!.log_complete !== true
    || collectors[0]!.effective_config_source !== "consumer_startup_readback") {
    throw new Error("complete collector readback is unavailable");
  }
  assertParityCollectorReadback(seed, collectors[0]!.effective_config);
  assertParityTableLayout(seed, collectors[0]!.layout_table_policies);
  const tunables = object(config.tunables, "tunables");
  if (tunables.parquet_writer != null) throw new Error("historical parity must not override the writer");
  const writer = object(collectors[0]!.writer_policy_overrides, "writer policy");
  function hasOverride(value: unknown): boolean {
    if (value == null) return false;
    if (Array.isArray(value)) return value.length !== 0;
    if (typeof value === "object") return Object.values(value).some(hasOverride);
    return true;
  }
  if (hasOverride(writer)) throw new Error("collector writer readback contains an override");
  const admission = object(evidence.parity_admission_readback, "pre-admission readback");
  const schedule = object(object(evidence.client_schedule, "client schedule").effective, "effective client schedule");
  if (admission.status !== "verified_before_schedule" || !Number.isSafeInteger(admission.observed_at_unix_ms)
    || !Number.isSafeInteger(schedule.run_start_unix_ms)
    || Number(admission.observed_at_unix_ms) >= Number(schedule.run_start_unix_ms)
    || typeof admission.collector_image_id !== "string" || admission.collector_image_id !== collectors[0]!.image_id) {
    throw new Error("pre-admission collector identity or timing is unavailable");
  }
  const admissionCollector = object(admission.collector, "pre-admission collector");
  assertParityCollectorReadback(seed, admissionCollector);
  if (hasOverride(object(admissionCollector.parquet_writer, "pre-admission writer"))) {
    throw new Error("pre-admission writer readback contains an override");
  }
  if (!Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) {
    throw new Error("client readback is unavailable");
  }
  const report = object(evidence.client_phase_reports[0], "client");
  const client = object(report.consumer_effective_config, "client readback");
  const admissionClient = object(admission.client, "pre-admission client");
  if (!isDeepStrictEqual(admissionClient, client)) throw new Error("pre-admission client readback changed");
  for (const [key, value] of Object.entries(object(tunables.client, "client tunables"))) {
    if (!isDeepStrictEqual(client[key], value)) throw new Error(`client readback differs at ${key}`);
  }
  const transport = object(tunables.transport, "transport");
  for (const key of ["ipc_compression", "grpc_send_compression", "max_flight_data_bytes", "max_message_bytes"]) {
    if (!isDeepStrictEqual(client[key], transport[key])) throw new Error(`client transport readback differs at ${key}`);
  }
  const counters = object(report.phase_counters, "client phase counters");
  const cumulative = object(counters.process_cumulative, "client cumulative counters");
  const delivery = object(counters.delivery_cumulative, "client cumulative delivery evidence");
  if (cumulative.ambiguous_rows !== 0 || !Number.isSafeInteger(cumulative.completed_rows) || Number(cumulative.completed_rows) < 0
    || delivery.scope !== "completed_process_lifetime_counters_not_unique_delivery"
    || ["retried_streams", "retried_rows", "ambiguous_streams", "ambiguous_rows"].some((key) => delivery[key] !== 0)) {
    throw new Error("client cumulative delivery evidence is ambiguous, retried or unavailable");
  }
  const guard = object(evidence.overload_guard, "overload guard");
  const settings = object(guard.settings, "overload settings");
  if (guard.status !== "complete" || guard.requested_container_memory_limit_bytes !== local.collector_memory_limit_bytes
    || settings.enabled !== true || settings.cap_fraction !== 0.9 || settings.zero_progress_windows !== 2
    || !Array.isArray(guard.last_caps)) throw new Error("overload guard evidence is unavailable or inconsistent");
  const caps = guard.last_caps.map((value) => object(value, "guard cap"))
    .filter((cap) => cap.metric === "container_memory_bytes");
  if (caps.length !== 1 || caps[0]!.confirmed !== true || caps[0]!.cap !== local.collector_memory_limit_bytes) {
    throw new Error("collector container memory cap is unconfirmed");
  }
}

export interface ParityHostReceipt {
  label: "local one-box scale";
  observedAt: string;
  hostTotalMemoryBytes: number;
  hostAvailableMemoryBytes: number;
  hostCpuCount: number;
  availableCpuCount: number;
  clientMemoryBudgetBytes: number;
  collectorMemoryLimitBytes: number;
  collectorLimitBoundBytes: number;
  sampledPeakCollectorRssBytes: null;
  sampledPeakCollectorRssUnavailableReason: string;
}

export function parityHostReceipt(
  meminfo: string, hostCpuCount: number, availableCpuCount: number,
  clientMemoryBudgetBytes: number, collectorMemoryLimitBytes: number, observedAt: string,
): ParityHostReceipt {
  function readMemory(field: string): number {
    const matches = [...meminfo.matchAll(new RegExp(`^${field}:\\s+(\\d+)\\s+kB\\s*$`, "gm"))];
    if (matches.length !== 1) throw new Error(`missing or ambiguous host ${field}`);
    const bytes = Number(matches[0]![1]) * 1024;
    if (!Number.isSafeInteger(bytes) || bytes <= 0) throw new Error(`invalid host ${field}`);
    return bytes;
  }
  const total = readMemory("MemTotal");
  const available = readMemory("MemAvailable");
  if (available > total || !Number.isSafeInteger(hostCpuCount) || hostCpuCount <= 0
    || !Number.isSafeInteger(availableCpuCount) || availableCpuCount <= 0
    || !Number.isSafeInteger(clientMemoryBudgetBytes) || clientMemoryBudgetBytes <= 0
    || !Number.isSafeInteger(collectorMemoryLimitBytes) || collectorMemoryLimitBytes <= 0
    || clientMemoryBudgetBytes >= available || Number.isNaN(Date.parse(observedAt))) {
    throw new Error("invalid parity host preflight inputs");
  }
  const bound = Math.floor((available - clientMemoryBudgetBytes) * 0.75);
  if (collectorMemoryLimitBytes > bound) throw new Error("collector cap exceeds 75% of available memory after client budget");
  return {
    label: "local one-box scale", observedAt,
    hostTotalMemoryBytes: total, hostAvailableMemoryBytes: available,
    hostCpuCount, availableCpuCount, clientMemoryBudgetBytes,
    collectorMemoryLimitBytes, collectorLimitBoundBytes: bound,
    sampledPeakCollectorRssBytes: null,
    sampledPeakCollectorRssUnavailableReason: "collector process RSS has not been sampled",
  };
}

export async function inspectParityHost(clientMemoryBudgetBytes: number, collectorMemoryLimitBytes: number): Promise<ParityHostReceipt> {
  return parityHostReceipt(
    await readFile("/proc/meminfo", "utf8"), cpus().length, availableParallelism(),
    clientMemoryBudgetBytes, collectorMemoryLimitBytes, new Date().toISOString(),
  );
}
