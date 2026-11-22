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

export function parityLadderConfig(seed: unknown, workers: number): JsonObject {
  if (![1, 2, 4, 8, 16].includes(workers)) throw new Error("unsupported parity ladder worker count");
  const config = object(structuredClone(seed), "parity seed");
  const tunables = object(config.tunables, "tunables");
  if (tunables.parquet_writer == null) throw new Error("historical writer evidence is not bound");
  const client = object(tunables.client, "client");
  if (client.data_profile !== "diagnostic" || client.payload_customer_id !== "direct-e64-confirmwide"
    || client.workers !== 8 || client.streams_per_worker !== 2
    || !Array.isArray(config.replica_counts) || config.replica_counts.length !== 1 || config.replica_counts[0] !== 1
    || !Array.isArray(config.tables) || config.tables.length !== 1
    || config.tables[0] !== "sqlserver_availability_replica_states") {
    throw new Error("invalid frozen parity seed");
  }
  client.workers = workers;
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
  if (tunables.parquet_writer == null) throw new Error("historical writer evidence is not bound");
  const writer = object(collectors[0]!.writer_policy_overrides, "writer policy");
  const expectedWriter = object(tunables.parquet_writer, "parquet writer");
  const expectedDefaults = object(expectedWriter.defaults, "parquet defaults");
  const actualDefaults = object(writer.defaults, "writer defaults");
  for (const [key, value] of Object.entries(expectedDefaults)) {
    if (!isDeepStrictEqual(actualDefaults[key], value)) throw new Error(`writer readback differs at ${key}`);
  }
  if (writer.max_row_group_rows != null) throw new Error("writer row-group override masks the historical layout");
  if (!Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) {
    throw new Error("client readback is unavailable");
  }
  const client = object(object(evidence.client_phase_reports[0], "client").consumer_effective_config, "client readback");
  for (const [key, value] of Object.entries(object(tunables.client, "client tunables"))) {
    if (!isDeepStrictEqual(client[key], value)) throw new Error(`client readback differs at ${key}`);
  }
  const transport = object(tunables.transport, "transport");
  for (const key of ["ipc_compression", "grpc_send_compression", "max_flight_data_bytes", "max_message_bytes"]) {
    if (!isDeepStrictEqual(client[key], transport[key])) throw new Error(`client transport readback differs at ${key}`);
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
