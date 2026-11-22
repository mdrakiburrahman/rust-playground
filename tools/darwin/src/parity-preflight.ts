import { readFile } from "node:fs/promises";
import { availableParallelism, cpus } from "node:os";

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
  const client = object(object(config.tunables, "tunables").client, "client");
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
