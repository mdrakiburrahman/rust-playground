import { basename, dirname, isAbsolute, join, normalize } from "node:path";
import { isDeepStrictEqual } from "node:util";
import { parityLadderConfig } from "./parity-preflight.js";

type JsonObject = Record<string, unknown>;

function object(value: unknown, name: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as JsonObject;
}

export const COLLECTOR_SOURCE_BASE = "ad83c6376086f020a7d4e8cffd66994eedb537fe";
export const CLIENT_MEMORY = 5 * 1024 ** 3;
export const COLLECTOR_MEMORY = 86 * 1024 ** 3;

export function diagnosticPaths(root: string, snapshot: string, output: string) {
  for (const path of [root, snapshot, output]) {
    if (!isAbsolute(path) || normalize(path) !== path) throw new Error("diagnostic paths must be normalized absolute paths");
  }
  if (!basename(root).startsWith("diagnostic-") || dirname(snapshot) !== root || dirname(output) !== root
    || snapshot === output || !basename(snapshot).startsWith("diagnostic-") || !basename(output).startsWith("diagnostic-")) {
    throw new Error("diagnostic snapshot and output must be distinct direct children of a fresh diagnostic run root");
  }
  return { root, snapshot, output, home: join(root, "home"), state: join(root, "state") };
}

export function realisticDiagnosticConfig(seed: unknown): JsonObject {
  const config = parityLadderConfig(seed, 8, "historical-parity-diagnostic");
  const tunables = object(config.tunables, "tunables");
  const client = object(tunables.client, "client");
  const transport = object(tunables.transport, "transport");
  const collector = object(tunables.collector, "collector");
  const batching = object(collector.batching, "batching");
  const pressure = object(collector.pressure, "pressure");
  const commit = object(collector.commit, "commit");
  const runtime = object(collector.runtime, "runtime");
  const platform = object(config.platform, "platform");
  const local = object(platform.local, "local");
  const guard = object(config.overload_guard, "overload guard");
  if (!isDeepStrictEqual(batching.global_capacity, { bytes: 4_290_000_000, rows: 25_000_000, batches: 16_384 })
    || pressure.buffered_bytes_limit !== 4_290_000_000 || pressure.buffered_rows_limit !== 25_000_000
    || pressure.buffered_batches_limit !== 16_384 || commit.batch_window !== "2s"
    || commit.max_files_per_commit !== 128 || commit.max_pending_files !== 512 || runtime.cpu_workers !== 32
    || local.collector_memory_limit_bytes !== COLLECTOR_MEMORY || pressure.rss_limit_bytes !== COLLECTOR_MEMORY
    || guard.enabled !== true || guard.cap_fraction !== 0.9 || guard.zero_progress_windows !== 2
    || config.warmup_seconds !== 60 || config.measure_seconds !== 150 || client.streams_per_worker !== 2) {
    throw new Error("frozen parity pressure, guard, timing or topology changed");
  }
  client.data_profile = "realistic";
  client.seed = 20260923;
  client.workers = 14;
  client.runtime_threads = 14;
  client.connections_per_worker = 2;
  client.max_memory_bytes = CLIENT_MEMORY;
  client.stream_retry_max_attempts = 1;
  transport.connect_retries = 0;
  platform.compose_project = "diagnostic-stable";
  return config;
}
