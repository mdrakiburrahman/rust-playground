import { parityLadderConfig } from "./parity-preflight.js";

type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

export function qualificationConfig(historical: unknown): JsonObject {
  const config = parityLadderConfig(historical, 8, "bounded-table-2500m-diagnostic");
  const tunables = object(config.tunables, "tunables");
  const client = object(tunables.client, "client");
  client.data_profile = "realistic";
  client.seed = 20260923;
  client.workers = 14;
  client.runtime_threads = 14;
  client.max_memory_bytes = 16 * 1024 ** 3;
  client.connections_per_worker = 2;
  tunables.parquet_writer = { defaults: { compression: "zstd", compression_level: 3 }, max_row_group_rows: 262144 };
  return config;
}
