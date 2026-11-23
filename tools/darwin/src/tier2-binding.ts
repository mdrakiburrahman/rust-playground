import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { checkTypedCompressionCandidate, type CompressionSpec } from "./compression-policy.js";

type JsonObject = Record<string, unknown>;

const ROLES = ["catalog", "client", "collector", "maintenance", "router", "sqlserver"];
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const REVISION = /^[a-f0-9]{40}$/;
const COLLECTOR_MEMORY_BYTES = 86 * 1024 ** 3;

function object(value: unknown, label: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

export interface CodeRole {
  imageId: string;
  revision: string;
}

export function bindCodeArm(seed: unknown, images: Record<string, string>, collector: CodeRole): JsonObject {
  const config = object(structuredClone(seed), "configuration");
  const platform = object(config.platform, "platform");
  const local = object(platform.local, "local");
  const tunables = object(config.tunables, "tunables");
  const collectorTunables = object(tunables.collector, "collector");
  const runtime = object(collectorTunables.runtime, "collector runtime");
  const pressure = object(collectorTunables.pressure, "collector pressure");
  const guard = object(config.overload_guard, "overload guard");
  const client = object(tunables.client, "client");
  const writer = object(tunables.parquet_writer, "writer");
  const defaults = object(writer.defaults, "writer defaults");
  if (platform.kind !== "local" || local.stable_image_build !== true
    || local.collector_memory_limit_bytes !== COLLECTOR_MEMORY_BYTES
    || pressure.rss_limit_bytes !== COLLECTOR_MEMORY_BYTES
    || guard.enabled !== true || guard.cap_fraction !== 0.9 || guard.zero_progress_windows !== 2
    || config.warmup_seconds !== 60 || config.measure_seconds !== 150
    || !isDeepStrictEqual(config.replica_counts, [1])
    || !isDeepStrictEqual(config.tables, ["sqlserver_availability_replica_states"])
    || client.data_profile !== "realistic" || defaults.compression !== "zstd" || defaults.compression_level !== 3
    || runtime.generated_partition_fast_path !== undefined) {
    throw new Error("code arm violates frozen topology, guard, profile, codec, or fast-path policy");
  }
  if (!REVISION.test(collector.revision) || !IMAGE.test(collector.imageId)
    || Object.keys(images).sort().join(",") !== ROLES.join(",")
    || ROLES.some(role => !IMAGE.test(images[role] ?? "")) || images.collector !== collector.imageId) {
    throw new Error("code arm requires six immutable role IDs and the matching collector revision");
  }
  if (local.expected_image_ids !== undefined || local.collector_image_override !== undefined) {
    throw new Error("seed already contains arm-specific image bindings");
  }
  local.expected_image_ids = { ...images };
  local.collector_image_override = { image_id: collector.imageId, source_revision: collector.revision };
  return config;
}

export function codeCommonConfigHash(seed: unknown): string {
  return createHash("sha256").update(JSON.stringify(seed)).digest("hex");
}

export function bindCompressionArm(
  frozenCandidate: unknown, candidate: unknown, qualificationSeed: unknown,
  images: Record<string, string>, collector: CodeRole,
): { config: JsonObject; compression: CompressionSpec } {
  const compression = checkTypedCompressionCandidate(frozenCandidate, candidate);
  const config = bindCodeArm(qualificationSeed, images, collector);
  const tunables = object(config.tunables, "tunables");
  const writer = object(tunables.parquet_writer, "writer");
  const defaults = object(writer.defaults, "writer defaults");
  defaults.compression = compression.codec;
  defaults.compression_level = compression.level;
  return { config, compression };
}
