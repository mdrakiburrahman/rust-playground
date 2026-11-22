import { isDeepStrictEqual } from "node:util";

type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as JsonObject;
}

function defaults(config: unknown): JsonObject {
  const root = object(config, "config");
  const tunables = object(root.tunables, "tunables");
  const writer = object(tunables.parquet_writer, "Parquet writer");
  return object(writer.defaults, "Parquet defaults");
}

export function checkCompressionCandidate(baseline: unknown, candidate: unknown): number {
  const baselineDefaults = defaults(baseline);
  const candidateDefaults = defaults(candidate);
  if (baselineDefaults.compression !== "zstd" || candidateDefaults.compression !== "zstd") {
    throw new Error("this lane requires the frozen zstd codec");
  }
  const baselineLevel = baselineDefaults.compression_level;
  if (typeof baselineLevel !== "number" || !Number.isInteger(baselineLevel) || baselineLevel < 1 || baselineLevel > 22) {
    throw new Error("frozen zstd level must be an integer from 1 through 22");
  }
  const level = candidateDefaults.compression_level;
  if (!Number.isInteger(level) || typeof level !== "number" || level < 1 || level > 22) {
    throw new Error("zstd level must be an integer from 1 through 22");
  }
  const clone = structuredClone(candidate);
  defaults(clone).compression_level = baselineLevel;
  if (!isDeepStrictEqual(baseline, clone)) {
    throw new Error("only the zstd compression level may differ from the frozen seed");
  }
  return level;
}
