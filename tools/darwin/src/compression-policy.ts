import { isDeepStrictEqual } from "node:util";

type JsonObject = Record<string, unknown>;

export type CompressionSpec =
  | { codec: "uncompressed" | "snappy" | "lz4" | "lz4_raw"; level: null }
  | { codec: "gzip" | "brotli" | "zstd"; level: number | null };

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

export function parseCompressionSpec(value: unknown): CompressionSpec {
  const spec = object(value, "compression");
  if (Object.keys(spec).some(key => key !== "codec" && key !== "level")
    || !Object.hasOwn(spec, "codec") || !Object.hasOwn(spec, "level")) {
    throw new Error("compression must contain only codec and level");
  }
  const { codec, level } = spec;
  switch (codec) {
    case "uncompressed":
    case "snappy":
    case "lz4":
    case "lz4_raw":
      if (level !== null) throw new Error(`${codec} does not accept a compression level`);
      return { codec, level: null };
    case "gzip":
    case "brotli":
    case "zstd": {
      const min = codec === "zstd" ? 1 : 0;
      const max = codec === "gzip" ? 9 : codec === "brotli" ? 11 : 22;
      if (level !== null && (typeof level !== "number" || !Number.isInteger(level) || level < min || level > max)) {
        throw new Error(`${codec} compression level must be null or an integer from ${min} through ${max}`);
      }
      return { codec, level };
    }
    default:
      throw new Error("unknown compression codec");
  }
}

export function configCompressionSpec(config: unknown): CompressionSpec {
  const configDefaults = defaults(config);
  return parseCompressionSpec({ codec: configDefaults.compression, level: configDefaults.compression_level ?? null });
}

export function checkTypedCompressionCandidate(baseline: unknown, candidate: unknown): CompressionSpec {
  const baselineDefaults = defaults(baseline);
  configCompressionSpec(baseline);
  const spec = configCompressionSpec(candidate);
  const clone = structuredClone(candidate);
  const cloneDefaults = defaults(clone);
  for (const key of ["compression", "compression_level"]) {
    if (Object.hasOwn(baselineDefaults, key)) cloneDefaults[key] = baselineDefaults[key];
    else delete cloneDefaults[key];
  }
  if (!isDeepStrictEqual(baseline, clone)) {
    throw new Error("only compression codec and level may differ from the frozen seed");
  }
  return spec;
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
