import assert from "node:assert/strict";
import { test } from "node:test";
import { checkCompressionCandidate } from "./compression-policy.js";

test("allows only a supported compression level against the frozen configuration", () => {
  const baseline = {
    platform: { kind: "local" },
    tunables: { parquet_writer: { defaults: { compression: "zstd", compression_level: 3 } } },
    tables: ["one"],
  };
  const candidate = structuredClone(baseline);
  candidate.tunables.parquet_writer.defaults.compression_level = 6;
  assert.equal(checkCompressionCandidate(baseline, candidate), 6);
  candidate.tables.push("another");
  assert.throws(() => checkCompressionCandidate(baseline, candidate), /only the zstd/);
  candidate.tables.pop();
  candidate.tunables.parquet_writer.defaults.compression_level = 999;
  assert.throws(() => checkCompressionCandidate(baseline, candidate), /zstd level/);
  candidate.tunables.parquet_writer.defaults.compression_level = 6;
  candidate.tunables.parquet_writer.defaults.compression = "snappy";
  assert.throws(() => checkCompressionCandidate(baseline, candidate), /frozen zstd/);
});
