import assert from "node:assert/strict";
import { test } from "node:test";
import { checkCompressionCandidate, checkTypedCompressionCandidate, parseCompressionSpec } from "./compression-policy.js";

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

test("validates codec-specific levels and freezes unrelated configuration", () => {
  const baseline: { tunables: { parquet_writer: { defaults: { compression: string; compression_level: number | null }; max_row_group_rows: number } } } = {
    tunables: { parquet_writer: { defaults: { compression: "zstd", compression_level: 3 }, max_row_group_rows: 262144 } },
  };
  for (const [codec, levels] of Object.entries({
    uncompressed: [null], snappy: [null], lz4: [null], lz4_raw: [null],
    gzip: [null, 0, 9], brotli: [null, 0, 11], zstd: [null, 1, 22],
  })) {
    for (const level of levels) {
      const candidate = structuredClone(baseline);
      candidate.tunables.parquet_writer.defaults = { compression: codec, compression_level: level };
      assert.deepEqual(checkTypedCompressionCandidate(baseline, candidate), { codec, level });
    }
  }
  for (const invalid of [
    { codec: "zstd", level: 0 }, { codec: "zstd", level: 23 },
    { codec: "gzip", level: -1 }, { codec: "gzip", level: 10 },
    { codec: "brotli", level: 12 }, { codec: "snappy", level: 1 },
    { codec: "lz4", level: 0 }, { codec: "lz4_raw", level: 0 },
    { codec: "lzo", level: null }, { codec: "zstd", level: 1.5 },
    { codec: "zstd" }, { codec: "zstd", level: 3, extra: true },
  ]) {
    assert.throws(() => parseCompressionSpec(invalid));
  }
  const changed = structuredClone(baseline);
  changed.tunables.parquet_writer.max_row_group_rows++;
  assert.throws(() => checkTypedCompressionCandidate(baseline, changed), /only compression codec and level/);
});
