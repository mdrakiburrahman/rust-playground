import assert from "node:assert/strict";
import { test } from "node:test";
import { scoreCases, type Pair } from "./score.js";

function pair(rate: number, bytes = 34.7, baselineRate = 100): Pair {
  return {
    baseline: { durableRowsPerSecond: baselineRate, parquetBytesPerRow: 34.7 },
    candidate: { durableRowsPerSecond: rate, parquetBytesPerRow: bytes },
  };
}

test("scores paired cases equally and rejects noisy or insufficient gains", () => {
  const cases = [
    { name: "small", pairs: [pair(110), pair(110), pair(110)] },
    { name: "large", pairs: [pair(120), pair(120), pair(120)] },
  ];
  const score = scoreCases(cases, "durable-rate");
  assert.ok(Math.abs(score.combinedScore - Math.sqrt(1.1 * 1.2)) < 1e-12);
  assert.equal(score.accepted, true);
  assert.equal(scoreCases([{ name: "noisy", pairs: [pair(80), pair(110), pair(150)] }], "durable-rate").accepted, false);
  assert.equal(scoreCases([{ name: "small gain", pairs: [pair(104), pair(104), pair(104)] }], "durable-rate").accepted, false);
});

test("rejects compression regressions and requires evidence for the throughput floor", () => {
  assert.equal(scoreCases([{ name: "oversize", pairs: [pair(110, 39), pair(110, 39), pair(110, 39)] }], "durable-rate").accepted, false);
  assert.equal(scoreCases([{ name: "uncertain", pairs: [pair(95, 30), pair(105, 30), pair(110, 30)] }], "parquet-bytes-per-row").accepted, false);
  assert.equal(scoreCases([{ name: "improved", pairs: [pair(110, 30), pair(110, 30), pair(110, 30)] }], "parquet-bytes-per-row").accepted, true);
});

test("rejects invalid measurements and too few independent pairs", () => {
  assert.throws(() => scoreCases([{ name: "few", pairs: [pair(110)] }], "durable-rate"));
  assert.throws(() => scoreCases([{ name: "invalid", pairs: [pair(NaN), pair(110), pair(110)] }], "durable-rate"));
  assert.throws(() => scoreCases([{ name: "zero", pairs: [pair(0), pair(110), pair(110)] }], "durable-rate"));
});
