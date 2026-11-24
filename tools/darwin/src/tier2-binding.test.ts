import assert from "node:assert/strict";
import test from "node:test";
import { bindCodeArm, bindCompressionArm, codeCommonConfigHash, type CodeArmTreatment } from "./tier2-binding.js";

const limit = 86 * 1024 ** 3;
const seed = {
  platform: { kind: "local", local: { stable_image_build: true, collector_memory_limit_bytes: limit } },
  overload_guard: { enabled: true, cap_fraction: 0.9, zero_progress_windows: 2 },
  warmup_seconds: 60, measure_seconds: 150, replica_counts: [1],
  tables: ["sqlserver_availability_replica_states"],
  tunables: { client: { data_profile: "realistic" },
    collector: { runtime: {}, pressure: { rss_limit_bytes: limit } },
    parquet_writer: { defaults: { compression: "zstd", compression_level: 3 } } },
};
const images = Object.fromEntries(["catalog", "client", "collector", "maintenance", "router", "sqlserver"]
  .map((role, index) => [role, `sha256:${String(index).repeat(64)}`]));
const role = { imageId: images.collector!, revision: "a".repeat(40) };
const treatment: CodeArmTreatment = { kind: "candidate", generated_partition_fast_path: true };

test("binds collector identity without changing common inputs", () => {
  const hash = codeCommonConfigHash(seed);
  const bound = bindCodeArm(seed, images, role);
  const local = (bound.platform as typeof seed.platform).local as Record<string, unknown>;
  assert.deepEqual(local.expected_image_ids, images);
  assert.deepEqual(local.collector_image_override, { image_id: role.imageId, source_revision: role.revision });
  const runtime = (bound.tunables as typeof seed.tunables).collector.runtime;
  assert.equal(Object.prototype.hasOwnProperty.call(runtime, "generated_partition_fast_path"), false);
  assert.equal(codeCommonConfigHash(seed), hash);
  assert.throws(() => bindCodeArm(seed, images, { ...role, imageId: images.router! }), /matching collector/);
  assert.throws(() => bindCodeArm(seed, images, { ...role, imageId: images.router! }, treatment), /matching collector/);
  assert.throws(() => bindCodeArm(seed, { ...images, client: "latest" }, role), /immutable/);
  assert.throws(() => bindCodeArm(bound, images, role), /already contains/);
  assert.throws(() => bindCodeArm(bound, images, role, treatment), /already contains/);
});

test("candidate treatment changes only the fast path without mutating its inputs", () => {
  const originalSeed = structuredClone(seed);
  const originalImages = structuredClone(images);
  const originalRole = structuredClone(role);
  const originalTreatment = structuredClone(treatment);
  const baseline = bindCodeArm(seed, images, role);
  const candidate = bindCodeArm(seed, images, role, treatment);
  const candidateRuntime = (candidate.tunables as typeof seed.tunables).collector.runtime as Record<string, unknown>;
  assert.equal(candidateRuntime.generated_partition_fast_path, true);
  const expected = structuredClone(baseline);
  const expectedRuntime = (expected.tunables as typeof seed.tunables).collector.runtime as Record<string, unknown>;
  expectedRuntime.generated_partition_fast_path = true;
  assert.deepStrictEqual(candidate, expected);
  assert.deepStrictEqual(seed, originalSeed);
  assert.deepStrictEqual(images, originalImages);
  assert.deepStrictEqual(role, originalRole);
  assert.deepStrictEqual(treatment, originalTreatment);
});

test("rejects malformed candidate treatments", () => {
  const originalSeed = structuredClone(seed);
  for (const invalid of [
    null, true, "candidate", [], {}, { kind: "baseline", generated_partition_fast_path: true },
    { kind: "candidate", generated_partition_fast_path: false },
    { kind: "candidate" }, { kind: "candidate", generated_partition_fast_path: true, extra: true },
  ]) {
    assert.throws(() => bindCodeArm(seed, images, role, invalid as CodeArmTreatment), /invalid code arm treatment/);
  }
  assert.deepStrictEqual(seed, originalSeed);
});

test("rejects weakened guard, topology and shortcut changes before admission", () => {
  for (const invalid of [
    { ...seed, overload_guard: { ...seed.overload_guard, cap_fraction: 0.95 } },
    { ...seed, overload_guard: { ...seed.overload_guard, enabled: false } },
    { ...seed, measure_seconds: 149 },
    { ...seed, replica_counts: [2] },
    { ...seed, tunables: { ...seed.tunables, client: { data_profile: "synthetic" } } },
    { ...seed, tunables: { ...seed.tunables, parquet_writer: {
      defaults: { compression: "snappy", compression_level: null } } } },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      pressure: { rss_limit_bytes: limit + 1 } } } },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      runtime: { generated_partition_fast_path: true } } } },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      runtime: { generated_partition_fast_path: false } } } },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      runtime: { generated_partition_fast_path: undefined } } } },
  ]) {
    const original = structuredClone(invalid);
    assert.throws(() => bindCodeArm(invalid, images, role), /violates/);
    assert.throws(() => bindCodeArm(invalid, images, role, treatment), /violates/);
    assert.deepStrictEqual(invalid, original);
  }
});

test("binds only the candidate codec into the guarded production configuration", () => {
  const candidate = { ...structuredClone(seed), tunables: { ...structuredClone(seed.tunables),
    parquet_writer: { defaults: { compression: "snappy", compression_level: null } } } };
  const { config, compression } = bindCompressionArm(seed, candidate, seed, images, role);
  assert.deepEqual(compression, { codec: "snappy", level: null });
  const runtime = (config.tunables as typeof seed.tunables).collector.runtime;
  assert.equal(Object.prototype.hasOwnProperty.call(runtime, "generated_partition_fast_path"), false);
  const expected = bindCodeArm(seed, images, role);
  const writer = (expected.tunables as typeof seed.tunables).parquet_writer;
  Object.assign(writer.defaults, { compression: "snappy", compression_level: null });
  assert.deepEqual(config, expected);
  candidate.overload_guard.cap_fraction = 0.95;
  assert.throws(() => bindCompressionArm(seed, candidate, seed, images, role), /only compression/);
});
