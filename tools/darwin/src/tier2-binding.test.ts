import assert from "node:assert/strict";
import test from "node:test";
import { bindCodeArm, codeCommonConfigHash } from "./tier2-binding.js";

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

test("binds collector identity without changing common inputs", () => {
  const hash = codeCommonConfigHash(seed);
  const bound = bindCodeArm(seed, images, role);
  const local = (bound.platform as typeof seed.platform).local as Record<string, unknown>;
  assert.deepEqual(local.expected_image_ids, images);
  assert.deepEqual(local.collector_image_override, { image_id: role.imageId, source_revision: role.revision });
  assert.equal(codeCommonConfigHash(seed), hash);
  assert.throws(() => bindCodeArm(seed, images, { ...role, imageId: images.router! }), /matching collector/);
  assert.throws(() => bindCodeArm(seed, { ...images, client: "latest" }, role), /immutable/);
  assert.throws(() => bindCodeArm(bound, images, role), /already contains/);
});

test("rejects weakened guard, topology and shortcut changes before admission", () => {
  for (const invalid of [
    { ...seed, overload_guard: { ...seed.overload_guard, cap_fraction: 0.95 } },
    { ...seed, replica_counts: [2] },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      runtime: { generated_partition_fast_path: true } } } },
    { ...seed, tunables: { ...seed.tunables, collector: { ...seed.tunables.collector,
      runtime: { generated_partition_fast_path: false } } } },
  ]) assert.throws(() => bindCodeArm(invalid, images, role), /violates/);
});
