import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { qualificationConfig } from "./qualification-config.js";
import { bindCodeArm } from "./tier2-binding.js";

test("derives the common Realistic lane without weakening the historical guard", async () => {
  const seed = JSON.parse(await readFile("experiments/darwin/historical-parity.oat.json", "utf8"));
  const config = qualificationConfig(seed);
  const images = Object.fromEntries(["catalog", "client", "collector", "maintenance", "router", "sqlserver"]
    .map((role, index) => [role, `sha256:${String(index).repeat(64)}`]));
  assert.doesNotThrow(() => bindCodeArm(config, images, { imageId: images.collector!, revision: "a".repeat(40) }));
  const tunables = config.tunables as { client: { workers: number; max_memory_bytes: number };
    collector: { batching: { per_table_capacity: { bytes: number } }; runtime: Record<string, unknown> } };
  assert.equal(tunables.client.workers, 14);
  assert.equal(tunables.client.max_memory_bytes, 16 * 1024 ** 3);
  assert.equal(tunables.collector.batching.per_table_capacity.bytes, 2_500_000_000);
  assert.equal("generated_partition_fast_path" in tunables.collector.runtime, false);
  assert.equal(seed.tunables.client.data_profile, "diagnostic");
});
