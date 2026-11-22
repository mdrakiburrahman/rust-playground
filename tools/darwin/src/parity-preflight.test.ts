import assert from "node:assert/strict";
import { test } from "node:test";
import { assertParityCollectorReadback, parityHostReceipt, parityLadderConfig } from "./parity-preflight.js";

const meminfo = "MemTotal: 1048576 kB\nMemAvailable: 524288 kB\n";
const timestamp = "2026-11-22T09:00:00Z";

test("records a bounded one-box host preflight and refuses stale unsafe caps", () => {
  const receipt = parityHostReceipt(meminfo, 32, 16, 1048576, 300000000, timestamp);
  assert.equal(receipt.hostTotalMemoryBytes, 1073741824);
  assert.equal(receipt.hostAvailableMemoryBytes, 536870912);
  assert.equal(receipt.collectorLimitBoundBytes, 401866752);
  assert.equal(receipt.label, "local one-box scale");
  assert.equal(receipt.sampledPeakCollectorRssBytes, null);
  assert.throws(() => parityHostReceipt(meminfo, 32, 16, 1048576, 401866753, timestamp), /exceeds/);
  assert.throws(() => parityHostReceipt("MemTotal: 100 kB\n", 32, 16, 1, 1, timestamp), /MemAvailable/);
});

test("the parity ladder changes only offered worker count", () => {
  const seed = {
    tables: ["sqlserver_availability_replica_states"], replica_counts: [1],
    tunables: { client: {
      data_profile: "diagnostic", payload_customer_id: "direct-e64-confirmwide",
      workers: 8, streams_per_worker: 2,
    }, collector: { runtime: { cpu_workers: 32 } } },
  };
  const config = parityLadderConfig(seed, 4);
  assert.deepEqual(config, { ...seed, tunables: { ...seed.tunables, client: { ...seed.tunables.client, workers: 4 } } });
  assert.equal(seed.tunables.client.workers, 8);
  assert.throws(() => parityLadderConfig(seed, 3), /worker count/);
  assert.throws(() => parityLadderConfig({ ...seed, replica_counts: [2] }, 4), /parity seed/);
});

test("collector parity readback rejects drift in historical limits and local pressure", () => {
  const collector = {
    batching: { global_capacity: { bytes: 4290000000 }, pipeline_depth: 8 },
    commit: { batch_window: "2s" },
    pressure: { rss_limit_bytes: 92341796864 },
    runtime: { cpu_workers: 32 },
  };
  const seed = { tunables: { collector } };
  assert.doesNotThrow(() => assertParityCollectorReadback(seed, collector));
  assert.throws(() => assertParityCollectorReadback(seed, {
    ...collector, batching: { ...collector.batching, pipeline_depth: 1 },
  }), /batching.pipeline_depth/);
  assert.throws(() => assertParityCollectorReadback(seed, {
    ...collector, pressure: { rss_limit_bytes: 4294967296 },
  }), /pressure.rss_limit_bytes/);
});
