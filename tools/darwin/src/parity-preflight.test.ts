import assert from "node:assert/strict";
import { test } from "node:test";
import { assertParityCollectorReadback, assertParityEvidence, assertParityTableLayout, parityHostReceipt, parityLadderConfig } from "./parity-preflight.js";

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
    limits: { max_message_bytes: 134217728 },
    cpu_pressure: "process",
    runtime: { cpu_workers: 32 },
    layout: { enabled: true, max_rows: 2000000, max_input_bytes: 1073741824, row_group_rows: 262144 },
  };
  const client = { data_profile: "diagnostic", workers: 4 };
  const transport = { ipc_compression: "none", grpc_send_compression: "none",
    max_flight_data_bytes: 67108864, max_message_bytes: 134217728 };
  const parquet_writer = { defaults: { compression: "zstd", compression_level: 3 } };
  const seed = { tunables: { collector, client, transport, parquet_writer },
    platform: { local: { collector_memory_limit_bytes: 92341796864 } } };
  const readback = { ...collector, layout: { process_policy: collector.layout } };
  assert.doesNotThrow(() => assertParityCollectorReadback(seed, readback));
  assert.throws(() => assertParityCollectorReadback(seed, {
    ...readback, batching: { ...collector.batching, pipeline_depth: 1 },
  }), /batching.pipeline_depth/);
  assert.throws(() => assertParityCollectorReadback(seed, {
    ...readback, pressure: { rss_limit_bytes: 4294967296 },
  }), /pressure.rss_limit_bytes/);
  const policy = { table_name: "telemetry.sqlserver.sqlserver_availability_replica_states",
    scope: "table_effective_at_file_construction_not_full_run_history", config: collector.layout };
  assert.doesNotThrow(() => assertParityTableLayout(seed, [policy]));
  assert.throws(() => assertParityTableLayout(seed, [{
    ...policy, config: { ...collector.layout, row_group_rows: 16384 },
  }]), /table layout differs at row_group_rows/);
  const evidence = {
    processes: [{ component: "collector", log_complete: true, effective_config_source: "consumer_startup_readback",
      effective_config: readback, layout_table_policies: [policy],
      writer_policy_overrides: { ...parquet_writer, max_row_group_rows: null } }],
    client_phase_reports: [{ consumer_effective_config: { ...client, ...transport } }],
    overload_guard: { status: "complete", requested_container_memory_limit_bytes: 92341796864,
      settings: { enabled: true, cap_fraction: 0.9, zero_progress_windows: 2 },
      last_caps: [{ metric: "container_memory_bytes", confirmed: true, cap: 92341796864 }] },
  };
  assert.doesNotThrow(() => assertParityEvidence(seed, evidence));
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, overload_guard: { ...evidence.overload_guard, last_caps: [] },
  }), /memory cap is unconfirmed/);
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, client_phase_reports: [{ consumer_effective_config: { ...client, ...transport, workers: 8 } }],
  }), /client readback differs at workers/);
});
