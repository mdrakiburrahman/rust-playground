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
  assert.throws(() => parityLadderConfig({ ...seed, tunables: { ...seed.tunables, parquet_writer: {} } }, 4),
    /must not override the writer/);
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
  const seed = { tunables: { collector, client, transport },
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
      image_id: "sha256:" + "a".repeat(64),
      effective_config: readback, layout_table_policies: [policy],
      writer_policy_overrides: { defaults: { compression: null, compression_level: null },
        columns: {}, max_row_group_rows: null } }],
    client_phase_reports: [{ consumer_effective_config: { ...client, ...transport },
      phase_counters: { process_cumulative: { completed_rows: 10, ambiguous_rows: 0 },
        delivery_cumulative: { scope: "completed_process_lifetime_counters_not_unique_delivery",
          retried_streams: 0, retried_rows: 0, ambiguous_streams: 0, ambiguous_rows: 0 } } }],
    client_schedule: { effective: { run_start_unix_ms: 2000 } },
    parity_admission_readback: { status: "verified_before_schedule", observed_at_unix_ms: 1000,
      collector_image_id: "sha256:" + "a".repeat(64),
      collector: { ...readback, parquet_writer: { defaults: { compression: null }, columns: {} } },
      client: { ...client, ...transport } },
    overload_guard: { status: "complete", requested_container_memory_limit_bytes: 92341796864,
      settings: { enabled: true, cap_fraction: 0.9, zero_progress_windows: 2 },
      last_caps: [{ metric: "container_memory_bytes", confirmed: true, cap: 92341796864 }] },
  };
  assert.doesNotThrow(() => assertParityEvidence(seed, evidence));
  assert.throws(() => assertParityEvidence({ ...seed, tunables: { ...seed.tunables, parquet_writer: {} } }, evidence),
    /must not override the writer/);
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, overload_guard: { ...evidence.overload_guard, last_caps: [] },
  }), /memory cap is unconfirmed/);
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, client_phase_reports: [{ ...evidence.client_phase_reports[0], consumer_effective_config: { ...client, ...transport, workers: 8 } }],
  }), /pre-admission client readback changed/);
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, client_phase_reports: [{ ...evidence.client_phase_reports[0],
      phase_counters: { ...evidence.client_phase_reports[0]!.phase_counters,
        process_cumulative: { completed_rows: 10, ambiguous_rows: 1 } } }],
  }), /delivery evidence is ambiguous/);
  for (const key of ["retried_streams", "retried_rows", "ambiguous_streams", "ambiguous_rows"] as const) {
    for (const value of [1, null]) {
      assert.throws(() => assertParityEvidence(seed, {
        ...evidence, client_phase_reports: [{ ...evidence.client_phase_reports[0]!,
          phase_counters: { ...evidence.client_phase_reports[0]!.phase_counters,
            delivery_cumulative: { ...evidence.client_phase_reports[0]!.phase_counters.delivery_cumulative, [key]: value } } }],
      }), /delivery evidence is ambiguous/);
    }
  }
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, client_phase_reports: [{ ...evidence.client_phase_reports[0]!,
      phase_counters: { process_cumulative: { completed_rows: 10, ambiguous_rows: 0 } } }],
  }), /client cumulative delivery evidence/);
  assert.throws(() => assertParityEvidence(seed, {
    ...evidence, parity_admission_readback: { ...evidence.parity_admission_readback, observed_at_unix_ms: 2000 },
  }), /pre-admission collector identity or timing/);
});
