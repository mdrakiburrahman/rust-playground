import assert from 'node:assert/strict';
import test from 'node:test';
import { parsePodResourceSample, startNodeObservation, type ObservationTarget } from './aks-node-observation.js';

const sample = (cpu: number, network: number) => JSON.stringify({
  cpu_usage_usec: cpu,
  cgroup_memory_current_bytes: 4096,
  process_rss_bytes: 2048,
  process_rss_hwm_bytes: 3072,
  network_rx_bytes: network,
  network_tx_bytes: network * 2,
  disk_read_bytes: network * 3,
  disk_write_bytes: network * 4,
});

test('pod resource samples validate every required counter', () => {
  assert.deepEqual(parsePodResourceSample(sample(10, 20)), {
    cpu_usage_usec: 10,
    cgroup_memory_current_bytes: 4096,
    process_rss_bytes: 2048,
    process_rss_hwm_bytes: 3072,
    network_rx_bytes: 20,
    network_tx_bytes: 40,
    disk_read_bytes: 60,
    disk_write_bytes: 80,
  });
  assert.throws(() => parsePodResourceSample(JSON.stringify({ cpu_usage_usec: -1 })), /cpu_usage_usec/);
  assert.throws(() => parsePodResourceSample(JSON.stringify({ unavailable_reason: 'cgroup_v2_unavailable' })), /cgroup_v2_unavailable/);
});

test('observation groups pod counters by node and role and preserves RSS high water separately', async () => {
  const target: ObservationTarget = { pod_uid: 'pod-1', pod_name: 'client-0', node_name: 'node-a', role: 'client', container_name: 'client' };
  let read = 0;
  const now = Date.now();
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: now - 1000,
    measurement_end_unix_ms: now + 500,
    sample_interval_ms: 100,
    execute: async () => sample(++read * 1_000_000, read * 100),
  });
  await new Promise(resolve => setTimeout(resolve, 150));
  const report = await observation.stop();
  assert.equal(report.schema_version, 1);
  assert.equal(report.nodes.length, 1);
  assert.equal(report.nodes[0]?.node_name, 'node-a');
  assert.equal(report.nodes[0]?.role, 'client');
  assert.equal(report.nodes[0]?.network_rx_bytes_delta, 100);
  assert.equal(report.pods[0]?.process_rss_high_water_bytes, 3072);
  assert.equal(report.pods[0]?.process_rss_sampled_max_bytes, 2048);
  assert.equal(report.unavailable_reason, 'observation_stopped_before_window_end');
});
