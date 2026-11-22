import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createKubeletNodeResourceReader,
  parsePodResourceSample,
  startNodeObservation,
  type ObservationClock,
  type ObservationTarget,
} from './aks-node-observation.js';

const sample = (
  cpu: number,
  network: number,
  counters_available = { cpu: true, memory: true, rss: true, network: true, disk: true },
) => JSON.stringify({
  cpu_usage_usec: cpu,
  cgroup_memory_current_bytes: 4096,
  process_rss_bytes: 2048,
  process_rss_hwm_bytes: 3072,
  network_rx_bytes: network,
  network_tx_bytes: network * 2,
  disk_read_bytes: network * 3,
  disk_write_bytes: network * 4,
  counters_available,
});

function fakeClock(start: number, sleepDurations: number[] = []): ObservationClock & { set(value: number): void } {
  let now = start;
  let sleeps = 0;
  return {
    now: () => now,
    set: value => { now = value; },
    sleep: async (milliseconds, signal) => {
      if (signal.aborted) return;
      now += sleepDurations[sleeps++] ?? milliseconds;
    },
  };
}

async function settleMicrotasks(): Promise<void> {
  for (let index = 0; index < 50; index++) await Promise.resolve();
}

const target: ObservationTarget = {
  pod_uid: 'pod-1',
  pod_name: 'client-0',
  node_name: 'node-a',
  role: 'client',
  container_name: 'client',
};

test('pod resource samples preserve observed zeros and distinguish unavailable counters', () => {
  const parsed = parsePodResourceSample(sample(0, 0));
  assert.equal(parsed.cpu_usage_usec, 0);
  assert.equal(parsed.network_rx_bytes, 0);
  assert.equal(parsed.counters_available.network, true);

  const unavailable = parsePodResourceSample(sample(0, 0, {
    cpu: false, memory: false, rss: false, network: false, disk: false,
  }));
  assert.equal(unavailable.network_rx_bytes, 0);
  assert.equal(unavailable.counters_available.network, false);
  assert.throws(() => parsePodResourceSample(JSON.stringify({ unavailable_reason: 'cgroup_v2_unavailable' })), /cgroup_v2_unavailable/);
});

test('counter reset in any adjacent pair is unavailable, even when the last value is higher', async () => {
  const clock = fakeClock(1000);
  const values = [100, 20, 110];
  let read = 0;
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 1000,
    measurement_end_unix_ms: 1300,
    sample_interval_ms: 100,
    clock,
    execute: async () => sample(read * 1000, values[read++]!),
  });

  await settleMicrotasks();
  const report = await observation.stop();
  assert.equal(report.pods[0]?.network_rx_bytes_delta, null);
  assert.equal(report.pods[0]?.network_rx_unavailable_reason, 'network_rx_counter_reset');
  assert.equal(report.pods[0]?.sample_count, 3);
});

test('real zero counters are reported as zero while no samples remain unavailable', async () => {
  const clock = fakeClock(0);
  let read = 0;
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 0,
    measurement_end_unix_ms: 200,
    sample_interval_ms: 100,
    clock,
    execute: async () => { read++; return sample(0, 0); },
  });

  await settleMicrotasks();
  const report = await observation.stop();
  assert.equal(read, 2);
  assert.equal(report.pods[0]?.cpu_millicores_mean, 0);
  assert.equal(report.pods[0]?.network_rx_bytes_delta, 0);
  assert.equal(report.pods[0]?.unavailable_reason, null);
});

test('unreadable counters remain null instead of looking like zero activity', async () => {
  const clock = fakeClock(0);
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 0,
    measurement_end_unix_ms: 200,
    sample_interval_ms: 100,
    clock,
    execute: async () => sample(0, 0, {
      cpu: false, memory: true, rss: true, network: false, disk: true,
    }),
  });

  await settleMicrotasks();
  const report = await observation.stop();
  assert.equal(report.pods[0]?.network_rx_bytes_delta, null);
  assert.equal(report.pods[0]?.network_rx_unavailable_reason, 'counter_unavailable');
  assert.equal(report.pods[0]?.cgroup_memory_current_bytes_peak, 4096);
});

test('CPU mean is elapsed-time weighted and target coverage uses actual sample times', async () => {
  const clock = fakeClock(1000, [100, 300]);
  const cpu = [0, 1000, 7000];
  let read = 0;
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 1000,
    measurement_end_unix_ms: 1500,
    sample_interval_ms: 100,
    clock,
    execute: async () => sample(cpu[read++]!, 0),
  });

  await settleMicrotasks();
  const report = await observation.stop();
  const pod = report.pods[0]!;
  assert.equal(pod.cpu_millicores_mean, 17.5);
  assert.equal(pod.cpu_millicores_peak, 20);
  assert.equal(pod.coverage_start_unix_ms, 1000);
  assert.equal(pod.coverage_end_unix_ms, 1400);
  assert.equal(pod.observed_span_ms, 400);
  assert.equal(pod.coverage_ratio, 0.8);
});

test('late resource reads are discarded rather than attributed to the measurement window', async () => {
  const clock = fakeClock(95);
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 0,
    measurement_end_unix_ms: 100,
    sample_interval_ms: 100,
    clock,
    execute: async () => {
      clock.set(120);
      return sample(1, 1);
    },
  });

  await settleMicrotasks();
  const report = await observation.stop();
  assert.equal(report.pods[0]?.sample_count, 0);
  assert.equal(report.pods[0]?.read_failure_count, 1);
  assert.equal(report.pods[0]?.unavailable_reason, 'sample_completed_after_measurement_end');
});

test('stop aborts and does not wait for an unresolved resource read', async () => {
  const clock = fakeClock(50);
  let signal: AbortSignal | undefined;
  const observation = startNodeObservation({
    targets: [target],
    measurement_start_unix_ms: 0,
    measurement_end_unix_ms: 1000,
    sample_interval_ms: 100,
    clock,
    execute: async (_target, _command, readSignal) => {
      signal = readSignal;
      return new Promise<string>(() => {});
    },
  });

  await settleMicrotasks();
  const report = await observation.stop();
  assert.equal(signal?.aborted, true);
  assert.equal(report.pods[0]?.unavailable_reason, 'observation_stopped_during_sample');
  assert.equal(report.node_totals[0]?.unavailable_reason, 'node_stats_source_not_configured');
});

test('read-only kubelet adapter separates node totals and capacity from target pod totals', async () => {
  const paths: string[] = [];
  const readNode = createKubeletNodeResourceReader(async path => {
    paths.push(path);
    return path.endsWith('/stats/summary')
      ? JSON.stringify({
        node: {
          cpu: { usageCoreNanoSeconds: 1000 },
          memory: { workingSetBytes: 2048 },
          network: { rxBytes: 0, txBytes: 0 },
        },
      })
      : JSON.stringify({ status: { capacity: { cpu: '8', memory: '32Gi' } } });
  });

  const result = await readNode('node-a', new AbortController().signal);
  assert.deepEqual(paths.sort(), [
    '/api/v1/nodes/node-a',
    '/api/v1/nodes/node-a/proxy/stats/summary',
  ]);
  assert.equal(result.cpu_capacity_millicores, 8000);
  assert.equal(result.memory_capacity_bytes, 32 * 1024 ** 3);
  assert.equal(result.network_rx_bytes, 0);
  assert.equal(result.disk_read_bytes, null);
  assert.equal(result.disk_unavailable_reason, 'node_disk_io_not_exposed_by_kubelet_stats_summary');
});
