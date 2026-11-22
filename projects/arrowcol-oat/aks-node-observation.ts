export type ObservationRole = 'collector' | 'client' | 'maintenance';

export interface ObservationTarget {
  pod_uid: string;
  pod_name: string;
  node_name: string;
  role: ObservationRole;
  container_name: string;
}

export interface PodResourceSample {
  cpu_usage_usec: number;
  cgroup_memory_current_bytes: number;
  process_rss_bytes: number;
  process_rss_hwm_bytes: number;
  network_rx_bytes: number;
  network_tx_bytes: number;
  disk_read_bytes: number;
  disk_write_bytes: number;
}

export interface ObservationPodSummary extends ObservationTarget {
  sample_count: number;
  cpu_millicores_mean: number | null;
  cpu_millicores_peak: number | null;
  cgroup_memory_current_bytes_peak: number | null;
  process_rss_sampled_max_bytes: number | null;
  process_rss_high_water_bytes: number | null;
  network_rx_bytes_delta: number | null;
  network_tx_bytes_delta: number | null;
  disk_read_bytes_delta: number | null;
  disk_write_bytes_delta: number | null;
  unavailable_reason: string | null;
}

export interface ObservationNodeSummary {
  node_name: string;
  role: ObservationRole;
  pod_uids: string[];
  sample_count: number;
  cpu_millicores_mean: number | null;
  cpu_millicores_peak: number | null;
  cgroup_memory_current_bytes_peak: number | null;
  process_rss_sampled_max_bytes: number | null;
  process_rss_high_water_sum_bytes: number | null;
  network_rx_bytes_delta: number | null;
  network_tx_bytes_delta: number | null;
  disk_read_bytes_delta: number | null;
  disk_write_bytes_delta: number | null;
}

export interface NodeResourceObservation {
  schema_version: 1;
  source: 'pod_procfs_cgroup_v2';
  scope: 'target_pods_aggregated_by_node_and_role';
  sample_interval_ms: number;
  measurement_start_unix_ms: number;
  measurement_end_unix_ms: number;
  nodes: ObservationNodeSummary[];
  pods: ObservationPodSummary[];
  unavailable_reason: string | null;
}

export type ObservationExecutor = (target: ObservationTarget, command: string) => Promise<string>;

export interface StartNodeObservationOptions {
  targets: readonly ObservationTarget[];
  measurement_start_unix_ms: number;
  measurement_end_unix_ms: number;
  sample_interval_ms?: number;
  execute: ObservationExecutor;
}

export interface NodeObservationHandle {
  stop(): Promise<NodeResourceObservation>;
}

export const POD_RESOURCE_SAMPLE_COMMAND = [
  'set -eu',
  'cg_path=$(awk -F: \'$1 == "0" {print $3}\' /proc/self/cgroup)',
  'cg="/sys/fs/cgroup${cg_path}"',
  'if [ ! -r "$cg/cpu.stat" ] || [ ! -r "$cg/memory.current" ] || [ ! -r "$cg/io.stat" ]; then',
  '  printf \'{"unavailable_reason":"cgroup_v2_unavailable"}\\n\'',
  '  exit 0',
  'fi',
  'cpu_usage_usec=$(awk \'$1 == "usage_usec" {print $2}\' "$cg/cpu.stat")',
  'memory_current=$(cat "$cg/memory.current")',
  'process_rss=$(awk \'$1 == "VmRSS:" {print $2 * 1024}\' /proc/1/status)',
  'process_rss_hwm=$(awk \'$1 == "VmHWM:" {print $2 * 1024}\' /proc/1/status)',
  'network=$(awk \'NR > 2 {iface = $1; sub(/:$/, "", iface); if (iface != "lo") {rx += $2; tx += $10}} END {printf "%.0f %.0f", rx, tx}\' /proc/net/dev)',
  'set -- $network; network_rx=${1:-0}; network_tx=${2:-0}',
  'disk=$(awk \'{for (i = 2; i <= NF; i++) {split($i, a, "="); if (a[1] == "rbytes") r += a[2]; else if (a[1] == "wbytes") w += a[2]}} END {printf "%.0f %.0f", r, w}\' "$cg/io.stat")',
  'set -- $disk; disk_read=${1:-0}; disk_write=${2:-0}',
  'printf \'{"cpu_usage_usec":%s,"cgroup_memory_current_bytes":%s,"process_rss_bytes":%s,"process_rss_hwm_bytes":%s,"network_rx_bytes":%s,"network_tx_bytes":%s,"disk_read_bytes":%s,"disk_write_bytes":%s}\\n\' "$cpu_usage_usec" "$memory_current" "$process_rss" "$process_rss_hwm" "$network_rx" "$network_tx" "$disk_read" "$disk_write"',
].join('\n');

function nonNegativeSafeInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error('invalid pod resource sample field: ' + name);
  }
  return value;
}

export function parsePodResourceSample(output: string): PodResourceSample {
  let value: unknown;
  try { value = JSON.parse(output); }
  catch { throw new Error('pod resource sample is not valid JSON'); }
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('pod resource sample is not an object');
  const sample = value as Record<string, unknown>;
  if (typeof sample.unavailable_reason === 'string') throw new Error(sample.unavailable_reason);
  return {
    cpu_usage_usec: nonNegativeSafeInteger(sample.cpu_usage_usec, 'cpu_usage_usec'),
    cgroup_memory_current_bytes: nonNegativeSafeInteger(sample.cgroup_memory_current_bytes, 'cgroup_memory_current_bytes'),
    process_rss_bytes: nonNegativeSafeInteger(sample.process_rss_bytes, 'process_rss_bytes'),
    process_rss_hwm_bytes: nonNegativeSafeInteger(sample.process_rss_hwm_bytes, 'process_rss_hwm_bytes'),
    network_rx_bytes: nonNegativeSafeInteger(sample.network_rx_bytes, 'network_rx_bytes'),
    network_tx_bytes: nonNegativeSafeInteger(sample.network_tx_bytes, 'network_tx_bytes'),
    disk_read_bytes: nonNegativeSafeInteger(sample.disk_read_bytes, 'disk_read_bytes'),
    disk_write_bytes: nonNegativeSafeInteger(sample.disk_write_bytes, 'disk_write_bytes'),
  };
}

type TimedSample = PodResourceSample & { round: number; observed_at_unix_ms: number };
type RateSample = { round: number; cpu_millicores: number };
type PodDetails = { target: ObservationTarget; samples: TimedSample[]; rates: RateSample[]; summary: ObservationPodSummary };

function validateOptions(options: StartNodeObservationOptions): void {
  if (!Number.isSafeInteger(options.measurement_start_unix_ms) || options.measurement_start_unix_ms < 0
      || !Number.isSafeInteger(options.measurement_end_unix_ms) || options.measurement_end_unix_ms <= options.measurement_start_unix_ms) {
    throw new Error('invalid node observation measurement window');
  }
  const interval = options.sample_interval_ms ?? 10_000;
  if (!Number.isSafeInteger(interval) || interval < 100 || interval > 60_000) throw new Error('sample_interval_ms must be 100..60000');
  const uids = new Set<string>();
  for (const target of options.targets) {
    for (const [name, value] of Object.entries(target)) {
      if (typeof value !== 'string' || value.length === 0) throw new Error('invalid observation target field: ' + name);
    }
    if (!['collector', 'client', 'maintenance'].includes(target.role)) throw new Error('invalid observation target role');
    if (uids.has(target.pod_uid)) throw new Error('duplicate observation pod UID');
    uids.add(target.pod_uid);
  }
  if (options.targets.length === 0) throw new Error('node observation requires at least one target pod');
}

function lastDelta(samples: TimedSample[], field: keyof PodResourceSample): number | null {
  if (samples.length < 2) return null;
  const first = samples[0]![field];
  const last = samples[samples.length - 1]![field];
  return last >= first ? last - first : null;
}

function summarizePod(target: ObservationTarget, samples: TimedSample[]): PodDetails {
  const rates: RateSample[] = [];
  for (let index = 1; index < samples.length; index++) {
    const previous = samples[index - 1]!;
    const current = samples[index]!;
    const elapsed = current.observed_at_unix_ms - previous.observed_at_unix_ms;
    if (elapsed <= 0 || current.cpu_usage_usec < previous.cpu_usage_usec) continue;
    rates.push({ round: current.round, cpu_millicores: (current.cpu_usage_usec - previous.cpu_usage_usec) / elapsed });
  }
  const cpu = rates.map(rate => rate.cpu_millicores);
  const deltas = [
    lastDelta(samples, 'network_rx_bytes'), lastDelta(samples, 'network_tx_bytes'),
    lastDelta(samples, 'disk_read_bytes'), lastDelta(samples, 'disk_write_bytes'),
  ];
  const unavailableReason = samples.length < 2 ? 'insufficient_resource_counter_samples'
    : deltas.some(delta => delta === null) ? 'resource_counter_reset' : null;
  return {
    target, samples, rates,
    summary: {
      ...target,
      sample_count: samples.length,
      cpu_millicores_mean: cpu.length ? cpu.reduce((sum, value) => sum + value, 0) / cpu.length : null,
      cpu_millicores_peak: cpu.length ? Math.max(...cpu) : null,
      cgroup_memory_current_bytes_peak: samples.length ? Math.max(...samples.map(sample => sample.cgroup_memory_current_bytes)) : null,
      process_rss_sampled_max_bytes: samples.length ? Math.max(...samples.map(sample => sample.process_rss_bytes)) : null,
      process_rss_high_water_bytes: samples.length ? Math.max(...samples.map(sample => sample.process_rss_hwm_bytes)) : null,
      network_rx_bytes_delta: deltas[0],
      network_tx_bytes_delta: deltas[1],
      disk_read_bytes_delta: deltas[2],
      disk_write_bytes_delta: deltas[3],
      unavailable_reason: unavailableReason,
    },
  };
}

function summarizeNodeRole(details: PodDetails[]): ObservationNodeSummary {
  const expectedPods = details.length;
  const valuesByRound = new Map<number, { cpu: number; cpuPods: number; memory: number; rss: number; pods: number }>();
  for (const pod of details) {
    for (const sample of pod.samples) {
      const values = valuesByRound.get(sample.round) ?? { cpu: 0, cpuPods: 0, memory: 0, rss: 0, pods: 0 };
      values.memory += sample.cgroup_memory_current_bytes;
      values.rss += sample.process_rss_bytes;
      values.pods++;
      valuesByRound.set(sample.round, values);
    }
    for (const rate of pod.rates) {
      const values = valuesByRound.get(rate.round) ?? { cpu: 0, cpuPods: 0, memory: 0, rss: 0, pods: 0 };
      values.cpu += rate.cpu_millicores;
      values.cpuPods++;
      valuesByRound.set(rate.round, values);
    }
  }
  const completeRounds = [...valuesByRound.values()].filter(values => values.pods === expectedPods);
  const completeCpuRounds = [...valuesByRound.values()].filter(values => values.cpuPods === expectedPods).map(values => values.cpu);
  const networkRx = details.map(detail => detail.summary.network_rx_bytes_delta);
  const networkTx = details.map(detail => detail.summary.network_tx_bytes_delta);
  const diskRead = details.map(detail => detail.summary.disk_read_bytes_delta);
  const diskWrite = details.map(detail => detail.summary.disk_write_bytes_delta);
  const sumComplete = (values: Array<number | null>): number | null =>
    values.every((value): value is number => value !== null) ? values.reduce((sum, value) => sum + value, 0) : null;
  return {
    node_name: details[0]!.target.node_name,
    role: details[0]!.target.role,
    pod_uids: details.map(detail => detail.target.pod_uid).sort(),
    sample_count: details.reduce((sum, detail) => sum + detail.samples.length, 0),
    cpu_millicores_mean: completeCpuRounds.length ? completeCpuRounds.reduce((sum, value) => sum + value, 0) / completeCpuRounds.length : null,
    cpu_millicores_peak: completeCpuRounds.length ? Math.max(...completeCpuRounds) : null,
    cgroup_memory_current_bytes_peak: completeRounds.length ? Math.max(...completeRounds.map(values => values.memory)) : null,
    process_rss_sampled_max_bytes: completeRounds.length ? Math.max(...completeRounds.map(values => values.rss)) : null,
    process_rss_high_water_sum_bytes: details.every(detail => detail.summary.process_rss_high_water_bytes !== null)
      ? details.reduce((sum, detail) => sum + detail.summary.process_rss_high_water_bytes!, 0) : null,
    network_rx_bytes_delta: sumComplete(networkRx),
    network_tx_bytes_delta: sumComplete(networkTx),
    disk_read_bytes_delta: sumComplete(diskRead),
    disk_write_bytes_delta: sumComplete(diskWrite),
  };
}

function createObservation(options: StartNodeObservationOptions, samplesByUid: Map<string, TimedSample[]>, failures: Set<string>, stoppedAt: number): NodeResourceObservation {
  const groups = new Map<string, PodDetails[]>();
  const pods = options.targets.map(target => {
    const details = summarizePod(target, samplesByUid.get(target.pod_uid) ?? []);
    const key = target.node_name + '\\0' + target.role;
    const group = groups.get(key) ?? [];
    group.push(details);
    groups.set(key, group);
    if (details.summary.unavailable_reason) failures.add(details.summary.unavailable_reason);
    return details.summary;
  });
  const nodes = [...groups.values()].map(summarizeNodeRole).sort((a, b) => a.node_name.localeCompare(b.node_name) || a.role.localeCompare(b.role));
  let unavailableReason: string | null = null;
  if (failures.size) unavailableReason = [...failures].sort()[0]!;
  else if (pods.every(pod => pod.sample_count === 0)) unavailableReason = 'no_pod_resource_samples';
  else if (stoppedAt < options.measurement_end_unix_ms) unavailableReason = 'observation_stopped_before_window_end';
  return {
    schema_version: 1,
    source: 'pod_procfs_cgroup_v2',
    scope: 'target_pods_aggregated_by_node_and_role',
    sample_interval_ms: options.sample_interval_ms ?? 10_000,
    measurement_start_unix_ms: options.measurement_start_unix_ms,
    measurement_end_unix_ms: options.measurement_end_unix_ms,
    nodes,
    pods,
    unavailable_reason: unavailableReason,
  };
}

export function startNodeObservation(options: StartNodeObservationOptions): NodeObservationHandle {
  validateOptions(options);
  const sampleInterval = options.sample_interval_ms ?? 10_000;
  const samplesByUid = new Map(options.targets.map(target => [target.pod_uid, [] as TimedSample[]]));
  const failures = new Set<string>();
  let stopping = false;
  let wake: (() => void) | undefined;
  const wait = (milliseconds: number): Promise<void> => new Promise(resolve => {
    if (milliseconds <= 0) { resolve(); return; }
    const timer = setTimeout(() => { if (wake === finish) wake = undefined; resolve(); }, milliseconds);
    const finish = () => { clearTimeout(timer); if (wake === finish) wake = undefined; resolve(); };
    wake = finish;
  });
  const loop = (async () => {
    let round = 0;
    while (!stopping) {
      const roundStart = Date.now();
      if (roundStart < options.measurement_start_unix_ms) {
        await wait(Math.min(sampleInterval, options.measurement_start_unix_ms - roundStart));
        continue;
      }
      if (roundStart >= options.measurement_end_unix_ms) break;
      const currentRound = round++;
      await Promise.all(options.targets.map(async target => {
        try {
          const output = await options.execute(target, POD_RESOURCE_SAMPLE_COMMAND);
          const sample = parsePodResourceSample(output);
          samplesByUid.get(target.pod_uid)!.push({ ...sample, round: currentRound, observed_at_unix_ms: Date.now() });
        } catch {
          failures.add('pod_resource_sample_unavailable');
        }
      }));
      const delay = Math.min(sampleInterval - (Date.now() - roundStart), options.measurement_end_unix_ms - Date.now());
      if (delay > 0) await wait(delay);
    }
  })();
  return {
    async stop() {
      stopping = true;
      wake?.();
      await loop;
      return createObservation(options, samplesByUid, failures, Date.now());
    },
  };
}
