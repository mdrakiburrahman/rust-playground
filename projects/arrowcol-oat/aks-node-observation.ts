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
  counters_available: {
    cpu: boolean;
    memory: boolean;
    rss: boolean;
    network: boolean;
    disk: boolean;
  };
}

export interface ObservationPodSummary extends ObservationTarget {
  sample_count: number;
  read_attempt_count: number;
  read_failure_count: number;
  coverage_start_unix_ms: number | null;
  coverage_end_unix_ms: number | null;
  observed_span_ms: number;
  measurement_window_ms: number;
  coverage_ratio: number;
  cpu_millicores_mean: number | null;
  cpu_millicores_peak: number | null;
  cpu_unavailable_reason: string | null;
  cgroup_memory_current_bytes_peak: number | null;
  memory_unavailable_reason: string | null;
  process_rss_sampled_max_bytes: number | null;
  process_rss_high_water_bytes: number | null;
  rss_unavailable_reason: string | null;
  network_rx_bytes_delta: number | null;
  network_rx_unavailable_reason: string | null;
  network_tx_bytes_delta: number | null;
  network_tx_unavailable_reason: string | null;
  disk_read_bytes_delta: number | null;
  disk_read_unavailable_reason: string | null;
  disk_write_bytes_delta: number | null;
  disk_write_unavailable_reason: string | null;
  unavailable_reason: string | null;
}

export interface ObservationNodeSummary {
  node_name: string;
  role: ObservationRole;
  pod_uids: string[];
  sample_count: number;
  coverage_start_unix_ms: number | null;
  coverage_end_unix_ms: number | null;
  observed_span_ms: number;
  measurement_window_ms: number;
  coverage_ratio: number;
  cpu_millicores_mean: number | null;
  cpu_millicores_peak: number | null;
  cpu_unavailable_reason: string | null;
  cgroup_memory_current_bytes_peak: number | null;
  process_rss_sampled_max_bytes: number | null;
  process_rss_high_water_sum_bytes: number | null;
  network_rx_bytes_delta: number | null;
  network_rx_unavailable_reason: string | null;
  network_tx_bytes_delta: number | null;
  network_tx_unavailable_reason: string | null;
  disk_read_bytes_delta: number | null;
  disk_read_unavailable_reason: string | null;
  disk_write_bytes_delta: number | null;
  disk_write_unavailable_reason: string | null;
}

export interface NodeHostSample {
  cpu_usage_core_nanoseconds: number;
  memory_working_set_bytes: number;
  cpu_capacity_millicores: number;
  memory_capacity_bytes: number;
  network_rx_bytes: number | null;
  network_tx_bytes: number | null;
  disk_read_bytes: number | null;
  disk_write_bytes: number | null;
  network_rx_unavailable_reason: string | null;
  network_tx_unavailable_reason: string | null;
  disk_unavailable_reason: string | null;
}

export interface ObservationNodeTotalSummary {
  node_name: string;
  sample_count: number;
  read_attempt_count: number;
  read_failure_count: number;
  coverage_start_unix_ms: number | null;
  coverage_end_unix_ms: number | null;
  observed_span_ms: number;
  measurement_window_ms: number;
  coverage_ratio: number;
  cpu_millicores_mean: number | null;
  cpu_millicores_peak: number | null;
  cpu_capacity_millicores: number | null;
  cpu_peak_utilization_percent: number | null;
  cpu_unavailable_reason: string | null;
  capacity_unavailable_reason: string | null;
  memory_working_set_bytes_peak: number | null;
  memory_capacity_bytes: number | null;
  memory_peak_utilization_percent: number | null;
  memory_unavailable_reason: string | null;
  network_rx_bytes_delta: number | null;
  network_rx_unavailable_reason: string | null;
  network_tx_bytes_delta: number | null;
  network_tx_unavailable_reason: string | null;
  disk_read_bytes_delta: number | null;
  disk_read_unavailable_reason: string | null;
  disk_write_bytes_delta: number | null;
  disk_write_unavailable_reason: string | null;
  unavailable_reason: string | null;
}

export interface NodeResourceObservation {
  schema_version: 2;
  source: 'pod_procfs_cgroup_v2';
  scope: 'target_pods_aggregated_by_node_and_role';
  node_total_source: 'kubelet_stats_summary' | 'unavailable';
  node_total_unavailable_reason: string | null;
  sample_interval_ms: number;
  sample_timeout_ms: number;
  measurement_start_unix_ms: number;
  measurement_end_unix_ms: number;
  nodes: ObservationNodeSummary[];
  node_totals: ObservationNodeTotalSummary[];
  pods: ObservationPodSummary[];
  unavailable_reason: string | null;
}

export type ObservationExecutor = (
  target: ObservationTarget,
  command: string,
  signal: AbortSignal,
) => Promise<string>;
export type ObservationNodeExecutor = (node_name: string, signal: AbortSignal) => Promise<NodeHostSample>;
export type ReadOnlyKubernetesGet = (path: string, signal: AbortSignal) => Promise<string>;

export interface ObservationClock {
  now(): number;
  sleep(milliseconds: number, signal: AbortSignal): Promise<void>;
}

export interface StartNodeObservationOptions {
  targets: readonly ObservationTarget[];
  measurement_start_unix_ms: number;
  measurement_end_unix_ms: number;
  sample_interval_ms?: number;
  sample_timeout_ms?: number;
  execute: ObservationExecutor;
  read_node?: ObservationNodeExecutor;
  clock?: ObservationClock;
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
  'cpu_available=true; cpu_usage_usec=0',
  'if ! cpu_usage_usec=$(awk \'$1 == "usage_usec" {print $2; found=1} END {if (!found) exit 1}\' "$cg/cpu.stat"); then cpu_available=false; fi',
  'memory_available=true; memory_current=0',
  'if ! memory_current=$(cat "$cg/memory.current") || ! printf "%s" "$memory_current" | grep -Eq "^[0-9]+$"; then memory_available=false; memory_current=0; fi',
  'rss_available=true; process_rss=0; process_rss_hwm=0',
  'if ! rss=$(awk \'$1 == "VmRSS:" {print $2; found=1} END {if (!found) exit 1}\' /proc/1/status); then rss_available=false; else process_rss=$((rss * 1024)); fi',
  'if ! hwm=$(awk \'$1 == "VmHWM:" {print $2; found=1} END {if (!found) exit 1}\' /proc/1/status); then rss_available=false; else process_rss_hwm=$((hwm * 1024)); fi',
  'network_available=true; network_rx=0; network_tx=0',
  'if ! network=$(awk \'NR > 2 {iface = $1; sub(/:$/, "", iface); if (iface != "lo") {rx += $2; tx += $10; found=1}} END {if (!found) exit 1; printf "%.0f %.0f", rx, tx}\' /proc/net/dev); then network_available=false; else set -- $network; network_rx=$1; network_tx=$2; fi',
  'disk_available=true; disk_read=0; disk_write=0',
  'if ! disk=$(awk \'{for (i = 2; i <= NF; i++) {split($i, a, "="); if (a[1] == "rbytes") {r += a[2]; found=1} else if (a[1] == "wbytes") {w += a[2]; found=1}}} END {if (!found) exit 1; printf "%.0f %.0f", r, w}\' "$cg/io.stat"); then disk_available=false; else set -- $disk; disk_read=$1; disk_write=$2; fi',
  'printf \'{"cpu_usage_usec":%s,"cgroup_memory_current_bytes":%s,"process_rss_bytes":%s,"process_rss_hwm_bytes":%s,"network_rx_bytes":%s,"network_tx_bytes":%s,"disk_read_bytes":%s,"disk_write_bytes":%s,"counters_available":{"cpu":%s,"memory":%s,"rss":%s,"network":%s,"disk":%s}}\\n\' "$cpu_usage_usec" "$memory_current" "$process_rss" "$process_rss_hwm" "$network_rx" "$network_tx" "$disk_read" "$disk_write" "$cpu_available" "$memory_available" "$rss_available" "$network_available" "$disk_available"',
].join('\n');

const defaultClock: ObservationClock = {
  now: () => Date.now(),
  sleep: (milliseconds, signal) => new Promise(resolve => {
    if (signal.aborted || milliseconds <= 0) {
      resolve();
      return;
    }
    let timer: ReturnType<typeof setTimeout>;
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', finish);
      resolve();
    };
    timer = setTimeout(finish, milliseconds);
    signal.addEventListener('abort', finish, { once: true });
  }),
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function nonNegativeSafeInteger(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error('invalid pod resource sample field: ' + name);
  }
  return value;
}

function requiredRecord(value: unknown, name: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error('invalid node resource sample field: ' + name);
  return value;
}

export function parsePodResourceSample(output: string): PodResourceSample {
  let value: unknown;
  try { value = JSON.parse(output); }
  catch { throw new Error('pod resource sample is not valid JSON'); }
  if (!isRecord(value)) throw new Error('pod resource sample is not an object');
  if (typeof value.unavailable_reason === 'string') throw new Error(value.unavailable_reason);
  const availability = requiredRecord(value.counters_available, 'counters_available');
  for (const name of ['cpu', 'memory', 'rss', 'network', 'disk'] as const) {
    if (typeof availability[name] !== 'boolean') throw new Error('invalid counter availability field: ' + name);
  }
  return {
    cpu_usage_usec: nonNegativeSafeInteger(value.cpu_usage_usec, 'cpu_usage_usec'),
    cgroup_memory_current_bytes: nonNegativeSafeInteger(value.cgroup_memory_current_bytes, 'cgroup_memory_current_bytes'),
    process_rss_bytes: nonNegativeSafeInteger(value.process_rss_bytes, 'process_rss_bytes'),
    process_rss_hwm_bytes: nonNegativeSafeInteger(value.process_rss_hwm_bytes, 'process_rss_hwm_bytes'),
    network_rx_bytes: nonNegativeSafeInteger(value.network_rx_bytes, 'network_rx_bytes'),
    network_tx_bytes: nonNegativeSafeInteger(value.network_tx_bytes, 'network_tx_bytes'),
    disk_read_bytes: nonNegativeSafeInteger(value.disk_read_bytes, 'disk_read_bytes'),
    disk_write_bytes: nonNegativeSafeInteger(value.disk_write_bytes, 'disk_write_bytes'),
    counters_available: {
      cpu: availability.cpu === true,
      memory: availability.memory === true,
      rss: availability.rss === true,
      network: availability.network === true,
      disk: availability.disk === true,
    },
  };
}

function objectPath(value: unknown, path: string, name: string): Record<string, unknown> {
  let current = value;
  for (const part of path.split('.')) current = isRecord(current) ? current[part] : undefined;
  return requiredRecord(current, name);
}

function numberField(value: unknown, name: string): number {
  return nonNegativeSafeInteger(value, name);
}

function parseQuantity(value: unknown, kind: 'cpu' | 'memory', name: string): number {
  if (typeof value !== 'string' || value.length === 0) throw new Error('invalid node resource sample field: ' + name);
  const match = /^([0-9]+(?:\.[0-9]+)?)(m|Ki|Mi|Gi|Ti|K|M|G|T)?$/.exec(value);
  if (!match) throw new Error('invalid node resource quantity: ' + name);
  const amount = Number(match[1]);
  const suffix = match[2] ?? '';
  const factor = kind === 'cpu'
    ? suffix === 'm' ? 1 : suffix === '' ? 1000 : NaN
    : ({ '': 1, Ki: 1024, Mi: 1024 ** 2, Gi: 1024 ** 3, Ti: 1024 ** 4, K: 1000, M: 1000 ** 2, G: 1000 ** 3, T: 1000 ** 4 } as Record<string, number>)[suffix];
  const result = amount * factor;
  if (!Number.isSafeInteger(result) || result < 0) throw new Error('invalid node resource quantity: ' + name);
  return result;
}

export function parseKubeletNodeResourceSample(summaryText: string, nodeText: string): NodeHostSample {
  let summary: unknown;
  let node: unknown;
  try {
    summary = JSON.parse(summaryText);
    node = JSON.parse(nodeText);
  } catch {
    throw new Error('kubelet node resource response is not valid JSON');
  }
  const nodeStats = objectPath(summary, 'node', 'node');
  const cpu = objectPath(nodeStats, 'cpu', 'node.cpu');
  const memory = objectPath(nodeStats, 'memory', 'node.memory');
  const capacity = objectPath(node, 'status.capacity', 'node.status.capacity');
  const network = isRecord(nodeStats.network) ? nodeStats.network : {};
  const diskUnavailable = 'node_disk_io_not_exposed_by_kubelet_stats_summary';
  const rxAvailable = network.rxBytes !== null && network.rxBytes !== undefined;
  const txAvailable = network.txBytes !== null && network.txBytes !== undefined;
  return {
    cpu_usage_core_nanoseconds: numberField(cpu.usageCoreNanoSeconds, 'node.cpu.usageCoreNanoSeconds'),
    memory_working_set_bytes: numberField(memory.workingSetBytes, 'node.memory.workingSetBytes'),
    cpu_capacity_millicores: parseQuantity(capacity.cpu, 'cpu', 'node.status.capacity.cpu'),
    memory_capacity_bytes: parseQuantity(capacity.memory, 'memory', 'node.status.capacity.memory'),
    network_rx_bytes: rxAvailable ? numberField(network.rxBytes, 'node.network.rxBytes') : null,
    network_tx_bytes: txAvailable ? numberField(network.txBytes, 'node.network.txBytes') : null,
    disk_read_bytes: null,
    disk_write_bytes: null,
    network_rx_unavailable_reason: rxAvailable ? null : 'node_network_rx_counter_unavailable',
    network_tx_unavailable_reason: txAvailable ? null : 'node_network_tx_counter_unavailable',
    disk_unavailable_reason: diskUnavailable,
  };
}

export function createKubeletNodeResourceReader(get: ReadOnlyKubernetesGet): ObservationNodeExecutor {
  return async (nodeName, signal) => {
    if (!/^[A-Za-z0-9.-]+$/.test(nodeName)) throw new Error('invalid Kubernetes node name');
    const encoded = encodeURIComponent(nodeName);
    const [summary, node] = await Promise.all([
      get('/api/v1/nodes/' + encoded + '/proxy/stats/summary', signal),
      get('/api/v1/nodes/' + encoded, signal),
    ]);
    return parseKubeletNodeResourceSample(summary, node);
  };
}

type TimedSample = PodResourceSample & { round: number; observed_at_unix_ms: number };
type TimedNodeSample = NodeHostSample & { round: number; observed_at_unix_ms: number };
type RateSample = { round: number; cpu_millicores: number; elapsed_ms: number };
type PodDetails = {
  target: ObservationTarget;
  samples: TimedSample[];
  rates: RateSample[];
  read_attempt_count: number;
  read_failure_count: number;
  unavailable_reason: string | null;
  summary: ObservationPodSummary;
};

function validateOptions(options: StartNodeObservationOptions): void {
  if (!Number.isSafeInteger(options.measurement_start_unix_ms) || options.measurement_start_unix_ms < 0
      || !Number.isSafeInteger(options.measurement_end_unix_ms) || options.measurement_end_unix_ms <= options.measurement_start_unix_ms) {
    throw new Error('invalid node observation measurement window');
  }
  const interval = options.sample_interval_ms ?? 10_000;
  const timeout = options.sample_timeout_ms ?? 5_000;
  if (!Number.isSafeInteger(interval) || interval < 100 || interval > 60_000) throw new Error('sample_interval_ms must be 100..60000');
  if (!Number.isSafeInteger(timeout) || timeout < 100 || timeout > 60_000) throw new Error('sample_timeout_ms must be 100..60000');
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

function failureReason(error: unknown, fallback: string): string {
  return error instanceof Error && /^[a-z0-9_]+$/.test(error.message) ? error.message : fallback;
}

function coverage(samples: Array<{ observed_at_unix_ms: number }>, windowMs: number) {
  const start = samples[0]?.observed_at_unix_ms ?? null;
  const end = samples.at(-1)?.observed_at_unix_ms ?? null;
  const span = start === null || end === null ? 0 : Math.max(0, end - start);
  return {
    coverage_start_unix_ms: start,
    coverage_end_unix_ms: end,
    observed_span_ms: span,
    measurement_window_ms: windowMs,
    coverage_ratio: windowMs > 0 ? Math.min(1, span / windowMs) : 0,
  };
}

function deltaFor(samples: TimedSample[], field: keyof Omit<PodResourceSample, 'counters_available'>, available: boolean, resetReason: string) {
  if (!available) return { value: null, reason: 'counter_unavailable' };
  if (samples.length < 2) return { value: null, reason: samples.length ? 'insufficient_counter_samples' : 'no_resource_samples' };
  for (let index = 1; index < samples.length; index++) {
    if (samples[index]![field] < samples[index - 1]![field]) return { value: null, reason: resetReason };
  }
  return { value: samples.at(-1)![field] - samples[0]![field], reason: null };
}

function cpuSummary(samples: TimedSample[]) {
  if (!samples.length) return { mean: null, peak: null, reason: 'no_resource_samples', rates: [] as RateSample[] };
  if (samples.some(sample => !sample.counters_available.cpu)) {
    return { mean: null, peak: null, reason: 'counter_unavailable', rates: [] as RateSample[] };
  }
  if (samples.length < 2) return { mean: null, peak: null, reason: 'insufficient_counter_samples', rates: [] as RateSample[] };
  const rates: RateSample[] = [];
  let cpuDelta = 0;
  let elapsedTotal = 0;
  for (let index = 1; index < samples.length; index++) {
    const previous = samples[index - 1]!;
    const current = samples[index]!;
    const elapsed = current.observed_at_unix_ms - previous.observed_at_unix_ms;
    if (current.cpu_usage_usec < previous.cpu_usage_usec) {
      return { mean: null, peak: null, reason: 'cpu_counter_reset', rates: [] as RateSample[] };
    }
    if (elapsed <= 0) return { mean: null, peak: null, reason: 'non_positive_sample_interval', rates: [] as RateSample[] };
    const delta = current.cpu_usage_usec - previous.cpu_usage_usec;
    const rate = delta / elapsed;
    cpuDelta += delta;
    elapsedTotal += elapsed;
    rates.push({ round: current.round, cpu_millicores: rate, elapsed_ms: elapsed });
  }
  return { mean: cpuDelta / elapsedTotal, peak: Math.max(...rates.map(rate => rate.cpu_millicores)), reason: null, rates };
}

function summarizePod(
  target: ObservationTarget,
  samples: TimedSample[],
  readAttemptCount: number,
  readFailureCount: number,
  failureReasons: Set<string>,
  windowMs: number,
): PodDetails {
  const cpu = cpuSummary(samples);
  const availability = samples.length
    ? samples.every(sample => Object.values(sample.counters_available).every(value => value))
    : false;
  const memoryAvailable = samples.length > 0 && samples.every(sample => sample.counters_available.memory);
  const rssAvailable = samples.length > 0 && samples.every(sample => sample.counters_available.rss);
  const networkAvailable = samples.length > 0 && samples.every(sample => sample.counters_available.network);
  const diskAvailable = samples.length > 0 && samples.every(sample => sample.counters_available.disk);
  const rx = deltaFor(samples, 'network_rx_bytes', networkAvailable, 'network_rx_counter_reset');
  const tx = deltaFor(samples, 'network_tx_bytes', networkAvailable, 'network_tx_counter_reset');
  const read = deltaFor(samples, 'disk_read_bytes', diskAvailable, 'disk_read_counter_reset');
  const write = deltaFor(samples, 'disk_write_bytes', diskAvailable, 'disk_write_counter_reset');
  const observed = coverage(samples, windowMs);
  const unavailableReason = samples.length === 0
    ? [...failureReasons].sort()[0] ?? 'no_readable_resource_counters'
    : null;
  return {
    target,
    samples,
    rates: cpu.rates,
    read_attempt_count: readAttemptCount,
    read_failure_count: readFailureCount,
    unavailable_reason: unavailableReason,
    summary: {
      ...target,
      sample_count: samples.length,
      read_attempt_count: readAttemptCount,
      read_failure_count: readFailureCount,
      ...observed,
      cpu_millicores_mean: cpu.mean,
      cpu_millicores_peak: cpu.peak,
      cpu_unavailable_reason: cpu.reason,
      cgroup_memory_current_bytes_peak: memoryAvailable ? Math.max(...samples.map(sample => sample.cgroup_memory_current_bytes)) : null,
      memory_unavailable_reason: memoryAvailable ? null : samples.length ? 'counter_unavailable' : 'no_resource_samples',
      process_rss_sampled_max_bytes: rssAvailable ? Math.max(...samples.map(sample => sample.process_rss_bytes)) : null,
      process_rss_high_water_bytes: rssAvailable ? Math.max(...samples.map(sample => sample.process_rss_hwm_bytes)) : null,
      rss_unavailable_reason: rssAvailable ? null : samples.length ? 'counter_unavailable' : 'no_resource_samples',
      network_rx_bytes_delta: rx.value,
      network_rx_unavailable_reason: rx.reason,
      network_tx_bytes_delta: tx.value,
      network_tx_unavailable_reason: tx.reason,
      disk_read_bytes_delta: read.value,
      disk_read_unavailable_reason: read.reason,
      disk_write_bytes_delta: write.value,
      disk_write_unavailable_reason: write.reason,
      unavailable_reason: unavailableReason ?? (availability ? null : 'one_or_more_counters_unavailable'),
    },
  };
}

function summarizeNodeRole(details: PodDetails[], windowMs: number): ObservationNodeSummary {
  const expectedPods = details.length;
  const valuesByRound = new Map<number, {
    cpu: number;
    cpuPods: number;
    cpuElapsed: number;
    memory: number;
    rss: number;
    pods: number;
  }>();
  for (const pod of details) {
    for (const sample of pod.samples) {
      if (!sample.counters_available.memory || !sample.counters_available.rss) continue;
      const values = valuesByRound.get(sample.round) ?? { cpu: 0, cpuPods: 0, cpuElapsed: 0, memory: 0, rss: 0, pods: 0 };
      values.memory += sample.cgroup_memory_current_bytes;
      values.rss += sample.process_rss_bytes;
      values.pods++;
      valuesByRound.set(sample.round, values);
    }
    for (const rate of pod.rates) {
      const values = valuesByRound.get(rate.round) ?? { cpu: 0, cpuPods: 0, cpuElapsed: 0, memory: 0, rss: 0, pods: 0 };
      values.cpu += rate.cpu_millicores;
      values.cpuPods++;
      values.cpuElapsed += rate.elapsed_ms;
      valuesByRound.set(rate.round, values);
    }
  }
  const completeRounds = [...valuesByRound.values()].filter(values => values.pods === expectedPods);
  const completeCpuRounds = [...valuesByRound.values()].filter(values => values.cpuPods === expectedPods);
  const sumComplete = (field: keyof ObservationPodSummary) => {
    const values = details.map(detail => detail.summary[field]);
    return values.every((value): value is number => typeof value === 'number')
      ? values.reduce((sum, value) => sum + value, 0)
      : null;
  };
  const starts = details.map(detail => detail.summary.coverage_start_unix_ms).filter((value): value is number => value !== null);
  const ends = details.map(detail => detail.summary.coverage_end_unix_ms).filter((value): value is number => value !== null);
  const start = starts.length ? Math.min(...starts) : null;
  const end = ends.length ? Math.max(...ends) : null;
  const span = start === null || end === null ? 0 : Math.max(0, end - start);
  const reason = (field: keyof ObservationPodSummary) => {
    const detail = details.find(item => item.summary[field] !== null);
    return detail ? String(detail.summary[field]) : null;
  };
  const highWater = details.map(detail => detail.summary.process_rss_high_water_bytes);
  return {
    node_name: details[0]!.target.node_name,
    role: details[0]!.target.role,
    pod_uids: details.map(detail => detail.target.pod_uid).sort(),
    sample_count: details.reduce((sum, detail) => sum + detail.samples.length, 0),
    coverage_start_unix_ms: start,
    coverage_end_unix_ms: end,
    observed_span_ms: span,
    measurement_window_ms: windowMs,
    coverage_ratio: Math.min(1, span / windowMs),
    cpu_millicores_mean: completeCpuRounds.length
      ? completeCpuRounds.reduce((sum, value) => sum + value.cpu * value.cpuElapsed / value.cpuPods, 0)
        / completeCpuRounds.reduce((sum, value) => sum + value.cpuElapsed / value.cpuPods, 0) : null,
    cpu_millicores_peak: completeCpuRounds.length ? Math.max(...completeCpuRounds.map(value => value.cpu)) : null,
    cpu_unavailable_reason: completeCpuRounds.length ? null : details[0]!.summary.cpu_unavailable_reason,
    cgroup_memory_current_bytes_peak: completeRounds.length ? Math.max(...completeRounds.map(values => values.memory)) : null,
    process_rss_sampled_max_bytes: completeRounds.length ? Math.max(...completeRounds.map(values => values.rss)) : null,
    process_rss_high_water_sum_bytes: highWater.every((value): value is number => value !== null)
      ? highWater.reduce((sum, value) => sum + value, 0) : null,
    network_rx_bytes_delta: sumComplete('network_rx_bytes_delta'),
    network_rx_unavailable_reason: reason('network_rx_unavailable_reason'),
    network_tx_bytes_delta: sumComplete('network_tx_bytes_delta'),
    network_tx_unavailable_reason: reason('network_tx_unavailable_reason'),
    disk_read_bytes_delta: sumComplete('disk_read_bytes_delta'),
    disk_read_unavailable_reason: reason('disk_read_unavailable_reason'),
    disk_write_bytes_delta: sumComplete('disk_write_bytes_delta'),
    disk_write_unavailable_reason: reason('disk_write_unavailable_reason'),
  };
}

function counterDelta<T extends { observed_at_unix_ms: number }>(
  samples: T[],
  read: (sample: T) => number | null,
  resetReason: string,
  unavailableReason: string,
) {
  if (!samples.length) return { value: null, reason: unavailableReason };
  if (samples.some(sample => read(sample) === null)) return { value: null, reason: unavailableReason };
  if (samples.length < 2) return { value: null, reason: 'insufficient_counter_samples' };
  const first = read(samples[0]!);
  const last = read(samples.at(-1)!);
  if (first === null || last === null) return { value: null, reason: unavailableReason };
  let previous = first;
  for (let index = 1; index < samples.length; index++) {
    const current = read(samples[index]!);
    if (current === null) return { value: null, reason: unavailableReason };
    if (current < previous) return { value: null, reason: resetReason };
    previous = current;
  }
  return { value: last - first, reason: null };
}

function summarizeNodeTotal(
  nodeName: string,
  samples: TimedNodeSample[],
  attempts: number,
  failures: number,
  reasons: Set<string>,
  windowMs: number,
): ObservationNodeTotalSummary {
  const observed = coverage(samples, windowMs);
  const noSampleReason = [...reasons].sort()[0] ?? 'node_stats_source_not_configured';
  const cpuRates: number[] = [];
  let cpuDelta = 0;
  let cpuElapsed = 0;
  let cpuReason: string | null = null;
  if (!samples.length) cpuReason = noSampleReason;
  else if (samples.length < 2) cpuReason = 'insufficient_counter_samples';
  else {
    for (let index = 1; index < samples.length; index++) {
      const previous = samples[index - 1]!;
      const current = samples[index]!;
      const elapsed = current.observed_at_unix_ms - previous.observed_at_unix_ms;
      if (current.cpu_usage_core_nanoseconds < previous.cpu_usage_core_nanoseconds) {
        cpuReason = 'node_cpu_counter_reset';
        break;
      }
      if (elapsed <= 0) {
        cpuReason = 'non_positive_sample_interval';
        break;
      }
      const delta = current.cpu_usage_core_nanoseconds - previous.cpu_usage_core_nanoseconds;
      cpuDelta += delta;
      cpuElapsed += elapsed;
      cpuRates.push(delta / (elapsed * 1000));
    }
  }
  const rxUnavailable = samples.length
    ? samples.find(sample => sample.network_rx_unavailable_reason)?.network_rx_unavailable_reason
      ?? 'node_network_counters_unavailable'
    : noSampleReason;
  const txUnavailable = samples.length
    ? samples.find(sample => sample.network_tx_unavailable_reason)?.network_tx_unavailable_reason
      ?? 'node_network_counters_unavailable'
    : noSampleReason;
  const diskUnavailable = samples.length
    ? samples.find(sample => sample.disk_unavailable_reason)?.disk_unavailable_reason
      ?? 'node_disk_io_not_exposed_by_kubelet_stats_summary'
    : noSampleReason;
  const rx = counterDelta(samples, sample => sample.network_rx_bytes, 'node_network_rx_counter_reset', rxUnavailable);
  const tx = counterDelta(samples, sample => sample.network_tx_bytes, 'node_network_tx_counter_reset', txUnavailable);
  const read = counterDelta(samples, sample => sample.disk_read_bytes, 'node_disk_read_counter_reset', diskUnavailable);
  const write = counterDelta(samples, sample => sample.disk_write_bytes, 'node_disk_write_counter_reset', diskUnavailable);
  const capacity = samples[0];
  const memoryAvailable = samples.length > 0;
  const memoryPeak = memoryAvailable ? Math.max(...samples.map(sample => sample.memory_working_set_bytes)) : null;
  const cpuMean = cpuReason === null && cpuElapsed > 0 ? cpuDelta / (cpuElapsed * 1000) : null;
  const cpuPeak = cpuReason === null && cpuRates.length ? Math.max(...cpuRates) : null;
  return {
    node_name: nodeName,
    sample_count: samples.length,
    read_attempt_count: attempts,
    read_failure_count: failures,
    ...observed,
    cpu_millicores_mean: cpuMean,
    cpu_millicores_peak: cpuPeak,
    cpu_capacity_millicores: capacity?.cpu_capacity_millicores ?? null,
    cpu_peak_utilization_percent: cpuPeak !== null && capacity?.cpu_capacity_millicores
      ? cpuPeak / capacity.cpu_capacity_millicores * 100 : null,
    cpu_unavailable_reason: cpuReason,
    capacity_unavailable_reason: capacity ? null : noSampleReason,
    memory_working_set_bytes_peak: memoryPeak,
    memory_capacity_bytes: capacity?.memory_capacity_bytes ?? null,
    memory_peak_utilization_percent: memoryPeak !== null && capacity?.memory_capacity_bytes
      ? memoryPeak / capacity.memory_capacity_bytes * 100 : null,
    memory_unavailable_reason: memoryAvailable ? null : noSampleReason,
    network_rx_bytes_delta: rx.value,
    network_rx_unavailable_reason: rx.reason,
    network_tx_bytes_delta: tx.value,
    network_tx_unavailable_reason: tx.reason,
    disk_read_bytes_delta: read.value,
    disk_read_unavailable_reason: read.reason,
    disk_write_bytes_delta: write.value,
    disk_write_unavailable_reason: write.reason,
    unavailable_reason: samples.length ? null : noSampleReason,
  };
}

function awaitAbortable<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(new Error('observation_aborted'));
  return new Promise((resolve, reject) => {
    const abort = () => reject(new Error('observation_aborted'));
    signal.addEventListener('abort', abort, { once: true });
    operation.then(
      value => { signal.removeEventListener('abort', abort); resolve(value); },
      error => { signal.removeEventListener('abort', abort); reject(error); },
    );
  });
}

export function startNodeObservation(options: StartNodeObservationOptions): NodeObservationHandle {
  validateOptions(options);
  const clock = options.clock ?? defaultClock;
  const sampleInterval = options.sample_interval_ms ?? 10_000;
  const sampleTimeout = options.sample_timeout_ms ?? 5_000;
  const windowMs = options.measurement_end_unix_ms - options.measurement_start_unix_ms;
  const samplesByUid = new Map(options.targets.map(target => [target.pod_uid, [] as TimedSample[]]));
  const attemptsByUid = new Map(options.targets.map(target => [target.pod_uid, 0]));
  const failuresByUid = new Map(options.targets.map(target => [target.pod_uid, 0]));
  const reasonsByUid = new Map(options.targets.map(target => [target.pod_uid, new Set<string>()]));
  const nodeNames = [...new Set(options.targets.map(target => target.node_name))].sort();
  const nodeSamples = new Map(nodeNames.map(node => [node, [] as TimedNodeSample[]]));
  const nodeAttempts = new Map(nodeNames.map(node => [node, 0]));
  const nodeFailures = new Map(nodeNames.map(node => [node, 0]));
  const nodeReasons = new Map(nodeNames.map(node => [node, new Set<string>()]));
  const stopController = new AbortController();
  const activeControllers = new Set<AbortController>();
  let stopping = false;
  let stopPromise: Promise<NodeResourceObservation> | undefined;

  const runRead = async <T>(read: (signal: AbortSignal) => Promise<T>): Promise<T> => {
    const controller = new AbortController();
    activeControllers.add(controller);
    const abortFromStop = () => controller.abort();
    if (stopController.signal.aborted) controller.abort();
    else stopController.signal.addEventListener('abort', abortFromStop, { once: true });
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; controller.abort(); }, sampleTimeout);
    try {
      return await awaitAbortable(Promise.resolve().then(() => {
        if (controller.signal.aborted) throw new Error('observation_aborted');
        return read(controller.signal);
      }), controller.signal);
    } finally {
      clearTimeout(timer);
      stopController.signal.removeEventListener('abort', abortFromStop);
      activeControllers.delete(controller);
      if (timedOut && !stopping) throw new Error('observation_sample_timeout');
    }
  };

  const loop = (async () => {
    let round = 0;
    while (!stopping) {
      const roundStart = clock.now();
      if (roundStart < options.measurement_start_unix_ms) {
        await clock.sleep(Math.min(sampleInterval, options.measurement_start_unix_ms - roundStart), stopController.signal);
        continue;
      }
      if (roundStart >= options.measurement_end_unix_ms) break;
      const currentRound = round++;
      const readTasks = options.targets.map(async target => {
        attemptsByUid.set(target.pod_uid, attemptsByUid.get(target.pod_uid)! + 1);
        try {
          const output = await runRead(signal => options.execute(target, POD_RESOURCE_SAMPLE_COMMAND, signal));
          const sample = parsePodResourceSample(output);
          const observedAt = clock.now();
          if (observedAt >= options.measurement_end_unix_ms) {
            reasonsByUid.get(target.pod_uid)!.add('sample_completed_after_measurement_end');
            failuresByUid.set(target.pod_uid, failuresByUid.get(target.pod_uid)! + 1);
            return;
          }
          samplesByUid.get(target.pod_uid)!.push({ ...sample, round: currentRound, observed_at_unix_ms: observedAt });
        } catch (error) {
          failuresByUid.set(target.pod_uid, failuresByUid.get(target.pod_uid)! + 1);
          reasonsByUid.get(target.pod_uid)!.add(stopping ? 'observation_stopped_during_sample'
            : error instanceof Error && error.message === 'observation_sample_timeout'
              ? 'observation_sample_timeout' : failureReason(error, 'pod_resource_sample_unavailable'));
        }
      });
      if (options.read_node) {
        readTasks.push(...nodeNames.map(async nodeName => {
          nodeAttempts.set(nodeName, nodeAttempts.get(nodeName)! + 1);
          try {
            const sample = await runRead(signal => options.read_node!(nodeName, signal));
            const observedAt = clock.now();
            if (observedAt >= options.measurement_end_unix_ms) {
              nodeReasons.get(nodeName)!.add('sample_completed_after_measurement_end');
              nodeFailures.set(nodeName, nodeFailures.get(nodeName)! + 1);
              return;
            }
            nodeSamples.get(nodeName)!.push({ ...sample, round: currentRound, observed_at_unix_ms: observedAt });
          } catch (error) {
            nodeFailures.set(nodeName, nodeFailures.get(nodeName)! + 1);
            nodeReasons.get(nodeName)!.add(stopping ? 'observation_stopped_during_sample'
              : error instanceof Error && error.message === 'observation_sample_timeout'
                ? 'observation_sample_timeout' : failureReason(error, 'node_stats_source_unavailable'));
          }
        }));
      }
      await Promise.all(readTasks);
      if (stopping) break;
      const afterRound = clock.now();
      if (afterRound >= options.measurement_end_unix_ms) break;
      const delay = Math.min(sampleInterval - (afterRound - roundStart), options.measurement_end_unix_ms - afterRound);
      if (delay > 0) await clock.sleep(delay, stopController.signal);
    }
  })();

  return {
    stop() {
      if (stopPromise) return stopPromise;
      stopping = true;
      stopController.abort();
      for (const controller of activeControllers) controller.abort();
      stopPromise = loop.then(() => {
        const groups = new Map<string, PodDetails[]>();
        const pods: ObservationPodSummary[] = [];
        for (const target of options.targets) {
          const details = summarizePod(
            target,
            samplesByUid.get(target.pod_uid)!,
            attemptsByUid.get(target.pod_uid)!,
            failuresByUid.get(target.pod_uid)!,
            reasonsByUid.get(target.pod_uid)!,
            windowMs,
          );
          pods.push(details.summary);
          const key = target.node_name + '\0' + target.role;
          const group = groups.get(key) ?? [];
          group.push(details);
          groups.set(key, group);
        }
        const nodes = [...groups.values()]
          .map(details => summarizeNodeRole(details, windowMs))
          .sort((left, right) => left.node_name.localeCompare(right.node_name) || left.role.localeCompare(right.role));
        const nodeTotals = nodeNames.map(nodeName => summarizeNodeTotal(
          nodeName,
          nodeSamples.get(nodeName)!,
          nodeAttempts.get(nodeName)!,
          nodeFailures.get(nodeName)!,
          nodeReasons.get(nodeName)!,
          windowMs,
        ));
        const stoppedAt = clock.now();
        return {
          schema_version: 2,
          source: 'pod_procfs_cgroup_v2',
          scope: 'target_pods_aggregated_by_node_and_role',
          node_total_source: options.read_node ? 'kubelet_stats_summary' : 'unavailable',
          node_total_unavailable_reason: !options.read_node
            ? 'node_stats_source_not_configured'
            : nodeTotals.every(node => node.sample_count === 0) ? 'node_stats_source_unavailable'
              : nodeTotals.some(node => node.sample_count === 0) ? 'partial_node_stats_unavailable' : null,
          sample_interval_ms: sampleInterval,
          sample_timeout_ms: sampleTimeout,
          measurement_start_unix_ms: options.measurement_start_unix_ms,
          measurement_end_unix_ms: options.measurement_end_unix_ms,
          nodes,
          node_totals: nodeTotals,
          pods,
          unavailable_reason: pods.every(pod => pod.sample_count === 0)
            ? 'no_readable_pod_resource_counters'
            : stoppedAt < options.measurement_end_unix_ms ? 'observation_stopped_before_window_end' : null,
        };
      });
      return stopPromise;
    },
  };
}
