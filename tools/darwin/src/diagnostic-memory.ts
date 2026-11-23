import { COLLECTOR_MEMORY } from './realistic-diagnostic-config.js';

export function fitsDiagnosticBound(bound: number): boolean {
  return Number.isSafeInteger(bound) && COLLECTOR_MEMORY <= bound - Math.ceil(bound / 20);
}

export function memoryOutcome(log: string) {
  function single(prefix: string) {
    const lines = log.split('\n').filter(s => s.startsWith(prefix));
    if (lines.length === 1) { try { return JSON.parse(lines[0]!.slice(prefix.length)); } catch { /* unavailable */ } }
    return null;
  }
  const sample = single('[oat] diagnostic terminal memory: ');
  const admission = single('[oat] diagnostic pre-schedule admission: ');
  const id = admission?.cpu_isolation?.collector?.container_id;
  const peak = sample?.cgroup?.peak_bytes;
  const peakValid = typeof id === 'string' && /^[a-f0-9]{64}$/.test(id) && sample?.cgroup?.collector_id === id
    && sample.cgroup.limit_bytes === COLLECTOR_MEMORY && Number.isSafeInteger(peak) && peak > 0;
  const stop = sample?.stop;
  const memoryStop = sample?.guard_status === 'triggered' && stop?.reason === 'hard_cap_fraction'
    && ['container_memory_bytes', 'collector_rss_bytes', 'collector_buffered_bytes'].includes(stop.metric);
  const engaged = memoryStop ? true
    : sample?.guard_status === 'complete' && sample.guard_cap === COLLECTOR_MEMORY && sample.stop === null ? false : null;
  const reasons = [];
  if (!peakValid) reasons.push(sample?.read_error ?? 'collector cgroup peak unavailable or identity/cap mismatch');
  if (engaged === null) reasons.push('complete overload guard evidence unavailable');
  if (engaged === true) reasons.push('overload guard engaged');
  return { collector_memory_limit_bytes: COLLECTOR_MEMORY, collector_cgroup_lifetime_peak_bytes: peakValid ? peak : null,
    guard_scope: 'OAT overload protection: container memory, collector RSS and buffered bytes',
    guard_status: sample?.guard_status ?? null, guard_engaged: engaged, guard_stop: sample?.stop ?? null, memory_limited: engaged,
    permits_cpu_attribution: reasons.length === 0, reasons, kustainer: admission?.kustainer ?? null };
}
