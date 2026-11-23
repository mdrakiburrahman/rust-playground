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
  const stopping = single('[oat] diagnostic stop memory: ');
  const guardStopPrefix = '[oat] diagnostic guard stop memory: ';
  const guardStop = single(guardStopPrefix);
  const snapshots = [stopping,sample?.cgroup];
  if(log.split('\n').some(l=>l.startsWith(guardStopPrefix))) snapshots.push(guardStop);
  const uint = (n:unknown) => Number.isSafeInteger(n) && Number(n)>=0;
  const identity = (x:any) => x && typeof x.collector_id==='string' && /^[0-9a-f]{64}$/.test(x.collector_id) && x.ps_all_ids?.length===1 && x.ps_all_ids[0]===x.collector_id && x.docker?.id===x.collector_id && (id===undefined || id===x.collector_id);
  const oom = snapshots.some(x=>identity(x) && (x.docker.oom_killed===true || (uint(x.events?.oom_kill) && x.events.oom_kill>0)));
  const max = snapshots.some(x=>identity(x) && uint(x.events?.max) && x.events.max>0);
  const complete = snapshots.every(x=>identity(x) && x.collector_id===id && uint(x.observed_at_unix_ms) && !x.read_error && x.docker.oom_killed===false && x.docker.status==='running' && x.docker.exit_code===0 && x.docker.restart_count===0 && x.limit_bytes===COLLECTOR_MEMORY && uint(x.peak_bytes) && x.peak_bytes>0 && [x.events?.max,x.events?.oom,x.events?.oom_kill,x.stat?.anon,x.stat?.file].every(uint));
  const peak = sample?.cgroup?.peak_bytes;
  const peakValid = typeof id === 'string' && /^[a-f0-9]{64}$/.test(id) && sample?.cgroup?.collector_id === id
    && sample.cgroup.limit_bytes === COLLECTOR_MEMORY && Number.isSafeInteger(peak) && peak > 0;
  const stop = sample?.stop;
  const memoryStop = sample?.guard_status === 'triggered' && stop?.reason === 'hard_cap_fraction'
    && ['container_memory_bytes', 'collector_rss_bytes', 'collector_cgroup_memory_bytes', 'collector_buffered_bytes'].includes(stop.metric);
  const engaged = memoryStop ? true
    : sample?.guard_status === 'complete' && sample.memory_signal==='cgroup_memory' && sample.guard_cap === COLLECTOR_MEMORY && sample.stop === null ? false : null;
  const reasons = [];
  if (!complete) reasons.push('incomplete or unstable collector stop/terminal memory evidence');
  if (oom) reasons.push('collector OOM kill observed');
  if (max) reasons.push('collector memory.max events observed');
  if (!peakValid) reasons.push(sample?.read_error ?? 'collector cgroup peak unavailable or identity/cap mismatch');
  if (engaged === null) reasons.push('complete overload guard evidence unavailable');
  if (engaged === true) reasons.push('overload guard engaged');
  return { collector_memory_limit_bytes: COLLECTOR_MEMORY, collector_cgroup_lifetime_peak_bytes: peakValid ? peak : null,
    guard_scope: 'OAT overload protection: container memory, cgroup memory (not process RSS), and buffered bytes',
    guard_status: sample?.guard_status ?? null, guard_engaged: engaged, guard_stop: sample?.stop ?? null, memory_limited: oom || engaged===true ? true : complete && engaged===false ? false : null,
    stop_snapshot: stopping, terminal_snapshot: sample?.cgroup ?? null, guard_stop_snapshot: guardStop,
    permits_cpu_attribution: reasons.length === 0, reasons, kustainer: admission?.kustainer ?? null };
}
