import {finite, physicalStages, stages, states, type Level, type Values} from './metrics.ts';
import type {Linux, System} from './linux.ts';

export type Header = {schema: 'collector-isolation/v1'; runId: string; level: Level; declaredCores?: number; startMs: number; endMs: number; intervalMs: number; system: System};
export type Obs<T> = {beginMs: number; endMs: number; value: T | null; error?: string};
export type Isolation = {ingress: Record<string,unknown> | null; physical: Record<string,unknown> | null};
export type SamplerCost = {atMs:number; cpuUs:number; rssBytes:number; promBodyBytes:number|null; isolationBodyBytes:number|null};
export type Sample = {sampler?:SamplerCost; isolation?: Obs<Isolation>; targetMs: number; wallClockOffsetMs: number; prom: Obs<Values>; linux: Obs<Linux>};
type Point = {ms: number; v: Values};
const midpoint = <T>(o: Obs<T>) => (o.beginMs + o.endMs) / 2;
const points = <T>(obs: Obs<T>[], select: (x: T) => Values): Point[] => obs.map(o => ({ms: midpoint(o), v: o.value && select(o.value) || {}}));
const seconds = (p: Point[]) => (p[p.length - 1].ms - p[0].ms) / 1000;
export function delta(p: Point[], key: string): number | null {
  if (p.length < 2) return null;
  let previous = -1;
  for (const {v} of p) {
    const n = finite(v[key]);
    if (n === null || n < previous) return null;
    previous = n;
  }
  return previous - p[0].v[key]!;
}
function scale(n: number | null, multiplier: number): number | null { return n === null ? null : finite(n * multiplier); }
function rate(p: Point[], key: string, multiplier = 1): number | null { return scale(delta(p, key), multiplier / seconds(p)); }
export function gauge(p: Point[], key: string) {
  let area = 0, peak = 0;
  for (let i = 0; i < p.length; i++) {
    const n = finite(p[i].v[key]);
    if (n === null) return null;
    peak = Math.max(peak, n);
    if (i) area += (p[i].ms - p[i-1].ms) * (n + p[i-1].v[key]!) / 2;
  }
  return {timeWeightedMean: area / (seconds(p) * 1000), sampledPeak: peak, first: p[0].v[key], last: p[p.length - 1].v[key]};
}
function sourceWindow<T>(obs: Obs<T>[], h: Header) {
  const first = obs[0], last = obs[obs.length - 1];
  const edgeToleranceMs = Math.min(500,h.intervalMs/2);
  const boundaryAligned = Math.max(Math.abs(midpoint(first)-h.startMs), Math.abs(midpoint(last)-h.endMs)) <= edgeToleranceMs;
  return {edgeToleranceMs, boundaryAligned, largestMidpointGapMs:Math.max(...obs.slice(1).map((o,i)=>midpoint(o)-midpoint(obs[i]))), firstRequest: [first.beginMs, first.endMs], lastRequest: [last.beginMs, last.endMs],
    midpointElapsedSeconds: (midpoint(last) - midpoint(first)) / 1000, firstMidpointSkewMs: midpoint(first) - h.startMs,
    lastMidpointSkewMs: midpoint(last) - h.endMs, maxReadMs: Math.max(...obs.map(o => o.endMs - o.beginMs)),
    unavailableSamples: obs.filter(o => !o.value).length};
}
function assertTiming<T>(obs: Obs<T>[]) {
  if (obs.some((o, i) => !Number.isFinite(o.beginMs) || !Number.isFinite(o.endMs) || o.endMs < o.beginMs || (i && o.beginMs < obs[i-1].endMs))) {
    throw Error('Invalid or overlapping sample timestamps');
  }
}
export function reduce(h: Header, samples: Sample[]) {
  if (h.schema !== 'collector-isolation/v1' || !/^L[0-5]$/.test(h.level) || !h.runId || !(h.endMs > h.startMs) ||
      samples.length < 2 || samples[0].targetMs !== h.startMs || samples[samples.length-1].targetMs !== h.endMs ||
      samples.some((s, i) => !Number.isFinite(s.wallClockOffsetMs) || (i && s.targetMs <= samples[i-1].targetMs))) throw Error('Incomplete or invalid collection window');
  if (samples.some(s=>Math.abs(s.wallClockOffsetMs)>Math.min(500,h.intervalMs/2))) throw Error('Collector UTC clock stepped beyond boundary tolerance');
  const prom = samples.map(s => s.prom), os = samples.map(s => s.linux);
  assertTiming(prom); assertTiming(os);
  // A lost/reused PID breaks both subject and collector-counter continuity.
  const first = os[0].value, last = os[os.length-1].value;
  if (!first || !last || os.some(o => o.value?.process.startTicks !== first.process.startTicks)) throw Error('Collector PID missing, restarted, or reused');
  const promWindow = sourceWindow(prom, h), linuxWindow = sourceWindow(os, h);
  const p = points(prom, v => promWindow.boundaryAligned ? v : {}), l = points(os, v => linuxWindow.boundaryAligned ? v.values : {});
  const hz = h.system.tickHz;
  if (!Number.isSafeInteger(hz) || hz <= 0) throw Error('Invalid CLK_TCK');
  const cores = rate(l, 'cpuTicks', 1/hz);
  const tids = linuxWindow.boundaryAligned && first.threads && last.threads ? Object.entries(first.threads).flatMap(([tid, a]) => {
    const b = last.threads![tid];
    return !b || b.startTicks !== a.startTicks || b.cpuTicks < a.cpuTicks ? [] : [{tid, name: b.name, startTicks: a.startTicks, cores: (b.cpuTicks-a.cpuTicks)/hz/seconds(l)}];
  }).sort((a,b) => b.cores - a.cores) : null;
  const unchangedNetns = first.netns !== null && os.every(o => o.value?.netns === first.netns);
  const slotSeconds = delta(p, 'groupSeconds.flush_slot');
  const receiptWaitSeconds = delta(p, 'groupSeconds.receipt_wait');
  const observerPoints = samples.map(s=>({ms:s.sampler?.atMs ?? midpoint(s.linux),v:{cpuUs:s.sampler?.cpuUs??null,rssBytes:s.sampler?.rssBytes??null}}));
  const bodySize = (key: 'promBodyBytes'|'isolationBodyBytes') => samples.some(s=>s.sampler?.[key]==null) ? null : samples.reduce((total,s)=>total+s.sampler![key]!,0);
  const durations = Object.fromEntries(stages.map(stage => {
    const sum = delta(p, `duration.${stage}.sum`), count = delta(p, `duration.${stage}.count`);
    return [stage, {completedObservations: count, completedWallSeconds: sum, meanSeconds: count && sum !== null ? sum/count : null}];
  }));
  return {schema: 'collector-isolation/report-v1' as const, runId: h.runId, level: h.level, declaredCores: h.declaredCores ?? null,
    window: {startUnixMs: h.startMs, endUnixMs: h.endMs, nominalSeconds: (h.endMs-h.startMs)/1000, samples: samples.length,
      maxWallClockOffsetMs: Math.max(...samples.map(s => Math.abs(s.wallClockOffsetMs))), collector: promWindow, linux: linuxWindow},
    observer: {scope:'sampling Node process CPU only; server-side scrape CPU is not separable', elapsedSeconds:seconds(observerPoints),
      processCpuSeconds:scale(delta(observerPoints,'cpuUs'),1e-6), meanCores:rate(observerPoints,'cpuUs',1e-6), rssBytes:gauge(observerPoints,'rssBytes'),
      requestedPeriodMs:h.intervalMs, skippedInteriorTicks:Math.ceil((h.endMs-h.startMs)/h.intervalMs)+1-samples.length,
      prometheusDecodedBodyBytes: bodySize('promBodyBytes'), isolationDecodedBodyBytes:h.level==='L5'?null:bodySize('isolationBodyBytes')},
    collector: {receivedLogical: {rowsPerSecond: rate(p, 'received_rows_total'), bytesPerSecond: rate(p, 'received_bytes_total')},
      successfulPublicationLogical: {rowsPerSecond: rate(p, 'delta_rows_total'), bytesPerSecond: rate(p, 'delta_bytes_total')},
      publishedFilePhysical: {rowsPerSecond: rate(p, 'parquet_file_rows_sum'), bytesPerSecond: rate(p, 'parquet_file_physical_bytes_sum')},
      stageDurations: durations, completedCommitBusyFraction: rate(p,'duration.commit.sum'),
      errors: Object.fromEntries(['delta_failures_total','downstream_nacks_total','dfe_enqueue_failures_total'].map(k => [k,delta(p,k)])),
      gauges: Object.fromEntries(['flight_active_streams','dfe_buffered_rows','dfe_buffered_bytes','delta_pending_commits',
        ...['rows','bytes','publications','oldest_age_seconds'].map(k=>`backlog.${k}`),
        ...['global','table'].flatMap(s=>['bytes','rows','batches'].flatMap(r=>['reserved','waiters'].map(k=>`${k}.${s}.${r}`))),
        ...states.map(k=>`groups.${k}`)].map(k=>[k,gauge(p,k)])),
      capacityAcquisitions: Object.fromEntries(['global','table'].flatMap(scope=>['bytes','rows','batches'].map(resource=>{
        const suffix=scope+'.'+resource;
        const elapsed=delta(p,'acquire_seconds_total.'+suffix), attempts=delta(p,'acquire_attempts_total.'+suffix);
        return [suffix,{completedAcquireSeconds:elapsed,completedAttempts:attempts,acquireSecondsPerElapsedSecond:rate(p,'acquire_seconds_total.'+suffix),
          attemptsPerSecond:rate(p,'acquire_attempts_total.'+suffix),meanAcquireSeconds:attempts && elapsed!==null ? elapsed/attempts : null}];
      }))),
      flush: {meanOccupiedSlots:gauge(p,'groups.flush_slot')?.timeWeightedMean ?? null, completedSlotGroupSeconds:slotSeconds,
        completedReceiptWaitGroupSeconds:receiptWaitSeconds, receiptWaitFractionOfCompletedSlotTime:slotSeconds && receiptWaitSeconds!==null ? receiptWaitSeconds/slotSeconds : null,
        scope:'Completed group-seconds, not an instantaneous utilization percentage; completion windows can cross either edge.'},
      completedGroupSecondsPerSecond: Object.fromEntries(states.map(s=>[s,rate(p,`groupSeconds.${s}`)]))},
    linux: {scope: {pid: h.system.pid, startTicks: first.process.startTicks, comm: first.process.name, bootId: h.system.bootId, clockTicksPerSecond: hz, mainTaskAllowedCpuList:os.every(o=>o.value?.cpusAllowed===first.cpusAllowed) ? first.cpusAllowed??null : null,
      networkNamespace: unchangedNetns ? first.netns : null, disks: 'visible kernel block-device counters; not process-attributed'},
      processCpuSeconds:scale(delta(l,'cpuTicks'),1/hz), processCores: cores,
      scheduler: {scope:'CPU and faults: whole process, all threads, no children. Context switches: main task only.',
        userCores: rate(l,'userTicks',1/hz), systemCores: rate(l,'systemTicks',1/hz),
        systemFraction: (() => { const u = delta(l,'userTicks'), s = delta(l,'systemTicks'); return u === null || s === null || !(u + s) ? null : s / (u + s); })(),
        minorFaultsPerSecond: rate(l,'minorFaults'), majorFaultsPerSecond: rate(l,'majorFaults'),
        voluntarySwitchesPerSecond: rate(l,'voluntarySwitches'), involuntarySwitchesPerSecond: rate(l,'involuntarySwitches')}, rssBytes: gauge(l,'rssBytes'), readStorageBytesPerSecond: rate(l,'io.read_bytes'), writeStorageBytesPerSecond: rate(l,'io.write_bytes'),
      cancelledWriteBytesPerSecond: rate(l,'io.cancelled_write_bytes'),
      cgroup: h.system.cgroup ? {scope:'cgroup v2 containing PID; includes siblings, not process-attributed', path:h.system.cgroup.path,
        cpuThrottledSeconds:scale(delta(l,'cgThrottledUs'),1e-6), throttledPeriods:delta(l,'cgThrottledPeriods'), cpuSomePressureFraction:rate(l,'cgCpuSomeUs',1e-6),
        memoryCurrentBytes:gauge(l,'cgMemoryBytes'), memoryLimitBytes:gauge(l,'cgMemoryLimitBytes'),memoryHighEvents:delta(l,'cgMemoryHigh'),
        oomEvents:delta(l,'cgMemoryOom'),oomKills:delta(l,'cgMemoryOomKills')} : null,
      threads: {scope: 'same TID and start time present at both boundaries; exited/new threads excluded; process includes all threads',
        firstCount: first.threads ? Object.keys(first.threads).length : null, lastCount: last.threads ? Object.keys(last.threads).length : null, matched: tids},
      interfaces: Object.fromEntries(h.system.interfaces.map(iface => [iface, !unchangedNetns ? null : {
        rxBytesPerSecond: rate(l,`net.${iface}.rxBytes`), txBytesPerSecond: rate(l,`net.${iface}.txBytes`),
        rxDrops: delta(l,`net.${iface}.rxDrops`), txDrops: delta(l,`net.${iface}.txDrops`), rxErrors:delta(l,`net.${iface}.rxErrors`), txErrors:delta(l,`net.${iface}.txErrors`)}])),
      disks: Object.fromEntries(h.system.disks.map(d => [d, {readBytesPerSecond: rate(l,`disk.${d}.readSectors`,512), writeBytesPerSecond: rate(l,`disk.${d}.writeSectors`,512),
        readsPerSecond:rate(l,`disk.${d}.reads`), writesPerSecond:rate(l,`disk.${d}.writes`), busyFraction:rate(l,`disk.${d}.busyMs`,.001),
        meanIoQueueDepth:rate(l,`disk.${d}.weightedIoMs`,.001), inFlight:gauge(l,`disk.${d}.inFlight`)}]))},
    littleLaw: littleLaw(p)};
}

export function littleLaw(p: Point[]) {
  const population = gauge(p, 'backlog.bytes');
  const bytes = delta(p,'publication_settled_bytes_total'), byteSeconds = delta(p,'publication_settled_byte_seconds_total');
  const healthy = p.every(s => s.v.publication_backlog_available === 1);
  const reason = !healthy || !population ? 'Incomplete/unavailable publication backlog population'
    : bytes === null || byteSeconds === null ? 'Settlement counters unavailable, invalid, or reset'
    : !bytes || !byteSeconds ? 'No positive settled bytes and residence time' : null;
  const residence = reason ? null : byteSeconds! / bytes!;
  return {population:'unique TableOwner admission to first successful settlement; logical input bytes', sampledLogicalBytes: population,
    settledBytes: healthy ? bytes : null, settledByteSeconds: healthy ? byteSeconds : null, byteWeightedResidenceSeconds: residence,
    impliedBytesPerSecond: residence && population ? population.timeWeightedMean/residence : null,
    observedSettledBytesPerSecond: reason ? null : bytes! / seconds(p), reason,
    caveat:'Time-weighted trapezoidal backlog samples versus completed-cohort residence. Carries across both boundaries and sub-sample bursts can separate the rates; not a steady-state capacity ceiling.'};
}

export function isolationEvidence(samples: Sample[], h: Header) {
  if(h.level === 'L5' || samples.some(s=>!s.isolation)) return null;
  const obs=samples.map(s=>s.isolation!); assertTiming(obs);
  const window=sourceWindow(obs,h);
  const p=points(obs, v=>{
    const data: Values={};
    if(window.boundaryAligned && v.ingress?.scope === 'transport_poll_excluding_async_wait') for(const k of ['rows','ipc_body_bytes','started','completed','poll_wall_ns','poll_thread_cpu_ns']) data[k]=finite(v.ingress[k]);
    return data;
  });
  const physical = window.boundaryAligned && ['L2','L3','L4'].includes(h.level) && obs.every(o=>o.value?.physical) ? physicalEvidence({run_id:h.runId,level:h.level,start_unix_ms:h.startMs,end_unix_ms:h.endMs,
    samples:obs.map(o=>({at_unix_ms:midpoint(o),snapshot:o.value!.physical!}))},h) : null;
  return {window, ingress: {scope:'completed diagnostic streams; IPC body bytes excluding headers. Poll costs exclude async idle; not decode time or CPU utilization.',
    rowsPerSecond:rate(p,'rows'), ipcBodyBytesPerSecond:rate(p,'ipc_body_bytes'), startedStreams:delta(p,'started'), completedStreams:delta(p,'completed'),
    pollWallSeconds:scale(delta(p,'poll_wall_ns'),1e-9), pollThreadCpuSeconds:scale(delta(p,'poll_thread_cpu_ns'),1e-9)},physical};
}

export function physicalEvidence(input: {run_id: string; level: string; start_unix_ms: number; end_unix_ms: number; samples: {at_unix_ms: number; snapshot: Record<string, unknown>}[]}, h: Header) {
  if (input.run_id !== h.runId || input.level !== h.level || input.start_unix_ms !== h.startMs || input.end_unix_ms !== h.endMs ||
      !['L2','L3','L4'].includes(h.level) || !Array.isArray(input.samples) || input.samples.length < 2) throw Error('Physical evidence identity/window mismatch');
  const store = input.samples[0].snapshot.object_store;
  const p = input.samples.map(({at_unix_ms: ms, snapshot: snap}, i) => {
    if (!Number.isFinite(ms) || (i && ms <= input.samples[i-1].at_unix_ms) || String(snap.level).toUpperCase() !== h.level ||
        snap.scope !== 'per_batch_physical_stage' || snap.object_store !== store) throw Error('Physical snapshot level, scope, store, or clock changed');
    const v: Values = {};
    for (const stage of physicalStages) {
      const obj = snap[stage] as Record<string,unknown> | null;
      for (const key of ['batches','rows','bytes','wall_ns','cpu_ns','errors']) v[`${stage}.${key}`] = finite(obj?.[key]);
    }
    for(const key of ['rejected_batches','cleanup_wall_ns','deleted_objects','retained_objects','retained_bytes','unconfirmed_puts']) v[key] = finite(snap[key]);
    v.cleanupErrors = finite(snap.cleanup_errors); v.admissionWaitNs = finite(snap.admission_wait_ns);
    return {ms,v};
  });
  const firstSkewMs = p[0].ms-h.startMs, lastSkewMs = p[p.length-1].ms-h.endMs;
  if (Math.max(Math.abs(firstSkewMs),Math.abs(lastSkewMs)) > Math.min(500,h.intervalMs/2)) throw Error('Physical snapshots do not bracket the requested window within the boundary tolerance');
  return {scope:'per_batch_physical_stage', objectStore: store, windowSeconds: seconds(p), firstSkewMs, lastSkewMs,
    stages: Object.fromEntries(physicalStages.map(s => [s, {batches:delta(p,`${s}.batches`), rowsPerSecond:rate(p,`${s}.rows`), parquetBytesPerSecond:rate(p,`${s}.bytes`),
      wallSeconds:scale(delta(p,`${s}.wall_ns`),1e-9), threadCpuSeconds:scale(delta(p,`${s}.cpu_ns`),1e-9), errors:delta(p,`${s}.errors`)}])),
    rejectedBatches:delta(p,'rejected_batches'), cleanupWallSeconds:scale(delta(p,'cleanup_wall_ns'),1e-9), deletedObjects:delta(p,'deleted_objects'),
    retainedObjectsAtEnd:p[p.length-1].v.retained_objects, retainedBytesAtEnd:p[p.length-1].v.retained_bytes, unconfirmedPuts:delta(p,'unconfirmed_puts'),
    cleanupErrors:delta(p,'cleanupErrors'), admissionWaitSeconds:scale(delta(p,'admissionWaitNs'),1e-9)};
}
