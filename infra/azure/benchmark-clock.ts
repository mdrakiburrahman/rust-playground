export class ClockEvidenceError extends Error {}
function requireClock(condition: unknown, message: string): asserts condition {
  if (!condition) throw new ClockEvidenceError(message);
}

export interface DirectClockReading {
  node: string;
  podUid: string;
  beforeMs: number;
  afterMs: number;
  observedNs: string;
  timeNamespace: string;
}

export function directClockBound(beforeMs: number, afterMs: number, observedNs: string) {
  requireClock(Number.isSafeInteger(beforeMs) && Number.isSafeInteger(afterMs) && beforeMs > 0 && afterMs >= beforeMs &&
    /^[1-9]\d{17,18}$/.test(observedNs), 'Original clock sample is invalid');
  const epoch = BigInt(observedNs), lower = epoch - (BigInt(afterMs) + 1n) * 1_000_000n,
    upper = epoch - BigInt(beforeMs) * 1_000_000n;
  requireClock(lower >= -100_000_000n && upper <= 100_000_000n, 'Original clock uncertainty exceeds 100 ms');
  return { beforeMs, afterMs, observedNs, lowerNs: String(lower), upperNs: String(upper) };
}

export function checkDirectClocks(nodes: ReadonlyMap<string, { before: string; after: string }>, startMs: number, endMs: number,
  before: readonly DirectClockReading[], after: readonly DirectClockReading[]) {
  requireClock(Number.isSafeInteger(startMs) && Number.isSafeInteger(endMs) && startMs < endMs &&
    nodes.size > 0 && before.length === nodes.size && after.length === nodes.size, 'Clock coverage or window is incomplete');
  for (const [node, observers] of nodes) {
    const first = before.filter(sample => sample.node === node);
    const last = after.filter(sample => sample.node === node);
    requireClock(first.length === 1 && last.length === 1 && first[0].podUid === observers.before && last[0].podUid === observers.after &&
      first[0].afterMs < startMs && last[0].beforeMs >= endMs && /^time:\[\d+\]$/.test(first[0].timeNamespace) &&
      first[0].timeNamespace === last[0].timeNamespace, 'Clock sample does not bracket the original node window or share its time namespace');
    directClockBound(first[0].beforeMs, first[0].afterMs, first[0].observedNs);
    directClockBound(last[0].beforeMs, last[0].afterMs, last[0].observedNs);
  }
}

export function directClockOffsets(nodes: ReadonlyMap<string, { before: string; after: string }>, startMs: number, endMs: number,
  before: readonly DirectClockReading[], after: readonly DirectClockReading[]) {
  checkDirectClocks(nodes, startMs, endMs, before, after);
  return new Map([...nodes.keys()].map(node => {
    const readings = [before.find(sample => sample.node === node)!, after.find(sample => sample.node === node)!]
      .map(sample => directClockBound(sample.beforeMs, sample.afterMs, sample.observedNs));
    const lower = readings.map(sample => BigInt(sample.lowerNs)), upper = readings.map(sample => BigInt(sample.upperNs));
    return [node, { lowerNs: lower[0] < lower[1] ? lower[0] : lower[1],
      upperNs: upper[0] > upper[1] ? upper[0] : upper[1] }] as const;
  }));
}
