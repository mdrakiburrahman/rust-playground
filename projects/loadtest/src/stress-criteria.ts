/** Fixed-window evidence, sampled at the same declared maintenance-wave phase. */
export interface WaveSample {
  timestamp: number;
  wave: number;
  phase: string;
  driverRows: number;
  rssBytes: number;
  physicalBytes: number;
  backlog: number;
  restarts: number;
  oomEvents: number;
  exhaustedRetries: number;
}

export interface StressCriteria {
  start: number;
  end: number;
  phase: string;
  minimumCycles: number;
  minimumSamples: number;
  minimumDriverRowsPerSecond: number;
  maximumBacklog: number;
  maximumRssBytes: number;
  maximumPhysicalBytes: number;
  maximumRelativeTrend: number;
  maximumRelativeVariation: number;
}

export interface StressVerdict {
  status: 'passed' | 'failed' | 'insufficient-evidence';
  reasons: string[];
  samples: number;
  cycles: number;
  driverRowsPerSecond?: number;
}

function relativeTrend(values: number[]): number {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  if (mean === 0) return 0;
  const center = (values.length - 1) / 2;
  const numerator = values.reduce((sum, value, index) => sum + (index - center) * (value - mean), 0);
  const denominator = values.reduce((sum, _, index) => sum + (index - center) ** 2, 0);
  return numerator / denominator * (values.length - 1) / mean;
}

/** Pure evaluator: never retries, extends the window, fabricates missing metrics, or waits until green. */
export function evaluateStress(allSamples: WaveSample[], criteria: StressCriteria): StressVerdict {
  const limits = ['start', 'end', 'minimumCycles', 'minimumSamples', 'minimumDriverRowsPerSecond',
    'maximumBacklog', 'maximumRssBytes', 'maximumPhysicalBytes', 'maximumRelativeTrend', 'maximumRelativeVariation'] as const;
  if (limits.some(key => typeof criteria[key] !== 'number' || !Number.isFinite(criteria[key])) ||
      criteria.end <= criteria.start || criteria.minimumSamples < 3 || criteria.minimumCycles < 1 ||
      !Number.isInteger(criteria.minimumCycles) || !Number.isInteger(criteria.minimumSamples) || !criteria.phase ||
      Object.entries(criteria).some(([key, value]) => !['start', 'end', 'phase'].includes(key) && Number(value) < 0)) {
    throw new Error('Invalid predeclared stress criteria');
  }
  const window = allSamples.filter(sample => sample.timestamp >= criteria.start && sample.timestamp <= criteria.end);
  const matched = window.filter(sample => sample.phase === criteria.phase);
  const reasons: string[] = [];
  let failed = false;
  let invalid = allSamples.some(sample => !Number.isFinite(sample.timestamp));
  if (invalid) reasons.push('Missing or nonfinite measurement timestamp');
  const numericKeys = ['timestamp', 'wave', 'driverRows', 'rssBytes', 'physicalBytes', 'backlog',
    'restarts', 'oomEvents', 'exhaustedRetries'] as const;
  for (const [index, sample] of window.entries()) {
    if (numericKeys.some(key => typeof sample[key] !== 'number' || !Number.isFinite(sample[key]) || sample[key] < 0) ||
        !Number.isInteger(sample.wave) || !sample.phase) {
      invalid = true;
      reasons.push('Missing, negative or nonfinite telemetry');
      continue;
    }
    const previous = window[index - 1];
    if (previous && (sample.timestamp <= previous.timestamp || sample.wave < previous.wave ||
        sample.wave > previous.wave + 1 || sample.driverRows < previous.driverRows)) {
      invalid = true;
      reasons.push('Counter reset, wave jump or unordered measurement window');
    }
    if (sample.oomEvents > 0 || sample.restarts > 0 || sample.exhaustedRetries > 0) {
      failed = true;
      reasons.push('OOM, unexpected restart or exhausted retries during qualification');
    }
    if (sample.backlog > criteria.maximumBacklog || sample.rssBytes > criteria.maximumRssBytes ||
        sample.physicalBytes > criteria.maximumPhysicalBytes) {
      failed = true;
      reasons.push('Predeclared resource or backlog ceiling exceeded');
    }
  }
  const cycles = Math.max(0, new Set(matched.map(sample => sample.wave)).size - 1);
  const result: StressVerdict = { status: 'passed', reasons, samples: matched.length, cycles };
  if (matched.length < criteria.minimumSamples || cycles < criteria.minimumCycles ||
      matched[0]?.timestamp !== criteria.start || matched.at(-1)?.timestamp !== criteria.end) {
    invalid = true;
    reasons.push('Insufficient matching-phase operation cycles, export samples or fixed-window coverage');
  }
  if (!invalid && matched.length >= 3) {
    const duration = (criteria.end - criteria.start) / 1000;
    result.driverRowsPerSecond = (matched.at(-1)!.driverRows - matched[0].driverRows) / duration;
    if (result.driverRowsPerSecond < criteria.minimumDriverRowsPerSecond ||
        matched.slice(1).some((sample, index) => sample.driverRows <= matched[index].driverRows)) {
      failed = true;
      reasons.push('Input driver stopped, stuttered or missed its predeclared pace');
    }
    for (const key of ['rssBytes', 'physicalBytes', 'backlog'] as const) {
      const values = matched.map(sample => sample[key]);
      const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
      const variation = mean === 0 ? 0 : (Math.max(...values) - Math.min(...values)) / mean;
      if (relativeTrend(values) > criteria.maximumRelativeTrend || variation > criteria.maximumRelativeVariation) {
        failed = true;
        reasons.push(`${key} is not stable at the declared maintenance-wave phase`);
      }
    }
  }
  result.status = failed ? 'failed' : invalid ? 'insufficient-evidence' : 'passed';
  result.reasons = [...new Set(reasons)];
  return result;
}
