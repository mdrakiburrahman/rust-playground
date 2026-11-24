export type Objective = "durable-rate" | "parquet-bytes-per-row";
export type EvidenceScope = "local" | "aks-headline";

export interface Measurement {
  durableRowsPerSecond: number;
  parquetBytesPerRow: number;
}

export interface Pair {
  baseline: Measurement;
  candidate: Measurement;
}

export interface Case {
  name: string;
  pairs: Pair[];
}

export interface CaseScore {
  name: string;
  objectiveRatio: number;
  throughputRatio: number;
  throughputLogUncertainty: number;
  objectiveLogUncertainty: number;
  bytesGuardPassed: boolean;
}

export interface Score {
  combinedScore: number;
  accepted: boolean;
  cases: CaseScore[];
}

const MIN_PAIRS = 3;
// This critical value is conservative for all sample sizes of at least three.
const T_95_DF_2 = 4.303;
const MAX_BYTES_PER_ROW = 34.7 * 1.1;

function assertPositive(value: number, label: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${label} must be finite and positive`);
  }
}

function summarize(logRatios: number[]): { ratio: number; uncertainty: number } {
  const mean = logRatios.reduce((sum, value) => sum + value, 0) / logRatios.length;
  const variance = logRatios.reduce((sum, value) => sum + (value - mean) ** 2, 0)
    / (logRatios.length - 1);
  const result = {
    ratio: Math.exp(mean),
    uncertainty: T_95_DF_2 * Math.sqrt(variance / logRatios.length),
  };
  assertPositive(result.ratio, "paired ratio");
  if (!Number.isFinite(result.uncertainty)) throw new Error("invalid paired uncertainty");
  return result;
}

export function scoreCases(cases: Case[], objective: Objective, scope: EvidenceScope = "local"): Score {
  if (cases.length === 0 || !["durable-rate", "parquet-bytes-per-row"].includes(objective)) {
    throw new Error("at least one case and a supported objective are required");
  }
  if (scope !== "local" && scope !== "aks-headline") throw new Error("unsupported evidence scope");
  const names = new Set<string>();
  const results = cases.map(({ name, pairs }): CaseScore => {
    if (!name || names.has(name) || pairs.length < MIN_PAIRS) {
      throw new Error("case names must be unique and each case needs at least three pairs");
    }
    names.add(name);
    const throughputLogs: number[] = [];
    const objectiveLogs: number[] = [];
    let bytesGuardPassed = true;
    for (const { baseline, candidate } of pairs) {
      for (const [label, measurement] of [["baseline", baseline], ["candidate", candidate]] as const) {
        assertPositive(measurement.durableRowsPerSecond, `${label} durable rows/s`);
        assertPositive(measurement.parquetBytesPerRow, `${label} Parquet bytes/row`);
      }
      const throughput = Math.log(candidate.durableRowsPerSecond) - Math.log(baseline.durableRowsPerSecond);
      throughputLogs.push(throughput);
      objectiveLogs.push(objective === "durable-rate"
        ? throughput
        : Math.log(baseline.parquetBytesPerRow) - Math.log(candidate.parquetBytesPerRow));
      bytesGuardPassed &&= candidate.parquetBytesPerRow <= baseline.parquetBytesPerRow * 1.1
        && (scope !== "aks-headline" || candidate.parquetBytesPerRow <= MAX_BYTES_PER_ROW);
    }
    const throughput = summarize(throughputLogs);
    const result = summarize(objectiveLogs);
    return {
      name,
      objectiveRatio: result.ratio,
      throughputRatio: throughput.ratio,
      throughputLogUncertainty: throughput.uncertainty,
      objectiveLogUncertainty: result.uncertainty,
      bytesGuardPassed,
    };
  });
  const combinedScore = Math.exp(results.reduce(
    (sum, result) => sum + Math.log(result.objectiveRatio), 0,
  ) / results.length);
  assertPositive(combinedScore, "combined score");
  const accepted = results.every((result) => {
    const objectiveEffect = Math.log(result.objectiveRatio);
    const throughputEffect = Math.log(result.throughputRatio);
    return result.bytesGuardPassed
      && result.objectiveRatio >= 1.05
      && result.objectiveLogUncertainty < objectiveEffect
      && (objective !== "parquet-bytes-per-row"
        || throughputEffect - result.throughputLogUncertainty >= 0);
  });
  return { combinedScore, accepted, cases: results };
}
