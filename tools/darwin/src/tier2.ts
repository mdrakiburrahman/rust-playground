import { scoreCases, type Measurement, type Pair, type Score } from "./score.js";
import { isAbsolute, normalize } from "node:path";

export type ArmRole = "baseline" | "candidate";
export type SeriesKind = "noise-control" | "trial";

export interface ArmBinding {
  collectorImageId: string;
  collectorRevision: string;
  nonCollectorImages: Record<string, string>;
  controllerSha256: string;
  observerSha256: string;
  workloadSha256: string;
  normalizedConfigSha256: string;
  treatmentSha256: string;
}

export interface Tier2Arm {
  pair: number;
  role: ArmRole;
  cellRunId: string;
  runRoot: string;
  binding: ArmBinding;
  measurement: Measurement & {
    startUnixMs: number;
    endUnixMs: number;
    completeBuckets: number;
  };
  deliveryIdentity: { verified: boolean };
  observer: {
    complete: boolean;
    readyUnixMs: number;
    startUnixMs: number;
    endUnixMs: number;
    maxGapMs: number;
  };
}

export interface Tier2Result {
  kind: SeriesKind;
  arms: Tier2Arm[];
  score: Score;
  noiseFloorLog: number | null;
  tier2Passed: boolean;
}

const ORDER: readonly (readonly ArmRole[])[] = [
  ["baseline", "candidate"], ["candidate", "baseline"], ["baseline", "candidate"],
];
const SHA = /^[a-f0-9]{64}$/;
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const REVISION = /^[a-f0-9]{40}$/;
const CELL = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const ROLES = ["catalog", "client", "maintenance", "router", "sqlserver"];
export const ARM_RUN_BUDGET_MS = 600_000;
export const CLEANUP_RESERVE_MS = 600_000;
export const RECONCILIATION_RESERVE_MS = 3_600_000;

function assertBinding(binding: ArmBinding): void {
  if (!IMAGE.test(binding.collectorImageId) || !REVISION.test(binding.collectorRevision)
    || !SHA.test(binding.controllerSha256) || !SHA.test(binding.observerSha256)
    || !SHA.test(binding.workloadSha256) || !SHA.test(binding.normalizedConfigSha256)
    || !SHA.test(binding.treatmentSha256)
    || Object.keys(binding.nonCollectorImages).sort().join(",") !== ROLES.join(",")
    || ROLES.some(role => !IMAGE.test(binding.nonCollectorImages[role] ?? ""))) {
    throw new Error("invalid frozen arm binding");
  }
}

function sameCommonBinding(left: ArmBinding, right: ArmBinding): boolean {
  return left.controllerSha256 === right.controllerSha256 && left.observerSha256 === right.observerSha256
    && left.workloadSha256 === right.workloadSha256 && left.normalizedConfigSha256 === right.normalizedConfigSha256
    && ROLES.every(role => left.nonCollectorImages[role] === right.nonCollectorImages[role]);
}

function sameBinding(left: ArmBinding, right: ArmBinding): boolean {
  return sameCommonBinding(left, right) && left.collectorImageId === right.collectorImageId
    && left.collectorRevision === right.collectorRevision && left.treatmentSha256 === right.treatmentSha256;
}

function validateArm(arm: Tier2Arm, expected: ArmBinding, cells: Set<string>, paths: Set<string>): void {
  assertBinding(arm.binding);
  if (!sameBinding(arm.binding, expected)) throw new Error("arm binding differs from its declared role");
  if (!CELL.test(arm.cellRunId) || !isAbsolute(arm.runRoot) || normalize(arm.runRoot) !== arm.runRoot
    || cells.has(arm.cellRunId) || paths.has(arm.runRoot)) {
    throw new Error("cell identity or fresh run path is invalid or reused");
  }
  cells.add(arm.cellRunId);
  paths.add(arm.runRoot);
  const window = arm.measurement;
  if (!Number.isSafeInteger(window.startUnixMs) || !Number.isSafeInteger(window.endUnixMs)
    || window.endUnixMs - window.startUnixMs < 150_000 || !Number.isSafeInteger(window.completeBuckets)
    || window.completeBuckets < 5 || !Number.isFinite(window.durableRowsPerSecond)
    || window.durableRowsPerSecond <= 0 || !Number.isFinite(window.parquetBytesPerRow) || window.parquetBytesPerRow <= 0
    || arm.deliveryIdentity.verified !== true || arm.observer.complete !== true
    || !Number.isSafeInteger(arm.observer.readyUnixMs) || !Number.isSafeInteger(arm.observer.startUnixMs)
    || !Number.isSafeInteger(arm.observer.endUnixMs) || !Number.isSafeInteger(arm.observer.maxGapMs)
    || arm.observer.maxGapMs < 0 || arm.observer.maxGapMs > 5000
    || arm.observer.readyUnixMs > arm.observer.startUnixMs
    || arm.observer.readyUnixMs > window.startUnixMs - 60_000
    || arm.observer.startUnixMs > window.startUnixMs - 60_000
    || arm.observer.endUnixMs < window.endUnixMs) {
    throw new Error("arm lacks complete warmup observation or reconciled measurement evidence");
  }
}

export function scoreTier2(kind: SeriesKind, arms: Tier2Arm[], baseline: ArmBinding, candidate: ArmBinding,
  control: Tier2Result | null = null): Tier2Result {
  assertBinding(baseline);
  assertBinding(candidate);
  if (!sameCommonBinding(baseline, candidate)) throw new Error("non-collector arm identities differ");
  if (kind === "noise-control" && !sameBinding(baseline, candidate)) throw new Error("A/A control bindings differ");
  if (kind === "trial" && sameBinding(baseline, candidate)) throw new Error("identical bindings are an A/A control, not a trial");
  const controlResult = kind === "trial" && control?.kind === "noise-control"
    ? scoreTier2("noise-control", control.arms, baseline, baseline) : null;
  if (kind === "trial" && !controlResult) throw new Error("trial requires a verified A/A noise control");
  if (arms.length !== 6) throw new Error("three complete independent pairs are required");
  const cells = new Set(controlResult?.arms.map(arm => arm.cellRunId));
  const paths = new Set(controlResult?.arms.map(arm => arm.runRoot));
  const pairs: Pair[] = [];
  for (let pair = 0; pair < 3; pair++) {
    const records = arms.filter(arm => arm.pair === pair);
    if (records.length !== 2 || records[0]!.role !== ORDER[pair]![0] || records[1]!.role !== ORDER[pair]![1]) {
      throw new Error("arm execution order must be AB, BA, AB");
    }
    const byRole = new Map(records.map(arm => [arm.role, arm]));
    const a = byRole.get("baseline");
    const b = byRole.get("candidate");
    if (!a || !b) throw new Error("pair roles are incomplete");
    for (const arm of [a, b]) {
      const expected = arm.role === "baseline" ? baseline : candidate;
      validateArm(arm, expected, cells, paths);
    }
    pairs.push({ baseline: a.measurement, candidate: b.measurement });
  }
  const score = scoreCases([{ name: "local", pairs }], "durable-rate");
  const summary = score.cases[0]!;
  const computedNoiseFloor = Math.abs(Math.log(summary.throughputRatio)) + summary.throughputLogUncertainty;
  const threshold = kind === "noise-control" ? computedNoiseFloor : controlResult!.noiseFloorLog;
  const tier2Passed = kind === "trial" && score.accepted
    && threshold !== null && Math.log(summary.throughputRatio) > threshold;
  return { kind, arms, score, noiseFloorLog: threshold, tier2Passed };
}

export async function runTier2(
  kind: SeriesKind, baseline: ArmBinding, candidate: ArmBinding, deadlineUnixMs: number,
  runArm: (pair: number, role: ArmRole, binding: ArmBinding, deadlineUnixMs: number) => Promise<Tier2Arm>,
  control: Tier2Result | null = null, now: () => number = Date.now,
): Promise<Tier2Result> {
  if (!Number.isSafeInteger(deadlineUnixMs)) throw new Error("original absolute deadline is required");
  assertBinding(baseline);
  assertBinding(candidate);
  if (!sameCommonBinding(baseline, candidate) || (kind === "noise-control" && !sameBinding(baseline, candidate))) {
    throw new Error("series bindings differ outside the collector");
  }
  if (kind === "trial" && sameBinding(baseline, candidate)) throw new Error("identical bindings are an A/A control, not a trial");
  if (kind === "trial") {
    if (!control || control.kind !== "noise-control") throw new Error("trial requires a verified A/A noise control");
    scoreTier2("noise-control", control.arms, baseline, baseline);
  }
  const arms: Tier2Arm[] = [];
  const cells = new Set(control?.arms.map(arm => arm.cellRunId));
  const paths = new Set(control?.arms.map(arm => arm.runRoot));
  for (const [pair, roles] of ORDER.entries()) {
    for (const role of roles) {
      const start = now();
      if (!Number.isSafeInteger(start) || start + ARM_RUN_BUDGET_MS + CLEANUP_RESERVE_MS
        + RECONCILIATION_RESERVE_MS > deadlineUnixMs) {
        throw new Error("insufficient original deadline for the next arm, cleanup and reconciliation");
      }
      const binding = role === "baseline" ? baseline : candidate;
      const arm = await runArm(pair, role, binding, start + ARM_RUN_BUDGET_MS);
      if (arm.pair !== pair || arm.role !== role) throw new Error("arm receipt has the wrong declared role");
      validateArm(arm, binding, cells, paths);
      arms.push(arm);
    }
  }
  return scoreTier2(kind, arms, baseline, candidate, control);
}
