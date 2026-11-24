import assert from "node:assert/strict";
import test from "node:test";
import { ARM_RUN_BUDGET_MS, CLEANUP_RESERVE_MS, RECONCILIATION_RESERVE_MS,
  runTier2, scoreTier2, type ArmBinding, type Tier2Arm } from "./tier2.js";

const hash = (value: string) => value.repeat(64);
const binding: ArmBinding = {
  collectorImageId: `sha256:${hash("a")}`, collectorRevision: "a".repeat(40),
  nonCollectorImages: Object.fromEntries(["catalog", "client", "maintenance", "router", "sqlserver"]
    .map((role, index) => [role, `sha256:${String(index).repeat(64)}`])),
  controllerSha256: hash("b"), observerSha256: hash("c"), workloadSha256: hash("d"),
  normalizedConfigSha256: hash("e"), treatmentSha256: hash("f"),
};
const candidate: ArmBinding = { ...binding, collectorImageId: `sha256:${hash("1")}`, collectorRevision: "b".repeat(40) };

function arm(pair: number, role: "baseline" | "candidate", image: ArmBinding, sequence: number): Tier2Arm {
  return {
    pair, role, binding: image, cellRunId: `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`,
    runRoot: `/cells/${sequence}`,
    measurement: { durableRowsPerSecond: role === "candidate" && image === candidate ? 110 : 100,
      parquetBytesPerRow: 34.7, startUnixMs: 100_000, endUnixMs: 250_000, completeBuckets: 5 },
    deliveryIdentity: { verified: true },
    observer: { complete: true, readyUnixMs: 30_000, startUnixMs: 40_000, endUnixMs: 250_000, maxGapMs: 1000 },
  };
}

test("runs distinct cells AB, BA, AB and scores by declared role", async () => {
  let sequence = 0;
  const run = async (pair: number, role: "baseline" | "candidate", image: ArmBinding) =>
    arm(pair, role, image, ++sequence);
  const deadline = ARM_RUN_BUDGET_MS + CLEANUP_RESERVE_MS + RECONCILIATION_RESERVE_MS;
  const control = await runTier2("noise-control", binding, binding, deadline, run, null, () => 0);
  assert.equal(control.tier2Passed, false);
  assert.equal(control.noiseFloorLog, 0);
  const result = await runTier2("trial", binding, candidate, deadline, run, control, () => 0);
  assert.deepEqual(result.arms.map(item => item.role), ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"]);
  assert.equal(result.tier2Passed, true);
  assert.ok(Math.abs(result.score.cases[0]!.throughputRatio - 1.1) < 1e-12);
  const noisyControl = structuredClone(control);
  for (const item of noisyControl.arms.filter(item => item.role === "candidate")) item.measurement.durableRowsPerSecond = 112;
  assert.equal(scoreTier2("trial", result.arms, binding, candidate, noisyControl).tier2Passed, false);
});

test("rejects reused cells, wrong image roles and incomplete warmup evidence", async () => {
  let sequence = 0;
  const run = async (pair: number, role: "baseline" | "candidate", image: ArmBinding) => arm(pair, role, image, ++sequence);
  const control = await runTier2("noise-control", binding, binding, 9_000_000, run, null, () => 0);
  const result = await runTier2("trial", binding, candidate, 9_000_000, run, control, () => 0);
  const duplicate = structuredClone(result.arms);
  duplicate[1]!.cellRunId = duplicate[0]!.cellRunId;
  assert.throws(() => scoreTier2("trial", duplicate, binding, candidate, control), /reused/);
  const wrongRole = structuredClone(result.arms);
  wrongRole[2]!.binding = binding;
  assert.throws(() => scoreTier2("trial", wrongRole, binding, candidate, control), /declared role/);
  const lateObserver = structuredClone(result.arms);
  lateObserver[0]!.observer.readyUnixMs = 40_001;
  assert.throws(() => scoreTier2("trial", lateObserver, binding, candidate, control), /warmup/);
  const brokenOrder = structuredClone(result.arms);
  [brokenOrder[2], brokenOrder[3]] = [brokenOrder[3]!, brokenOrder[2]!];
  assert.throws(() => scoreTier2("trial", brokenOrder, binding, candidate, control), /AB, BA, AB/);
});

test("reserves time for cleanup and reconciliation before admitting an arm", async () => {
  let calls = 0;
  await assert.rejects(runTier2("noise-control", binding, binding,
    ARM_RUN_BUDGET_MS + CLEANUP_RESERVE_MS + RECONCILIATION_RESERVE_MS - 1,
    async () => { calls++; throw new Error("should not run"); }, null, () => 0), /insufficient original deadline/);
  assert.equal(calls, 0);
});

test("stops immediately on invalid arm evidence and rejects control cell reuse", async () => {
  let count = 0;
  const control = await runTier2("noise-control", binding, binding, 9_000_000, async (pair, role, bound) =>
    arm(pair, role, bound, ++count), null, () => 0);
  let launched = 0;
  await assert.rejects(runTier2("trial", binding, candidate, 9_000_000, async (pair, role, bound) => {
    launched++;
    return { ...arm(pair, role, bound, 20), observer: {
      complete: true, readyUnixMs: 30_000, startUnixMs: 40_000, endUnixMs: 250_000, maxGapMs: 5001,
    } };
  }, control, () => 0), /warmup observation/);
  assert.equal(launched, 1);
  await assert.rejects(runTier2("trial", binding, candidate, 9_000_000,
    async (pair, role, bound) => arm(pair, role, bound, 1), control, () => 0), /reused/);
  await assert.rejects(runTier2("trial", binding, binding, 9_000_000,
    async (pair, role, bound) => arm(pair, role, bound, 20), control, () => 0), /A\/A control/);
});
