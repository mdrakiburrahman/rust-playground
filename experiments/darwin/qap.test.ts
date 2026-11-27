import assert from "node:assert/strict";
import { chmodSync, linkSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { join } from "node:path";
import { test } from "node:test";
import { assessLiveObservation, canonical, parseCandidateText, parsePlan, parsePressureAttestation, parseProposal, proposalHash, qapCapacity, screen, sha256, type Proposal } from "./qap-contract.js";
import { loadCandidate } from "./qap-evaluator.js";

const fixed = (cores: 4 | 8 | 16 | 32, nodes: number): Proposal => ({
  cluster: { coresPerNode: cores, nodes, autoscale: { enabled: false, minNodes: nodes, maxNodes: nodes } },
  queryAccelerationCapacity: null,
});
const baseline: Proposal = {
  cluster: { coresPerNode: 4, nodes: 2, autoscale: { enabled: true, minNodes: 2, maxNodes: 4 } },
  queryAccelerationCapacity: null,
};
const plan = () => ({ schemaVersion: 1 as const, proposals: [structuredClone(baseline)] });
const pressureFixture = () => ({
  schemaVersion: 1, kind: "controller-attested-qap-pressure", sampleCount: 5, durationSeconds: 40,
  queryAcceleration: { total: 4, consumed: 4, remaining: 0 }, pendingFiles: 1, acceleratedRows: 0,
  baseline: { coresPerNode: 4, nodes: 2, clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 0.5 },
});

test("pressure admission requires trusted matched saturation and exact approved policy values", () => {
  assert.deepEqual(parsePressureAttestation(pressureFixture()), pressureFixture());
  for (const modify of [
    (p: any) => { p.sampleCount = 4; },
    (p: any) => { p.durationSeconds = 39; },
    (p: any) => { p.pendingFiles = 0; },
    (p: any) => { p.queryAcceleration.consumed = 3; p.queryAcceleration.remaining = 1; },
    (p: any) => { p.baseline.coresPerNode = 8; },
    (p: any) => { p.baseline.coreUtilizationCoefficient = 1; },
    (p: any) => { p.authorized = true; },
  ]) {
    const value = pressureFixture(); modify(value);
    assert.throws(() => parsePressureAttestation(value));
  }
  for (const coefficient of [0.5, 1] as const) {
    const p = fixed(8, 4);
    p.queryAccelerationCapacity = { clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: coefficient };
    assert.throws(() => parseProposal(p), /pressure-not-established/);
    assert.deepEqual(parseProposal(p, true), p);
    for (const cap of [1, 99, 101]) {
      assert.throws(() => parseProposal({ ...p, queryAccelerationCapacity: {
        ...p.queryAccelerationCapacity, clusterMaximumConcurrentOperations: cap,
      } }, true), /attested-bounds/);
    }
  }
  for (const coefficient of [0, 0.1, 0.75, 1.5, "0.5", true]) {
    const p = { ...fixed(8, 4), queryAccelerationCapacity: {
      clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: coefficient,
    } };
    assert.throws(() => parseProposal(p, true), /attested-bounds/);
  }
  assert.throws(() => parsePlan({ ...plan(), pressureAttested: true }, true), /field/);
});

test("pressure coverage requires an explicit matched control and never equates null with coefficient0.5", () => {
  const low = fixed(8, 4);
  low.queryAccelerationCapacity = { clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 0.5 };
  const high = structuredClone(low);
  high.queryAccelerationCapacity!.coreUtilizationCoefficient = 1;
  const matched = parsePlan({ schemaVersion: 1, proposals: [low, high] }, true);
  assert.equal(screen(matched, true).coverage.matchedCapacityComparisons.length, 1);
  assert.equal(screen(matched).coverage.matchedCapacityComparisons.length, 0);
  const noControl = parsePlan({ schemaVersion: 1, proposals: [fixed(8, 4), high] }, true);
  assert.equal(screen(noControl, true).coverage.matchedCapacityComparisons.length, 0);
  const differentTopology = structuredClone(high);
  differentTopology.cluster = fixed(16, 2).cluster;
  assert.equal(screen(parsePlan({ schemaVersion: 1, proposals: [low, differentTopology] }, true), true)
    .coverage.matchedCapacityComparisons.length, 0);
  assert.ok(screen(matched, true).combined_score > screen(noControl, true).combined_score);
});

test("strict data boundary rejects unknown, executable, aliased and overbudget proposals", () => {
  assert.deepEqual(parseCandidateText(`${canonical(plan())}\n`), plan());
  for (const mutate of [
    (p: any) => { p.command = "echo unsafe"; },
    (p: any) => { p.proposals[0].cluster.sku = "../../private"; },
    (p: any) => { p.proposals[0].cluster.nodes = "2"; },
    (p: any) => { p.proposals[0].cluster.autoscale.enabled = 1; },
    (p: any) => { p.proposals[0].cluster.autoscale.maxNodes = 9; },
    (p: any) => { p.proposals[0].cluster.coresPerNode = 32; },
    (p: any) => { p.proposals = Array.from({ length: 9 }, () => baseline); },
    (p: any) => { p.proposals.push(baseline); },
    (p: any) => { p.proposals[0].queryAccelerationCapacity = { clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 0.5 }; },
  ]) {
    const value = plan(); mutate(value);
    assert.throws(() => parsePlan(value));
  }
  assert.throws(() => parseCandidateText('{"schemaVersion":1,"schemaVersion":1,"proposals":[]}\n'));
  assert.throws(() => parseCandidateText(JSON.stringify(plan()) + "\n"), /canonical/);
  assert.throws(() => parseCandidateText(`${" ".repeat(16_385)}${canonical(plan())}\n`), /too-large/);
  const inconsistent = fixed(8, 4); inconsistent.cluster.autoscale.minNodes = 2;
  assert.throws(() => parseProposal(inconsistent), /fixed-node/);
  const pressure = fixed(8, 4);
  pressure.queryAccelerationCapacity = { clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 0.5 };
  assert.deepEqual(parseProposal(pressure, true), pressure);
  assert.throws(() => parseProposal({ ...pressure, queryAccelerationCapacity: {
    ...pressure.queryAccelerationCapacity, coreUtilizationCoefficient: Number.NaN,
  } }, true));
});

test("screening rewards matched-core and matched-peak autoscale comparisons, not large machines alone", () => {
  const seed = screen(plan());
  const matrix = parsePlan({ schemaVersion: 1, proposals: [baseline, fixed(4, 4), fixed(8, 2)] });
  const result = screen(matrix);
  assert.deepEqual(result.coverage.matchedCoreBudgets, [16]);
  assert.deepEqual(result.coverage.autoscaleComparisons, ["4:4"]);
  assert.ok(result.combined_score > seed.combined_score);
  assert.ok(screen({ schemaVersion: 1, proposals: [fixed(32, 2)] }).combined_score < seed.combined_score);
  assert.equal(qapCapacity(2, 4, 100, 0.5), 4);
  assert.equal(qapCapacity(8, 8, 100, 0.5), 32);
  assert.equal(qapCapacity(8, 8, 5, 0.5), 5);
  assert.equal(qapCapacity(4, 4, 100, 0.1), 4);
});

test("candidate tree cannot redirect paths, execute code, mutate scoring or substitute linked files", () => {
  const root = join(process.cwd(), "out", `qap-test-${randomUUID()}`);
  mkdirSync(join(root, ".git"), { recursive: true });
  const candidate = join(root, "candidate.json");
  const text = `${canonical(plan())}\n`;
  try {
    writeFileSync(candidate, text, { mode: 0o600 });
    assert.deepEqual(loadCandidate(root), plan());
    writeFileSync(join(root, "qap-evaluator.js"), "mutated");
    assert.throws(() => loadCandidate(root), /tree-mutated/);
    rmSync(join(root, "qap-evaluator.js"));
    chmodSync(candidate, 0o700);
    assert.throws(() => loadCandidate(root), /unsafe-file/);
    chmodSync(candidate, 0o600);
    linkSync(candidate, join(root, ".git", "alias"));
    assert.throws(() => loadCandidate(root), /unsafe-file/);
    rmSync(join(root, ".git", "alias"));
    rmSync(candidate);
    symlinkSync(join(root, ".git", "payload"), candidate);
    writeFileSync(join(root, ".git", "payload"), text);
    assert.throws(() => loadCandidate(root));
    rmSync(candidate);
    writeFileSync(candidate, text);
    rmSync(join(root, ".git"), { recursive: true });
    symlinkSync(root, join(root, ".git"), "dir");
    assert.throws(() => loadCandidate(root), /git-symlink/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});

test("trusted live adapter refuses missing latency, forged bindings, partial coverage, budget and rate failures", () => {
  const proposal = fixed(8, 4);
  const workload = "a".repeat(64);
  const good = {
    schemaVersion: 1, kind: "trusted-live-observation", proposalSha256: proposalHash(proposal),
    workloadSha256: workload, evidenceSha256: "b".repeat(64), durationSeconds: 600,
    durableRows: 6_360_000_000, minWindowDurableRowsPerSecond: 10_600_000,
    unchangedProducer: true, p95FullyAcceleratedUpperMs: 120_000,
    allMeasuredCohortsCovered: true, growingSteadyStateBacklog: false, exactTerminalParity: true,
    actualSkuVerified: true, maxObservedEngineVcpu: 32, maxObservedNodes: 4,
    configuredAutoscaleMaxNodes: 4, configuredAutoscaleMaxEngineVcpu: 32, telemetryComplete: true,
  };
  assert.equal(assessLiveObservation(good, proposal, workload).qualified, true);
  for (const change of [
    { p95FullyAcceleratedUpperMs: null }, { minWindowDurableRowsPerSecond: 10_599_999 },
    { durationSeconds: 599 }, { durableRows: 6_359_999_999 }, { unchangedProducer: false },
    { allMeasuredCohortsCovered: false }, { growingSteadyStateBacklog: true }, { exactTerminalParity: false },
    { actualSkuVerified: false }, { maxObservedEngineVcpu: 65 }, { configuredAutoscaleMaxNodes: 9 },
    { configuredAutoscaleMaxEngineVcpu: 128 }, { telemetryComplete: false },
  ]) {
    const result = assessLiveObservation({ ...good, ...change }, proposal, workload);
    assert.equal(result.qualified, false);
    assert.equal(result.combined_score, 0);
  }
  assert.throws(() => assessLiveObservation({ ...good, proposalSha256: "c".repeat(64) }, proposal, workload), /binding/);
  assert.throws(() => assessLiveObservation({ ...good, minWindowDurableRowsPerSecond: Infinity }, proposal, workload));
  assert.throws(() => assessLiveObservation({ ...good, exactTerminalParity: "true" }, proposal, workload));
  assert.throws(() => assessLiveObservation({ ...good, evidencePath: "/private" }, proposal, workload), /field/);
});

test("absolute trusted evaluator emits zero for mutations and refuses tampered scoring or exhausted budgets", () => {
  const root = join(process.cwd(), "out", `qap-test-${randomUUID()}`);
  const source = join(root, "source");
  const trusted = join(root, "trusted");
  mkdirSync(join(source, ".git"), { recursive: true });
  mkdirSync(trusted);
  try {
    execFileSync("npx", ["--no-install", "tsc", "--ignoreConfig", "--target", "ES2022",
      "--module", "NodeNext", "--moduleResolution", "NodeNext", "--strict", "--skipLibCheck",
      "--types", "node", "--rootDir", "experiments/darwin", "--outDir", trusted,
      "experiments/darwin/qap-contract.ts", "experiments/darwin/qap-evaluator.ts"],
    { timeout: 60_000, stdio: "pipe" });
    writeFileSync(join(source, "candidate.json"), `${canonical(plan())}\n`);
    const receiptFile = join(root, "receipts.jsonl");
    writeFileSync(receiptFile, "");
    const files = Object.fromEntries(["qap-contract.js", "qap-evaluator.js"].map((name) =>
      [name, sha256(readFileSync(join(trusted, name)))]));
    const manifestPath = join(trusted, "manifest.json");
    const manifest = { schemaVersion: 1, files, receiptFile, deadlineMs: Date.now() + 60_000,
      maxEvaluations: 16, sourceCommit: "a".repeat(40), pressureAttestationSha256: null };
    writeFileSync(manifestPath, canonical(manifest));
    const invoke = () => spawnSync(process.execPath, [join(trusted, "qap-evaluator.js"), manifestPath],
      { cwd: source, encoding: "utf8", timeout: 10_000 });
    const first = invoke();
    assert.equal(first.status, 0, first.stderr);
    const receipt = JSON.parse(first.stdout.trim().slice(8));
    assert.equal(receipt.kind, "screening");
    assert.equal(receipt.qualified, false);
    assert.equal(receipt.valid, 1);
    assert.equal(receipt.freshPrivateCases, 24);
    const second = JSON.parse(invoke().stdout.trim().slice(8));
    assert.notEqual(receipt.privateCaseNonceSha256, second.privateCaseNonceSha256);
    writeFileSync(join(source, "workload.json"), "{}");
    const invalid = invoke();
    assert.equal(invalid.status, 0);
    assert.equal(JSON.parse(invalid.stdout.trim().slice(8)).combined_score, 0);
    rmSync(join(source, "workload.json"));
    const pressureText = `${canonical(pressureFixture())}\n`;
    const pressurePath = join(trusted, "pressure-attestation.json");
    const pressureManifest = { ...manifest, pressureAttestationSha256: sha256(pressureText) };
    writeFileSync(pressurePath, pressureText);
    const policy = fixed(8, 4);
    policy.queryAccelerationCapacity = { clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 1 };
    writeFileSync(join(source, "candidate.json"), `${canonical({ schemaVersion: 1, proposals: [policy] })}\n`);
    assert.equal(JSON.parse(invoke().stdout.trim().slice(8)).valid, 0);
    writeFileSync(manifestPath, canonical(pressureManifest));
    const pressure = invoke();
    assert.equal(pressure.status, 0, pressure.stderr);
    const pressureReceipt = JSON.parse(pressure.stdout.trim().slice(8));
    assert.equal(pressureReceipt.valid, 1);
    assert.equal(pressureReceipt.screeningMode, "pressure-attested");
    assert.equal(pressureReceipt.pressureAttestationSha256, sha256(pressureText));
    assert.equal(pressureReceipt.freshPrivateCases, 36);
    writeFileSync(pressurePath, `${pressureText} `);
    const mutatedPressure = invoke();
    assert.notEqual(mutatedPressure.status, 0);
    assert.equal(mutatedPressure.stdout, "");
    writeFileSync(pressurePath, pressureText);
    writeFileSync(manifestPath, canonical({ ...manifest, deadlineMs: 0 }));
    const expired = invoke();
    assert.notEqual(expired.status, 0);
    assert.equal(expired.stdout, "");
    writeFileSync(manifestPath, canonical(manifest));
    writeFileSync(receiptFile, "{}\n".repeat(16));
    assert.notEqual(invoke().status, 0);
    writeFileSync(receiptFile, "");
    writeFileSync(join(trusted, "qap-contract.js"), `${readFileSync(join(trusted, "qap-contract.js"), "utf8")}\n`);
    const tampered = invoke();
    assert.notEqual(tampered.status, 0);
    assert.equal(tampered.stdout, "");
    assert.equal(readFileSync(receiptFile, "utf8"), "");
  } finally { rmSync(root, { recursive: true, force: true }); }
});
