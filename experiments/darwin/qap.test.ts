import assert from "node:assert/strict";
import { chmodSync, linkSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { join } from "node:path";
import { test } from "node:test";
import { assessLiveObservation, canonical, parseCandidateText, parsePlan, parseProposal, proposalHash, qapCapacity, screen, sha256, type Proposal } from "./qap-contract.js";
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
  pressure.queryAccelerationCapacity.coreUtilizationCoefficient = Number.NaN;
  assert.throws(() => parseProposal(pressure, true));
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
      maxEvaluations: 16, sourceCommit: "a".repeat(40) };
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
