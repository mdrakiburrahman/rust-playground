import { createHash } from "node:crypto";

export interface Proposal {
  cluster: {
    coresPerNode: 4 | 8 | 16 | 32;
    nodes: number;
    autoscale: { enabled: boolean; minNodes: number; maxNodes: number };
  };
  queryAccelerationCapacity: null | {
    clusterMaximumConcurrentOperations: 100;
    coreUtilizationCoefficient: 0.5 | 1;
  };
}

export interface CandidatePlan {
  schemaVersion: 1;
  proposals: Proposal[];
}

export interface PressureAttestation {
  schemaVersion: 1;
  kind: "controller-attested-qap-pressure";
  sampleCount: number;
  durationSeconds: number;
  queryAcceleration: { total: number; consumed: number; remaining: number };
  pendingFiles: number;
  acceleratedRows: number;
  baseline: {
    coresPerNode: 4;
    nodes: 2;
    clusterMaximumConcurrentOperations: 100;
    coreUtilizationCoefficient: 0.5;
  };
}

export interface LiveObservation {
  schemaVersion: 1;
  kind: "trusted-live-observation";
  proposalSha256: string;
  workloadSha256: string;
  evidenceSha256: string;
  durationSeconds: number;
  durableRows: number;
  minWindowDurableRowsPerSecond: number;
  unchangedProducer: boolean;
  p95FullyAcceleratedUpperMs: number | null;
  allMeasuredCohortsCovered: boolean;
  growingSteadyStateBacklog: boolean;
  exactTerminalParity: boolean;
  actualSkuVerified: boolean;
  maxObservedEngineVcpu: number;
  maxObservedNodes: number;
  configuredAutoscaleMaxNodes: number;
  configuredAutoscaleMaxEngineVcpu: number;
  telemetryComplete: boolean;
}

export function record(value: unknown, fields: readonly string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("object-required");
  const result = value as Record<string, unknown>;
  if (Object.getPrototypeOf(result) !== Object.prototype) throw new Error("plain-object-required");
  if (Object.keys(result).sort().join("\0") !== [...fields].sort().join("\0")) throw new Error("unknown-or-missing-field");
  return result;
}

function number(value: unknown, min: number, max: number, integer = true): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < min || value > max ||
      (integer && !Number.isSafeInteger(value))) throw new Error("number-out-of-bounds");
  return value;
}

function bool(value: unknown): boolean {
  if (typeof value !== "boolean") throw new Error("boolean-required");
  return value;
}

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) =>
      `${JSON.stringify(key)}:${canonical((value as Record<string, unknown>)[key])}`).join(",")}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new Error("non-json-value");
  return encoded;
}

export function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

export function proposalHash(value: Proposal): string {
  return sha256(canonical(value));
}

export function parsePressureAttestation(value: unknown): PressureAttestation {
  const p = record(value, ["schemaVersion", "kind", "sampleCount", "durationSeconds",
    "queryAcceleration", "pendingFiles", "acceleratedRows", "baseline"]);
  if (p.schemaVersion !== 1 || p.kind !== "controller-attested-qap-pressure") throw new Error("pressure-attestation-version");
  number(p.sampleCount, 5, 1000);
  number(p.durationSeconds, 40, 600, false);
  number(p.pendingFiles, 1, Number.MAX_SAFE_INTEGER);
  number(p.acceleratedRows, 0, Number.MAX_SAFE_INTEGER);
  const q = record(p.queryAcceleration, ["total", "consumed", "remaining"]);
  const b = record(p.baseline, ["coresPerNode", "nodes",
    "clusterMaximumConcurrentOperations", "coreUtilizationCoefficient"]);
  if (b.coresPerNode !== 4 || b.nodes !== 2 || b.clusterMaximumConcurrentOperations !== 100 ||
      b.coreUtilizationCoefficient !== 0.5 || q.total !== 4 || q.consumed !== 4 || q.remaining !== 0) {
    throw new Error("pressure-attestation-not-matched-saturation");
  }
  return structuredClone(value) as PressureAttestation;
}

export function parseProposal(value: unknown, pressureImplicated = false): Proposal {
  const p = record(value, ["cluster", "queryAccelerationCapacity"]);
  const c = record(p.cluster, ["coresPerNode", "nodes", "autoscale"]);
  const a = record(c.autoscale, ["enabled", "minNodes", "maxNodes"]);
  const cores = number(c.coresPerNode, 4, 32);
  if (![4, 8, 16, 32].includes(cores)) throw new Error("unsupported-core-shape");
  const nodes = number(c.nodes, 2, 8);
  const enabled = bool(a.enabled);
  const min = number(a.minNodes, 2, 8);
  const max = number(a.maxNodes, 2, 8);
  if (min > nodes || nodes > max || cores * max > 64) throw new Error("engine-budget-exceeded");
  if (!enabled && (min !== nodes || max !== nodes)) throw new Error("fixed-node-range-mismatch");
  if (p.queryAccelerationCapacity !== null) {
    if (!pressureImplicated) throw new Error("capacity-pressure-not-established");
    const q = record(p.queryAccelerationCapacity, ["clusterMaximumConcurrentOperations", "coreUtilizationCoefficient"]);
    if (q.clusterMaximumConcurrentOperations !== 100 || ![0.5, 1].includes(q.coreUtilizationCoefficient as number)) {
      throw new Error("capacity-policy-outside-attested-bounds");
    }
  }
  return structuredClone(value) as Proposal;
}

export function parsePlan(value: unknown, pressureImplicated = false): CandidatePlan {
  const p = record(value, ["schemaVersion", "proposals"]);
  if (p.schemaVersion !== 1 || !Array.isArray(p.proposals) || p.proposals.length < 1 || p.proposals.length > 8) {
    throw new Error("plan-budget-exceeded");
  }
  const proposals = p.proposals.map((item) => parseProposal(item, pressureImplicated));
  if (new Set(proposals.map(proposalHash)).size !== proposals.length) throw new Error("duplicate-proposal");
  return { schemaVersion: 1, proposals };
}

// Requiring one canonical representation also rejects duplicate JSON keys and escaped-key aliases.
export function parseCandidateText(text: string, pressureImplicated = false): CandidatePlan {
  if (Buffer.byteLength(text) > 16_384) throw new Error("candidate-too-large");
  const value: unknown = JSON.parse(text);
  const plan = parsePlan(value, pressureImplicated);
  if (text !== `${canonical(plan)}\n`) throw new Error("candidate-must-be-canonical-json-with-newline");
  return plan;
}

export function qapCapacity(nodes: number, coresPerNode: number, cap: number, coefficient: number): number {
  return Math.min(cap, nodes * Math.max(1, coresPerNode * coefficient));
}

export function screen(plan: CandidatePlan, pressureAttested = false) {
  const matchedCoreBudgets = new Set<number>();
  const autoscaleComparisons = new Set<string>();
  const matchedCapacityComparisons = new Set<string>();
  let baselinePresent = false;
  for (const a of plan.proposals) {
    const x = a.cluster;
    const auto = x.autoscale;
    if (x.coresPerNode === 4 && x.nodes === 2 && auto.enabled && auto.minNodes === 2 &&
        auto.maxNodes === 4 && a.queryAccelerationCapacity === null) baselinePresent = true;
    for (const b of plan.proposals) {
      const y = b.cluster;
      if (pressureAttested && a.queryAccelerationCapacity?.clusterMaximumConcurrentOperations === 100 &&
          b.queryAccelerationCapacity?.clusterMaximumConcurrentOperations === 100 &&
          a.queryAccelerationCapacity.coreUtilizationCoefficient === 0.5 &&
          b.queryAccelerationCapacity.coreUtilizationCoefficient === 1 && canonical(x) === canonical(y)) {
        matchedCapacityComparisons.add(canonical(x));
      }
      if (canonical(a.queryAccelerationCapacity) !== canonical(b.queryAccelerationCapacity)) continue;
      if (!auto.enabled && !y.autoscale.enabled && x.coresPerNode !== y.coresPerNode &&
          x.coresPerNode * x.nodes === y.coresPerNode * y.nodes) {
        matchedCoreBudgets.add(x.coresPerNode * x.nodes);
      }
      if (auto.enabled && !y.autoscale.enabled && auto.minNodes < auto.maxNodes &&
          x.coresPerNode === y.coresPerNode && auto.maxNodes === y.nodes) {
        autoscaleComparisons.add(`${x.coresPerNode}:${auto.maxNodes}`);
      }
    }
  }
  const coverage = {
    baselinePresent,
    matchedCoreBudgets: [...matchedCoreBudgets].sort((a, b) => a - b),
    autoscaleComparisons: [...autoscaleComparisons].sort(),
    matchedCapacityComparisons: [...matchedCapacityComparisons].sort(),
    proposals: plan.proposals.length,
  };
  // Experimental-design coverage, never a model of latency, throughput, cost, or Azure availability.
  const score = pressureAttested ?
    (baselinePresent ? 10 : 0) + 15 * Math.min(2, matchedCoreBudgets.size) +
      15 * Math.min(1, autoscaleComparisons.size) + 40 * Math.min(1, matchedCapacityComparisons.size) +
      5 / plan.proposals.length :
    (baselinePresent ? 20 : 0) + 15 * Math.min(3, matchedCoreBudgets.size) +
      15 * Math.min(2, autoscaleComparisons.size) + 5 / plan.proposals.length;
  return { combined_score: score, coverage };
}

export function assessLiveObservation(value: unknown, proposal: Proposal, expectedWorkloadSha256: string) {
  const r = record(value, [
    "schemaVersion", "kind", "proposalSha256", "workloadSha256", "evidenceSha256",
    "durationSeconds", "durableRows", "minWindowDurableRowsPerSecond", "unchangedProducer",
    "p95FullyAcceleratedUpperMs", "allMeasuredCohortsCovered", "growingSteadyStateBacklog",
    "exactTerminalParity", "actualSkuVerified", "maxObservedEngineVcpu", "maxObservedNodes",
    "configuredAutoscaleMaxNodes", "configuredAutoscaleMaxEngineVcpu", "telemetryComplete",
  ]);
  parseProposal(proposal, true);
  if (r.schemaVersion !== 1 || r.kind !== "trusted-live-observation") throw new Error("observation-version");
  for (const hash of [r.proposalSha256, r.workloadSha256, r.evidenceSha256, expectedWorkloadSha256]) {
    if (typeof hash !== "string" || !/^[a-f0-9]{64}$/.test(hash)) throw new Error("observation-hash");
  }
  if (r.proposalSha256 !== proposalHash(proposal) || r.workloadSha256 !== expectedWorkloadSha256) {
    throw new Error("observation-binding-mismatch");
  }
  const duration = number(r.durationSeconds, 0, 86_400, false);
  const rows = number(r.durableRows, 0, Number.MAX_SAFE_INTEGER);
  const minRate = number(r.minWindowDurableRowsPerSecond, 0, Number.MAX_SAFE_INTEGER, false);
  const peakCores = number(r.maxObservedEngineVcpu, 1, Number.MAX_SAFE_INTEGER);
  const peakNodes = number(r.maxObservedNodes, 1, Number.MAX_SAFE_INTEGER);
  const maxNodes = number(r.configuredAutoscaleMaxNodes, 1, Number.MAX_SAFE_INTEGER);
  const maxCores = number(r.configuredAutoscaleMaxEngineVcpu, 1, Number.MAX_SAFE_INTEGER);
  const reasons: string[] = [];
  if (duration !== 600 || rows < 10_600_000 * 600 || minRate < 10_600_000) reasons.push("producer-load-not-sustained");
  for (const key of ["unchangedProducer", "allMeasuredCohortsCovered", "exactTerminalParity",
    "actualSkuVerified", "telemetryComplete"] as const) {
    if (!bool(r[key])) reasons.push(key);
  }
  if (bool(r.growingSteadyStateBacklog)) reasons.push("growing-steady-state-backlog");
  if (peakCores > 64 || peakNodes > 8 || maxCores > 64 || maxNodes > 8 ||
      maxNodes !== proposal.cluster.autoscale.maxNodes ||
      maxCores !== proposal.cluster.coresPerNode * proposal.cluster.autoscale.maxNodes ||
      peakNodes > maxNodes || peakCores > maxCores) reasons.push("engine-budget-or-shape");
  const p95 = r.p95FullyAcceleratedUpperMs === null ? null :
    number(r.p95FullyAcceleratedUpperMs, 0, 86_400_000, false);
  if (p95 === null) reasons.push("latency-not-proven");
  return {
    kind: "trusted-live-assessment" as const,
    qualified: reasons.length === 0,
    p95FullyAcceleratedUpperMs: p95,
    combined_score: reasons.length === 0 ? 1 / (1 + p95!) : 0,
    reasons,
  };
}
