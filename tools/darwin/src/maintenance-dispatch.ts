import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, mkdir, readFile, realpath } from "node:fs/promises";
import path from "node:path";
import {
  evaluateMaintenance, FIXTURE_REVISION, FIXTURE_SHA256, prepareMaintenance,
  scoreMaintenancePairs, type MaintenanceLane,
} from "./maintenance-adapter.js";

const SUPPORT_REVISION = "f564ced0d38046f72c9b74f0f366a3657e146b7d";
const FIXTURE = "crates/arrow-deltalake/examples/darwin_maintenance.rs";
const MAINTENANCE = "crates/arrow-deltalake/src/maintenance.rs";
const COMPARATOR = "crates/arrow-deltalake/examples/darwin_maintenance_verify.rs";
const SANDBOX = "tools/darwin/src/maintenance-sandbox.rs";

// This context is supplied only by the trusted outer runner, never by a candidate.
export interface TrustedMaintenanceContext {
  repo: string;
  home: string;
}

export interface ControlRequest {
  mode: "control";
  experiments: Array<"maintenance-active" | "maintenance-idle">;
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`invalid ${label}`);
  }
  return value as Record<string, unknown>;
}

function count(value: unknown, label: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`invalid ${label}`);
  }
  return value;
}

function physical(value: unknown, lane: MaintenanceLane): void {
  const result = record(value, "physical comparator evidence");
  if (lane === "idle") {
    if (result.idle_commits !== 0 || result.due_append_commits !== 1 || result.due_rows !== 1536) {
      throw new Error("invalid idle physical comparator evidence");
    }
    return;
  }
  if (result.rows !== 17408 || result.initial_files !== 34 || result.protected_files !== 2) {
    throw new Error("invalid active physical comparator evidence");
  }
  for (const key of ["initial_bytes", "final_files", "final_bytes", "rewrite_commits",
    "rewrite_input_files", "rewrite_input_bytes", "rewrite_output_files", "rewrite_output_bytes"]) {
    count(result[key], key, 1);
  }
}

function elapsed(value: unknown, arm: string, lane: MaintenanceLane): number {
  const sample = record(value, `${arm} sample`);
  if (sample.arm !== arm || typeof sample.elapsedSeconds !== "number"
    || !Number.isFinite(sample.elapsedSeconds) || sample.elapsedSeconds <= 0
    || sample.inverseElapsedPerSecond !== 1 / sample.elapsedSeconds) {
    throw new Error(`invalid ${arm} elapsed evidence`);
  }
  physical(sample.physical, lane);
  if (lane === "idle") {
    if (!Array.isArray(sample.reportedObjectStoreCounts) || sample.reportedObjectStoreCounts.length !== 3) {
      throw new Error("missing idle object-store instrumentation");
    }
    for (const [index, item] of sample.reportedObjectStoreCounts.entries()) {
      const observed = record(item, "object-store instrumentation");
      if (observed.case !== ["empty", "quiescent", "protected"][index]
        || observed.claimedPasses !== 30 || observed.claimedProbeInvocations !== 150 || observed.write !== 0) {
        throw new Error("invalid idle object-store instrumentation");
      }
      count(observed.get, "object-store get");
      count(observed.list, "object-store list");
    }
  }
  return sample.elapsedSeconds;
}

// This validator is not an admission or execution API; only dispatch invokes the evaluator.
export function validateControlEvidence(
  value: unknown, lane: MaintenanceLane, expectedBindings: Readonly<Record<string, string>>,
): { ratio: number; logUncertainty: number } {
  const result = record(value, "maintenance result");
  const bindings = record(result.bindings, "maintenance bindings");
  if (result.lane !== `maintenance-${lane}` || result.storage !== "local-file"
    || result.scope !== "maintenance-only" || result.objective !== "inverse full fixture process elapsed"
    || result.accepted !== false || result.screenPassed !== false
    || JSON.stringify(bindings) !== JSON.stringify(expectedBindings)
    || bindings.baseRevision !== FIXTURE_REVISION || bindings.fixtureSha256 !== FIXTURE_SHA256
    || bindings.baselineMaintenanceSha256 !== bindings.candidateMaintenanceSha256
    || bindings.baselineBinarySha256 !== bindings.candidateBinarySha256) {
    throw new Error("maintenance control identity or scope differs");
  }
  for (const key of ["baselineMaintenanceSha256", "candidateMaintenanceSha256", "baselineBinarySha256",
    "candidateBinarySha256", "comparatorSourceSha256", "comparatorBinarySha256",
    "sandboxSourceSha256", "sandboxBinarySha256", "seedSha256", "adapterSourceSha256"]) {
    const binding = bindings[key];
    if (typeof binding !== "string" || !/^[a-f0-9]{64}$/.test(binding)) {
      throw new Error(`invalid maintenance binding: ${key}`);
    }
  }
  if (!Array.isArray(result.audits) || result.audits.length !== 2) {
    throw new Error("missing cadence audit evidence");
  }
  for (const [index, item] of result.audits.entries()) {
    const audit = record(item, "cadence audit");
    const arm = index === 0 ? "baseline" : "candidate";
    if (audit.arm !== arm || audit.trace !== `${lane}-audit-${arm}.trace`) throw new Error("invalid cadence audit");
    physical(audit.physical, lane);
  }
  if (!Array.isArray(result.pairs) || result.pairs.length !== 3) throw new Error("missing maintenance pairs");
  const pairs = result.pairs.map((item) => {
    const pair = record(item, "maintenance pair");
    return { baseline: elapsed(pair.baseline, "baseline", lane), candidate: elapsed(pair.candidate, "candidate", lane) };
  });
  const score = scoreMaintenancePairs(pairs, false);
  if (result.ratio !== score.ratio || result.logUncertainty !== score.logUncertainty || score.screenPassed) {
    throw new Error("invalid maintenance score");
  }
  return { ratio: score.ratio, logUncertainty: score.logUncertainty };
}

function sha256(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

async function trustedInputs(context: TrustedMaintenanceContext): Promise<{
  repo: string; home: string; src: string; snapshotCommit: string; hashes: Record<string, string>;
}> {
  if (Object.keys(record(context, "trusted runner context")).sort().join(",") !== "home,repo") {
    throw new Error("invalid trusted runner context");
  }
  const repo = path.resolve(context.repo);
  const home = path.resolve(context.home);
  if (await realpath(repo) !== repo || await realpath(home) !== home) throw new Error("trusted roots must not traverse symlinks");
  const git = (...args: string[]) => execFileSync("git", ["-C", repo, ...args], { maxBuffer: 64 * 1024 * 1024 });
  if (git("rev-parse", "--show-toplevel").toString().trim() !== repo) throw new Error("invalid trusted Git root");
  const manifest = record(JSON.parse(await readFile(path.join(home, "trusted", "manifest.json"), "utf8")) as unknown, "trusted manifest");
  if (typeof manifest.commit !== "string" || !/^[a-f0-9]{40}$/.test(manifest.commit)
    || git("rev-parse", `${manifest.commit}^{commit}`).toString().trim() !== manifest.commit) {
    throw new Error("invalid trusted snapshot commit");
  }
  const files = record(manifest.files, "trusted snapshot files");
  const src = path.join(home, "trusted", "src");
  const hashes: Record<string, string> = {};
  for (const [file, pinned] of [[FIXTURE, FIXTURE_REVISION], [MAINTENANCE, FIXTURE_REVISION],
    [COMPARATOR, SUPPORT_REVISION], [SANDBOX, SUPPORT_REVISION]] as const) {
    const full = path.join(src, file);
    const stat = await lstat(full);
    if (!stat.isFile() || stat.isSymbolicLink() || await realpath(full) !== full) throw new Error(`invalid trusted input: ${file}`);
    const digest = sha256(await readFile(full));
    const fingerprint = record(files[file], `manifest entry ${file}`);
    if (fingerprint.sha256 !== digest || sha256(git("show", `${manifest.commit}:${file}`)) !== digest
      || sha256(git("show", `${pinned}:${file}`)) !== digest) {
      throw new Error(`trusted input differs from pinned source: ${file}`);
    }
    hashes[file] = digest;
  }
  return { repo, home, src, snapshotCommit: manifest.commit, hashes };
}

export async function dispatchMaintenanceControl(context: TrustedMaintenanceContext, request: ControlRequest): Promise<void> {
  const input = record(request, "maintenance request");
  if (Object.keys(input).sort().join(",") !== "experiments,mode" || input.mode !== "control"
    || !Array.isArray(input.experiments) || input.experiments.length < 1 || input.experiments.length > 2
    || new Set(input.experiments).size !== input.experiments.length
    || input.experiments.some((name: unknown) => name !== "maintenance-active" && name !== "maintenance-idle")) {
    throw new Error("only fixed same-source maintenance controls are enabled; native admission is unavailable");
  }
  const { repo, home, src, snapshotCommit, hashes } = await trustedInputs(context);
  const data = path.join(home, "data");
  await mkdir(data, { recursive: true });
  if (await realpath(data) !== data) throw new Error("trusted data root must not traverse symlinks");
  const prepared = await prepareMaintenance({
    repo, baseRevision: FIXTURE_REVISION,
    comparatorSource: path.join(src, COMPARATOR), sandboxSource: path.join(src, SANDBOX),
    output: path.join(data, "maintenance-control"),
  });
  if (prepared.bindings.baselineMaintenanceSha256 !== hashes[MAINTENANCE]
    || prepared.bindings.comparatorSourceSha256 !== hashes[COMPARATOR]
    || prepared.bindings.sandboxSourceSha256 !== hashes[SANDBOX]) {
    throw new Error("trusted inputs changed during preparation");
  }
  for (const name of request.experiments) {
    const lane: MaintenanceLane = name === "maintenance-active" ? "active" : "idle";
    const raw = await evaluateMaintenance(prepared, lane);
    const score = validateControlEvidence(raw, lane, prepared.bindings);
    console.log("@FEEDBACK Same-source local maintenance control only; no native candidate admission or promotion.");
    console.log(`@RESULT ${JSON.stringify({
      version: 1, experiment: name, tier: "tier1", objective: "maintenance-inverse-elapsed",
      scope: "local-file-maintenance-only", valid: 1, combined_score: score.ratio,
      log_uncertainty: score.logUncertainty, accepted: false, mode: "same-source-control",
      durable_rows_per_second: null, object_store_counts: "fixture-reported, not independent transient-write proof",
      admission: "unavailable", snapshot_commit: snapshotCommit, evidence: raw,
    })}`);
  }
}
