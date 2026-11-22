import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { cp, mkdir, mkdtemp, readFile, readdir, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { compareStopped, FIXTURE_REVISION, FIXTURE_SHA256, scoreMaintenancePairs } from "./maintenance-adapter.js";
import { dispatchMaintenanceControl, validateControlEvidence } from "./maintenance-dispatch.js";

const SUPPORT_REVISION = "f564ced0d38046f72c9b74f0f366a3657e146b7d";
const INPUTS = [
  "crates/arrow-deltalake/examples/darwin_maintenance.rs",
  "crates/arrow-deltalake/src/maintenance.rs",
  "crates/arrow-deltalake/examples/darwin_maintenance_verify.rs",
  "tools/darwin/src/maintenance-sandbox.rs",
];

function activePhysical(): Record<string, number> {
  return { rows: 17408, initial_files: 34, protected_files: 2, initial_bytes: 10, final_files: 4,
    final_bytes: 10, rewrite_commits: 1, rewrite_input_files: 32, rewrite_input_bytes: 10,
    rewrite_output_files: 2, rewrite_output_bytes: 10 };
}

function fixtureEvidence(): { result: Record<string, unknown>; bindings: Record<string, string> } {
  const digest = "a".repeat(64);
  const bindings: Record<string, string> = {
    baseRevision: FIXTURE_REVISION, fixtureSha256: FIXTURE_SHA256, seedSha256: digest,
    baselineMaintenanceSha256: digest, candidateMaintenanceSha256: digest, baselineBinarySha256: digest,
    candidateBinarySha256: digest, comparatorSourceSha256: digest, comparatorBinarySha256: digest,
    sandboxSourceSha256: digest, sandboxBinarySha256: digest, adapterSourceSha256: digest,
  };
  const sample = (arm: string) => ({ arm, elapsedSeconds: 1, inverseElapsedPerSecond: 1, physical: activePhysical() });
  const pairs = Array.from({ length: 3 }, () => ({ baseline: sample("baseline"), candidate: sample("candidate") }));
  const score = scoreMaintenancePairs(pairs.map(() => ({ baseline: 1, candidate: 1 })), false);
  return { bindings, result: {
    lane: "maintenance-active", storage: "local-file", scope: "maintenance-only",
    objective: "inverse full fixture process elapsed", accepted: false, ...score, bindings, pairs,
    audits: ["baseline", "candidate"].map((arm) => ({ arm, trace: `active-audit-${arm}.trace`, physical: activePhysical() })),
  } };
}

test("refuses changed-source and caller-supplied path requests before execution", async () => {
  const context = { repo: "/nonexistent", home: "/nonexistent" };
  await assert.rejects(Reflect.apply(dispatchMaintenanceControl, undefined, [context, {
    mode: "candidate", experiments: ["maintenance-active"],
  }]), /native admission is unavailable/);
  await assert.rejects(Reflect.apply(dispatchMaintenanceControl, undefined, [context, {
    mode: "control", experiments: ["maintenance-active"], candidateSource: "/tmp/source",
  }]), /native admission is unavailable/);
});

test("rejects absent, invalid or inconsistent physical evidence and scores", () => {
  const { result, bindings } = fixtureEvidence();
  assert.equal(validateControlEvidence(result, "active", bindings).ratio, 1);
  const pairs = result.pairs as Array<{ baseline: Record<string, unknown>; candidate: Record<string, unknown> }>;
  delete pairs[0].candidate.physical;
  assert.throws(() => validateControlEvidence(result, "active", bindings), /physical comparator evidence/);
  pairs[0].candidate.physical = { ...activePhysical(), protected_files: 1 };
  assert.throws(() => validateControlEvidence(result, "active", bindings), /physical comparator evidence/);
  pairs[0].candidate.physical = activePhysical();
  result.ratio = Number.POSITIVE_INFINITY;
  assert.throws(() => validateControlEvidence(result, "active", bindings), /invalid maintenance score/);
  delete result.audits;
  assert.throws(() => validateControlEvidence(result, "active", bindings), /cadence audit evidence/);
});

test("real trusted control and missing protected file", {
  skip: process.env.DARWIN_MAINTENANCE_E2E !== "1", timeout: 1_800_000,
}, async () => {
  const repo = path.resolve(".");
  const home = await mkdtemp(path.join(tmpdir(), "maintenance-dispatch-"));
  const files: Record<string, { sha256: string }> = {};
  for (const name of INPUTS) {
    const bytes = execFileSync("git", ["-C", repo, "show", `${SUPPORT_REVISION}:${name}`]);
    const destination = path.join(home, "trusted", "src", name);
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, bytes);
    files[name] = { sha256: createHash("sha256").update(bytes).digest("hex") };
  }
  await writeFile(path.join(home, "trusted", "manifest.json"), JSON.stringify({ commit: SUPPORT_REVISION, files }));
  const lines: string[] = [];
  const original = console.log;
  console.log = (line: string) => { lines.push(line); original(line); };
  try {
    await dispatchMaintenanceControl({ repo, home }, {
      mode: "control", experiments: ["maintenance-active", "maintenance-idle"],
    });
  } finally {
    console.log = original;
  }
  const results = lines.filter((line) => line.startsWith("@RESULT ")).map((line) => JSON.parse(line.slice(8)) as Record<string, unknown>);
  assert.equal(results.length, 2);
  for (const [index, result] of results.entries()) {
    assert.equal(result.experiment, ["maintenance-active", "maintenance-idle"][index]);
    assert.equal(result.tier, "tier1");
    assert.equal(result.scope, "local-file-maintenance-only");
    assert.equal(result.accepted, false);
    assert.equal(result.durable_rows_per_second, null);
    assert.equal(result.admission, "unavailable");
    assert.ok(typeof result.combined_score === "number" && Number.isFinite(result.combined_score));
  }
  const root = path.join(home, "data", "maintenance-control");
  const bindings = JSON.parse(await readFile(path.join(root, "bindings.json"), "utf8")) as Record<string, string>;
  const prepared = {
    root, bindings, seed: path.join(root, "seed"), baselineBinary: path.join(root, "artifacts", "baseline"),
    candidateBinary: path.join(root, "artifacts", "candidate"), comparatorBinary: path.join(root, "artifacts", "comparator"),
    sandboxBinary: path.join(root, "artifacts", "sandbox"),
  };
  const bad = path.join(root, "negative-missing-protected");
  await cp(path.join(root, "active-1-baseline"), bad, { recursive: true, force: false, errorOnExist: true });
  const protectedDirectory = path.join(bad, "active", "__arrowcol_ingestion=2024-01-01-00-02");
  const file = (await readdir(protectedDirectory)).find((name) => name.endsWith(".parquet"));
  assert.ok(file);
  await unlink(path.join(protectedDirectory, file));
  await assert.rejects(compareStopped(prepared, "active", bad), /No such file or directory/);
  console.log(`Dispatch control evidence: ${root}`);
});
