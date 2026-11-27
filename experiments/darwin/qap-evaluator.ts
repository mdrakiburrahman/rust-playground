import { constants, closeSync, fstatSync, lstatSync, openSync, readFileSync, readdirSync, realpathSync, writeSync } from "node:fs";
import { randomBytes, randomInt } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { canonical, parseCandidateText, parsePlan, parsePressureAttestation, proposalHash, record, screen, sha256 } from "./qap-contract.js";

export function readRegular(path: string, maxBytes: number): string {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.size > maxBytes || stat.nlink !== 1 || (stat.mode & 0o111) !== 0) {
      throw new Error("unsafe-file");
    }
    const text = readFileSync(fd, "utf8");
    if (Buffer.byteLength(text) > maxBytes) throw new Error("file-grew");
    return text;
  } finally { closeSync(fd); }
}

export function loadCandidate(directory: string, pressureAttested = false) {
  const root = resolve(directory);
  if (realpathSync(root) !== root || !lstatSync(root).isDirectory()) throw new Error("unsafe-candidate-root");
  const entries = readdirSync(root).sort();
  if (entries.join("\0") !== [".git", "candidate.json"].join("\0")) throw new Error("candidate-tree-mutated");
  if (lstatSync(join(root, ".git")).isSymbolicLink()) throw new Error("candidate-git-symlink");
  return parseCandidateText(readRegular(join(root, "candidate.json"), 16_384), pressureAttested);
}

function evaluate(manifestPath: string) {
  const trusted = dirname(realpathSync(manifestPath));
  if (trusted !== dirname(fileURLToPath(import.meta.url))) throw new Error("wrong-trust-root");
  const manifestText = readRegular(join(trusted, "manifest.json"), 65_536);
  const manifest = record(JSON.parse(manifestText), [
    "schemaVersion", "files", "receiptFile", "deadlineMs", "maxEvaluations", "sourceCommit", "pressureAttestationSha256",
  ]);
  if (manifest.schemaVersion !== 1 || typeof manifest.deadlineMs !== "number" ||
      Date.now() >= manifest.deadlineMs || manifest.maxEvaluations !== 16) throw new Error("evaluation-budget");
  const files = record(manifest.files, ["qap-contract.js", "qap-evaluator.js"]);
  for (const [name, expected] of Object.entries(files)) {
    if (sha256(readRegular(join(trusted, name), 1_048_576)) !== expected) throw new Error("trusted-code-mutated");
  }
  const pressureHash = manifest.pressureAttestationSha256;
  const pressureAttested = pressureHash !== null;
  if (pressureAttested) {
    if (typeof pressureHash !== "string" || !/^[a-f0-9]{64}$/.test(pressureHash)) throw new Error("pressure-binding");
    const text = readRegular(join(trusted, "pressure-attestation.json"), 16_384);
    if (sha256(text) !== pressureHash) throw new Error("pressure-attestation-mutated");
    parsePressureAttestation(JSON.parse(text));
  }
  const receiptFile = join(dirname(trusted), "receipts.jsonl");
  if (manifest.receiptFile !== receiptFile) throw new Error("receipt-path");
  const fd = openSync(receiptFile, constants.O_RDWR | constants.O_APPEND | constants.O_NOFOLLOW);
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size > 1_048_576) throw new Error("unsafe-receipt-file");
    const prior = readFileSync(fd, "utf8").split("\n").filter(Boolean);
    if (prior.length >= 16) throw new Error("evaluation-count-exhausted");
    const start = performance.now();
    let receipt: Record<string, unknown>;
    try {
      const candidate = loadCandidate(process.cwd(), pressureAttested);
      // Fresh private negative cases are generated after admission, never given to the evolution agent.
      const nonce = randomBytes(32).toString("hex");
      const privateCases = pressureAttested ? 36 : 24;
      for (let i = 0; i < privateCases; i++) {
        const bad = structuredClone(candidate) as unknown as Record<string, unknown>;
        if (pressureAttested && i % 3 === 2) {
          const p = structuredClone(candidate.proposals[0]);
          bad.proposals = [{ ...p, queryAccelerationCapacity: {
            clusterMaximumConcurrentOperations: 100, coreUtilizationCoefficient: 0.75,
          } }];
        } else if (i % 2 === 0) bad[`unknown_${randomBytes(8).toString("hex")}`] = randomInt(1, 100);
        else {
          const p = structuredClone(candidate.proposals[0]);
          p.cluster.autoscale.maxNodes = randomInt(9, 1000);
          bad.proposals = [p];
        }
        let rejected = false;
        try { parsePlan(bad, pressureAttested); } catch { rejected = true; }
        if (!rejected) throw new Error("private-admission-case-failed");
      }
      const screening = screen(candidate, pressureAttested);
      receipt = {
        schemaVersion: 1, kind: "screening", qualified: false, valid: 1,
        ...screening, planSha256: sha256(canonical(candidate)),
        proposalSha256s: candidate.proposals.map(proposalHash), proposalPlan: candidate,
        freshPrivateCases: privateCases, privateCaseNonceSha256: sha256(nonce), reasons: [],
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : "invalid-candidate";
      const reason = /^[a-z0-9-]+$/.test(message) ? message : "invalid-candidate";
      receipt = { schemaVersion: 1, kind: "screening", qualified: false, valid: 0, combined_score: 0, reasons: [reason] };
    }
    receipt.trustedManifestSha256 = sha256(manifestText);
    receipt.screeningMode = pressureAttested ? "pressure-attested" : "topology";
    receipt.pressureAttestationSha256 = pressureHash;
    receipt.elapsedMs = performance.now() - start;
    receipt.evaluation = prior.length + 1;
    writeSync(fd, `${canonical(receipt)}\n`);
    // Only sanitized data is returned. No raw candidate text or exception paths cross the boundary.
    console.log(`@RESULT ${canonical(receipt)}`);
  } finally { closeSync(fd); }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length !== 3) throw new Error("arguments");
    evaluate(process.argv[2]);
  } catch {
    console.error("trusted-evaluator-admission-failed");
    process.exitCode = 1;
  }
}
