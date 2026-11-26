import { chmodSync, closeSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, readdirSync, realpathSync, writeFileSync } from "node:fs";
import { spawn, execFileSync } from "node:child_process";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { parseEnv } from "node:util";
import { fileURLToPath } from "node:url";
import { canonical, parseCandidateText, parsePressureAttestation, sha256 } from "./qap-contract.js";
import { readRegular } from "./qap-evaluator.js";

function run(command: string, args: string[], cwd: string): string {
  return execFileSync(command, args, { cwd, encoding: "utf8", timeout: 60_000, maxBuffer: 4_194_304 }).trim();
}

function write(path: string, data: string, mode = 0o600) {
  writeFileSync(path, data, { flag: "wx", mode });
}

export async function main(args: string[]) {
  if (args.length !== 2 && (args.length !== 4 || args[2] !== "--pressure-attestation")) {
    throw new Error("usage: qap-run.ts <absolute-darwin-binary> <new-out/qap-run-name> [--pressure-attestation <absolute-file>]");
  }
  const repo = realpathSync(process.cwd());
  if (run("git", ["rev-parse", "--show-toplevel"], repo) !== repo) throw new Error("run-from-repository-root");
  if (run("git", ["status", "--porcelain"], repo)) throw new Error("commit-clean-tree-before-evolution");
  const [binary, output] = args;
  let pressureText: string | null = null;
  if (args.length === 4) {
    const path = args[3];
    if (!isAbsolute(path) || realpathSync(path) !== path) throw new Error("pressure-attestation-path");
    const text = readRegular(path, 16_384);
    const attestation = parsePressureAttestation(JSON.parse(text));
    if (text !== `${canonical(attestation)}\n`) throw new Error("pressure-attestation-must-be-canonical");
    pressureText = text;
  }
  const pressureAttestationSha256 = pressureText === null ? null : sha256(pressureText);
  if (!isAbsolute(binary) || realpathSync(binary) !== binary || !lstatSync(binary).isFile()) throw new Error("binary-path");
  const copilotPath = process.env.COPILOT_CLI_PATH;
  if (copilotPath && (!isAbsolute(copilotPath) || realpathSync(copilotPath) !== copilotPath ||
      !lstatSync(copilotPath).isFile())) throw new Error("copilot-runtime-path");
  const libraryPath = process.env.DARWIN_LIBRARY_PATH;
  const runtimeLibraries: Record<string, string> = {};
  if (libraryPath) {
    if (!isAbsolute(libraryPath) || realpathSync(libraryPath) !== libraryPath ||
        !lstatSync(libraryPath).isDirectory()) throw new Error("runtime-library-path");
    const names = readdirSync(libraryPath).filter((name) => /\.so(?:\.|$)/.test(name)).sort();
    for (const required of ["libssl.so.3", "libcrypto.so.3", "libsqlite3.so.0", "libstdc++.so.6"]) {
      if (!names.includes(required)) throw new Error("runtime-library-missing");
    }
    for (const name of names) {
      const file = realpathSync(join(libraryPath, name));
      if (relative(libraryPath, file).startsWith(`..${sep}`) || !lstatSync(file).isFile()) throw new Error("runtime-library-escape");
      runtimeLibraries[name] = sha256(readFileSync(file));
    }
  }
  if (!/^out\/qap-[a-z0-9-]+$/.test(output)) throw new Error("output-must-be-new-out-qap-name");
  const root = resolve(repo, output);
  if (existsSync(root)) throw new Error("output-already-exists");
  if (!existsSync(join(repo, "out"))) mkdirSync(join(repo, "out"), { mode: 0o700 });
  if (realpathSync(join(repo, "out")) !== join(repo, "out")) throw new Error("unsafe-output-parent");
  const deadlineMs = Math.min(Date.now() + 20 * 60_000, Date.parse("2026-11-27T12:00:00Z"));
  if (deadlineMs - Date.now() < 300_000) throw new Error("campaign-restore-reserve");
  const sourceCommit = run("git", ["rev-parse", "HEAD"], repo);
  const committed = (path: string) => execFileSync("git", ["show", `${sourceCommit}:${path}`], { cwd: repo, encoding: "utf8" });
  const seedText = committed("experiments/darwin/qap-candidate.json");
  parseCandidateText(seedText);
  const yaml = committed(pressureText === null ? "experiments/darwin/qap-screening.yaml" :
    "experiments/darwin/qap-pressure-screening.yaml");
  if (yaml.split('"@TRUSTED_COMMAND@"').length !== 2) throw new Error("command-template");
  const dotenv = existsSync(join(repo, ".env")) ? parseEnv(readFileSync(join(repo, ".env"), "utf8")) : {};
  const token = process.env.COPILOT_GITHUB_TOKEN || dotenv.COPILOT_GITHUB_TOKEN;
  if (!token) throw new Error("copilot-token-missing-bootstrap-sync-required");
  const copilotHost = process.env.COPILOT_GH_HOST || dotenv.COPILOT_GH_HOST || "github.com";
  if (!/^(github\.com|[a-z0-9-]+\.ghe\.com)$/.test(copilotHost)) throw new Error("unsupported-copilot-auth-host");
  mkdirSync(root, { mode: 0o700 });
  const source = join(root, "source");
  const trusted = join(root, "trusted");
  const home = join(root, "home");
  const scratch = join(root, "scratch");
  for (const directory of [source, trusted, home, scratch]) mkdirSync(directory, { mode: 0o700 });
  write(join(root, "package.json"), '{"type":"module"}\n');
  write(join(source, "candidate.json"), seedText);
  run("git", ["init", "--initial-branch=seed", "--quiet"], source);
  run("git", ["config", "core.hooksPath", "/dev/null"], source);
  run("git", ["add", "candidate.json"], source);
  run("git", ["-c", "user.name=QAP screening", "-c", "user.email=qap-screening@localhost",
    "commit", "--quiet", "-m", `Sanitized candidate seed from ${sourceCommit}`], source);
  const seedCommit = run("git", ["rev-parse", "HEAD"], source);
  run("npx", ["--no-install", "tsc", "--ignoreConfig", "--target", "ES2022", "--module", "NodeNext",
    "--moduleResolution", "NodeNext", "--strict", "--skipLibCheck", "--types", "node",
    "--rootDir", "experiments/darwin", "--outDir", trusted,
    "experiments/darwin/qap-contract.ts", "experiments/darwin/qap-evaluator.ts"], repo);
  const files: Record<string, string> = {};
  for (const file of ["qap-contract.js", "qap-evaluator.js"]) {
    files[file] = sha256(readFileSync(join(trusted, file)));
    chmodSync(join(trusted, file), 0o444);
  }
  const receiptFile = join(root, "receipts.jsonl");
  write(receiptFile, "");
  if (pressureText !== null) write(join(trusted, "pressure-attestation.json"), pressureText, 0o444);
  const manifest = { schemaVersion: 1, files, receiptFile, deadlineMs, maxEvaluations: 16, sourceCommit, pressureAttestationSha256 };
  write(join(trusted, "manifest.json"), `${canonical(manifest)}\n`, 0o444);
  const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
  const command = [process.execPath, join(trusted, "qap-evaluator.js"), join(trusted, "manifest.json")].map(quote).join(" ");
  write(join(root, "screening.yaml"), yaml.replace('"@TRUSTED_COMMAND@"', JSON.stringify(command)), 0o444);
  chmodSync(trusted, 0o555);
  const metadata = {
    schemaVersion: 1, kind: "darwin-screening-run", sourceCommit, seedCommit,
    screeningMode: pressureText === null ? "topology" : "pressure-attested", pressureAttestationSha256,
    binarySha256: sha256(readFileSync(binary)), trustedManifestSha256: sha256(readFileSync(join(trusted, "manifest.json"))),
    copilotRuntimeSha256: copilotPath ? sha256(readFileSync(copilotPath)) : null,
    runtimeLibraries,
    candidateSeedSha256: sha256(seedText), deadlineMs, maxIterations: 4, maxEvaluations: 16,
    copilotHost,
    environment: ["PATH", "HOME", "TMPDIR", "XDG_CONFIG_HOME", "COPILOT_GITHUB_TOKEN", "COPILOT_GH_HOST", "COPILOT_AUTO_UPDATE",
      ...(copilotPath ? ["COPILOT_CLI_PATH"] : []), ...(libraryPath ? ["LD_LIBRARY_PATH"] : []),
      "DARWIN_CONFINE_AGENTS", "DARWIN_DISABLE_BANNER"],
    qualification: "screening-only; no Azure requests or measured performance",
  };
  write(join(root, "run.json"), `${canonical(metadata)}\n`);
  const logPath = join(root, "darwin.log");
  const log = openSync(logPath, "wx", 0o600);
  const started = Date.now();
  const child = spawn(binary, ["evolve", join(root, "screening.yaml"), "--repo", source, "--home", join(root, "state")], {
    cwd: source,
    detached: true,
    stdio: ["ignore", log, log],
    env: {
      PATH: process.env.PATH,
      HOME: home, TMPDIR: scratch, XDG_CONFIG_HOME: join(home, ".config"),
      COPILOT_GITHUB_TOKEN: token,
      COPILOT_GH_HOST: copilotHost, COPILOT_AUTO_UPDATE: "false",
      ...(copilotPath ? { COPILOT_CLI_PATH: copilotPath } : {}),
      ...(libraryPath ? { LD_LIBRARY_PATH: libraryPath } : {}),
      DARWIN_CONFINE_AGENTS: "1", DARWIN_DISABLE_BANNER: "1",
    },
  });
  const stop = (signal: NodeJS.Signals) => {
    if (child.pid) { try { process.kill(-child.pid, signal); } catch { /* Already exited. */ } }
  };
  let hardKill: NodeJS.Timeout | undefined;
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    stop("SIGTERM");
    hardKill = setTimeout(() => stop("SIGKILL"), 10_000);
  }, deadlineMs - Date.now());
  let exit: number | null = null;
  let errorCode: string | null = null;
  try {
    exit = await new Promise<number | null>((accept) => {
      child.once("error", () => { errorCode = "darwin-process-start-failed"; accept(null); });
      child.once("exit", (code) => accept(code));
    });
  } finally {
    clearTimeout(timer);
    if (hardKill) clearTimeout(hardKill);
    stop("SIGKILL");
    closeSync(log);
  }
  const receipts = readFileSync(receiptFile, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line));
  const proposals = receipts.filter((r) => r.valid === 1).sort((a, b) => b.combined_score - a.combined_score);
  if (proposals.length) write(join(root, "best-screening-plan.json"), `${canonical(proposals[0].proposalPlan)}\n`, 0o444);
  const afterCommit = run("git", ["rev-parse", "HEAD"], repo);
  const unchangedTree = afterCommit === sourceCommit && run("git", ["status", "--porcelain"], repo) === "";
  const unchangedRuntimeLibraries = !libraryPath ||
    (canonical(Object.keys(runtimeLibraries).sort()) ===
      canonical(readdirSync(libraryPath).filter((name) => /\.so(?:\.|$)/.test(name)).sort()) &&
      Object.entries(runtimeLibraries).every(([name, hash]) => sha256(readFileSync(join(libraryPath, name))) === hash));
  const coverage = proposals[0]?.coverage;
  const pressureComparisonComplete = pressureText === null ? null : Boolean(coverage?.baselinePresent &&
    coverage.matchedCapacityComparisons.length >= 1 && coverage.matchedCoreBudgets.length >= 1 &&
    coverage.autoscaleComparisons.length >= 1);
  const result = {
    ...metadata, exit, errorCode, timedOut, elapsedMs: Date.now() - started, unchangedTree, unchangedRuntimeLibraries,
    pressureComparisonComplete,
    evaluations: receipts.length, acceptedEvaluations: proposals.length,
    distinctPlans: new Set(proposals.map((r) => r.planSha256)).size,
    bestScreeningScore: proposals[0]?.combined_score ?? null,
    rawLogSha256: sha256(readFileSync(logPath)), receiptsSha256: sha256(readFileSync(receiptFile)),
  };
  write(join(root, "result.json"), `${canonical(result)}\n`, 0o444);
  console.log(JSON.stringify(result, null, 2));
  if (exit !== 0 || timedOut || !unchangedTree || !unchangedRuntimeLibraries || result.distinctPlans < 2) {
    throw new Error("darwin-run-not-confirmed-see-private-result-and-log");
  }
  if (pressureComparisonComplete === false) {
    throw new Error("pressure-run-missing-matched-comparison");
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : "qap-run-failed");
    process.exitCode = 1;
  });
}
