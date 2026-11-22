import { createHash } from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { cp, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rmdir, writeFile } from "node:fs/promises";
import { dirname, join, relative, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

export type MaintenanceLane = "active" | "idle";

export const FIXTURE_REVISION = "f345860e51816c04c6ac3c3f42c7df35519d34b5";
export const FIXTURE_SHA256 = "bcbe8877128a60f2ba39da5cfb14e95205c983280cc319dc45bb12f894bc69b3";
const LOCK_SHA256 = "7fbc71f511e50ac954563d7556827a767f0f371cf7c09af05c35bf6b0a760df3";
const FIXTURE = "crates/arrow-deltalake/examples/darwin_maintenance.rs";
const VERIFY = "crates/arrow-deltalake/examples/darwin_maintenance_verify.rs";
const MAINTENANCE = "crates/arrow-deltalake/src/maintenance.rs";

export interface PrepareOptions {
  repo: string;
  baseRevision: string;
  comparatorSource: string;
  sandboxSource: string;
  output: string;
  // Only an already-admitted maintenance.rs is copied. This is not a native-code admission API.
  candidateSource?: string;
}

export interface PreparedMaintenance {
  root: string;
  seed: string;
  baselineBinary: string;
  candidateBinary: string;
  comparatorBinary: string;
  sandboxBinary: string;
  bindings: Record<string, string>;
}

interface Sample {
  arm: "baseline" | "candidate";
  elapsedSeconds: number;
  inverseElapsedPerSecond: number;
  physical: unknown;
  reportedObjectStoreCounts?: unknown;
}

function hash(bytes: string | Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

async function fileHash(file: string): Promise<string> {
  return hash(await readFile(file));
}

function run(file: string, args: string[], cwd: string, env: NodeJS.ProcessEnv, log?: string): string {
  const result = spawnSync(file, args, { cwd, env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024, stdio: ["ignore", "pipe", "pipe"] });
  if (log) writeFileSync(log, `${result.stdout ?? ""}\n${result.stderr ?? ""}`, { flag: "wx" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${file} failed: ${(result.stderr ?? "").slice(-8000)}`);
  return result.stdout;
}

function buildEnv(home: string, target: string): NodeJS.ProcessEnv {
  return {
    HOME: home, USER: "darwin", LANG: "C.UTF-8",
    PATH: `${dirname(process.execPath)}:/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/bin`,
    CARGO_HOME: process.env.CARGO_HOME ?? "/usr/local/cargo",
    RUSTUP_HOME: process.env.RUSTUP_HOME ?? "/usr/local/rustup",
    CARGO_TARGET_DIR: target,
  };
}

export async function treeHash(root: string, withMetadata = false): Promise<string> {
  const entries: string[] = [];
  const rootStat = await lstat(root, { bigint: true });
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("fixture root must be a directory");
  if (withMetadata) entries.push(`root ${rootStat.ino} ${rootStat.mtimeNs} ${rootStat.ctimeNs}`);
  async function visit(directory: string, prefix: string): Promise<void> {
    const names = (await readdir(directory)).sort();
    for (const name of names) {
      const file = join(directory, name);
      const rel = prefix ? `${prefix}/${name}` : name;
      const stat = await lstat(file, { bigint: true });
      const state = withMetadata ? ` ${stat.ino} ${stat.mtimeNs} ${stat.ctimeNs}` : "";
      if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) {
        throw new Error(`unexpected non-regular fixture entry: ${rel}`);
      }
      if (stat.isDirectory()) {
        entries.push(`d ${rel}${state}`);
        await visit(file, rel);
      } else {
        if (stat.nlink !== 1n) throw new Error(`hard-linked fixture file: ${rel}`);
        entries.push(`f ${rel} ${stat.size} ${await fileHash(file)}${state}`);
      }
    }
  }
  await visit(root, "");
  return hash(entries.join("\n"));
}

function isWithin(parent: string, child: string): boolean {
  const path = relative(parent, child);
  return path === "" || (!path.startsWith("..") && !path.startsWith("/"));
}

export async function prepareMaintenance(options: PrepareOptions): Promise<PreparedMaintenance> {
  if (!/^[a-f0-9]{40}$/.test(options.baseRevision)) throw new Error("base revision must be a full Git SHA");
  const repo = await realpath(options.repo);
  const root = resolve(options.output);
  if (isWithin(repo, root) || isWithin(root, repo)) throw new Error("output must be outside the source checkout");
  await mkdir(root, { mode: 0o700 });
  const home = join(root, "home");
  const target = join(root, "target");
  const source = join(root, "source");
  const artifacts = join(root, "artifacts");
  await mkdir(home);
  await mkdir(artifacts);
  const env = buildEnv(home, target);
  const git = (...args: string[]) => run("git", ["-C", repo, ...args], repo, env).trim();
  if (git("rev-parse", `${options.baseRevision}^{commit}`) !== options.baseRevision) throw new Error("base revision does not identify a commit");
  if (hash(run("git", ["-C", repo, "show", `${options.baseRevision}:${FIXTURE}`], repo, env)) !== FIXTURE_SHA256) {
    throw new Error("base does not contain the pinned fixture");
  }
  const comparatorSource = await realpath(options.comparatorSource);
  const sandboxSource = await realpath(options.sandboxSource);
  const comparatorSourceHash = await fileHash(comparatorSource);
  const sandboxSourceHash = await fileHash(sandboxSource);
  const candidateBytes = options.candidateSource ? await readFile(await realpath(options.candidateSource)) : undefined;
  git("worktree", "add", "--detach", source, options.baseRevision);
  run("git", ["-C", source, "submodule", "update", "--init", "--recursive"], source, env, join(root, "submodule-build.log"));
  if (await fileHash(join(source, FIXTURE)) !== FIXTURE_SHA256 || await fileHash(join(source, "Cargo.lock")) !== LOCK_SHA256) {
    throw new Error("fixture or lockfile does not match the pinned build");
  }
  const submodules = run("git", ["-C", source, "submodule", "status", "--recursive"], source, env);
  if (submodules.split("\n").some((line) => line && !line.startsWith(" "))) throw new Error("submodules are not pinned");
  const baselineSourceHash = await fileHash(join(source, MAINTENANCE));
  const candidateSourceHash = candidateBytes ? hash(candidateBytes) : baselineSourceHash;
  await cp(comparatorSource, join(source, VERIFY));
  const cargo = "/usr/local/cargo/bin/cargo";
  const buildArgs = ["build", "--locked", "--release", "--package", "arrow-deltalake",
    "--example", "darwin_maintenance", "--example", "darwin_maintenance_verify"];
  run(cargo, buildArgs, source, env, join(root, "baseline-build.log"));
  const builtFixture = join(target, "release", "examples", "darwin_maintenance");
  const baselineBinary = join(artifacts, "baseline");
  const candidateBinary = join(artifacts, "candidate");
  const comparatorBinary = join(artifacts, "comparator");
  const sandboxBinary = join(artifacts, "sandbox");
  await cp(builtFixture, baselineBinary);
  await cp(join(target, "release", "examples", "darwin_maintenance_verify"), comparatorBinary);
  if (candidateBytes && candidateSourceHash !== baselineSourceHash) {
    await writeFile(join(source, MAINTENANCE), candidateBytes);
    run(cargo, ["build", "--locked", "--release", "--package", "arrow-deltalake", "--example", "darwin_maintenance"], source, env, join(root, "candidate-build.log"));
  }
  await cp(builtFixture, candidateBinary);
  run("/usr/local/cargo/bin/rustc", ["--edition", "2021", "-D", "warnings", sandboxSource, "-o", sandboxBinary], source, env);
  if (await fileHash(join(source, FIXTURE)) !== FIXTURE_SHA256
    || await fileHash(join(source, "Cargo.lock")) !== LOCK_SHA256
    || await fileHash(join(source, VERIFY)) !== comparatorSourceHash
    || await fileHash(join(source, MAINTENANCE)) !== candidateSourceHash
    || await fileHash(comparatorSource) !== comparatorSourceHash
    || await fileHash(sandboxSource) !== sandboxSourceHash) throw new Error("build inputs changed during preparation");
  const bindings: Record<string, string> = {
    baseRevision: options.baseRevision, fixtureSha256: FIXTURE_SHA256, lockSha256: LOCK_SHA256,
    baselineMaintenanceSha256: baselineSourceHash, candidateMaintenanceSha256: candidateSourceHash,
    comparatorSourceSha256: comparatorSourceHash, sandboxSourceSha256: sandboxSourceHash,
    adapterSourceSha256: await fileHash(fileURLToPath(import.meta.url)),
    baselineBinarySha256: await fileHash(baselineBinary), candidateBinarySha256: await fileHash(candidateBinary),
    comparatorBinarySha256: await fileHash(comparatorBinary), sandboxBinarySha256: await fileHash(sandboxBinary),
    submodulesSha256: hash(submodules), submodules: submodules.trimEnd(),
    rustcVersion: run("/usr/local/cargo/bin/rustc", ["--version"], source, env).trim(),
    lldbVersion: run("/usr/bin/lldb", ["--version"], source, env).split("\n")[0],
    nodeVersion: process.version,
  };
  const seed = join(root, "seed");
  run(baselineBinary, ["seed", seed], source, env, join(root, "seed.log"));
  const prime = join(root, "prime");
  await cp(seed, prime, { recursive: true, force: false, errorOnExist: true });
  const initialized = await namespaceRun([sandboxBinary, baselineBinary, "idle", prime], prime);
  await writeFile(join(root, "prime.stdout"), initialized.stdout, { flag: "wx" });
  await writeFile(join(root, "prime.stderr"), initialized.stderr, { flag: "wx" });
  run(comparatorBinary, ["idle", seed, prime], root, env, join(root, "prime-comparator.log"));
  for (const name of ["empty", "quiescent", "protected"]) {
    const before = new Set(await readdir(join(seed, name)));
    const added = (await readdir(join(prime, name))).filter((entry) => !before.has(entry)).sort();
    if (JSON.stringify(added) !== JSON.stringify([".arrow-lake-writer.lock", "_arrowcol_maintenance"])) {
      throw new Error(`unexpected initial idle sidecars: ${name}`);
    }
    const sidecars = (await readdir(join(prime, name, "_arrowcol_maintenance"))).sort();
    if (JSON.stringify(sidecars) !== JSON.stringify([".lock", "vacuum-lite.json"])) {
      throw new Error(`unexpected initial vacuum sidecars: ${name}`);
    }
    for (const entry of added) await cp(join(prime, name, entry), join(seed, name, entry), { recursive: true, force: false, errorOnExist: true });
  }
  bindings.seedInitialization = "pinned seed plus trusted one-time idle sidecar initialization";
  bindings.seedSha256 = await treeHash(seed);
  await writeFile(join(root, "bindings.json"), `${JSON.stringify(bindings, null, 2)}\n`, { flag: "wx" });
  return { root, seed, baselineBinary, candidateBinary, comparatorBinary, sandboxBinary, bindings };
}

async function assertBindings(prepared: PreparedMaintenance): Promise<void> {
  const files: [string, string][] = [
    [prepared.baselineBinary, "baselineBinarySha256"], [prepared.candidateBinary, "candidateBinarySha256"],
    [prepared.comparatorBinary, "comparatorBinarySha256"], [prepared.sandboxBinary, "sandboxBinarySha256"],
  ];
  for (const [file, key] of files) {
    if (await fileHash(file) !== prepared.bindings[key]) throw new Error(`bound artifact changed: ${key}`);
  }
  if (await fileHash(fileURLToPath(import.meta.url)) !== prepared.bindings.adapterSourceSha256) throw new Error("adapter source changed");
  if (await treeHash(prepared.seed) !== prepared.bindings.seedSha256) throw new Error("seed changed");
}

function namespaceRun(args: string[], home: string): Promise<{ stdout: string; stderr: string; elapsedSeconds: number }> {
  return new Promise((resolvePromise, reject) => {
    const started = performance.now();
    const child = spawn("/usr/bin/unshare", ["-Urnmpf", "--mount-proc", ...args], {
      cwd: home, env: { HOME: home, PATH: "/usr/bin:/bin", LANG: "C.UTF-8" },
      stdio: ["ignore", "pipe", "pipe"], detached: true,
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let outputExceeded = false;
    function killGroup(): void {
      if (child.pid && child.exitCode === null) {
        try { process.kill(-child.pid, "SIGKILL"); } catch (error) {
          if (!(error instanceof Error && "code" in error && error.code === "ESRCH")) throw error;
        }
      }
    }
    const timer = setTimeout(() => {
      timedOut = true;
      killGroup();
    }, 120_000);
    child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); if (stdout.length > 1_000_000) { outputExceeded = true; killGroup(); } });
    child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); if (stderr.length > 1_000_000) { outputExceeded = true; killGroup(); } });
    child.once("error", (error) => { clearTimeout(timer); reject(error); });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      if (code !== 0 || timedOut || outputExceeded) reject(new Error(`isolated run failed: ${timedOut ? "timeout" : outputExceeded ? "output limit" : signal ?? code}: ${stderr.slice(-8000)}`));
      else resolvePromise({ stdout, stderr, elapsedSeconds: (performance.now() - started) / 1000 });
    });
  });
}

function quoted(value: string): string {
  if (/[\r\n]/.test(value)) throw new Error("newline in debugger path");
  return `"${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

export function checkCadence(lane: MaintenanceLane, trace: string, rewriteCommits: number): void {
  if (!/Process \d+ exited with status = 0 \(0x00000000\)/.test(trace)) throw new Error("traced fixture did not exit successfully");
  if (!/^Breakpoint 1: where = /m.test(trace)) throw new Error("first cadence symbol did not resolve uniquely");
  if (lane === "idle") {
    const observed = [...trace.matchAll(/^\s*rdx = 0x([0-9a-f]+)\s*$/gm)].map((match) => Number.parseInt(match[1], 16));
    const expected = [...Array.from({ length: 90 }, () => [0, 1, 2, 3, 4]).flat(), 0, 0, 1, 1, 1];
    if (JSON.stringify(observed) !== JSON.stringify(expected)) throw new Error("idle probe cadence differs");
  } else {
    if (!/^Breakpoint 2: where = /m.test(trace) || !Number.isSafeInteger(rewriteCommits) || rewriteCommits < 1 || rewriteCommits >= 64) {
      throw new Error("active cadence symbols or commit count invalid");
    }
    const observed = [...trace.matchAll(/^\s*rip = .*?::(prepare_optimize|apply_prepared_optimize)::h[0-9a-f]+/gm)].map((match) => match[1]);
    const expected = [...Array.from({ length: rewriteCommits }, () => ["prepare_optimize", "apply_prepared_optimize"]).flat(),
      "prepare_optimize", "prepare_optimize", "apply_prepared_optimize"];
    if (JSON.stringify(observed) !== JSON.stringify(expected)) throw new Error("active optimize cadence differs");
  }
}

async function auditCadence(prepared: PreparedMaintenance, lane: MaintenanceLane, armName: "baseline" | "candidate"): Promise<unknown> {
  const arm = join(prepared.root, `${lane}-audit-${armName}`);
  await cp(prepared.seed, arm, { recursive: true, force: false, errorOnExist: true });
  if (await treeHash(arm) !== prepared.bindings.seedSha256) throw new Error("audit input is not the fixed seed");
  const idleBefore = lane === "idle" ? await idleStates(arm) : undefined;
  const binary = armName === "baseline" ? prepared.baselineBinary : prepared.candidateBinary;
  const script = join(prepared.root, `${lane}-audit-${armName}.lldb`);
  const childStdout = join(prepared.root, `${lane}-audit-${armName}.stdout`);
  const childStderr = join(prepared.root, `${lane}-audit-${armName}.stderr`);
  const commands = [
    "settings set target.disable-aslr false", "settings set target.process.stop-on-exec true",
    `target create ${quoted(prepared.sandboxBinary)}`,
    `process launch --stdout ${quoted(childStdout)} --stderr ${quoted(childStderr)} -- ${quoted(binary)} ${lane} ${quoted(arm)}`,
  ];
  if (lane === "idle") {
    commands.push("breakpoint set --func-regex probe_no_work_with_inventory::h[0-9a-f]+$",
      "breakpoint command add 1", "register read rdx", "continue", "DONE");
  } else {
    commands.push("breakpoint set --func-regex prepare_optimize::h[0-9a-f]+$",
      "breakpoint set --func-regex apply_prepared_optimize::h[0-9a-f]+$",
      "breakpoint command add 1", "register read rip", "continue", "DONE",
      "breakpoint command add 2", "register read rip", "continue", "DONE");
  }
  commands.push("process continue", "breakpoint list");
  await writeFile(script, `${commands.join("\n")}\n`, { flag: "wx" });
  const observed = await namespaceRun(["/usr/bin/lldb", "--no-lldbinit", "--batch", "-s", script], arm);
  await writeFile(join(prepared.root, `${lane}-audit-${armName}.trace`), observed.stdout, { flag: "wx" });
  await writeFile(join(prepared.root, `${lane}-audit-${armName}.debugger-stderr`), observed.stderr, { flag: "wx" });
  const physical = await compareStopped(prepared, lane, arm) as { rewrite_commits?: number };
  if (idleBefore && JSON.stringify(await idleStates(arm)) !== JSON.stringify(idleBefore)) {
    throw new Error("idle audit changed physical file identity or timestamps");
  }
  checkCadence(lane, observed.stdout, physical.rewrite_commits ?? 0);
  return { arm: armName, physical, trace: `${lane}-audit-${armName}.trace` };
}

async function idleStates(arm: string): Promise<string[]> {
  return Promise.all(["empty", "quiescent", "protected"].map((name) => treeHash(join(arm, name), true)));
}

export async function compareStopped(prepared: PreparedMaintenance, lane: MaintenanceLane, arm: string): Promise<unknown> {
  if (lane !== "active" && lane !== "idle") throw new Error("unsupported maintenance lane");
  if (dirname(resolve(arm)) !== resolve(prepared.root) || await realpath(arm) !== resolve(arm)) {
    throw new Error("stopped arm must be a direct, non-symlinked child of the evidence root");
  }
  await assertBindings(prepared);
  const stoppedHash = await treeHash(arm);
  const inspectionRoot = await mkdtemp(join(prepared.root, "inspection-"));
  const inspection = join(inspectionRoot, "snapshot");
  await cp(arm, inspection, { recursive: true, force: false, errorOnExist: true });
  if (await treeHash(inspection) !== stoppedHash) throw new Error("inspection copy differs from stopped arm");
  const env = buildEnv(join(prepared.root, "home"), join(prepared.root, "target"));
  const output = run(prepared.comparatorBinary, [lane, prepared.seed, inspection], prepared.root, env);
  if (await treeHash(arm) !== stoppedHash) throw new Error("stopped arm changed during comparison");
  await assertBindings(prepared);
  if (lane === "idle") {
    for (const name of ["empty", "quiescent", "protected"]) {
      if (await treeHash(join(prepared.seed, name)) !== await treeHash(join(arm, name))) {
        throw new Error(`idle physical snapshot changed: ${name}`);
      }
    }
  }
  return JSON.parse(output) as unknown;
}

function objectStoreCounts(stdout: string): unknown {
  const result = JSON.parse(stdout) as { cases?: Array<Record<string, unknown>> };
  if (!Array.isArray(result.cases) || result.cases.length !== 3) throw new Error("idle telemetry missing");
  return result.cases.map((entry) => ({
    case: entry.case, get: entry.store_get_calls, list: entry.store_list_calls, write: entry.store_write_calls,
    claimedPasses: entry.passes, claimedProbeInvocations: entry.probe_invocations,
  }));
}

export async function evaluateMaintenance(prepared: PreparedMaintenance, lane: MaintenanceLane): Promise<unknown> {
  if (lane !== "active" && lane !== "idle") throw new Error("unsupported maintenance lane");
  await assertBindings(prepared);
  const lock = join(prepared.root, "evaluating");
  await mkdir(lock);
  const audits = [
    await auditCadence(prepared, lane, "baseline"),
    await auditCadence(prepared, lane, "candidate"),
  ];
  const pairs: Array<{ baseline: Sample; candidate: Sample }> = [];
  for (let pair = 0; pair < 3; pair++) {
    const order: Array<"baseline" | "candidate"> = pair === 1 ? ["candidate", "baseline"] : ["baseline", "candidate"];
    const samples = new Map<string, Sample>();
    for (const armName of order) {
      const arm = join(prepared.root, `${lane}-${pair + 1}-${armName}`);
      await cp(prepared.seed, arm, { recursive: true, force: false, errorOnExist: true });
      if (await treeHash(arm) !== prepared.bindings.seedSha256) throw new Error("arm is not a fresh seed copy");
      const idleBefore = lane === "idle" ? await idleStates(arm) : undefined;
      const binary = armName === "baseline" ? prepared.baselineBinary : prepared.candidateBinary;
      const observed = await namespaceRun([prepared.sandboxBinary, binary, lane, arm], arm);
      await writeFile(join(prepared.root, `${lane}-${pair + 1}-${armName}.stdout`), observed.stdout, { flag: "wx" });
      await writeFile(join(prepared.root, `${lane}-${pair + 1}-${armName}.stderr`), observed.stderr, { flag: "wx" });
      const physical = await compareStopped(prepared, lane, arm);
      if (idleBefore && JSON.stringify(await idleStates(arm)) !== JSON.stringify(idleBefore)) {
        throw new Error("idle run changed physical file identity or timestamps");
      }
      const sample: Sample = {
        arm: armName, elapsedSeconds: observed.elapsedSeconds,
        inverseElapsedPerSecond: 1 / observed.elapsedSeconds, physical,
      };
      if (lane === "idle") sample.reportedObjectStoreCounts = objectStoreCounts(observed.stdout);
      samples.set(armName, sample);
    }
    const baseline = samples.get("baseline");
    const candidate = samples.get("candidate");
    if (!baseline || !candidate) throw new Error("incomplete maintenance pair");
    pairs.push({ baseline, candidate });
  }
  const ratio = Math.exp(pairs.reduce((sum, pair) => sum + Math.log(pair.baseline.elapsedSeconds / pair.candidate.elapsedSeconds), 0) / pairs.length);
  const result = {
    lane: `maintenance-${lane}`, storage: "local-file", scope: "maintenance-only",
    objective: "inverse full fixture process elapsed", ratio, pairs, audits, bindings: prepared.bindings,
    accepted: false, qualification: "three-pair screening only; not durable ingestion",
    objectStoreCounts: "self-reported fixture instrumentation, not independent write proof",
  };
  await writeFile(join(prepared.root, `${lane}-result.json`), `${JSON.stringify(result, null, 2)}\n`, { flag: "wx" });
  await rmdir(lock);
  return result;
}
