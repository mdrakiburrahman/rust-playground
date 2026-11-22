import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { cp, mkdir, mkdtemp, open, readFile, readdir, realpath, rmdir, writeFile } from "node:fs/promises";
import { userInfo } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { checkCompressionCandidate } from "./compression-policy.js";
import { readReconciledOatResult } from "./oat-result.js";
import { artifactHash, hashTsx, type IdentityTools } from "./identity-reconcile.js";
import type { SnapshotManifest } from "./snapshot.js";

type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

type ReplayIdentity = Pick<Awaited<ReturnType<typeof readReconciledOatResult>>, "images" | "clientEvidence"> & { binaryHash: string };

export function assertMatchingReplayInputs(baseline: ReplayIdentity, candidate: ReplayIdentity): void {
  if (baseline.binaryHash !== candidate.binaryHash
    || JSON.stringify(baseline.images) !== JSON.stringify(candidate.images)
    || baseline.clientEvidence.payloadCorpusSha256 !== candidate.clientEvidence.payloadCorpusSha256) {
    throw new Error("baseline and candidate did not use identical OAT/workload binaries and business corpus");
  }
}

export function localReplayOptions(raw: unknown, bindHost: string, endpointHost: string, snapshotRoot: string): JsonObject {
  const local = object(raw, "local platform");
  if (!Number.isSafeInteger(local.collector_memory_limit_bytes) || Number(local.collector_memory_limit_bytes) <= 0) {
    throw new Error("local replay requires a frozen collector memory limit");
  }
  return { ...local, bind_host: bindHost, endpoint_host: endpointHost, snapshot_root: snapshotRoot };
}

export function cleanEnv(home: string, stackBytes: number): NodeJS.ProcessEnv {
  return {
    HOME: home, USER: userInfo().username, LANG: "C.UTF-8",
    PATH: `${dirname(process.execPath)}:/usr/local/cargo/bin:/usr/local/bin:/usr/bin:/bin`,
    CARGO_HOME: "/usr/local/cargo", RUSTUP_HOME: "/usr/local/rustup",
    RUST_MIN_STACK: String(stackBytes),
  };
}

function git(workspace: string, env: NodeJS.ProcessEnv, ...args: string[]): string {
  return execFileSync("git", ["-C", workspace, ...args], { encoding: "utf8", env, maxBuffer: 64 * 1024 * 1024 }).trimEnd();
}

export function hasPinnedSubmodules(workspace: string, env: NodeJS.ProcessEnv): boolean {
  return !git(workspace, env, "submodule", "status", "--recursive").split("\n").some(line => line && !line.startsWith(" "));
}

async function checkDependencyLinks(runtime: string, directory: string): Promise<void> {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const target = join(directory, entry.name);
    if (entry.isSymbolicLink()) {
      const resolved = await realpath(target);
      if (resolved !== runtime && !resolved.startsWith(`${runtime}/`)) throw new Error("trusted dependency points outside the isolated runtime");
    } else if (entry.isDirectory()) {
      await checkDependencyLinks(runtime, target);
    } else if (!entry.isFile()) {
      throw new Error("trusted dependencies contain a non-file");
    }
  }
}

async function prepareRuntime(repo: string, home: string, manifest: SnapshotManifest, producerRevision: string, stackBytes: number): Promise<string> {
  const runtime = join(home, "trusted", "runtime");
  const preparationHome = join(home, "trusted", "prepare-home");
  await mkdir(preparationHome, { recursive: true });
  const env = cleanEnv(preparationHome, stackBytes);
  if (!/^[a-f0-9]{40}$/.test(producerRevision)) throw new Error("producer revision must be a full Git SHA");
  git(repo, env, "merge-base", "--is-ancestor", producerRevision, manifest.commit);
  try {
    await realpath(runtime);
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    git(repo, env, "worktree", "add", "--detach", runtime, producerRevision);
    git(runtime, env, "submodule", "update", "--init", "--recursive");
    await cp(join(repo, "node_modules"), join(runtime, "node_modules"), { recursive: true, dereference: false, verbatimSymlinks: true });
  }
  if (git(runtime, env, "rev-parse", "--show-toplevel") !== runtime
    || git(runtime, env, "rev-parse", "HEAD") !== producerRevision
    || git(runtime, env, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
    || !hasPinnedSubmodules(runtime, env)) {
    throw new Error("trusted OAT runtime is not clean at the frozen source revision");
  }
  await checkDependencyLinks(runtime, join(runtime, "node_modules"));
  return runtime;
}

export async function runOat(runtime: string, configPath: string, home: string, toolDir: string, logPath: string, stackBytes: number, parity = false): Promise<void> {
  const log = await open(logPath, "wx");
  try {
    const env = cleanEnv(home, stackBytes);
    if (parity) env.OAT_DARWIN_PARITY_PRECHECK = "1";
    env.PATH = `${toolDir}:${env.PATH}`;
    const child = spawn(process.execPath, ["--import", "tsx", "projects/arrowcol-oat/cli.ts",
      "--config", configPath, "--mode", "local", "--no-viewer"], {
      cwd: runtime, env, stdio: ["ignore", log.fd, log.fd],
    });
    const timedOut = { value: false };
    const timer = setTimeout(() => { timedOut.value = true; child.kill("SIGTERM"); }, 6 * 60 * 60 * 1000);
    try {
      const code = await new Promise<number>((resolve, reject) => {
        child.once("error", reject);
        child.once("exit", (status, signal) => {
          if (status === 0 && !timedOut.value) resolve(status);
          else reject(new Error(`OAT run failed (${timedOut.value ? "deadline" : signal ?? status}); inspect ${logPath} and owned-cell recovery before retrying`));
        });
      });
      if (code !== 0) throw new Error("OAT run did not succeed");
    } finally { clearTimeout(timer); }
  } finally { await log.close(); }
}

export async function sha256(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

export async function prepareReplayRuntime(repo: string, home: string, stackBytes: number): Promise<{ runtime: string; binaryHash: string }> {
  const manifest = JSON.parse(await readFile(join(home, "trusted", "manifest.json"), "utf8")) as SnapshotManifest;
  const producer = object(JSON.parse(await readFile(join(home, "trusted", "producer.json"), "utf8")), "producer binding");
  if (typeof producer.revision !== "string" || !Number.isSafeInteger(stackBytes)
    || stackBytes < 2 * 1024 * 1024 || stackBytes > 128 * 1024 * 1024) throw new Error("invalid producer or controller stack setting");
  const runtime = await prepareRuntime(repo, home, manifest, producer.revision, stackBytes);
  const output = execFileSync(join(home, "trusted", "bin", "cargo"), [
    "build", "--locked", "--package", "arrowcol-oat-rust", "--bin", "arrowcol-oat-rust", "--message-format=json-render-diagnostics",
  ], {
    cwd: runtime, env: cleanEnv(join(home, "trusted", "prepare-home"), stackBytes),
    encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "inherit"],
  });
  const executables: string[] = [];
  for (const line of output.split("\n").filter(Boolean)) {
    const entry = object(JSON.parse(line), "Cargo message");
    if (entry.reason === "compiler-artifact" && entry.manifest_path === join(runtime, "projects", "arrowcol-oat-rust", "Cargo.toml")
      && typeof entry.executable === "string" && object(entry.target, "Cargo target").name === "arrowcol-oat-rust") {
      executables.push(await realpath(entry.executable));
    }
  }
  const expected = join(runtime, "target", "release", "arrowcol-oat-rust");
  if (executables.length !== 1 || executables[0] !== expected) throw new Error("Cargo did not select the pinned release controller");
  if (git(runtime, cleanEnv(join(home, "trusted", "prepare-home"), stackBytes), "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")) {
    throw new Error("trusted OAT source changed while building the controller");
  }
  return { runtime, binaryHash: await sha256(expected) };
}

async function prepareIdentityTools(
  home: string, runtime: string, manifest: SnapshotManifest, stackBytes: number,
): Promise<IdentityTools> {
  const env = cleanEnv(join(home, "trusted", "prepare-home"), stackBytes);
  const verifier = join(home, "trusted", "src", "tools", "darwin", "src", "delivery-identity-cli.ts");
  for (const name of ["delivery-identity.ts", "delivery-identity-cli.ts"]) {
    const path = join(home, "trusted", "src", "tools", "darwin", "src", name);
    if (await sha256(path) !== manifest.files[`tools/darwin/src/${name}`]?.sha256) {
      throw new Error("identity verifier differs from trusted source");
    }
  }
  for (const [pkg, bin] of [["collector-chain", "delivery-identity-reader"], ["arrowcol-oat-rust", "oat-manifest-extract"]]) {
    execFileSync("/usr/local/cargo/bin/cargo", ["build", "--locked", "--release", "--package", pkg!, "--bin", bin!], {
      cwd: runtime, env, stdio: "inherit",
    });
  }
  const decoder = join(runtime, "target", "release", "delivery-identity-reader");
  const extractor = join(runtime, "target", "release", "oat-manifest-extract");
  const tsxRoot = await realpath(join(runtime, "node_modules", "tsx"));
  const node = await realpath(process.execPath);
  return {
    node, nodeSha256: await sha256(node), tsxRoot, tsxSha256: await hashTsx(tsxRoot),
    decoder, decoderSha256: await sha256(decoder), extractor, extractorSha256: await sha256(extractor),
    verifier, verifierArtifactSha256: await artifactHash(verifier), runtime, env,
  };
}

export async function replayCompressionPair(
  repo: string, home: string, candidate: unknown, bindHost: string, endpointHost: string, stackBytes: number, baselineOnly = false,
): Promise<JsonObject> {
  if (!/^[0-9a-fA-F:.]+$/.test(bindHost) || !/^[0-9a-fA-F:.]+$/.test(endpointHost)) {
    throw new Error("local bind and endpoint hosts must be explicit IP addresses");
  }
  const producer = object(JSON.parse(await readFile(join(home, "trusted", "producer.json"), "utf8")), "producer binding");
  if (typeof producer.revision !== "string" || !Number.isSafeInteger(stackBytes)
    || stackBytes < 2 * 1024 * 1024 || stackBytes > 128 * 1024 * 1024) throw new Error("invalid producer or controller stack setting");
  const seed = JSON.parse(await readFile(join(home, "trusted", "src", "experiments", "darwin", "candidate.oat.json"), "utf8")) as unknown;
  const manifest = JSON.parse(await readFile(join(home, "trusted", "manifest.json"), "utf8")) as SnapshotManifest;
  if (producer.revision !== manifest.commit) throw new Error("identity verifier requires the trusted producer revision");
  const seedSha256 = await sha256(join(home, "trusted", "src", "experiments", "darwin", "candidate.oat.json"));
  const candidateLevel = checkCompressionCandidate(seed, candidate);
  const baselineLevel = checkCompressionCandidate(seed, seed);
  if (baselineOnly && candidateLevel !== baselineLevel) throw new Error("baseline-only smoke requires the frozen seed");
  const seedClient = object(object(object(seed, "seed").tunables, "tunables").client, "client");
  if (seedClient.data_profile !== "realistic" || seedClient.seed !== 20260923) throw new Error("realistic corpus identity is not pinned");
  const data = join(home, "data");
  await mkdir(data, { recursive: true });
  const lock = join(data, "evaluating");
  await mkdir(lock);
  let completed = false;
  try {
    const { runtime, binaryHash: preparedBinaryHash } = await prepareReplayRuntime(repo, home, stackBytes);
    const identityTools = await prepareIdentityTools(home, runtime, manifest, stackBytes);
    const results = [];
    const arms: [string, unknown, number][] = [["baseline", seed, baselineLevel]];
    if (!baselineOnly) arms.push(["candidate", candidate, candidateLevel]);
    for (const [label, raw, level] of arms) {
      const runRoot = await mkdtemp(join(data, `${label}-`));
      const runHome = join(runRoot, "home");
      await mkdir(runHome);
      const config = object(structuredClone(raw), "run config");
      const platform = object(config.platform, "platform");
      if (platform.kind !== "local" || !Array.isArray(config.tables) || config.tables.length !== 1
        || config.tables[0] !== "sqlserver_availability_replica_states") throw new Error("unsupported replay topology or fixture");
      platform.compose_state_root = join(runRoot, "state");
      platform.local = localReplayOptions(platform.local, bindHost, endpointHost, join(runRoot, "snapshots"));
      config.result_url = pathToFileURL(join(runRoot, "results.parquet")).href;
      config.website_url = pathToFileURL(join(runRoot, "site", "results.parquet")).href;
      const configPath = join(runRoot, "config.json");
      await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`, { flag: "wx", mode: 0o400 });
      const configHash = await sha256(configPath);
      await runOat(runtime, configPath, runHome, join(home, "trusted", "bin"), join(runRoot, "run.log"), stackBytes);
      if (await sha256(configPath) !== configHash) throw new Error("OAT runtime config changed during the run");
      const result = await readReconciledOatResult(
        join(runRoot, "results.parquet"), join(runRoot, "snapshots"), "sqlserver_availability_replica_states", producer.revision, level, seedClient.data_profile,
        undefined, { tools: identityTools, outputDir: runRoot },
      );
      const binaryHash = await sha256(join(runtime, "target", "release", "arrowcol-oat-rust"));
      if (binaryHash !== preparedBinaryHash) throw new Error("OAT release controller changed after preparation");
      const env = cleanEnv(runHome, stackBytes);
      if (git(runtime, env, "rev-parse", "HEAD") !== producer.revision
        || git(runtime, env, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")) {
        throw new Error("trusted source changed during OAT run");
      }
      results.push({ ...result, binaryHash, configHash, runRoot });
    }
    const [baseline, candidateResult] = results;
    if (baselineOnly && baseline) {
      completed = true;
      return {
        source_revision: producer.revision, trusted_revision: manifest.commit, seed_sha256: seedSha256,
        controller_stack_bytes: stackBytes, profile: "realistic-seed-20260923-v9", baseline_level: baselineLevel,
        baseline,
      };
    }
    if (!baseline || !candidateResult) throw new Error("baseline and candidate are required");
    assertMatchingReplayInputs(baseline, candidateResult);
    const ratio = candidateResult.measurement.durableRowsPerSecond / baseline.measurement.durableRowsPerSecond;
    completed = true;
    return {
      source_revision: producer.revision, trusted_revision: manifest.commit, seed_sha256: seedSha256,
      controller_stack_bytes: stackBytes, profile: "realistic-seed-20260923-v9", baseline_level: baselineLevel,
      candidate_level: candidateLevel, paired_rate_ratio: ratio,
      bytes_guard_passed: candidateResult.measurement.parquetBytesPerRow <= baseline.measurement.parquetBytesPerRow * 1.1,
      baseline, candidate: candidateResult,
    };
  } finally {
    if (completed) await rmdir(lock);
  }
}
