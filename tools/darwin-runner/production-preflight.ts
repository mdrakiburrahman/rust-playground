import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, mkdtemp, readFile, readdir, realpath, rm, rmdir } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { startTrustedBridgeListener, type TrustedBridgeListener } from "./bridge-listener.js";
import type { ConfigValidator, DispatchValidationContext, Workload } from "./dispatch-contract.js";

const EXPERIMENT = "compression-config";
const CONFIG_PATH = "experiments/darwin/candidate.oat.json";
const MAX_GIT_OUTPUT_BYTES = 64 * 1024 * 1024;
const MAX_VALIDATOR_OUTPUT_BYTES = 8 * 1024 * 1024;

interface SnapshotManifest {
  commit: string;
  files: Record<string, { sha256: string; executable: boolean }>;
  submodules: Record<string, string>;
}

interface CompressionPolicyModule {
  checkCompressionCandidate(baseline: unknown, candidate: unknown): number;
}

interface ExperimentPolicyModule {
  experimentPathsFor(experiment: string): readonly string[];
}

interface CandidatePathModule {
  checkCandidatePaths(
    root: string,
    baseline: SnapshotManifest["files"],
    allowed: readonly string[],
    submodules: SnapshotManifest["submodules"],
  ): Promise<string[]>;
}

export type MaintenanceExperiment = "maintenance-active" | "maintenance-idle";

export interface MaintenanceControlRequest {
  mode: "control";
  experiments: MaintenanceExperiment[];
}

export type MaintenanceControlAdapter = (
  context: { repo: string; home: string },
  request: MaintenanceControlRequest,
) => Promise<unknown>;

export interface ArrowColCompressionPreflightOptions {
  socketPath: string;
  repoRoot: string;
  darwinHome: string;
  startOneArm: Workload;
  maintenanceControlAdapter?: MaintenanceControlAdapter;
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function maintenanceControlRequest(value: unknown): MaintenanceControlRequest {
  const request = object(value, "maintenance control request");
  const names = request.experiments;
  if (Object.keys(request).sort().join(",") !== "experiments,mode" || request.mode !== "control"
    || !Array.isArray(names) || names.length < 1 || names.length > 2) {
    throw new Error("maintenance lane accepts only fixed maintenance control experiments");
  }
  const experiments: MaintenanceExperiment[] = [];
  for (const name of names) {
    if (name !== "maintenance-active" && name !== "maintenance-idle") {
      throw new Error("maintenance lane accepts only fixed maintenance control experiments");
    }
    if (experiments.includes(name)) throw new Error("maintenance lane does not accept duplicate experiments");
    experiments.push(name);
  }
  return { mode: "control", experiments };
}

function fullSha(value: unknown, label: string): string {
  if (typeof value !== "string" || !/^[0-9a-f]{40}$/.test(value)) {
    throw new Error(`${label} must be a full lowercase Git SHA`);
  }
  return value;
}

function cleanEnvironment(home: string): NodeJS.ProcessEnv {
  return {
    HOME: home,
    PATH: "/usr/local/bin:/usr/bin:/bin",
    LANG: "C.UTF-8",
    LC_ALL: "C.UTF-8",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
  };
}

function gitText(repo: string, args: string[], home: string): string {
  return execFileSync("git", ["-C", repo, ...args], {
    encoding: "utf8",
    env: cleanEnvironment(home),
    maxBuffer: MAX_GIT_OUTPUT_BYTES,
  }).trimEnd();
}

function gitBytes(repo: string, args: string[], home: string): Buffer {
  const output = execFileSync("git", ["-C", repo, ...args], {
    env: cleanEnvironment(home),
    maxBuffer: MAX_GIT_OUTPUT_BYTES,
  });
  return Buffer.isBuffer(output) ? output : Buffer.from(output);
}

function isInside(parent: string, child: string): boolean {
  const relative = path.relative(parent, child);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}

async function trustedModule<T>(trustedRoot: string, name: string): Promise<T> {
  const modulePath = path.join(trustedRoot, "compiled", name);
  const canonical = await realpath(modulePath);
  const metadata = await lstat(modulePath);
  if (!isInside(path.join(trustedRoot, "compiled"), canonical) || !metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`invalid trusted module: ${name}`);
  }
  return import(pathToFileURL(canonical).href) as Promise<T>;
}

async function verifySnapshotFile(
  trustedRoot: string,
  manifest: SnapshotManifest,
  relative: string,
): Promise<Buffer> {
  const entry = manifest.files[relative];
  if (!entry || !/^[0-9a-f]{64}$/.test(entry.sha256) || typeof entry.executable !== "boolean") {
    throw new Error(`trusted snapshot is missing a valid fingerprint for ${relative}`);
  }
  const file = path.join(trustedRoot, "src", ...relative.split("/"));
  const canonical = await realpath(file);
  const metadata = await lstat(file);
  if (!isInside(path.join(trustedRoot, "src"), canonical) || !metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`invalid trusted snapshot file: ${relative}`);
  }
  const bytes = await readFile(file);
  const digest = createHash("sha256").update(bytes).digest("hex");
  if (digest !== entry.sha256 || ((metadata.mode & 0o111) !== 0) !== entry.executable) {
    throw new Error(`trusted snapshot fingerprint changed: ${relative}`);
  }
  return bytes;
}

async function loadSnapshot(trustedRoot: string, repoRoot: string): Promise<{
  manifest: SnapshotManifest;
  producerRevision: string;
  baseline: Buffer;
}> {
  const rawManifest = object(JSON.parse(await readFile(path.join(trustedRoot, "manifest.json"), "utf8")), "Darwin snapshot manifest");
  const rawFiles = object(rawManifest.files, "Darwin snapshot files");
  const rawSubmodules = object(rawManifest.submodules, "Darwin snapshot submodules");
  const files: SnapshotManifest["files"] = {};
  for (const [relative, value] of Object.entries(rawFiles)) {
    const entry = object(value, `snapshot fingerprint for ${relative}`);
    if (typeof entry.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(entry.sha256) || typeof entry.executable !== "boolean") {
      throw new Error(`invalid snapshot fingerprint for ${relative}`);
    }
    files[relative] = { sha256: entry.sha256, executable: entry.executable };
  }
  const submodules: SnapshotManifest["submodules"] = {};
  for (const [relative, value] of Object.entries(rawSubmodules)) submodules[relative] = fullSha(value, `snapshot submodule ${relative}`);
  const manifest: SnapshotManifest = {
    commit: fullSha(rawManifest.commit, "Darwin snapshot commit"),
    files,
    submodules,
  };
  const rawProducer = object(JSON.parse(await readFile(path.join(trustedRoot, "producer.json"), "utf8")), "Darwin producer binding");
  const producerRevision = fullSha(rawProducer.revision, "Darwin producer revision");
  gitText(repoRoot, ["cat-file", "-e", `${manifest.commit}^{commit}`], os.tmpdir());
  gitText(repoRoot, ["cat-file", "-e", `${producerRevision}^{commit}`], os.tmpdir());
  gitText(repoRoot, ["merge-base", "--is-ancestor", producerRevision, manifest.commit], os.tmpdir());

  const baseline = await verifySnapshotFile(trustedRoot, manifest, CONFIG_PATH);
  const committedBaseline = gitBytes(repoRoot, ["show", `${manifest.commit}:${CONFIG_PATH}`], os.tmpdir());
  if (!baseline.equals(committedBaseline)) throw new Error("trusted compression seed differs from the pinned snapshot commit");
  for (const relative of ["tools/darwin/src/compression-policy.ts", "tools/darwin/src/paths.ts", "tools/darwin/src/policy.ts"]) {
    await verifySnapshotFile(trustedRoot, manifest, relative);
  }
  return { manifest, producerRevision, baseline };
}

async function validateRuntime(trustedRoot: string, producerRevision: string): Promise<{ root: string; validator: string }> {
  const runtime = path.join(trustedRoot, "runtime");
  if (await realpath(runtime) !== runtime
    || gitText(runtime, ["rev-parse", "--show-toplevel"], os.tmpdir()) !== runtime
    || gitText(runtime, ["rev-parse", "HEAD"], os.tmpdir()) !== producerRevision
    || gitText(runtime, ["status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"], os.tmpdir())) {
    throw new Error("trusted OAT runtime is not clean at the pinned producer revision");
  }
  const submodules = gitText(runtime, ["submodule", "status", "--recursive"], os.tmpdir()).split("\n");
  if (submodules.some((line) => line && !line.startsWith(" "))) {
    throw new Error("trusted OAT runtime has an unpinned submodule");
  }
  const validator = path.join(runtime, "target", "release", "arrowcol-oat-rust");
  const metadata = await lstat(validator);
  if (await realpath(validator) !== validator || !metadata.isFile() || metadata.isSymbolicLink()
    || (metadata.mode & 0o111) === 0) {
    throw new Error("pinned Rust OAT config validator is unavailable");
  }
  return { root: runtime, validator };
}

interface CachedSubmoduleWorktree {
  source: string;
  candidate: string;
}

function errorCode(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error
    && typeof error.code === "string" ? error.code : undefined;
}

async function initializeCachedSubmodules(
  candidateParent: string,
  sourceParent: string,
  home: string,
  worktrees: CachedSubmoduleWorktree[],
): Promise<void> {
  const modulesFile = path.join(candidateParent, ".gitmodules");
  let metadata;
  try {
    metadata = await lstat(modulesFile);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return;
    throw error;
  }
  if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error("candidate .gitmodules is not a regular file");

  const declarations = gitText(candidateParent, ["config", "--file", ".gitmodules", "--get-regexp", "^submodule\\..*\\.path$"], home)
    .split("\n").filter(Boolean);
  for (const declaration of declarations) {
    const match = /^(submodule\.(.+)\.path)\s+(.+)$/.exec(declaration);
    if (!match) throw new Error("candidate .gitmodules contains an invalid path entry");
    const name = match[2];
    const relative = match[3];
    if (relative.startsWith("/") || relative.includes("\\")
      || relative.split("/").some((part) => part === "" || part === "." || part === "..")) {
      throw new Error("candidate .gitmodules contains an unsafe path");
    }
    const treeEntry = gitText(candidateParent, ["ls-tree", "HEAD", "--", relative], home);
    const gitlink = /^160000 commit ([0-9a-f]{40})\t(.+)$/.exec(treeEntry);
    if (!gitlink || gitlink[2] !== relative) throw new Error(`candidate submodule is not pinned in Git: ${relative}`);
    const expected = gitlink[1];
    const sourcePath = path.join(sourceParent, ...relative.split("/"));
    const source = await realpath(sourcePath);
    if (source !== sourcePath
      || gitText(source, ["rev-parse", "--show-toplevel"], home) !== source
      || gitText(source, ["rev-parse", "HEAD"], home) !== expected
      || gitText(source, ["status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"], home)) {
      throw new Error(`pinned submodule is not available as a clean local worktree: ${relative}`);
    }

    const candidate = path.join(candidateParent, ...relative.split("/"));
    try {
      const entries = await readdir(candidate);
      if (entries.length !== 0) throw new Error(`candidate submodule mountpoint is not empty: ${relative}`);
      await rmdir(candidate);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    gitText(source, ["-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach", candidate, expected], home);
    worktrees.push({ source, candidate });
    await initializeCachedSubmodules(candidate, source, home, worktrees);
    if (gitText(candidate, ["rev-parse", "--show-toplevel"], home) !== candidate
      || gitText(candidate, ["rev-parse", "HEAD"], home) !== expected
      || gitText(candidate, ["status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"], home)) {
      throw new Error(`candidate submodule worktree is not clean at its pinned commit: ${relative}`);
    }
  }
}

function removeCandidateWorktrees(
  repoRoot: string,
  candidateTree: string,
  rootAdded: boolean,
  submodules: CachedSubmoduleWorktree[],
  home: string,
): void {
  const failures: Error[] = [];
  for (const worktree of [...submodules].reverse()) {
    try {
      gitText(worktree.source, ["-c", "core.hooksPath=/dev/null", "worktree", "remove", "--force", worktree.candidate], home);
    } catch (error) {
      failures.push(new Error(`failed to remove cached submodule worktree ${worktree.candidate}`, { cause: error }));
    }
  }
  if (rootAdded) {
    try {
      gitText(repoRoot, ["-c", "core.hooksPath=/dev/null", "worktree", "remove", "--force", candidateTree], home);
    } catch (error) {
      failures.push(new Error(`failed to remove candidate worktree ${candidateTree}`, { cause: error }));
    }
  }
  if (failures.length > 0) throw new AggregateError(failures, "candidate worktree cleanup failed");
}
function json(bytes: Buffer, label: string): unknown {
  try {
    return JSON.parse(bytes.toString("utf8")) as unknown;
  } catch {
    throw new Error(`${label} is not valid JSON`);
  }
}

export async function dispatchArrowColMaintenanceControl(
  options: Pick<ArrowColCompressionPreflightOptions, "repoRoot" | "darwinHome" | "maintenanceControlAdapter">,
  value: unknown,
): Promise<void> {
  const adapter = options.maintenanceControlAdapter;
  if (!adapter) throw new Error("maintenance lane is blocked: trusted maintenance adapter is not injected");
  const request = maintenanceControlRequest(value);
  const repo = await realpath(options.repoRoot);
  if (gitText(repo, ["rev-parse", "--show-toplevel"], os.tmpdir()) !== repo) {
    throw new Error("maintenance control repo must be a trusted Git root");
  }
  const home = await realpath(options.darwinHome);
  await adapter({ repo, home }, request);
}

export async function startArrowColCompressionPreflight(
  options: ArrowColCompressionPreflightOptions,
): Promise<TrustedBridgeListener> {
  for (const name of ["COPILOT_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"] as const) {
    if (process.env[name] !== undefined) {
      const credential = name === "COPILOT_GITHUB_TOKEN" ? "model token" : "GitHub credentials";
      throw new Error(`trusted ArrowCol preflight worker must not receive ${credential}`);
    }
  }
  const repoRoot = await realpath(options.repoRoot);
  if (gitText(repoRoot, ["rev-parse", "--show-toplevel"], os.tmpdir()) !== repoRoot) {
    throw new Error("trusted candidate source must be a Git repository root");
  }
  const darwinHome = await realpath(options.darwinHome);
  const trustedRoot = await realpath(path.join(darwinHome, "trusted"));
  const { manifest, producerRevision } = await loadSnapshot(trustedRoot, repoRoot);
  const runtime = await validateRuntime(trustedRoot, producerRevision);
  const [compression, experimentPolicy, pathPolicy] = await Promise.all([
    trustedModule<CompressionPolicyModule>(trustedRoot, "compression-policy.js"),
    trustedModule<ExperimentPolicyModule>(trustedRoot, "policy.js"),
    trustedModule<CandidatePathModule>(trustedRoot, "paths.js"),
  ]);
  const allowed = experimentPolicy.experimentPathsFor(EXPERIMENT);
  if (allowed.length !== 1 || allowed[0] !== CONFIG_PATH) {
    throw new Error("pinned compression lane does not have the expected single config allowlist");
  }

  const validateConfig: ConfigValidator = async (baselineBytes, candidateBytes, context: DispatchValidationContext) => {
    if (context.producerCommit !== manifest.commit || context.configPath !== CONFIG_PATH) {
      throw new Error("dispatch does not match the pinned ArrowCol snapshot");
    }
    const temporaryRoot = await mkdtemp(path.join(os.tmpdir(), "arrowcol-preflight-"));
    const candidateTree = path.join(temporaryRoot, "candidate");
    const env = cleanEnvironment(temporaryRoot);
    const submoduleWorktrees: CachedSubmoduleWorktree[] = [];
    let worktreeAdded = false;
    try {
      gitText(repoRoot, ["-c", "core.hooksPath=/dev/null", "worktree", "add", "--detach", candidateTree, context.candidateCommit], temporaryRoot);
      worktreeAdded = true;
      await initializeCachedSubmodules(candidateTree, repoRoot, temporaryRoot, submoduleWorktrees);
      if (gitText(candidateTree, ["rev-parse", "HEAD"], temporaryRoot) !== context.candidateCommit
        || gitText(candidateTree, ["status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none"], temporaryRoot)) {
        throw new Error("candidate worktree is not the exact clean committed tree");
      }
      const configFile = path.join(candidateTree, ...CONFIG_PATH.split("/"));
      const committedConfig = await readFile(configFile);
      if (!committedConfig.equals(candidateBytes)) throw new Error("candidate config differs from its committed Git blob");
      const pathProblems = await pathPolicy.checkCandidatePaths(candidateTree, manifest.files, allowed, manifest.submodules);
      if (pathProblems.length > 0) throw new Error(`candidate violates the pinned production path check: ${pathProblems.join(", ")}`);

      const baseline = json(baselineBytes, "pinned compression seed");
      const candidate = json(candidateBytes, "committed candidate config");
      const compressionLevel = compression.checkCompressionCandidate(baseline, candidate);
      execFileSync(runtime.validator, ["--validate-config", configFile], {
        cwd: runtime.root,
        env: cleanEnvironment(temporaryRoot),
        encoding: "utf8",
        maxBuffer: MAX_VALIDATOR_OUTPUT_BYTES,
        timeout: 30_000,
        stdio: ["ignore", "pipe", "pipe"],
      });
      return compressionLevel;
    } finally {
      try {
        removeCandidateWorktrees(repoRoot, candidateTree, worktreeAdded, submoduleWorktrees, temporaryRoot);
      } finally {
        await rm(temporaryRoot, { recursive: true, force: true });
      }
    }
  };

  return startTrustedBridgeListener({
    socketPath: options.socketPath,
    repoRoot,
    producerCommit: manifest.commit,
    configPath: CONFIG_PATH,
    validateConfig,
    startWorkload: options.startOneArm,
  });
}
