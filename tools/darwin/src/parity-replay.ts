import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, readFile, realpath, rmdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { readReconciledOatResult } from "./oat-result.js";
import { inspectParityHost, parityLadderConfig, type ParityProfile } from "./parity-preflight.js";
import { parityImageIds } from "./image-binding.js";
export { parityImageIds } from "./image-binding.js";
import { cleanEnv, hasPinnedSubmodules, localReplayOptions, prepareReplayRuntime, runOat, sha256 } from "./replay.js";
import type { SnapshotManifest } from "./snapshot.js";

type JsonObject = Record<string, unknown>;

export interface CollectorImageOverride {
  imageId: string;
  sourceRevision: string;
}

export function parseCollectorCpus(value: string): number[] {
  if (!/^(0|[1-9]\d*)(,(0|[1-9]\d*))*$/.test(value)) throw new Error("collector CPUs must be an ordered comma-separated list");
  const cpus = value.split(",").map(Number);
  if (cpus.some((cpu, index) => !Number.isSafeInteger(cpu) || cpu > 4095 || (index > 0 && cpu <= cpus[index - 1]!))) {
    throw new Error("collector CPUs must be an ordered unique list in 0..4095");
  }
  return cpus;
}

function linuxCpus(value: string): number[] {
  const cpus: number[] = [];
  for (const part of value.split(",")) {
    const match = /^(0|[1-9]\d*)(?:-(0|[1-9]\d*))?$/.exec(part);
    if (!match) throw new Error("invalid Linux CPU allocation");
    const first = Number(match[1]);
    const last = Number(match[2] ?? match[1]);
    if (!Number.isSafeInteger(first) || !Number.isSafeInteger(last) || first > last || last > 4095) {
      throw new Error("invalid Linux CPU allocation");
    }
    for (let cpu = first; cpu <= last; cpu++) cpus.push(cpu);
  }
  if (new Set(cpus).size !== cpus.length) throw new Error("duplicate Linux CPU allocation");
  return cpus.sort((left, right) => left - right);
}

export function parityCpuAllocation(log: string, requested: number[] | undefined): JsonObject | null {
  const prefix = "[oat] collector CPU allocation: ";
  const lines = log.split("\n").filter(line => line.startsWith(prefix));
  if (!requested) {
    if (lines.length !== 0) throw new Error("unexpected collector CPU allocation");
    return null;
  }
  if (lines.length !== 1) throw new Error("collector CPU allocation evidence is missing or repeated");
  const evidence = object(JSON.parse(lines[0]!.slice(prefix.length)), "collector CPU allocation");
  if (JSON.stringify(evidence.requested) !== JSON.stringify(requested)
    || typeof evidence.container_id !== "string" || !/^[a-f0-9]{64}$/.test(evidence.container_id)
    || typeof evidence.configured !== "string" || typeof evidence.effective !== "string"
    || JSON.stringify(linuxCpus(evidence.configured)) !== JSON.stringify(requested)
    || JSON.stringify(linuxCpus(evidence.effective)) !== JSON.stringify(requested)) {
    throw new Error("collector CPU allocation evidence differs from the requested stratum");
  }
  return evidence;
}

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

function git(runtime: string, env: NodeJS.ProcessEnv, ...args: string[]): string {
  return execFileSync("git", ["-C", runtime, ...args], { encoding: "utf8", env }).trimEnd();
}

export async function buildFooterInspector(runtime: string, home: string, stackBytes: number): Promise<{ path: string; sha256: string }> {
  const env = cleanEnv(home, stackBytes);
  const output = execFileSync("/usr/local/cargo/bin/cargo", [
    "build", "--locked", "--release", "--package", "arrowcol-oat-rust", "--bin", "oat-parity-footer",
    "--message-format=json-render-diagnostics",
  ], { cwd: runtime, env, encoding: "utf8", maxBuffer: 64 * 1024 * 1024, stdio: ["ignore", "pipe", "inherit"] });
  const paths: string[] = [];
  for (const line of output.split("\n").filter(Boolean)) {
    const entry = object(JSON.parse(line), "Cargo message");
    if (entry.reason === "compiler-artifact" && entry.manifest_path === join(runtime, "projects", "arrowcol-oat-rust", "Cargo.toml")
      && typeof entry.executable === "string" && object(entry.target, "Cargo target").name === "oat-parity-footer") {
      paths.push(await realpath(entry.executable));
    }
  }
  const path = join(runtime, "target", "release", "oat-parity-footer");
  if (paths.length !== 1 || paths[0] !== path) throw new Error("Cargo did not select the trusted parity footer inspector");
  return { path, sha256: await sha256(path) };
}

export async function replayParityArm(
  repo: string, home: string, workers: number, bindHost: string, endpointHost: string, stackBytes: number,
  collectorOverride?: CollectorImageOverride,
  collectorCpus?: number[],
  profile: ParityProfile = "historical-parity-diagnostic",
  generatedPartitionFastPath?: boolean,
): Promise<JsonObject> {
  if (!/^[0-9a-fA-F:.]+$/.test(bindHost) || !/^[0-9a-fA-F:.]+$/.test(endpointHost)) {
    throw new Error("local bind and endpoint hosts must be explicit IP addresses");
  }
  const trusted = join(home, "trusted");
  const manifest = JSON.parse(await readFile(join(trusted, "manifest.json"), "utf8")) as SnapshotManifest;
  const producer = object(JSON.parse(await readFile(join(trusted, "producer.json"), "utf8")), "producer binding");
  if (typeof producer.revision !== "string" || !/^[a-f0-9]{40}$/.test(producer.revision)) throw new Error("invalid producer revision");
  if (producer.revision !== manifest.commit) throw new Error("parity producer must be the trusted snapshot revision");
  if (collectorOverride) {
    if (!/^sha256:[a-f0-9]{64}$/.test(collectorOverride.imageId)
      || !/^[a-f0-9]{40}$/.test(collectorOverride.sourceRevision)) {
      throw new Error("collector override requires an immutable image ID and full source revision");
    }
    execFileSync("git", ["-C", repo, "merge-base", "--is-ancestor", collectorOverride.sourceRevision, producer.revision]);
    const observed = execFileSync("docker", [
      "image", "inspect", "--format", '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}',
      collectorOverride.imageId,
    ], { encoding: "utf8" }).trim();
    if (observed !== `${collectorOverride.imageId} ${collectorOverride.sourceRevision}`) {
      throw new Error("collector override image identity or revision does not match");
    }
  }
  const seedPath = join(trusted, "src", "experiments", "darwin", "historical-parity.oat.json");
  const seedHash = await sha256(seedPath);
  if (manifest.files["experiments/darwin/historical-parity.oat.json"]?.sha256 !== seedHash) {
    throw new Error("parity seed differs from the trusted snapshot");
  }
  const seed = object(JSON.parse(await readFile(seedPath, "utf8")), "parity seed");
  if (seed.warmup_seconds !== 60 || seed.measure_seconds !== 150) throw new Error("unexpected parity timing contract");
  const config = parityLadderConfig(seed, workers, profile, generatedPartitionFastPath);
  const data = join(home, "data");
  await mkdir(data, { recursive: true });
  const lock = join(data, "evaluating");
  await mkdir(lock);
  let completed = false;
  try {
    const { runtime, binaryHash } = await prepareReplayRuntime(repo, home, stackBytes);
    const footer = await buildFooterInspector(runtime, join(trusted, "prepare-home"), stackBytes);
    const env = cleanEnv(join(trusted, "prepare-home"), stackBytes);
    if (git(runtime, env, "rev-parse", "HEAD") !== producer.revision
      || git(runtime, env, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
      || !hasPinnedSubmodules(runtime, env)) throw new Error("trusted parity runtime changed while building");
    const runRoot = await mkdtemp(join(data, `parity-${workers}-`));
    const runHome = join(runRoot, "home");
    await mkdir(runHome);
    const platform = object(config.platform, "platform");
    if (platform.kind !== "local") throw new Error("parity replay requires local mode");
    platform.compose_state_root = join(runRoot, "state");
    platform.local = localReplayOptions(platform.local, bindHost, endpointHost, join(runRoot, "snapshots"));
    if (collectorOverride) object(platform.local, "local").collector_image_override = {
      image_id: collectorOverride.imageId, source_revision: collectorOverride.sourceRevision,
    };
    if (collectorCpus) {
      if (JSON.stringify(parseCollectorCpus(collectorCpus.join(","))) !== JSON.stringify(collectorCpus)) {
        throw new Error("invalid collector CPU allocation");
      }
      object(platform.local, "local").collector_cpus = collectorCpus;
    }
    const identityPath = join(data, "ladder-identity.json");
    let identity: JsonObject | undefined;
    try {
      identity = object(JSON.parse(await readFile(identityPath, "utf8")), "ladder identity");
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
    if (identity) {
      if (identity.source_revision !== producer.revision || identity.seed_sha256 !== seedHash
        || (identity.profile ?? "historical-parity-diagnostic") !== profile
        || (identity.generated_partition_fast_path ?? null) !== (generatedPartitionFastPath ?? null)
        || identity.collector_revision !== (collectorOverride?.sourceRevision ?? producer.revision)
        || identity.collector_override_image_id !== (collectorOverride?.imageId ?? null)
        || JSON.stringify(identity.collector_cpus ?? null) !== JSON.stringify(collectorCpus ?? null)
        || typeof identity.payload_corpus_sha256 !== "string" || !/^[a-f0-9]{64}$/.test(identity.payload_corpus_sha256)) {
        throw new Error("ladder identity does not match the trusted producer and seed");
      }
      object(platform.local, "local").expected_image_ids = object(identity.images, "ladder images");
    }
    config.result_url = pathToFileURL(join(runRoot, "results.parquet")).href;
    config.website_url = pathToFileURL(join(runRoot, "site", "results.parquet")).href;
    const configPath = join(runRoot, "config.json");
    await writeFile(configPath, `${JSON.stringify(config, null, 2)}\n`, { flag: "wx", mode: 0o400 });
    const configHash = await sha256(configPath);
    const client = object(object(config.tunables, "tunables").client, "client");
    const local = object(platform.local, "local");
    if (!Number.isSafeInteger(client.max_memory_bytes) || !Number.isSafeInteger(local.collector_memory_limit_bytes)) {
      throw new Error("parity memory budgets are unavailable");
    }
    const host = await inspectParityHost(Number(client.max_memory_bytes), Number(local.collector_memory_limit_bytes));
    await writeFile(join(runRoot, "host.json"), `${JSON.stringify(host, null, 2)}\n`, { flag: "wx" });
    await runOat(runtime, configPath, runHome, join(trusted, "bin"), join(runRoot, "run.log"), stackBytes, true);
    if (await sha256(configPath) !== configHash || await sha256(seedPath) !== seedHash) throw new Error("parity configuration changed during the run");
    const decodeFile = async (path: string, active: boolean): Promise<number> => {
      if (await sha256(footer.path) !== footer.sha256) throw new Error("trusted footer inspector changed");
      const output = execFileSync(footer.path, [active ? "--active" : "--decode", path], {
        env: cleanEnv(runHome, stackBytes), timeout: 600_000, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
      }).trim();
      if (await sha256(footer.path) !== footer.sha256) throw new Error("trusted footer inspector changed");
      if (!/^(0|[1-9][0-9]*)$/.test(output) || !Number.isSafeInteger(Number(output))) throw new Error("invalid decoded row count");
      return Number(output);
    };
    const result = await readReconciledOatResult(
      join(runRoot, "results.parquet"), join(runRoot, "snapshots"), "sqlserver_availability_replica_states",
      producer.revision, null, "diagnostic", { config, decodeFile, collectorRevision: collectorOverride?.sourceRevision },
    );
    const log = await readFile(join(runRoot, "run.log"), "utf8");
    const images = parityImageIds(log, result.images);
    const cpuAllocation = parityCpuAllocation(log, collectorCpus);
    if (collectorOverride && images.collector !== collectorOverride.imageId) throw new Error("collector image override was not used");
    const payload = result.clientEvidence.payloadCorpusSha256;
    if (typeof payload !== "string" || !/^[a-f0-9]{64}$/.test(payload)) throw new Error("parity corpus identity missing");
    if (identity && (JSON.stringify(identity.images) !== JSON.stringify(images)
      || identity.payload_corpus_sha256 !== payload)) throw new Error("ladder image or corpus identity changed");
    if (await sha256(join(runtime, "target", "release", "arrowcol-oat-rust")) !== binaryHash
      || git(runtime, env, "rev-parse", "HEAD") !== producer.revision
      || git(runtime, env, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
      || !hasPinnedSubmodules(runtime, env)) throw new Error("trusted parity runtime changed during the run");
    const receipt = {
      label: "local one-box scale", source_revision: producer.revision, trusted_revision: manifest.commit,
      collector_revision: collectorOverride?.sourceRevision ?? producer.revision,
      collector_override_image_id: collectorOverride?.imageId ?? null,
      collector_cpu_allocation: cpuAllocation,
      profile, seed_sha256: seedHash, config_sha256: configHash,
      generated_partition_fast_path: generatedPartitionFastPath ?? null,
      controller_sha256: binaryHash, footer_inspector_sha256: footer.sha256, workers, host, runRoot,
      identity_reconciliation: { available: false, reason: "Diagnostic corpus does not carry unique delivery identities" },
      ...result,
    };
    await writeFile(join(runRoot, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`, { flag: "wx" });
    if (!identity) {
      await writeFile(identityPath, `${JSON.stringify({
        source_revision: producer.revision, seed_sha256: seedHash, images, payload_corpus_sha256: payload,
        collector_revision: collectorOverride?.sourceRevision ?? producer.revision,
        collector_override_image_id: collectorOverride?.imageId ?? null,
        collector_cpus: collectorCpus ?? null,
        profile,
        generated_partition_fast_path: generatedPartitionFastPath ?? null,
      }, null, 2)}\n`, { flag: "wx", mode: 0o400 });
    }
    completed = true;
    return receipt;
  } finally {
    if (completed) await rmdir(lock);
  }
}
