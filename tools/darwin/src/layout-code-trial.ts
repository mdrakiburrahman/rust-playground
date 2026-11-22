import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { cp, lstat, readFile, readdir, realpath, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { isDeepStrictEqual } from "node:util";
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm";
import { readReconciledOatResult } from "./oat-result.js";
import { snapshot, type SnapshotManifest } from "./snapshot.js";
import type { Manifest } from "./paths.js";

const BASE = "f9b87c9ec9b7d8fc9b1bd82c17dd0d2057f19c62";
const REVIEWED = "f3c4425c93d394632af0f0c1f11c631d23fe7811";
const LAYOUT = "crates/arrow-deltalake/src/layout.rs";
const DOCKERFILE = "projects/arrowcol-dataflow-rust/Dockerfile";
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const SHA = /^[a-f0-9]{64}$/;
type Arm = "baseline" | "candidate";
type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}
function digest(bytes: Uint8Array): string { return createHash("sha256").update(bytes).digest("hex"); }
function id(value: unknown): string {
  if (typeof value !== "string" || !DIGEST.test(value)) throw new Error("immutable image ID required");
  return value;
}
function git(repo: string, ...args: string[]): Buffer {
  return execFileSync("git", ["--no-replace-objects", "-C", repo, ...args], { maxBuffer: 64 * 1024 * 1024 });
}
async function verifyTree(root: string, manifest: Manifest): Promise<void> {
  const expected = new Set(Object.keys(manifest));
  const directories = new Set<string>();
  for (const name of expected) {
    let parent = dirname(name);
    while (parent !== ".") { directories.add(parent); parent = dirname(parent); }
  }
  async function visit(path: string): Promise<void> {
    for (const entry of await readdir(join(root, path), { withFileTypes: true })) {
      const name = path ? `${path}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        if (!directories.has(name)) throw new Error(`unexpected source directory: ${name}`);
        await visit(name);
      } else {
        if (!entry.isFile() || !expected.delete(name)) throw new Error(`unexpected source file: ${name}`);
        const stat = await lstat(join(root, name));
        const record = manifest[name];
        if (!record || !stat.isFile() || stat.isSymbolicLink() || ((stat.mode & 0o111) !== 0) !== record.executable
          || digest(await readFile(join(root, name))) !== record.sha256) throw new Error(`source mismatch: ${name}`);
      }
    }
  }
  await visit("");
  if (expected.size) throw new Error(`missing source file: ${expected.values().next().value}`);
}

export interface LiveRole { service: string; imageId: string; containerId: string; }
export function inspectLayoutContainers(raw: unknown, project: string, expectedImage: string, bothImages: readonly string[], memory: number): LiveRole[] {
  id(expectedImage);
  if (bothImages.length !== 2 || bothImages.some(image => !DIGEST.test(image)) || !bothImages.includes(expectedImage)
    || bothImages[0] === bothImages[1] || !Number.isSafeInteger(memory) || memory <= 0 || !Array.isArray(raw)) {
    throw new Error("invalid image pair, memory cap or live container list");
  }
  const roles: LiveRole[] = [];
  const names = new Set<string>();
  for (const value of raw) {
    const container = object(value, "container");
    const config = object(container.Config, "container config");
    const labels = object(config.Labels, "container labels");
    const state = object(container.State, "container state");
    const host = object(container.HostConfig, "container host config");
    const service = labels["com.docker.compose.service"];
    if (labels["com.docker.compose.project"] !== project || typeof service !== "string" || !service || names.has(service)
      || state.Running !== true || container.RestartCount !== 0) throw new Error("invalid live Compose role or state");
    names.add(service);
    const imageId = id(container.Image);
    const containerId = container.Id;
    if (typeof containerId !== "string" || !/^[a-f0-9]{64}$/.test(containerId)) throw new Error("invalid container identity");
    if (service === "dataflow-1") {
      if (imageId !== expectedImage || config.Image !== expectedImage || host.Memory !== memory) {
        throw new Error("live collector is not the selected immutable image and memory cap");
      }
    } else if (bothImages.includes(imageId) || bothImages.includes(String(config.Image))) {
      throw new Error("collector image is shared with another role");
    }
    roles.push({ service, imageId, containerId });
  }
  if (!names.has("dataflow-1") || !names.has("router") || !names.has("arrowcol-catalog")) {
    throw new Error("missing live collector or required non-collector role");
  }
  return roles.sort((a, b) => a.service.localeCompare(b.service));
}

export function compareLayoutReadbacks(baseline: unknown, candidate: unknown): void {
  function read(value: unknown): JsonObject {
    const evidence = object(value, "OAT evidence");
    if (!Array.isArray(evidence.processes)) throw new Error("missing process evidence");
    const collectors = evidence.processes.map(p => object(p, "process")).filter(p => p.component === "collector");
    if (collectors.length !== 1 || collectors[0].log_complete !== true
      || collectors[0].effective_config_source !== "consumer_startup_readback") throw new Error("missing collector readback");
    const policies = collectors[0].layout_table_policies;
    if (!Array.isArray(policies) || policies.length !== 1) throw new Error("missing or ambiguous table layout proof");
    const policy = object(policies[0], "table layout");
    const config = object(policy.config, "table layout config");
    if (policy.table_name !== "telemetry.sqlserver.sqlserver_active_sessions"
      || typeof policy.table_id !== "string" || !SHA.test(policy.table_id)
      || policy.scope !== "table_effective_at_file_construction_not_full_run_history"
      || config.enabled !== true || !Array.isArray(config.sort_columns) || !config.sort_columns.length
      || !config.sort_columns.every(column => typeof column === "string" && /^[A-Za-z0-9_]+$/.test(column))) {
      throw new Error("enabled table layout with nonempty sort keys was not observed");
    }
    return policy;
  }
  if (!isDeepStrictEqual(read(baseline), read(candidate))) throw new Error("table layout readbacks differ between arms");
}

export function compareOatImages(
  baseline: unknown, candidate: unknown, images: Readonly<Record<Arm, string>>, baselineRoles: LiveRole[], candidateRoles: LiveRole[],
): void {
  function read(value: unknown, image: string, roles: LiveRole[]): { processes: JsonObject[]; client: string } {
    const evidence = object(value, "OAT evidence");
    if (!Array.isArray(evidence.processes)) throw new Error("missing OAT process image evidence");
    const processes = evidence.processes.map(p => object(p, "OAT process"));
    const seen = new Set<string>();
    for (const process of processes) {
      const component = process.component;
      if (typeof component !== "string" || seen.has(component) || process.log_complete !== true) throw new Error("invalid process image evidence");
      seen.add(component);
      const imageId = id(process.image_id);
      if (component === "collector") {
        if (imageId !== image || process.image !== image) throw new Error("OAT collector image differs from the live immutable image");
      } else {
        if (imageId === images.baseline || imageId === images.candidate) throw new Error("OAT non-collector aliases a collector image");
        const service = component === "router" ? "router" : component === "maintenance" ? "delta-maintenance" : undefined;
        if (!service || !roles.some(role => role.service === service && role.imageId === imageId)) throw new Error("OAT process role differs from live Compose");
      }
    }
    if (!seen.has("collector") || !seen.has("router") || !Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) {
      throw new Error("missing OAT collector, router or client image evidence");
    }
    const client = object(object(evidence.client_phase_reports[0], "client report").execution_image, "client image");
    const clientId = id(client.image_id);
    if (client.source !== "docker_container_inspect" || clientId === images.baseline || clientId === images.candidate) throw new Error("invalid client image evidence");
    return { processes: processes.map(p => ({ component: p.component, effective_config: p.effective_config, writer_policy_overrides: p.writer_policy_overrides })), client: clientId };
  }
  const left = read(baseline, images.baseline, baselineRoles);
  const right = read(candidate, images.candidate, candidateRoles);
  left.processes.sort((a, b) => String(a.component).localeCompare(String(b.component)));
  right.processes.sort((a, b) => String(a.component).localeCompare(String(b.component)));
  if (!isDeepStrictEqual(left, right)) throw new Error("OAT role settings or client image differ between arms");
}

export function scopedProject(base: string, runId: string): string {
  if (!/^[a-z0-9][a-z0-9_-]*$/.test(base) || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(runId)) throw new Error("invalid project or run identity");
  const suffix = runId.replaceAll("-", "");
  return base.slice(0, 48 - suffix.length - 1).replace(/[-_]+$/, "") + "-" + suffix;
}

export function assertSettledDelivery(value: unknown): void {
  const evidence = object(value, "OAT evidence");
  const reconciliation = object(evidence.reconciliation, "client reconciliation");
  const output = object(object(evidence.durable_commits, "durable commits").run_cumulative_output, "cumulative output");
  if (!Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) throw new Error("missing client acknowledgment evidence");
  const report = object(evidence.client_phase_reports[0], "client report");
  const counts = object(object(report.phase_counters, "client counters").process_cumulative, "cumulative client counts");
  const completed = counts.completed_rows; const ambiguous = counts.ambiguous_rows; const delta = output.rows;
  if (![completed, ambiguous, delta].every(n => Number.isSafeInteger(n) && Number(n) >= 0)
    || !Number.isSafeInteger(Number(completed) + Number(ambiguous)) || Number(delta) < Number(completed)
    || Number(delta) > Number(completed) + Number(ambiguous) || output.settled !== true
    || reconciliation.status !== "within_bound" || reconciliation.settled !== true
    || reconciliation.scope !== "process_cumulative_client_rows_vs_this_cells_new_ingestion_commits_including_warmup"
    || reconciliation.completed_rows !== completed || reconciliation.ambiguous_rows !== ambiguous || reconciliation.delta_rows !== delta) {
    throw new Error("client acknowledgments and Delta commits are unsettled or inconsistent");
  }
}

export function assertCollectorShutdown(value: unknown, containerId: string): void {
  const shutdown = object(object(value, "OAT evidence").shutdown_drain, "shutdown drain");
  if (shutdown.schema_version !== 1 || !Array.isArray(shutdown.collectors) || shutdown.collectors.length !== 1) throw new Error("collector shutdown evidence is unavailable");
  const collector = object(shutdown.collectors[0], "shutdown collector");
  const drain = object(collector.drain, "collector drain");
  if (collector.container_id !== containerId || drain.status !== "observed" || drain.sigterm_sent !== true
    || drain.exit_code !== 0 || drain.log_complete !== true) throw new Error("collector shutdown is unsettled or belongs to a different container");
}

export function comparableRunConfig(raw: unknown, runRoot: string): JsonObject {
  if (!isAbsolute(runRoot) || resolve(runRoot) !== runRoot) throw new Error("run root must be an absolute normalized path");
  const config = object(structuredClone(raw), "run config");
  const platform = object(config.platform, "platform");
  const local = object(platform.local, "local platform");
  if (platform.kind !== "local" || !isDeepStrictEqual(config.replica_counts, [1])
    || !isDeepStrictEqual(config.tables, ["sqlserver_active_sessions"])
    || !Number.isSafeInteger(local.collector_memory_limit_bytes) || Number(local.collector_memory_limit_bytes) <= 0
    || platform.compose_state_root !== join(runRoot, "state") || local.snapshot_root !== join(runRoot, "snapshots")
    || config.result_url !== pathToFileURL(join(runRoot, "results.parquet")).href
    || config.website_url !== pathToFileURL(join(runRoot, "site", "results.parquet")).href) {
    throw new Error("invalid fixed local workload, one-writer topology or run-owned paths");
  }
  delete platform.compose_state_root; delete local.snapshot_root; delete config.result_url; delete config.website_url;
  return config;
}

interface Capture { project: string; runRoot: string; config: JsonObject; configPath: string; configHash: string; controller: string; controllerHash: string; roles: LiveRole[]; }
export class LayoutCodeTrial {
  readonly #home: string;
  readonly #source: Record<Arm, string>;
  readonly #expected: Record<Arm, Manifest>;
  #images?: Record<Arm, string>;
  readonly #captures = new Map<Arm, Capture>();
  private constructor(home: string, source: Record<Arm, string>, expected: Record<Arm, Manifest>) {
    this.#home = home; this.#source = source; this.#expected = expected;
  }
  static async prepare(repo: string, home: string): Promise<LayoutCodeTrial> {
    repo = await realpath(repo);
    home = resolve(home);
    if (git(repo, "rev-parse", "HEAD").toString("utf8").trim() !== BASE) throw new Error("layout source must be the exact pinned baseline");
    const parentLayout = git(repo, "show", `${REVIEWED}^:${LAYOUT}`);
    const baseLayout = git(repo, "show", `${BASE}:${LAYOUT}`);
    const replacement = git(repo, "show", `${REVIEWED}:${LAYOUT}`);
    const names = git(repo, "diff", "--name-only", `${REVIEWED}^`, REVIEWED).toString("utf8").trim();
    if (!parentLayout.equals(baseLayout) || baseLayout.equals(replacement) || names !== LAYOUT) {
      throw new Error("reviewed production-only patch does not apply to the pinned baseline");
    }
    const manifest: SnapshotManifest = await snapshot(repo, home);
    if (manifest.commit !== BASE) throw new Error("layout source must be the exact pinned baseline");
    const all: Manifest = { ...manifest.files, ...manifest.submoduleFiles };
    if (!all[LAYOUT] || all[LAYOUT].sha256 !== digest(baseLayout) || !all[DOCKERFILE]) throw new Error("trusted source manifest mismatch");
    const baseline = join(home, "trusted", "src");
    const candidate = join(home, "trusted", "candidate-src");
    await verifyTree(baseline, all);
    await cp(baseline, candidate, { recursive: true, errorOnExist: true, force: false });
    await writeFile(join(candidate, LAYOUT), replacement);
    const candidateManifest: Manifest = { ...all, [LAYOUT]: { ...all[LAYOUT], sha256: digest(replacement) } };
    await verifyTree(candidate, candidateManifest);
    return new LayoutCodeTrial(home, { baseline, candidate }, { baseline: all, candidate: candidateManifest });
  }
  async build(): Promise<Readonly<Record<Arm, string>>> {
    if (this.#images) throw new Error("collector images are already built");
    const env = { HOME: this.#home, PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8" };
    const images = {} as Record<Arm, string>;
    for (const arm of ["baseline", "candidate"] as const) await verifyTree(this.#source[arm], this.#expected[arm]);
    for (const arm of ["baseline", "candidate"] as const) {
      const root = this.#source[arm];
      await verifyTree(root, this.#expected[arm]);
      const iid = join(this.#home, "trusted", `${arm}.iid`);
      execFileSync("docker", ["build", "--iidfile", iid, "--file", join(root, DOCKERFILE), "--build-arg", `VCS_REF=${BASE}`, root], { env, stdio: "inherit" });
      await verifyTree(root, this.#expected[arm]);
      const image = id((await readFile(iid, "utf8")).trim());
      const actual = execFileSync("docker", ["image", "inspect", "--format", "{{.Id}}", image], { env, encoding: "utf8" }).trim();
      if (actual !== image) throw new Error("built image ID is not available on the selected daemon");
      images[arm] = image;
    }
    if (images.baseline === images.candidate) throw new Error("collector builds produced the same image identity");
    this.#images = Object.freeze(images);
    return this.#images;
  }
  async capture(arm: Arm, project: string, configPath: string, runRoot: string, controller: string): Promise<void> {
    if (!this.#images || this.#captures.has(arm) || !/^[a-z0-9][a-z0-9_-]+$/.test(project)) throw new Error("invalid or duplicate arm capture");
    if (!isAbsolute(configPath) || !isAbsolute(controller) || relative(runRoot, configPath) !== "config.json") throw new Error("invalid trusted run paths");
    const configBytes = await readFile(configPath);
    const config = comparableRunConfig(JSON.parse(configBytes.toString("utf8")), runRoot);
    const memory = Number(object(object(config.platform, "platform").local, "local").collector_memory_limit_bytes);
    const env = { HOME: this.#home, PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8" };
    const lines = execFileSync("docker", ["ps", "--no-trunc", "--quiet", "--filter", `label=com.docker.compose.project=${project}`], { env, encoding: "utf8" }).trim();
    const ids = lines ? lines.split("\n") : [];
    if (!ids.length || ids.some(value => !/^[a-f0-9]{64}$/.test(value))) throw new Error("live Compose container IDs are unavailable");
    const raw = JSON.parse(execFileSync("docker", ["inspect", ...ids], { env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 })) as unknown;
    const roles = inspectLayoutContainers(raw, project, this.#images[arm], [this.#images.baseline, this.#images.candidate], memory);
    this.#captures.set(arm, { project, runRoot, config, configPath, configHash: digest(configBytes), controller, controllerHash: digest(await readFile(controller)), roles });
  }
  async compareReconciledArms(): Promise<{ baseline: Awaited<ReturnType<typeof readReconciledOatResult>>; candidate: Awaited<ReturnType<typeof readReconciledOatResult>> }> {
    const baseline = this.#captures.get("baseline");
    const candidate = this.#captures.get("candidate");
    const stableRoles = (capture: Capture) => capture.roles.filter(role => role.service !== "dataflow-1").map(({ service, imageId }) => ({ service, imageId }));
    if (!baseline || !candidate || !this.#images || baseline.project === candidate.project || baseline.controllerHash !== candidate.controllerHash
      || !isDeepStrictEqual(baseline.config, candidate.config) || !isDeepStrictEqual(stableRoles(baseline), stableRoles(candidate))) {
      throw new Error("arm settings, controller or non-collector roles differ");
    }
    const env = { HOME: this.#home, PATH: "/usr/local/bin:/usr/bin:/bin", LANG: "C.UTF-8" };
    async function read(capture: Capture) {
      if (digest(await readFile(capture.configPath)) !== capture.configHash || digest(await readFile(capture.controller)) !== capture.controllerHash) {
        throw new Error("run config or controller changed after capture");
      }
      const tunables = object(capture.config.tunables, "tunables");
      const defaults = object(object(tunables.parquet_writer, "parquet writer").defaults, "writer defaults");
      if (defaults.compression !== "zstd" || !Number.isSafeInteger(defaults.compression_level)) throw new Error("unsupported frozen compression");
      const path = join(capture.runRoot, "results.parquet");
      const bytes = await readFile(path);
      if (!bytes.length || bytes.length > 16 * 1024 * 1024) throw new Error("invalid result size");
      const result = await readReconciledOatResult(path, join(capture.runRoot, "snapshots"), "sqlserver_active_sessions", BASE, Number(defaults.compression_level));
      if (digest(await readFile(path)) !== digest(bytes)) throw new Error("OAT result changed during reconciliation");
      const table = tableFromIPC(readParquet(bytes).intoIPCStream());
      if (table.numRows !== 1) throw new Error("invalid OAT evidence rows");
      const row = table.toArray()[0];
      if (typeof row.evidence_json !== "string") throw new Error("missing OAT evidence");
      const evidence = JSON.parse(row.evidence_json) as unknown;
      const platform = object(capture.config.platform, "platform");
      if (typeof platform.compose_project !== "string" || capture.project !== scopedProject(platform.compose_project, result.cellRunId)) {
        throw new Error("live Compose project does not match the reconciled cell");
      }
      assertCollectorShutdown(evidence, capture.roles.find(role => role.service === "dataflow-1")!.containerId);
      assertSettledDelivery(evidence);
      for (const command of [["ps", "--all", "--quiet"], ["network", "ls", "--quiet"], ["volume", "ls", "--quiet"]]) {
        const remaining = execFileSync("docker", [...command, "--filter", "label=com.docker.compose.project=" + capture.project], { env, encoding: "utf8" }).trim();
        if (remaining) throw new Error("run-owned Docker cleanup is unsettled");
      }
      try {
        await lstat(join(capture.runRoot, "state", result.cellRunId));
        throw new Error("run-owned state remains after cleanup");
      } catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
      }
      return { result, evidence };
    }
    const left = await read(baseline);
    const right = await read(candidate);
    compareLayoutReadbacks(left.evidence, right.evidence);
    compareOatImages(left.evidence, right.evidence, this.#images, baseline.roles, candidate.roles);
    return { baseline: left.result, candidate: right.result };
  }
}
