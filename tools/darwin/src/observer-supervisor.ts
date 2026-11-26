import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm";

const exec = promisify(execFile);
const SHA = /^[a-f0-9]{64}$/;
const IMAGE = /^sha256:[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const ID = /^[a-f0-9]{64}$/;
const hash = (bytes: Uint8Array): string => createHash("sha256").update(bytes).digest("hex");
const isObject = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const object = (value: unknown, name: string): Record<string, unknown> => {
  if (!isObject(value)) throw new Error(`invalid ${name}`);
  return value;
};
const json = (value: string): unknown => JSON.parse(value);
const time = (value: unknown, name: string): number => {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error(`invalid ${name}`);
  return value;
};
const inspect = async (id: string): Promise<Record<string, unknown>> => {
  const value = json(await docker("inspect", id));
  if (!Array.isArray(value) || value.length !== 1) throw new Error("invalid Docker inspect response");
  return object(value[0], "Docker inspect");
};
const observation = (value: unknown, name: string): string | null => {
  const entry = object(value, name);
  if (entry.value === null && typeof entry.unavailable_reason === "string" && entry.unavailable_reason.trim()) return null;
  if (typeof entry.value === "string" && entry.unavailable_reason === null) return entry.value;
  throw new Error(`invalid ${name} observation`);
};
const sampleTimes = (value: unknown): { start: number; end: number; monotonic: bigint } => {
  const sample = object(value, "sample");
  const start = time(sample.sample_start_unix_ms, "sample start");
  const end = time(sample.sample_end_unix_ms, "sample end");
  if (end < start || typeof sample.monotonic_ns !== "string" || !/^[0-9]+$/.test(sample.monotonic_ns)) {
    throw new Error("invalid sample timing");
  }
  if (!Array.isArray(sample.threads) || sample.threads.length === 0) throw new Error("missing thread accounting");
  for (const rawThread of sample.threads) {
    const thread = object(rawThread, "thread accounting");
    const stats = observation(thread.schedstat, "thread schedstat");
    observation(thread.comm, "thread comm");
    observation(thread.stat, "thread stat");
    if (typeof thread.tid !== "string" || !/^[0-9]+$/.test(thread.tid)
      || typeof thread.startTicks !== "string" || !/^[0-9]+$/.test(thread.startTicks)
      || (stats !== null && !/^[0-9]+\s+[0-9]+\s+[0-9]+\s*$/.test(stats))) {
      throw new Error("missing thread accounting");
    }
  }
  for (const name of ["collector_io", "system_diskstats", "collector_namespace_netdev", "collector_metrics"]) {
    observation(sample[name], name);
  }
  const cgroup = object(sample.cgroup, "cgroup sample");
  for (const name of ["cpu.max", "cpu.stat", "cpuset.cpus.effective", "memory.current", "memory.max", "memory.events", "io.stat"]) {
    observation(cgroup[name], name);
  }
  return { start, end, monotonic: BigInt(sample.monotonic_ns) };
};
const docker = async (...args: string[]): Promise<string> => (await exec("docker", args, {
  encoding: "utf8", timeout: 15_000, maxBuffer: 8 * 1024 * 1024,
})).stdout.trim();
const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, ms));
const READ_READY_FILES = String.raw`
const fs = require("node:fs"), limit = 8 * 1024 * 1024;
function text(path) {
  let value;
  try { value = fs.readFileSync(path); }
  catch (error) { if (error && error.code === "ENOENT") return null; throw error; }
  if (value.length > limit) throw new Error("readiness file exceeds bound");
  if (value.length === 0 || value[value.length - 1] !== 10) return null;
  return value.toString("utf8");
}
function firstSample() {
  let fd;
  try { fd = fs.openSync("/capture/samples.ndjson", "r"); }
  catch (error) { if (error && error.code === "ENOENT") return null; throw error; }
  try {
    const size = fs.fstatSync(fd).size;
    const bytes = Buffer.alloc(Math.min(limit, size));
    const length = fs.readSync(fd, bytes, 0, bytes.length, 0), data = bytes.subarray(0, length);
    const end = data.indexOf(10);
    if (end < 0 && size > limit) throw new Error("sample exceeds read bound");
    return end < 0 ? null : data.subarray(0, end).toString("utf8");
  } finally { fs.closeSync(fd); }
}
const ready = text("/capture/ready.json"), metadata = text("/capture/metadata.json"), sample = firstSample();
process.stdout.write(JSON.stringify({ ready, metadata, sample }));
`;
const READ_COMPLETE_RECORD = String.raw`
const fs = require("node:fs"), fd = fs.openSync(process.argv[1], "r");
try {
  const size = fs.fstatSync(fd).size, limit = 8 * 1024 * 1024;
  const offset = process.argv[2] === "first" ? 0 : Math.max(0, size - limit);
  const data = Buffer.alloc(Math.min(limit, size - offset));
  const length = fs.readSync(fd, data, 0, data.length, offset), bytes = data.subarray(0, length);
  const end = process.argv[2] === "first" ? bytes.indexOf(10) : bytes.lastIndexOf(10);
  if (end < 0) throw new Error("no complete sample");
  const previous = process.argv[2] === "first" || end === 0 ? -1 : bytes.lastIndexOf(10, end - 1);
  if (previous < 0 && offset !== 0) throw new Error("sample exceeds read bound");
  process.stdout.write(bytes.subarray(previous + 1, end));
} finally { fs.closeSync(fd); }
`;
async function readCompleteSample(containerId: string, path: string, position: "first" | "last"): Promise<{ start: number; end: number; monotonic: bigint }> {
  if (!ID.test(containerId) || !path.startsWith("/")) throw new Error("invalid sample source");
  return sampleTimes(json(await docker("exec", containerId, "node", "-e", READ_COMPLETE_RECORD, path, position)));
}
export async function readLastCompleteSample(containerId: string, path: string): Promise<{ startUnixMs: number; endUnixMs: number }> {
  const sample = await readCompleteSample(containerId, path, "last");
  return { startUnixMs: sample.start, endUnixMs: sample.end };
}
const copyFileFromContainer = async (id: string, path: string): Promise<Buffer> => {
  const archive = (await exec("docker", ["cp", `${id}:${path}`, "-"], {
    encoding: "buffer", timeout: 15_000, maxBuffer: 64 * 1024 * 1024,
  })).stdout;
  if (archive.length < 512) throw new Error("container file archive is unavailable");
  const type = archive[156];
  const sizeText = archive.subarray(124, 136).toString("ascii").replace(/\0.*$/, "").trim();
  if ((type !== 0 && type !== 48) || !/^[0-7]+$/.test(sizeText)) throw new Error("invalid container file archive");
  const size = parseInt(sizeText, 8);
  if (size <= 0 || size > archive.length - 512) throw new Error("truncated container file archive");
  return archive.subarray(512, 512 + size);
};

export interface ObserverLaunch {
  composeProjectBase: string;
  cellRunId: string;
  collectorImageId: string;
  collectorRevision: string;
  executableSha256: string;
  observerImageId: string;
  observerBundlePath: string;
  observerBundleSha256: string;
  observerSourcePath: string;
  observerSourceSha256: string;
  configPath: string;
  configSha256: string;
  durationSeconds: number;
  sampleMs: number;
  cpuList: string | null;
  readyTimeoutMs: number;
}

export interface ObserverEvidence {
  complete: true;
  readyUnixMs: number;
  startUnixMs: number;
  endUnixMs: number;
  maxGapMs: number;
  collectorContainerId: string;
  collectorInitPid: number;
  collectorPid: number;
  collectorStartTicks: string;
  collectorImageId: string;
  collectorRevision: string;
  configSha256: string;
  observerSourceSha256: string;
  observerBundleSha256: string;
  observerImageId: string;
  resultSha256: string;
  artifacts: Record<string, string>;
}

export function projectName(base: string, runId: string): string {
  if (!/^[a-z0-9][a-z0-9_-]*$/.test(base) || !UUID.test(runId)) throw new Error("invalid run-scoped compose identity");
  const suffix = runId.replaceAll("-", "");
  return `${base.slice(0, 48 - suffix.length - 1).replace(/[-_]+$/, "")}-${suffix}`;
}

async function checkInputs(options: ObserverLaunch): Promise<void> {
  projectName(options.composeProjectBase, options.cellRunId);
  if (!IMAGE.test(options.collectorImageId) || !IMAGE.test(options.observerImageId)
    || !/^[a-f0-9]{40}$/.test(options.collectorRevision)
    || [options.executableSha256, options.observerBundleSha256, options.observerSourceSha256, options.configSha256].some(x => !SHA.test(x))
    || !Number.isSafeInteger(options.durationSeconds) || options.durationSeconds < 1 || options.durationSeconds > 3600
    || !Number.isSafeInteger(options.sampleMs) || options.sampleMs < 100 || options.sampleMs > 5000
    || !Number.isSafeInteger(options.readyTimeoutMs) || options.readyTimeoutMs < 100 || options.readyTimeoutMs > 120_000) {
    throw new Error("invalid observer launch binding");
  }
  const inputs: [string, string][] = [[options.configPath, options.configSha256], [options.observerSourcePath, options.observerSourceSha256],
    [options.observerBundlePath, options.observerBundleSha256]];
  for (const [path, expected] of inputs) {
    if (hash(await readFile(path)) !== expected) throw new Error("observer input hash mismatch");
  }
  const config = object(json((await readFile(options.configPath)).toString()), "config");
  if (object(config.platform, "config platform").compose_project !== options.composeProjectBase) {
    throw new Error("compose project differs from frozen config");
  }
}

const DESCENDANT_PROBE = String.raw`
const fs = require("node:fs"), crypto = require("node:crypto");
const root = Number(process.argv[1]), expected = process.argv[2];
function ticks(pid) {
  const stat = fs.readFileSync("/proc/" + pid + "/stat", "utf8");
  const end = stat.lastIndexOf(")"), fields = stat.slice(end + 2).trim().split(/\s+/);
  if (end < 0 || !/^[0-9]+$/.test(fields[19] || "")) throw new Error("invalid process identity");
  return fields[19];
}
function scan() {
  const queue = [root], visited = new Set(), records = [], matches = [];
  const rootGroup = fs.readFileSync("/proc/" + root + "/cgroup", "utf8");
  while (queue.length) {
    const pid = queue.shift();
    if (!Number.isSafeInteger(pid) || pid <= 1 || visited.has(pid)) throw new Error("invalid descendant tree");
    visited.add(pid);
    const before = ticks(pid), children = new Set();
    if (fs.readFileSync("/proc/" + pid + "/cgroup", "utf8") !== rootGroup) throw new Error("descendant cgroup changed");
    for (const tid of fs.readdirSync("/proc/" + pid + "/task")) {
      if (!/^[0-9]+$/.test(tid)) throw new Error("invalid thread id");
      let entries;
      try { entries = fs.readFileSync("/proc/" + pid + "/task/" + tid + "/children", "utf8").trim(); }
      catch (error) { if (error && error.code === "ENOENT") continue; throw error; }
      if (entries) for (const child of entries.split(/\s+/)) {
        if (!/^[0-9]+$/.test(child)) throw new Error("invalid child id");
        children.add(Number(child));
      }
    }
    const digest = crypto.createHash("sha256").update(fs.readFileSync("/proc/" + pid + "/exe")).digest("hex");
    if (ticks(pid) !== before) throw new Error("process identity changed");
    const sorted = [...children].sort((a,b) => a-b);
    records.push([pid, before, sorted]);
    if (digest === expected) matches.push({ pid, startTicks: before });
    queue.push(...sorted);
  }
  records.sort((a,b) => a[0]-b[0]);
  return { records, matches };
}
const a = scan(), b = scan();
if (JSON.stringify(a) !== JSON.stringify(b) || a.matches.length !== 1) throw new Error("collector descendant is missing, unstable or ambiguous");
process.stdout.write(JSON.stringify(a.matches[0]));
`;

interface CollectorIdentity { id: string; initPid: number; pid: number; startTicks: string }

async function resolveCollectorProcess(options: ObserverLaunch, initPid: number): Promise<{ pid: number; startTicks: string }> {
  const id = await docker("create", "--pull", "never", "--pid", "host", "--network", "none", "--user", "0:0",
    "--cap-drop", "ALL", "--cap-add", "SYS_PTRACE", "--security-opt", "no-new-privileges",
    "--entrypoint", "node", options.observerImageId, "-e", DESCENDANT_PROBE, String(initPid), options.executableSha256);
  if (!ID.test(id)) throw new Error("Docker did not return a probe container ID");
  try {
    const probe = await inspect(id);
    const host = object(probe.HostConfig, "probe host config");
    if (probe.Image !== options.observerImageId || host.PidMode !== "host" || host.NetworkMode !== "none"
      || !Array.isArray(probe.Mounts) || probe.Mounts.some((mount: unknown) => !isObject(mount) || mount.Type !== "tmpfs")) {
      throw new Error("probe container isolation mismatch");
    }
    const result = object(json(await docker("start", "-a", id)), "collector descendant");
    if (typeof result.pid !== "number" || !Number.isSafeInteger(result.pid) || result.pid <= 1
      || typeof result.startTicks !== "string" || !/^[0-9]+$/.test(result.startTicks)) throw new Error("invalid collector descendant");
    return { pid: result.pid, startTicks: result.startTicks };
  } finally { await docker("rm", "-f", id); }
}

async function collectorIdentity(options: ObserverLaunch): Promise<CollectorIdentity> {
  const project = projectName(options.composeProjectBase, options.cellRunId);
  const listed = (await docker("ps", "--no-trunc", "--quiet", "--filter", `label=com.docker.compose.project=${project}`,
    "--filter", "label=com.docker.compose.service=dataflow-1")).split(/\s+/).filter(Boolean);
  if (listed.length !== 1 || !ID.test(listed[0])) throw new Error("expected exactly one running run-scoped collector");
  const c = await inspect(listed[0]);
  const labels = object(object(c.Config, "collector config").Labels, "collector labels");
  const state = object(c.State, "collector state");
  if (c.Id !== listed[0] || c.Image !== options.collectorImageId || state.Running !== true
    || typeof state.Pid !== "number" || !Number.isSafeInteger(state.Pid) || state.Pid <= 1
    || labels["com.docker.compose.project"] !== project || labels["com.docker.compose.service"] !== "dataflow-1"
    || labels["org.opencontainers.image.revision"] !== options.collectorRevision) throw new Error("collector identity mismatch");
  const initPid = state.Pid;
  const process = await resolveCollectorProcess(options, initPid);
  const current = await inspect(listed[0]);
  if (current.Id !== c.Id || current.Image !== c.Image || object(current.State, "collector state").Pid !== initPid
    || object(current.State, "collector state").Running !== true) throw new Error("collector changed during process discovery");
  return { id: listed[0], initPid, ...process };
}

/** Start an isolated host-PID observer and return only after readiness AND its first real sample. */
export async function launchObserver(options: ObserverLaunch): Promise<{
  containerId: string; collectorContainerId: string; collectorInitPid: number; collectorPid: number; readyUnixMs: number;
  latestSample: () => Promise<{ startUnixMs: number; endUnixMs: number }>;
  finish: () => Promise<void>;
  collect: (outputDir: string, resultPath: string, maxGapMs: number) => Promise<ObserverEvidence>;
  stop: () => Promise<void>;
}> {
  options = { ...options };
  await checkInputs(options);
  const collector = await collectorIdentity(options);
  const args = ["create", "--pull", "never", "--pid", "host", "--network", "none",
    "--user", "0:0", "--cap-drop", "ALL",
    "--cap-add", "SYS_PTRACE", "--security-opt", "no-new-privileges", "--entrypoint", "node", options.observerImageId,
    "/observer.mjs", "--pid", String(collector.pid), "--executable-sha256", options.executableSha256,
    "--duration-seconds", String(options.durationSeconds), "--sample-ms", String(options.sampleMs), "--output", "/capture"];
  if (options.cpuList !== null) args.push("--cpu-list", options.cpuList);
  const id = await docker(...args);
  if (!ID.test(id)) throw new Error("Docker did not return an observer container ID");
  let removed = false;
  const stop = async (): Promise<void> => {
    if (!removed) { await docker("rm", "-f", id); removed = true; }
  };
  try {
    const observer = await inspect(id);
    const hostConfig = object(observer.HostConfig, "observer host config");
    if (observer.Image !== options.observerImageId || hostConfig.PidMode !== "host"
      || hostConfig.NetworkMode !== "none" || !Array.isArray(observer.Mounts)
      || observer.Mounts.some((mount: unknown) => !isObject(mount) || mount.Type !== "tmpfs")) {
      throw new Error("observer container isolation mismatch");
    }
    await docker("cp", options.observerBundlePath, `${id}:/observer.mjs`);
    // Recheck the file copied into Docker, rather than trusting a mutable source path.
    const copied = await copyFileFromContainer(id, "/observer.mjs");
    if (hash(copied) !== options.observerBundleSha256) {
      throw new Error("copied observer bundle hash mismatch");
    }
    await checkInputs(options);
    const before = await collectorIdentity(options);
    if (before.id !== collector.id || before.initPid !== collector.initPid || before.pid !== collector.pid
      || before.startTicks !== collector.startTicks) throw new Error("collector changed before observer start");
    await docker("start", id);
    const deadline = Date.now() + options.readyTimeoutMs;
    let readyUnixMs: number | null = null;
    for (;;) {
      const state = object((await inspect(id)).State, "observer state");
      if (state.Running !== true) throw new Error("observer exited before readiness");
      const files = object(json(await docker("exec", id, "node", "-e", READ_READY_FILES)), "readiness files");
      if ([files.ready, files.metadata, files.sample].some(value => value !== null && typeof value !== "string")) {
        throw new Error("invalid readiness files");
      }
      let at: number | null = null, sampleStart: number | null = null;
      if (typeof files.ready === "string") at = time(object(json(files.ready), "readiness").ready_unix_ms, "readiness");
      if (typeof files.metadata === "string") {
        const metadata = object(json(files.metadata), "metadata");
        if (metadata.collector_pid !== collector.pid || metadata.collector_start_ticks !== collector.startTicks
          || metadata.collector_executable_sha256 !== options.executableSha256) throw new Error("readiness identity mismatch");
      }
      if (typeof files.sample === "string") sampleStart = sampleTimes(json(files.sample)).start;
      if (at !== null && sampleStart !== null && at > sampleStart) throw new Error("readiness timing mismatch");
      if (at !== null && sampleStart !== null && typeof files.metadata === "string") {
        readyUnixMs = at;
        break;
      }
      if (Date.now() >= deadline) throw new Error("observer readiness timed out");
      await delay(100);
    }
    const after = await collectorIdentity(options);
    if (after.id !== collector.id || after.initPid !== collector.initPid || after.pid !== collector.pid
      || after.startTicks !== collector.startTicks) throw new Error("collector changed during observer startup");
    const waitForCompletion = async (timeoutMs: number): Promise<Record<string, unknown>> => {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const state = object((await inspect(id)).State, "observer state");
        if (state.Running !== true) {
          if (state.Running !== false || state.ExitCode !== 0) throw new Error("observer did not complete successfully");
          const completion = object(json((await copyFileFromContainer(id, "/capture/completion.json")).toString()), "completion");
          if (completion.complete !== true || completion.failure !== null) throw new Error("observer completion record failed");
          return completion;
        }
        if (Date.now() >= deadline) throw new Error("observer completion timed out");
        await delay(250);
      }
    };
    const latestSample = async (): Promise<{ startUnixMs: number; endUnixMs: number }> => {
      const state = object((await inspect(id)).State, "observer state");
      if (state.Running !== true) throw new Error("observer exited before post-measurement sample");
      return readLastCompleteSample(id, "/capture/samples.ndjson");
    };
    return {
      containerId: id, collectorContainerId: collector.id, collectorInitPid: collector.initPid, collectorPid: collector.pid, readyUnixMs, latestSample, stop,
      finish: async () => {
        const state = object((await inspect(id)).State, "observer state");
        if (state.Running !== true) throw new Error("observer exited before cooperative finish");
        await docker("kill", "--signal", "USR1", id);
        const completion = await waitForCompletion(30_000);
        if (completion.finished_by_request !== true) throw new Error("observer did not finish cooperatively");
      },
      collect: async (outputDir, resultPath, maxGapMs) => {
        await waitForCompletion((options.durationSeconds + 30) * 1000);
        await mkdir(outputDir, { recursive: false });
        await docker("cp", `${id}:/capture/.`, outputDir);
        await checkInputs(options);
        return verifyObserverEvidence(outputDir, resultPath, options, collector, maxGapMs);
      },
    };
  } catch (error) { await stop(); throw error; }
}

/** Read actual observer files and OAT parquet; this is not delivery or Tier-2 qualification. */
export async function verifyObserverEvidence(outputDir: string, resultPath: string, options: ObserverLaunch,
  collector: CollectorIdentity, maxAllowedGapMs: number): Promise<ObserverEvidence> {
  await checkInputs(options);
  if (!ID.test(collector.id) || !Number.isSafeInteger(collector.initPid) || collector.initPid <= 1
    || !/^[0-9]+$/.test(collector.startTicks) || !Number.isSafeInteger(collector.pid) || collector.pid <= 1
    || !Number.isSafeInteger(maxAllowedGapMs) || maxAllowedGapMs < options.sampleMs || maxAllowedGapMs > 5000) throw new Error("invalid evidence bounds");
  const names = ["metadata.json", "ready.json", "samples.ndjson", "completion.json"];
  const bytes = await Promise.all(names.map(name => readFile(join(outputDir, name))));
  const metadata = object(json(bytes[0].toString()), "metadata");
  const ready = object(json(bytes[1].toString()), "readiness");
  const completion = object(json(bytes[3].toString()), "completion");
  const result = await readFile(resultPath);
  if (result.length === 0 || result.length > 16 * 1024 * 1024) throw new Error("invalid OAT result size");
  const table = tableFromIPC(readParquet(result).intoIPCStream());
  if (table.numRows !== 1) throw new Error("expected one OAT result");
  const row = object(table.toArray()[0], "OAT result");
  if (Number(row.schema_version) !== 3 || row.mode !== "local" || row.status !== "succeeded" || row.run_id !== options.cellRunId
    || typeof row.evidence_json !== "string") throw new Error("OAT result identity mismatch");
  const oat = object(json(row.evidence_json), "OAT evidence");
  const schedule = object(object(oat.client_schedule, "schedule").effective, "effective schedule");
  const window = object(object(oat.durable_commits, "durable commits").output_throughput, "measurement window");
  const start = time(schedule.run_start_unix_ms, "run start");
  const measurementStart = time(schedule.measurement_start_unix_ms, "measurement start");
  const end = time(schedule.measurement_end_unix_ms, "measurement end");
  const config = object(json((await readFile(options.configPath)).toString()), "config");
  if (measurementStart - start !== time(config.warmup_seconds, "warmup seconds") * 1000
    || end - measurementStart !== time(config.measure_seconds, "measurement seconds") * 1000
    || measurementStart - start < 60_000 || end - measurementStart < 150_000
    || window.measurement_start_unix_ms !== measurementStart || window.measurement_end_unix_ms !== end) throw new Error("inconsistent OAT schedule");
  if (!Array.isArray(oat.processes)) throw new Error("OAT collector identity mismatch");
  const rawProcesses: unknown[] = oat.processes;
  const processes = rawProcesses.map(p => object(p, "OAT process")).filter(p => p.component === "collector");
  if (processes.length !== 1 || processes[0].image_id !== options.collectorImageId
    || processes[0].source_revision !== options.collectorRevision || processes[0].log_complete !== true) throw new Error("OAT collector identity mismatch");
  const interval = object(metadata.interval, "observer interval");
  if (metadata.version !== 1 || metadata.collector_pid !== collector.pid || metadata.collector_executable_sha256 !== options.executableSha256
    || metadata.collector_start_ticks !== collector.startTicks || interval.start_unix_ms !== 0
    || interval.duration_seconds !== options.durationSeconds || interval.sample_ms !== options.sampleMs
    || completion.complete !== true || completion.failure !== null) throw new Error("observer metadata or completion mismatch");
  const readyTime = time(ready.ready_unix_ms, "readiness");
  if (bytes[2].length === 0 || bytes[2][bytes[2].length - 1] !== 10) throw new Error("incomplete sample record");
  const lines = bytes[2].toString().trimEnd().split("\n");
  if (lines.length < 2) throw new Error("insufficient observer samples");
  let previousStart = -1, previousEnd = -1, previousMono = -1n, maxGap = 0;
  let first = -1, last = -1;
  for (const line of lines) {
    const sample = sampleTimes(json(line));
    const s = sample.start, e = sample.end, mono = sample.monotonic;
    if (e < s || s < previousEnd || (previousMono >= 0n && (mono <= previousMono
      || Math.abs(Number(mono - previousMono) / 1e6 - (e - previousEnd)) > 250))) throw new Error("sample clock or order mismatch");
    if (previousStart >= 0) maxGap = Math.max(maxGap, s - previousStart);
    first = first < 0 ? s : first; last = e; previousStart = s; previousEnd = e; previousMono = mono;
  }
  const observed = object(completion.observed_interval, "observed interval");
  if (readyTime > first || first > start || last < end || maxGap > maxAllowedGapMs
    || observed.start_unix_ms !== first || observed.end_unix_ms !== last
    || time(completion.end_unix_ms, "completion") < last) throw new Error("observation does not cover the bounded run");
  const artifacts = Object.fromEntries(names.map((name, i) => [name, hash(bytes[i]) ]));
  await checkInputs(options);
  for (const [i, name] of names.entries()) {
    if (hash(await readFile(join(outputDir, name))) !== artifacts[name]) throw new Error("observer evidence changed during verification");
  }
  if (hash(await readFile(resultPath)) !== hash(result)) throw new Error("OAT result changed during verification");
  return { complete: true, readyUnixMs: readyTime, startUnixMs: first, endUnixMs: last, maxGapMs: maxGap,
    collectorContainerId: collector.id, collectorInitPid: collector.initPid, collectorPid: collector.pid, collectorStartTicks: collector.startTicks,
    collectorImageId: options.collectorImageId, collectorRevision: options.collectorRevision,
    configSha256: options.configSha256, observerSourceSha256: options.observerSourceSha256,
    observerBundleSha256: options.observerBundleSha256, observerImageId: options.observerImageId,
    resultSha256: hash(result), artifacts };
}
