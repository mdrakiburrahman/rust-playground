import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { appendFile, mkdir, readFile, readdir, readlink, stat, writeFile } from "node:fs/promises";
import { execFile, spawn } from "node:child_process";
import { hostname } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

const exec = promisify(execFile);
type Observation<T> = { value: T | null; unavailable_reason: string | null };

export function cpuList(value: string): number[] {
  if (!/^(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?(,(0|[1-9][0-9]*)(-(0|[1-9][0-9]*))?)*$/.test(value)) {
    throw new Error("CPU list must contain nonnegative integers or ascending ranges");
  }
  const cpus: number[] = [];
  for (const part of value.split(",")) {
    const [start, end = start] = part.split("-").map(Number) as [number, number?];
    if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || end < start || end > 65535) {
      throw new Error("CPU range is invalid");
    }
    for (let cpu = start; cpu <= end; cpu++) {
      if (cpus.includes(cpu)) throw new Error("CPU list contains duplicates");
      cpus.push(cpu);
    }
  }
  return cpus.sort((a, b) => a - b);
}

export function processStartTime(contents: string): string {
  const end = contents.lastIndexOf(")");
  const fields = contents.slice(end + 2).trim().split(/\s+/);
  if (end < 0 || !/^[0-9]+$/.test(fields[19] ?? "")) throw new Error("invalid /proc stat identity");
  return fields[19]!;
}

export function allowedCpus(status: string): string {
  const match = /^Cpus_allowed_list:\s*(.+)$/m.exec(status);
  if (!match) throw new Error("effective thread affinity is unavailable");
  return cpuList(match[1]!.trim()).join(",");
}

async function optionalText(path: string): Promise<Observation<string>> {
  try {
    return { value: await readFile(path, "utf8"), unavailable_reason: null };
  } catch (error) {
    if (!(error instanceof Error)) throw error;
    return { value: null, unavailable_reason: `${path}: ${error.message}` };
  }
}

async function digest(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

interface Options {
  pid: number;
  executableSha256: string;
  cpuList: string | null;
  startUnixMs: number;
  durationSeconds: number;
  output: string;
  metricsUrl: string | null;
  sampleMs: number;
  profile: boolean;
}

interface Thread {
  tid: string;
  affinity: string;
}

interface Auxiliary {
  name: string;
  result: Promise<Observation<string>>;
}

function auxiliary(name: string, command: string, args: string[], output: string): Auxiliary {
  const destination = createWriteStream(join(output, `${name}.txt`), { flags: "wx" });
  const errors = createWriteStream(join(output, `${name}.stderr.txt`), { flags: "wx" });
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.pipe(destination);
  child.stderr.pipe(errors);
  const result = new Promise<Observation<string>>(resolveResult => {
    let failure: string | null = null;
    child.on("error", error => { failure = error.message; });
    child.on("close", (code, signal) => {
      destination.end();
      errors.end();
      resolveResult({
        value: code === 0 && failure === null ? `${name}.txt` : null,
        unavailable_reason: failure ?? (code === 0 ? null : `${command} exit=${code} signal=${signal}`),
      });
    });
  });
  return { name, result };
}

async function threads(pid: number): Promise<Thread[]> {
  const tids = (await readdir(`/proc/${pid}/task`)).sort((a, b) => Number(a) - Number(b));
  return Promise.all(tids.map(async tid => ({
    tid, affinity: allowedCpus(await readFile(`/proc/${pid}/task/${tid}/status`, "utf8")),
  })));
}

function sameThreads(actual: Thread[], expected: Thread[]): boolean {
  return actual.map(thread => thread.tid).join(",") === expected.map(thread => thread.tid).join(",");
}

export async function observe(options: Options): Promise<void> {
  if (process.platform !== "linux") throw new Error("physics observer requires Linux /proc");
  if (!Number.isSafeInteger(options.pid) || options.pid <= 1 || options.pid === process.pid) throw new Error("invalid collector PID");
  if (!/^[a-f0-9]{64}$/.test(options.executableSha256)) throw new Error("expected executable SHA256 is required");
  if (!Number.isSafeInteger(options.durationSeconds) || options.durationSeconds < 1 || options.durationSeconds > 3600
    || !Number.isSafeInteger(options.sampleMs) || options.sampleMs < 100 || options.sampleMs > 10000
    || !Number.isSafeInteger(options.startUnixMs) || options.startUnixMs < 0) throw new Error("invalid bounded observation interval");
  const target = `/proc/${options.pid}`;
  const exe = `${target}/exe`;
  if (await digest(exe) !== options.executableSha256) throw new Error("collector executable SHA256 mismatch");
  const originalStart = processStartTime(await readFile(`${target}/stat`, "utf8"));
  const originalExe = await stat(exe);
  const originalThreads = await threads(options.pid);
  if (originalThreads.length === 0) throw new Error("collector has no visible threads");
  const requested = options.cpuList === null ? null : cpuList(options.cpuList);
  if (requested && originalThreads.some(thread => requested.some(cpu => !cpuList(thread.affinity).includes(cpu)))) {
    throw new Error("requested collector CPUs exceed an original thread's allowed allocation");
  }
  const output = resolve(options.output);
  await mkdir(output, { recursive: false });
  const cgroup = await readFile(`${target}/cgroup`, "utf8");
  const v2 = /^0::(\/.*)$/m.exec(cgroup);
  const cgroupRoot = v2 && !v2[1]!.split("/").includes("..") ? join("/sys/fs/cgroup", v2[1]!) : null;
  const cgroupFiles = ["cpu.max", "cpu.stat", "cpuset.cpus.effective", "memory.current", "memory.max", "memory.events", "io.stat"];
  const capturedCgroup = async (): Promise<Record<string, Observation<string>>> => Object.fromEntries(await Promise.all(
    cgroupFiles.map(async file => [file, cgroupRoot
      ? await optionalText(join(cgroupRoot, file))
      : { value: null, unavailable_reason: "target cgroup v2 placement is unavailable or not mounted in observer namespace" }]),
  ));
  const identity = async (): Promise<Thread[]> => {
    const currentExe = await stat(exe);
    if (processStartTime(await readFile(`${target}/stat`, "utf8")) !== originalStart
      || currentExe.ino !== originalExe.ino || currentExe.dev !== originalExe.dev) throw new Error("collector PID/executable identity changed");
    const current = await threads(options.pid);
    if (!sameThreads(current, originalThreads)) throw new Error("collector thread set changed; intervention is invalid");
    if (requested && current.some(thread => thread.affinity !== requested.join(","))) throw new Error("collector affinity changed");
    return current;
  };
  const metadata = {
    version: 1, host: hostname(), collector_pid: options.pid, collector_start_ticks: originalStart,
    collector_executable: await readlink(exe), collector_executable_sha256: options.executableSha256,
    observer_pid: process.pid, observer_affinity: allowedCpus(await readFile("/proc/self/status", "utf8")),
    collector_cgroup: cgroup, collector_cgroup_mountinfo: await readFile(`${target}/mountinfo`, "utf8"),
    requested_collector_cpu_list: requested, original_threads: originalThreads,
    interval: { start_unix_ms: options.startUnixMs, duration_seconds: options.durationSeconds, sample_ms: options.sampleMs },
    cpu_scope: "per-thread schedstat CPU nanoseconds; affinity is not cgroup quota",
    disk_scope: "observer-visible system-wide diskstats; cgroup io.stat separately; neither is Parquet bytes",
    network_scope: "collector network-namespace net/dev counters, not per-process; includes other namespace traffic",
    collector_network_namespace: await readlink(`${target}/ns/net`),
    observer_network_namespace: await readlink("/proc/self/ns/net"),
    durability_scope: "none; join raw timestamps to trusted OAT Delta reconciliation, never Flight ACK",
    memory_bandwidth: { value: null, unavailable_reason: "no calibrated memory-controller PMU measurement supplied; RSS is not bandwidth" },
    hardware_limits: { value: null, unavailable_reason: "affinity and byte rates alone do not establish disk, NIC or memory saturation" },
    cgroup_before: await capturedCgroup(),
  };
  await writeFile(join(output, "metadata.json"), `${JSON.stringify(metadata, null, 2)}\n`, { flag: "wx" });
  const auxiliaryJobs: Auxiliary[] = [];
  let interventionStarted = false;
  let failure: unknown = null;
  const abort = new AbortController();
  const interrupt = (): void => { abort.abort(new Error("observer interrupted")); };
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
  try {
    if (requested) {
      interventionStarted = true;
      for (const thread of originalThreads) await exec("taskset", ["-pc", requested.join(","), thread.tid]);
    }
    await identity();
    await writeFile(join(output, "ready.json"), `${JSON.stringify({ ready_unix_ms: Date.now(), effective_threads: await threads(options.pid) })}\n`, { flag: "wx" });
    const remaining = options.startUnixMs - Date.now();
    if (remaining > 0) await new Promise<void>((accept, reject) => {
      const timer = setTimeout(accept, remaining);
      abort.signal.addEventListener("abort", () => { clearTimeout(timer); reject(abort.signal.reason); }, { once: true });
    });
    if (abort.signal.aborted) throw abort.signal.reason;
    if (options.startUnixMs !== 0 && Date.now() - options.startUnixMs > 250) throw new Error("observer missed scheduled start by more than 250ms");
    const start = Date.now();
    const end = start + options.durationSeconds * 1000;
    if (options.profile) {
      auxiliaryJobs.push(
        auxiliary("pidstat", "pidstat", ["-t", "-u", "-r", "-d", "-p", String(options.pid), "1", String(options.durationSeconds)], output),
        auxiliary("iostat", "iostat", ["-x", "-y", "-t", "1", String(options.durationSeconds)], output),
        auxiliary("perf", "perf", ["record", "-F", "99", "-g", "-p", String(options.pid), "-o", join(output, "perf.data"), "--", "sleep", String(options.durationSeconds)], output),
      );
    }
    while (Date.now() < end) {
      if (abort.signal.aborted) throw abort.signal.reason;
      const sampleStart = Date.now();
      const effective = await identity();
      const threadSamples = await Promise.all(effective.map(async thread => ({
        ...thread,
        comm: await readFile(`${target}/task/${thread.tid}/comm`, "utf8"),
        schedstat: await optionalText(`${target}/task/${thread.tid}/schedstat`),
        stat: await readFile(`${target}/task/${thread.tid}/stat`, "utf8"),
      })));
      let metrics: Observation<string> = { value: null, unavailable_reason: "no collector metrics URL configured; queue/flush/commit waits unavailable here" };
      if (options.metricsUrl !== null) {
        try {
          const response = await fetch(options.metricsUrl, { signal: AbortSignal.timeout(Math.min(options.sampleMs, 1000)) });
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          metrics = { value: await response.text(), unavailable_reason: null };
        } catch (error) {
          if (!(error instanceof Error)) throw error;
          metrics = { value: null, unavailable_reason: error.message };
        }
      }
      const sample = {
        sample_start_unix_ms: sampleStart, sample_end_unix_ms: Date.now(), monotonic_ns: process.hrtime.bigint().toString(),
        threads: threadSamples, collector_status: await readFile(`${target}/status`, "utf8"),
        collector_io: await optionalText(`${target}/io`), cgroup: await capturedCgroup(),
        system_cpu_stat: await readFile("/proc/stat", "utf8"), system_diskstats: await optionalText("/proc/diskstats"),
        collector_namespace_netdev: await optionalText(`${target}/net/dev`), collector_metrics: metrics,
      };
      await appendFile(join(output, "samples.ndjson"), `${JSON.stringify(sample)}\n`);
      await new Promise<void>(accept => setTimeout(accept, Math.max(0, Math.min(options.sampleMs - (Date.now() - sampleStart), end - Date.now()))));
    }
    await identity();
    if (await digest(exe) !== options.executableSha256) throw new Error("collector executable changed during observation");
  } catch (error) {
    failure = error;
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", interrupt);
    const restores: Observation<string>[] = [];
    if (interventionStarted) {
      for (const thread of originalThreads) {
        try {
          if (processStartTime(await readFile(`${target}/stat`, "utf8")) !== originalStart) throw new Error("collector PID was reused");
          await exec("taskset", ["-pc", thread.affinity, thread.tid]);
          const restored = allowedCpus(await readFile(`${target}/task/${thread.tid}/status`, "utf8"));
          if (restored !== thread.affinity) throw new Error(`thread ${thread.tid} affinity restore mismatch`);
          restores.push({ value: thread.tid, unavailable_reason: null });
        } catch (error) {
          if (!(error instanceof Error)) throw error;
          restores.push({ value: null, unavailable_reason: error.message });
        }
      }
    }
    const auxiliaryResults = Object.fromEntries(await Promise.all(auxiliaryJobs.map(async job => [job.name, await job.result])));
    await writeFile(join(output, "completion.json"), `${JSON.stringify({
      complete: failure === null && restores.every(result => result.value !== null),
      failure: failure instanceof Error ? failure.message : failure === null ? null : String(failure),
      end_unix_ms: Date.now(), restores, auxiliary: auxiliaryResults, cgroup_after: await capturedCgroup(),
    }, null, 2)}\n`, { flag: "wx" });
    if (restores.some(result => result.value === null)) throw new Error("collector affinity restore failed; see completion.json");
  }
  if (failure !== null) throw failure;
}

async function main(): Promise<void> {
  const { values } = parseArgs({ options: {
    pid: { type: "string" }, "executable-sha256": { type: "string" }, "cpu-list": { type: "string" },
    "start-unix-ms": { type: "string", default: "0" }, "duration-seconds": { type: "string" },
    output: { type: "string" }, "metrics-url": { type: "string" }, "sample-ms": { type: "string", default: "1000" },
    profile: { type: "boolean", default: false },
  }, strict: true });
  if (!values.output) throw new Error("--output is required and must not exist");
  await observe({
    pid: Number(values.pid), executableSha256: values["executable-sha256"] ?? "", cpuList: values["cpu-list"] ?? null,
    startUnixMs: Number(values["start-unix-ms"]), durationSeconds: Number(values["duration-seconds"]), output: values.output,
    metricsUrl: values["metrics-url"] ?? null, sampleMs: Number(values["sample-ms"]), profile: values.profile ?? false,
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
