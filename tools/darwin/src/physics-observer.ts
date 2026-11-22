import { createHash } from "node:crypto";
import { createWriteStream } from "node:fs";
import { appendFile, mkdir, readFile, readdir, readlink, stat, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { hostname } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { parseArgs } from "node:util";

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

export function cgroupDirectory(placement: string, mountinfo: string): string | null {
  const v2 = /^0::(\/.*)$/m.exec(placement);
  if (!v2 || v2[1]!.split("/").includes("..")) return null;
  for (const line of mountinfo.split("\n")) {
    const [mount, filesystem] = line.split(" - ");
    if (!filesystem?.startsWith("cgroup2 ")) continue;
    const fields = mount!.split(" ");
    const root = fields[3]!;
    const point = fields[4]!;
    if (!root || !point || root.includes("\\") || point.includes("\\")) continue;
    if (v2[1] === root) return point;
    const prefix = root === "/" ? "/" : `${root}/`;
    if (v2[1]!.startsWith(prefix)) return join(point, v2[1]!.slice(prefix.length));
  }
  return null;
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
  startTicks: string;
}

interface Auxiliary {
  name: string;
  result: Promise<Observation<string>>;
  stop: () => void;
}

export function auxiliary(name: string, command: string, args: string[], output: string): Auxiliary {
  const destination = createWriteStream(join(output, `${name}.txt`), { flags: "wx" });
  const errors = createWriteStream(join(output, `${name}.stderr.txt`), { flags: "wx" });
  const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.pipe(destination);
  child.stderr.pipe(errors);
  let reaped = false;
  let killTimer: ReturnType<typeof setTimeout> | null = null;
  const result = new Promise<Observation<string>>(resolveResult => {
    let failure: string | null = null;
    child.on("error", error => { failure = error.message; });
    child.on("close", (code, signal) => {
      reaped = true;
      if (killTimer !== null) clearTimeout(killTimer);
      destination.end();
      errors.end();
      resolveResult({
        value: code === 0 && failure === null ? `${name}.txt` : null,
        unavailable_reason: failure ?? (code === 0 ? null : `${command} exit=${code} signal=${signal}`),
      });
    });
  });
  return { name, result, stop: () => {
    if (reaped || killTimer !== null) return;
    child.kill("SIGINT");
    killTimer = setTimeout(() => { if (!reaped) child.kill("SIGKILL"); }, 1000);
  } };
}

async function threads(pid: number): Promise<Thread[]> {
  const tids = (await readdir(`/proc/${pid}/task`)).sort((a, b) => Number(a) - Number(b));
  const result: Thread[] = [];
  for (const tid of tids) {
    try {
      const startTicks = processStartTime(await readFile(`/proc/${pid}/task/${tid}/stat`, "utf8"));
      const affinity = allowedCpus(await readFile(`/proc/${pid}/task/${tid}/status`, "utf8"));
      if (processStartTime(await readFile(`/proc/${pid}/task/${tid}/stat`, "utf8")) !== startTicks) continue;
      result.push({ tid, affinity, startTicks });
    } catch (error) {
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
  }
  return result;
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
  const verifyAffinity = (current: Thread[]): void => {
    if (requested && current.some(thread => thread.affinity !== requested.join(","))) {
      throw new Error("collector affinity differs from the run-scoped CPU allocation; observer never changes affinity");
    }
  };
  verifyAffinity(originalThreads);
  const output = resolve(options.output);
  await mkdir(output, { recursive: false });
  const cgroup = await readFile(`${target}/cgroup`, "utf8");
  const cgroupRoot = cgroupDirectory(cgroup, await readFile("/proc/self/mountinfo", "utf8"));
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
    verifyAffinity(current);
    return current;
  };
  const metadata = {
    version: 1, host: hostname(), collector_pid: options.pid, collector_start_ticks: originalStart,
    collector_executable: await readlink(exe), collector_executable_sha256: options.executableSha256,
    observer_pid: process.pid, observer_affinity: allowedCpus(await readFile("/proc/self/status", "utf8")),
    collector_cgroup: cgroup, collector_cgroup_mountinfo: await readFile(`${target}/mountinfo`, "utf8"),
    requested_collector_cpu_list: requested, original_threads: originalThreads,
    resource_control: "read-only observer; collector cpuset/quota must be set at run creation by trusted harness; clients untouched",
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
  const threadCpu = new Map<string, { tid: string; start_ticks: string; first_cpu_ns: bigint; last_cpu_ns: bigint; first_wait_ns: bigint; last_wait_ns: bigint; samples: number }>();
  let observedStart: number | null = null;
  let observedEnd: number | null = null;
  let failure: unknown = null;
  const abort = new AbortController();
  const interrupt = (): void => { abort.abort(new Error("observer interrupted")); };
  process.once("SIGINT", interrupt);
  process.once("SIGTERM", interrupt);
  try {
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
        auxiliary("perf", "perf", ["record", "-F", "99", "-g", "-p", String(options.pid), "-o", join(output, "perf.data")], output),
      );
    }
    while (Date.now() < end) {
      if (abort.signal.aborted) throw abort.signal.reason;
      const sampleStart = Date.now();
      const effective = await identity();
      const threadSamples = await Promise.all(effective.map(async thread => {
        const comm = await optionalText(`${target}/task/${thread.tid}/comm`);
        const schedstat = await optionalText(`${target}/task/${thread.tid}/schedstat`);
        const currentStat = await optionalText(`${target}/task/${thread.tid}/stat`);
        const stable = currentStat.value !== null && processStartTime(currentStat.value) === thread.startTicks;
        return {
          ...thread, comm, schedstat: stable ? schedstat : { value: null, unavailable_reason: "thread exited or TID was reused during sample" },
          stat: stable ? currentStat : { value: null, unavailable_reason: "thread exited or TID was reused during sample" },
        };
      }));
      for (const thread of threadSamples) {
        if (thread.schedstat.value === null) continue;
        const fields = thread.schedstat.value.trim().split(/\s+/);
        if (!/^[0-9]+$/.test(fields[0] ?? "") || !/^[0-9]+$/.test(fields[1] ?? "")) {
          throw new Error("invalid thread CPU/runqueue accounting");
        }
        const cpu = BigInt(fields[0]!);
        const wait = BigInt(fields[1]!);
        const key = `${thread.tid}:${thread.startTicks}`;
        const prior = threadCpu.get(key);
        if (prior) {
          if (cpu < prior.last_cpu_ns || wait < prior.last_wait_ns) throw new Error("thread CPU counters moved backwards");
          prior.last_cpu_ns = cpu;
          prior.last_wait_ns = wait;
          prior.samples += 1;
        } else {
          threadCpu.set(key, {
            tid: thread.tid, start_ticks: thread.startTicks, first_cpu_ns: cpu, last_cpu_ns: cpu,
            first_wait_ns: wait, last_wait_ns: wait, samples: 1,
          });
        }
      }
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
        sample_start_unix_ms: sampleStart,
        threads: threadSamples, collector_status: await readFile(`${target}/status`, "utf8"),
        collector_io: await optionalText(`${target}/io`), cgroup: await capturedCgroup(),
        system_cpu_stat: await readFile("/proc/stat", "utf8"), system_diskstats: await optionalText("/proc/diskstats"),
        collector_namespace_netdev: await optionalText(`${target}/net/dev`), collector_metrics: metrics,
      };
      observedStart ??= sampleStart;
      observedEnd = Date.now();
      await appendFile(join(output, "samples.ndjson"), `${JSON.stringify({
        ...sample, sample_end_unix_ms: observedEnd, monotonic_ns: process.hrtime.bigint().toString(),
      })}\n`);
      await new Promise<void>(accept => setTimeout(accept, Math.max(0, Math.min(options.sampleMs - (Date.now() - sampleStart), end - Date.now()))));
    }
    await identity();
    if (await digest(exe) !== options.executableSha256) throw new Error("collector executable changed during observation");
  } catch (error) {
    failure = error;
  } finally {
    process.removeListener("SIGINT", interrupt);
    process.removeListener("SIGTERM", interrupt);
    for (const job of auxiliaryJobs) job.stop();
    const auxiliaryResults = Object.fromEntries(await Promise.all(auxiliaryJobs.map(async job => [job.name, await job.result])));
    const elapsedNs = observedStart !== null && observedEnd !== null ? (observedEnd - observedStart) * 1_000_000 : 0;
    const hotThreads = [...threadCpu.values()].map(thread => ({
      tid: thread.tid, start_ticks: thread.start_ticks, samples: thread.samples,
      observed_cpu_ns: (thread.last_cpu_ns - thread.first_cpu_ns).toString(),
      observed_runqueue_wait_ns: (thread.last_wait_ns - thread.first_wait_ns).toString(),
      observed_cpu_percent_of_full_capture: elapsedNs > 0
        ? Number(thread.last_cpu_ns - thread.first_cpu_ns) / elapsedNs * 100 : null,
    })).sort((a, b) => (b.observed_cpu_percent_of_full_capture ?? 0) - (a.observed_cpu_percent_of_full_capture ?? 0));
    await writeFile(join(output, "completion.json"), `${JSON.stringify({
      complete: failure === null,
      failure: failure instanceof Error ? failure.message : failure === null ? null : String(failure),
      end_unix_ms: Date.now(), auxiliary: auxiliaryResults, cgroup_after: await capturedCgroup(),
      thread_cpu: hotThreads, thread_cpu_scope: "observed cumulative deltas between surviving same-start-tick samples; CPU before first/after last sample and single-sample threads excluded",
      observed_interval: { start_unix_ms: observedStart, end_unix_ms: observedEnd },
    }, null, 2)}\n`, { flag: "wx" });
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
