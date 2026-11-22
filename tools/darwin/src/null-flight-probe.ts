import { createHash } from "node:crypto";
import { spawn, execFileSync } from "node:child_process";
import { createWriteStream, openSync } from "node:fs";
import { mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import { cpuList } from "./physics-observer.js";

function integer(value: string | undefined, name: string, max: number): number {
  if (!value || !/^[1-9][0-9]*$/.test(value) || Number(value) > max) throw new Error(`invalid ${name}`);
  return Number(value);
}

function object(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as Record<string, unknown>;
}

export function clientArguments(config: unknown): string[] {
  const settings = object(config, "client config");
  const owned = new Set(["endpoint", "schedule-file", "run-start-unix-ms", "duration-secs", "warmup-secs", "pilot", "edge-secret-file"]);
  return Object.entries(settings).flatMap(([key, value]) => {
    if (!/^[a-z][a-z0-9_]*$/.test(key)) throw new Error(`invalid client config key ${key}`);
    const flag = key.replaceAll("_", "-");
    if (owned.has(flag)) throw new Error(`probe owns ${flag}`);
    if (typeof value !== "string" && typeof value !== "boolean" && !(typeof value === "number" && Number.isFinite(value))) {
      throw new Error(`invalid client config value ${key}`);
    }
    return [`--${flag}`, String(value)];
  });
}

async function sha256(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

function run(command: string, args: string[], log: string): { done: Promise<number>; stop: () => void } {
  const output = createWriteStream(log, { fd: openSync(log, "wx") });
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("ARROWCOL_CLIENT_") && key !== "ARROWCOL_OAT_RUN_ID"));
  const child = spawn(command, args, { env, stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.pipe(output, { end: false });
  child.stderr.pipe(output, { end: false });
  let ended = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const done = new Promise<number>((accept, reject) => {
    child.on("error", reject);
    child.on("close", code => {
      ended = true;
      if (timer !== null) clearTimeout(timer);
      output.end(() => accept(code ?? -1));
    });
  });
  return { done, stop: () => {
    if (ended || timer !== null) return;
    child.kill("SIGTERM");
    timer = setTimeout(() => { if (!ended) child.kill("SIGKILL"); }, 1000);
  } };
}

async function main(): Promise<void> {
  if (process.platform !== "linux") throw new Error("null Flight probe requires Linux taskset");
  const { values } = parseArgs({ strict: true, options: {
    "client-bin": { type: "string" }, "sink-bin": { type: "string" }, "client-cpus": { type: "string" },
    "sink-cpus": { type: "string" }, "client-config": { type: "string" }, output: { type: "string" },
    "duration-seconds": { type: "string", default: "30" }, "warmup-seconds": { type: "string", default: "5" },
    "source-revision": { type: "string" },
  } });
  const revision = values["source-revision"];
  if (!revision || !/^[a-f0-9]{40}$/.test(revision)
    || execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim() !== revision) throw new Error("source revision is not pinned to HEAD");
  if (!values["client-bin"] || !values["sink-bin"] || !values["client-config"] || !values.output
    || !values["client-cpus"] || !values["sink-cpus"]) throw new Error("explicit binaries, CPU allocations, client config and output are required");
  const clientCpus = cpuList(values["client-cpus"]);
  const sinkCpus = cpuList(values["sink-cpus"]);
  if (clientCpus.some(cpu => sinkCpus.includes(cpu))) throw new Error("client and null sink CPU allocations must be disjoint");
  const seconds = integer(values["duration-seconds"], "duration", 600);
  const warmup = integer(values["warmup-seconds"], "warmup", 300);
  const configBytes = await readFile(values["client-config"]);
  const config = object(JSON.parse(configBytes.toString()), "client config");
  if (config.table !== "sqlserver_availability_replica_states" || config.data_profile !== "realistic") {
    throw new Error("capacity proof requires one fixed public DMV table and realistic replay identities");
  }
  const args = clientArguments(config);
  const client = resolve(values["client-bin"]);
  const sink = resolve(values["sink-bin"]);
  const hashes = { client: await sha256(client), sink: await sha256(sink) };
  const output = resolve(values.output);
  await mkdir(output, { recursive: false });
  await writeFile(join(output, "config.json"), configBytes, { flag: "wx" });
  const readyPath = join(output, "sink-ready.json");
  const schedulePath = join(output, "schedule");
  const sinkProcess = run("taskset", ["-c", sinkCpus.join(","), sink, "--ready-file", readyPath,
    "--duration-secs", String(seconds + warmup + 660), "--runtime-threads", String(sinkCpus.length),
  ], join(output, "sink.log"));
  let clientProcess: ReturnType<typeof run> | null = null;
  let sinkExit: number | null = null;
  let sinkFailure: unknown = null;
  void sinkProcess.done.then(code => { sinkExit = code; }, error => { sinkFailure = error; });
  try {
    let ready: Record<string, unknown> | null = null;
    const deadline = Date.now() + 30_000;
    while (ready === null) {
      if (sinkExit !== null || sinkFailure !== null) throw new Error("null sink exited before readiness");
      if (Date.now() > deadline) throw new Error("null sink readiness timed out");
      try {
        await stat(readyPath);
        ready = object(JSON.parse(await readFile(readyPath, "utf8")), "sink ready");
      } catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
        await new Promise<void>(accept => setTimeout(accept, 50));
      }
    }
    if (typeof ready.endpoint !== "string" || !/^http:\/\/127\.0\.0\.1:[1-9][0-9]*$/.test(ready.endpoint)) throw new Error("invalid loopback sink endpoint");
    const clientLog = join(output, "client.log");
    clientProcess = run("taskset", ["-c", clientCpus.join(","), client, ...args,
      "--endpoint", ready.endpoint, "--schedule-file", schedulePath,
      "--duration-secs", String(seconds), "--warmup-secs", String(warmup),
    ], clientLog);
    let clientExit: number | null = null;
    let clientFailure: unknown = null;
    void clientProcess.done.then(code => { clientExit = code; }, error => { clientFailure = error; });
    const prepareDeadline = Date.now() + 600_000;
    while (!(await readFile(clientLog, "utf8")).split("\n").some(line => line.startsWith("OAT_CLIENT_READY "))) {
      if (clientExit !== null || clientFailure !== null) throw new Error("client exited before corpus preparation completed");
      if (Date.now() > prepareDeadline) throw new Error("client preparation timed out");
      await new Promise<void>(accept => setTimeout(accept, 100));
    }
    const start = Date.now() + 1000;
    await writeFile(schedulePath, `${start}\n`, { flag: "wx" });
    const timeout = setTimeout(() => { clientProcess?.stop(); }, (seconds + warmup + 35) * 1000);
    let exit: number;
    try { exit = await clientProcess.done; } finally { clearTimeout(timeout); }
    if (exit !== 0) throw new Error(`null-sink client failed (exit ${exit}); see client.log`);
    if (sinkExit !== null || sinkFailure !== null) throw new Error("null sink ended before client drain");
    const snapshots = (await readFile(clientLog, "utf8")).split("\n").filter(line => line.startsWith("OAT_CLIENT_METRICS "))
      .map(line => object(JSON.parse(line.slice("OAT_CLIENT_METRICS ".length)), "client metrics"));
    const terminal = snapshots.filter(snapshot => snapshot.phase === "completed");
    if (terminal.length !== 1) throw new Error("expected exactly one completed client receipt");
    const metrics = terminal[0]!;
    const manifest = object(metrics.logical_batch_manifest, "logical manifest");
    if (manifest.complete !== true || manifest.truncated !== false) throw new Error("logical replay manifest is incomplete");
    const preparation = object(metrics.preparation, "preparation");
    const window = object(metrics.measurement_window, "measurement window");
    if (window.start_unix_ms !== start + warmup * 1000 || window.end_unix_ms !== start + (warmup + seconds) * 1000
      || typeof window.offered_rows !== "number" || typeof window.completed_rows !== "number"
      || !Number.isSafeInteger(window.offered_rows) || !Number.isSafeInteger(window.completed_rows)
      || window.offered_rows <= 0 || window.completed_rows <= 0) throw new Error("invalid shared-window client counters");
    if (await sha256(client) !== hashes.client || await sha256(sink) !== hashes.sink
      || execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim() !== revision) throw new Error("source/binaries changed during capacity proof");
    await writeFile(join(output, "receipt.json"), `${JSON.stringify({
      scope: "client_to_loopback_decode_discard_sink_not_collector_durability", host: hostname(), source_revision: revision,
      binaries: hashes, client_cpus: clientCpus, sink_cpus: sinkCpus, seconds, warmup_seconds: warmup,
      config_sha256: createHash("sha256").update(configBytes).digest("hex"),
      payload_corpus_sha256: preparation.payload_corpus_sha256,
      offered_rows_per_second: window.offered_rows / seconds,
      admission_completed_rows_per_second: window.completed_rows / seconds,
      durable_rows_per_second: null, durability_unavailable_reason: "discard sink has no Delta table",
      client_metrics: metrics,
    }, null, 2)}\n`, { flag: "wx" });
  } finally {
    clientProcess?.stop();
    sinkProcess.stop();
    await Promise.allSettled([sinkProcess.done, ...(clientProcess ? [clientProcess.done] : [])]);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main().catch(error => { console.error(error); process.exitCode = 1; });
}
