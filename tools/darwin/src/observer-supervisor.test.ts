import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import test from "node:test";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { Table as WasmTable, writeParquet } from "parquet-wasm";
import { projectName, readLastCompleteSample, verifyObserverEvidence, type ObserverLaunch } from "./observer-supervisor.js";

const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const cell = "123e4567-e89b-42d3-a456-426614174000";
const image = `sha256:${"a".repeat(64)}`;
const revision = "b".repeat(40);

async function fixture() {
  const root = await mkdtemp(join(process.cwd(), ".observer-test-"));
  const config = JSON.stringify({ warmup_seconds: 60, measure_seconds: 150, platform: { compose_project: "arrowcol-oat-historical-parity" } });
  const options: ObserverLaunch = {
    composeProjectBase: "arrowcol-oat-historical-parity", cellRunId: cell,
    collectorImageId: image, collectorRevision: revision, executableSha256: "c".repeat(64),
    observerImageId: image, observerBundlePath: join(root, "bundle.mjs"), observerBundleSha256: sha("bundle"),
    observerSourcePath: join(root, "source.ts"), observerSourceSha256: sha("source"),
    configPath: join(root, "config.json"), configSha256: sha(config), durationSeconds: 240, sampleMs: 1000,
    cpuList: null, readyTimeoutMs: 1000,
  };
  const inputs: [string, string][] = [["bundle.mjs", "bundle"], ["source.ts", "source"], ["config.json", config]];
  await Promise.all(inputs.map(([file, value]) => writeFile(join(root, file), value)));
  const observed = (value = "present"): { value: string | null; unavailable_reason: string | null } => ({ value, unavailable_reason: null });
  const samples = Array.from({ length: 213 }, (_, i) => ({
    sample_start_unix_ms: 99000 + i * 1000, sample_end_unix_ms: 99010 + i * 1000,
    monotonic_ns: String(BigInt(i + 1) * 1_000_000_000n),
    threads: [{ tid: "31", startTicks: "200", schedstat: observed("1 2 3\n"), comm: observed(), stat: observed() }],
    collector_io: observed(), system_diskstats: observed(), collector_namespace_netdev: observed(), collector_metrics: observed(),
    cgroup: Object.fromEntries(["cpu.max", "cpu.stat", "cpuset.cpus.effective", "memory.current", "memory.max", "memory.events", "io.stat"].map(name => [name, observed()])),
  }));
  const metadata = { version: 1, collector_pid: 31, collector_executable_sha256: options.executableSha256,
    collector_start_ticks: "200", interval: { sample_ms: 1000, duration_seconds: 240, start_unix_ms: 0 } };
  const completion = { complete: true, failure: null, end_unix_ms: 311020,
    observed_interval: { start_unix_ms: 99000, end_unix_ms: 311010 } };
  const oat = { client_schedule: { effective: { run_start_unix_ms: 100000, measurement_start_unix_ms: 160000, measurement_end_unix_ms: 310000 } },
    durable_commits: { output_throughput: { measurement_start_unix_ms: 160000, measurement_end_unix_ms: 310000 } },
    processes: [{ component: "collector", image_id: image, source_revision: revision, log_complete: true }] };
  const writeResult = async () => {
    const table = tableFromArrays({ schema_version: [3], mode: ["local"], status: ["succeeded"], run_id: [cell], evidence_json: [JSON.stringify(oat)] });
    await writeFile(join(root, "result.parquet"), writeParquet(WasmTable.fromIPCStream(tableToIPC(table, "stream"))));
  };
  const writeSamples = () => writeFile(join(root, "samples.ndjson"), samples.map(x => JSON.stringify(x)).join("\n") + "\n");
  await Promise.all([writeResult(), writeSamples(), writeFile(join(root, "metadata.json"), JSON.stringify(metadata)),
    writeFile(join(root, "ready.json"), JSON.stringify({ ready_unix_ms: 98900 })),
    writeFile(join(root, "completion.json"), JSON.stringify(completion))]);
  const verify = () => verifyObserverEvidence(root, join(root, "result.parquet"), options, { id: "d".repeat(64), initPid: 30, pid: 31, startTicks: "200" }, 5000);
  return { root, options, samples, completion, oat, writeSamples, writeResult, verify };
}

test("binds real observer bytes and raw OAT schedule and image", async () => {
  const f = await fixture();
  try {
    const result = await f.verify();
    assert.equal(result.startUnixMs, 99000);
    assert.equal(result.endUnixMs, 311010);
    assert.equal(result.maxGapMs, 1000);
    assert.equal(result.collectorImageId, image);
    assert.equal(result.artifacts["samples.ndjson"], createHash("sha256").update(await readFile(join(f.root, "samples.ndjson"))).digest("hex"));
    f.oat.processes[0].source_revision = "f".repeat(40);
    await f.writeResult();
    await assert.rejects(f.verify(), /collector identity/);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test("preserves unavailable optional metrics but rejects missing reasons and partial records", async () => {
  const f = await fixture();
  try {
    f.samples[0].threads[0].schedstat = { value: null, unavailable_reason: "thread exited" };
    f.samples[0].collector_io = { value: null, unavailable_reason: "permission denied" };
    f.samples[0].cgroup["cpu.stat"] = { value: null, unavailable_reason: "not mounted" };
    await f.writeSamples();
    assert.equal((await f.verify()).complete, true);
    f.samples[0].threads[0].schedstat.unavailable_reason = null;
    await f.writeSamples();
    await assert.rejects(f.verify(), /schedstat observation/);
    f.samples[0].threads[0].schedstat.unavailable_reason = "thread exited";
    await f.writeSamples();
    const path = join(f.root, "samples.ndjson");
    const contents = await readFile(path, "utf8");
    await writeFile(path, contents.slice(0, -1));
    await assert.rejects(f.verify(), /incomplete sample record/);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test("rejects sample gaps, missing accounting, early termination and altered input", async () => {
  const f = await fixture();
  try {
    f.samples.splice(10, 6);
    await f.writeSamples();
    await assert.rejects(f.verify(), /bounded run/);
    f.samples[0].threads[0].schedstat.value = "";
    await f.writeSamples();
    await assert.rejects(f.verify(), /thread accounting/);
    f.completion.complete = false;
    await writeFile(join(f.root, "completion.json"), JSON.stringify(f.completion));
    await assert.rejects(f.verify(), /completion mismatch/);
    await writeFile(f.options.configPath, "{}");
    await assert.rejects(f.verify(), /input hash mismatch/);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test("rejects forged coverage and clock discontinuity", async () => {
  const f = await fixture();
  try {
    await writeFile(join(f.root, "ready.json"), JSON.stringify({ ready_unix_ms: 100001 }));
    await assert.rejects(f.verify(), /bounded run/);
    await writeFile(join(f.root, "ready.json"), JSON.stringify({ ready_unix_ms: 98900 }));
    f.samples[20].monotonic_ns = f.samples[19].monotonic_ns;
    await f.writeSamples();
    await assert.rejects(f.verify(), /clock or order/);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test("host-PID startup observes an exactly labeled test process before admission", { skip: !process.env.DARWIN_OBSERVER_TEST_IMAGE }, async t => {
  const { execFileSync, spawn } = await import("node:child_process");
  const { launchObserver } = await import("./observer-supervisor.js");
  const imageId = process.env.DARWIN_OBSERVER_TEST_IMAGE;
  if (!imageId) throw new Error("Docker test image is required");
  const root = await mkdtemp(join(process.cwd(), ".observer-live-test-"));
  const docker = (...args: string[]) => execFileSync("docker", args, { encoding: "utf8", timeout: 30000 }).trim();
  let collector = "";
  let stop: (() => Promise<void>) | undefined;
  try {
    const source = await readFile(join(process.cwd(), "tools/darwin/src/physics-observer.ts"), "utf8");
    const sourcePath = join(root, "physics-observer.ts");
    await writeFile(sourcePath, source);
    const cooperative = source.includes("SIGUSR1");
    if (process.env.DARWIN_CHURN_ONLY) t.diagnostic(`observer source sha256=${sha(source)} ESRCH-mentioned=${source.includes("ESRCH")}`);
    const bundlePath = join(root, "observer.mjs");
    execFileSync("node_modules/.bin/esbuild", [sourcePath, "--bundle", "--platform=node", "--format=esm", `--outfile=${bundlePath}`], { stdio: "ignore" });
    const configPath = join(root, "config.json");
    await writeFile(configPath, JSON.stringify({ warmup_seconds: 60, measure_seconds: 150, platform: { compose_project: "observer-test" } }));
    const project = projectName("observer-test", cell);
    const churnMs = Number(process.env.DARWIN_OBSERVER_CHURN_MS ?? "80");
    assert.ok(Number.isInteger(churnMs) && churnMs >= 20 && churnMs <= 1000);
    const collectorCode = `const {Worker}=require("node:worker_threads");setInterval(()=>{const w=new Worker("setTimeout(()=>{},25)",{eval:true});w.on("error",()=>{});},${churnMs})`;
    collector = docker("run", "--pull", "never", "-d", "--network", "none", "--entrypoint", "bash",
      "--label", `com.docker.compose.project=${project}`, "--label", "com.docker.compose.service=dataflow-1",
      "--label", `org.opencontainers.image.revision=${revision}`, imageId, "-c", `node -e '${collectorCode}' & wait`);
    const monitorCode = `const fs=require("node:fs"), seen=new Set(), tids=new Set();let n=0;
      const p=fs.readFileSync("/proc/1/task/1/children","utf8").trim().split(/\\s+/)[0];
      const timer=setInterval(()=>{const list=fs.readdirSync("/proc/"+p+"/task").sort();seen.add(list.join(","));
      for(const id of list)tids.add(id);if(++n===300){clearInterval(timer);console.log(JSON.stringify({samples:n,snapshots:seen.size,tids:tids.size}));}},40);`;
    const monitor = spawn("docker", ["exec", collector, "node", "-e", monitorCode], { stdio: ["ignore", "pipe", "pipe"] });
    const monitorResult = new Promise<{ code: number | null; stdout: string; stderr: string }>(resolve => {
      let stdout = "", stderr = "";
      monitor.stdout.setEncoding("utf8").on("data", chunk => { stdout += chunk; });
      monitor.stderr.setEncoding("utf8").on("data", chunk => { stderr += chunk; });
      monitor.once("error", error => resolve({ code: null, stdout, stderr: error.message }));
      monitor.once("close", code => resolve({ code, stdout, stderr }));
    });
    const executableHash = docker("exec", collector, "sh", "-c", "sha256sum $(command -v node)").split(" ")[0];
    const options: ObserverLaunch = { composeProjectBase: "observer-test", cellRunId: cell,
      collectorImageId: imageId, collectorRevision: revision, executableSha256: executableHash,
      observerImageId: imageId, observerBundlePath: bundlePath, observerBundleSha256: sha((await readFile(bundlePath)).toString()),
      observerSourcePath: sourcePath, observerSourceSha256: sha(source),
      configPath, configSha256: sha((await readFile(configPath)).toString()), durationSeconds: cooperative ? 30 : 3, sampleMs: 200,
      cpuList: null, readyTimeoutMs: 10000 };
    const supervisor = await launchObserver(options);
    stop = supervisor.stop;
    assert.equal(supervisor.collectorContainerId, collector);
    assert.notEqual(supervisor.collectorInitPid, supervisor.collectorPid);
    assert.equal(monitor.exitCode, null, "thread monitor must span all launch probes");
    if (process.env.DARWIN_OBSERVER_DELAY_FINISH) await monitorResult;
    let sample: { startUnixMs: number; endUnixMs: number };
    try { sample = await supervisor.latestSample(); }
    catch (error) {
      const { spawnSync } = await import("node:child_process");
      const logs = spawnSync("docker", ["logs", supervisor.containerId], { encoding: "utf8", timeout: 15000 });
      t.diagnostic(`observer stdout: ${logs.stdout}; stderr: ${logs.stderr}`);
      const completionPath = join(root, "failed-completion.json");
      try {
        docker("cp", `${supervisor.containerId}:/capture/completion.json`, completionPath);
        t.diagnostic(`observer completion: ${await readFile(completionPath, "utf8")}`);
      } catch (captureError) { t.diagnostic(`completion capture failed: ${String(captureError)}`); }
      throw error;
    }
    assert.ok(sample.endUnixMs >= sample.startUnixMs && sample.startUnixMs >= supervisor.readyUnixMs);
    if (cooperative) {
      await new Promise(resolve => setTimeout(resolve, 500));
      await supervisor.finish();
    }
    const monitored = await monitorResult;
    assert.equal(monitored.code, 0, monitored.stderr);
    const activity: unknown = JSON.parse(monitored.stdout);
    assert.ok(activity !== null && typeof activity === "object" && "samples" in activity && "snapshots" in activity
      && "tids" in activity && typeof activity.snapshots === "number" && activity.snapshots > 1
      && typeof activity.tids === "number" && activity.tids > 2);
    t.diagnostic(`host-PID descendant probe completed three double scans during thread churn: ${monitored.stdout.trim()}`);
    if (process.env.DARWIN_CHURN_ONLY) return;
    assert.equal(docker("inspect", "-f", "{{.HostConfig.PidMode}}", supervisor.containerId), "host");
    assert.equal(docker("inspect", "-f", "{{.HostConfig.NetworkMode}}", supervisor.containerId), "none");
    const fixtureData = await fixture();
    try {
      const sampleBytes = JSON.stringify(fixtureData.samples[0]);
      const malformedPath = join(root, "malformed.mjs");
      const malformed = `import {mkdirSync,writeFileSync} from "node:fs";mkdirSync("/capture");
        writeFileSync("/capture/ready.json", "{broken}\\n");setInterval(()=>{},1000);`;
      await writeFile(malformedPath, malformed);
      await assert.rejects(launchObserver({ ...options, observerBundlePath: malformedPath, observerBundleSha256: sha(malformed) }), SyntaxError);
      const noNodePath = join(root, "no-node.mjs");
      const noNode = `import {chmodSync} from "node:fs";chmodSync(process.execPath,0);setInterval(()=>{},1000);`;
      await writeFile(noNodePath, noNode);
      await assert.rejects(launchObserver({ ...options, observerBundlePath: noNodePath, observerBundleSha256: sha(noNode) }), /docker exec|observer exited before readiness/i);
      docker("exec", collector, "node", "-e", "require('node:fs').writeFileSync('/sample-fixture', process.argv[1] + '\\n' + '{\"unfinished\":')", sampleBytes);
      assert.deepEqual(await readLastCompleteSample(collector, "/sample-fixture"), { startUnixMs: 99000, endUnixMs: 99010 });
      fixtureData.oat.processes[0].image_id = imageId;
      await fixtureData.writeResult();
      await assert.rejects(supervisor.collect(join(root, "capture"), join(fixtureData.root, "result.parquet"), 5000), /bounded run/);
      const completion: unknown = JSON.parse(await readFile(join(root, "capture", "completion.json"), "utf8"));
      assert.ok(completion !== null && typeof completion === "object" && "complete" in completion && completion.complete === true);
      if (cooperative) assert.ok("finished_by_request" in completion && completion.finished_by_request === true);
      assert.ok((await readFile(join(root, "capture", "samples.ndjson"), "utf8")).trim().split("\n").length >= (cooperative ? 1 : 3));
    } finally { await rm(fixtureData.root, { recursive: true, force: true }); }
    const otherCell = "123e4567-e89b-42d3-a456-426614174001";
    const duplicate = docker("run", "--pull", "never", "-d", "--network", "none", "--entrypoint", "bash",
      "--label", `com.docker.compose.project=${projectName("observer-test", otherCell)}`, "--label", "com.docker.compose.service=dataflow-1",
      "--label", `org.opencontainers.image.revision=${revision}`, imageId, "-c",
      "node -e 'setInterval(() => {}, 1000)' & node -e 'setInterval(() => {}, 1000)' & wait");
    try {
      await new Promise(resolve => setTimeout(resolve, 500));
      await assert.rejects(launchObserver({ ...options, cellRunId: otherCell }), /ambiguous/);
    } finally { docker("rm", "-f", duplicate); }
  } finally {
    if (stop) await stop();
    if (collector) docker("rm", "-f", collector);
    await rm(root, { recursive: true, force: true });
  }
});
