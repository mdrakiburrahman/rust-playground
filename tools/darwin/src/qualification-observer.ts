import { execFile } from "node:child_process";
import { readFile, readdir, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { launchObserver, projectName, type ObserverEvidence } from "./observer-supervisor.js";
import { sha256 } from "./replay.js";

const exec = promisify(execFile);
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export interface ObservedArmOptions {
  collectorImageId: string;
  collectorRevision: string;
  executableSha256: string;
  observerImageId: string;
  observerBundlePath: string;
  observerBundleSha256: string;
  observerSourcePath: string;
  observerSourceSha256: string;
  deadlineUnixMs: number;
}

async function writeBarrier(path: string, value: unknown): Promise<void> {
  const temporary = `${path}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value)}\n`, { flag: "wx", mode: 0o400 });
  await rename(temporary, path);
}

export async function runObservedArm(
  root: string, run: () => Promise<void>, options: ObservedArmOptions,
): Promise<ObserverEvidence> {
  const configPath = join(root, "config.json");
  const configSha256 = await sha256(configPath);
  const config: unknown = JSON.parse(await readFile(configPath, "utf8"));
  if (!config || typeof config !== "object" || Array.isArray(config)) throw new Error("invalid observer config");
  const platform = Reflect.get(config, "platform");
  const base: unknown = platform && typeof platform === "object" ? Reflect.get(platform, "compose_project") : undefined;
  if (!platform || typeof platform !== "object" || Array.isArray(platform)
    || typeof base !== "string"
    || Reflect.get(platform, "compose_state_root") !== join(root, "state")
    || Reflect.get(config, "warmup_seconds") !== 60 || Reflect.get(config, "measure_seconds") !== 150) {
    throw new Error("observer config does not match the frozen local schedule");
  }
  const outcome: { settled: boolean; failed: boolean; error: unknown } = { settled: false, failed: false, error: null };
  const running = run().then(() => { outcome.settled = true; },
    error => { outcome.settled = true; outcome.failed = true; outcome.error = error; });
  let observer: Awaited<ReturnType<typeof launchObserver>> | undefined;
  const checkRunning = (): void => {
    if (outcome.settled) throw outcome.error ?? new Error("OAT exited before observer completion");
    if (Date.now() >= options.deadlineUnixMs) throw new Error("arm deadline expired before observer completion");
  };
  try {
    let runId = "";
    while (!runId) {
      checkRunning();
      let entries: string[];
      try { entries = await readdir(join(root, "state")); }
      catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
        await sleep(250);
        continue;
      }
      const candidates = entries.filter(entry => UUID.test(entry));
      if (candidates.length > 1) throw new Error("multiple OAT cells found under a fresh arm root");
      runId = candidates[0] ?? "";
      if (!runId) await sleep(250);
    }
    const project = projectName(base, runId);
    for (;;) {
      checkRunning();
      const { stdout } = await exec("docker", ["ps", "--no-trunc", "--quiet",
        "--filter", `label=com.docker.compose.project=${project}`,
        "--filter", "label=com.docker.compose.service=dataflow-1"], { timeout: 15_000 });
      const containers = stdout.trim().split(/\s+/).filter(Boolean);
      if (containers.length > 1) throw new Error("multiple collectors found in the arm");
      if (containers.length === 1) break;
      await sleep(250);
    }
    observer = await launchObserver({
      composeProjectBase: base, cellRunId: runId, configPath, configSha256,
      collectorImageId: options.collectorImageId, collectorRevision: options.collectorRevision,
      executableSha256: options.executableSha256, observerImageId: options.observerImageId,
      observerBundlePath: options.observerBundlePath, observerBundleSha256: options.observerBundleSha256,
      observerSourcePath: options.observerSourcePath, observerSourceSha256: options.observerSourceSha256,
      durationSeconds: 3600, sampleMs: 1000, cpuList: null, readyTimeoutMs: 120_000,
    });
    const state = join(root, "state", runId);
    await writeBarrier(join(state, "observer-ready.json"), {
      version: 1, run_id: runId, collector_image_id: options.collectorImageId,
      collector_container_id: observer.collectorContainerId, observer_container_id: observer.containerId,
      ready_unix_ms: observer.readyUnixMs,
    });
    let start = 0;
    while (!start) {
      checkRunning();
      let schedule: string;
      try { schedule = (await readFile(join(state, "control", "schedule"), "utf8")).trim(); }
      catch (error) {
        if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
        await sleep(250);
        continue;
      }
      if (schedule !== "pending") {
        if (!/^[1-9][0-9]*$/.test(schedule) || !Number.isSafeInteger(Number(schedule))) {
          throw new Error("invalid observed schedule");
        }
        start = Number(schedule);
      } else await sleep(250);
    }
    const end = start + 210_000;
    if (!Number.isSafeInteger(end) || end >= options.deadlineUnixMs) throw new Error("scheduled window exceeds arm deadline");
    while (Date.now() < end) { checkRunning(); await sleep(Math.min(1000, end - Date.now())); }
    for (;;) {
      checkRunning();
      if ((await observer.latestSample()).endUnixMs >= end) break;
      await sleep(200);
    }
    await observer.finish();
    await writeBarrier(join(state, "observer-done.json"), {
      version: 1, run_id: runId, observer_container_id: observer.containerId, finished_unix_ms: Date.now(),
    });
    await running;
    if (outcome.failed) throw outcome.error;
    return await observer.collect(join(root, "observer"), join(root, "results.parquet"), 5000);
  } finally {
    await running;
    if (observer) await observer.stop();
  }
}
