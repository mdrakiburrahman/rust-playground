import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { allowedCpus, auxiliary, cgroupDirectory, cpuList, observe, processStartTime } from "./physics-observer.js";

test("CPU allocation rejects ambiguous, overlapping or inverted ranges", () => {
  assert.deepEqual(cpuList("2,4-6"), [2, 4, 5, 6]);
  for (const value of ["", "01", "1-", "4-2", "1,1", "1-3,3", "-1", "65536", "1.5"]) {
    assert.throws(() => cpuList(value));
  }
  assert.equal(allowedCpus("Name:\tcollector\nCpus_allowed_list:\t2,4-6\n"), "2,4,5,6");
  assert.throws(() => allowedCpus("Name:\tcollector"));
});

test("PID start identity handles spaces and parentheses in comm", () => {
  const fields = ["S", ...Array.from({ length: 18 }, () => "0"), "1234"];
  assert.equal(processStartTime(`42 (collector (worker)) ${fields.join(" ")}`), "1234");
  assert.throws(() => processStartTime("42 (collector) S 0"));
});

test("cgroup placement is resolved against observer mount root, never a guessed system path", () => {
  const mount = "42 1 0:29 /docker/collector /sys/fs/cgroup rw - cgroup2 cgroup rw";
  assert.equal(cgroupDirectory("0::/docker/collector\n", mount), "/sys/fs/cgroup");
  assert.equal(cgroupDirectory("0::/docker/collector/worker\n", mount), "/sys/fs/cgroup/worker");
  assert.equal(cgroupDirectory("0::/docker/client\n", mount), null);
  assert.equal(cgroupDirectory("0::/docker/collector/../client\n", mount), null);
});

test("read-only observer accepts thread churn and preserves run-created and observer affinity", { skip: process.platform !== "linux" }, async () => {
  const root = await mkdtemp(join(tmpdir(), "physics-observer-"));
  const observer = allowedCpus(await readFile("/proc/self/status", "utf8"));
  const cap = String(cpuList(observer)[0]);
  const child = spawn("taskset", ["-c", cap, process.execPath, "-e",
    "const { Worker } = require('node:worker_threads'); setInterval(() => new Worker('setTimeout(() => {}, 300)', { eval: true }), 250);",
  ], { stdio: "ignore" });
  assert.ok(child.pid);
  const ended = new Promise<void>(accept => { child.once("exit", () => accept()); });
  try {
    await new Promise<void>(accept => setTimeout(accept, 250));
    const original = allowedCpus(await readFile(`/proc/${child.pid}/status`, "utf8"));
    const sha256 = createHash("sha256").update(await readFile(`/proc/${child.pid}/exe`)).digest("hex");
    const options = {
      pid: child.pid, executableSha256: sha256, cpuList: cap,
      startUnixMs: 0, durationSeconds: 2, output: join(root, "capture"), metricsUrl: null, sampleMs: 100, profile: false,
    };
    await assert.rejects(observe({ ...options, executableSha256: "0".repeat(64) }), /SHA256 mismatch/);
    await assert.rejects(observe({ ...options, cpuList: "65535" }), /run-scoped CPU allocation/);
    await observe(options);
    assert.equal(allowedCpus(await readFile(`/proc/${child.pid}/status`, "utf8")), original);
    assert.equal(allowedCpus(await readFile("/proc/self/status", "utf8")), observer);
    const completion = JSON.parse(await readFile(join(root, "capture", "completion.json"), "utf8"));
    assert.equal(completion.complete, true);
    const samples = (await readFile(join(root, "capture", "samples.ndjson"), "utf8")).trim().split("\n").map(line => JSON.parse(line));
    assert.ok(samples.length >= 2);
    assert.ok(samples.every(sample => sample.threads.every((thread: { affinity: string }) => thread.affinity === options.cpuList)));
    assert.ok(samples.every(sample => sample.sample_end_unix_ms >= sample.sample_start_unix_ms));
    assert.ok(new Set(samples.map(sample => sample.threads.map((thread: { tid: string; startTicks: string }) =>
      `${thread.tid}:${thread.startTicks}`).join(","))).size > 1, "thread churn was not observed");
    const metadata = JSON.parse(await readFile(join(root, "capture", "metadata.json"), "utf8"));
    assert.equal(metadata.memory_bandwidth.value, null);
    assert.equal(metadata.durability_scope.startsWith("none;"), true);
  } finally {
    child.kill("SIGTERM");
    await ended;
    await rm(root, { recursive: true, force: true });
  }
});

test("auxiliary failure cleanup stops and reaps only the owned process with a bounded wait", async () => {
  const root = await mkdtemp(join(tmpdir(), "physics-auxiliary-"));
  try {
    const job = auxiliary("owned", process.execPath, ["-e", "process.on('SIGINT', () => {}); setInterval(() => {}, 1000)"], root);
    await new Promise<void>(accept => setTimeout(accept, 250));
    const start = Date.now();
    job.stop();
    const result = await job.result;
    assert.ok(Date.now() - start < 2500);
    assert.equal(result.value, null);
    assert.match(result.unavailable_reason ?? "", /SIGKILL/);
    job.stop();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
