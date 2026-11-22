import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { allowedCpus, cpuList, observe, processStartTime } from "./physics-observer.js";

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

test("collector-only intervention preserves observer affinity and restores every target thread", { skip: process.platform !== "linux" }, async () => {
  const root = await mkdtemp(join(tmpdir(), "physics-observer-"));
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], { stdio: "ignore" });
  assert.ok(child.pid);
  const ended = new Promise<void>(accept => { child.once("exit", () => accept()); });
  try {
    await new Promise<void>(accept => setTimeout(accept, 250));
    const original = allowedCpus(await readFile(`/proc/${child.pid}/status`, "utf8"));
    const observer = allowedCpus(await readFile("/proc/self/status", "utf8"));
    const sha256 = createHash("sha256").update(await readFile(`/proc/${child.pid}/exe`)).digest("hex");
    const options = {
      pid: child.pid, executableSha256: sha256, cpuList: String(cpuList(original)[0]),
      startUnixMs: 0, durationSeconds: 1, output: join(root, "capture"), metricsUrl: null, sampleMs: 100, profile: false,
    };
    await assert.rejects(observe({ ...options, executableSha256: "0".repeat(64) }), /SHA256 mismatch/);
    await assert.rejects(observe({ ...options, cpuList: "65535" }), /allowed allocation/);
    await observe(options);
    assert.equal(allowedCpus(await readFile(`/proc/${child.pid}/status`, "utf8")), original);
    assert.equal(allowedCpus(await readFile("/proc/self/status", "utf8")), observer);
    const completion = JSON.parse(await readFile(join(root, "capture", "completion.json"), "utf8"));
    assert.equal(completion.complete, true);
    assert.ok(completion.restores.length > 0);
    const samples = (await readFile(join(root, "capture", "samples.ndjson"), "utf8")).trim().split("\n").map(line => JSON.parse(line));
    assert.ok(samples.length >= 2);
    assert.ok(samples.every(sample => sample.threads.every((thread: { affinity: string }) => thread.affinity === options.cpuList)));
    const metadata = JSON.parse(await readFile(join(root, "capture", "metadata.json"), "utf8"));
    assert.equal(metadata.memory_bandwidth.value, null);
    assert.equal(metadata.durability_scope.startsWith("none;"), true);
  } finally {
    child.kill("SIGTERM");
    await ended;
    await rm(root, { recursive: true, force: true });
  }
});
