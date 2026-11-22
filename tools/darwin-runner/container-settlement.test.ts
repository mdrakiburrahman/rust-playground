import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { ContainerSettlementSupervisor } from "./container-settlement.js";

const id = "a".repeat(64);
function statePath(t: { after(fn: () => void): void }): string {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-settlement-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return path.join(root, "active.json");
}
const created = {
  Id: id,
  Config: { User: "10001:10001" },
  HostConfig: {
    AutoRemove: false, Privileged: false, Init: true, PidMode: "", NetworkMode: "none",
    IpcMode: "private", UTSMode: "", CgroupnsMode: "private", CapAdd: null,
    CapDrop: ["ALL"], SecurityOpt: ["no-new-privileges"],
    Devices: [], DeviceRequests: null,
    RestartPolicy: { Name: "no" },
  },
  Mounts: [],
  State: {
    Status: "created", Running: false, Paused: false, Restarting: false,
    Dead: false, Pid: 0, ExitCode: 0, OOMKilled: false, Error: "",
  },
};

test("unproved settlement preserves evidence and quarantines even after supervisor restart", async (t) => {
  const calls: string[] = [];
  let inspections = 0;
  const state = statePath(t);
  const supervisor = new ContainerSettlementSupervisor(state, async (args) => {
    calls.push(args[0]!);
    if (args[0] === "inspect") {
      if (inspections++ === 0) return JSON.stringify(created);
      throw new Error("daemon unavailable");
    }
    return "";
  });
  const result = await supervisor.run(id, { timeoutMs: 1000 });
  assert.deepEqual(result, {
    containerId: id, outcome: "launch_failed", exitCode: null, settlement: "unproved", reason: "inspection_failed",
  });
  assert.deepEqual(calls, ["inspect", "start", "inspect", "kill", "inspect"]);
  await assert.rejects(supervisor.run(id, { timeoutMs: 1000 }), /quarantined/);
  assert.deepEqual(JSON.parse(readFileSync(state, "utf8")), { version: 1, ...result });
  const restarted = new ContainerSettlementSupervisor(state, async () => { throw new Error("must not call Docker"); });
  await assert.rejects(restarted.run(id, { timeoutMs: 1000 }), /EEXIST/);
});

test("rejects shared PID, added capabilities and mounts before starting", async (t) => {
  for (const unsafe of [
    { ...created, HostConfig: { ...created.HostConfig, PidMode: "host" } },
    { ...created, HostConfig: { ...created.HostConfig, CapAdd: ["SYS_ADMIN"] } },
    { ...created, Mounts: [{ Destination: "/var/run/docker.sock" }] },
  ]) {
    const calls: string[] = [];
    const supervisor = new ContainerSettlementSupervisor(statePath(t), async (args) => {
      calls.push(args[0]!);
      return JSON.stringify(unsafe);
    });
    assert.equal((await supervisor.run(id, { timeoutMs: 1000 })).settlement, "unproved");
    assert.deepEqual(calls, ["inspect"]);
    await assert.rejects(supervisor.run(id, { timeoutMs: 1000 }), /quarantined/);
  }
});

test("a late exit observation cannot convert a timeout to success", async (t) => {
  let inspections = 0;
  const stopped = {
    ...created,
    State: { ...created.State, Status: "exited" },
  };
  const supervisor = new ContainerSettlementSupervisor(statePath(t), async (args) => {
    if (args[0] !== "inspect") return "";
    if (inspections++ === 0) return JSON.stringify(created);
    if (inspections === 2) await new Promise((resolve) => setTimeout(resolve, 40));
    return JSON.stringify(stopped);
  });
  const receipt = await supervisor.run(id, { timeoutMs: 10 });
  assert.equal(receipt.outcome, "timed_out");
  assert.equal(receipt.settlement, "verified");
  assert.equal(receipt.reason, "timeout");
  await assert.rejects(supervisor.run(id, { timeoutMs: 1000 }), /quarantined/);
});

test("a failed start remains unproved even if a kill request succeeds", async (t) => {
  const calls: string[] = [];
  const supervisor = new ContainerSettlementSupervisor(statePath(t), async (args) => {
    calls.push(args[0]!);
    if (args[0] === "inspect") return JSON.stringify(created);
    if (args[0] === "start") throw new Error("request outcome unknown");
    return "";
  });
  const receipt = await supervisor.run(id, { timeoutMs: 1000 });
  assert.equal(receipt.reason, "start_failed");
  assert.equal(receipt.settlement, "unproved");
  assert.deepEqual(calls, ["inspect", "start", "kill"]);
  await assert.rejects(supervisor.run(id, { timeoutMs: 1000 }), /quarantined/);
});

const image = process.env.DARWIN_SETTLEMENT_TEST_IMAGE;
test("Docker proves completion, descendant cleanup, timeout and cancellation", { skip: !image, timeout: 60_000 }, async (t) => {
  if (!image || !/^sha256:[a-f0-9]{64}$/.test(image)) throw new Error("test image must be an immutable image ID");
  const containerIds: string[] = [];
  const cli = (args: string[]) => execFileSync("/usr/bin/docker", args, {
    encoding: "utf8", timeout: 10_000, stdio: ["ignore", "pipe", "pipe"],
    env: { PATH: "/usr/bin:/bin", ...(process.env.DOCKER_HOST ? { DOCKER_HOST: process.env.DOCKER_HOST } : {}) },
  }).trim();
  const create = (script: string) => {
    const container = cli([
      "create", "--init", "--network", "none", "--cap-drop", "ALL",
      "--security-opt", "no-new-privileges", "--pids-limit", "32", "--memory", "128m",
      "--read-only", "--user", "10001:10001", "--entrypoint", "/bin/sh", image, "-c", script,
    ]);
    assert.match(container, /^[a-f0-9]{64}$/);
    containerIds.push(container);
    return container;
  };
  try {
    const supervisor = new ContainerSettlementSupervisor(statePath(t));
    const normal = create("exit 0");
    assert.deepEqual(await supervisor.run(normal, { timeoutMs: 5000 }), {
      containerId: normal, outcome: "exited", exitCode: 0, settlement: "verified", reason: "completed",
    });
    const leftover = create("sleep 30 & p=$!; kill -0 \"$p\" || exit 1; exit 0");
    assert.deepEqual(await supervisor.run(leftover, { timeoutMs: 5000 }), {
      containerId: leftover, outcome: "exited", exitCode: 0, settlement: "verified", reason: "completed",
    });
    assert.throws(() => cli(["top", leftover]));

    const timed = create("sleep 30 & wait");
    const timeoutSupervisor = new ContainerSettlementSupervisor(statePath(t));
    const timeoutReceipt = await timeoutSupervisor.run(timed, { timeoutMs: 300 });
    assert.equal(timeoutReceipt.outcome, "timed_out");
    assert.equal(timeoutReceipt.settlement, "verified");
    await assert.rejects(timeoutSupervisor.run(normal, { timeoutMs: 1000 }), /quarantined/);

    const cancelled = create("sleep 30 & wait");
    const cancelSupervisor = new ContainerSettlementSupervisor(statePath(t));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 300);
    try {
      const cancelReceipt = await cancelSupervisor.run(cancelled, { timeoutMs: 5000, signal: controller.signal });
      assert.equal(cancelReceipt.outcome, "cancelled");
      assert.equal(cancelReceipt.settlement, "verified");
      await assert.rejects(cancelSupervisor.run(normal, { timeoutMs: 1000 }), /quarantined/);
    } finally { clearTimeout(timer); }

    const interrupted = create("sleep 30 & wait");
    const marker = statePath(t);
    const child = spawn(process.execPath, ["--import", "tsx", "--input-type=module", "-e",
      'import { ContainerSettlementSupervisor } from "./tools/darwin-runner/container-settlement.ts"; await new ContainerSettlementSupervisor(process.argv[1]).run(process.argv[2], { timeoutMs: 30000 });',
      marker, interrupted], {
      env: { PATH: "/usr/bin:/bin", ...(process.env.DOCKER_HOST ? { DOCKER_HOST: process.env.DOCKER_HOST } : {}) },
      stdio: "ignore",
    });
    const childExit = once(child, "exit");
    try {
      let running = false;
      for (let attempt = 0; attempt < 50; attempt++) {
        if (cli(["inspect", "--format", "{{.State.Status}}", interrupted]) === "running") { running = true; break; }
        if (child.exitCode !== null || child.signalCode !== null) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      assert.equal(running, true);
      assert.equal(child.kill("SIGKILL"), true);
      await childExit;
      assert.equal(cli(["inspect", "--format", "{{.State.Status}}", interrupted]), "running");
      await assert.rejects(new ContainerSettlementSupervisor(marker).run(interrupted, { timeoutMs: 1000 }), /EEXIST/);
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill("SIGKILL");
        await childExit;
      }
    }
  } finally {
    for (const container of containerIds) cli(["rm", "-f", container]);
  }
});
