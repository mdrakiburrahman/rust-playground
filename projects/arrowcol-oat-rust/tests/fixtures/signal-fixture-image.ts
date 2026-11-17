import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";

const image = process.env.OAT_SIGNAL_FIXTURE_IMAGE;
assert.match(image ?? "", /^sha256:[a-f0-9]{64}$/, "supply an actual local Docker image ID, not a registry tag/digest");
const uid = "00000000-0000-4000-8000-000000000001";
const otherUid = "00000000-0000-4000-8000-000000000002";

function docker(args: string[], allowFailure = false) {
  const result = spawnSync("docker", args, { encoding: "utf8", timeout: 15_000, maxBuffer: 128 * 1024 });
  if (result.error) throw result.error;
  assert.equal(result.signal, null, "Docker command must complete inside its original timeout");
  if (!allowFailure) assert.equal(result.status, 0, `${args[0]}: ${result.stderr}`);
  return { code: result.status, stdout: result.stdout, stderr: result.stderr };
}

interface Container {
  Id: string;
  Image: string;
  Path: string;
  Args: string[];
  Config: { User: string; Labels: Record<string, string>; Env: string[] };
  HostConfig: { PidMode: string; NetworkMode: string; ReadonlyRootfs: boolean; CapDrop: string[]; SecurityOpt: string[]; Memory: number; NanoCpus: number };
  State: { Running: boolean; Paused: boolean; ExitCode: number };
}

function inspect(id: string): Container {
  const values: Container[] = JSON.parse(docker(["inspect", "--type", "container", id]).stdout);
  assert.equal(values.length, 1);
  return values[0]!;
}

async function until<T>(observe: () => T, accept: (value: T) => boolean, label: string): Promise<T> {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const value = observe();
    if (accept(value)) return value;
    await delay(50);
  }
  assert.fail(`original observation deadline expired: ${label}`);
}

async function owned(podUid: string | null, run: (id: string) => Promise<void>) {
  const owner = randomUUID();
  const name = `e1-d47-signal-${owner}`;
  let id: string | undefined;
  try {
    const args = [
      "run", "--detach", "--name", name,
      "--label", `io.arrowcol.signal-witness=${owner}`,
      "--network", "none", "--read-only", "--cap-drop", "ALL",
      "--security-opt", "no-new-privileges", "--user", "10001:10001",
      "--memory", "64m", "--cpus", "0.1", "--pids-limit", "8",
    ];
    if (podUid !== null) args.push("--env", `OAT_FIXTURE_POD_UID=${podUid}`);
    args.push(image!, "wait");
    id = docker(args).stdout.trim();
    assert.match(id, /^[a-f0-9]{64}$/);
    const actual = inspect(id);
    assert.equal(actual.Image, image);
    assert.equal(actual.Path, "/oat-signal-fixture");
    assert.deepEqual(actual.Args, ["wait"]);
    assert.equal(actual.Config.Labels["io.arrowcol.signal-witness"], owner);
    assert.equal(actual.Config.User, "10001:10001");
    assert.equal(actual.HostConfig.PidMode, "", "never share a host or another container PID namespace");
    assert.equal(actual.HostConfig.NetworkMode, "none");
    assert.equal(actual.HostConfig.ReadonlyRootfs, true);
    assert.deepEqual(actual.HostConfig.CapDrop, ["ALL"]);
    assert.ok(actual.HostConfig.SecurityOpt.includes("no-new-privileges"));
    assert.equal(actual.HostConfig.Memory, 64 * 1024 * 1024);
    assert.equal(actual.HostConfig.NanoCpus, 100_000_000);
    await run(id);
  } finally {
    const remaining = docker(["inspect", "--type", "container", id ?? name], true);
    if (remaining.code === 0) {
      const actual: Container[] = JSON.parse(remaining.stdout);
      assert.equal(actual.length, 1);
      assert.equal(actual[0]!.Config.Labels["io.arrowcol.signal-witness"], owner, "never clean a foreign container");
      docker(["rm", "--force", actual[0]!.Id]);
      console.log(JSON.stringify({ event: "owned_container_removed", container_id: actual[0]!.Id, owner }));
    } else {
      assert.match(remaining.stderr, /No such (object|container)/, "unexpected cleanup inspection failure");
    }
  }
}

function events(id: string) {
  const result = docker(["logs", id]);
  return result.stdout.trim().split("\n").filter(Boolean).map(line => JSON.parse(line) as { event: string; pod_uid: string; elapsed_ms: number });
}

async function ready(id: string) {
  await until(() => events(id), values => values.some(value => value.event === "ready"), "handlers ready");
  assert.equal(inspect(id).State.Running, true);
}

function signal(id: string, value: "TERM" | "USR1" | "INT" | "STOP" | "CONT") {
  docker(["kill", `--signal=${value}`, id]);
}

test("real private PID1 rejects early/queued release, holds repeated TERM and releases exact UID", { timeout: 45_000 }, async () => {
  await owned(uid, async id => {
    await ready(id);
    const wrong = docker(["exec", id, "/oat-signal-fixture", "release", otherUid], true);
    assert.notEqual(wrong.code, 0);
    assert.match(wrong.stderr, /UID does not match/);
    const substituted = docker(["exec", "--env", `OAT_FIXTURE_POD_UID=${otherUid}`, id, "/oat-signal-fixture", "release", otherUid], true);
    assert.notEqual(substituted.code, 0);
    assert.match(substituted.stderr, /PID1 Downward-API pod UID does not match/);
    const notPid1 = docker(["exec", id, "/oat-signal-fixture", "wait"], true);
    assert.notEqual(notPid1.code, 0);
    assert.match(notPid1.stderr, /wait must be PID1/);
    const early = docker(["exec", id, "/oat-signal-fixture", "release", uid], true);
    assert.equal(early.code, 0, early.stderr);
    await until(() => events(id), values => values.some(value => value.event === "early_release_invalid"), "early release invalid");
    assert.equal(inspect(id).State.Running, true);

    // Docker kill unpauses cgroup-paused containers. Observe a real SIGSTOP
    // instead, and never use the reported host PIDs as signal targets.
    const processStates = () => docker(["top", id, "-eo", "pid,stat,comm"]).stdout.trim().split("\n").slice(1)
      .map(line => line.trim().split(/\s+/)[1]!);
    signal(id, "STOP");
    const before = await until(processStates, values => values.length > 0 && values.every(value => value.startsWith("T")), "owned process actually stopped");
    signal(id, "USR1");
    signal(id, "TERM");
    const after = processStates();
    assert.ok(after.length > 0 && after.every(value => value.startsWith("T")), "both signals must remain queued until CONT");
    console.log(JSON.stringify({ event: "queued_signal_witness", container_id: id, before, after }));
    signal(id, "CONT");
    await until(() => events(id), values => values.some(value => value.event === "term_seen"), "queued TERM observed");
    signal(id, "TERM");
    signal(id, "TERM");
    await delay(1000);
    assert.equal(inspect(id).State.Running, true, "pre-TERM USR1 must not latch, repeated TERM must not release");
    assert.equal(events(id).filter(value => value.event === "term_seen").length, 1);
    console.log(JSON.stringify({ event: "bounded_container_stats", container_id: id, stats: JSON.parse(docker(["stats", "--no-stream", "--format", "{{json .}}", id]).stdout) }));
    const release = docker(["exec", id, "/oat-signal-fixture", "release", uid], true);
    const terminal = await until(() => inspect(id).State, value => !value.Running, "actual successful release exit");
    assert.equal(terminal.ExitCode, 0);
    assert.ok(events(id).some(value => value.event === "release_seen"));
    console.log(JSON.stringify({ event: "signal_witness", container_id: id, image_id: image, release_exec_code: release.code, terminal, events: events(id) }));
    assert.equal(release.code, 0, `release exec raced PID1 exit: ${release.stderr}`);
  });
});

test("SIGINT is a real nonzero failure, never successful release", { timeout: 25_000 }, async () => {
  await owned(uid, async id => {
    await ready(id);
    signal(id, "TERM");
    await until(() => events(id), values => values.some(value => value.event === "term_seen"), "TERM before INT");
    signal(id, "INT");
    const terminal = await until(() => inspect(id).State, value => !value.Running, "INT failure");
    assert.notEqual(terminal.ExitCode, 0);
    assert.ok(!events(id).some(value => value.event === "release_seen"));
    console.log(JSON.stringify({ event: "sigint_failure_witness", container_id: id, terminal, events: events(id) }));
  });
});

test("missing Downward-API identity fails before readiness", { timeout: 20_000 }, async () => {
  await owned(null, async id => {
    const terminal = await until(() => inspect(id).State, value => !value.Running, "missing UID refusal");
    assert.notEqual(terminal.ExitCode, 0);
    assert.equal(events(id).length, 0);
    assert.match(docker(["logs", id]).stderr, /missing Downward-API pod UID/);
  });
});
