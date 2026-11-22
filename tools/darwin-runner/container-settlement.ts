import { execFile } from "node:child_process";
import { constants } from "node:fs";
import { lstat, open, realpath, unlink, type FileHandle } from "node:fs/promises";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const CONTROL_TIMEOUT_MS = 5_000;
const CLEANUP_TIMEOUT_MS = 10_000;
const POLL_INTERVAL_MS = 50;

export type SettlementOutcome = "exited" | "timed_out" | "cancelled" | "resource_limit" | "launch_failed";
export interface SettlementReceipt {
  containerId: string;
  outcome: SettlementOutcome;
  exitCode: number | null;
  settlement: "verified" | "unproved";
  reason: "completed" | "nonzero_exit" | "oom" | "timeout" | "cancelled" | "preflight_failed" | "start_failed" | "inspection_failed";
}

interface ContainerInspect {
  Id: string;
  Config: { User: string };
  HostConfig: {
    AutoRemove: boolean;
    Privileged: boolean;
    Init: boolean;
    PidMode: string;
    NetworkMode: string;
    IpcMode: string;
    UTSMode: string;
    CgroupnsMode: string;
    CapAdd: string[] | null;
    CapDrop: string[] | null;
    SecurityOpt: string[] | null;
    Devices: unknown[] | null;
    DeviceRequests: unknown[] | null;
    RestartPolicy: { Name: string };
  };
  Mounts: unknown[];
  State: {
    Status: string;
    Running: boolean;
    Paused: boolean;
    Restarting: boolean;
    Dead: boolean;
    Pid: number;
    ExitCode: number;
    OOMKilled: boolean;
    Error: string;
  };
}

type DockerCommand = (args: string[]) => Promise<string>;

async function docker(args: string[]): Promise<string> {
  const env: NodeJS.ProcessEnv = { PATH: "/usr/bin:/bin" };
  if (process.env.DOCKER_HOST) {
    if (!/^unix:\/\/\/[^ \t\r\n]+$/.test(process.env.DOCKER_HOST)) {
      throw new Error("Docker must use a local Unix socket");
    }
    env.DOCKER_HOST = process.env.DOCKER_HOST;
  }
  const { stdout } = await execFileAsync("/usr/bin/docker", args, {
    env, timeout: CONTROL_TIMEOUT_MS, maxBuffer: 128 * 1024,
  });
  return stdout;
}

function isPrivateContainer(value: ContainerInspect, id: string): boolean {
  const host = value.HostConfig;
  const user = /^([1-9][0-9]*)(?::[0-9]+)?$/.test(value.Config?.User ?? "");
  return value.Id === id && user && host?.AutoRemove === false && host.Privileged === false && host.Init === true
    && host.PidMode === "" && host.NetworkMode === "none"
    && host.IpcMode === "private" && host.UTSMode === "" && host.CgroupnsMode === "private"
    && host.CapDrop?.includes("ALL") === true
    && (host.CapAdd === null || (Array.isArray(host.CapAdd) && host.CapAdd.length === 0))
    && host.SecurityOpt?.some((option) => option === "no-new-privileges" || option === "no-new-privileges:true") === true
    && host.SecurityOpt.every((option) => option === "no-new-privileges" || option === "no-new-privileges:true")
    && (host.Devices === null || (Array.isArray(host.Devices) && host.Devices.length === 0))
    && (host.DeviceRequests === null || (Array.isArray(host.DeviceRequests) && host.DeviceRequests.length === 0))
    && host.RestartPolicy?.Name === "no" && Array.isArray(value.Mounts) && value.Mounts.length === 0;
}

function settled(value: ContainerInspect): boolean {
  const state = value.State;
  return state?.Status === "exited" && state.Running === false && state.Paused === false
    && state.Restarting === false && state.Dead === false && state.Pid === 0
    && Number.isSafeInteger(state.ExitCode) && state.ExitCode >= 0 && state.ExitCode <= 255
    && typeof state.OOMKilled === "boolean" && state.Error === "";
}

/**
 * Supervises a trusted, already-created private-PID container. This is not
 * source/executable admission or a fixture/oracle launcher.
 */
export class ContainerSettlementSupervisor {
  private quarantined = false;
  private active = false;

  constructor(private readonly statePath: string, private readonly command: DockerCommand = docker) {}

  private async acquireMarker(id: string): Promise<FileHandle> {
    const directory = path.dirname(this.statePath);
    if (!path.isAbsolute(this.statePath) || path.resolve(this.statePath) !== this.statePath
      || await realpath(directory) !== directory) throw new Error("settlement state path must be canonical");
    const stat = await lstat(directory);
    const uid = process.getuid?.();
    if (!stat.isDirectory() || uid === undefined || (stat.uid !== uid && stat.uid !== 0)
      || (stat.mode & 0o022) !== 0) throw new Error("settlement state directory must be private");
    const handle = await open(this.statePath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
    try {
      await handle.writeFile(`${JSON.stringify({ version: 1, containerId: id })}\n`);
      await handle.sync();
      const dir = await open(directory, constants.O_RDONLY);
      try { await dir.sync(); } finally { await dir.close(); }
      return handle;
    } catch (error) {
      await handle.close();
      throw error;
    }
  }

  private async releaseMarker(handle: FileHandle): Promise<void> {
    const [opened, current] = await Promise.all([handle.stat(), lstat(this.statePath)]);
    if (!current.isFile() || opened.ino !== current.ino || opened.dev !== current.dev) {
      throw new Error("settlement marker changed");
    }
    await unlink(this.statePath);
    const dir = await open(path.dirname(this.statePath), constants.O_RDONLY);
    try { await dir.sync(); } finally { await dir.close(); }
  }

  private async preserveReceipt(handle: FileHandle, receipt: SettlementReceipt): Promise<void> {
    const bytes = Buffer.from(`${JSON.stringify({ version: 1, ...receipt })}\n`);
    let offset = 0;
    while (offset < bytes.length) {
      const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset, offset);
      if (bytesWritten === 0) throw new Error("could not write settlement evidence");
      offset += bytesWritten;
    }
    await handle.truncate(bytes.length);
    await handle.sync();
  }

  private async inspect(id: string): Promise<ContainerInspect> {
    const output = await this.command(["inspect", "--type", "container", "--format", "{{json .}}", id]);
    const value = JSON.parse(output) as ContainerInspect;
    if (!isPrivateContainer(value, id)) throw new Error("container boundary is not independently inspectable");
    return value;
  }

  async run(id: string, options: { timeoutMs: number; signal?: AbortSignal }): Promise<SettlementReceipt> {
    if (this.quarantined || this.active) throw new Error("settlement supervisor is quarantined or busy");
    if (!/^[a-f0-9]{64}$/.test(id) || !Number.isSafeInteger(options.timeoutMs)
      || options.timeoutMs < 1 || options.timeoutMs > 24 * 60 * 60 * 1000) {
      throw new Error("invalid container identity or deadline");
    }
    this.active = true;
    let marker: FileHandle | undefined;
    try {
      marker = await this.acquireMarker(id);
      const receipt = await this.supervise(id, options);
      if (receipt.outcome === "exited" && receipt.exitCode === 0 && receipt.settlement === "verified") {
        await this.releaseMarker(marker);
      } else {
        this.quarantined = true;
        await this.preserveReceipt(marker, receipt);
      }
      return receipt;
    } catch (error) {
      this.quarantined = true;
      throw error;
    } finally {
      if (marker) await marker.close();
      this.active = false;
    }
  }

  private async supervise(id: string, options: { timeoutMs: number; signal?: AbortSignal }): Promise<SettlementReceipt> {
    let started = false;
    let outcome: SettlementOutcome = "launch_failed";
    let reason: SettlementReceipt["reason"] = "preflight_failed";
    let last: ContainerInspect | undefined;
    try {
      last = await this.inspect(id);
      if (last.State?.Status !== "created" || last.State.Running !== false || last.State.Pid !== 0) {
        throw new Error("container was not newly created");
      }
      if (options.signal?.aborted) {
        outcome = "cancelled";
        return { containerId: id, outcome, exitCode: null, settlement: "unproved", reason: "cancelled" };
      }
      const deadline = performance.now() + options.timeoutMs;
      try {
        await this.command(["start", id]);
        started = true;
      } catch {
        // A failed client request does not prove that the daemon did not start it.
        await this.command(["kill", id]).catch(() => undefined);
        return { containerId: id, outcome, exitCode: null, settlement: "unproved", reason: "start_failed" };
      }
      reason = "inspection_failed";
      while (true) {
        if (options.signal?.aborted) { outcome = "cancelled"; reason = "cancelled"; break; }
        if (performance.now() >= deadline) { outcome = "timed_out"; reason = "timeout"; break; }
        last = await this.inspect(id);
        if (options.signal?.aborted) { outcome = "cancelled"; reason = "cancelled"; break; }
        if (performance.now() >= deadline) { outcome = "timed_out"; reason = "timeout"; break; }
        if (settled(last)) {
          outcome = last.State.OOMKilled ? "resource_limit" : "exited";
          reason = last.State.OOMKilled ? "oom" : last.State.ExitCode === 0 ? "completed" : "nonzero_exit";
          return { containerId: id, outcome, exitCode: last.State.ExitCode, settlement: "verified", reason };
        }
        if (last.State?.Status !== "running" || last.State.Running !== true || last.State.Paused !== false
          || last.State.Restarting !== false || last.State.Dead !== false
          || !Number.isSafeInteger(last.State.Pid) || last.State.Pid <= 0) {
          throw new Error("container entered an unexpected state");
        }
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      }
    } catch {
      outcome = "launch_failed";
    }

    if (started) {
      await this.command(["kill", id]).catch(() => undefined);
      const cleanupDeadline = performance.now() + CLEANUP_TIMEOUT_MS;
      while (performance.now() < cleanupDeadline) {
        try {
          last = await this.inspect(id);
          if (settled(last)) {
            return { containerId: id, outcome, exitCode: last.State.ExitCode, settlement: "verified", reason };
          }
        } catch {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
      }
    }
    return { containerId: id, outcome, exitCode: null, settlement: "unproved", reason };
  }
}
