import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import { DEFAULT_CLEANUP_RESERVE_MS, loadOrCreateDeadline, remainingBudgetMs, type DeadlineRecord } from "./deadline-state.js";

export const COMPRESSION_CONFIG_DEADLINE_UTC = "2026-11-23T05:06:42.133Z";

interface ExitResult { code: number | null; signal: NodeJS.Signals | null }

function waitForExit(child: ChildProcess): Promise<ExitResult> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      resolve({ code: child.exitCode, signal: child.signalCode });
      return;
    }
    child.once("exit", (code, signal) => resolve({ code, signal }));
    child.once("error", () => resolve({ code: 127, signal: null }));
  });
}

function sanitizeEnvironment(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  delete env.COPILOT_GITHUB_TOKEN;
  delete env.DARWIN_TEST_SENTINEL;
  return env;
}

function signalExitCode(signal: NodeJS.Signals | null): number {
  if (!signal) return 1;
  if (signal === "SIGINT") return 130;
  if (signal === "SIGTERM") return 143;
  return 1;
}

export async function runDeadlinedCommand(options: {
  deadline: DeadlineRecord;
  command: string;
  args: string[];
  containerName?: string;
  cleanupReserveMs?: number;
  now?: () => number;
}): Promise<number> {
  const now = options.now ?? Date.now;
  const cleanupReserveMs = options.cleanupReserveMs ?? DEFAULT_CLEANUP_RESERVE_MS;
  const budgetMs = remainingBudgetMs(options.deadline, now(), cleanupReserveMs);
  const env = sanitizeEnvironment();
  const child = spawn(options.command, options.args, { stdio: "inherit", env });
  const exitPromise = waitForExit(child);
  let timedOut = false;
  let externalSignal: NodeJS.Signals | undefined;
  let stopper: ChildProcess | undefined;
  let forceKillTimer: NodeJS.Timeout | undefined;
  let terminating: Promise<void> | undefined;

  const terminate = async (): Promise<void> => {
    if (terminating) return terminating;
    terminating = (async () => {
      child.kill("SIGTERM");
      if (options.containerName) {
        stopper = spawn("docker", ["stop", "--time", "20", options.containerName], {
          stdio: "ignore",
          env,
        });
        stopper.on("error", () => process.stderr.write("Docker container cleanup command failed.\n"));
      }
      forceKillTimer = setTimeout(() => {
        if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
        if (stopper && stopper.exitCode === null && stopper.signalCode === null) stopper.kill("SIGKILL");
      }, cleanupReserveMs);
      if (stopper) await waitForExit(stopper);
      await exitPromise;
      if (forceKillTimer) clearTimeout(forceKillTimer);
    })();
    return terminating;
  };

  const deadlineTimer = setTimeout(() => {
    timedOut = true;
    void terminate();
  }, budgetMs);
  const signalHandler = (signal: NodeJS.Signals) => {
    externalSignal = signal;
    void terminate();
  };
  process.once("SIGINT", signalHandler);
  process.once("SIGTERM", signalHandler);

  const result = await exitPromise;
  clearTimeout(deadlineTimer);
  process.removeListener("SIGINT", signalHandler);
  process.removeListener("SIGTERM", signalHandler);
  if (terminating) await terminating;
  if (timedOut) return 124;
  if (externalSignal) return signalExitCode(externalSignal);
  return result.code ?? signalExitCode(result.signal);
}

function main(): void {
  const [repoRoot, statePath, runId, deadlineUtc, containerName, separator, command, ...args] = process.argv.slice(2);
  if (!repoRoot || !statePath || !runId || !deadlineUtc || !containerName || separator !== "--" || !command) {
    throw new Error("usage: deadline-runner.ts <repo> <state-file> <run-id> <deadline-utc> <container-name|-> -- <command> [args...]");
  }
  if (deadlineUtc !== COMPRESSION_CONFIG_DEADLINE_UTC) {
    throw new Error("compression-config deadline is fixed at " + COMPRESSION_CONFIG_DEADLINE_UTC);
  }
  const record = loadOrCreateDeadline({ statePath, repoRoot, runId, deadlineUtc });
  const name = containerName === "-" ? undefined : containerName;
  if (name && name !== `darwin-controller-${runId}`) throw new Error("controller container name must be fixed for this run");
  void runDeadlinedCommand({ deadline: record, command, args, containerName: name }).then((code) => {
    process.exitCode = code;
  });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
