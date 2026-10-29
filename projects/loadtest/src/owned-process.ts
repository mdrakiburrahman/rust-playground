import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

export interface OwnedProcessOutcome {
  exit_code: number | null;
  signal: number | null;
  timed_out: boolean;
  interrupted: boolean;
  descendants: boolean;
}

export class OwnedProcessError extends Error {
  constructor(readonly outcome: OwnedProcessOutcome | null, readonly supervisor_timed_out = false) {
    super('Owned mutation process did not complete cleanly; retain occupancy');
  }
}

function checkedOutcome(value: unknown): OwnedProcessOutcome {
  if (!value || typeof value !== 'object') throw new OwnedProcessError(null);
  const result = value as Record<string, unknown>;
  const numeric = (value: unknown, min: number, max: number): value is number | null =>
    value === null || (typeof value === 'number' && Number.isSafeInteger(value) && value >= min && value <= max);
  if (Object.keys(result).length !== 5 || !numeric(result.exit_code, 0, 255) ||
      !numeric(result.signal, 1, 64) || typeof result.timed_out !== 'boolean' ||
      typeof result.interrupted !== 'boolean' || typeof result.descendants !== 'boolean' ||
      (result.exit_code !== null && result.signal !== null)) throw new OwnedProcessError(null);
  return { exit_code: result.exit_code, signal: result.signal, timed_out: result.timed_out,
    interrupted: result.interrupted, descendants: result.descendants };
}

function groupExists(pid: number): boolean {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false;
    throw error;
  }
}

async function drainGroup(pid: number): Promise<void> {
  for (const signal of ['SIGTERM', 'SIGKILL'] as const) {
    if (!groupExists(pid)) return;
    try {
      process.kill(-pid, signal);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
    }
    const deadline = Date.now() + 2000;
    while (groupExists(pid) && Date.now() < deadline) await delay(25);
  }
  if (groupExists(pid)) throw new Error('Owned child process group did not drain; mutation outcome remains unknown');
}

function supervisorExited(pid: number | undefined): boolean {
  if (pid === undefined) return false;
  try {
    return /^\d+ \(.*\) [ZX] /su.test(readFileSync(`/proc/${pid}/stat`, 'utf8'));
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'ENOENT';
  }
}

/** Owns the entire Linux process group, including Azure CLI wrapper descendants. */
export async function runOwnedProcess(
  command: string, args: string[],
  options: { cwd: string; env: NodeJS.ProcessEnv; timeoutMs: number; stderrFile?: string },
): Promise<void> {
  if (process.platform !== 'linux' || !Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new Error('Owned process groups require Linux and a finite positive deadline');
  }
  const supervisor = fileURLToPath(new URL('../scripts/owned-process.py', import.meta.url));
  const child = spawn('python3', [supervisor, String(options.timeoutMs), command, ...args], {
    cwd: options.cwd, detached: true,
    env: { ...options.env, OWNED_PROCESS_STDERR_FILE: options.stderrFile ?? '' }, stdio: ['ignore', 'pipe', 'ignore'],
  });
  let commandGroup: number | undefined;
  let receipt = '';
  child.stdout.setEncoding('utf8').on('data', (data: string) => {
    receipt += data;
    const first = receipt.split('\n')[0];
    if (receipt.includes('\n') && /^[1-9][0-9]*$/u.test(first) && Number.isSafeInteger(Number(first))) {
      commandGroup = Number(first);
    }
  });
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    const expire = (graceful: boolean): void => {
      // A timer starved by a blocked event loop must not beat an exit that is merely not yet delivered;
      // allow one bounded grace for the pending close, then fail closed.
      if (graceful && (child.exitCode !== null || child.signalCode !== null || supervisorExited(child.pid))) {
        timer = setTimeout(() => expire(false), 5000);
        return;
      }
      reject(new OwnedProcessError(null, true));
    };
    timer = setTimeout(() => expire(true), options.timeoutMs + 6000);
  });
  const interrupt = (): void => { child.kill('SIGTERM'); };
  process.once('SIGINT', interrupt);
  process.once('SIGTERM', interrupt);
  try {
    const completion = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once('error', reject);
      child.once('close', (code, signal) => resolve({ code, signal }));
    });
    const outcome = await Promise.race([completion, deadline]);
    const descendantsRemain = commandGroup !== undefined && groupExists(commandGroup);
    let status: OwnedProcessOutcome | null = null;
    const lines = receipt.trimEnd().split('\n');
    if (lines.length === 2) {
      try {
        status = checkedOutcome(JSON.parse(lines[1]) as unknown);
      } catch {
        throw new OwnedProcessError(null);
      }
    }
    if (outcome.code !== 0 || outcome.signal !== null || commandGroup === undefined || descendantsRemain) {
      throw new OwnedProcessError(status);
    }
    if (!status || status.exit_code !== 0 || status.signal !== null || status.timed_out ||
        status.interrupted || status.descendants) throw new OwnedProcessError(status);
  } finally {
    clearTimeout(timer);
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
    try {
      if (commandGroup !== undefined) await drainGroup(commandGroup);
    } finally {
      if (child.pid) await drainGroup(child.pid);
    }
  }
}
