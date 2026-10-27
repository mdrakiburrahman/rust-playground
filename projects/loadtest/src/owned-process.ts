import { spawn } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

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
  command: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv; timeoutMs: number },
): Promise<void> {
  if (process.platform !== 'linux' || !Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0) {
    throw new Error('Owned process groups require Linux and a finite positive deadline');
  }
  const supervisor = fileURLToPath(new URL('../scripts/owned-process.py', import.meta.url));
  const child = spawn('python3', [supervisor, String(options.timeoutMs), command, ...args], {
    cwd: options.cwd, env: options.env, detached: true, stdio: ['ignore', 'pipe', 'ignore'],
  });
  let commandGroup: number | undefined;
  let receipt = '';
  child.stdout.setEncoding('utf8').on('data', (data: string) => {
    receipt += data;
    if (/^[1-9][0-9]*\n$/u.test(receipt) && Number.isSafeInteger(Number(receipt.trim()))) {
      commandGroup = Number(receipt.trim());
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
      reject(new Error('Owned mutation supervisor exceeded its deadline; retain occupancy'));
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
    if (outcome.code !== 0 || outcome.signal !== null || commandGroup === undefined || descendantsRemain) {
      throw new Error(`Owned mutation process did not complete cleanly (${descendantsRemain ? 'live descendants' : outcome.signal ?? outcome.code}); retain occupancy`);
    }
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
