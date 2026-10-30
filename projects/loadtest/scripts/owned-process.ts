#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { appendFileSync, closeSync, openSync, readFileSync, readdirSync } from 'node:fs';
import { constants } from 'node:os';
import { performance } from 'node:perf_hooks';
import { setTimeout as delay } from 'node:timers/promises';
import koffi from 'koffi';

const libc = koffi.load(null);
const prctl = libc.func('int prctl(int, unsigned long, unsigned long, unsigned long, unsigned long)');
const waitpid = libc.func('int waitpid(int, _Out_ int *, int)');

function hasChildren(): boolean {
  for (;;) {
    const status = [0];
    const pid = waitpid(-1, status, 1);
    if (pid > 0) continue;
    if (pid === 0) return true;
    if (koffi.errno() === 10) return false; // ECHILD
    if (koffi.errno() !== 4) throw new Error('Cannot reap owned descendants'); // EINTR
  }
}

function survivingChildren(): string[] {
  const names: string[] = [];
  for (const entry of readdirSync('/proc')) {
    if (!/^\d+$/u.test(entry)) continue;
    try {
      const stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
      const end = stat.lastIndexOf(')');
      const fields = stat.slice(end + 2).trim().split(/\s+/u);
      if (fields[1] === String(process.pid) && fields[0] !== 'Z') {
        names.push(stat.slice(stat.indexOf('(') + 1, end));
      }
    } catch (error) {
      if (!['ENOENT', 'ESRCH'].includes((error as NodeJS.ErrnoException).code ?? '')) throw error;
    }
  }
  return names.sort();
}

async function cleanup(group: number): Promise<void> {
  for (const signal of ['SIGTERM', 'SIGKILL'] as const) {
    try { process.kill(-group, signal); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
    }
    const deadline = performance.now() + 2000;
    while (hasChildren() && performance.now() < deadline) await delay(25);
    if (!hasChildren()) return;
  }
  throw new Error('Owned descendants did not drain; retain occupancy');
}

async function main(): Promise<number> {
  if (process.platform !== 'linux' || prctl(36, 1, 0, 0, 0) !== 0) {
    throw new Error('Cannot establish owned CLI subreaper');
  }
  const timeout = Number(process.argv[2]);
  if (!Number.isSafeInteger(timeout) || timeout <= 0 || !process.argv[3]) {
    throw new Error('A command and positive deadline are required');
  }
  let interrupted = false;
  const interrupt = (): void => { interrupted = true; };
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', interrupt);
  const stderrPath = process.env.OWNED_PROCESS_STDERR_FILE ?? '';
  delete process.env.OWNED_PROCESS_STDERR_FILE;
  const stderr = stderrPath ? openSync(stderrPath, 'a', 0o600) : 'ignore';
  let child;
  try {
    child = spawn(process.argv[3], process.argv.slice(4), {
      detached: true, stdio: ['ignore', 'ignore', stderr],
    });
  } finally {
    if (typeof stderr === 'number') closeSync(stderr);
  }
  // Node's child watcher must reap the direct child before waitpid(-1) reaps adopted orphans.
  let finished = false;
  let status: number | null = null;
  let signal: NodeJS.Signals | null = null;
  let spawnError: Error | undefined;
  child.once('error', error => { spawnError = error; finished = true; });
  child.once('exit', (code, value) => { status = code; signal = value; finished = true; });
  if (child.pid === undefined) {
    await delay(0);
    throw spawnError ?? new Error('Cannot spawn owned CLI');
  }
  console.log(child.pid);
  let descendants = true;
  let timedOut = false;
  try {
    const deadline = performance.now() + timeout;
    while (!finished && !interrupted && performance.now() < deadline) await delay(25);
    if (spawnError) throw spawnError;
    descendants = finished ? hasChildren() : true;
    if (descendants && stderrPath) {
      appendFileSync(stderrPath, `owned-process: surviving descendants: ${survivingChildren().join(',')}\n`);
    }
    timedOut = !finished && !interrupted && performance.now() >= deadline;
  } finally {
    // Let libuv deliver/reap the direct child before polling adopted descendants.
    if (!finished) {
      try { process.kill(-child.pid, 'SIGTERM'); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
      }
      const deadline = performance.now() + 2000;
      while (!finished && performance.now() < deadline) await delay(25);
      if (!finished) {
        try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error;
        }
        while (!finished && performance.now() < deadline + 2000) await delay(25);
      }
      if (!finished) throw new Error('Owned child did not drain; retain occupancy');
    }
    await cleanup(child.pid);
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
  }
  // Report the pre-cleanup state, not the SIGTERM/SIGKILL used during cleanup.
  console.log(JSON.stringify({
    exit_code: timedOut || interrupted ? null : status,
    signal: timedOut || interrupted ? null : signal ? constants.signals[signal] : null,
    timed_out: timedOut, interrupted, descendants,
  }));
  return status === 0 && !interrupted && !descendants && !timedOut ? 0 : 1;
}

main().then(code => { process.exitCode = code; }).catch(error => {
  console.error(`Owned process supervision failed: ${error instanceof Error ? error.name : 'Error'}`);
  process.exitCode = 2;
});
