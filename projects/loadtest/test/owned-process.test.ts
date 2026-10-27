import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';

import { OwnedProcessError, runOwnedProcess } from '../src/owned-process.js';

test('owned process exposes only checked numeric outcomes for actual failure, signal and timeout', async () => {
  for (const [script, timeoutMs, expected] of [
    ['console.error("https://secret.invalid/password");process.exit(3)', 2000,
      { exit_code: 3, signal: null, timed_out: false }],
    ['process.kill(process.pid,"SIGTERM")', 2000,
      { exit_code: null, signal: 15, timed_out: false }],
    ['setInterval(()=>{},1000)', 200,
      { exit_code: null, signal: null, timed_out: true }],
  ] as const) {
    await assert.rejects(runOwnedProcess(process.execPath, ['-e', script], {
      cwd: process.cwd(), env: process.env, timeoutMs,
    }), (error: unknown) => {
      assert(error instanceof OwnedProcessError);
      assert(error.outcome);
      for (const [key, value] of Object.entries(expected)) {
        assert.equal(error.outcome[key as keyof typeof error.outcome], value);
      }
      assert(!JSON.stringify(error).includes('secret.invalid'));
      return true;
    });
  }
});

test('owned process accepts clean completion, rejects failure and validates deadlines', async () => {
  const options = { cwd: process.cwd(), env: process.env, timeoutMs: 2000 };
  await runOwnedProcess(process.execPath, ['-e', 'process.exit(0)'], options);
  await assert.rejects(runOwnedProcess(process.execPath, ['-e', 'process.exit(3)'], options), /retain occupancy/u);
  await assert.rejects(runOwnedProcess(process.execPath, [], { ...options, timeoutMs: 0 }), /finite positive/u);
  await assert.rejects(runOwnedProcess('/definitely-absent-loadtest-binary', [], options), /retain occupancy/u);
});

test('a completed process is not failed by a backstop timer starved behind a blocked event loop', async () => {
  // Mirrors run 8568: the CLI finished, then synchronous gate work blocked Node past the backstop.
  const started = Date.now();
  const outcome = runOwnedProcess(process.execPath, ['-e', 'process.exit(0)'], {
    cwd: process.cwd(), env: process.env, timeoutMs: 200,
  });
  assert.equal(spawnSync('sleep', ['7.5']).status, 0);
  assert(Date.now() - started > 6200, 'event loop must stay blocked beyond the backstop deadline');
  await outcome;
});

for (const mode of ['deadline', 'parent-exit', 'interrupt']) {
  test(`owned process kills and reaps actual CLI descendants after ${mode}`, async t => {
    const root = mkdtempSync(path.join(os.tmpdir(), 'loadtest-owned-process-'));
    t.after(() => rmSync(root, { force: true, recursive: true }));
    const pidFile = path.join(root, 'pid');
    const script = [
      'const {spawn}=require("node:child_process");',
      'const fs=require("node:fs");',
      'const child=spawn(process.execPath,["-e","setInterval(()=>{},1000)"],{stdio:"ignore"});',
      `fs.writeFileSync(${JSON.stringify(pidFile)},String(child.pid));`,
      mode === 'parent-exit' ? 'process.exit(0);' : 'setInterval(()=>{},1000);',
    ].join('');
    const outcome = assert.rejects(runOwnedProcess(process.execPath, ['-e', script], {
      cwd: root, env: process.env, timeoutMs: mode === 'interrupt' ? 5000 : 1500,
    }), /retain occupancy/u);
    if (mode === 'interrupt') {
      const deadline = Date.now() + 2000;
      while (!existsSync(pidFile) && Date.now() < deadline) await delay(10);
      assert(existsSync(pidFile), 'actual descendant must start before interrupting');
      process.kill(process.pid, 'SIGTERM');
    }
    await outcome;
    const pid = Number(readFileSync(pidFile, 'utf8'));
    assert(Number.isSafeInteger(pid) && pid > 1);
    assert.throws(() => process.kill(pid, 0), (error: unknown) =>
      error instanceof Error && 'code' in error && error.code === 'ESRCH');
  });
}
