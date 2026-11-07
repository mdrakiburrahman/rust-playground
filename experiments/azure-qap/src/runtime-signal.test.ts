import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { runtimeSignalCommand } from './runtime-signal.js';

for (const supervisor of [false, true]) {
  test(`signals the ${supervisor ? 'actual tsx child' : 'direct Node runtime'} without external kill`, { timeout: 15_000 }, async () => {
    const dir = path.resolve(`.temp/runtime-signal-${process.pid}-${supervisor}`);
    fs.mkdirSync(dir, { recursive: true });
    const entry = path.join(dir, 'job.mjs');
    const ready = path.join(dir, 'ready');
    const done = path.join(dir, 'done');
    const bin = path.join(dir, 'bin');
    fs.mkdirSync(bin);
    fs.symlinkSync(process.execPath, path.join(bin, 'node'));
    fs.writeFileSync(entry, `
import fs from 'node:fs';
const keepAlive = setInterval(() => {}, 1000);
process.on('SIGTERM', () => fs.writeFileSync(${JSON.stringify(done)}, 'scoped cleanup done'));
process.on('SIGUSR1', () => {
  if (!fs.existsSync(${JSON.stringify(done)})) process.exit(2);
  clearInterval(keepAlive);
});
fs.writeFileSync(${JSON.stringify(ready)}, String(process.pid));
`);
    const child = spawn(process.execPath, supervisor ? ['node_modules/.bin/tsx', entry] : [entry],
      { stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = new Promise<number | null>(resolve => child.on('exit', resolve));
    let stderr = '';
    child.stderr.on('data', bytes => { stderr += bytes; });
    const wait = async (file: string) => {
      for (let i = 0; i < 200 && !fs.existsSync(file); i++) {
        assert.equal(child.exitCode, null, stderr);
        await sleep(25);
      }
      assert.ok(fs.existsSync(file), stderr);
    };
    const signal = (value: 'SIGTERM' | 'SIGUSR1', expected = entry) => {
      const [command, ...args] = runtimeSignalCommand(value, child.pid!, expected);
      return spawnSync(command!, args, { encoding: 'utf8', env: { ...process.env, PATH: bin } });
    };
    try {
      await wait(ready);
      const externalKill = spawnSync('kill', ['-0', String(child.pid)], { env: { ...process.env, PATH: bin } });
      assert.equal((externalKill.error as NodeJS.ErrnoException).code, 'ENOENT');
      const rejected = signal('SIGTERM', entry + '-unrelated');
      assert.notEqual(rejected.status, 0);
      assert.match(rejected.stderr, /refusing to signal/);
      assert.equal(fs.existsSync(done), false);
      const stopped = signal('SIGTERM');
      assert.equal(stopped.status, 0, stopped.stderr);
      await wait(done);
      assert.equal(fs.readFileSync(done, 'utf8'), 'scoped cleanup done');
      assert.equal(child.exitCode, null, 'artifact holder must stay available after cleanup');
      const released = signal('SIGUSR1');
      assert.equal(released.status, 0, released.stderr);
      assert.equal(await exited, 0, stderr);
    } finally {
      if (child.exitCode === null) {
        if (fs.existsSync(ready)) {
          const runtimePid = Number(fs.readFileSync(ready, 'utf8'));
          try { process.kill(runtimePid, 'SIGKILL'); }
          catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; }
        }
        child.kill('SIGKILL');
      }
      await exited;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
}
