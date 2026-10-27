import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const separator = process.argv.indexOf('--');
const [command, ...args] = separator < 0 ? [] : process.argv.slice(separator + 1);
assert.ok(command, 'usage: check-fixture-isolation.ts -- COMMAND [ARG...]');

let samples = 0;
let failure: string | undefined;
function inspect(): void {
  const result = spawnSync('git', ['status', '--porcelain=v1', '--untracked-files=all'], {
    cwd: root, encoding: 'utf8', env: { ...process.env, GIT_OPTIONAL_LOCKS: '0' },
  });
  samples++;
  if (result.error || result.status !== 0 || result.stdout !== '') {
    failure ??= result.error?.message ?? (result.stdout || result.stderr || `git status exited ${result.status}`);
  }
}

inspect();
assert.equal(failure, undefined, 'fixture isolation requires an initially clean worktree');
const child = spawn(command, args, { cwd: root, stdio: 'inherit' });
const timer = setInterval(inspect, 50);
const result = await new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
  child.once('error', reject);
  child.once('exit', (code, signal) => resolve({ code, signal }));
}).finally(() => clearInterval(timer));
inspect();
assert.equal(failure, undefined, `worktree changed during fixture tests:\n${failure ?? ''}`);
assert.equal(result.signal, null, `test command terminated by ${result.signal}`);
assert.equal(result.code, 0, `test command exited ${result.code}`);
console.log(`PASS fixture isolation: ${samples} clean git-status observations during and after the test command`);
