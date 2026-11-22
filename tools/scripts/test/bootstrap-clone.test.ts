import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';

const source = readFileSync(resolve('dev/bootstrap-dev-env.ps1'), 'utf8');
const cloneScript = source.match(/\$cloneScript = @'\r?\n([\s\S]*?)\r?\n'@/u)?.[1] ??
  (() => { throw new Error('WSL clone script must be present in the bootstrap'); })();

function runClone(mode: string, existing = false) {
  const directory = mkdtempSync(join(tmpdir(), 'bootstrap-clone-'));
  try {
    const bin = join(directory, 'bin');
    const target = join(directory, 'checkout');
    const calls = join(directory, 'calls');
    const sleeps = join(directory, 'sleeps');
    mkdirSync(bin);
    if (existing) mkdirSync(target);
    writeFileSync(join(bin, 'git'), `#!/bin/bash
if [ "$1" = config ]; then exit 0; fi
printf x >> "$CALLS"
count=$(wc -c < "$CALLS")
if [ "$MODE" = recover ] && [ "$count" -ge 3 ]; then mkdir "$6"; exit 0; fi
if [ "$MODE" = partial ]; then mkdir "$6"; fi
if [ "$MODE" = auth ]; then echo "authentication failed for $5" >&2; exit 42; fi
echo "fatal: unable to access $5: Could not resolve host: example.test" >&2
exit 128
`, { mode: 0o755 });
    writeFileSync(join(bin, 'wslpath'), '#!/bin/bash\nprintf "%s" "$2"\n', { mode: 0o755 });
    writeFileSync(join(bin, 'sleep'), '#!/bin/bash\nprintf "%s\\n" "$1" >> "$SLEEPS"\n', { mode: 0o755 });
    const result = spawnSync('bash', ['-c', cloneScript, 'bash', 'user', 'mail', 'branch',
      'https://user:test-secret@example.test/repo', 'credential-helper', target], {
      encoding: 'utf8',
      env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, MODE: mode, CALLS: calls, SLEEPS: sleeps },
    });
    const count = (() => {
      try { return readFileSync(calls, 'utf8').length; } catch { return 0; }
    })();
    const delays = (() => {
      try { return readFileSync(sleeps, 'utf8').trim().split('\n'); } catch { return []; }
    })();
    assert.doesNotMatch(result.stdout + result.stderr, /test-secret|example\.test/u);
    return { result, count, delays };
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test('fresh WSL clone recovers from transient DNS failures', () => {
  const { result, count, delays } = runClone('recover');
  assert.equal(result.status, 0);
  assert.equal(count, 3);
  assert.deepEqual(delays, ['2', '4']);
});

test('fresh WSL clone reports bounded DNS exhaustion', () => {
  const { result, count, delays } = runClone('dns');
  assert.equal(result.status, 128);
  assert.equal(count, 4);
  assert.deepEqual(delays, ['2', '4', '8']);
  assert.match(result.stderr, /DNS after 4 attempts/u);
});

test('fresh WSL clone does not retry other failures or overwrite destinations', () => {
  const auth = runClone('auth');
  assert.equal(auth.result.status, 42);
  assert.equal(auth.count, 1);
  assert.deepEqual(auth.delays, []);
  const partial = runClone('partial');
  assert.equal(partial.result.status, 128);
  assert.equal(partial.count, 1);
  assert.match(partial.result.stderr, /refusing to overwrite/u);
  const existing = runClone('dns', true);
  assert.equal(existing.result.status, 1);
  assert.equal(existing.count, 0);
});
