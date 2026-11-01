import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { fixtureDirectory } from './fixture-directory.js';

test('fixture trees are unique, gitignored, and retain repository ancestry', t => {
  const first = fixtureDirectory('isolation-');
  const second = fixtureDirectory('isolation-');
  t.after(() => {
    rmSync(first, { recursive: true, force: true });
    rmSync(second, { recursive: true, force: true });
  });
  assert.notEqual(first, second);
  const repository = path.resolve(first, '../../..');
  assert.equal(path.dirname(first), path.join(repository, '.temp/test-fixtures'));
  assert.ok(existsSync(path.join(repository, 'Cargo.lock')));
  assert.ok(existsSync(path.join(repository, 'package-lock.json')));
  const marker = path.join(first, 'untracked-fixture.txt');
  writeFileSync(marker, 'fixture');
  execFileSync('git', ['check-ignore', '--quiet', '--', marker], { cwd: repository });
  assert.equal(execFileSync('git', [
    'status', '--porcelain=v1', '--untracked-files=all', '--', first, second,
  ], { cwd: repository, encoding: 'utf8' }), '');
});
