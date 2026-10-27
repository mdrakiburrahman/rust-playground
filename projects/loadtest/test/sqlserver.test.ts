import assert from 'node:assert/strict';
import { rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { sqlserverAccess } from '../src/main.js';
import { fixtureDirectory } from './fixture-directory.js';

test('SQL Server access prints host and container ports and SA credentials from the secret', (t) => {
  const directory = fixtureDirectory('sqlserver-');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'sqlserver-sa-password.txt');
  for (const suffix of ['', '\n', '\n\n']) {
    writeFileSync(file, `Aa1!secret${suffix}`);
    assert.equal(sqlserverAccess('14334', file),
      'SQL Server: localhost,14334 (host port 14334 -> container port 1433, username sa, password Aa1!secret)');
  }
});

test('SQL Server access surfaces missing or invalid secrets', (t) => {
  const directory = fixtureDirectory('sqlserver-');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'sqlserver-sa-password.txt');
  assert.throws(() => sqlserverAccess('14333', file), { code: 'ENOENT' });
  for (const password of ['', '\n', 'Aa1!secret\nother', 'Aa1!secret\r\n']) {
    writeFileSync(file, password);
    assert.throws(() => sqlserverAccess('14333', file), /non-empty and single-line/u);
  }
});
