import assert from 'node:assert/strict';
import { rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { sqlserverAccess } from '../src/main.js';
import { fixtureDirectory } from './fixture-directory.js';

test('SQL Server startup access prints the credential file, never its contents', (t) => {
  const directory = fixtureDirectory('sqlserver-');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'sqlserver-sa-password.txt');
  writeFileSync(file, 'synthetic-startup-secret-sentinel\n');
  const line = sqlserverAccess('14334', file);
  assert.match(line, /SQL Server: localhost,14334 /u);
  assert.match(line, /host port 14334 -> container port 1433/u);
  assert.match(line, /username sa,/u);
  assert.ok(line.includes(`credential file ${file}`));
  assert.doesNotMatch(line, /synthetic-startup-secret-sentinel/u);
});
