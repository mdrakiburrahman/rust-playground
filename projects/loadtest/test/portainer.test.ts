import assert from 'node:assert/strict';
import { rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import { portainerAccess } from '../src/main.js';
import { fixtureDirectory } from './fixture-directory.js';

function passwordFile(t: { after: (callback: () => void) => void }, content: string): string {
  const directory = fixtureDirectory('portainer-');
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const file = path.join(directory, 'sqlserver-sa-password.txt');
  writeFileSync(file, content);
  return file;
}

test('portainer startup access prints the credential file, never its contents', (t) => {
  const file = passwordFile(t, 'synthetic-startup-secret-sentinel\n');
  const line = portainerAccess('9443', file);
  assert.match(line, /https:\/\/localhost:9443 /u);
  assert.match(line, /port 9443,/u);
  assert.match(line, /username admin,/u);
  assert.ok(line.includes(`credential file ${file}`));
  assert.doesNotMatch(line, /synthetic-startup-secret-sentinel/u);
});
