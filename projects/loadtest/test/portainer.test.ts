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

test('portainer access prints URL, port, username and password', (t) => {
  const line = portainerAccess('9443', passwordFile(t, 'Aa1!secret\n'));
  assert.match(line, /https:\/\/localhost:9443 /u);
  assert.match(line, /port 9443,/u);
  assert.match(line, /username admin,/u);
  assert.match(line, /password Aa1!secret;/u);
});

test('portainer password matches the seeded value with or without a trailing newline', (t) => {
  const fake = 'fixture-value';
  assert.match(portainerAccess('9444', passwordFile(t, fake)), new RegExp(`localhost:9444 .*password ${fake};`, 'u'));
  assert.doesNotMatch(portainerAccess('9443', passwordFile(t, 'Aa1!secret\n')), /\n/u);
});
