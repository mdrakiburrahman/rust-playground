import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { nativeImages, nativeManager, nativeNames } from '../src/native-catalogues.js';
import { type Execute } from '../src/contract.js';

test('native inspection rejects uninstalled services and namespace injection before executing commands', async () => {
  const workspace = mkdtempSync(join(tmpdir(), 'native-inspection-'));
  let calls = 0;
  const manager = nativeManager(workspace, async () => { calls++; throw new Error('Unexpected Docker invocation'); });
  try {
    assert.deepEqual(await manager.status('unity'), { provider: 'unity', installed: false, buildSupported: false });
    await assert.rejects(() => manager.inspect('hive', 'hive', 'db; DROP DATABASE x'), /identifier/);
    await assert.rejects(() => manager.inspect('unity', undefined, 'schema'), /parent namespaces/);
    await assert.rejects(() => manager.inspect('unity'), /Start the unity/);
    assert.equal(calls, 0);
  } finally { rmSync(workspace, { recursive: true, force: true }); }
});

test('native lifecycle refuses containers with foreign ownership or changed images', async () => {
  const workspace = mkdtempSync(join(tmpdir(), 'native-ownership-'));
  const id = '70e6f4a1-e338-4f5c-bce3-e32159af1111';
  const root = join(workspace, '.temp/contract-builder/native');
  mkdirSync(root, { recursive: true });
  writeFileSync(join(root, 'state.json'), JSON.stringify({ id }));
  const commands: string[][] = [];
  let owner = 'foreign';
  let image = nativeImages.unity;
  let endpoint = 'unix:///var/run/docker.sock';
  const execute: Execute = async (_command, rawArgs) => {
    if (rawArgs[0] === 'context') return endpoint;
    const args = rawArgs.slice(2);
    commands.push(args);
    if (args[1] === 'ls') return 'container-id';
    return JSON.stringify([{ Id: 'container-id', Config: { Image: image, Labels: { 'io.arrowcol.native.owner': owner } }, State: { Running: true } }]);
  };
  try {
    const manager = nativeManager(workspace, execute);
    await assert.rejects(() => manager.stop('unity'), /Refusing foreign/);
    owner = id; image = 'untrusted:changed';
    await assert.rejects(() => manager.stop('unity'), /Refusing foreign/);
    assert.ok(commands.every((args) => !args.includes('stop')));
    image = nativeImages.unity;
    assert.equal((await manager.status('unity')).container, nativeNames(id).unity);
    endpoint = 'tcp://remote:2375';
    await assert.rejects(() => nativeManager(workspace, execute).up('unity'), /local Docker socket/);
    assert.ok(commands.every((args) => !args.includes('create')));
  } finally { rmSync(workspace, { recursive: true, force: true }); }
});
