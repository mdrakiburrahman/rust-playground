import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { nativeImages, nativeManager, nativeNames } from '../src/native-catalogues.js';
import { enqueue, loadConfig, saveRun, writeJson, type Execute } from '../src/contract.js';

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

test('enabling native builds rejects active workflows and endpoint collisions before changing services', async () => {
  const workspace = mkdtempSync(join(tmpdir(), 'native-build-guards-'));
  const id = '70e6f4a1-e338-4f5c-bce3-e32159af1111';
  const root = join(workspace, '.temp/contract-builder/native');
  mkdirSync(root, { recursive: true });
  writeJson(join(root, 'state.json'), { id });
  const config = loadConfig(workspace);
  const configPath = join(workspace, '.temp/contract-builder/config.json');
  config.registries.push({
    id: 'unity', label: 'Unity Catalog', authority: `${id}:unity`,
    endpoint: 'http://127.0.0.1:18819', mode: 'owned',
    native: { provider: 'unity', endpoint: `http://${nativeNames(id).unity}:8080/api/2.1/unity-catalog` },
  });
  writeJson(configPath, config);
  const commands: string[][] = [];
  const execute: Execute = async (_command, rawArgs) => {
    commands.push(rawArgs);
    if (rawArgs[0] === 'context') return 'unix:///var/run/docker.sock';
    const args = rawArgs.slice(2);
    if (args[0] === 'container' && args[1] === 'ls') return 'owned-container';
    assert.deepEqual(args.slice(0, 2), ['container', 'inspect'], 'only read-only ownership probes are allowed');
    return JSON.stringify([{
      Id: 'owned-container', Config: { Image: nativeImages.unity, Labels: { 'io.arrowcol.native.owner': id } },
      State: { Running: true },
    }]);
  };
  try {
    const manager = nativeManager(workspace, execute);
    const run = enqueue(workspace, { registry: 'unity', mode: 'create', ddl: 'CREATE TABLE demo.prod.events (id BIGINT)' });
    await assert.rejects(() => manager.enableBuild('unity'), /Wait for native builds/);
    assert.deepEqual(commands, []);
    run.status = 'failed';
    saveRun(workspace, run);
    config.registries.pop();
    config.registries[0].endpoint = 'http://127.0.0.1:18819';
    writeJson(configPath, config);
    const before = readFileSync(configPath, 'utf8');
    await assert.rejects(() => manager.enableBuild('unity'), /endpoint or authority is already configured/);
    assert.equal(readFileSync(configPath, 'utf8'), before);
  } finally { rmSync(workspace, { recursive: true, force: true }); }
});
