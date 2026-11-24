import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync, existsSync, symlinkSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
import {
  buildContract, contractRoot, digest, enqueue, generatorPath, hostPath, loadConfig,
  parseRequest, publishArchive, readRun, recoverRuns, registryEnvironment, resumePromotion, runDirectory, saveRun, type Execute,
} from '../src/contract.js';
import { protocPaths } from '../src/protoc.js';
import { inspectRunDocument } from '../src/run-inspection.js';
import { pipelineSteps } from '../ui/run-pipeline.js';

function fixture() {
  const workspace = mkdtempSync(join(tmpdir(), 'contract-workflow-'));
  const put = (path: string, value: string) => {
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, value);
  };
  put(generatorPath(workspace), 'test-generator');
  put(join(workspace, 'tools/libs/arrowcol-sdk-gen/src/contract.ts'), 'test-workflow');
  put(join(workspace, 'package-lock.json'), '{}');
  const pin = { table: 'telemetry.prod.events', tableId: '4a9d8c13-96a2-4c33-8888-e883bfbc1111', version: 1, fingerprint: 'schema-only' };
  const calls: string[][] = [];
  const environments: { operation: string; env: NodeJS.ProcessEnv | undefined }[] = [];
  let fail = '';
  const execute: Execute = async (command, args, _log, env) => {
    const operation = args[0] === 'catalog' ? args[1] : args[0];
    calls.push([command, ...args]);
    environments.push({ operation, env });
    if (operation === fail) throw new Error(`injected ${operation} failure`);
    if (operation === 'properties') return JSON.stringify({ definitions_sha256: `sha256:${'a'.repeat(64)}`, options: [] });
    const out = args.includes('--out') ? resolve(workspace, args[args.indexOf('--out') + 1]) : '';
    if (['prepare', 'export'].includes(operation)) {
      put(join(out, 'manifest.json'), JSON.stringify({ schema: { metadata: { properties: { 'delta.appendOnly': 'true' } } } }));
      if (operation === 'prepare') put(join(out, 'candidate.pb'), 'exact-candidate');
      return JSON.stringify(pin);
    }
    if (operation === 'package-artifact') {
      put(out, 'deterministic-archive');
      return JSON.stringify({ sha256: `sha256:${digest('deterministic-archive')}`, bytes: 21 });
    }
    return '{}';
  };
  return {
    workspace, calls, environments, execute, fail: (operation: string) => { fail = operation; },
    cleanup: () => rmSync(workspace, { recursive: true, force: true }),
  };
}
test('registry authorities persist and managed services have isolated endpoints', () => {
  const f = fixture();
  try {
    const first = loadConfig(f.workspace);
    assert.deepEqual(loadConfig(f.workspace), first);
    assert.notEqual(first.registries[0].authority, first.registries[1].authority);
    assert.notEqual(first.registries[0].endpoint, first.registries[1].endpoint);
    assert.equal(hostPath({ ...first, hostWorkspace: 'Q:\\src\\rust-playground' }, '.temp/releases/a.tar.gz'), 'Q:\\src\\rust-playground\\.temp\\releases\\a.tar.gz');
    writeFileSync(join(f.workspace, contractRoot, 'config.json'), '{ broken');
    assert.throws(() => loadConfig(f.workspace), SyntaxError);
  } finally { f.cleanup(); }
});
test('requests reject absent versions, arbitrary modes, oversized DDL and malformed identities', () => {
  assert.throws(() => parseRequest({ registry: 'local', mode: 'rebuild', table: 'a.b.c' }));
  assert.throws(() => parseRequest({ registry: 'local', mode: 'shell', ddl: 'echo bad' }));
  assert.throws(() => parseRequest({ registry: 'local', mode: 'create', ddl: 'x'.repeat(65537) }));
  assert.throws(() => parseRequest({ registry: 'local', mode: 'rebuild', table: '../bad', version: 1 }));
  const create = { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' };
  assert.deepEqual(parseRequest(create), create, 'legacy requests stay byte-shape compatible');
  assert.deepEqual(parseRequest({ ...create, codegen: 'direct' }), create);
  assert.deepEqual(parseRequest({ ...create, codegen: 'protoc' }), { ...create, codegen: 'protoc', protoEntry: 'schema.proto' });
  assert.throws(() => parseRequest({ ...create, codegen: 'arbitrary-plugin' }));
  assert.throws(() => parseRequest({ ...create, protoEntry: 'schema.proto' }));
  for (const protoEntry of ['../other.proto', 'options.proto', 'arrowcol_codegen.proto', 'a\\b.proto']) {
    assert.throws(() => parseRequest({ ...create, codegen: 'protoc', protoEntry }));
  }
});
test('run reads tolerate a transient rename gap but surface persistent and other errors', (context) => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const read = context.mock.method(fs, 'readFileSync');
    syncBuiltinESMExports();
    read.mock.mockImplementationOnce(() => { throw Object.assign(new Error('rename gap'), { code: 'ENOENT' }); });
    assert.deepEqual(readRun(f.workspace, queued.id), queued);
    assert.equal(read.mock.callCount(), 2);
    read.mock.resetCalls();
    read.mock.mockImplementation(() => { throw Object.assign(new Error('missing run'), { code: 'ENOENT' }); });
    assert.throws(() => readRun(f.workspace, queued.id), /missing run/);
    assert.equal(read.mock.callCount(), 6);
    read.mock.resetCalls();
    read.mock.mockImplementation(() => { throw Object.assign(new Error('denied'), { code: 'EACCES' }); });
    assert.throws(() => readRun(f.workspace, queued.id), /denied/);
    assert.equal(read.mock.callCount(), 1);
  } finally {
    context.mock.restoreAll();
    syncBuiltinESMExports();
    f.cleanup();
  }
});
test('CREATE prepares, generates, verifies and stages before exact promotion and publication', async () => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'succeeded', run.error ?? '');
    assert.equal(run.registration, 'committed');
    const operations = f.calls.map((call) => call[1] === 'catalog' ? call[2] : call[1]);
    assert.ok(operations.indexOf('prepare') < operations.indexOf('generate'));
    assert.ok(operations.indexOf('test') < operations.indexOf('package-artifact'));
    assert.ok(operations.indexOf('package-artifact') < operations.indexOf('promote'));
    assert.equal(operations.filter((operation) => operation === 'promote').length, 1);
    for (const operation of ['prepare', 'promote']) {
      assert.deepEqual(f.environments.find((entry) => entry.operation === operation)?.env,
        registryEnvironment(f.workspace, queued.registry), 'prepare and promotion use the selected authority signing key');
    }
    assert.ok(run.artifact);
    assert.equal(digest(readFileSync(resolve(f.workspace, run.artifact.path))), run.artifact.sha256);
    assert.deepEqual(readRun(f.workspace, run.id), run);
    assert.deepEqual(run.steps?.map((step) => step.phase),
      ['validating', 'preparing', 'generating', 'verifying', 'packaging', 'finalizing', 'promoting', 'publishing']);
    assert.ok(run.steps?.every((step) => step.status === 'succeeded' && step.finishedAt && step.finishedAt >= step.startedAt));
    assert.equal(pipelineSteps(run).find((step) => step.phase === 'validating-proto')?.status, 'skipped');
    await assert.rejects(() => buildContract(f.workspace, run.id, f.execute), /Only a queued run/);
  } finally { f.cleanup(); }
});
test('recovery preserves a completed release when the worker exits during its liveness probe', async (context) => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const completed = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(completed.status, 'succeeded');
    saveRun(f.workspace, { ...completed, status: 'running', phase: 'publishing', artifact: undefined });
    context.mock.method(process, 'kill', () => {
      saveRun(f.workspace, completed);
      throw Object.assign(new Error('worker exited after final save'), { code: 'ESRCH' });
    });
    recoverRuns(f.workspace);
    assert.deepEqual(readRun(f.workspace, queued.id), completed);
  } finally { context.mock.restoreAll(); f.cleanup(); }
});
test('native promotion retains its separate receipt without repackaging the verified archive', async () => {
  const f = fixture();
  try {
    const config = loadConfig(f.workspace);
    config.registries[0].native = { provider: 'unity', endpoint: 'http://127.0.0.1:18080/api/2.1/unity-catalog' };
    writeFileSync(join(f.workspace, contractRoot, 'config.json'), JSON.stringify(config));
    const receipt = { provider: 'unity', native_table_id: 'native-assigned-id', contract_sha256: 'digest' };
    const execute: Execute = async (...args) => {
      const output = await f.execute(...args);
      return args[1][1] === 'promote' ? JSON.stringify({ registered: true, registrationReceipt: receipt }) : output;
    };
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const run = await buildContract(f.workspace, queued.id, execute);
    assert.equal(run.status, 'succeeded', run.error ?? '');
    assert.equal(run.artifact?.sha256, digest('deterministic-archive'));
    assert.deepEqual(run.registrationReceipt, receipt);
    const document = inspectRunDocument(f.workspace, run.id, 'registration');
    assert.equal(document.available, true);
    assert.deepEqual(JSON.parse(document.text!), receipt);
  } finally { f.cleanup(); }
});
test('failed native SDK verification cannot invoke promotion', async () => {
  const f = fixture();
  try {
    const config = loadConfig(f.workspace);
    config.registries[0].native = { provider: 'hive', endpoint: 'thrift://127.0.0.1:19083' };
    writeFileSync(join(f.workspace, contractRoot, 'config.json'), JSON.stringify(config));
    f.fail('test');
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE hive.prod.events (id BIGINT)' });
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'failed');
    assert.equal(run.registration, 'not-attempted');
    assert.ok(!f.calls.some((call) => call.includes('promote')));
    assert.equal(run.artifact, undefined);
  } finally { f.cleanup(); }
});
test('recovery fails an interrupted worker without discarding staged bytes or registration state', (context) => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const interrupted = {
      ...queued, status: 'running' as const, phase: 'publishing', pid: process.pid,
      registration: 'committed' as const, staged: { sha256: digest('archive'), bytes: 7 },
      steps: [{ phase: 'publishing', status: 'running' as const, startedAt: queued.createdAt }],
    };
    saveRun(f.workspace, interrupted);
    context.mock.method(process, 'kill', () => {
      throw Object.assign(new Error('worker exited'), { code: 'ESRCH' });
    });
    recoverRuns(f.workspace);
    const recovered = readRun(f.workspace, queued.id);
    assert.equal(recovered.status, 'failed');
    assert.equal(recovered.registration, 'committed');
    assert.deepEqual(recovered.staged, interrupted.staged);
    assert.match(recovered.error ?? '', /Explicitly resume/);
    assert.equal(recovered.steps?.at(-1)?.status, 'failed');
  } finally { context.mock.restoreAll(); f.cleanup(); }
});
test('SDK verification failure does not register or publish a table', async () => {
  const f = fixture();
  try {
    f.fail('test');
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'failed');
    assert.equal(run.phase, 'verifying');
    assert.equal(run.registration, 'not-attempted');
    assert.equal(run.artifact, undefined);
    assert.equal(run.staged, undefined);
    assert.ok(!f.calls.some((call) => call.includes('promote')));
    assert.equal(run.steps?.at(-1)?.status, 'failed');
    assert.match(run.steps?.at(-1)?.error ?? '', /injected test failure/);
    assert.equal(pipelineSteps(run).find((step) => step.phase === 'promoting')?.status, 'pending');
  } finally { f.cleanup(); }
});
for (const failingPhase of ['validating-proto', 'generating']) test(`${failingPhase} failure cannot register or publish the candidate`, async () => {
  const f = fixture();
  try {
    const paths = protocPaths(f.workspace);
    writeFileSync(paths.exporter, 'exporter');
    writeFileSync(paths.plugin, 'plugin');
    writeFileSync(join(f.workspace, 'tools/libs/arrowcol-sdk-gen/src/protoc.ts'), 'driver');
    const execute: Execute = (command, args, log, env) => {
      if (command === 'protoc' && args[0] === '--version') return Promise.resolve('libprotoc test');
      if (command === 'protoc') {
        if (failingPhase === 'generating' && args.some((arg) => arg.startsWith('--descriptor_set_out='))) return Promise.resolve('');
        return Promise.reject(new Error('injected protoc failure'));
      }
      if (command === paths.exporter) return Promise.resolve('');
      return f.execute(command, args, log, env);
    };
    const queued = enqueue(f.workspace, {
      registry: 'local', mode: 'create', codegen: 'protoc',
      ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)',
    });
    const failed = await buildContract(f.workspace, queued.id, execute);
    assert.equal(failed.status, 'failed');
    assert.equal(failed.phase, failingPhase);
    assert.equal(failed.steps?.at(-1)?.phase, failingPhase);
    assert.equal(failed.steps?.at(-1)?.status, 'failed');
    assert.match(failed.error ?? '', /injected protoc failure/);
    assert.equal(failed.registration, 'not-attempted');
    assert.equal(failed.artifact, undefined);
    assert.ok(!f.calls.some((call) => call.includes('promote')));
  } finally { f.cleanup(); }
});
test('unknown promotion preserves verified bytes and explicit resume promotes the same candidate', async () => {
  const f = fixture();
  try {
    f.fail('promote');
    const queued = enqueue(f.workspace, { registry: 'hosted', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const failed = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(failed.status, 'failed');
    assert.equal(failed.registration, 'unknown');
    assert.ok(failed.staged);
    assert.equal(failed.artifact, undefined);
    const candidatePath = join(runDirectory(f.workspace, queued.id), 'candidate.pb');
    assert.ok(existsSync(candidatePath));
    const count = f.calls.length;
    f.fail('');
    const resumed = await resumePromotion(f.workspace, failed.id, f.execute);
    assert.equal(resumed.status, 'succeeded', resumed.error ?? '');
    assert.equal(resumed.artifact?.sha256, failed.staged.sha256);
    const promotion = pipelineSteps(resumed).find((step) => step.phase === 'promoting')!;
    assert.equal(promotion.status, 'succeeded');
    assert.deepEqual(promotion.attempts.map((attempt) => attempt.status), ['failed', 'succeeded']);
    assert.ok(promotion.attempts.every((attempt) => attempt.finishedAt! >= attempt.startedAt));
    assert.deepEqual(f.calls.slice(count).map((call) => call.slice(1, 3)), [['catalog', 'properties'], ['catalog', 'promote']]);
  } finally { f.cleanup(); }
});
test('rebuilding an explicit committed version never prepares or promotes', async () => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'rebuild', table: 'telemetry.prod.events', version: 1 });
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'succeeded', run.error ?? '');
    assert.equal(run.registration, 'existing');
    assert.ok(f.calls.some((call) => call.includes('export') && call.includes('--version')));
    assert.ok(!f.calls.some((call) => call.includes('prepare') || call.includes('promote')));
    assert.equal(pipelineSteps(run).find((step) => step.phase === 'promoting')?.status, 'skipped');
    const legacy = { ...run, steps: undefined };
    assert.ok(pipelineSteps(legacy).filter((step) => !step.skip && step.phase !== 'submitted')
      .every((step) => step.status === 'not-recorded' && !step.startedAt));
    const resumedLegacy = { ...run, steps: run.steps!.filter((step) => ['finalizing', 'publishing'].includes(step.phase)) };
    assert.equal(pipelineSteps(resumedLegacy).find((step) => step.phase === 'verifying')?.status, 'not-recorded');
    assert.equal(pipelineSteps(resumedLegacy).find((step) => step.phase === 'publishing')?.status, 'succeeded');
  } finally { f.cleanup(); }
});

test('run inspection allowlists documents, reports missing outputs and bounds file reads', () => {
  const f = fixture();
  try {
    const run = enqueue(f.workspace, { registry: 'local', mode: 'create', codegen: 'protoc',
      protoEntry: 'events.proto', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    assert.equal(inspectRunDocument(f.workspace, run.id, 'ddl').text, run.request.mode === 'create' ? run.request.ddl : '');
    assert.equal(inspectRunDocument(f.workspace, run.id, 'compiler').available, false);
    assert.throws(() => inspectRunDocument(f.workspace, run.id, '../../config.json'), /Unknown run document/);
    assert.throws(() => inspectRunDocument(f.workspace, run.id, '__proto__'), /Unknown run document/);
    assert.throws(() => inspectRunDocument(f.workspace, '../../', 'schema'), /Invalid run/);
    const directory = join(runDirectory(f.workspace, run.id), 'payload', 'compiler');
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, 'events.proto'), 'x'.repeat(150_000));
    const document = inspectRunDocument(f.workspace, run.id, 'compiler');
    assert.equal(document.text?.length, 128 * 1024);
    assert.equal(document.truncated, true);
    rmSync(join(directory, 'events.proto'));
    symlinkSync(join(f.workspace, 'package-lock.json'), join(directory, 'events.proto'));
    assert.throws(() => inspectRunDocument(f.workspace, run.id, 'compiler'), /symbolic links/);
  } finally { f.cleanup(); }
});
test('immutable publication accepts identical bytes and rejects a conflicting destination', () => {
  const f = fixture();
  try {
    const source = join(f.workspace, 'source');
    const destination = join(f.workspace, 'destination');
    writeFileSync(source, 'original');
    publishArchive(source, destination);
    publishArchive(source, destination);
    writeFileSync(source, 'changed staging bytes');
    assert.equal(readFileSync(destination, 'utf8'), 'original', 'release does not share a mutable staging inode');
    const other = join(f.workspace, 'other');
    writeFileSync(other, 'different');
    assert.throws(() => publishArchive(other, destination), /Immutable release collision/);
    assert.equal(readFileSync(destination, 'utf8'), 'original');
  } finally { f.cleanup(); }
});

test('changed registry authority cannot redirect a queued candidate build', async () => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    const config = loadConfig(f.workspace);
    config.registries[0].authority = 'different-authority';
    writeFileSync(join(f.workspace, contractRoot, 'config.json'), JSON.stringify(config));
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'failed');
    assert.match(run.error ?? '', /Registry configuration changed/);
    assert.ok(!f.calls.some((call) => call.includes('prepare') || call.includes('promote')));
  } finally { f.cleanup(); }
});

test('damaged staged bytes cannot be promoted on resume', async () => {
  const f = fixture();
  try {
    f.fail('promote');
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    await buildContract(f.workspace, queued.id, f.execute);
    writeFileSync(join(runDirectory(f.workspace, queued.id), 'contract-sdk.tar.gz'), 'damaged');
    const count = f.calls.length;
    f.fail('');
    const run = await resumePromotion(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'failed');
    assert.match(run.error ?? '', /Staged archive digest mismatch/);
    assert.deepEqual(f.calls.slice(count).map((call) => call.slice(1, 3)), [['catalog', 'properties']]);
    assert.equal(run.artifact, undefined);
  } finally { f.cleanup(); }
});

test('a stale UI default vocabulary is rejected before preparation', async () => {
  const f = fixture();
  try {
    const queued = enqueue(f.workspace, {
      registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)',
      definitionsDigest: `sha256:${'b'.repeat(64)}`,
    });
    const run = await buildContract(f.workspace, queued.id, f.execute);
    assert.equal(run.status, 'failed');
    assert.match(run.error ?? '', /Property definitions changed/);
    assert.ok(!f.calls.some((call) => call.includes('prepare') || call.includes('promote')));
  } finally { f.cleanup(); }
});

test('finalization rejects changed compiled defaults instead of silently reinterpreting a candidate', async () => {
  const f = fixture();
  try {
    f.fail('promote');
    const queued = enqueue(f.workspace, { registry: 'local', mode: 'create', ddl: 'CREATE TABLE telemetry.prod.events (id BIGINT)' });
    await buildContract(f.workspace, queued.id, f.execute);
    const changed: Execute = (command, args, log, env) => args[1] === 'properties'
      ? Promise.resolve(JSON.stringify({ definitions_sha256: `sha256:${'b'.repeat(64)}` }))
      : f.execute(command, args, log, env);
    const before = f.calls.length;
    const run = await resumePromotion(f.workspace, queued.id, changed);
    assert.equal(run.status, 'failed');
    assert.match(run.error ?? '', /Compiled option definitions changed/);
    assert.equal(f.calls.length, before);
    assert.equal(run.artifact, undefined);
  } finally { f.cleanup(); }
});
