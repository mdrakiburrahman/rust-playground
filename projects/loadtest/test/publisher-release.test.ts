import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test, type TestContext } from 'node:test';

import { BENCHMARK_PROJECT } from '../src/cli.js';
import { admitPublisherRelease, sqlIdentity } from '../src/publisher-release.js';
import { fixtureDirectory } from './fixture-directory.js';

function fixture(t: TestContext) {
  const workspace = fixtureDirectory('release-');
  t.after(() => rmSync(workspace, { recursive: true, force: true }));
  const directory = path.join(workspace, 'projects/loadtest/.temp/benchmark/latest');
  const results = path.join(directory, 'results');
  mkdirSync(results, { recursive: true });
  writeFileSync(path.join(results, 'run-metadata.json'), '{"run_id":"run-1"}');
  const sdk = path.join(directory, 'sdk');
  writeFileSync(sdk, 'frozen-sdk', { mode: 0o700 });
  writeFileSync(path.join(results, 'provenance.json'), JSON.stringify({
    helpers: { host: { 'arrowcol-sdk-gen': {
      sha256: createHash('sha256').update('frozen-sdk').digest('hex'), bytes: Buffer.byteLength('frozen-sdk'),
    } } },
  }));
  const events: string[] = [];
  let generation = 'first';
  let stopped = false;
  let writerStartedAt = '2026-01-01T00:00:00Z';
  const labels = (service: string) => ({
    'com.docker.compose.project': BENCHMARK_PROJECT,
    'com.docker.compose.service': service,
    'com.docker.compose.project.config_files': path.join(workspace, 'projects/loadtest/compose.yaml'),
  });
  const docker = (args: string[]): string => {
    if (args[0] === 'inspect' && args[2] === '{{.State.StartedAt}}') return writerStartedAt;
    if (args[0] === 'ps') return args.includes('--filter') ? 'sql-id' : 'direct-id\ndataflow-id';
    if (args[0] === 'inspect' && args[1] !== '--format') return JSON.stringify([{
      Id: 'a'.repeat(64), Image: 'sha256:sql', Config: { Labels: labels('sqlserver') },
      State: { Running: true, StartedAt: generation },
    }]);
    const service = args.at(-1) === 'direct-id' ? 'direct-1' : 'dataflow-1';
    return JSON.stringify({ id: args.at(-1), labels: labels(service), image: 'image', mounts: [],
      state: { Running: !stopped, ExitCode: 0, OOMKilled: false } });
  };
  const stop = async () => { events.push('stop'); stopped = true; return ['direct-1', 'dataflow-1']; };
  const release = async () => {
    assert(stopped);
    assert(existsSync(path.join(results, 'mutator-drain.json')));
    events.push('sql-release');
    writeFileSync(path.join(results, 'publisher-release.json'),
      JSON.stringify({ passed: true, run_id: 'run-1', errors: [], routing: {}, receipts: {} }));
  };
  const compact = async () => { events.push('final-compaction'); };
  return { workspace, directory, results, sdk, docker, stop, release, compact, events,
    sql: sqlIdentity(workspace, docker), drift: () => { generation = 'replacement'; },
    restartWriter: () => { writerStartedAt = '2026-01-01T00:00:01Z'; } };
}

test('release reads SQL only after every writer exits and persists a same-run protocol binding', async t => {
  const f = fixture(t);
  const mutators = await admitPublisherRelease(f);
  assert.deepEqual(f.events, ['stop', 'sql-release', 'final-compaction']);
  assert.equal(mutators.length, 2);
  const receipt = JSON.parse(readFileSync(path.join(f.results, 'publisher-admission.json'), 'utf8'));
  assert.equal(receipt.protocol, 'sql-terminal-state-and-owned-clean-exit');
  assert.equal(receipt.run_id, 'run-1');
  assert.equal(receipt.release_sha256.length, 64);
  assert.equal(receipt.sdk_sha256.length, 64);
});

test('stop failure prevents SQL release and all downstream compaction', async t => {
  const f = fixture(t);
  let compacted = false;
  await assert.rejects(async () => {
    await admitPublisherRelease({ ...f, stop: async () => { throw new Error('drain failed'); } });
    compacted = true;
  }, /drain failed/u);
  assert.equal(compacted, false);
  assert.deepEqual(f.events, []);
});

test('SQL release failure never admits downstream maintenance', async t => {
  const f = fixture(t);
  let compacted = false;
  await assert.rejects(async () => {
    await admitPublisherRelease({ ...f, release: async () => { throw new Error('SQL owner draining'); } });
    compacted = true;
  }, /SQL owner draining/u);
  assert.equal(compacted, false);
  assert.equal(existsSync(path.join(f.results, 'publisher-admission.json')), false);
});

test('SQL generation changes before or during read fail admission', async t => {
  for (const duringRead of [false, true]) {
    const f = fixture(t);
    if (!duringRead) f.drift();
    await assert.rejects(admitPublisherRelease({
      ...f, release: async () => { await f.release(); f.drift(); },
    }), /generation changed/u);
    assert.deepEqual(f.events, duringRead ? ['stop', 'sql-release'] : ['stop']);
    assert.equal(existsSync(path.join(f.results, 'publisher-admission.json')), false);
  }
});

test('stale or failed SQL receipt is not a clean-exit substitute', async t => {
  for (const proof of [
    { passed: false, run_id: 'run-1', errors: ['owner draining'], routing: {}, receipts: {} },
    { passed: true, run_id: 'old-run', errors: [], routing: {}, receipts: {} },
    { passed: true, run_id: 'run-1', errors: [], routing: {} },
  ]) {
    const f = fixture(t);
    await assert.rejects(admitPublisherRelease({ ...f, release: async () => {
      writeFileSync(path.join(f.results, 'publisher-release.json'), JSON.stringify(proof));
    } }), /not proven/u);
    assert.equal(existsSync(path.join(f.results, 'publisher-admission.json')), false);
  }
});

test('compaction failure or generation drift propagates before final validation', async t => {
  const f = fixture(t);
  await assert.rejects(admitPublisherRelease({ ...f, compact: async () => {
    throw new Error('real compaction failed');
  } }), /real compaction failed/u);
  const drift = fixture(t);
  await assert.rejects(admitPublisherRelease({ ...drift, compact: async () => { drift.drift(); } }),
    /generation changed during final compaction/u);
});

test('SDK changes since startup or during SQL read fail admission', async t => {
  for (const duringRead of [false, true]) {
    const f = fixture(t);
    if (!duringRead) writeFileSync(f.sdk, 'replacement-sdk');
    await assert.rejects(admitPublisherRelease({ ...f, release: async () => {
      await f.release();
      writeFileSync(f.sdk, 'replacement-sdk');
    } }), /SDK changed/u);
    assert(!f.events.includes('final-compaction'));
  }
});

test('same-container process restart during release is not a clean successor generation', async t => {
  const f = fixture(t);
  await assert.rejects(admitPublisherRelease({ ...f, release: async () => {
    await f.release();
    f.restartWriter();
  } }), /Writer generation changed during release/u);
  assert(!f.events.includes('final-compaction'));
});
