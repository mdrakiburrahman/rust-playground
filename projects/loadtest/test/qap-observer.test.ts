import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';

import { exportQapPoints, qapPoints, startQapObserver } from '../src/qap-observer.js';

test('qualification drains the in-flight observer and requires one fresh final sample without validation contention', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'qap-scheduling-'));
  t.after(() => rm(root, { force: true, recursive: true }));
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'] });
  let release!: () => void;
  const blocked = new Promise<void>(resolve => { release = resolve; });
  let announce!: () => void;
  const started = new Promise<void>(resolve => { announce = resolve; });
  let helpers = 0, exports = 0;
  let validating = false;
  const observer = startQapObserver({
    binary: 'fake-helper', cwd: root, env: {}, results: root,
    endpoint: 'http://fixture.invalid', otlpEndpoint: 'http://fixture.invalid', intervalMs: 1,
  }, {
    runHelper: async (_command, args, options) => {
      assert.equal(validating, false);
      assert.equal(args[args.indexOf('--timeout') + 1], '30s');
      assert.equal(options.timeoutMs, 40_000);
      if (++helpers === 1) { announce(); await blocked; }
      await writeFile(args[args.indexOf('--output') + 1]!, JSON.stringify(fixture()));
    },
    publish: async () => { assert.equal(validating, false); exports++; },
  });
  await started;
  let stopped = false;
  const draining = observer().then(() => { stopped = true; });
  await Promise.resolve();
  assert.equal(stopped, false, 'stop must await the already-admitted helper');
  release();
  await draining;
  validating = true;
  t.mock.timers.tick(600_000);
  assert.equal(helpers, 1, 'no periodic helper may overlap the heavy validation window');
  validating = false;
  await observer.sampleFinal();
  await observer.sampleFinal();
  await observer();
  assert.equal(helpers, 2);
  assert.equal(exports, 2);
  const receipts = (await readFile(path.join(root, 'qap-observer.jsonl'), 'utf8')).trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(receipts.map(receipt => receipt.final), [false, true]);
  assert.deepEqual(receipts.map(receipt => receipt.artifact), ['qap-observations/0/observation.json', 'qap-observations/1/observation.json']);
  const main = await readFile(new URL('../src/main.ts', import.meta.url), 'utf8');
  const quiesce = main.indexOf("phase = 'qap-observer-quiesce'");
  const drain = main.indexOf('await stopQapObserver()', quiesce);
  const validate = main.indexOf("await runLocalLoadgen(context, 'validate'", drain);
  const ui = main.indexOf('await runUiGate(', validate);
  const final = main.indexOf('await stopQapObserver.sampleFinal()', ui);
  assert(quiesce >= 0 && drain > quiesce && validate > drain && ui > validate && final > ui);
  assert(main.indexOf("phase = 'publication-drain'", drain) < validate);
});

for (const failingPhase of ['helper', 'export'] as const) {
  test(`final ${failingPhase} failure remains recorded and fails qualification without retry`, async t => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'qap-final-failure-'));
    t.after(() => rm(root, { force: true, recursive: true }));
    let announce!: () => void;
    const published = new Promise<void>(resolve => { announce = resolve; });
    let helpers = 0, exports = 0;
    const observer = startQapObserver({
      binary: 'fake-helper', cwd: root, env: {}, results: root,
      endpoint: 'http://fixture.invalid', otlpEndpoint: 'http://fixture.invalid',
    }, {
      runHelper: async (_command, args) => {
        if (++helpers === 2 && failingPhase === 'helper') throw new Error('injected final helper failure');
        await writeFile(args[args.indexOf('--output') + 1]!, JSON.stringify(fixture()));
      },
      publish: async () => {
        if (++exports === 2 && failingPhase === 'export') throw new Error('injected final export failure');
        announce();
      },
    });
    await published;
    await observer();
    await assert.rejects(observer.sampleFinal(), /qualification is incomplete/u);
    await assert.rejects(observer.sampleFinal(), /qualification is incomplete/u);
    await assert.rejects(observer(), /qualification is incomplete/u);
    assert.equal(helpers, 2);
    const receipts = (await readFile(path.join(root, 'qap-observer.jsonl'), 'utf8')).trim().split('\n').map(line => JSON.parse(line));
    assert.equal(receipts.length, 2);
    assert.equal(receipts[1].final, true);
    assert.equal(receipts[1].phase, failingPhase);
    assert.equal(receipts[1].exported, false);
  });
}

for (const mode of ['child-failure', 'invalid-output', 'invalid-diagnostic', 'array-code', 'nested-array-code'] as const) {
  test(`observer reports fixed privacy-safe phases for actual ${mode}`, async t => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'qap-diagnostic-'));
    t.after(() => rm(root, { force: true, recursive: true }));
    const binary = path.join(root, 'helper.mjs');
    await writeFile(binary, `#!${process.execPath}
import fs from 'node:fs';
import path from 'node:path';
const args = process.argv;
const root = args[args.indexOf('--results') + 1];
const output = args[args.indexOf('--output') + 1];
const mode = ${JSON.stringify(mode)};
const diagnostic = value => fs.writeFileSync(path.join(root, 'observation-diagnostic.json'), JSON.stringify(value));
console.error('https://secret.invalid/password');
if (mode === 'child-failure') {
  diagnostic({code:'deadline_exceeded',elapsed_ms:30000,queries_completed:64});
  process.exit(3);
}
if (mode === 'invalid-diagnostic') diagnostic({code:'https://secret.invalid/password'});
if (mode === 'array-code' || mode === 'nested-array-code') {
  diagnostic({code:mode === 'array-code' ? ['completed'] : [['completed']],elapsed_ms:1,queries_completed:0});
}
fs.writeFileSync(output, 'https://secret.invalid/password');
`);
    await chmod(binary, 0o700);
    const server = createServer((request, response) => {
      request.resume();
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end('{}');
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    t.after(() => new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())));
    const address = server.address();
    assert(address && typeof address !== 'string');
    const stop = startQapObserver({ binary, cwd: root, env: process.env, results: root,
      endpoint: 'http://secret.invalid', otlpEndpoint: `http://127.0.0.1:${address.port}` });
    const deadline = Date.now() + 5000;
    let text = '';
    while (Date.now() < deadline) {
      try {
        text = await readFile(path.join(root, 'qap-observer.jsonl'), 'utf8');
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      if (text) break;
      await delay(10);
    }
    await assert.rejects(stop(), /qualification is incomplete/u);
    assert(text, 'the actual child must produce a failure receipt');
    assert(!text.includes('secret.invalid') && !text.includes(root));
    const failure = JSON.parse(text.trim()) as Record<string, unknown>;
    assert.equal(failure.sequence, 0);
    assert.equal(failure.exported, false);
    assert.equal(failure.phase, mode === 'child-failure' ? 'helper' : 'output');
    assert.equal(failure.code, mode === 'child-failure' ? 'helper_failed' : 'output_failed');
    assert(Number.isSafeInteger(failure.elapsed_ms) && Number(failure.elapsed_ms) >= 0);
    assert.equal(failure.diagnostic_invalid, ['invalid-diagnostic', 'array-code', 'nested-array-code'].includes(mode));
    if (mode === 'array-code' || mode === 'nested-array-code') {
      assert.equal(failure.helper_code, null);
      assert.equal(failure.queries_completed, null);
      assert(!text.includes('["completed"]'));
    }
    if (mode === 'child-failure') {
      assert.equal(failure.helper_code, 'deadline_exceeded');
      assert.equal(failure.queries_completed, 64);
      assert.equal(failure.helper_elapsed_ms, 30000);
      assert.deepEqual(failure.process, { exit_code: 3, signal: null, timed_out: false,
        interrupted: false, descendants: false });
    }
  });
}

interface FixtureTable {
  available: boolean;
  statistics: {
    AccelerationCompletePercentage: number | null;
    AccelerationPendingDataFilesCount: number | null;
    HotSize: number | null;
    LastUpdatedDateTime: string | null;
  };
  state: { is_healthy: boolean | null };
}

function fixture() {
  const observed = 1_800_000_000_000;
  const expected_tables = Array.from({ length: 13 }, (_, index) => 'dataflow_sqlserver_fixture_' + index);
  return {
    observed_unix_ms: observed, backend: 'native-qap', mode: 'local', expected_tables,
    tables: Object.fromEntries<FixtureTable>(expected_tables.map((name, index) => [name, {
      available: true, statistics: {
        AccelerationCompletePercentage: 80 + index, AccelerationPendingDataFilesCount: index,
        HotSize: 1024, LastUpdatedDateTime: new Date(observed - (index + 1) * 1000).toISOString(),
      }, state: { is_healthy: index !== 0 },
    }])),
  };
}

test('QAP gauges aggregate all expected Dataflow tables without table, tenant, file or run labels', () => {
  const value = fixture();
  const result = qapPoints(value);
  for (const pool of ['dataflow']) {
    const values = Object.fromEntries(result.points.filter(point => point.pool === pool).map(point => [point.name, point.value]));
    assert.equal(values.qap_completion_percent, 80);
    assert.equal(values.qap_pending_artifacts, 78);
    assert.equal(values.qap_cached_bytes, value.expected_tables.length * 1024);
    assert.equal(values.qap_unhealthy_catalogs, 1);
    assert.equal(values.qap_lag_seconds, value.expected_tables.length);
    assert.equal(values.qap_observation_complete, 1);
  }
});

test('QAP partial/missing measurements never become zero or reuse previous aggregate values', () => {
  const value = fixture();
  value.tables.dataflow_sqlserver_fixture_0.statistics.HotSize = null;
  value.tables.dataflow_sqlserver_fixture_1.state.is_healthy = null;
  value.tables.dataflow_sqlserver_fixture_2.statistics.LastUpdatedDateTime = 'not-a-date';
  const points = qapPoints(value).points;
  assert(!points.some(point => point.pool === 'dataflow' && ['qap_cached_bytes', 'qap_unhealthy_catalogs', 'qap_lag_seconds'].includes(point.name)));
  assert.equal(points.find(point => point.pool === 'dataflow' && point.name === 'qap_observation_complete')?.value, 0);
  delete value.tables.dataflow_sqlserver_fixture_3;
  assert.throws(() => qapPoints(value), /expected Dataflow source-table set/u);
});

test('QAP rejects retired Direct tables and any missing retained Dataflow table', () => {
  const missing = fixture();
  delete missing.tables.dataflow_sqlserver_fixture_12;
  assert.throws(() => qapPoints(missing), /expected Dataflow source-table set/u);

  const retired = fixture();
  retired.tables.direct_sqlserver_wait_stats = retired.tables[retired.expected_tables[0]];
  assert.throws(() => qapPoints(retired), /expected Dataflow source-table set/u);

  const retiredExpected = fixture();
  retiredExpected.expected_tables.push('direct_sqlserver_wait_stats');
  assert.throws(() => qapPoints(retiredExpected), /unique Dataflow SQL Server source tables/u);
});

test('QAP rejects nonfinite percentages, future timestamps and malformed provenance', () => {
  const value = fixture();
  value.tables.dataflow_sqlserver_fixture_0.statistics.AccelerationCompletePercentage = Number.NaN;
  value.tables.dataflow_sqlserver_fixture_0.statistics.LastUpdatedDateTime = new Date(value.observed_unix_ms + 1).toISOString();
  const points = qapPoints(value).points.filter(point => point.pool === 'dataflow');
  assert(!points.some(point => ['qap_completion_percent', 'qap_lag_seconds'].includes(point.name)));
  assert.throws(() => qapPoints({ ...value, backend: 'bridge' }), /Malformed/u);
  assert.throws(() => qapPoints({ ...value, observed_unix_ms: Number.NaN }), /Malformed/u);
});

test('actual HTTP OTLP export preserves measured zeros and rejects partial acceptance', async t => {
  const bodies: unknown[] = [];
  let reject = false;
  const server = createServer((request, response) => {
    let body = '';
    request.setEncoding('utf8').on('data', data => { body += String(data); });
    request.on('end', () => {
      bodies.push(JSON.parse(body) as unknown);
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify(reject ? { partialSuccess: { rejectedDataPoints: '1' } } : {}));
    });

  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())));
  const address = server.address();
  assert(address && typeof address !== 'string');
  const endpoint = `http://127.0.0.1:${address.port}`;
  await exportQapPoints(endpoint, 1_800_000_000_000, [{ name: 'qap_pending_artifacts', pool: 'dataflow', value: 0 }]);
  const serialized = JSON.stringify(bodies[0]);
  assert(serialized.includes('"asDouble":0'));
  assert(serialized.includes('"timeUnixNano":"1800000000000000000"'));
  assert(!serialized.includes('tenant') && !serialized.includes('table') && !serialized.includes('run_id'));
  reject = true;
  await assert.rejects(exportQapPoints(endpoint, 1_800_000_000_000, [{ name: 'qap_pending_artifacts', pool: 'dataflow', value: 0 }]), /rejected/u);
});
