import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { test } from 'node:test';

import { exportQapPoints, qapPoints, startQapObserver } from '../src/qap-observer.js';

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
  return {
    observed_unix_ms: observed, backend: 'native-qap', mode: 'local',
    tables: Object.fromEntries<FixtureTable>(['direct', 'dataflow'].flatMap(pool => Array.from({ length: 13 }, (_, index) => [
      `${pool}_sqlserver_fixture_${index}`, {
        available: true, statistics: {
          AccelerationCompletePercentage: 80 + index, AccelerationPendingDataFilesCount: index,
          HotSize: 1024, LastUpdatedDateTime: new Date(observed - (index + 1) * 1000).toISOString(),
        }, state: { is_healthy: index !== 0 },
      },
    ] as const))),
  };
}

test('QAP gauges aggregate all13 per pool without table, tenant, file or run labels', () => {
  const result = qapPoints(fixture());
  for (const pool of ['direct', 'dataflow']) {
    const values = Object.fromEntries(result.points.filter(point => point.pool === pool).map(point => [point.name, point.value]));
    assert.equal(values.qap_completion_percent, 80);
    assert.equal(values.qap_pending_artifacts, 78);
    assert.equal(values.qap_cached_bytes, 13 * 1024);
    assert.equal(values.qap_unhealthy_catalogs, 1);
    assert.equal(values.qap_lag_seconds, 13);
    assert.equal(values.qap_observation_complete, 1);
  }
});

test('QAP partial/missing measurements never become zero or reuse previous aggregate values', () => {
  const value = fixture();
  value.tables.direct_sqlserver_fixture_0.statistics.HotSize = null;
  value.tables.direct_sqlserver_fixture_1.state.is_healthy = null;
  value.tables.direct_sqlserver_fixture_2.statistics.LastUpdatedDateTime = 'not-a-date';
  const points = qapPoints(value).points;
  assert(!points.some(point => point.pool === 'direct' && ['qap_cached_bytes', 'qap_unhealthy_catalogs', 'qap_lag_seconds'].includes(point.name)));
  assert.equal(points.find(point => point.pool === 'direct' && point.name === 'qap_observation_complete')?.value, 0);
  assert.equal(points.find(point => point.pool === 'dataflow' && point.name === 'qap_observation_complete')?.value, 1);
  delete value.tables.direct_sqlserver_fixture_3;
  assert.throws(() => qapPoints(value), /all 13/u);
});

test('QAP rejects nonfinite percentages, future timestamps and malformed provenance', () => {
  const value = fixture();
  value.tables.direct_sqlserver_fixture_0.statistics.AccelerationCompletePercentage = Number.NaN;
  value.tables.direct_sqlserver_fixture_0.statistics.LastUpdatedDateTime = new Date(value.observed_unix_ms + 1).toISOString();
  const points = qapPoints(value).points.filter(point => point.pool === 'direct');
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
  await exportQapPoints(endpoint, 1_800_000_000_000, [{ name: 'qap_pending_artifacts', pool: 'direct', value: 0 }]);
  const serialized = JSON.stringify(bodies[0]);
  assert(serialized.includes('"asDouble":0'));
  assert(serialized.includes('"timeUnixNano":"1800000000000000000"'));
  assert(!serialized.includes('tenant') && !serialized.includes('table') && !serialized.includes('run_id'));
  reject = true;
  await assert.rejects(exportQapPoints(endpoint, 1_800_000_000_000, [{ name: 'qap_pending_artifacts', pool: 'direct', value: 0 }]), /rejected/u);
});
