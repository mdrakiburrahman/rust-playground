import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer, type RequestListener } from 'node:http';
import path from 'node:path';
import { test, type TestContext } from 'node:test';

import { parseBenchmarkOptions } from '../src/cli.js';
import {
  kustoComposeStartArguments, kustoStatusSummary, readKustoStatus, requireHealthyKusto, safeKustoError, startKusto,
} from '../src/kusto.js';
import { benchmarkCoreServices, localLoadgenEnvironment, type BenchmarkNetwork } from '../src/main.js';

const ready = { Tables: [{ Columns: [{ ColumnName: 'NodeId', ColumnType: 'string' }], Rows: [['local-node']] }] };

function fixture(t: TestContext): string {
  const directory = path.resolve(`projects/loadtest/test/.fixture-kusto-${randomUUID()}`);
  mkdirSync(directory);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return directory;
}

async function endpoint(t: TestContext, handler: RequestListener): Promise<string> {
  const server = createServer(handler);
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  });
  const address = server.address();
  assert(address && typeof address !== 'string');
  return `http://127.0.0.1:${address.port}`;
}

test('Kusto starts independently without core --wait or required dependencies', () => {
  assert(!benchmarkCoreServices.includes('kusto'));
  assert.deepEqual(kustoComposeStartArguments, [
    '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', 'kusto',
  ]);
});

test('both local helper commands always receive the allocated internal Kusto endpoint', () => {
  const network: BenchmarkNetwork = {
    subnet: '10.89.42.0/24', sql: '10.89.42.2', hive: '10.89.42.3', router: '10.89.42.10',
    direct: '10.89.42.21', dataflow: '10.89.42.31', kusto: '10.89.42.50',
    kustoExplorer: '10.89.42.51', playwright: '10.89.42.52', kustoBridge: '10.89.42.53',
  };
  const env = localLoadgenEnvironment({
    directory: '/workspace/onelake/benchmark/latest', network, options: parseBenchmarkOptions([]),
    env: { LOADTEST_KUSTO_ENDPOINT: 'http://wrong-endpoint:1', KUSTO_PORT: '18080' },
  }, '/workspace/onelake/benchmark/latest/results');
  assert.equal(env.LOADTEST_KUSTO_ENDPOINT, 'http://10.89.42.50:8080');
  assert.equal(env.LOADTEST_DIRECT_ROOT, '/workspace/onelake/benchmark/latest/direct');
  assert.equal(env.LOADTEST_DATAFLOW_ROOT, '/workspace/onelake/benchmark/latest/dataflow');
  assert.equal(env.LOADTEST_KUSTO_BRIDGE_ENDPOINT, undefined);
  for (const mode of ['local', 'azure']) {
    const optional = localLoadgenEnvironment({
      directory: '/workspace/onelake/benchmark/latest', network,
      options: parseBenchmarkOptions(['--mode', mode], {}),
      env: { LOADTEST_KUSTO_BRIDGE_ENDPOINT: 'http://explicit-bridge:9464' },
    }, '/workspace/onelake/benchmark/latest/results');
    assert.equal(optional.LOADTEST_KUSTO_BRIDGE_ENDPOINT, mode === 'local' ? 'http://explicit-bridge:9464' : undefined);
  }
});

test('readiness uses the documented cluster command without requiring the not-yet-created database', async t => {
  const results = fixture(t);
  const requests: Array<{ method?: string; url?: string; type?: string; body: unknown }> = [];
  const address = await endpoint(t, (request, response) => {
    let body = '';
    request.on('data', chunk => { body += String(chunk); });
    request.on('end', () => {
      requests.push({
        method: request.method, url: request.url, type: request.headers['content-type'], body: JSON.parse(body),
      });
      response.end(JSON.stringify(ready));
    });
  });
  let starts = 0;
  const status = await startKusto({ endpoint: address, results, start: () => { starts++; }, timeoutMs: 1_000 });
  assert.equal(starts, 1);
  assert.equal(status.state, 'healthy');
  assert.equal(status.phase, 'probe');
  assert.deepEqual(requests, [{ method: 'POST', url: '/v1/rest/mgmt', type: 'application/json', body: { csl: '.show cluster' } }]);
  assert.deepEqual(JSON.parse(readFileSync(path.join(results, 'kusto-startup.json'), 'utf8')), status);
  assert(!existsSync(path.join(results, 'kusto-status.json')), 'startup must not overwrite Rust synchronization status');
  assert.throws(() => requireHealthyKusto(readKustoStatus(results)), /DEGRADED/u);
});

test('a temporarily unavailable cluster can become ready within the bounded probe deadline', async t => {
  const results = fixture(t);
  let requests = 0;
  const address = await endpoint(t, (_request, response) => {
    requests++;
    response.statusCode = requests === 1 ? 503 : 200;
    response.end(requests === 1 ? 'TOP_SECRET' : JSON.stringify(ready));
  });
  const status = await startKusto({
    endpoint: address, results, start: () => {}, timeoutMs: 1_000, requestTimeoutMs: 200, intervalMs: 1,
  });
  assert.equal(status.state, 'healthy');
  assert.equal(requests, 2);
  assert(!readFileSync(path.join(results, 'kusto-startup.json'), 'utf8').includes('TOP_SECRET'));
});

test('image/startup failure is retained and does not throw or overwrite a later recovered mirror status', async t => {
  const results = fixture(t);
  const status = await startKusto({
    endpoint: 'http://127.0.0.1:1', results,
    start: () => { throw new Error('image pull failed: password=TOP_SECRET token=TOP_SECRET'); },
  });
  assert.equal(status.state, 'degraded');
  assert.equal(status.phase, 'start');
  assert.match(status.error ?? '', /image pull failed/u);
  assert(!readFileSync(path.join(results, 'kusto-startup.json'), 'utf8').includes('TOP_SECRET'));
  writeFileSync(path.join(results, 'kusto-status.json'), JSON.stringify({
    state: 'healthy', live_observed: true, files_ingested: 26,
  }));
  assert.equal(readKustoStatus(results).state, 'healthy');
  assert.equal(JSON.parse(readFileSync(path.join(results, 'kusto-startup.json'), 'utf8')).state, 'degraded');
});

for (const [name, body] of Object.entries({
  malformed: 'TOP_SECRET',
  error: JSON.stringify({ error: { message: 'TOP_SECRET' } }),
  exception: JSON.stringify({ ...ready, Exceptions: ['TOP_SECRET'] }),
  partial: JSON.stringify({ ...ready, HasErrors: true }),
  empty: JSON.stringify({ Tables: [] }),
  noNodes: JSON.stringify({ Tables: [{ Columns: ready.Tables[0].Columns, Rows: [] }] }),
})) {
  test(`HTTP 200 with ${name} cluster response is degraded and never saves its body`, async t => {
    const results = fixture(t);
    const address = await endpoint(t, (_request, response) => response.end(body));
    const status = await startKusto({
      endpoint: address, results, start: () => {}, timeoutMs: 40, requestTimeoutMs: 20, intervalMs: 1,
    });
    assert.equal(status.state, 'degraded');
    assert.equal(status.phase, 'probe');
    assert.match(status.error ?? '', /Kusto readiness failed/u);
    assert(!readFileSync(path.join(results, 'kusto-startup.json'), 'utf8').includes('TOP_SECRET'));
  });
}

test('non-success HTTP response bodies are withheld while the real status is retained', async t => {
  const results = fixture(t);
  const address = await endpoint(t, (_request, response) => {
    response.statusCode = 401;
    response.end('TOP_SECRET');
  });
  const status = await startKusto({
    endpoint: address, results, start: () => {}, timeoutMs: 40, requestTimeoutMs: 20, intervalMs: 1,
  });
  assert.equal(status.state, 'degraded');
  assert.match(status.error ?? '', /HTTP 401/u);
  assert(!readFileSync(path.join(results, 'kusto-startup.json'), 'utf8').includes('TOP_SECRET'));
});

test('an unresponsive emulator cannot hold startup indefinitely', async t => {
  const results = fixture(t);
  const address = await endpoint(t, () => {});
  const status = await startKusto({
    endpoint: address, results, start: () => {}, timeoutMs: 40, requestTimeoutMs: 10, intervalMs: 1,
  });
  assert.equal(status.state, 'degraded');
  assert.match(status.error ?? '', /Kusto readiness failed/u);
});

test('final status distinguishes a healthy live mirror from degraded or unverifiable runs', t => {
  const results = fixture(t);
  assert.equal(readKustoStatus(results).state, 'degraded');
  const file = path.join(results, 'kusto-status.json');
  const healthy = { state: 'healthy', live_observed: true, files_ingested: 26 } as const;
  writeFileSync(file, JSON.stringify(healthy));
  assert.deepEqual(readKustoStatus(results), healthy);
  assert.match(kustoStatusSummary(readKustoStatus(results)), /Kusto healthy \(26 files ingested; live query observed\)/u);
  requireHealthyKusto(readKustoStatus(results));
  for (const status of [
    {}, { ...healthy, state: 'unknown' }, { ...healthy, live_observed: undefined },
    { ...healthy, files_ingested: -1 }, { ...healthy, files_ingested: Number.MAX_SAFE_INTEGER + 1 },
    { ...healthy, error: { message: 'TOP_SECRET' } },
  ]) {
    writeFileSync(file, JSON.stringify(status));
    const actual = readKustoStatus(results);
    assert.equal(actual.state, 'degraded');
    assert.throws(() => requireHealthyKusto(actual), /DEGRADED/u);
  }
  writeFileSync(file, 'not JSON TOP_SECRET');
  assert(!kustoStatusSummary(readKustoStatus(results)).includes('TOP_SECRET'));
});

test('an error cannot be presented as healthy and --validate requires a live query observation', t => {
  const results = fixture(t);
  const file = path.join(results, 'kusto-status.json');
  for (const state of ['healthy', 'degraded']) {
    writeFileSync(file, JSON.stringify({
      state, live_observed: true, files_ingested: 26, error: 'ingestion failed password=TOP_SECRET',
    }));
    const status = readKustoStatus(results);
    assert.equal(status.state, 'degraded');
    assert.match(kustoStatusSummary(status), /Kusto DEGRADED: ingestion failed/u);
    assert(!kustoStatusSummary(status).includes('TOP_SECRET'));
    assert.throws(() => requireHealthyKusto(status), /DEGRADED/u);
  }
  writeFileSync(file, JSON.stringify({ state: 'healthy', live_observed: false, files_ingested: 26 }));
  const status = readKustoStatus(results);
  assert.match(kustoStatusSummary(status), /live query NOT observed/u);
  assert.throws(() => requireHealthyKusto(status), /no in-run live query observation/u);
});

test('diagnostic errors redact authorization, secret assignments and credential-bearing URLs', () => {
  const text = safeKustoError(new Error([
    'connect failed: https://user:TOP_SECRET@localhost:8080/?token=TOP_SECRET&sig=TOP_SECRET',
    'Authorization: Bearer TOP_SECRET', '"password": "TOP_SECRET"', "secret='TOP_SECRET'",
  ].join('\n')));
  assert.match(text, /connect failed/u);
  assert(!text.includes('TOP_SECRET'));
  assert(!text.includes('user:'));
  assert(!text.includes('\n'));
  assert(safeKustoError('x'.repeat(4000)).length <= 2048);
});
