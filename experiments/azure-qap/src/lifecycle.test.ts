import assert from 'node:assert/strict';
import { test, mock } from 'node:test';
import { Azure, Config, HttpFailure } from '../../../infra/azure/bootstrap.ts';
import type { StageContext } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { Kusto } from './kusto.js';
import { profile } from './profile.js';
import { Scenarios, fingerprint, type FixtureDriver, type Oracle, type Writer } from './scenarios.js';

const config = profile({ format: 1, rowsPerFile: 2, filesPerPartition: 2, partitions: 4, appendBatches: 4,
  appendIntervalMs: 100, observeSeconds: 5, sampleIntervalMs: 1000, qapWaitSeconds: 30,
  vacuumRetentionSeconds: 0 }, true);

function harness() {
  const snapshots = new Map<string, Oracle>();
  const events: { kind: string; value: unknown }[] = [];
  let stops = 0;
  const fixture = (): Oracle => ({ version: 1, files: [
    { path: 'a/1', bytes: 10, partition: 'a' }, { path: 'a/2', bytes: 10, partition: 'a' },
    { path: 'b/1', bytes: 10, partition: 'b' },
  ], records: [[1], [2]], rows: 2, columns: ['id'], fingerprint: fingerprint([[1], [2]]) });
  const get = (table: string): Oracle => {
    const snapshot = snapshots.get(table);
    if (!snapshot) throw new Error('Test fixture is absent.');
    return snapshot;
  };
  const driver: FixtureDriver = {
    async seed(table) { const result = fixture(); snapshots.set(table, result); return result; },
    async seedBroad(table) { const result = fixture(); snapshots.set(table, result); return result; },
    async inspect(table) { return get(table); },
    async append(table) {
      const old = get(table);
      const records = [...old.records, [3]];
      snapshots.set(table, { ...old, version: old.version + 1, records, rows: records.length,
        fingerprint: fingerprint(records), files: [...old.files, { path: 'b/2', bytes: 10, partition: 'b' }] });
    },
    async writer(): Promise<Writer> {
      return { finished: Promise.resolve(), async stop() { stops++; } };
    },
    async maintain(_table, operation) { return { changed: true, deletedFiles: operation === 'vacuum' ? 2 : 0 }; },
  };
  const kusto = new Kusto(new Azure(new Config({
    AZURE_SUBSCRIPTION_ID: '11111111-1111-1111-1111-111111111111',
  })), 'test', (kind, value) => events.push({ kind, value }));
  mock.method(kusto, 'initialize', async () => {});
  mock.method(kusto, 'expose', async () => {});
  mock.method(kusto, 'probeProvenance', async () => {});
  mock.method(kusto, 'sample', async (table: string) => {
    const snapshot = get(table);
    return { at: new Date().toISOString(), version: snapshot.version, completionPercent: 100,
      pendingFiles: 0, healthy: true, acceleratedRows: snapshot.rows };
  });
  mock.method(kusto, 'query', async (table: string) => ({ records: get(table).records, requestId: 'test' }));
  const ctx: StageContext = {
    experimentId: 'test', stage: 'test', signal: new AbortController().signal,
    deadline: Date.now() + 10_000,
    record: (kind, value) => { events.push({ kind, value }); }, flush: async () => {},
  };
  return { driver, kusto, ctx, events, stops: () => stops, scenarios: new Scenarios(driver, kusto, config) };
}

test('exact query parity does not promote missing ingestion diagnostics to PASS', async () => {
  const h = harness();
  const result = await h.scenarios.initialize(h.ctx);
  assert.equal(result.status, 'inconclusive');
  assert.equal(result.rows?.[0]?.correctness, 'PASS');
  assert.equal(result.rows?.[0]?.backgroundCpuMs, null);
});
test('expected immediate-VACUUM reader error remains a finding after convergence', async () => {
  const h = harness();
  await h.scenarios.initialize(h.ctx);
  let calls = 0;
  mock.method(h.kusto, 'query', async () => {
    if (calls++ === 0) throw new Error('A vacuumed file is no longer available.');
    return { requestId: 'test', records: [[1], [2]] };
  });
  const result = await h.scenarios.immediateVacuum(h.ctx);
  assert.equal(result.status, 'finding');
  assert.equal(result.rows?.[0]?.correctness, 'PASS');
  assert.ok(h.events.some(event => event.kind === 'lagging-reader-observations'));
});
test('final incorrect query parity is not downgraded to an expected transient finding', async () => {
  const h = harness();
  await h.scenarios.initialize(h.ctx);
  let calls = 0;
  mock.method(h.kusto, 'query', async () => {
    if (calls++ === 0) throw new Error('Missing old file.');
    return { requestId: 'test', records: [] };
  });
  const result = await h.scenarios.immediateVacuum(h.ctx);
  assert.equal(result.status, 'failed');
  assert.equal(result.rows?.[0]?.correctness, 'FAIL');
});
test('telemetry failure is awaited and surfaced instead of silently abandoning the observer', async () => {
  const h = harness();
  await h.scenarios.initialize(h.ctx);
  mock.method(h.kusto, 'sample', async () => { throw new Error('Diagnostic transport failed.'); });
  await assert.rejects(h.scenarios.append(h.ctx), /Diagnostic transport failed/);
});
test('falsy thrown mutation failures are not mistaken for successful observations', async () => {
  const h = harness();
  await h.scenarios.initialize(h.ctx);
  mock.method(h.driver, 'append', async () => { throw null; });
  await assert.rejects(h.scenarios.append(h.ctx), (reason: unknown) => reason === null);
});
test('finished writer cannot satisfy concurrency coverage and is still joined/stopped', async () => {
  const h = harness();
  await h.scenarios.initialize(h.ctx);
  await assert.rejects(h.scenarios.concurrent(h.ctx), /Writer finished before maintenance/);
  assert.equal(h.stops(), 1);
});
test('native repoint guard is a finding with old-result parity, never an implicit cache reset', async () => {
  for (const wrongRows of [false, true]) {
    const h = harness();
    await h.scenarios.initialize(h.ctx);
    let defines = 0;
    mock.method(h.kusto, 'define', async () => {
      if (++defines === 1) return;
      throw new HttpFailure(400, 'HTTP 400: ' + JSON.stringify({ error: {
        '@message': "Cannot alter the connection string of external table 'source_generation' as it has query_acceleration policy enabled. " +
          'Please remove the query acceleration policy before altering the connection string.',
      } }));
    });
    mock.method(h.kusto, 'enable', async () => {});
    mock.method(h.kusto, 'sample', async (table: string) => {
      const oracle = await h.driver.inspect(table === 'source_generation' ? 'source_old' : table, h.ctx.signal);
      return { at: new Date().toISOString(), version: oracle.version, completionPercent: 100,
        pendingFiles: 0, healthy: true, acceleratedRows: oracle.rows };
    });
    mock.method(h.kusto, 'query', async () => ({ requestId: 'test',
      records: wrongRows ? [] : (await h.driver.inspect('source_old', h.ctx.signal)).records }));
    mock.method(h.kusto, 'management', async () => { throw new Error('An implicit policy reset is forbidden.'); });
    const result = await h.scenarios.repoint(h.ctx);
    assert.equal(result.status, wrongRows ? 'failed' : 'finding');
    assert.equal(result.rows?.[0]?.correctness, wrongRows ? 'FAIL' : 'PASS');
    assert.equal(defines, 2);
    assert.ok(h.events.some(event => event.kind === 'native-repoint-rejected'));
  }
});
