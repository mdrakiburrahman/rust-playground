import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test, type TestContext } from 'node:test';
import { pathToFileURL } from 'node:url';
import { LocalStressRuntime, maintenanceArguments } from '../src/stress-runtime.js';
import { parseStressPlan, runStressDriver } from '../src/stress-driver.js';

const target = 'telemetry.sqlserver.sqlserver_cpu_utilization';
const tableParts = target.split('.');
const fixtureLayout = { batchRows: 4, rowGroupRows: 2, pageRows: 1 };
const partition = (offset: number): string => new Date(Math.floor(Date.now() / 60_000) * 60_000 + offset * 60_000)
  .toISOString().slice(0, 16).replace('T', '-').replace(':', '-');

function fixture(t: TestContext, privateInsideResults = false) {
  const owned = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-stress-ready-'));
  t.after(() => rmSync(owned, { recursive: true, force: true }));
  const directory = path.join(owned, 'benchmark');
  const results = path.join(directory, 'results');
  mkdirSync(results, { recursive: true });
  const reader = path.join(owned, 'reader');
  const calls = path.join(owned, 'calls.jsonl');
  const privateScratch = path.join(privateInsideResults ? results : owned, 'private');
  const canary = 'password=PRIVATE_READER_SENTINEL /private/canary-source.parquet';
  writeFileSync(reader, `#!${process.execPath}
import {appendFileSync,readFileSync} from 'node:fs';
appendFileSync(${JSON.stringify(calls)},JSON.stringify(process.argv.slice(2))+'\\n');
const [command,input]=process.argv.slice(2);
const file=command==='summarize' ? input+'/fixture.json' : input+'.layout.json';
const value=JSON.parse(readFileSync(file,'utf8'));
if(value.fail) {
 process.stderr.write(value.flood ? 'x'.repeat(131072) : '', () => {
  console.error('fixture reader failure '+${JSON.stringify(canary)});
  process.exitCode=7;
  if(value.hang) setInterval(() => {}, 1000);
 });
} else console.log(JSON.stringify(value));
`, { mode: 0o755 });
  const startedAt = Date.now() - 10;
  const progress = (remaining = 29_000) => {
    writeFileSync(path.join(results, 'run-metadata.json'), JSON.stringify({ run_id: 'readiness-fixture' }));
    writeFileSync(path.join(results, 'driver-progress.json'), JSON.stringify({
      run_id: 'readiness-fixture', scenario: 'paired', phase: 'measure', timestamp: Date.now(),
      phase_started_at: startedAt, phase_duration_ms: 30_000, phase_remaining_ms: remaining,
      scheduled_rows: 4, send_policy_failures: 0, receipt_failures: 0,
    }));
  };
  progress();
  const install = (pool: string, files = [
    { path: 'a.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-2) } },
    { path: 'b.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-2) } },
    { path: 'c.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-1) } },
  ]) => {
    const root = path.join(directory, pool);
    const table = path.join(root, ...tableParts);
    mkdirSync(path.join(table, '_delta_log'), { recursive: true });
    writeFileSync(path.join(table, '_delta_log', '00000000000000000000.json'), '{}\n');
    writeFileSync(path.join(root, 'fixture.json'), JSON.stringify({
      tables: { [target]: { rows: files.reduce((sum, file) => sum + file.rows, 0), version: 1,
        partition_columns: ['__arrowcol_ingestion'], files } },
    }));
    for (const file of files) writeFileSync(path.join(table, `${file.path}.layout.json`),
      JSON.stringify({ rows: file.rows, row_groups: [{ rows: 2 }, { rows: 2 }] }));
  };
  const runtime = new LocalStressRuntime({
    workspace: process.cwd(), directory, project: 'fixture-ready', composeFiles: [],
    env: process.env, reader, directAdmin: '', dataflowAdmin: '', quiver: false, physicalRoots: {},
    fixture: fixtureLayout, windowSeconds: 9, sampleTimeoutSeconds: 1, measureSeconds: 30,
    privateScratch,
    loadEnded: new AbortController().signal, checkRunning: () => {},
  });
  return { runtime, directory, results, reader, calls, install, progress, startedAt, privateScratch, canary };
}

function assertPublicTreeIsClean(directory: string, canary: string, privateScratch: string): void {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    assert(!entry.name.endsWith('.private.log'));
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) assertPublicTreeIsClean(file, canary, privateScratch);
    else {
      const value = readFileSync(file, 'utf8');
      assert(!value.includes(canary) && !value.includes('PRIVATE_READER_SENTINEL'),
        'the recursive public upload tree must never contain raw stderr');
      assert(!value.includes(privateScratch), 'private diagnostic locations are not exported');
    }
  }
}

test('v0 missing then actual BOTH-pool compactable fixtures publishes a bound readiness receipt', async t => {
  const f = fixture(t);
  let createdAt = 0;
  const timer = setTimeout(() => {
    f.install('direct'); f.install('dataflow'); createdAt = Date.now();
  }, 150);
  t.after(() => clearTimeout(timer));
  let deadline = 0;
  await runStressDriver({
    plan: parseStressPlan({
      version: 1, durationSeconds: 9, waveIntervalSeconds: 3, operationTimeoutSeconds: 1,
      startupTimeoutSeconds: 5, sampleTimeoutSeconds: 1, fixture: fixtureLayout,
      limits: { minimumCycles: 3, minimumSamples: 4, minimumDriverRowsPerSecond: 1,
        maximumBacklog: 1, maximumRssBytes: 100, maximumPhysicalBytes: 100,
        maximumRelativeTrend: 0.1, maximumRelativeVariation: 0.2 },
    }),
    results: f.results, loadEnded: new AbortController().signal, checkRunning: () => {},
    waitForInput: actualDeadline => { deadline = actualDeadline; return f.runtime.waitForInput(actualDeadline); },
    operate: async () => {},
    sample: async wave => ({ timestamp: Date.now(), wave, phase: 'wave-boundary', driverRows: wave * 100,
      rssBytes: 10, physicalBytes: 10, backlog: 0, restarts: 0, oomEvents: 0, exhaustedRetries: 0 }),
  });
  const receipt = JSON.parse(readFileSync(path.join(f.results, 'stress-readiness.json'), 'utf8'));
  assert(receipt.observed_at >= createdAt);
  assert(receipt.observed_at < Date.now() + 1);
  assert.equal(receipt.startup_deadline, deadline);
  const window = JSON.parse(readFileSync(path.join(f.results, 'stress-window.json'), 'utf8'));
  assert(window.criteria.start >= receipt.observed_at);
  assert.equal(window.criteria.end - window.criteria.start, 9000);
  assert(window.criteria.end + 1000 <= receipt.input_deadline);
  assert.equal(window.input_deadline, receipt.input_deadline);
  assert.equal(receipt.generator.phase_started_at, f.startedAt);
  assert.equal(receipt.generator.phase_duration_ms, 30_000);
  assert(receipt.generator.conservatively_remaining_ms >= receipt.required_input_ms);
  assert.deepEqual(receipt.pools.map((pool: { pool: string }) => pool.pool), ['direct', 'dataflow']);
  assert(receipt.pools.every((pool: { selected_bin_files: number; row_groups: number }) =>
    pool.selected_bin_files === 2 && pool.row_groups === 2));
});

test('one ready root cannot start a window while the second root remains unready', async t => {
  const f = fixture(t);
  f.install('direct');
  const timer = setTimeout(() => f.install('dataflow'), 500);
  t.after(() => clearTimeout(timer));
  const start = Date.now();
  await f.runtime.waitForInput(start + 5000);
  assert(Date.now() - start >= 500);
});

test('missing roots expire the original absolute startup deadline without extending it', async t => {
  const f = fixture(t);
  const start = Date.now();
  await assert.rejects(f.runtime.waitForInput(start + 180), /startup deadline/u);
  assert(Date.now() - start < 1000);
  assert(!existsSync(path.join(f.results, 'stress-readiness.json')));
  assert(!existsSync(f.calls), 'missing v0 is not a failed reader invocation to retry');
});

for (const mode of ['newest-only', 'future-open', 'latest-eligible-single-file', 'oversized', 'single-rowgroup'] as const) {
  test(`fixture readiness rejects ${mode} rather than any two closed files`, async t => {
    const f = fixture(t);
    const files = mode === 'newest-only' ? [
      { path: 'a.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-1) } },
      { path: 'b.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-1) } },
    ] : mode === 'future-open' ? [
      { path: 'a.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(1) } },
      { path: 'b.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(1) } },
      { path: 'z.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(2) } },
    ] : [
      { path: 'a.parquet', rows: 4, bytes: mode === 'oversized' ? 64 * 1024 * 1024 : 10,
        partition_values: { __arrowcol_ingestion: partition(-3) } },
      { path: 'b.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-3) } },
      ...(mode === 'latest-eligible-single-file' ? [
        { path: 'c.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-2) } },
      ] : []),
      { path: 'z.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: partition(-1) } },
    ];
    f.install('direct', files); f.install('dataflow', files);
    if (mode === 'single-rowgroup') {
      for (const pool of ['direct', 'dataflow']) {
        const table = path.join(f.directory, pool, ...tableParts);
        for (const file of files) writeFileSync(path.join(table, `${file.path}.layout.json`),
          JSON.stringify({ rows: file.rows, row_groups: [{ rows: 4 }] }));
      }
    }
    await assert.rejects(f.runtime.waitForInput(Date.now() + 700));
    assert(!existsSync(path.join(f.results, 'stress-readiness.json')));
  });
}

test('nonretryable reader failure is preserved privately and is not polled until success', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow');
  writeFileSync(path.join(f.directory, 'direct/fixture.json'), '{"fail":true}');
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /did not complete cleanly/u);
  assert.equal(readFileSync(f.calls, 'utf8').trim().split('\n').length, 1);
  const directories = readdirSync(f.privateScratch);
  assert.equal(directories.length, 1);
  const privateDirectory = path.join(f.privateScratch, directories[0]);
  assert.equal(statSync(privateDirectory).mode & 0o777, 0o700);
  const privateLog = path.join(privateDirectory, 'reader-stderr.private.log');
  assert.equal(statSync(privateLog).mode & 0o777, 0o600);
  assert.match(readFileSync(privateLog, 'utf8'), /PRIVATE_READER_SENTINEL/u);
  assertPublicTreeIsClean(f.results, f.canary, f.privateScratch);
  assertPublicTreeIsClean(f.directory, f.canary, f.privateScratch);
});

test('raw reader stderr retains only a bounded private tail outside public artifacts', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow');
  writeFileSync(path.join(f.directory, 'direct/fixture.json'), '{"fail":true,"flood":true}');
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /did not complete cleanly/u);
  const [name] = readdirSync(f.privateScratch);
  const log = path.join(f.privateScratch, name, 'reader-stderr.private.log');
  assert.equal(statSync(log).size, 65_536);
  assert.match(readFileSync(log, 'utf8'), /PRIVATE_READER_SENTINEL/u);
  assertPublicTreeIsClean(f.directory, f.canary, f.privateScratch);
});

test('large complete reader JSON is retained independently of the private stderr bound', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow');
  const input = path.join(f.directory, 'direct/fixture.json');
  const complete = { ...JSON.parse(readFileSync(input, 'utf8')), large_json: 'O'.repeat(131072) };
  writeFileSync(input, JSON.stringify(complete));
  await f.runtime.waitForInput(Date.now() + 5000);
  assert.deepEqual(JSON.parse(readFileSync(path.join(f.results, 'stress-readiness-direct.json'), 'utf8')), complete);
  const [name] = readdirSync(f.privateScratch);
  assert.deepEqual(readdirSync(path.join(f.privateScratch, name)), ['reader-stderr.private.log']);
  assertPublicTreeIsClean(f.results, f.canary, f.privateScratch);
});

test('a timed-out reader preserves even a short private stderr cause before EOF', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow');
  writeFileSync(path.join(f.directory, 'direct/fixture.json'), '{"fail":true,"hang":true}');
  await assert.rejects(f.runtime.waitForInput(Date.now() + 1000), /did not complete cleanly/u);
  const [name] = readdirSync(f.privateScratch);
  const log = path.join(f.privateScratch, name, 'reader-stderr.private.log');
  assert.match(readFileSync(log, 'utf8'), /PRIVATE_READER_SENTINEL/u);
  assertPublicTreeIsClean(f.directory, f.canary, f.privateScratch);
});

test('private diagnostic storage cannot be configured inside the public artifact tree', t => {
  assert.throws(() => fixture(t, true), /outside the public results tree/u);
});

test('a symlink cannot redirect private reader stderr into public artifacts', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow');
  symlinkSync(f.results, f.privateScratch, 'dir');
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /outside the public results tree/u);
  assert(!existsSync(f.calls), 'reject before invoking any reader or writing raw stderr');
});

test('malformed committed partition metadata fails immediately without retry', async t => {
  const f = fixture(t);
  f.install('direct', [
    { path: 'a.parquet', rows: 4, bytes: 10, partition_values: { __arrowcol_ingestion: '2026-02-31-00-00' } },
  ]);
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /Malformed CPU ingestion partition/u);
  assert.equal(readFileSync(f.calls, 'utf8').trim().split('\n').length, 1);
});

test('actual generator remaining budget must cover the whole window and final sample before start', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow'); f.progress(9000);
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /cannot cover the immutable window/u);
  assert(!existsSync(path.join(f.results, 'stress-readiness.json')));
});

test('a remaining-input counter cannot exceed the declared generator lifetime', async t => {
  const f = fixture(t);
  f.install('direct'); f.install('dataflow'); f.progress(31_000);
  await assert.rejects(f.runtime.waitForInput(Date.now() + 5000), /exceeds its declared phase duration/u);
  assert(!existsSync(path.join(f.results, 'stress-readiness.json')));
});

test('production maintenance CLI accepts shared arguments and emits actual canonical operation names', {
  skip: process.env.LOADTEST_MAINTENANCE_BINARY ? false : 'Supply the built production maintenance binary for this integration test',
}, t => {
  const f = fixture(t);
  const parent = path.join(f.directory, 'real-cli');
  const table = path.join(parent, 'sqlserver_cpu_utilization');
  mkdirSync(path.join(table, '_delta_log'), { recursive: true });
  writeFileSync(path.join(table, '_delta_log', '00000000000000000000.json'), [
    { protocol: { minReaderVersion: 1, minWriterVersion: 2 } },
    { metaData: { id: '11111111-1111-1111-1111-111111111111', format: { provider: 'parquet', options: {} },
      schemaString: JSON.stringify({ type: 'struct', fields: [
        { name: 'customer_id', type: 'string', nullable: false, metadata: {} },
        { name: '__arrowcol_ingestion', type: 'string', nullable: false, metadata: {} },
      ] }), partitionColumns: ['__arrowcol_ingestion'], configuration: {}, createdTime: Date.now() } },
  ].map(value => JSON.stringify(value)).join('\n') + '\n');
  const binary = process.env.LOADTEST_MAINTENANCE_BINARY;
  assert(binary);
  const output = execFileSync(binary, [
    ...maintenanceArguments(pathToFileURL(`${parent}${path.sep}`).href, 5), '--no-metrics-listener',
  ], { encoding: 'utf8', timeout: 15_000, maxBuffer: 1024 * 1024 });
  const reports = output.split('\n').filter(line => line.startsWith('{')).map(line => JSON.parse(line));
  assert.deepEqual(reports.map(report => report.operation), ['optimize', 'vacuum', 'checkpoint']);
  assert(reports.every(report => report.applied === true && report.report && !report.error_kind));
});
