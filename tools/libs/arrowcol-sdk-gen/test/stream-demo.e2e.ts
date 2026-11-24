import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { closeSync, existsSync, openSync, readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { commandRunner, contractRoot, writeJson } from '../src/contract.js';

const id = process.env.CONTRACT_STREAM_RUN;
assert.ok(id, 'Set CONTRACT_STREAM_RUN to a successful new flat events contract; the proof refuses nonempty tables');
const execute = commandRunner(process.cwd());
const cli = ['--no-install', 'tsx', 'tools/libs/arrowcol-sdk-gen/src/stream-demo.ts'];
const call = (action: string, extra: string[] = []) => execute('npx', [...cli, `--action=${action}`, `--run-id=${id}`, ...extra]);
await call('prepare');
const root = resolve(contractRoot, 'demos', id);
const baseline = JSON.parse(await call('inspect'));
assert.deepEqual(baseline.rows, [], 'Use a fresh empty table; this proof never deletes data or replays CREATE');
for (const language of ['rust', 'cpp', 'csharp']) await call('build', [`--language=${language}`]);
assert.ok(!existsSync(join(root, 'serve.lock')), 'Do not run the proof alongside another demo collector');
let collector: ChildProcess | undefined, kustoStarted = false;
const expected: [number, number, string][] = [];
const reports: { language: string; node: boolean; rows: number }[] = [];
let report: object | undefined;
const sorted = (rows: unknown[][]) => rows.sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
try {
  const fd = openSync(join(root, 'e2e-serve.log'), 'a');
  try {
    collector = spawn(process.execPath, ['--import', 'tsx', 'tools/libs/arrowcol-sdk-gen/src/stream-demo.ts',
      '--action=serve', `--run-id=${id}`, '--node'], {
      shell: false, stdio: ['ignore', fd, fd],
    });
  } finally { closeSync(fd); }
  let spawnError: Error | undefined;
  collector.on('error', (error) => { spawnError = error; });
  let ready = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    if (spawnError) throw spawnError;
    assert.ok(collector.exitCode === null && collector.signalCode === null, readFileSync(join(root, 'e2e-serve.log'), 'utf8'));
    if (existsSync(join(root, 'endpoints.json')) && existsSync(join(root, 'serve.lock'))) {
      const endpoints = JSON.parse(readFileSync(join(root, 'endpoints.json'), 'utf8'));
      if (endpoints.pid === collector.pid && String(endpoints.pid) === readFileSync(join(root, 'serve.lock'), 'utf8')) {
        ready = true; break;
      }
    }
    await delay(250);
  }
  assert.ok(ready, 'Demo collectors did not become ready');
  for (const language of ['rust', 'cpp', 'csharp']) {
    for (const node of [false, true]) {
      const output = await call('emit', [`--language=${language}`, '--duration-seconds=2', ...(node ? ['--node'] : [])]);
      const result = output.trim().split('\n').map((line) => line.trim()).reverse().find((line) => line.startsWith('{"language":'));
      assert.ok(result, `Missing emitter summary: ${output}`);
      const summary = JSON.parse(result);
      assert.equal(summary.language, language);
      assert.ok(Number.isInteger(summary.admitted_rows) && summary.admitted_rows > 0);
      reports.push({ language, node, rows: summary.admitted_rows });
      for (let index = 0; index < summary.admitted_rows; index++) expected.push([index, 1_700_000_000_000_000 + index, `${language}-event-${index}`]);
    }
  }
  let committed: { rows: unknown[][]; files: string[] } = { rows: [], files: [] };
  for (let attempt = 0; attempt < 60; attempt++) {
    committed = JSON.parse(await call('inspect'));
    if (committed.rows.length >= expected.length) break;
    await delay(500);
  }
  assert.deepEqual(sorted(committed.rows), sorted(expected), 'Committed Delta must exactly match all six sample sends, including duplicate sample IDs');
  kustoStarted = true;
  await call('kusto-up');
  const query: { TableKind?: string; Rows?: unknown[][] }[] = JSON.parse(await call('kusto-query'));
  const result = query.find((frame) => frame.TableKind === 'PrimaryResult');
  assert.ok(result?.Rows, 'Missing Kusto primary result');
  const kustoRows = result.Rows.map(([eventId, timestamp, payload]) => {
    assert.equal(typeof timestamp, 'string');
    const parts = /^(.+T\d{2}:\d{2}:\d{2})(?:\.(\d{1,7}))?Z$/.exec(String(timestamp));
    assert.ok(parts, `Unexpected Kusto timestamp: ${timestamp}`);
    const fraction = parts[2] ?? '';
    assert.ok(fraction.length <= 6 || fraction[6] === '0', 'Unexpected sub-microsecond data');
    const micros = BigInt(Date.parse(`${parts[1]}Z`)) * 1000n + BigInt(fraction.padEnd(6, '0').slice(0, 6));
    return [eventId, Number(micros), payload];
  });
  assert.deepEqual(sorted(kustoRows), sorted(expected), 'Kusto must expose exactly the committed values and microseconds');
  report = { result: 'passed', run: id, artifactSha256: baseline.artifactSha256,
    sends: reports, exactRows: expected.length, deltaFiles: committed.files,
    topology: 'Rust/C++/C# standard protobuf -> shared SDK -> direct regional OR node -> regional -> Delta -> Kusto native external table' };
} finally {
  const errors: string[] = [];
  if (collector?.pid && collector.exitCode === null && collector.signalCode === null) {
    collector.kill('SIGTERM');
    for (let i = 0; i < 120 && collector.exitCode === null && collector.signalCode === null; i++) await delay(250);
    if (collector.exitCode === null && collector.signalCode === null) {
      errors.push('Demo collector supervisor did not stop gracefully; inspect the retained process and logs');
    }
  }
  if (kustoStarted && existsSync(join(root, 'kusto.json'))) {
    try { await call('kusto-stop'); } catch (error) { errors.push(String(error)); }
  }
  assert.deepEqual(errors, [], errors.join('\n'));
}
assert.ok(report);
writeJson(join(root, 'stream-e2e-result.json'), report);
console.log(JSON.stringify({ ...report, evidence: root }, null, 2));
