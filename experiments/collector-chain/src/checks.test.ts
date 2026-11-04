import assert from 'node:assert/strict';
import test from 'node:test';
import { batching, clientCoverage, mapHostPath, parseOptions, pendingFlush } from './checks.js';
import type { TableLog } from './delta.js';

const expect = { clients: 2, dmvTables: 1, rows: 5, signals: 3 };
const log = (rows: number, commits: TableLog['commits'] = []): TableLog => ({ commits, activeFiles: commits.length, rows });

test('parseOptions applies defaults and rejects out-of-range values', () => {
  assert.deepEqual(parseOptions([]),
    { clients: 3, rows: 100, signals: 10, keep: false, stream: false, interval: 10, kustoPort: 8080, help: false });
  assert.deepEqual(parseOptions(['--stream', '--interval', '5']),
    { clients: 3, rows: 100, signals: 10, keep: true, stream: true, interval: 5, kustoPort: 8080, help: false });
  assert.throws(() => parseOptions(['--clients', '0']), /--clients/);
  assert.throws(() => parseOptions(['--interval', '5']), /needs --stream/);
  assert.throws(() => parseOptions(['--bogus']));
});

test('mapHostPath picks the most specific bind mount', () => {
  const mounts = [
    { Type: 'bind', Source: '/host/repo', Destination: '/workspaces/repo' },
    { Type: 'bind', Source: '/host/scratch', Destination: '/workspaces/repo/.temp' },
  ];
  assert.equal(mapHostPath('/workspaces/repo/.temp/run/delta', mounts), '/host/scratch/run/delta');
  assert.throws(() => mapHostPath('/elsewhere', mounts), /No host bind mount/);
});

test('pendingFlush waits for every DMV table and OTAP signal', () => {
  const otap = new Map([['logs', log(6)], ['spans', log(6)], ['univariate_metrics', log(2)], ['resource_attrs', log(4)]]);
  assert.deepEqual(pendingFlush(new Map([['t', log(10)]]), otap, expect), []);
  assert.deepEqual(pendingFlush(new Map([['t', log(5)]]), new Map([...otap, ['spans', log(3)]]), expect),
    ['t has 5/10 rows', 'otap/spans has 3/6 rows']);
});

test('clientCoverage flags missing rows and unknown clients', () => {
  const dmv = new Map([['dmv_t', [{ client: 'a', rows: 5 }, { client: 'b', rows: 4 }]]]);
  const otap = ['logs', 'spans'].flatMap(signal => ['a', 'b'].map(client => ({ signal, client, rows: 3 })))
    .concat([{ signal: 'metrics', client: 'a', rows: 1 }, { signal: 'metrics', client: 'z', rows: 1 }]);
  assert.deepEqual(clientCoverage(['a', 'b'], dmv, otap, expect).map(row => [row.subject, row.ok]),
    [['a', true], ['b', false], ['unknown clients', false]]);
});

test('batching passes only when a table needs fewer commits than clients', () => {
  const commit = { version: 1, files: 2, rows: 10, transactions: 2 };
  const rows = batching('ArrowCol', new Map([['one', log(10, [commit])], ['two', log(10, [commit, { ...commit, version: 2 }])]]), 2);
  assert.deepEqual(rows.map(row => row.ok), [true, false]);
  assert.match(rows[0]!.observed, /up to 2 publications per commit/);
});
