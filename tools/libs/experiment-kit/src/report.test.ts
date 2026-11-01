import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readdir, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { runExperiment, type Experiment } from './experiment.js';
import {
  JsonSafetyError,
  escapeCell,
  isSecretKey,
  redactString,
  renderTable,
  stringifySafeJson,
  toSafeJsonValue,
  writeFileAtomic,
  type Column,
} from './report.js';

interface Throughput {
  readonly region: string;
  readonly rowsPerSecond: number | null | undefined;
  readonly healthy: boolean;
}

const THROUGHPUT: readonly Column<Throughput>[] = [
  { header: 'Region', value: (row) => row.region },
  { header: 'Rate', unit: 'rows/s', value: (row) => row.rowsPerSecond, align: 'right', format: (v) => v.toFixed(1) },
  { header: 'Healthy', value: (row) => row.healthy, align: 'center' },
];

async function scratch(t: test.TestContext): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), 'experiment-kit-report-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

test('renderTable uses caller columns, explicit units, alignment and formatting', () => {
  const table = renderTable(THROUGHPUT, [
    { region: 'east', rowsPerSecond: 12.345, healthy: true },
    { region: 'west', rowsPerSecond: 0, healthy: false },
  ]);
  assert.equal(
    table,
    [
      '| Region | Rate (rows/s) | Healthy |',
      '| :--- | ---: | :---: |',
      '| east | 12.3 | true |',
      '| west | 0.0 | false |',
      '',
    ].join('\n'),
  );
});

test('missing values render as a marker, never zero', () => {
  const rows: Throughput[] = [{ region: 'north', rowsPerSecond: undefined, healthy: true }];
  assert.match(renderTable(THROUGHPUT, rows), /\| north \| n\/a \| true \|/);
  assert.match(renderTable(THROUGHPUT, rows, { missing: 'missing' }), /\| north \| missing \|/);
  assert.doesNotMatch(renderTable(THROUGHPUT, rows), /\| 0/);
});

test('renderTable escapes pipes, backslashes and newlines in cells and headers', () => {
  assert.equal(escapeCell('a|b\\c\r\nd\ne'), 'a\\|b\\\\c<br>d<br>e');
  const table = renderTable<{ readonly v: string }>([{ header: 'A|B', value: (row) => row.v }], [{ v: 'x|y\nz' }]);
  assert.equal(table.split('\n').length, 4);
  assert.match(table, /\| A\\\|B \|/);
  assert.match(table, /\| x\\\|y<br>z \|/);
});

test('renderTable rejects non-finite numbers and empty column sets', () => {
  const cols: Column<{ readonly n: number }>[] = [{ header: 'N', value: (row) => row.n }];
  assert.throws(() => renderTable(cols, [{ n: Number.NaN }]), /row 0, column "N": non-finite number NaN/);
  assert.throws(() => renderTable(cols, [{ n: Number.NEGATIVE_INFINITY }]), RangeError);
  assert.throws(() => renderTable([], []), RangeError);
});

test('toSafeJsonValue rejects values JSON would silently change', () => {
  const cyclic: Record<string, unknown> = {};
  cyclic['self'] = cyclic;
  const cases: Array<readonly [unknown, RegExp]> = [
    [Number.NaN, /non-finite number NaN/],
    [{ a: [1, Number.POSITIVE_INFINITY] }, /\$\.a\[1\]: non-finite number Infinity/],
    [10n, /unsupported bigint/],
    [{ a: undefined }, /\$\.a: undefined value/],
    [() => 1, /unsupported function/],
    [Symbol('s'), /unsupported symbol/],
    [new Date(0), /unsupported object Date/],
    [new Map(), /unsupported object Map/],
    [cyclic, /circular reference/],
    // eslint-disable-next-line no-sparse-arrays
    [[1, , 3], /sparse array hole/],
  ];
  for (const [value, message] of cases) {
    assert.throws(() => toSafeJsonValue(value), (error: unknown) => error instanceof JsonSafetyError && message.test(error.message));
  }
});

test('toSafeJsonValue accepts plain data, shared references and normalizes -0', () => {
  const shared = { n: 1 };
  assert.deepEqual(toSafeJsonValue({ a: shared, b: shared, z: -0, s: null, list: ['x', true] }), {
    a: { n: 1 },
    b: { n: 1 },
    z: 0,
    s: null,
    list: ['x', true],
  });
  assert.equal(Object.is(toSafeJsonValue(-0), 0), true);
  const proto = toSafeJsonValue(JSON.parse('{"__proto__": {"polluted": true}}')) as Record<string, unknown>;
  assert.equal(Object.getPrototypeOf(proto), Object.prototype);
  assert.deepEqual(Object.keys(proto), ['__proto__']);
  assert.equal(stringifySafeJson({ a: 1 }), '{\n  "a": 1\n}\n');
});

test('redaction masks secret keys, credential patterns and caller secrets', () => {
  assert.equal(isSecretKey('accessToken'), true);
  assert.equal(isSecretKey('client_secret'), true);
  assert.equal(isSecretKey('ConnectionString'), true);
  assert.equal(isSecretKey('tokenCount'), false);
  assert.equal(isSecretKey('latencyMs'), false);
  assert.equal(
    redactString('Authorization: Bearer abc.def AccountKey=xyz;Endpoint=e ?sv=1&sig=QQ%3D&se=2 val-1234', ['val-1234']),
    'Authorization: Bearer [REDACTED] AccountKey=[REDACTED];Endpoint=e ?sv=1&sig=[REDACTED]&se=2 [REDACTED]',
  );
  assert.deepEqual(toSafeJsonValue({ apiKey: 'k', nested: { password: Number.NaN } }), {
    apiKey: '[REDACTED]',
    nested: { password: '[REDACTED]' },
  });
});

test('writeFileAtomic replaces whole files and leaves no temp files', async (t) => {
  const dir = await scratch(t);
  const file = path.join(dir, 'report.md');
  await writeFileAtomic(file, 'old\n');
  await writeFileAtomic(file, 'new\n');
  assert.equal(await readFile(file, 'utf8'), 'new\n');

  const big = (c: string): string => c.repeat(1_000_000);
  await Promise.all([writeFileAtomic(file, big('a')), writeFileAtomic(file, big('b'))]);
  const final = await readFile(file, 'utf8');
  assert.ok(final === big('a') || final === big('b'));
  assert.deepEqual(await readdir(dir), ['report.md']);
});

test('writeFileAtomic failure keeps the previous target and cleans up', async (t) => {
  const dir = await scratch(t);
  const target = path.join(dir, 'occupied');
  await mkdir(path.join(target, 'child'), { recursive: true });
  await assert.rejects(writeFileAtomic(target, 'data'));
  assert.deepEqual(await readdir(dir), ['occupied']);
  assert.deepEqual(await readdir(target), ['child']);
  await assert.rejects(writeFileAtomic(path.join(dir, 'missing-dir', 'f'), 'x'), /ENOENT/);
});

test('a second synthetic experiment reuses the runner with different columns', async (t) => {
  const dir = await scratch(t);
  const throughput: Experiment<Throughput> = {
    id: 'synthetic-throughput',
    title: 'Throughput | regions',
    columns: THROUGHPUT,
    stages: [
      {
        name: 'sample',
        run: async (ctx) => {
          ctx.record('regions', { count: 2 });
          return {
            status: 'finding',
            summary: 'west slower\nsee evidence',
            rows: [
              { region: 'east', rowsPerSecond: 100, healthy: true },
              { region: 'west|2', rowsPerSecond: null, healthy: false },
            ],
          };
        },
      },
    ],
  };
  const printed: string[] = [];
  const outcome = await runExperiment(throughput, { outDir: path.join(dir, 'out'), timeoutMs: 5_000, output: (md) => printed.push(md) });
  assert.equal(outcome.report.status, 'finding');
  assert.equal(printed[0], await readFile(outcome.files.markdown, 'utf8'));
  assert.match(outcome.markdown, /^## Throughput \\\| regions\n/);
  assert.match(outcome.markdown, /\| Region \| Rate \(rows\/s\) \| Healthy \|/);
  assert.match(outcome.markdown, /\| west\\\|2 \| n\/a \| false \|/);
  assert.match(outcome.markdown, /west slower<br>see evidence/);
  assert.deepEqual(outcome.report.rows[1], { region: 'west|2', rowsPerSecond: null, healthy: false });
});