import test from 'node:test';
import assert from 'node:assert/strict';
import { HttpFailure } from '../../../infra/azure/bootstrap.ts';
import type { ProcessOptions, ProcessResult } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { anchorMinute, checkMutation, childEnvironment, deletePrefix, Fixtures, maintenanceArgs, maintenanceReport,
  parseSnapshot, sourceDelta, trialName, type Snapshot, type SourceFile, type StorageClient } from './fixtures.js';

const ROOT = 'abfss://experimentqap@acct.dfs.core.windows.net/runs/r1/';
const SHA = 'a'.repeat(64);
const GENERATION = '01234567-89ab-cdef-0123-456789abcdef';

const file = (path: string, bytes = 100): SourceFile => ({ path, bytes, partition: path.split('/')[0]! });
const BEFORE = [file('p=expired/a.parquet'), file('p=expired/b.parquet'), file('p=compact/c.parquet'), file('p=compact/d.parquet'),
  file('p=control/e.parquet'), file('p=live/f.parquet')];

function snapshot(version: number, files: SourceFile[], rows = files.length): Snapshot {
  return { version, rows, fingerprint: `v${version}`, files, expectedGroups: [] };
}

function wire(action: string, trial: string, version: number, files: SourceFile[], extra: Record<string, unknown> = {}) {
  return { ok: true, action, table: trial, version, rows: files.length, fingerprint: 'fp', files,
    expectedGroups: files.map((item, id) => ({ partition: item.partition, id, value: id, payloadSha256: SHA, multiplicity: 1 })),
    ...extra };
}

function processResult(stdout: unknown, exitCode = 0, stderr = ''): ProcessResult {
  return { exitCode, signal: null, aborted: false, stdout: typeof stdout === 'string' ? stdout : JSON.stringify(stdout) + '\n',
    stderr, stdoutTruncated: false, stderrTruncated: false, strayProcessesKilled: false, outputClosed: true, durationMs: 1 };
}

function fakeRunner(results: ProcessResult[]) {
  const calls: { command: string; args: readonly string[]; options: ProcessOptions }[] = [];
  const run = async (command: string, args: readonly string[], options: ProcessOptions) => {
    calls.push({ command, args, options });
    const next = results.shift();
    if (!next) throw new Error('unexpected process call');
    return next;
  };
  return { run, calls };
}

const fixtures = (run: ReturnType<typeof fakeRunner>['run']) =>
  new Fixtures({ repo: '/repo', root: ROOT, env: {}, redact: text => text.replaceAll('secret', '[private]'), run });

test('trial names and anchors', () => {
  assert.equal(trialName('optimize', 1), 'optimize');
  assert.equal(trialName('reload', 3), 'reload_3');
  assert.throws(() => trialName('purge', 0), /start at 1/);
  assert.equal(anchorMinute(Date.UTC(2025, 0, 1, 10, 5, 42, 9)), Date.UTC(2025, 0, 1, 10, 5));
});

test('parseSnapshot validates the fixture JSON contract', () => {
  const parsed = parseSnapshot(wire('seed-scale', 'optimize', 0, BEFORE), 'optimize', 'seed-scale');
  assert.equal(parsed.rows, 6);
  assert.equal(parsed.files.length, 6);
  assert.throws(() => parseSnapshot(wire('seed-scale', 'purge', 0, BEFORE), 'optimize', 'seed-scale'), /mismatched/);
  assert.throws(() => parseSnapshot(wire('inspect-scale', 'optimize', 0, BEFORE), 'optimize', 'seed-scale'), /mismatched/);
  assert.throws(() => parseSnapshot(wire('seed-scale', 'optimize', 0, [file('../escape.parquet')]), 'optimize', 'seed-scale'),
    /invalid relative file path/);
  assert.throws(() => parseSnapshot(wire('seed-scale', 'optimize', 0, [BEFORE[0]!, BEFORE[0]!]), 'optimize', 'seed-scale'),
    /duplicate files|duplicated/);
  assert.throws(() => parseSnapshot({ ...wire('seed-scale', 'optimize', 0, BEFORE), rows: 7 }, 'optimize', 'seed-scale'),
    /do not account for every row/);
});

test('sourceDelta splits added, removed and unchanged files and rejects in-place rewrites', () => {
  const delta = sourceDelta(BEFORE, [...BEFORE.slice(0, 2), ...BEFORE.slice(4), file('p=compact/g.parquet', 200)]);
  assert.deepEqual(delta.added.map(item => item.path), ['p=compact/g.parquet']);
  assert.deepEqual(delta.removed.map(item => item.path), ['p=compact/c.parquet', 'p=compact/d.parquet']);
  assert.equal(delta.unchanged.length, 4);
  assert.throws(() => sourceDelta(BEFORE, [file('p=expired/a.parquet', 999)]), /rewritten in place/);
});

test('checkMutation enforces what each operation claims to do', () => {
  const before = snapshot(0, BEFORE);
  const optimized = snapshot(1, [...BEFORE.slice(0, 2), ...BEFORE.slice(4), file('p=compact/g.parquet', 200)], 6);
  assert.equal(checkMutation('optimize', before, optimized, 0).unchanged.length, 4);
  assert.throws(() => checkMutation('optimize', before, before, 0), /OPTIMIZE did not rewrite/);
  assert.throws(() => checkMutation('optimize', before, { ...optimized, rows: 5 }, 0), /changed the row count/);

  const purged = snapshot(1, BEFORE.slice(2));
  assert.equal(checkMutation('purge', before, purged, 0).removed.length, 2);
  assert.throws(() => checkMutation('purge', before, optimized, 0), /PURGE did not drop/);

  assert.equal(checkMutation('vacuum', before, before, 2).unchanged.length, 6);
  assert.throws(() => checkMutation('vacuum', before, before, 0), /VACUUM deleted no tombstoned file/);
  assert.throws(() => checkMutation('vacuum', before, purged, 2), /the Delta snapshot changed/);
  assert.equal(checkMutation('reload', before, before, 0).unchanged.length, 6);
  assert.throws(() => checkMutation('reload', before, snapshot(1, BEFORE), 0), /the Delta snapshot changed/);
  assert.throws(() => checkMutation('purge', before, snapshot(1, [], 0), 0), /nothing to observe/);
});

test('maintenanceArgs runs the real delta-maintenance contracts', () => {
  const optimize = maintenanceArgs(ROOT, 'optimize', 'optimize', 60_000, false, 999_000);
  assert.deepEqual(optimize.slice(0, 10), ['--root', ROOT, '--table', 'optimize', '--operation', 'optimize', '--granularity', 'minute',
    '--anchor-ms', '60000']);
  assert.ok(optimize.includes('--latest-partitions'));
  assert.ok(!optimize.includes('--apply'));
  const purge = maintenanceArgs(ROOT, 'purge', 'purge', 60_000, true, 999_000);
  assert.deepEqual(purge.slice(-3), ['--purge-retention-secs', '86400', '--apply']);
  const vacuum = maintenanceArgs(ROOT, 'vacuum_2', 'vacuum', 60_000, true, 999_000);
  assert.equal(vacuum[vacuum.indexOf('--anchor-ms') + 1], '999000');
  assert.deepEqual(vacuum.slice(-4), ['--vacuum-retention-secs', '0', '--allow-short-vacuum-retention', '--apply']);
  assert.throws(() => maintenanceArgs(ROOT, 'Bad-Name', 'purge', 60_000, false, 1), /Invalid Kusto table/);
  assert.throws(() => maintenanceArgs(ROOT, 'purge', 'purge', 0, false, 1), /requires the fixture anchor/);
});

test('maintenanceReport reads exactly one JSON report line', () => {
  const report = { report: { outcome: 'committed', interrupted: false, files_deleted: 2, files_removed: 3 } };
  assert.deepEqual(maintenanceReport(`starting\n{"event":"plan"}\n${JSON.stringify(report)}\n`),
    { filesDeleted: 2, filesRemoved: 3, outcome: 'committed', interrupted: false });
  assert.throws(() => maintenanceReport('no json'), /exactly one JSON report/);
  assert.throws(() => maintenanceReport(`${JSON.stringify(report)}\n${JSON.stringify(report)}`), /exactly one JSON report/);
  assert.throws(() => maintenanceReport('{"report":{"interrupted":false}}'), /no outcome/);
});

test('childEnvironment scopes the binaries to the run root', () => {
  const env = childEnvironment({ ARROWCOL_RESET_GENERATION: GENERATION, AZURE_SUBSCRIPTION_ID: 'sub', ARROWCOL_LAYOUT_SORT_COLUMNS: 'id',
    ARROWCOL_LAYOUT_BLOOM_COLUMNS: 'id', PATH: '/bin' }, ROOT, '/repo/.temp/qap-stage');
  assert.equal(env.ARROWCOL_DELTA_ROOT, ROOT);
  assert.equal(env.ARROWCOL_PARTITION_GRANULARITY, 'minute');
  assert.equal(env.TMPDIR, '/repo/.temp/qap-stage');
  assert.equal(env.PATH, '/bin');
  assert.equal(env.ARROWCOL_LAYOUT_SORT_COLUMNS, undefined);
  assert.equal(env.ARROWCOL_LAYOUT_BLOOM_COLUMNS, undefined);
  assert.throws(() => childEnvironment({ AZURE_SUBSCRIPTION_ID: 'sub' }, ROOT, '/s'), /azure-infra:provision/);
  assert.throws(() => childEnvironment({ ARROWCOL_RESET_GENERATION: GENERATION }, ROOT, '/s'), /AZURE_SUBSCRIPTION_ID/);
});

test('seed passes --tombstones, retries transport errors and checks the publisher closed', async () => {
  const seeded = wire('seed-scale', 'vacuum', 1, BEFORE.slice(2), { publisherClosed: true, tombstones: 2, totalBytes: 600 });
  const runner = fakeRunner([processResult({ ok: false, error: 'error sending request for url' }, 1), processResult(seeded)]);
  const result = await fixtures(runner.run).seed('vacuum', 60_000, 1, 2, true, new AbortController().signal, 60);
  assert.equal(result.tombstones, 2);
  assert.equal(result.totalBytes, 600);
  assert.equal(result.version, 1);
  assert.equal(runner.calls.length, 2);
  assert.equal(runner.calls[0]!.command, '/repo/target/release/arrowcol-experiment');
  assert.deepEqual(runner.calls[0]!.args, ['--root', ROOT, '--table', 'vacuum', '--allow-table', 'vacuum', '--timeout-seconds', '60',
    'seed-scale', '--anchor-ms', '60000', '--seed-mib', '1', '--files-per-partition', '2', '--tombstones']);

  const plain = fakeRunner([processResult({ ...seeded, tombstones: null })]);
  await assert.rejects(fixtures(plain.run).seed('vacuum', 60_000, 1, 2, true, new AbortController().signal), /no pre-seeded tombstones/);
  const open = fakeRunner([processResult({ ...seeded, publisherClosed: false })]);
  await assert.rejects(fixtures(open.run).seed('vacuum', 60_000, 1, 2, true, new AbortController().signal), /publisher/);
  const fatal = fakeRunner([processResult({ ok: false, error: 'secret access denied' }, 1)]);
  await assert.rejects(fixtures(fatal.run).seed('vacuum', 60_000, 1, 2, true, new AbortController().signal),
    error => error instanceof Error && error.message.includes('[private] access denied') && !error.message.includes('secret'));
  assert.equal(fatal.calls.length, 1);
});

test('maintain runs a dry run, then applies', async () => {
  const report = (deleted: number) => processResult(`${JSON.stringify({ report: { outcome: deleted ? 'committed' : 'planned',
    interrupted: false, files_deleted: deleted, files_removed: 0 } })}\n`);
  const runner = fakeRunner([report(0), report(2)]);
  const result = await fixtures(runner.run).maintain('vacuum', 'vacuum', 60_000, new AbortController().signal, () => 120_000);
  assert.equal(result.plan.filesDeleted, 0);
  assert.equal(result.applied.filesDeleted, 2);
  assert.equal(runner.calls[0]!.command, '/repo/target/release/delta-maintenance');
  assert.ok(!runner.calls[0]!.args.includes('--apply'));
  assert.ok(runner.calls[1]!.args.includes('--apply'));
  assert.match(fixtures(runner.run).maintainCommand('vacuum', 'vacuum', 60_000, 120_000),
    /^target\/release\/delta-maintenance --root .* --operation vacuum .*--apply$/);

  const failing = fakeRunner([processResult('', 2, 'secret boom')]);
  await assert.rejects(fixtures(failing.run).maintain('purge', 'purge', 60_000, new AbortController().signal),
    /purge dry run failed: \[private\] boom/);
});

function storage(responses: (Record<string, string> | Error)[], listing: string) {
  const urls: string[] = [];
  const azure = {
    c: { account: 'acct' },
    request: async (method: string, url: string) => {
      assert.equal(method, 'DELETE');
      urls.push(url);
      const next = responses.shift();
      if (!next) throw new Error('unexpected delete');
      if (next instanceof Error) throw next;
      return [null, next] as [Buffer | null, Record<string, string>];
    },
    blob: async () => [Buffer.from(listing), {}] as [Buffer | null, Record<string, string>],
  };
  return { azure: azure as unknown as StorageClient, urls };
}

const EMPTY = '<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="c"><Prefix>runs/r1/</Prefix><Blobs /></EnumerationResults>';
const LEFT = '<?xml version="1.0"?><EnumerationResults><Blobs><Blob><Name>runs/r1/x</Name></Blob></Blobs></EnumerationResults>';

test('deletePrefix follows continuations, retries transient errors and verifies nothing is left', async () => {
  const sleeps: number[] = [];
  const { azure, urls } = storage([new HttpFailure(503, 'busy'), { 'x-ms-continuation': 'tok' }, {}], EMPTY);
  await deletePrefix(azure, 'experimentqap', 'runs/r1/', async ms => { sleeps.push(ms); });
  assert.equal(urls.length, 3);
  assert.equal(urls[0], 'https://acct.dfs.core.windows.net/experimentqap/runs/r1?recursive=true');
  assert.match(urls[2]!, /recursive=true&continuation=tok$/);
  assert.deepEqual(sleeps, [2000]);

  await assert.rejects(deletePrefix(storage([{}], LEFT).azure, 'experimentqap', 'runs/r1/', async () => {}), /still has blobs/);
  await assert.rejects(deletePrefix(storage([new HttpFailure(403, 'denied')], EMPTY).azure, 'experimentqap', 'runs/r1/',
    async () => {}), /denied/);
  for (const prefix of ['runs/', 'runs/r1/x/', '../runs/r1/', 'data/r1/', 'runs/R1/'])
    await assert.rejects(deletePrefix(storage([], EMPTY).azure, 'experimentqap', prefix), /Refusing to delete/);
});
