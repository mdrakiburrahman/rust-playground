import test from 'node:test';
import assert from 'node:assert/strict';
import { MIB, type Profile } from './profile.js';
import type { Fixtures, SeedResult, Snapshot, SourceFile } from './fixtures.js';
import type { AccelerationSample } from './kusto.js';
import { BudgetExceeded, Narrator, planTrials, runTrial, watch, type Clock, type Trial, type TrialEnvironment } from './scenario.js';

const START = Date.UTC(2025, 0, 1, 10, 0);
const RUN = '20250101t100000_ab12';
const PRINCIPAL = '00000000-0000-0000-0000-000000000001';
const PROFILE: Profile = { operations: ['optimize', 'reload'], files: 2, fileMib: 1, repetitions: 1, parallel: 2, tailMs: 60_000,
  logWaitMs: 300_000, budgetMs: 900_000, settleMs: 20_000, pollMs: 10_000, logPollMs: 30_000 };

function fakeClock(start = START): Clock & { advance(ms: number): void } {
  let now = start;
  return { now: () => now, sleep: async (ms: number) => { now += ms; }, advance: (ms: number) => { now += ms; } };
}

const file = (partition: string, name: string): SourceFile => ({ path: `ingestion=${partition}/${name}.parquet`, bytes: MIB, partition });
const SEEDED = [file('e', 'a'), file('e', 'b'), file('c', 'a'), file('c', 'b'), file('k', 'a'), file('l', 'a')];
const SEED: SeedResult = { version: 0, rows: 6, fingerprint: 'f0', files: SEEDED, expectedGroups: [], tombstones: 0, totalBytes: 6 * MIB };
const OPTIMIZED: Snapshot = { version: 1, rows: 6, fingerprint: 'f1', expectedGroups: [],
  files: [file('e', 'a'), file('e', 'b'), file('k', 'a'), file('l', 'a'), file('c', 'z')] };

test('planTrials runs one round per repetition with lowercase names, tables and prefixes', () => {
  assert.deepEqual(planTrials('r1', { operations: ['optimize', 'reload'], repetitions: 2 }).map(trial =>
    [trial.name, trial.table, trial.prefix, trial.repetition]), [
    ['optimize', 'qap_r1_optimize', 'runs/r1/optimize/', 1],
    ['reload', 'qap_r1_reload', 'runs/r1/reload/', 1],
    ['optimize_2', 'qap_r1_optimize_2', 'runs/r1/optimize_2/', 2],
    ['reload_2', 'qap_r1_reload_2', 'runs/r1/reload_2/', 2],
  ]);
});

test('Narrator stamps elapsed time and prints each command kind in full only once', () => {
  const clock = fakeClock();
  const lines: string[] = [];
  const narrator = new Narrator(clock, START, text => lines.push(text));
  clock.advance(65_000);
  narrator.say('optimize', 'hello');
  narrator.code('optimize', 'QAP state', 'line 1\nline 2', 'qap_optimize');
  narrator.code('purge', 'QAP state', 'line 1\nline 2', 'qap_purge');
  narrator.code('purge', 'QAP state', 'line 3', 'qap_purge', true);
  assert.deepEqual(lines, [
    '[01:05] [optimize] hello',
    '[01:05] [optimize] QAP state:\n    line 1\n    line 2',
    '[01:05] [purge]   QAP state: same as printed above for qap_optimize, applied to qap_purge',
    '[01:05] [purge] QAP state:\n    line 3',
  ]);
});

const caughtUp = (version: number, rows: number): AccelerationSample =>
  ({ at: 'now', version, completionPercent: 100, pendingFiles: 0, acceleratedRows: rows, healthy: true });
const behind: AccelerationSample = { at: 'now', version: 0, completionPercent: 40, pendingFiles: 3, acceleratedRows: 2, healthy: true };

test('watch restarts the hold when QAP regresses after looking caught up', async () => {
  const clock = fakeClock();
  const samples = [behind, caughtUp(0, 6), behind, caughtUp(0, 6), caughtUp(0, 6), caughtUp(0, 6)];
  const said: string[] = [];
  const result = await watch({ kusto: { sample: async () => samples.shift()! }, table: 't', target: { version: 0, rows: 6 },
    holdMs: 20_000, pollMs: 10_000, deadline: START + 600_000, clock, signal: new AbortController().signal, say: text => said.push(text) });
  assert.deepEqual({ ...result, last: undefined }, { firstCaughtUpAt: START + 10_000, caughtUpAt: START + 30_000, regressions: 1,
    samples: 6, last: undefined });
  assert.ok(said.some(text => text.startsWith('REGRESSION')));

  await assert.rejects(watch({ kusto: { sample: async () => behind }, table: 't', target: { version: 0, rows: 6 }, holdMs: 0,
    pollMs: 10_000, deadline: START + 25_000, clock: fakeClock(), signal: new AbortController().signal, say: () => undefined }),
  (error: unknown) => error instanceof BudgetExceeded && /Budget ran out while waiting for QAP \(Delta v0\/0, 40%/.test(error.message));
});

const BLOB = (trial: string) => `https://acct.blob.core.windows.net/experimentqap/runs/${RUN}/${trial}/`;
const COLUMNS = ['Phase', 'Path', 'OperationName', 'StatusCode', 'Requests', 'Bytes', 'Last'].map(name => ({ name }));
const logRow = (trial: string, phase: string, path: string, last = '2025-01-01T10:01:00Z') =>
  [phase, BLOB(trial) + path, 'GetBlob', 200, 1, MIB, last];

interface Harness { env: TrialEnvironment; lines: string[]; kinds: string[]; calls: string[]; tables: string[] }

function harness(options: { logs?: unknown[][] | null; seed?: () => Promise<SeedResult>; sample?: () => AccelerationSample;
  deadline?: number } = {}): Harness {
  const clock = fakeClock();
  const lines: string[] = [];
  const kinds: string[] = [];
  const calls: string[] = [];
  const tables: string[] = [];
  let state = { version: SEED.version, rows: SEED.rows };
  const fixtures = {
    seedArgs: () => ['seed-scale'],
    maintainCommand: () => 'target/release/delta-maintenance --apply',
    seed: options.seed ?? (async () => SEED),
    inspect: async (trial: string) => trial.startsWith('optimize') ? OPTIMIZED : SEED,
    maintain: async (trial: string, operation: string) => {
      calls.push(`maintain ${trial} ${operation}`);
      state = { version: OPTIMIZED.version, rows: OPTIMIZED.rows };
      const report = { filesDeleted: 0, filesRemoved: 2, interrupted: false, outcome: 'committed' };
      return { plan: report, applied: report };
    },
  } as unknown as Pick<Fixtures, 'seed' | 'inspect' | 'maintain' | 'seedArgs' | 'maintainCommand'>;
  const kusto = {
    create: async (table: string) => { calls.push(`create ${table}`); },
    drop: async (table: string) => { calls.push(`drop ${table}`); },
    sample: async () => options.sample?.() ?? caughtUp(state.version, state.rows),
    extents: async () => [{ partition: 'k', extent: '11111111-1111-1111-1111-111111111111', rows: 1 }],
    parity: async () => { calls.push('parity'); },
    marker: async (table: string) => { calls.push(`marker ${table}`); },
  } as unknown as TrialEnvironment['kusto'];
  const logs = options.logs === null ? undefined : {
    query: async () => ({ tables: [{ columns: COLUMNS, rows: options.logs ?? [] }] }),
    scope: { workspace: PRINCIPAL, account: 'acct', container: 'experimentqap', requester: PRINCIPAL },
  };
  const env: TrialEnvironment = { runId: RUN, profile: PROFILE, fixtures, kusto, logs, principal: PRINCIPAL, clock,
    source: (trial: Trial) => `abfss://experimentqap@acct.dfs.core.windows.net/runs/${RUN}/${trial.name};managed_identity=${PRINCIPAL}`,
    narrator: new Narrator(clock, START, text => lines.push(text)), deadline: options.deadline ?? START + PROFILE.budgetMs,
    budget: new AbortController().signal, interrupt: new AbortController().signal,
    record: (_trial, kind) => { kinds.push(kind); }, created: table => { tables.push(table); } };
  return { env, lines, kinds, calls, tables };
}

const [OPTIMIZE, RELOAD] = planTrials(RUN, PROFILE) as [Trial, Trial];
const LATE = '2100-01-01T00:00:00Z';

test('an OPTIMIZE trial runs every phase and counts re-reads of unchanged files only', async () => {
  const h = harness({ logs: [
    ...SEEDED.map(seeded => logRow('optimize', 'warm-up', seeded.path)),
    logRow('optimize', 'after-mutation', 'ingestion=k/a.parquet'),
    logRow('optimize', 'after-mutation', 'ingestion=c/z.parquet'),
    logRow('optimize', 'late', '_delta_log/00000000000000000001.json', LATE),
  ] });
  const result = await runTrial(h.env, OPTIMIZE);
  assert.equal(result.status, 'complete', result.error ?? '');
  assert.equal(result.telemetry, 'complete');
  assert.deepEqual(result.files, { before: 6, added: 1, removed: 2, unchanged: 4, unchangedBytes: 4 * MIB });
  assert.deepEqual([result.versionBefore, result.versionAfter, result.regressions, result.catchUpMs, result.recoveryMs], [0, 1, 0, 0, 0]);
  assert.deepEqual(result.reads?.unchanged, { files: 1, bytes: MIB, requests: 1, paths: ['ingestion=k/a.parquet'] });
  assert.equal(result.reads?.afterMutation.files, 2);
  assert.equal(result.reads?.warmUp.files, 6);
  assert.equal(result.logPolls, 2);
  assert.deepEqual(result.extents, { untouchedRows: 1, keptRows: 1, unacceleratedRows: 0 });
  assert.match(result.readsQuery!, new RegExp(`/experimentqap/runs/${RUN}/optimize/`));
  assert.deepEqual(h.tables, [OPTIMIZE.table]);
  assert.deepEqual(h.calls, [`create ${OPTIMIZE.table}`, 'parity', 'maintain optimize optimize', 'parity']);
  assert.deepEqual(h.kinds.filter(kind => kind !== 'qap'), ['seed', 'extents-before', 'maintenance', 'inspect', 'extents-after',
    'storage-reads', 'result']);
  const text = h.lines.join('\n');
  for (const phase of ['SEED:', 'ACCELERATE:', 'MUTATE: OPTIMIZE', 'TAIL:', 'LOG WAIT:', 'StorageBlobLogs\n', '.show external table',
    'Logs complete: QAP read 6 Parquet file(s) while warming up, then re-read 1/4 UNCHANGED file(s)'])
    assert.ok(text.includes(phase), phase);
});

test('RELOAD drops and recreates the table without maintenance and expects every file re-read', async () => {
  const h = harness({ logs: [
    ...SEEDED.map(seeded => logRow('reload', 'after-mutation', seeded.path)),
    logRow('reload', 'late', '_delta_log/00000000000000000000.json', LATE),
  ] });
  const result = await runTrial(h.env, RELOAD);
  assert.equal(result.status, 'complete', result.error ?? '');
  assert.deepEqual(h.calls, [`create ${RELOAD.table}`, 'parity', `drop ${RELOAD.table}`, `create ${RELOAD.table}`, 'parity']);
  assert.equal(result.files?.unchanged, 6);
  assert.equal(result.reads?.unchanged.files, 6);
});

test('failures, missing logs and budget overruns become honest trial statuses', async () => {
  const seedFailure = await runTrial(harness({ seed: async () => { throw new Error('seed boom'); } }).env, OPTIMIZE);
  assert.deepEqual([seedFailure.status, seedFailure.telemetry, seedFailure.error], ['failed', 'not-reached', 'seed boom']);

  const noLogs = harness({ logs: null });
  const skipped = await runTrial(noLogs.env, OPTIMIZE);
  assert.deepEqual([skipped.status, skipped.telemetry, skipped.reads], ['late-logs-pending', 'skipped', undefined]);

  const pending = await runTrial(harness({ logs: [logRow('reload', 'warm-up', SEEDED[0]!.path)] }).env, RELOAD);
  assert.deepEqual([pending.status, pending.telemetry, pending.reads?.unchanged.files], ['late-logs-pending', 'pending', 0]);
  assert.ok(pending.logPolls! > 1);

  const stuck = harness({ sample: () => behind, deadline: START + 60_000 });
  const overrun = await runTrial(stuck.env, OPTIMIZE);
  assert.equal(overrun.status, 'budget-exceeded');
  assert.equal(overrun.telemetry, 'not-reached');
  assert.match(overrun.error!, /Budget ran out while waiting for QAP/);
  assert.deepEqual(stuck.tables, [OPTIMIZE.table], 'the table is registered for cleanup before it is created');
});
