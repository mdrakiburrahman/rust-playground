import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { MIB } from './profile.js';
import { Evidence, exitCode, positiveControl, resultsTable, writeReport, type RunReport } from './report.js';
import type { TrialResult } from './scenario.js';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const none = { files: 0, bytes: 0, requests: 0, paths: [] };

function trial(overrides: Partial<TrialResult> & { reread?: number; unchanged?: number } = {}): TrialResult {
  const { reread, unchanged = 6, ...rest } = overrides;
  return { trial: 'reload', operation: 'reload', repetition: 1, table: 'qap_r_reload', prefix: 'runs/r/reload/', status: 'complete',
    telemetry: 'complete', startedAt: '2025-01-01T10:00:00.000Z', durationMs: 240_000, regressions: 0, versionBefore: 0, versionAfter: 0,
    files: { before: 6, added: 0, removed: 0, unchanged, unchangedBytes: unchanged * MIB },
    reads: reread === undefined ? undefined : { warmUp: { ...none, files: 6 }, unchanged: { ...none, files: reread, bytes: reread * MIB },
      afterMutation: { ...none, files: reread } },
    ...rest };
}

test('the reload positive control passes only when every unchanged file was seen re-read', () => {
  assert.equal(positiveControl([trial({ operation: 'optimize', trial: 'optimize', reread: 0 })]).status, 'not-run');
  assert.deepEqual(positiveControl([trial({ reread: 6 })]),
    { status: 'detected', detail: 'reload re-read 6/6 files: StorageBlobLogs capture QAP reads.' });
  assert.equal(positiveControl([trial({ reread: 6, status: 'late-logs-pending', telemetry: 'pending' })]).status, 'detected',
    'all re-reads already observed count even while later logs are still arriving');

  const cases: [Partial<TrialResult> & { reread?: number; unchanged?: number }, RegExp][] = [
    [{ reread: 2, status: 'late-logs-pending', telemetry: 'pending' }, /reload logs still pending \(2\/6 files re-read\)/],
    [{ status: 'late-logs-pending', telemetry: 'skipped' }, /reload telemetry skipped \(no reads counted\)/],
    [{ status: 'budget-exceeded', telemetry: 'not-reached', files: undefined }, /reload ran out of budget \(no reads counted\)/],
    [{ status: 'failed', telemetry: 'not-reached', error: 'boom' }, /reload failed/],
    [{ reread: 5 }, /reload logs complete but only 5\/6 files re-read/],
    [{ reread: 0, unchanged: 0 }, /only 0\/0/],
  ];
  for (const [overrides, detail] of cases) {
    const control = positiveControl([trial(overrides)]);
    assert.equal(control.status, 'not-detected', JSON.stringify(overrides));
    assert.match(control.detail, detail);
  }
  const mixed = positiveControl([trial({ reread: 6 }), trial({ trial: 'reload_2', repetition: 2, reread: 1, telemetry: 'pending',
    status: 'late-logs-pending' })]);
  assert.equal(mixed.status, 'not-detected');
  assert.match(mixed.detail, /^reload_2 logs still pending/);
});

test('exit code: nonzero for errors, failed trials or cleanup and an undetected reload; 130 when interrupted', () => {
  const base = { trials: [trial({ operation: 'optimize', trial: 'optimize', status: 'late-logs-pending', telemetry: 'pending' })],
    control: { status: 'not-run' as const, detail: '' }, cleanup: { status: 'done' as const, errors: [] }, interrupted: false };
  assert.equal(exitCode(base), 0, 'pending logs alone are an honest report, not a failure');
  assert.equal(exitCode({ ...base, control: { status: 'detected', detail: '' } }), 0);
  assert.equal(exitCode({ ...base, control: { status: 'not-detected', detail: '' } }), 1);
  assert.equal(exitCode({ ...base, error: 'boom' }), 1);
  assert.equal(exitCode({ ...base, trials: [trial({ status: 'failed' })] }), 1);
  assert.equal(exitCode({ ...base, cleanup: { status: 'failed', errors: ['x'] } }), 1);
  assert.equal(exitCode({ ...base, control: { status: 'not-detected', detail: '' }, interrupted: true }), 130);
});

test('the results table shows unchanged re-reads per trial', () => {
  const table = resultsTable([trial({ reread: 6 }), trial({ trial: 'optimize', operation: 'optimize', reread: 1, unchanged: 4,
    status: 'late-logs-pending', telemetry: 'pending' })]);
  assert.match(table, /Unchanged re-read/);
  assert.match(table, /6\/6/);
  assert.match(table, /1\/4/);
  assert.match(table, /late-logs-pending/);
  assert.match(table, /4m/);
});

test('writeReport and Evidence write redacted JSON, Markdown and query files', async () => {
  fs.mkdirSync(path.join(REPO, '.temp'), { recursive: true });
  const out = fs.mkdtempSync(path.join(REPO, '.temp', 'qap-report-test-'));
  try {
    const secret = 'super-secret-subscription';
    const evidence = new Evidence(path.join(out, 'evidence.jsonl'), [secret]);
    evidence.record('reload', 'note', { text: `id ${secret}`, missing: undefined });
    const line = JSON.parse(fs.readFileSync(path.join(out, 'evidence.jsonl'), 'utf8').trim());
    assert.deepEqual([line.trial, line.kind, line.data], ['reload', 'note', { text: 'id [REDACTED]' }]);

    const reload = trial({ reread: 6, readsQuery: 'StorageBlobLogs | take 1', rawQuery: 'StorageBlobLogs | take 2' });
    const report: RunReport = { runId: 'r', startedAt: reload.startedAt, finishedAt: reload.startedAt, durationMs: 300_000,
      profile: { tail: '2m' }, location: { account: 'acct', container: 'experimentqap', database: 'experimentqap', workspace: 'ws',
        workspaceCustomerId: '00000000-0000-0000-0000-000000000002' },
      trials: [reload], control: positiveControl([reload]), cleanup: { status: 'done', errors: [] }, interrupted: false,
      error: `failed for ${secret}`, exitCode: 1 };
    await writeReport(report, out, [secret]);
    const json = fs.readFileSync(path.join(out, 'report.json'), 'utf8');
    const markdown = fs.readFileSync(path.join(out, 'report.md'), 'utf8');
    assert.ok(!json.includes(secret) && !markdown.includes(secret));
    assert.equal(JSON.parse(json).control.status, 'detected');
    assert.match(markdown, /\*\*Positive control:\*\* detected/);
    assert.match(markdown, /## Cleanup: done/);
    assert.match(markdown, /az monitor log-analytics query -w 00000000-0000-0000-0000-000000000002/);
    assert.equal(fs.readFileSync(path.join(out, 'queries', 'reload.kql'), 'utf8'), 'StorageBlobLogs | take 1\n');
    assert.equal(fs.readFileSync(path.join(out, 'queries', 'reload.raw.kql'), 'utf8'), 'StorageBlobLogs | take 2\n');
  } finally {
    fs.rmSync(out, { recursive: true, force: true });
  }
});
