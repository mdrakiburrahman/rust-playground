import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { collectorStartupObserved, extractTerminalSummary, parseTerminalSummary, terminalSummaryClear, terminalTableDigest } from './aks-terminal-evidence.js';

const legacySummary = () => ({ schema_version: 1, admitted: 1, committed: 1, failed: 0, cancelled_ambiguous: 0, pending: 0,
  committed_rows: 10, incomplete: 0, commits: [{ table_id: 'a'.repeat(64), version: 0, rows: 10, files: 1, parquet_bytes: 100 }] });
const summary = () => ({ ...legacySummary(), schema_version: 2 as const,
  publication_evidence: [{ clean: true, reasons: [], outcomes: [{ app_id: 'offline-client', app_version: 42,
    status: 'committed_now', delta_version: 0, reason: null }] }] });
const line = (value: unknown) => `2026-11-16T00:00:00Z arrowcol_terminal_evidence ${JSON.stringify(value)}\n`;
const rustFixture = readFileSync(new URL('../../crates/arrowcol-dataflow-components/tests/fixtures/terminal-evidence-v2.json',
  import.meta.url), 'utf8').trimEnd();

test('terminal schema accepts exact counters, hashed commit tuples, and one-to-one publication versions', () => {
  assert.equal(terminalSummaryClear(parseTerminalSummary(summary())), true);
  for (const value of [
    { ...summary(), pending: -1 }, { ...summary(), admitted: Number.MAX_SAFE_INTEGER + 1 },
    { ...summary(), token: 'secret' }, { ...summary(), incomplete: true },
    { ...summary(), commits: [{ ...summary().commits[0], table_id: 'raw-table-url' }] },
    { ...summary(), commits: [{ ...summary().commits[0], parquet_bytes: '100' }] },
  ]) assert.throws(() => parseTerminalSummary(value), /unavailable/);
  assert.throws(() => parseTerminalSummary({ ...summary(), publication_evidence: undefined }), /unavailable/);
  assert.throws(() => parseTerminalSummary({ ...summary(), publication_evidence: [
    ...summary().publication_evidence, ...summary().publication_evidence,
  ] }), /Duplicate terminal publication identity/);
  assert.throws(() => parseTerminalSummary({ ...summary(), publication_evidence: [{
    ...summary().publication_evidence[0], outcomes: [{ ...summary().publication_evidence[0]!.outcomes[0], delta_version: 42 }],
  }] }), /versions differ/);
  assert.throws(() => parseTerminalSummary({ ...summary(), publication_evidence: [{
    ...summary().publication_evidence[0], token: 'unexpected',
  }] }), /malformed/);
});

test('persisted v1 lifecycle summaries remain readable but cannot validate finalization', () => {
  const parsed = parseTerminalSummary(legacySummary());
  assert.deepEqual(parsed, legacySummary());
  assert.equal(terminalSummaryClear(parsed), false);
  assert.equal(extractTerminalSummary(line(legacySummary())).status, 0);
});

test('Rust-emitted v2 contract fixture parses and proves exact publication coverage', () => {
  const result = extractTerminalSummary(`2026-01-01T00:00:00Z arrowcol_terminal_evidence ${rustFixture}\n`);
  assert.equal(result.status, 0);
  assert.ok(result.summary);
  assert.equal(terminalSummaryClear(result.summary), true);
});

test('terminal accounting refuses ambiguity, unresolved work, incomplete attribution and nonconservation', () => {
  for (const value of [
    { ...summary(), incomplete: 1 }, { ...summary(), cancelled_ambiguous: 1, admitted: 2 },
    { ...summary(), failed: 1, admitted: 2 }, { ...summary(), pending: 1, admitted: 2 },
    { ...summary(), admitted: 2 }, { ...summary(), committed_rows: 11 },
  ]) assert.equal(terminalSummaryClear(parseTerminalSummary(value)), false);
  assert.throws(() => parseTerminalSummary({ ...summary(), committed_rows: 20,
    commits: [summary().commits[0], summary().commits[0]] }), /versions differ/);
});

test('terminal extraction refuses absent, malformed or duplicate records rather than selecting the last', () => {
  assert.deepEqual(extractTerminalSummary(line(summary())), { summary: summary(), status: 0 });
  assert.deepEqual(extractTerminalSummary('ordinary log\n'), { summary: null, status: 1 });
  assert.deepEqual(extractTerminalSummary(line(summary()) + line(summary())), { summary: null, status: 2 });
  assert.deepEqual(extractTerminalSummary('2026-11-16T00:00:00Z arrowcol_terminal_evidence {"schema_version":1}\n'),
    { summary: null, status: 3 });
  assert.equal(extractTerminalSummary(line(summary()).replace('Z arrowcol_', 'Z untrusted arrowcol_')).status, 3);
});

test('terminal table hash preserves the configured ABFSS scheme and writer trailing slash', () => {
  const url = 'abfss://data@account.dfs.core.windows.net/planned/table/';
  assert.equal(terminalTableDigest('account', 'data', 'planned/table'), createHash('sha256').update(url).digest('hex'));
  assert.notEqual(terminalTableDigest('account', 'data', 'planned/table'),
    createHash('sha256').update(url.replace('abfss:', 'az:')).digest('hex'));
  assert.throws(() => terminalTableDigest('account', 'data', 'planned/../table'), /unavailable/);
});

test('rotated tails and partial responses cannot establish the startup-to-terminal envelope', () => {
  const startup = '2026-11-16T00:00:00Z arrowcol_effective_config {"component":"receiver"}\n';
  assert.equal(collectorStartupObserved(startup + line(summary())), true);
  assert.equal(collectorStartupObserved(line(summary())), false);
  assert.equal(collectorStartupObserved(line(summary()) + startup), false);
  assert.equal(collectorStartupObserved((startup + line(summary())).trimEnd()), false);
});
