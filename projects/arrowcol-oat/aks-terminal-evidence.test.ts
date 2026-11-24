import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { collectorStartupObserved, extractTerminalSummary, parseTerminalSummary, terminalSummaryClear, terminalTableDigest } from './aks-terminal-evidence.js';

const summary = () => ({ schema_version: 1, admitted: 1, committed: 1, failed: 0, cancelled_ambiguous: 0, pending: 0,
  committed_rows: 10, incomplete: 0, commits: [{ table_id: 'a'.repeat(64), version: 0, rows: 10, files: 1, parquet_bytes: 100 }] });
const line = (value: unknown) => `2026-11-16T00:00:00Z arrowcol_terminal_evidence ${JSON.stringify(value)}\n`;

test('terminal schema accepts only exact checked numeric counters and hashed table commit tuples', () => {
  assert.equal(terminalSummaryClear(parseTerminalSummary(summary())), true);
  for (const value of [
    { ...summary(), pending: -1 }, { ...summary(), admitted: Number.MAX_SAFE_INTEGER + 1 },
    { ...summary(), token: 'secret' }, { ...summary(), incomplete: true },
    { ...summary(), commits: [{ ...summary().commits[0], table_id: 'raw-table-url' }] },
    { ...summary(), commits: [{ ...summary().commits[0], parquet_bytes: '100' }] },
  ]) assert.throws(() => parseTerminalSummary(value), /unavailable/);
});

test('terminal accounting refuses ambiguity, unresolved work, incomplete attribution and nonconservation', () => {
  for (const value of [
    { ...summary(), incomplete: 1 }, { ...summary(), cancelled_ambiguous: 1, admitted: 2 },
    { ...summary(), failed: 1, admitted: 2 }, { ...summary(), pending: 1, admitted: 2 },
    { ...summary(), admitted: 2 }, { ...summary(), committed_rows: 11 },
    { ...summary(), committed_rows: 20, commits: [summary().commits[0], summary().commits[0]] },
  ]) assert.equal(terminalSummaryClear(parseTerminalSummary(value)), false);
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
