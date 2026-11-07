import { test } from 'node:test';
import assert from 'node:assert/strict';
import { complete, observeHydration, parseCounts, type Observation } from './hydration.ts';

const full = [['old', 'before', 10, 10, 0], ['old', 'after', 10, 10, 0], ['new', 'after', 10, 0, 10]];

test('old before/after and new filled rows must each match exactly, not merely expose a column', () => {
  assert.equal(complete(parseCounts(full), 10), true);
  for (const rows of [full.slice(1), [...full, ['new', 'before', 10, 0, 10]],
    [full[0]!, ['old', 'after', 10, 9, 1], full[2]!],
    [full[0]!, full[1]!, ['new', 'after', 20, 0, 20]]])
    assert.equal(complete(parseCounts(rows), 10), false);
});

test('malformed and duplicate aggregates are fatal rather than polled as incomplete', () => {
  for (const rows of [[full[0]!, full[0]!], [['old', 'after', 10, 0, 1]],
    [['old', 'after', '10', 10, 0]], [['new', 'after', 1.5, 0, 1.5]], [['unknown', 'after', 10, 10, 0]]])
    assert.throws(() => parseCounts(rows), /Malformed/);
});

test('bounded polling records schema-unavailable, partial data and exact observed elapsed time', async () => {
  let at = 1000, attempt = 0;
  const recorded: Observation[] = [];
  const counts = await observeHydration({ query: async () => {
    attempt++;
    if (attempt === 1) throw new Error("Failed to resolve scalar expression named 'bar'");
    return attempt === 2 ? full.slice(0, 2) : full;
  } }, 'foo', {
    deadline: 2000, rowsPerPhase: 10, intervalMs: 100, signal: new AbortController().signal,
    now: () => at, wait: async ms => { at += ms; }, record: row => recorded.push(row),
  });
  assert.equal(complete(counts, 10), true);
  assert.deepEqual(recorded.map(row => row.elapsedMs), [0, 100, 200]);
  assert.match(recorded[0]!.error!, /bar/);
});

test('a successful response arriving at or after deadline cannot claim hydration success', async () => {
  let at = 0;
  await assert.rejects(() => observeHydration({ query: async () => { at = 200; return full; } }, 'foo', {
    deadline: 200, rowsPerPhase: 10, signal: new AbortController().signal,
    now: () => at, record: () => {},
  }), /deadline exceeded/);
});

test('timeout includes the last counts and sleep never exceeds remaining deadline', async () => {
  let at = 0;
  const waits: number[] = [];
  await assert.rejects(() => observeHydration({ query: async () => full.slice(0, 1) }, 'foo', {
    deadline: 250, rowsPerPhase: 10, intervalMs: 100, signal: new AbortController().signal,
    now: () => at, wait: async ms => { waits.push(ms); at += ms; }, record: () => {},
  }), /last observation:.*"client":"old"/);
  assert.deepEqual(waits, [100, 100, 50]);
});

test('authorization, partial-result and malformed-response failures are never swallowed', async () => {
  for (const error of [new Error('HTTP 403'), new Error('Kusto returned a partial result')])
    await assert.rejects(() => observeHydration({ query: async () => { throw error; } }, 'foo', {
      deadline: Date.now() + 1000, rowsPerPhase: 10, signal: new AbortController().signal, record: () => {},
    }), error);
});
