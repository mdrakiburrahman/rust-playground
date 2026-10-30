import assert from 'node:assert/strict';
import { test } from 'node:test';
import { canonicalRecords, fingerprint, pendingWorkWarning, requireMutation, type Oracle } from './scenarios.js';

function oracle(paths: string[], records = [[1], [2]], version = 1): Oracle {
  return { version, files: paths.map(path => ({ path, bytes: 10, partition: path[0]! })),
    rows: records.length, columns: ['id'], records, fingerprint: fingerprint(records) };
}

test('exact canonical multiset comparison ignores order but preserves duplicates and values', () => {
  assert.equal(canonicalRecords([[2], [1]]), canonicalRecords([[1], [2]]));
  assert.notEqual(fingerprint([[1], [1], [2]]), fingerprint([[1], [2]]));
  assert.notEqual(fingerprint([[1, 'a']]), fingerprint([[1, 'b']]));
  assert.notEqual(fingerprint([[1]]), fingerprint([['1']]));
});
test('no-op append, OPTIMIZE and PURGE do not satisfy mutation coverage', () => {
  const before = oracle(['a/1', 'a/2', 'b/1']);
  for (const operation of ['append', 'partial-optimize', 'broad-optimize', 'purge'] as const)
    assert.throws(() => requireMutation(operation, before, before), /actual Delta commit/);
});
test('partial OPTIMIZE needs untouched files and an exact unchanged row multiset', () => {
  const before = oracle(['a/1', 'a/2', 'b/1']);
  requireMutation('partial-optimize', before, oracle(['a/3', 'b/1'], [[2], [1]], 2));
  assert.throws(() => requireMutation('partial-optimize', before, oracle(['a/3'], [[1], [2]], 2)), /untouched/);
  assert.throws(() => requireMutation('partial-optimize', before, oracle(['a/3', 'b/1'], [[1]], 2)), /multiset/);
});
test('broad OPTIMIZE coverage requires more than one actually rewritten closed partition', () => {
  requireMutation('broad-optimize', oracle(['a/1', 'a/2', 'b/1', 'b/2', 'c/1']),
    oracle(['a/3', 'b/3', 'c/1'], [[1], [2]], 2));
  assert.throws(() => requireMutation('broad-optimize', oracle(['a/1', 'a/2', 'b/1']),
    oracle(['a/3', 'b/1'], [[1], [2]], 2)), /more than one/);
});
test('VACUUM requires actual physical deletes and must not alter active rows or version', () => {
  const before = oracle(['a/1']);
  assert.throws(() => requireMutation('vacuum', before, before, { changed: false, deletedFiles: 0 }), /physical deletion/);
  requireMutation('vacuum', before, before, { changed: true, deletedFiles: 3 });
  assert.throws(() => requireMutation('vacuum', before, oracle(['a/1'], [[1]], 2),
    { changed: true, deletedFiles: 3 }), /active snapshot/);
});
test('blind append adds rows/files without replacement, and PURGE removes existing old rows', () => {
  const before = oracle(['a/1', 'b/1']);
  requireMutation('append', before, oracle(['a/1', 'b/1', 'b/2'], [[1], [2], [3]], 2));
  requireMutation('purge', before, oracle(['b/1'], [[2]], 2));
  assert.throws(() => requireMutation('append', before, oracle(['b/1', 'b/2'], [[1], [2], [3]], 2)), /without removing/);
});
test('Azure partial-OPTIMIZE seven-pending/one-replacement observation is a warning, not origin proof', () => {
  const warning = pendingWorkWarning(1, 7, 12, [
    { at: 'baseline', version: 11, pendingFiles: 0 },
    { at: 'mutation', version: 12, pendingFiles: 7, completionPercent: 12.5, acceleratedRows: 256 },
    { at: 'ready', version: 12, pendingFiles: 0, completionPercent: 100, acceleratedRows: 2816 },
  ]);
  assert.match(warning!, /7 pending.*1 new\/replacement.*7 unchanged/);
  assert.match(warning!, /not per-file proof/);
  const priorVersion = pendingWorkWarning(0, 4, 13, [{ at: 'purge', version: 12, pendingFiles: 7 }]);
  assert.match(priorVersion!, /versions: 12; target Delta version: 13/);
  assert.match(priorVersion!, /prior-version or background work/);
  assert.equal(pendingWorkWarning(3, 1, 12, [{ at: 'broad', version: 12, pendingFiles: 3 }]), undefined);
  assert.equal(pendingWorkWarning(1, 0, 12, [{ at: 'new', version: 12, pendingFiles: 2 }]), undefined);
  assert.equal(pendingWorkWarning(0, 4, 12, [{ at: 'metadata', version: 12, pendingFiles: 1 }])?.includes('1 pending'), true);
});
