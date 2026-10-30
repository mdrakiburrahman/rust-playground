import assert from 'node:assert/strict';
import { test } from 'node:test';
import { profile } from './profile.js';

const rapid = {
  format: 1, rowsPerFile: 256, filesPerPartition: 4, partitions: 4, appendBatches: 16,
  appendIntervalMs: 1000, observeSeconds: 10, sampleIntervalMs: 1000, qapWaitSeconds: 600,
  vacuumRetentionSeconds: 0,
};
test('rapid profile really requests zero retention and requires explicit reader-risk acknowledgement', () => {
  assert.throws(() => profile(rapid, false), /old-snapshot readers/);
  assert.equal(profile(rapid, true).vacuumRetentionSeconds, 0);
  assert.equal(profile({ ...rapid, vacuumRetentionSeconds: 604800 }, false).vacuumRetentionSeconds, 604800);
});
test('bounded experiment settings reject typos, negative retention and excessive data', () => {
  assert.throws(() => profile({ ...rapid, vaccumRetentionSeconds: 0 }, true), /Unknown/);
  assert.throws(() => profile({ ...rapid, vacuumRetentionSeconds: -1 }, true), /vacuumRetentionSeconds/);
  assert.throws(() => profile({ ...rapid, sampleIntervalMs: 0 }, true), /sampleIntervalMs/);
  assert.throws(() => profile({ ...rapid, rowsPerFile: 1024, filesPerPartition: 16, appendBatches: 64 }, true), /budget/);
  assert.throws(() => profile({ ...rapid, rowsPerFile: Infinity }, true), /rowsPerFile/);
});
