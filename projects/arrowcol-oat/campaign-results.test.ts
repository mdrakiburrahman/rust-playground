import assert from 'node:assert/strict';
import test from 'node:test';
import { renderCampaignResults } from './campaign-results.js';

const rows = [
  { cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: 'default', repeat: 1, committedRowsPerSecond: 100 },
  { cohort: 'C1', candidate: 'snappy', factorX: 'codec', valueX: 'snappy', factorY: 'level', valueY: 'default', repeat: 2, committedRowsPerSecond: 120 },
  { cohort: 'C1', candidate: 'zstd', factorX: 'codec', valueX: 'zstd', factorY: 'level', valueY: '3', repeat: 1, committedRowsPerSecond: 90 },
];

test('renders two-factor committed-rate heatmaps and repeat-range leaderboards', () => {
  const svg = renderCampaignResults(rows, 'C1');
  assert.match(svg, /<svg[^>]+role="img"/);
  assert.match(svg, /codec × level heatmap/);
  assert.match(svg, /Leaderboard \| mean and repeat range/);
  assert.match(svg, /110 \(100–120\)/);
  assert.match(svg, /committed new rows\/s/);
});

test('renders Kustainer death as a null rate with its required comment', () => {
  const svg = renderCampaignResults([...rows, {
    cohort: 'C1', candidate: 'kustainer-cap', factorX: 'codec', valueX: 'zstd', factorY: 'level', valueY: '6',
    repeat: 1, committedRowsPerSecond: null, diedFirst: 'Kustainer', comment: 'emulator exited before the window completed',
  }], 'C1');
  assert.match(svg, /Kustainer \(committed rows\/s: null\).*emulator exited/);
  assert.throws(() => renderCampaignResults([...rows, {
    cohort: 'C1', candidate: 'bad', factorX: 'codec', valueX: 'zstd', factorY: 'level', valueY: '6',
    repeat: 1, committedRowsPerSecond: 50, diedFirst: 'Kustainer', comment: 'unexpected rate',
  }], 'C1'), /dead Kustainer must have a null committed rate/);
});
