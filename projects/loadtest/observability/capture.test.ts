import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { parse } from 'yaml';

import { dashboardUrl, parseCaptureArgs } from './capture.js';

const dashboardPath = fileURLToPath(new URL('./throughput-dashboard.json', import.meta.url));
const composePath = fileURLToPath(new URL('../compose.yaml', import.meta.url));
const prometheusPath = fileURLToPath(new URL('./prometheus.yaml', import.meta.url));

test('capture plan uses one explicit time window and fixed run variants', () => {
  const plan = parseCaptureArgs([
    '--baseline-run=run-001', '--baseline-from=2026-11-15T10:00:00Z', '--baseline-to=2026-11-15T10:05:00Z',
    '--bottleneck-run=run-002', '--bottleneck-from=2026-11-15T12:00:00Z', '--bottleneck-to=2026-11-15T12:05:00Z',
    '--best-run=run-003', '--best-from=2026-11-15T14:00:00Z', '--best-to=2026-11-15T14:05:00Z',
  ]);
  assert.deepEqual(plan.captures.map(({ name }) => name), [
    'baseline', 'diagnosed-bottleneck', 'best-sustained-candidate',
  ]);
  assert.equal(plan.component, '.*');
  const urls = plan.captures.map(capture => dashboardUrl('http://127.0.0.1:3000', plan, capture));
  assert.equal(new Set(urls.map(url =>
    Number(url.searchParams.get('to')) - Number(url.searchParams.get('from')))).size, 1);
  assert.deepEqual(urls.map(url => url.searchParams.get('from')), [
    String(Date.parse('2026-11-15T10:00:00Z')),
    String(Date.parse('2026-11-15T12:00:00Z')),
    String(Date.parse('2026-11-15T14:00:00Z')),
  ]);
  assert.deepEqual(urls.map(url => url.searchParams.get('var-run')), ['run-001', 'run-002', 'run-003']);
  assert.deepEqual(urls.map(url => url.searchParams.get('var-variant')), [
    'baseline', 'diagnosed-bottleneck', 'best-sustained-candidate',
  ]);
});

test('capture plan rejects timezone ambiguity, reversed windows, and unbounded labels', () => {
  const fixed = [
    '--baseline-run=base', '--baseline-from=2026-11-15T10:00:00Z', '--baseline-to=2026-11-15T10:05:00Z',
    '--bottleneck-run=diag', '--bottleneck-from=2026-11-15T12:00:00Z', '--bottleneck-to=2026-11-15T12:05:00Z',
    '--best-run=best', '--best-from=2026-11-15T14:00:00Z', '--best-to=2026-11-15T14:05:00Z',
  ];
  assert.throws(() => parseCaptureArgs([
    ...fixed.filter(option => !option.startsWith('--baseline-from=')),
    '--baseline-from=2026-11-15T10:00:00',
  ]), /explicit timezone/u);
  assert.throws(() => parseCaptureArgs([
    ...fixed.filter(option => !option.startsWith('--baseline-to=')),
    '--baseline-to=2026-11-15T09:59:00Z',
  ]), /must precede/u);
  assert.throws(() => parseCaptureArgs([
    ...fixed.filter(option => !option.startsWith('--best-to=')),
    '--best-to=2026-11-15T14:06:00Z',
  ]), /same measurement-window duration/u);
  assert.throws(() => parseCaptureArgs([
    ...fixed.filter(option => !option.startsWith('--baseline-run=')),
    '--baseline-run=.*',
  ]), /fixed low-cardinality/u);
});

test('throughput dashboard keeps the required panel order and bounded metric labels', async () => {
  const dashboard = JSON.parse(await readFile(dashboardPath, 'utf8')) as {
    panels: Array<{ title: string; targets?: Array<{ expr?: string; legendFormat?: string }> }>;
  };
  const titles = dashboard.panels.map(panel => panel.title);
  assert.deepEqual(titles, [
    'Campaign verdict (OAT receipt)', 'Committed new rows / s',
    'Row-to-commit latency (p50 / p95 / p99)', 'Queue age (oldest pending, seconds)',
    'Stage: collect (p95)', 'Stage: build (p95)', 'Stage: sort (p95)',
    'Stage: encode (p95)', 'Stage: upload (p95)', 'Stage: commit (p95)',
    'CPU saturation by service', 'Working set by service', 'Physical Parquet file-size distribution',
    'Maintenance candidates and skips', 'Maintenance log I/O', 'Checkpoint debt and progress',
    'QAP accelerated (%)', 'QAP pending files', 'QAP pending size (bytes)',
    'LastUpdatedDateTime age (refresh, not row latency)', 'QAP unhealthy catalogs (count)',
    'Native NotHealthyReason', 'Scrape health (all targets)', 'OTel export failures',
  ]);
  const queryText = dashboard.panels.flatMap(panel => (panel.targets ?? []).map(target =>
    `${target.expr ?? ''} ${target.legendFormat ?? ''}`)).join('\n');
  assert.match(queryText, /run=~"\$run"/u);
  assert.match(queryText, /variant=~"\$variant"/u);
  assert.match(queryText, /component=~"\$component"/u);
  assert.doesNotMatch(queryText, /\b(?:row|file|publication|object)_(?:id|uuid)\b/iu);
  assert.match(JSON.stringify(dashboard), /AccelerationPendingDataFilesCount/u);
  assert.match(JSON.stringify(dashboard), /LastUpdatedDateTime/u);
  assert.match(JSON.stringify(dashboard), /NotHealthyReason/u);
});

test('Grafana provisioning stays digest-pinned and scrapes the router', async () => {
  const compose = parse(await readFile(composePath, 'utf8')) as {
    services: { lgtm: { image: string; volumes: string[] } };
  };
  const prometheus = parse(await readFile(prometheusPath, 'utf8')) as {
    scrape_configs: Array<{ job_name: string; metrics_path?: string; static_configs: Array<{ targets: string[] }> }>;
  };
  assert.match(compose.services.lgtm.image, /^grafana\/otel-lgtm@sha256:[a-f0-9]{64}$/u);
  assert(compose.services.lgtm.volumes.some(volume => volume.includes('throughput-dashboard.json')));
  assert(prometheus.scrape_configs.some(config =>
    config.job_name === 'router' && config.metrics_path === '/metrics' &&
    config.static_configs.some(target => target.targets.includes('router:50050'))));
});
