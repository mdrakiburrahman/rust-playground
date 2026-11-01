import assert from 'node:assert/strict';
import { chmodSync, copyFileSync, mkdirSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';

import YAML from 'yaml';

import { helperIdentities, safeCollectorPolicies, safeComposeSettings, sha256, writeProvenance } from '../src/provenance.js';
import { parseBenchmarkOptions } from '../src/cli.js';
import { buildBenchmarkRuntimeConfigs } from '../src/runtime-config.js';
import { fixtureDirectory } from './fixture-directory.js';

test('helper identities bind actual selected host artifacts and mounted copies without private paths', t => {
  const root = fixtureDirectory('helper-provenance-');
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const selected = path.join(root, 'private-source-target', 'debug');
  const stale = path.join(root, 'target', 'debug');
  const mounted = path.join(root, 'mounted');
  for (const directory of [selected, stale, mounted]) mkdirSync(directory, { recursive: true });
  for (const name of ['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect']) {
    writeFileSync(path.join(selected, name),
      name === 'delta-inspect' ? Buffer.alloc(2 * 1024 * 1024 + 17, 0x61) : `selected-${name}\n`, { mode: 0o755 });
    writeFileSync(path.join(stale, name), 'stale-default-target', { mode: 0o755 });
    if (name !== 'delta-inspect') copyFileSync(path.join(selected, name), path.join(mounted, name));
  }
  const receipt = helperIdentities(selected, mounted);
  assert.equal(receipt.host['delta-inspect'].sha256, sha256(Buffer.alloc(2 * 1024 * 1024 + 17, 0x61)));
  assert.equal(receipt.host['delta-inspect'].bytes, 2 * 1024 * 1024 + 17);
  assert.equal(receipt.host['arrowcol-loadgen'].sha256, receipt.mounted['arrowcol-loadgen'].sha256);
  assert(!JSON.stringify(receipt).includes(root));
  assert(!JSON.stringify(receipt).includes('private-source-target'));
  assert.throws(() => helperIdentities(stale, mounted), /does not match the selected host artifact/u);
  writeFileSync(path.join(mounted, 'arrowcol-loadgen'), 'stale-mounted-helper');
  assert.throws(() => helperIdentities(selected, mounted), /does not match the selected host artifact/u);
  copyFileSync(path.join(selected, 'arrowcol-loadgen'), path.join(mounted, 'arrowcol-loadgen'));
  chmodSync(path.join(selected, 'delta-inspect'), 0o644);
  assert.throws(() => helperIdentities(selected, mounted), /private paths withheld/u);
  chmodSync(path.join(selected, 'delta-inspect'), 0o755);
  unlinkSync(path.join(selected, 'delta-inspect'));
  assert.throws(() => helperIdentities(selected, mounted), error =>
    error instanceof Error && !error.message.includes(root) && /provenance is unavailable/u.test(error.message));
});

test('stack-start provenance refuses selected helper replacement after its initial receipt', t => {
  const root = fixtureDirectory('helper-startup-');
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const selected = path.join(root, 'selected', 'debug');
  const mounted = path.join(root, 'projects/loadtest/.temp/bin/latest');
  const directory = path.join(root, 'benchmark');
  for (const target of [selected, mounted, path.join(directory, 'results')]) mkdirSync(target, { recursive: true });
  for (const name of ['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect']) {
    writeFileSync(path.join(selected, name), name, { mode: 0o755 });
    if (name !== 'delta-inspect') copyFileSync(path.join(selected, name), path.join(mounted, name));
  }
  const output = path.join(directory, 'results/provenance.json');
  const initial = { helpers: helperIdentities(selected, mounted) };
  writeFileSync(output, JSON.stringify(initial));
  writeFileSync(path.join(root, 'docker'), '#!/bin/sh\nexit 0\n', { mode: 0o755 });
  const options = parseBenchmarkOptions([]);
  const env = { ...process.env, PATH: `${root}:${process.env.PATH}` };
  writeProvenance(root, directory, 'fixture', options, env, selected, true);
  const before = readFileSync(output, 'utf8');
  assert.deepEqual(JSON.parse(before).helpers, initial.helpers);
  writeFileSync(path.join(selected, 'arrowcol-loadgen'), 'replacement');
  copyFileSync(path.join(selected, 'arrowcol-loadgen'), path.join(mounted, 'arrowcol-loadgen'));
  assert.throws(() => writeProvenance(root, directory, 'fixture', options, env, selected, true),
    /Selected helper executable changed during stack startup/u);
  assert.equal(readFileSync(output, 'utf8'), before, 'retain the original artifact receipt rather than relabeling it');
});

test('provenance whitelists nonsensitive effective resource settings and never stores raw Compose environment', () => {
  const service = {
    cpus: 2, cpu_quota: 200000, cpu_period: 100000,
    command: ['--num-cores', '2', '--password', 'TOP_SECRET'],
    environment: { TOKIO_WORKER_THREADS: '2', ARROWCOL_MAX_MESSAGE_BYTES: '33554432', MSSQL_MEMORY_LIMIT_MB: '16384', PASSWORD: 'TOP_SECRET' },
    secrets: ['TOP_SECRET'],
  };
  const settings = safeComposeSettings({ services: {
    'direct-1': service, 'dataflow-1': service, sqlserver: service,
    kusto: {
      ...service, mem_limit: '4294967296', image: 'emulator@sha256:pinned',
      environment: { ACCEPT_EULA: 'Y', PASSWORD: 'TOP_SECRET' },
      ports: [{ host_ip: '127.0.0.1', published: '8080', target: 8080, protocol: 'tcp', password: 'TOP_SECRET' }],
      volumes: [
        { type: 'volume', source: 'TOP_SECRET', target: '/kustodata' },
        { type: 'bind', source: '/TOP_SECRET', target: '/delta/direct', read_only: true },
      ],
    },
  } });
  assert(!JSON.stringify(settings).includes('TOP_SECRET'));
  assert.equal(settings['dataflow-1'].numCores, '2');
  assert.equal(settings['direct-1'].environment.TOKIO_WORKER_THREADS, '2');
  assert.equal(settings.sqlserver.environment.MSSQL_MEMORY_LIMIT_MB, '16384');
  assert.equal(settings.kusto.memoryLimit, '4294967296');
  assert.equal(settings.kusto.image, 'emulator@sha256:pinned');
  assert.match(settings.kusto.purpose, /not a performance benchmark/u);
  assert.deepEqual(settings.kusto.environment, { ACCEPT_EULA: 'Y' });
  assert.deepEqual(settings.kusto.ports, [{ target: 8080, published: '8080', host_ip: '127.0.0.1', protocol: 'tcp' }]);
  assert.deepEqual(settings.kusto.mounts, [
    { type: 'volume', target: '/kustodata' }, { type: 'bind', target: '/delta/direct', read_only: true },
  ]);
  assert.notEqual(sha256('before'), sha256('after'));
  assert.equal(sha256('before').length, 64);
});

test('policy provenance reports generated 128 MiB budgets rather than unchanged production defaults', () => {
  const runtime = buildBenchmarkRuntimeConfigs(
    readFileSync(path.resolve('projects/loadtest/config/direct.yaml.tmpl'), 'utf8'),
    readFileSync(path.resolve('projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'),
    { instanceKey: 'loadtest-direct-1', advertisedEndpoint: '10.89.0.21:50051', generation: 1 },
  );
  const policies = safeCollectorPolicies(YAML.parse(runtime.directYaml), YAML.parse(runtime.dataflowYaml));
  assert.equal(policies.direct.buffer.global_capacity.bytes, 134217728);
  assert.equal(policies.dataflow.batching.global_capacity.bytes, 134217728);
  assert.equal(policies.direct.buffer.per_table_capacity.bytes, 33554432);
  assert.equal(policies.dataflow.batching.max_arrow_memory_per_flush, 33554432);
  assert.equal(policies.dataflow.registration.buffered_bytes_limit, 134217728);
  assert.equal(policies.dataflow.registration.buffered_rows_limit, 4000000);
  assert.equal(policies.dataflow.registration.buffered_batches_limit, 2048);
});

test('collector policy provenance includes actual bounded flush/commit limits without auth or SQL secrets', () => {
  const direct = YAML.parse(readFileSync(path.resolve('projects/loadtest/config/direct.yaml.tmpl'), 'utf8'));
  const dataflow = YAML.parse(readFileSync(path.resolve('projects/arrowcol-dataflow-rust/config.yaml'), 'utf8'));
  direct.auth.password = 'TOP_SECRET';
  direct.sql_server.password = 'TOP_SECRET';
  dataflow.groups.arrowcol.pipelines.ingest.nodes.flight.config.catalog.password = 'TOP_SECRET';
  const policies = safeCollectorPolicies(direct, dataflow);
  assert(!JSON.stringify(policies).includes('TOP_SECRET'));
  assert.equal(policies.direct.buffer.max_arrow_memory_per_flush, 536870912);
  assert.equal(policies.dataflow.commit.max_pending_files, 256);
  assert.equal(policies.direct.receipts.receipt_lease_duration_ms, 20000);
  assert.deepEqual(policies.direct.sql_pool, {
    receipts_max_size: 16, routing_max_size: 4, catalog_max_size: 4, acquire_timeout_ms: 5000,
  });
});
