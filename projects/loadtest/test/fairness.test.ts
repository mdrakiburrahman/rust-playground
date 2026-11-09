import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';
import { benchmarkComposeFiles, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs, dataflowConfigFile } from '../src/runtime-config.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const source = readFileSync(path.join(root, dataflowConfigFile(false)), 'utf8');
const quiverSource = readFileSync(path.join(root, dataflowConfigFile(true)), 'utf8');
const production = YAML.parse(source);
const nodes = production.groups.arrowcol.pipelines.ingest.nodes;

test('runtime builder applies bounded DFE buffer and registration budgets', () => {
  const runtime = buildBenchmarkRuntimeConfigs(source);
  const builtNodes = YAML.parse(runtime.dataflowYaml).groups.arrowcol.pipelines.ingest.nodes;
  const batching = builtNodes.delta.config.batching;
  assert.deepEqual(batching.global_capacity, { bytes: 134217728, rows: 4000000, batches: 2048 });
  assert.deepEqual(batching.per_table_capacity, { bytes: 33554432, rows: 1000000, batches: 512 });
  assert.equal(batching.max_arrow_memory_per_flush, 33554432);
  assert.equal(builtNodes.flight.config.registration.buffered_bytes_limit, 134217728);
  assert.equal(builtNodes.flight.config.registration.buffered_rows_limit, 4000000);
  assert.equal(builtNodes.flight.config.registration.buffered_batches_limit, 2048);
  assert(batching.max_rows_per_flush <= batching.per_table_capacity.rows);
  assert(batching.max_batches_per_flush <= batching.per_table_capacity.batches);
});

test('runtime builder configures native DFE Quiver only when requested', () => {
  const runtime = YAML.parse(buildBenchmarkRuntimeConfigs(quiverSource, { quiver: true }).dataflowYaml);
  const built = runtime.groups.arrowcol.pipelines.ingest.nodes;
  assert.equal(built.flight.config.native_quiver, true);
  assert.equal(built.durable.type, 'processor:durable_buffer');
  assert.equal(built.durable.config.path, '/quiver');
  assert.equal(built.durable.config.retention_size_cap, '4GiB');
  assert.equal(built.durable.config.wal_flush_interval, '0s');
  assert.equal(built.durable.config.max_age, undefined);
  assert.throws(() => buildBenchmarkRuntimeConfigs(source, { quiver: true }), /requires the Dataflow native Quiver topology/u);
  assert.throws(() => buildBenchmarkRuntimeConfigs(quiverSource), /requires --quiver/u);
});

test('runtime Compose override mounts only DFE config and optional DFE Quiver storage', () => {
  const directory = '/workspace-root/onelake/benchmark/fair-run';
  const override = JSON.parse(buildBenchmarkComposeOverride(directory));
  assert.deepEqual(Object.keys(override.services), ['dataflow-1']);
  assert.deepEqual(override.services['dataflow-1'].volumes, [{
    type: 'bind', source: `${directory}/dataflow-state-1/dataflow.yaml`,
    target: '/etc/arrowcol-dataflow-rust/config.yaml', read_only: true,
    bind: { create_host_path: false },
  }]);
  assert.deepEqual(JSON.parse(buildBenchmarkComposeOverride(directory, { quiver: true })).services['dataflow-1'].volumes[1], {
    type: 'bind', source: `${directory}/dataflow-state-1/quiver`, target: '/quiver', bind: { create_host_path: false },
  });
  assert.deepEqual(benchmarkComposeFiles('compose.yaml', {}), ['--file', 'compose.yaml']);
  assert.deepEqual(benchmarkComposeFiles('compose.yaml', { LOADTEST_COMPOSE_OVERRIDE_FILE: 'runtime.json' }),
    ['--file', 'compose.yaml', '--file', 'runtime.json']);
});

test('DFE production capacity retains explicit flush and commit policies', () => {
  const batching = nodes.delta.config.batching;
  assert(batching.max_rows_per_flush <= batching.per_table_capacity.rows);
  assert(batching.max_batches_per_flush <= batching.per_table_capacity.batches);
  assert.equal(nodes.delta.config.commit.max_files_per_commit, production.groups.arrowcol.pipelines.ingest.nodes.delta.config.commit.max_files_per_commit);
});
