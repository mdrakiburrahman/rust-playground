import path from 'node:path';

import YAML from 'yaml';
import type { AzureTargets, LoadtestMode } from './mode.js';

export const benchmarkBufferBudget = Object.freeze({
  global_capacity: Object.freeze({ bytes: 134217728, rows: 4000000, batches: 2048 }),
  per_table_capacity: Object.freeze({ bytes: 33554432, rows: 1000000, batches: 512 }),
  max_arrow_memory_per_flush: 33554432,
});

/** Native Quiver placement for `--quiver`; directories are container paths on writable bind mounts. */
export const benchmarkQuiver = Object.freeze({
  directDirectory: '/state/quiver',
  dataflowDirectory: '/quiver',
  dataflowHostDirectory: 'dataflow-state-1/quiver',
  directHostDirectory: 'direct-state-1/quiver',
  capacityBytes: 4294967296,
});

/** Workspace-relative Dataflow configuration: `--quiver` selects the durable-buffer topology. */
export function dataflowConfigFile(quiver: boolean): string {
  return `projects/arrowcol-dataflow-rust/${quiver ? 'config.quiver.yaml' : 'config.yaml'}`;
}

interface DirectIdentity {
  instanceKey: string;
  advertisedEndpoint: string;
  generation: number;
}

export interface BenchmarkRuntimeOptions {
  /** Persist every batch to native Quiver (fsync before acknowledgement) before publishing it. */
  quiver?: boolean;
  mode?: LoadtestMode;
  azure?: AzureTargets;
  azureConfigHostDirectory?: string;
  azureEnvironment?: NodeJS.ProcessEnv;
}

export function buildBenchmarkRuntimeConfigs(
  directSource: string,
  dataflowSource: string,
  identity?: DirectIdentity,
  options: BenchmarkRuntimeOptions = {},
): { directYaml: string; dataflowYaml: string } {
  if (identity) {
    if (!Number.isSafeInteger(identity.generation) || identity.generation <= 0) throw new Error('Invalid direct generation');
    directSource = directSource
      .replaceAll('__INSTANCE_KEY__', identity.instanceKey)
      .replaceAll('__ADVERTISED_ENDPOINT__', identity.advertisedEndpoint)
      .replaceAll('__GENERATION__', String(identity.generation));
  }
  const direct = YAML.parse(directSource);
  const dataflow = YAML.parse(dataflowSource);
  const nodes = dataflow.groups.arrowcol.pipelines.ingest.nodes;
  if (options.mode === 'azure') {
    if (!options.azure) throw new Error('Azure runtime requires validated targets');
    direct.delta.root = options.azure.directRoot;
    nodes.delta.config.base_path = options.azure.dataflowRoot;
  }
  for (const buffer of [direct.buffer, nodes.delta.config.batching]) {
    buffer.global_capacity = { ...benchmarkBufferBudget.global_capacity };
    buffer.per_table_capacity = { ...benchmarkBufferBudget.per_table_capacity };
    buffer.max_arrow_memory_per_flush = benchmarkBufferBudget.max_arrow_memory_per_flush;
    if (buffer.max_rows_per_flush > buffer.per_table_capacity.rows ||
        buffer.max_batches_per_flush > buffer.per_table_capacity.batches) {
      throw new Error('Benchmark capacity is below the preserved flush row/batch limits');
    }
  }
  Object.assign(nodes.flight.config.registration, {
    buffered_bytes_limit: benchmarkBufferBudget.global_capacity.bytes,
    buffered_rows_limit: benchmarkBufferBudget.global_capacity.rows,
    buffered_batches_limit: benchmarkBufferBudget.global_capacity.batches,
  });
  const durable = Object.values(nodes as Record<string, { type?: string; config?: Record<string, unknown> }>)
    .filter((node) => node.type === 'processor:durable_buffer');
  if (options.quiver) {
    if (durable.length !== 1 || nodes.flight.config.native_quiver !== true) {
      throw new Error('--quiver requires the Dataflow native Quiver topology (config.quiver.yaml)');
    }
    durable[0].config = {
      ...durable[0].config,
      path: benchmarkQuiver.dataflowDirectory,
      retention_size_cap: `${benchmarkQuiver.capacityBytes / 1073741824}GiB`,
      wal_flush_interval: '0s',
    };
    direct.buffer.native_quiver = {
      ...direct.buffer.native_quiver,
      enabled: true,
      directory: benchmarkQuiver.directDirectory,
      capacity_bytes: benchmarkQuiver.capacityBytes,
      wal_flush_interval_ms: 0,
    };
  } else if (durable.length > 0 || nodes.flight.config.native_quiver === true || direct.buffer.native_quiver?.enabled) {
    throw new Error('Native Quiver configuration requires --quiver');
  }
  return { directYaml: YAML.stringify(direct), dataflowYaml: YAML.stringify(dataflow) };
}

export function buildBenchmarkComposeOverride(hostDirectory: string, options: BenchmarkRuntimeOptions = {}): string {
  const cloud: Record<string, { environment: Record<string, string>; volumes: object[] }> = {};
  if (options.mode === 'azure') {
    if (!options.azureConfigHostDirectory) throw new Error('Azure runtime requires isolated writable CLI configuration');
    const environment: Record<string, string> = { AZURE_CONFIG_DIR: '/run/azure', AZURE_USE_AZURE_CLI: 'true' };
    for (const key of ['AZURE_SUBSCRIPTION_ID', 'AZURE_STORAGE_ACCOUNT', 'AZURE_STORAGE_CONTAINER', 'AZURE_STORAGE_CHECKPOINT_CONTAINER']) {
      const value = options.azureEnvironment?.[key];
      if (!value) throw new Error(`Azure runtime requires ${key}`);
      environment[key] = value;
    }
    for (const service of ['direct-1', 'dataflow-1']) {
      cloud[service] = { environment, volumes: [{
        type: 'bind', source: path.join(options.azureConfigHostDirectory, service),
        target: '/run/azure', bind: { create_host_path: false },
      }] };
    }
  }
  return JSON.stringify({
    services: {
      ...(cloud['direct-1'] ? { 'direct-1': cloud['direct-1'] } : {}),
      'dataflow-1': {
        ...(cloud['dataflow-1'] ? { environment: cloud['dataflow-1'].environment } : {}),
        volumes: [{
          type: 'bind',
          source: path.join(hostDirectory, 'dataflow-state-1/dataflow.yaml'),
          target: '/etc/arrowcol-dataflow-rust/config.yaml',
          read_only: true,
          bind: { create_host_path: false },
        }, ...(options.quiver ? [{
          type: 'bind',
          source: path.join(hostDirectory, benchmarkQuiver.dataflowHostDirectory),
          target: benchmarkQuiver.dataflowDirectory,
          bind: { create_host_path: false },
        }] : []), ...(cloud['dataflow-1']?.volumes ?? [])],
      },
    },
  }, null, 2);
}

export function benchmarkComposeFiles(baseFile: string, env: NodeJS.ProcessEnv): string[] {
  return [
    '--file', baseFile,
    ...(env.LOADTEST_COMPOSE_OVERRIDE_FILE ? ['--file', env.LOADTEST_COMPOSE_OVERRIDE_FILE] : []),
  ];
}
