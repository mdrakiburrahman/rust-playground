import path from 'node:path';

import YAML from 'yaml';
import type { AzureTargets, LoadtestMode } from './mode.js';

export const benchmarkBufferBudget = Object.freeze({
  global_capacity: Object.freeze({ bytes: 134217728, rows: 4000000, batches: 2048 }),
  per_table_capacity: Object.freeze({ bytes: 33554432, rows: 1000000, batches: 512 }),
  max_arrow_memory_per_flush: 33554432,
});

/** Native Quiver placement for --quiver; directories are container paths on writable bind mounts. */
export const benchmarkQuiver = Object.freeze({
  dataflowDirectory: '/quiver',
  dataflowHostDirectory: 'dataflow-state-1/quiver',
  capacityBytes: 4294967296,
});

/** Workspace-relative Dataflow configuration: --quiver selects the durable-buffer topology. */
export function dataflowConfigFile(quiver: boolean): string {
  return 'projects/arrowcol-dataflow-rust/config' + (quiver ? '.quiver.yaml' : '.yaml');
}

export function buildLocalLoadgenEndpoints(
  network: { router: string },
  runnerNetwork: 'host' | 'compose',
  hostPorts: { router: string; catalog: string },
): { router: string; catalog: string } {
  if (runnerNetwork === 'compose') {
    return { router: `http://${network.router}:50050`, catalog: 'http://arrowcol-catalog:8817' };
  }
  return {
    router: `http://127.0.0.1:${hostPorts.router}`,
    catalog: `http://127.0.0.1:${hostPorts.catalog}`,
  };
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
  dataflowSource: string,
  options: BenchmarkRuntimeOptions = {},
): { dataflowYaml: string } {
  const dataflow = YAML.parse(dataflowSource);
  const nodes = dataflow.groups.arrowcol.pipelines.ingest.nodes;
  const batching = nodes.delta.config.batching;
  if (options.mode === 'azure') {
    if (!options.azure) throw new Error('Azure runtime requires validated targets');
    nodes.delta.config.base_path = options.azure.dataflowRoot;
  }
  batching.global_capacity = { ...benchmarkBufferBudget.global_capacity };
  batching.per_table_capacity = { ...benchmarkBufferBudget.per_table_capacity };
  batching.max_arrow_memory_per_flush = benchmarkBufferBudget.max_arrow_memory_per_flush;
  if (batching.max_rows_per_flush > batching.per_table_capacity.rows ||
      batching.max_batches_per_flush > batching.per_table_capacity.batches) {
    throw new Error('Benchmark capacity is below the preserved flush row/batch limits');
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
      retention_size_cap: String(benchmarkQuiver.capacityBytes / 1073741824) + 'GiB',
      wal_flush_interval: '0s',
    };
  } else if (durable.length > 0 || nodes.flight.config.native_quiver === true) {
    throw new Error('Native Quiver configuration requires --quiver');
  }
  return { dataflowYaml: YAML.stringify(dataflow) };
}

export function buildBenchmarkComposeOverride(hostDirectory: string, options: BenchmarkRuntimeOptions = {}): string {
  const cloud: Record<string, { environment: Record<string, string>; volumes: object[] }> = {};
  if (options.mode === 'azure') {
    if (!options.azureConfigHostDirectory || !options.azure) {
      throw new Error('Azure runtime requires isolated writable CLI configuration and validated roots');
    }
    const environment: Record<string, string> = { AZURE_CONFIG_DIR: '/run/azure', AZURE_USE_AZURE_CLI: 'true' };
    for (const key of ['AZURE_SUBSCRIPTION_ID', 'AZURE_STORAGE_ACCOUNT', 'AZURE_STORAGE_CONTAINER',
      'AZURE_STORAGE_CHECKPOINT_CONTAINER', 'ARROWCOL_RESET_GENERATION']) {
      const value = options.azureEnvironment?.[key];
      if (!value) throw new Error('Azure runtime requires ' + key);
      environment[key] = value;
    }
    if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/iu.test(environment.ARROWCOL_RESET_GENERATION)) {
      throw new Error('Azure runtime requires a metadata-issued ARROWCOL_RESET_GENERATION UUID');
    }
    const roots: Record<string, string> = {
      'dataflow-1': options.azure.dataflowRoot,
      'arrowcol-catalog': options.azure.dataflowRoot,
      'tenant-stream': options.azure.tenantRoot,
      'delta-maintenance': options.azure.dataflowRoot,
    };
    for (const [service, root] of Object.entries(roots)) {
      cloud[service] = { environment: { ...environment, ARROWCOL_DELTA_ROOT: root }, volumes: [{
        type: 'bind', source: path.join(options.azureConfigHostDirectory, service),
        target: '/run/azure', bind: { create_host_path: false },
      }] };
    }
  }
  return JSON.stringify({
    services: {
      ...(cloud['arrowcol-catalog'] ? { 'arrowcol-catalog': cloud['arrowcol-catalog'] } : {}),
      ...(cloud['tenant-stream'] ? { 'tenant-stream': cloud['tenant-stream'] } : {}),
      ...(cloud['delta-maintenance'] ? { 'delta-maintenance': cloud['delta-maintenance'] } : {}),
      'dataflow-1': {
        ...(cloud['dataflow-1'] ? { environment: cloud['dataflow-1'].environment } : {}),
        volumes: [{
          type: 'bind', source: path.join(hostDirectory, 'dataflow-state-1/dataflow.yaml'),
          target: '/etc/arrowcol-dataflow-rust/config.yaml', read_only: true,
          bind: { create_host_path: false },
        }, ...(options.quiver ? [{
          type: 'bind', source: path.join(hostDirectory, benchmarkQuiver.dataflowHostDirectory),
          target: benchmarkQuiver.dataflowDirectory, bind: { create_host_path: false },
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
