import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { closeSync, fstatSync, openSync, readFileSync, readSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import YAML from 'yaml';

import type { BenchmarkOptions } from './cli.js';
import { benchmarkComposeFiles, dataflowConfigFile } from './runtime-config.js';

export function sha256(text: string | Buffer): string {
  return createHash('sha256').update(text).digest('hex');
}

export function executableIdentity(file: string): { sha256: string; bytes: number } {
  let descriptor: number | undefined;
  try {
    descriptor = openSync(file, 'r');
    const before = fstatSync(descriptor);
    if (!before.isFile() || !(before.mode & 0o111)) throw new Error('Invalid executable');
    const hash = createHash('sha256');
    const buffer = Buffer.allocUnsafe(1024 * 1024);
    let bytes = 0, count: number;
    while ((count = readSync(descriptor, buffer, 0, buffer.length, null)) !== 0) {
      hash.update(buffer.subarray(0, count)); bytes += count;
    }
    const after = fstatSync(descriptor);
    if (!Number.isSafeInteger(bytes) || bytes !== before.size ||
        before.size !== after.size || before.mtimeMs !== after.mtimeMs || before.ctimeMs !== after.ctimeMs) {
      throw new Error('Executable changed during hashing');
    }
    return { sha256: hash.digest('hex'), bytes };
  } catch {
    throw new Error('Helper executable provenance is unavailable or changed; private paths withheld');
  } finally {
    if (descriptor !== undefined) closeSync(descriptor);
  }
}

export function helperIdentities(selectedDirectory: string, mountedDirectory: string) {
  const host = Object.fromEntries(['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect'].map(name =>
    [name, executableIdentity(path.join(selectedDirectory, name))]));
  const mounted = Object.fromEntries(['arrowcol-loadgen', 'arrowcol-sdk-gen'].map(name =>
    [name, executableIdentity(path.join(mountedDirectory, name))]));
  if (Object.entries(mounted).some(([name, value]) =>
    value.sha256 !== host[name].sha256 || value.bytes !== host[name].bytes)) {
    throw new Error('Mounted helper executable does not match the selected host artifact');
  }
  return { host, mounted };
}

type Document = Record<string, any>;

function pick(document: Document, names: string[]): Document {
  return Object.fromEntries(names.filter((name) => document[name] !== undefined).map((name) => [name, document[name]]));
}

export function safeCollectorPolicies(direct: Document, dataflow: Document): Document {
  const nodes = dataflow.groups.arrowcol.pipelines.ingest.nodes;
  return {
    direct: {
      flight: pick(direct.flight, [
        'max_concurrent_streams', 'max_metadata_bytes', 'max_schema_bytes', 'max_message_bytes',
        'batch_channel_capacity', 'put_result_channel_capacity', 'max_stream_duration_ms', 'admission_timeout_ms', 'max_ddl_bytes',
      ]),
      buffer: pick(direct.buffer, [
        'global_capacity', 'per_table_capacity', 'max_arrow_memory_per_flush', 'max_rows_per_flush', 'max_batches_per_flush',
        'max_age_ms', 'age_check_interval_ms', 'max_concurrent_flushes', 'receipt_lease_duration_ms', 'adaptive_file',
      ]),
      delta: pick(direct.delta, ['commit_batch_window_ms', 'max_files_per_commit', 'max_pending_files']),
      routing: pick(direct.routing, ['rss_limit_bytes', 'require_lease_headers']),
      receipts: pick(direct.receipts, [
        'owner_lease_ttl_ms', 'owner_heartbeat_ms', 'table_lease_ttl_ms', 'table_lease_renew_ms',
        'receipt_lease_duration_ms', 'admission_timeout_ms', 'mutation_batch_size', 'status_batch_size', 'reconciliation_batch_size',
      ]),
      shutdown: pick(direct.shutdown, ['stream_drain_timeout_ms', 'publication_drain_timeout_ms']),
      sql_pool: pick(direct.sql_server.pool ?? {}, [
        'receipts_max_size', 'routing_max_size', 'catalog_max_size', 'acquire_timeout_ms',
      ]),
      native_quiver: pick(direct.buffer.native_quiver ?? {}, [
        'enabled', 'directory', 'capacity_bytes', 'wal_flush_interval_ms', 'segment_max_open_ms', 'max_in_flight',
        'recovery_window_ms',
      ]),
    },
    dataflow: {
      pipeline: pick(dataflow.groups.arrowcol.pipelines.ingest.policies, ['channel_capacity']),
      registration: pick(nodes.flight.config.registration, [
        'max_concurrent_streams', 'rss_limit_bytes', 'buffered_bytes_limit', 'buffered_rows_limit', 'buffered_batches_limit',
        'heartbeat_interval', 'pressure_interval', 'registration_ttl',
      ]),
      limits: pick(nodes.flight.config.limits, [
        'max_metadata_bytes', 'max_schema_bytes', 'max_message_bytes', 'batch_channel_capacity', 'put_result_channel_capacity',
        'max_stream_duration', 'admission_timeout', 'max_ddl_bytes',
      ]),
      commit: pick(nodes.delta.config.commit, ['batch_window', 'max_files_per_commit', 'max_pending_files']),
      batching: pick(nodes.delta.config.batching, [
        'global_capacity', 'per_table_capacity', 'max_arrow_memory_per_flush', 'max_rows_per_flush', 'max_batches_per_flush',
        'max_age', 'max_concurrent_flushes', 'adaptive_file',
      ]),
      native_quiver: nodes.flight.config.native_quiver === true,
      durable_buffer: pick(Object.values(nodes as Document).find((node) => node.type === 'processor:durable_buffer')?.config ?? {}, [
        'path', 'retention_size_cap', 'size_cap_policy', 'wal_flush_interval', 'max_segment_open_duration', 'poll_interval',
        'initial_retry_interval', 'max_retry_interval', 'max_in_flight',
      ]),
    },
  };
}

export function safeComposeSettings(compose: Document): Document {
  const names = ['direct-1', 'dataflow-1', 'sqlserver', 'kusto'].filter(name => compose.services[name]);
  return Object.fromEntries(names.map((name) => {
    const service = compose.services[name];
    const environment = service.environment ?? {};
    return [name, {
      cpus: service.cpus,
      cpuQuota: service.cpu_quota,
      cpuPeriod: service.cpu_period,
      memoryLimit: service.mem_limit,
      environment: pick(environment, [
        'TOKIO_WORKER_THREADS', 'ARROWCOL_MAX_MESSAGE_BYTES', 'MSSQL_MEMORY_LIMIT_MB', 'ACCEPT_EULA',
      ]),
      numCores: name === 'dataflow-1' && service.command?.includes('--num-cores')
        ? service.command[service.command.indexOf('--num-cores') + 1] : undefined,
      ...(name === 'kusto' ? {
        image: service.image,
        purpose: 'functional inspection only; not a performance benchmark',
        ports: (service.ports ?? []).map((port: Document) => pick(port, ['target', 'published', 'host_ip', 'protocol'])),
        mounts: (service.volumes ?? []).map((volume: Document) => pick(volume, ['type', 'target', 'read_only'])),
      } : {}),
    }];
  }));
}

export function writeProvenance(
  workspace: string,
  directory: string,
  project: string,
  options: BenchmarkOptions,
  env: NodeJS.ProcessEnv,
  helperDirectory: string,
  started = false,
): void {
  const output = path.join(directory, 'results/provenance.json');
  const execute = (command: string, args: string[]): string => {
    const result = spawnSync(command, args, { cwd: workspace, env, encoding: 'utf8', timeout: 30_000, maxBuffer: 64 * 1024 * 1024 });
    if (result.error || result.status !== 0) throw new Error(`Provenance ${command} failed (output withheld)`);
    return result.stdout;
  };
  if (started) {
    const provenance = JSON.parse(readFileSync(output, 'utf8')) as Document;
    const helpers = helperIdentities(helperDirectory, path.join(workspace, 'projects/loadtest/.temp/bin/latest'));
    if (JSON.stringify(helpers) !== JSON.stringify(provenance.helpers)) {
      throw new Error('Selected helper executable changed during stack startup');
    }
    const ids = execute('docker', ['ps', '--all', '--quiet', '--filter', `label=com.docker.compose.project=${project}`])
      .trim().split('\n').filter(Boolean);
    provenance.containers = ids.map((id) => JSON.parse(execute('docker', [
      'inspect', '--format',
      '{"id":{{json .Id}},"imageId":{{json .Image}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"nanoCpus":{{json .HostConfig.NanoCpus}},"memoryLimit":{{json .HostConfig.Memory}}}',
      id,
    ])) as unknown);
    provenance.stackStartedAt = new Date().toISOString();
    writeFileSync(output, `${JSON.stringify(provenance, null, 2)}\n`);
    return;
  }
  const files = [
    'projects/loadtest/compose.yaml', 'projects/loadtest/config/direct.yaml.tmpl',
    dataflowConfigFile(options.quiver), 'projects/loadtest/config/router.yaml',
  ];
  const direct = YAML.parse(readFileSync(path.join(directory, 'direct-state-1/direct.yaml'), 'utf8')) as Document;
  const dataflow = YAML.parse(readFileSync(path.join(directory, 'dataflow-state-1/dataflow.yaml'), 'utf8')) as Document;
  const revision = (root: string): Document => ({
    head: execute('git', ['-C', root, 'rev-parse', 'HEAD']).trim(),
    dirtyDiffSha256: sha256(execute('git', ['-C', root, 'diff', '--binary', 'HEAD'])),
    untrackedSourceHashes: Object.fromEntries(execute('git', [
      '-C', root, 'ls-files', '--others', '--exclude-standard', '--', '*.rs', '*.ts',
    ]).trim().split('\n').filter(Boolean).map((file) => [file, sha256(readFileSync(path.join(workspace, root, file)))])),
  });
  const provenance: Document = {
    createdAt: new Date().toISOString(), project, options,
    source: revision('.'), otelArrowSource: revision('submodules/otel-arrow'),
    helpers: helperIdentities(helperDirectory, path.join(workspace, 'projects/loadtest/.temp/bin/latest')),
    configSha256: Object.fromEntries(files.map((file) => [file, sha256(readFileSync(path.join(workspace, file)))])),
    generatedDirectConfigSha256: sha256(readFileSync(path.join(directory, 'direct-state-1/direct.yaml'))),
    generatedDataflowConfigSha256: sha256(readFileSync(path.join(directory, 'dataflow-state-1/dataflow.yaml'))),
    runtimeComposeOverrideSha256: sha256(readFileSync(path.join(directory, 'router-state/compose-runtime.json'))),
    policies: safeCollectorPolicies(direct, dataflow),
    workload: {
      ...options, scenario: 'paired', tables: env.LOADTEST_TABLES?.split(',') ?? 'all schema fixtures (13 tables)',
      maxAttempts: env.LOADTEST_MAX_ATTEMPTS ?? '16',
      continuous: options.measureSeconds === undefined, keepStack: true,
    },
    sampler: { intervalMs: 10_000, file: 'resource-samples.jsonl', workingSet: 'max(0, memory.current - memory.stat.inactive_file)' },
    containers: [],
  };
  writeFileSync(output, `${JSON.stringify(provenance, null, 2)}\n`);
  try {
    const effective = JSON.parse(execute('docker', [
      'compose', '--project-name', project, ...benchmarkComposeFiles(path.join(workspace, files[0]), env),
      '--profile', 'benchmark', 'config', '--format', 'json',
    ])) as Document;
    provenance.effectiveComposeSha256 = sha256(JSON.stringify(effective));
    provenance.effectiveResources = safeComposeSettings(effective);
    const cliCores = Number(provenance.effectiveResources['dataflow-1'].numCores);
    const pipelineAllocation = dataflow.groups.arrowcol.pipelines.ingest.policies?.resources?.core_allocation;
    const groupAllocation = dataflow.groups.arrowcol.policies?.resources?.core_allocation;
    provenance.dataflowCoreAllocation = {
      sourcePolicy: pick(dataflow.policies.resources.core_allocation, ['type', 'count']),
      cliNumCores: cliCores,
      resolvedPolicy: pipelineAllocation ?? groupAllocation ?? { type: 'core_count', count: cliCores },
      resolution: 'pipeline > group > CLI-overridden engine policy',
      streamLimitScope: 'process-wide shared permits across DFE receiver cores',
      bufferBudgetScope: 'process-wide shared DeltaBackend keyed by base_path; no per-core division',
      aggregateMaxConcurrentStreams: dataflow.groups.arrowcol.pipelines.ingest.nodes.flight.config.registration.max_concurrent_streams,
    };
    // Resolve only the non-sensitive Flight message size override, not arbitrary environment strings.
    provenance.policies.dataflow.limits.max_message_bytes = Number(effective.services['dataflow-1'].environment.ARROWCOL_MAX_MESSAGE_BYTES);
    writeFileSync(output, `${JSON.stringify(provenance, null, 2)}\n`);
  } catch {
    provenance.failure = { phase: 'effective-settings', timestamp: new Date().toISOString() };
    writeFileSync(output, `${JSON.stringify(provenance, null, 2)}\n`);
    throw new Error('Unable to capture effective benchmark provenance; raw Compose output was not saved');
  }
}
