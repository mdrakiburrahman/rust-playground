import { createHash } from 'node:crypto';
import { parseAllDocuments } from 'yaml';
import { clientRepository, imageBuilds, repositories } from './publish-images.js';

const runLabel = 'arrowcol-oat/run-id';
const profileLabel = 'arrowcol-oat/client-profile';
const clientIndexLabel = 'arrowcol-oat/client-index';
export const MAX_SCALED_CLIENTS = 8;

export const collectorShapes = {
  e64: { vmSize: 'Standard_E64ds_v6', vcpus: 64, runtimeCores: 48, memoryGiB: 384 },
  e192: { vmSize: 'Standard_E192ids_v6', vcpus: 192, runtimeCores: 176, memoryGiB: 1536 },
} as const;
export type CollectorShape = keyof typeof collectorShapes;

export interface ScaledPoolSettings {
  enableAutoScaling: boolean;
  minCount: number;
  maxCount: number;
  count: number;
}

export interface ScaledPoolPlan {
  pool: 'benchd64';
  clientCount: number;
  original: ScaledPoolSettings;
  active: Pick<ScaledPoolSettings, 'enableAutoScaling' | 'minCount' | 'maxCount'>;
}

export interface CandidateImageManifest {
  schemaVersion: 1;
  sourceCommit: string;
  workloadConfigSha256: string;
  images: Record<string, string>;
}

export interface EffectiveWorkloadConfig {
  schemaVersion: 1;
  profile: 'diagnostic-replay' | 'non-parity';
  dataProfile: 'diagnostic' | 'realistic';
  ipcCompression: 'none' | 'lz4_frame' | 'zstd';
  clientCount: number;
  generatorArgs: string[];
  performanceConfig: Record<string, string>;
  dataIdentities: { clientId: string; podOrdinal: number }[];
  historicalDataIdentityEligible: boolean;
  sha256: string;
}

// Pin the CLI defaults from dmv_client::Options instead of inheriting image defaults.
const clientDefaults: Record<string, string> = {
  'corpus-rows': '0', 'corpus-bytes': '0', 'batch-rows': '0', workers: '0', 'runtime-threads': '0',
  'data-mode': 'shared', 'data-profile': 'diagnostic', 'string-width': '0', cardinality: '1024',
  'null-fraction': '0', skew: '0', entropy: '1', seed: '20260923', 'max-memory-bytes': '536870912',
  'interval-us': '0', 'connections-per-worker': '1', 'streams-per-worker': '0', 'max-rows': '0',
  'stream-retry-max-attempts': '4', 'stream-retry-base-backoff-ms': '10', 'stream-retry-max-backoff-ms': '250',
  'ipc-compression': 'none', 'dictionary-handling': 'hydrate', 'grpc-send-compression': 'none',
  'grpc-accept-compression': 'none', 'max-flight-data-bytes': '2097152', 'max-message-bytes': '16777216',
  'stream-window-bytes': '1048576', 'connection-window-bytes': '16777216', 'max-frame-bytes': '16384',
  'header-table-bytes': '4096', 'max-header-list-bytes': '16384', 'adaptive-window': 'false',
  'keepalive-interval-ms': '10000', 'keepalive-timeout-ms': '20000', 'keepalive-while-idle': 'true',
  'connect-timeout-ms': '30000', 'request-timeout-ms': '30000', 'tcp-nodelay': 'true',
  'tcp-keepalive-ms': '60000', 'tcp-keepalive-interval-ms': '0', 'tcp-keepalive-retries': '0',
  'endpoint-concurrency-limit': '256', 'endpoint-buffer-size': '1024', 'connect-retries': '0',
  'retry-backoff-ms': '100', 'rate-limit-rps': '0', 'pod-ordinal': '0', pilot: 'false',
};
const distributionFlags = ['string-length-distribution', 'value-variation', 'guid-uniqueness'];
const controlFlags = ['endpoint', 'client-id', 'table', 'rows', 'concurrency', 'edge-secret-file',
  'warmup-secs', 'duration-secs', 'schedule-file', 'schedule-timeout-secs', 'pod-ordinal'];

function resolvedClientConfig(resource: Record<string, unknown>): Record<string, string> {
  const generator = findGenerator(resource);
  const pod = requireRecord(requireRecord(requireRecord(resource.spec, 'Job spec').template, 'Job template').spec, 'Pod spec');
  const container = requireRecord((pod.containers as unknown[])[generator.containerIndex], 'Generator');
  if (container.command !== undefined || container.envFrom !== undefined || pod.initContainers !== undefined) {
    throw new Error('Unknown generator command, envFrom or init-container configuration source.');
  }
  const config: Record<string, string> = {};
  const allowed = new Set([...Object.keys(clientDefaults), ...distributionFlags, ...controlFlags]);
  if (generator.args.length % 2 !== 0) throw new Error('Generator arguments must be flag/value pairs.');
  for (let index = 0; index < generator.args.length; index += 2) {
    const flag = generator.args[index];
    const value = generator.args[index + 1];
    if (typeof flag !== 'string' || !flag.startsWith('--') || typeof value !== 'string' ||
        !allowed.has(flag.slice(2)) || config[flag.slice(2)] !== undefined) {
      throw new Error('Unknown or duplicate effective generator argument.');
    }
    config[flag.slice(2)] = value;
  }
  if (container.env !== undefined && !Array.isArray(container.env)) throw new Error('Generator env is invalid.');
  const seen = new Set<string>();
  for (const raw of (container.env ?? []) as unknown[]) {
    const env = requireRecord(raw, 'Generator env');
    if (typeof env.name !== 'string' || !env.name.startsWith('ARROWCOL_CLIENT_') || typeof env.value !== 'string' ||
        env.valueFrom !== undefined || seen.has(env.name)) throw new Error('Unknown effective generator environment source.');
    seen.add(env.name);
    const flag = env.name.slice('ARROWCOL_CLIENT_'.length).toLowerCase().replaceAll('_', '-');
    if (!allowed.has(flag) || (config[flag] !== undefined && config[flag] !== env.value)) {
      throw new Error('Conflicting or unknown effective generator environment value.');
    }
    config[flag] = env.value;
  }
  const resolved = { ...clientDefaults, ...config };
  if (!['diagnostic', 'realistic'].includes(resolved['data-profile']!) ||
      !['none', 'lz4_frame', 'zstd'].includes(resolved['ipc-compression']!)) throw new Error('Unknown workload profile or IPC codec.');
  const realistic = resolved['data-profile'] === 'realistic';
  resolved['string-length-distribution'] ??= realistic ? 'uniform' : 'fixed';
  resolved['value-variation'] ??= realistic ? 'per_row' : 'cardinality';
  resolved['guid-uniqueness'] ??= realistic ? 'per_row' : 'cardinality';
  if (realistic && resolved['string-width'] === '0') resolved['string-width'] = '96';
  const enums: Record<string, string[]> = {
    'data-mode': ['shared', 'independent'], 'dictionary-handling': ['hydrate'],
    'grpc-send-compression': ['none', 'gzip', 'zstd'], 'grpc-accept-compression': ['none', 'gzip', 'zstd'],
    'string-length-distribution': ['fixed', 'uniform'], 'value-variation': ['cardinality', 'per_row'],
    'guid-uniqueness': ['cardinality', 'per_row'],
  };
  for (const [flag, values] of Object.entries(enums)) if (!values.includes(resolved[flag]!)) throw new Error('Unknown effective ' + flag + '.');
  for (const flag of ['adaptive-window', 'keepalive-while-idle', 'tcp-nodelay', 'pilot']) {
    if (!['true', 'false'].includes(resolved[flag]!)) throw new Error('Unknown effective ' + flag + '.');
  }
  const numeric = Object.keys(clientDefaults).filter(flag => !['data-profile', 'ipc-compression', ...Object.keys(enums),
    'adaptive-window', 'keepalive-while-idle', 'tcp-nodelay', 'pilot'].includes(flag));
  for (const flag of [...numeric, 'rows', 'concurrency', 'duration-secs', 'warmup-secs', 'schedule-timeout-secs']) {
    if (!/^\d+(?:\.\d+)?$/.test(resolved[flag] ?? '') || !Number.isFinite(Number(resolved[flag]))) throw new Error('Unknown effective numeric ' + flag + '.');
  }
  if (resolved['batch-rows'] === '0') resolved['batch-rows'] = resolved.rows!;
  if (resolved['corpus-rows'] === '0' && resolved['corpus-bytes'] === '0') resolved['corpus-rows'] = resolved.rows!;
  if (resolved['streams-per-worker'] === '0') resolved['streams-per-worker'] = resolved.concurrency!;
  if (resolved.workers === '0' || resolved['runtime-threads'] === '0') {
    throw new Error('Effective workload must specify workers and runtime threads; CPU-derived defaults are unknown.');
  }
  return resolved;
}

export interface ScaledClientJob {
  name: string;
  clientId: string;
  index: number;
  podOrdinal: number;
}

export interface ScaledClientReadiness {
  jobName: string;
  ready: boolean;
  failed: boolean;
}

export interface ScaledClientPlacement {
  jobName: string;
  podName: string;
  podUid: string;
  nodeName: string;
  imageId: string;
}

export interface ScaledRuntimeObservation {
  collectorPodUid: string;
  collectorNodeName: string;
  collectorImageId: string;
  clients: ScaledClientPlacement[];
  nodeEvidence?: { nodeName: string; vmSize: string; allocatableCpuCores: number; allocatableMemoryBytes: number }[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${label} is invalid.`);
  return value;
}

export function summarizeScaledClientWindow(logs: string, binding: {
  podUid: string; clientId: string; start: number; end: number; completedRows: number; workers: number; streamsPerWorker: number;
}) {
  const receipts = logs.split(/\r?\n/).filter(line => line.startsWith('OAT_CLIENT_METRICS ')).map(line =>
    requireRecord(JSON.parse(line.slice('OAT_CLIENT_METRICS '.length)), 'Client metrics receipt'));
  if (receipts.some(receipt => receipt.phase === 'failed')) throw new Error('Scaled client emitted a failed terminal receipt.');
  const terminals = receipts.filter(receipt => receipt.phase === 'completed');
  if (terminals.length > 1) throw new Error('Scaled client emitted duplicate terminal receipts.');
  const terminal = terminals[0];
  const common = {
    schema_version: 1, pod_uid: binding.podUid, client_id: binding.clientId,
    measurement_start_unix_ms: binding.start, measurement_end_unix_ms: binding.end,
    completed_rows: binding.completedRows, completion_semantics: 'client_admission_response_not_delta_commit',
    send_to_commit_latency_us: { p50: null, p95: null, p99: null, samples: null,
      unavailable_reason: 'No actual send-to-Delta-version join; admission percentiles are not subtracted from commit percentiles.' },
    storage_commit_duration_us: { p50: null, p95: null, p99: null, samples: null,
      unavailable_reason: 'Delta Add reconciliation proves committed rows, not storage commit duration.' },
    configured_workers: binding.workers, configured_streams_per_worker: binding.streamsPerWorker,
    configured_transport_slots: binding.workers * binding.streamsPerWorker,
    slot_occupancy: null, slot_occupancy_unavailable_reason: 'Client receipts do not sample active slot occupancy.',
    completed_rows_per_stream: null, flight_data_messages_per_stream: null,
    stream_detail_unavailable_reason: 'Receipts do not enumerate each completed stream and its FlightData messages; fixed rows or message counts are not inferred.',
  };
  let measurement: Record<string, unknown> | null = null;
  let unavailableReason: string | null = null;
  if (!terminal || terminal.version !== 1 || !isRecord(terminal.measurement_window)) {
    unavailableReason = 'No supported final measurement-window receipt; process-cumulative counters are not substituted.';
  } else {
    const window = terminal.measurement_window;
    if (terminal.completion_semantics !== 'validated_admission_response_not_delta_commit' ||
        window.scope !== 'per_stream_admission_completion_observed_in_half_open_shared_unix_window' ||
        window.start_unix_ms !== binding.start || window.end_unix_ms !== binding.end ||
        window.completed_rows !== binding.completedRows) throw new Error('Client receipt does not match the observed global window and completed rows.');
    const missing: Record<string, string> = {};
    const count = (value: unknown, name: string): number | null => {
      if (value === undefined || value === null) { missing[name] = 'Not present in the final measurement-window receipt.'; return null; }
      if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Invalid client measurement counter: ' + name);
      return value;
    };
    const configuredBytes = count(window.completed_flight_data_bytes_configured_ipc_compression, 'configured_ipc_flight_data_bytes');
    const uncompressedBytes = count(window.completed_flight_data_bytes_uncompressed, 'uncompressed_flight_data_bytes');
    const semantics = isRecord(terminal.bytes_semantics) ? terminal.bytes_semantics : {};
    if ((configuredBytes !== null && semantics.configured_ipc_compression_flight_data_bytes !==
        'sum_of_FlightData_protobuf_encoded_lengths_after_configured_Arrow_IPC_compression') ||
        (uncompressedBytes !== null && semantics.uncompressed_flight_data_bytes !==
        'sum_of_FlightData_protobuf_encoded_lengths_before_Arrow_IPC_compression')) {
      throw new Error('Client receipt has unknown FlightData byte semantics.');
    }
    const errors = Object.fromEntries(['rejected_streams', 'rejected_rows', 'retried_streams', 'retried_rows',
      'given_up_streams', 'given_up_rows', 'ambiguous_streams', 'ambiguous_rows', 'send_errors'].map(name => [name, count(window[name], name)]));
    const windows: Record<string, unknown>[] = [];
    let cursor = binding.start;
    let samples = 0;
    if (Array.isArray(window.windows) && window.windows.length > 0) {
      for (const raw of window.windows) {
        const entry = requireRecord(raw, 'Client latency subwindow');
        if (entry.start_unix_ms !== cursor || typeof entry.end_unix_ms !== 'number' ||
            !Number.isSafeInteger(entry.end_unix_ms) || entry.end_unix_ms <= cursor || entry.end_unix_ms > binding.end ||
            entry.send_latency_scope !== 'attempts_grouped_by_fresh_send_timestamp_in_half_open_window') {
          throw new Error('Client latency subwindows do not cover the global window with the declared attempt scope.');
        }
        const latency = requireRecord(entry.send_latency_us, 'Client send latency');
        const sampleCount = count(latency.samples, 'latency.samples');
        const quantiles = Object.fromEntries(['p50', 'p95', 'p99'].map(name => [name, count(latency[name], 'latency.' + name)]));
        samples += sampleCount ?? 0;
        if (!Number.isSafeInteger(samples)) throw new Error('Client latency sample count overflow.');
        windows.push({ start_unix_ms: cursor, end_unix_ms: entry.end_unix_ms, samples: sampleCount, ...quantiles,
          precision: latency.precision ?? null });
        cursor = entry.end_unix_ms;
      }
      if (cursor !== binding.end) throw new Error('Client latency subwindows do not finish at the global window end.');
    }
    const preparation = isRecord(terminal.preparation) ? terminal.preparation : {};
    measurement = {
      snapshot_scope: window.scope,
      completed_rows: binding.completedRows, completed_streams: count(window.completed_streams, 'completed_streams'),
      configured_ipc_flight_data_bytes: configuredBytes, uncompressed_flight_data_bytes: uncompressedBytes,
      byte_semantics: 'sum_of_FlightData_protobuf_encoded_lengths; excludes_gRPC_HTTP2_framing_and_NIC_bytes',
      errors_and_retries: errors,
      prepared_configured_live_slots: count(preparation.configured_live_slots, 'prepared_configured_live_slots'),
      prepared_admitted_live_slots: count(preparation.admitted_live_slots, 'prepared_admitted_live_slots'),
      send_latency_us: { scope: 'attempts_grouped_by_fresh_send_timestamp_in_half_open_window',
        samples: windows.length && !missing['latency.samples'] ? samples : null, p50: null, p95: null, p99: null,
        unavailable_reason: 'Only subwindow quantiles are recorded; aggregate percentiles cannot be reconstructed or averaged.',
        windows },
      unavailable: missing,
    };
  }
  return { ...common, measurement, measurement_unavailable_reason: unavailableReason };
}

function validateClientCount(clientCount: number): void {
  if (!Number.isSafeInteger(clientCount) || clientCount < 1 || clientCount > MAX_SCALED_CLIENTS) {
    throw new Error(`Scaled client count must be an integer from 1 to ${MAX_SCALED_CLIENTS}.`);
  }
}

function validateRunId(runId: string): string {
  if (!/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i.test(runId)) throw new Error('Scaled profile run ID must be a UUID.');
  return runId.replaceAll('-', '').toLowerCase();
}

function parseDocuments(source: string) {
  const documents = parseAllDocuments(source, { version: '1.1' });
  if (documents.some(document => document.errors.length > 0)) throw new Error('Scaled profile source manifest contains invalid YAML.');
  return documents;
}

function loadJobDocument(source: string) {
  const documents = parseDocuments(source);
  const matches = documents.filter(document => {
    const resource = document.toJSON();
    if (!isRecord(resource) || resource.kind !== 'Job') return false;
    const metadata = isRecord(resource.metadata) ? resource.metadata : {};
    return metadata.name === 'direct-load-confirmwide';
  });
  if (matches.length !== 1) throw new Error('Scaled profile requires exactly one direct-load-confirmwide Job.');
  return { documents, loadJob: matches[0]! };
}

function findGenerator(resource: Record<string, unknown>): { containerIndex: number; args: unknown[] } {
  const spec = requireRecord(resource.spec, 'Load Job spec');
  const template = requireRecord(spec.template, 'Load Job template');
  const podSpec = requireRecord(template.spec, 'Load pod spec');
  if (!Array.isArray(podSpec.containers)) throw new Error('Load Job has no containers.');
  const generators = podSpec.containers.flatMap((container, index) => {
    if (!isRecord(container) || container.name !== 'generator') return [];
    if (!Array.isArray(container.args)) throw new Error('Load generator args are invalid.');
    return [{ containerIndex: index, args: container.args }];
  });
  if (generators.length !== 1) throw new Error('Load Job must have exactly one generator container.');
  return generators[0]!;
}

export function scaledClientJobs(runId: string, clientCount: number): ScaledClientJob[] {
  validateClientCount(clientCount);
  const runHex = validateRunId(runId);
  return Array.from({ length: clientCount }, (_, offset) => {
    const index = offset + 1;
    const ordinal = String(index).padStart(2, '0');
    return {
      name: `direct-load-${runHex}-client-${ordinal}`,
      clientId: clientCount === 1 ? 'direct-e64-confirmwide' : `direct-e64-client-${String(index).padStart(4, '0')}`,
      index,
      podOrdinal: offset,
    };
  });
}

export function renderScaledCollector(manifest: string, runId: string, shape: CollectorShape): { manifest: string; poolName: string } {
  const poolName = 's' + validateRunId(runId).slice(0, 10);
  const hardware = collectorShapes[shape];
  if (!hardware) throw new Error('Unknown scaled collector shape.');
  const documents = parseDocuments(manifest);
  const writer = documents.filter(document => document.getIn(['kind']) === 'Deployment' && document.getIn(['metadata', 'name']) === 'arrowcol');
  const config = documents.filter(document => document.getIn(['kind']) === 'ConfigMap' && document.getIn(['metadata', 'name']) === 'arrowcol-config');
  if (writer.length !== 1 || config.length !== 1) throw new Error('Scaled collector requires exactly one writer and configuration.');
  const podPath = ['spec', 'template', 'spec'];
  for (const document of documents) {
    if (document.getIn([...podPath, 'nodeSelector', 'arrow-collector/role']) === 'arrowcol' ||
        document.getIn([...podPath, 'nodeSelector', 'kubernetes.azure.com/agentpool']) === 'benche64') {
      document.setIn([...podPath, 'nodeSelector', 'kubernetes.azure.com/agentpool'], poolName);
      const pod = requireRecord(requireRecord(requireRecord(requireRecord(document.toJSON(), 'Collector-stage resource').spec,
        'Collector-stage spec').template, 'Collector-stage template').spec, 'Collector-stage pod');
      const existing = pod.tolerations ?? [];
      if (!Array.isArray(existing)) throw new Error('Scaled collector-stage tolerations are invalid.');
      document.setIn([...podPath, 'tolerations'], [...existing,
        { key: runLabel, operator: 'Equal', value: runId, effect: 'NoSchedule' }]);
    }
  }
  const containerPath = [...podPath, 'containers', 0];
  if (writer[0]!.getIn([...containerPath, 'name']) !== 'dataflow') throw new Error('Scaled writer container shape changed.');
  writer[0]!.setIn([...podPath, 'nodeSelector', 'kubernetes.azure.com/agentpool'], poolName);
  writer[0]!.setIn(['spec', 'template', 'metadata', 'labels', runLabel], runId);
  writer[0]!.setIn(['spec', 'template', 'metadata', 'annotations', 'benchmark.arrowcol/tuning'], 'scaled-' + shape);
  writer[0]!.setIn([...containerPath, 'args'], ['--config', '/etc/oat/dataflow.yaml', '--num-cores', String(hardware.runtimeCores)]);
  writer[0]!.setIn([...containerPath, 'resources'], {
    requests: { cpu: String(hardware.runtimeCores), memory: hardware.memoryGiB + 'Gi' },
    limits: { cpu: String(hardware.runtimeCores), memory: hardware.memoryGiB + 'Gi' },
  });
  const dataflow = config[0]!.getIn(['data', 'dataflow.yaml']);
  if (typeof dataflow !== 'string') throw new Error('Scaled dataflow configuration is invalid.');
  const embedded = parseDocuments(dataflow);
  if (embedded.length !== 1) throw new Error('Scaled dataflow configuration must be a single YAML document.');
  embedded[0]!.setIn(['policies', 'resources', 'core_allocation', 'count'], hardware.runtimeCores);
  embedded[0]!.setIn(['groups', 'arrowcol', 'pipelines', 'ingest', 'nodes', 'flight', 'config', 'registration', 'rss_limit_bytes'],
    Math.floor(hardware.memoryGiB * 1024 ** 3 * 0.8));
  config[0]!.setIn(['data', 'dataflow.yaml'], embedded[0]!.toString());
  return { manifest: documents.map(document => document.toString()).join(''), poolName };
}

export function assertTrustedImageStatement(statement: unknown, digest: string, commit: string, imageRepository: string): void {
  const value = requireRecord(statement, 'Verified image statement');
  const subjects = Array.isArray(value.subject) ? value.subject : [];
  const predicate = requireRecord(value.predicate, 'Verified image predicate');
  const invocation = requireRecord(predicate.invocation, 'Verified build invocation');
  const source = requireRecord(invocation.configSource, 'Verified primary build source');
  const parameters = requireRecord(invocation.parameters, 'Verified build parameters');
  const builder = requireRecord(predicate.builder, 'Verified builder identity');
  const build = imageBuilds.find(([role]) => (role === 'client' ? clientRepository : repositories[role]) === imageRepository.split('/').at(-1));
  const materials = Array.isArray(predicate.materials) ? predicate.materials : [];
  if (value._type !== 'https://in-toto.io/Statement/v0.1' ||
      value.predicateType !== 'https://slsa.dev/provenance/v0.2' ||
      !build || builder.id !== 'urn:arrowcol:oat:publish-images:buildLocalImages:v1' ||
      source.uri !== 'git+https://github.com/mdrakiburrahman/rust-playground' ||
      !isRecord(source.digest) || source.digest.sha1 !== commit ||
      source.entryPoint !== 'projects/arrowcol-oat/publish-images.ts#buildLocalImages' ||
      parameters.revision !== commit || parameters.role !== build[0] || parameters.dockerfile !== build[1] || parameters.context !== build[2] ||
      !subjects.some(subject => isRecord(subject) && subject.name === imageRepository &&
        isRecord(subject.digest) && subject.digest.sha256 === digest.slice(7)) ||
      !materials.some(material => isRecord(material) &&
        material.uri === 'git+https://github.com/mdrakiburrahman/rust-playground' &&
        isRecord(material.digest) && material.digest.sha1 === commit)) {
    throw new Error('Trusted build provenance does not bind the image role/digest to its primary source, Dockerfile and builder.');
  }
}
export function renderScaledProfileManifest(source: string, runId: string, clientCount: number): string {
  const jobs = scaledClientJobs(runId, clientCount);
  const normalizedRunId = runId.toLowerCase();
  const { documents, loadJob } = loadJobDocument(source);
  const sourceResource = requireRecord(loadJob.toJSON(), 'Load Job');
  const generator = findGenerator(sourceResource);
  const clientIdIndexes = generator.args.flatMap((argument, index) => argument === '--client-id' ? [index + 1] : []);
  if (clientIdIndexes.length !== 1 || clientIdIndexes[0]! >= generator.args.length ||
      typeof generator.args[clientIdIndexes[0]!] !== 'string') {
    throw new Error('Load Job must contain exactly one --client-id value.');
  }
  const metadata = requireRecord(sourceResource.metadata, 'Load Job metadata');
  const jobLabels = requireRecord(metadata.labels, 'Load Job labels');
  const template = requireRecord(requireRecord(sourceResource.spec, 'Load Job spec').template, 'Load Job template');
  const templateMetadata = requireRecord(template.metadata, 'Load pod metadata');
  const podLabels = requireRecord(templateMetadata.labels, 'Load pod labels');
  const podSpec = requireRecord(template.spec, 'Load pod spec');
  if (typeof jobLabels[runLabel] !== 'string' || typeof podLabels[runLabel] !== 'string' ||
      jobLabels[runLabel] !== podLabels[runLabel] || jobLabels[runLabel] !== normalizedRunId) {
    throw new Error('Load Job ownership labels do not match the scaled run.');
  }
  const existingAffinity = podSpec.affinity;
  if (existingAffinity !== undefined && !isRecord(existingAffinity)) throw new Error('Load Job affinity is invalid.');
  const antiAffinity = existingAffinity && isRecord(existingAffinity.podAntiAffinity)
    ? existingAffinity.podAntiAffinity : {};
  const existingRules = antiAffinity.requiredDuringSchedulingIgnoredDuringExecution;
  if (existingRules !== undefined && !Array.isArray(existingRules)) throw new Error('Load Job required pod anti-affinity is invalid.');
  const affinity = {
    ...(existingAffinity ?? {}),
    podAntiAffinity: {
      ...antiAffinity,
      requiredDuringSchedulingIgnoredDuringExecution: [
        ...(existingRules ?? []),
        {
          labelSelector: { matchLabels: { app: 'arrowcol-oat-client', [runLabel]: normalizedRunId } },
          topologyKey: 'kubernetes.io/hostname',
        },
      ],
    },
  };

  const output: string[] = [];
  for (const document of documents) {
    if (document !== loadJob) {
      output.push(document.toString());
      continue;
    }
    for (const job of jobs) {
      const cloned = document.clone();
      const cloneResource = requireRecord(cloned.toJSON(), 'Cloned load Job');
      const cloneGenerator = findGenerator(cloneResource);
      cloned.setIn(['metadata', 'name'], job.name);
      cloned.setIn(['metadata', 'labels', runLabel], normalizedRunId);
      cloned.setIn(['spec', 'template', 'metadata', 'labels', runLabel], normalizedRunId);
      cloned.setIn(['metadata', 'labels', profileLabel], 'scaled');
      cloned.setIn(['metadata', 'labels', clientIndexLabel], String(job.index));
      cloned.setIn(['spec', 'template', 'metadata', 'labels', profileLabel], 'scaled');
      cloned.setIn(['spec', 'template', 'metadata', 'labels', clientIndexLabel], String(job.index));
      cloned.setIn(['spec', 'template', 'spec', 'containers', cloneGenerator.containerIndex, 'args', clientIdIndexes[0]!], job.clientId);
      const pinned = resolvedClientConfig(requireRecord(cloned.toJSON(), 'Cloned Job'));
      pinned['pod-ordinal'] = String(job.podOrdinal);
      if (clientCount > 1) pinned['schedule-timeout-secs'] = '900';
      cloned.setIn(['spec', 'template', 'spec', 'containers', cloneGenerator.containerIndex, 'args'],
        Object.entries(pinned).filter(([flag]) => flag !== 'pilot').flatMap(([flag, value]) => ['--' + flag, value]));
      cloned.setIn(['spec', 'template', 'spec', 'containers', cloneGenerator.containerIndex, 'env'],
        [{ name: 'ARROWCOL_CLIENT_PILOT', value: pinned.pilot }]);
      cloned.setIn(['spec', 'template', 'spec', 'affinity'], affinity);
      output.push(cloned.toString());
    }
  }
  const rendered = output.join('');
  effectiveWorkloadConfig(rendered);
  return rendered;
}

export function planScaledClientPool(settings: ScaledPoolSettings, nodeCount: number, clientCount: number): ScaledPoolPlan {
  validateClientCount(clientCount);
  if (settings.enableAutoScaling !== true || settings.minCount !== 0 || !Number.isSafeInteger(settings.maxCount) ||
      settings.maxCount < 1 || !Number.isSafeInteger(settings.count) || settings.count !== 0 || nodeCount !== 0) {
    throw new Error('Scaled client pool must be autoscaled min 0, configured and observed at zero nodes before it can be changed.');
  }
  return {
    pool: 'benchd64',
    clientCount,
    original: { ...settings },
    active: { enableAutoScaling: true, minCount: 0, maxCount: clientCount },
  };
}

export function planScaledClientPoolRestore(
  plan: ScaledPoolPlan,
  current: ScaledPoolSettings,
  nodeCount: number,
): Pick<ScaledPoolSettings, 'enableAutoScaling' | 'minCount' | 'maxCount'> {
  if (current.count !== 0 || nodeCount !== 0 || current.enableAutoScaling !== plan.active.enableAutoScaling ||
      current.minCount !== plan.active.minCount || current.maxCount !== plan.active.maxCount) {
    throw new Error('Scaled client pool changed or is not drained; refusing to overwrite its live settings.');
  }
  return {
    enableAutoScaling: plan.original.enableAutoScaling,
    minCount: plan.original.minCount,
    maxCount: plan.original.maxCount,
  };
}

export function allScaledClientsReady(expectedJobs: readonly string[], observations: readonly ScaledClientReadiness[]): boolean {
  if (expectedJobs.length < 1 || expectedJobs.length > MAX_SCALED_CLIENTS || new Set(expectedJobs).size !== expectedJobs.length) {
    throw new Error('Expected scaled client Job set is invalid.');
  }
  const expected = new Set(expectedJobs);
  const byName = new Map<string, ScaledClientReadiness>();
  for (const observation of observations) {
    if (!expected.has(observation.jobName) || byName.has(observation.jobName)) {
      throw new Error('Scaled client readiness contains an unexpected or duplicate Job.');
    }
    if (observation.failed) throw new Error(`Scaled client Job ${observation.jobName} failed before synchronized start.`);
    byName.set(observation.jobName, observation);
  }
  return expectedJobs.every(name => byName.get(name)?.ready === true);
}

function requireImageDigest(imageId: string, expectedDigest: string, label: string, attestedIndexDigest?: string): void {
  const allowed = [expectedDigest, ...(attestedIndexDigest ? [attestedIndexDigest] : [])];
  if (!imageId || allowed.some(digest => !/^sha256:[0-9a-f]{64}$/.test(digest)) ||
      !allowed.some(digest => imageId.endsWith('@' + digest))) {
    throw new Error(`${label} image ID does not match the candidate image digest.`);
  }
}

export function assertScaledRuntimeObservation(
  expectedJobs: readonly string[],
  observation: ScaledRuntimeObservation,
  clientDigest: string,
  collectorDigest: string,
  attestedIndexDigests?: { client: string; collector: string },
): void {
  if (!observation.collectorPodUid || !observation.collectorNodeName) throw new Error('Scaled runtime observation is missing collector pod identity or placement.');
  if (expectedJobs.length < 1 || expectedJobs.length > MAX_SCALED_CLIENTS || new Set(expectedJobs).size !== expectedJobs.length) {
    throw new Error('Expected scaled client Job set is invalid.');
  }
  requireImageDigest(observation.collectorImageId, collectorDigest, 'Collector', attestedIndexDigests?.collector);
  if (observation.clients.length !== expectedJobs.length) throw new Error('Scaled runtime observation is missing or duplicating client pods.');
  const seenJobs = new Set<string>();
  const seenPods = new Set<string>();
  const seenPodUids = new Set<string>();
  const seenNodes = new Set<string>();
  for (const client of observation.clients) {
    if (!expectedJobs.includes(client.jobName) || seenJobs.has(client.jobName) || !client.podName || !client.podUid || !client.nodeName ||
        seenPods.has(client.podName) || seenPodUids.has(client.podUid) || seenNodes.has(client.nodeName) || client.nodeName === observation.collectorNodeName) {
      throw new Error('Scaled client placement is missing, duplicated, or not on a distinct node.');
    }
    requireImageDigest(client.imageId, clientDigest, `Client ${client.jobName}`, attestedIndexDigests?.client);
    seenJobs.add(client.jobName);
    seenPods.add(client.podName);
    seenPodUids.add(client.podUid);
    seenNodes.add(client.nodeName);
  }
  if (seenJobs.size !== expectedJobs.length) throw new Error('Scaled runtime observation is missing a client Job.');
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}


export function applyCandidateImageManifest(
  renderedManifest: string,
  registry: string,
  candidateCommit: string,
  provenance: CandidateImageManifest,
): string {
  if (!/^[0-9a-f]{40}$/i.test(candidateCommit) || !registry || provenance.schemaVersion !== 1 ||
      provenance.sourceCommit.toLowerCase() !== candidateCommit.toLowerCase()) {
    throw new Error('Candidate image manifest does not match the requested source commit.');
  }
  if (!/^[0-9a-f]{64}$/.test(provenance.workloadConfigSha256) ||
      effectiveWorkloadConfig(renderedManifest).sha256 !== provenance.workloadConfigSha256) {
    throw new Error('Candidate image manifest does not bind the rendered workload configuration.');
  }
  const documents = parseDocuments(renderedManifest);
  const imageNodes = documents.flatMap(document => {
    const resource = document.toJSON();
    if (!isRecord(resource) || !['Pod', 'Job', 'Deployment', 'DaemonSet', 'StatefulSet'].includes(String(resource.kind))) return [];
    const path = resource.kind === 'Pod' ? ['spec'] : ['spec', 'template', 'spec'];
    const spec = requireRecord(resource.spec, 'Image resource spec');
    const pod = resource.kind === 'Pod' ? spec :
      requireRecord(requireRecord(spec.template, 'Image pod template').spec, 'Image pod spec');
    return ['containers', 'initContainers', 'ephemeralContainers'].flatMap(key => {
      const containers = pod[key];
      if (containers === undefined) return [];
      if (!Array.isArray(containers)) throw new Error('Image container list is invalid.');
      return containers.map((container, index) => {
        const value = requireRecord(container, 'Image container').image;
        if (typeof value !== 'string') throw new Error('Container image is invalid.');
        return { document, path: [...path, key, index, 'image'], value };
      });
    });
  });
  if (!imageNodes.length) throw new Error('Candidate manifest contains no workload images.');
  const sourceReferences = new Set<string>();
  const replacements = new Map<string, string>();
  const registryPrefix = escapeRegExp(registry);
  for (const { value: image } of imageNodes) {
    const match = new RegExp('^' + registryPrefix + '/([^/@]+)@(sha256:[0-9a-f]{64})$').exec(image);
    if (!match) throw new Error('Rendered benchmark image is outside the pinned stamped registry.');
    const sourceReference = match[1] + '@' + match[2];
    const candidateDigest = provenance.images[sourceReference];
    if (!candidateDigest || !/^sha256:[0-9a-f]{64}$/.test(candidateDigest)) {
      throw new Error('Candidate image provenance is missing a valid digest for ' + sourceReference + '.');
    }
    sourceReferences.add(sourceReference);
    replacements.set(image, registry + '/' + match[1] + '@' + candidateDigest);
  }
  const providedReferences = Object.keys(provenance.images).sort();
  const expectedReferences = [...sourceReferences].sort();
  if (providedReferences.length !== expectedReferences.length ||
      providedReferences.some((reference, index) => reference !== expectedReferences[index])) {
    throw new Error('Candidate image manifest must cover every exact source image reference in the rendered workload.');
  }
  for (const { document, path, value } of imageNodes) {
    document.setIn(path, replacements.get(value)!);
  }
  return documents.map(document => document.toString()).join('');
}

export function effectiveWorkloadConfig(manifest: string): EffectiveWorkloadConfig {
  const loadJobs = parseDocuments(manifest).flatMap(document => {
    const resource = document.toJSON();
    if (!isRecord(resource) || resource.kind !== 'Job') return [];
    const metadata = isRecord(resource.metadata) ? resource.metadata : {};
    const name = metadata.name;
    return name === 'direct-load-confirmwide' ||
      (typeof name === 'string' && /^direct-load-[0-9a-f]{32}-client-[0-9]{2}$/.test(name)) ? [resource] : [];
  });
  if (loadJobs.length < 1 || loadJobs.length > MAX_SCALED_CLIENTS) throw new Error('Effective workload must contain 1 to 8 load Jobs.');
  const clientIds = new Set<string>();
  const podOrdinals = new Set<number>();
  const dataIdentities: { clientId: string; podOrdinal: number }[] = [];
  let effectiveArgs: string[] | undefined;
  let performanceConfig: Record<string, string> | undefined;
  for (const loadJob of loadJobs) {
    const resolved = resolvedClientConfig(loadJob);
    const args = Object.entries(resolved).sort(([a], [b]) => a.localeCompare(b)).flatMap(([flag, value]) => ['--' + flag, value]);
    if (!args.every(value => typeof value === 'string') || args.length % 2 !== 0 ||
        args.some((value, index) => index % 2 === 0 && !(value as string).startsWith('--'))) {
      throw new Error('Effective workload generator arguments must be flag/value pairs.');
    }
    const clientIdFlags = args.flatMap((value, index) => value === '--client-id' ? [index] : []);
    if (clientIdFlags.length !== 1 || typeof args[clientIdFlags[0]! + 1] !== 'string' ||
        clientIds.has(args[clientIdFlags[0]! + 1] as string)) {
      throw new Error('Effective workload client IDs must be present and unique.');
    }
    clientIds.add(args[clientIdFlags[0]! + 1] as string);
    const podOrdinal = Number(resolved['pod-ordinal']);
    if (!Number.isSafeInteger(podOrdinal) || podOrdinal < 0 || podOrdinal > 0xffffffff || podOrdinals.has(podOrdinal)) {
      throw new Error('Effective workload pod ordinals must be valid and distinct.');
    }
    podOrdinals.add(podOrdinal);
    dataIdentities.push({ clientId: resolved['client-id']!, podOrdinal });
    const clientIdIndex = clientIdFlags[0]!;
    const argsWithoutClientId = args.filter((_value, index) => index !== clientIdIndex && index !== clientIdIndex + 1 &&
      args[index - index % 2] !== '--pod-ordinal');
    if (effectiveArgs && JSON.stringify(effectiveArgs) !== JSON.stringify(argsWithoutClientId)) {
      throw new Error('Scaled clients do not have equivalent workload arguments.');
    }
    effectiveArgs = argsWithoutClientId;
    performanceConfig = Object.fromEntries(Object.entries(resolved).filter(([key]) => !['client-id', 'pod-ordinal'].includes(key)).sort(([a], [b]) => a.localeCompare(b)));
  }
  const valueOf = (flag: string): string => {
    const indexes = effectiveArgs!.flatMap((value, index) => value === flag ? [index + 1] : []);
    if (indexes.length !== 1) throw new Error('Effective workload must specify ' + flag + ' exactly once.');
    return effectiveArgs![indexes[0]!]!;
  };
  if (valueOf('--warmup-secs') !== '60' || valueOf('--duration-secs') !== '600') {
    throw new Error('Scaled workload provenance requires a 60-second warmup and a shared 600-second window.');
  }
  dataIdentities.sort((left, right) => left.podOrdinal - right.podOrdinal || left.clientId.localeCompare(right.clientId));
  const historicalDataIdentityEligible = dataIdentities.length === 1 &&
    dataIdentities[0]!.clientId === 'direct-e64-confirmwide' && dataIdentities[0]!.podOrdinal === 0;
  const contract = {
    schemaVersion: 1 as const,
    profile: performanceConfig!['data-profile'] === 'diagnostic' && performanceConfig!['ipc-compression'] === 'none' &&
      performanceConfig!['grpc-send-compression'] === 'none' && performanceConfig!['grpc-accept-compression'] === 'none' &&
      historicalDataIdentityEligible
      ? 'diagnostic-replay' as const : 'non-parity' as const,
    dataProfile: performanceConfig!['data-profile'] as 'diagnostic' | 'realistic',
    ipcCompression: performanceConfig!['ipc-compression'] as 'none' | 'lz4_frame' | 'zstd',
    clientCount: loadJobs.length,
    generatorArgs: effectiveArgs!,
    performanceConfig: performanceConfig!,
    dataIdentities,
    historicalDataIdentityEligible,
  };
  return { ...contract, sha256: createHash('sha256').update(JSON.stringify(contract)).digest('hex') };
}
