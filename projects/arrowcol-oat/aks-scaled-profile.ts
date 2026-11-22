import { createHash } from 'node:crypto';
import { parseAllDocuments } from 'yaml';

const runLabel = 'arrowcol-oat/run-id';
const profileLabel = 'arrowcol-oat/client-profile';
const clientIndexLabel = 'arrowcol-oat/client-index';
export const MAX_SCALED_CLIENTS = 8;

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
  'retry-backoff-ms': '100', 'rate-limit-rps': '0',
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
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`${label} is invalid.`);
  return value;
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
      clientId: `direct-e64-${runHex}-client-${ordinal}`,
      index,
    };
  });
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
      pinned['pod-ordinal'] = String(job.index);
      cloned.setIn(['spec', 'template', 'spec', 'containers', cloneGenerator.containerIndex, 'args'],
        Object.entries(pinned).flatMap(([flag, value]) => ['--' + flag, value]));
      cloned.deleteIn(['spec', 'template', 'spec', 'containers', cloneGenerator.containerIndex, 'env']);
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

function requireImageDigest(imageId: string, expectedDigest: string, label: string): void {
  if (!imageId || !/^sha256:[0-9a-f]{64}$/.test(expectedDigest) || !imageId.endsWith('@' + expectedDigest)) {
    throw new Error(`${label} image ID does not match the candidate image digest.`);
  }
}

export function assertScaledRuntimeObservation(
  expectedJobs: readonly string[],
  observation: ScaledRuntimeObservation,
  clientDigest: string,
  collectorDigest: string,
): void {
  if (!observation.collectorPodUid || !observation.collectorNodeName) throw new Error('Scaled runtime observation is missing collector pod identity or placement.');
  if (expectedJobs.length < 1 || expectedJobs.length > MAX_SCALED_CLIENTS || new Set(expectedJobs).size !== expectedJobs.length) {
    throw new Error('Expected scaled client Job set is invalid.');
  }
  requireImageDigest(observation.collectorImageId, collectorDigest, 'Collector');
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
    requireImageDigest(client.imageId, clientDigest, `Client ${client.jobName}`);
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
  const contract = {
    schemaVersion: 1 as const,
    profile: performanceConfig!['data-profile'] === 'diagnostic' && performanceConfig!['ipc-compression'] === 'none'
      ? 'diagnostic-replay' as const : 'non-parity' as const,
    dataProfile: performanceConfig!['data-profile'] as 'diagnostic' | 'realistic',
    ipcCompression: performanceConfig!['ipc-compression'] as 'none' | 'lz4_frame' | 'zstd',
    clientCount: loadJobs.length,
    generatorArgs: effectiveArgs!,
    performanceConfig: performanceConfig!,
  };
  return { ...contract, sha256: createHash('sha256').update(JSON.stringify(contract)).digest('hex') };
}
