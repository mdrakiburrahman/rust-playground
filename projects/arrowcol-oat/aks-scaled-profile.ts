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
  profile: 'diagnostic-replay';
  dataProfile: 'diagnostic';
  ipcCompression: 'none';
  clientCount: number;
  generatorArgs: string[];
  sha256: string;
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
  const imageValues: string[] = [];
  const collectImages = (value: unknown): void => {
    if (Array.isArray(value)) { value.forEach(collectImages); return; }
    if (!isRecord(value)) return;
    for (const [key, child] of Object.entries(value)) {
      if (key === 'image' && typeof child === 'string') imageValues.push(child);
      else collectImages(child);
    }
  };
  for (const document of parseDocuments(renderedManifest)) collectImages(document.toJSON());
  const sourceReferences = new Set<string>();
  const replacements = new Map<string, string>();
  const registryPrefix = escapeRegExp(registry);
  for (const image of imageValues) {
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
  let result = renderedManifest;
  for (const [before, after] of replacements) result = result.replaceAll(before, after);
  return result;
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
  for (const loadJob of loadJobs) {
    const args = findGenerator(loadJob).args;
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
    const argsWithoutClientId = args.filter((_value, index) => index !== clientIdIndex && index !== clientIdIndex + 1) as string[];
    if (effectiveArgs && JSON.stringify(effectiveArgs) !== JSON.stringify(argsWithoutClientId)) {
      throw new Error('Scaled clients do not have equivalent workload arguments.');
    }
    effectiveArgs = argsWithoutClientId;
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
    profile: 'diagnostic-replay' as const,
    dataProfile: 'diagnostic' as const,
    ipcCompression: 'none' as const,
    clientCount: loadJobs.length,
    generatorArgs: effectiveArgs!,
  };
  return { ...contract, sha256: createHash('sha256').update(JSON.stringify(contract)).digest('hex') };
}
