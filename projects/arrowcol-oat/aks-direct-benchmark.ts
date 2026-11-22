#!/usr/bin/env -S npx --no-install tsx
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { isDeepStrictEqual, parseArgs } from 'node:util';
import { parseAllDocuments } from 'yaml';
import { Azure, Config, load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { capturedGeneration } from './aks-setup.js';
import { prepareSecrets, renderBootstrapEnv, writePrivateFile } from './runtime.js';
import { readDeltaAddWindow } from './aks-delta-preservation.js';
import { reconcileCollectorWindow } from './aks-collector-window.js';
import { assertProducedBuildStatement, verifyBuildEnvelope } from './aks-candidate-build.js';
import { applyCollectorPromotion, assertPromotionEnvironment, collectorCapacityReadback, collectorSettings,
  type CollectorContract, type CollectorContractRequest, type ProjectCollector } from './aks-collector-promotion.js';
import { createKubeletNodeResourceReader, startNodeObservation, type NodeObservationHandle, type NodeResourceObservation } from './aks-node-observation.js';
import {
  applyCandidateImageManifest, assertScaledRuntimeObservation, assertTrustedImageStatement, collectorShapes,
  effectiveWorkloadConfig, planScaledClientPool, planScaledClientPoolRestore, renderScaledCollector,
  renderScaledProfileManifest, scaledClientJobs, summarizeScaledClientWindow,
  type CandidateImageManifest, type CollectorShape, type ScaledPoolPlan, type ScaledPoolSettings,
  type ScaledRuntimeObservation,
} from './aks-scaled-profile.js';

const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
const namespacePrefix = 'arrow-collector-';
const ownerLabel = 'arrowcol-oat/owner-token';
const runLabel = 'arrowcol-oat/run-id';
const tablePath = 'telemetry/sqlserver/sqlserver_availability_replica_states';
const table = tablePath.replaceAll('/', '.');
const uuidPattern = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
const workloadPools = ['benche64', 'benchd64'] as const;
export interface BenchmarkDependencies {
  runTree?: typeof run_tree;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  readDelta?: typeof readDeltaAddWindow;
  generation?: typeof capturedGeneration;
  startObservation?: typeof startNodeObservation;
  projectCollector?: ProjectCollector;
}
const executionScope = new AsyncLocalStorage<BenchmarkDependencies>();
const now = () => executionScope.getStore()?.now?.() ?? Date.now();
const sleep = (ms: number) => executionScope.getStore()?.sleep?.(ms) ?? new Promise<void>(resolve => setTimeout(resolve, ms));
const invokeTree: typeof run_tree = (...args) => (executionScope.getStore()?.runTree ?? run_tree)(...args);

const stageScope = new AsyncLocalStorage<{ name: string; deadline: number; timeoutMs: number }>();
const manifestStages = ['identity', 'sql', 'migration', 'catalog', 'writer', 'initializer', 'registration', 'load'] as const;

function boundedTimeoutSeconds(timeout: number): number {
  const stage = stageScope.getStore();
  if (!stage) return timeout;
  const remaining = Math.ceil((stage.deadline - now()) / 1000);
  if (remaining < 1) throw new Error(stage.name + ' stage timed out after ' + (stage.timeoutMs / 1000) + ' seconds.');
  return Math.min(timeout, remaining);
}

const stageAwareRunTree: typeof run_tree = (args, timeout, cwd, env, onStderr, maxOutputBytes, signal) =>
  invokeTree(args, boundedTimeoutSeconds(timeout), cwd, env, onStderr, maxOutputBytes, signal);

async function runStage<T>(name: string, timeoutMs: number, action: () => Promise<T>): Promise<T> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new Error(name + ' stage timeout is invalid.');
  const stage = { name, timeoutMs, deadline: now() + timeoutMs };
  return stageScope.run(stage, async () => {
    try {
      const result = await action();
      if (now() >= stage.deadline) throw new Error('stage deadline elapsed.');
      return result;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      const timedOut = now() >= stage.deadline || /timed out|deadline elapsed/i.test(detail);
      throw new Error(name + ' stage ' + (timedOut ? 'timed out' : 'failed') + ' after ' + (timeoutMs / 1000) + ' seconds' + (detail ? ': ' + detail : ''), { cause: error });
    }
  });
}

type JsonObject = Record<string, unknown>;

function isRecord(value: unknown): value is JsonObject {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requireRecord(value: unknown, label: string): JsonObject {
  if (!isRecord(value)) throw new Error(`${label} is not a JSON object.`);
  return value;
}

function shellQuote(value: string): string {
  return "'" + value.replaceAll("'", "'\\''") + "'";
}

export function replaceInitializerExpiry(rendered: string, expiry: number): string {
  const expiryField = /("expires_at_unix_ms"\s*:\s*)9876543210000(?=\s*[,}])/g;
  const matches = rendered.match(expiryField) ?? [];
  if (matches.length !== 1) throw new Error('Initializer expiry placeholder count is not exactly one.');
  return rendered.replace(expiryField, (_match, prefix: string) => `${prefix}${expiry}`);
}

function checkResult(result: Awaited<ReturnType<typeof run_tree>>, label: string): string {
  if (result.returncode !== 0) {
    const detail = result.stderr.trim().replace(/[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '[redacted]').slice(0, 1000);
    throw new Error(`${label} failed (exit ${result.returncode})${detail ? `: ${detail}` : ''}`);
  }
  return result.stdout.trim();
}

async function command(args: string[], label: string, timeout = 120, signal?: AbortSignal): Promise<string> {
  return checkResult(await invokeTree(args, boundedTimeoutSeconds(timeout), ROOT, undefined, undefined, undefined, signal), label);
}

function parseJson(text: string, label: string): unknown {
  try { return JSON.parse(text); }
  catch { throw new Error(`${label} returned invalid JSON.`); }
}

export function isSchemaFingerprint(value: unknown): value is string {
  return typeof value === 'string' && /^sha256:[0-9a-f]{64}$/.test(value);
}

function argNumber(value: unknown, label: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error(`${label} is invalid.`);
  return value;
}

async function kubectl(config: Config, args: string[], label: string, timeout = 120, signal?: AbortSignal): Promise<string> {
  return command(['kubectl', '--context', config.cluster, ...args], label, timeout, signal);
}

async function getObject(config: Config, resource: string, namespace: string, name: string, signal?: AbortSignal): Promise<JsonObject | undefined> {
  const output = await kubectl(config, ['get', resource, name, '-n', namespace, '-o', 'json', '--ignore-not-found'],
    `Read ${resource}/${name}`, 120, signal);
  if (!output) return undefined;
  return requireRecord(parseJson(output, `${resource}/${name}`), `${resource}/${name}`);
}

async function waitUntil<T>(label: string, timeoutMs: number, predicate: () => Promise<T | undefined>, intervalMs = 5000): Promise<T> {
  const localEnd = now() + timeoutMs;
  const stage = stageScope.getStore();
  const end = Math.min(localEnd, stage?.deadline ?? Number.POSITIVE_INFINITY);
  while (now() < end) {
    const value = await predicate();
    if (value !== undefined) return value;
    await sleep(Math.min(intervalMs, Math.max(0, end - now())));
  }
  if (stage && stage.deadline <= localEnd) throw new Error(stage.name + ' stage timed out after ' + (stage.timeoutMs / 1000) + ' seconds.');
  throw new Error(label + ' timed out.');
}

function namespaceMatches(resource: JsonObject, runId: string, owner: string): boolean {
  const metadata = isRecord(resource.metadata) ? resource.metadata : {};
  const labels = isRecord(metadata.labels) ? metadata.labels : {};
  const annotations = isRecord(metadata.annotations) ? metadata.annotations : {};
  return labels[runLabel] === runId && labels[ownerLabel] === owner &&
    annotations[runLabel] === runId && annotations[ownerLabel] === owner;
}

async function readNamespace(config: Config, namespace: string): Promise<JsonObject | undefined> {
  const output = await kubectl(config, ['get', 'namespace', namespace, '-o', 'json', '--ignore-not-found'], `Read namespace ${namespace}`);
  if (!output) return undefined;
  return requireRecord(parseJson(output, `namespace ${namespace}`), `namespace ${namespace}`);
}

async function verifyNamespace(config: Config, namespace: string, runId: string, owner: string, uid?: string): Promise<string> {
  const current = await readNamespace(config, namespace);
  if (!current || !namespaceMatches(current, runId, owner)) throw new Error('The benchmark namespace is absent or no longer carries this run ownership.');
  const metadata = requireRecord(current.metadata, 'Namespace metadata');
  if (typeof metadata.uid !== 'string' || (uid !== undefined && metadata.uid !== uid)) {
    throw new Error('The benchmark namespace UID changed; refusing to continue.');
  }
  return metadata.uid;
}

async function configureKubeAuth(config: Config): Promise<void> {
  await command(['az', 'aks', 'get-credentials', '--subscription', config.subscription, '--resource-group', config.group,
    '--name', config.cluster, '--context', config.cluster, '--overwrite-existing', '--only-show-errors'],
  'Fetch the selected AKS context', 180);
  const help = await command(['kubelogin', 'get-token', '--help'], 'Verify kubelogin environment-override support', 30);
  if (!help.includes('--disable-environment-override')) throw new Error('Installed kubelogin cannot safely configure Azure CLI authentication.');
  await command(['kubelogin', 'convert-kubeconfig', '--context', config.cluster, '-l', 'azurecli',
    '--subscription', config.subscription, '--disable-environment-override'], 'Configure kubelogin for the selected AKS context', 60);
}
async function poolNodeCount(config: Config, pool: string): Promise<number> {
  const output = await kubectl(config, ['get', 'nodes', '-l', `kubernetes.azure.com/agentpool=${pool}`, '-o', 'json'], `Read ${pool} node count`);
  const value = requireRecord(parseJson(output, `${pool} nodes`), `${pool} nodes`);
  if (!Array.isArray(value.items)) throw new Error(`${pool} node inventory is invalid.`);
  return value.items.length;
}

async function readPool(config: Config, pool: string, includeShape = false): Promise<JsonObject> {
  const output = await command(['az', 'aks', 'nodepool', 'show', '--subscription', config.subscription,
    '--resource-group', config.group, '--cluster-name', config.cluster, '--name', pool,
    '--query', includeShape
      ? '{enableAutoScaling:enableAutoScaling,minCount:minCount,maxCount:maxCount,count:count,vmSize:vmSize}'
      : '{enableAutoScaling:enableAutoScaling,minCount:minCount,maxCount:maxCount,count:count}',
    '--output', 'json', '--only-show-errors'], `Read ${pool} node-pool configuration`);
  return requireRecord(parseJson(output, `${pool} node-pool configuration`), `${pool} node-pool configuration`);
}
async function requirePoolsAtZero(config: Config): Promise<void> {
  for (const pool of workloadPools) {
    const configuration = await readPool(config, pool);
    if (configuration.enableAutoScaling !== true || configuration.minCount !== 0 || configuration.maxCount !== 1) {
      throw new Error(`Workload pool ${pool} must retain autoscaling min 0/max 1; refusing to change shared pool configuration.`);
    }
    if (await poolNodeCount(config, pool) !== 0) throw new Error(`Workload pool ${pool} is not at zero nodes; refusing to start a benchmark.`);
  }
}

export function poolBlockingPods(items: unknown[]): string[] {
  return items.flatMap(rawPod => {
    if (!isRecord(rawPod)) return ['<invalid pod>'];
    const metadata = isRecord(rawPod.metadata) ? rawPod.metadata : {};
    const owners = Array.isArray(metadata.ownerReferences) ? metadata.ownerReferences : [];
    if (owners.some(owner => isRecord(owner) && owner.kind === 'DaemonSet')) return [];
    const status = isRecord(rawPod.status) ? rawPod.status : {};
    const namespace = typeof metadata.namespace === 'string' ? metadata.namespace : '<unknown namespace>';
    const name = typeof metadata.name === 'string' ? metadata.name : '<unknown pod>';
    const phase = typeof status.phase === 'string' ? status.phase : '<unknown phase>';
    return [`${namespace}/${name} (${phase})`];
  });
}

export function poolCleanupDecision(nodes: number, count: unknown, blockers: string[]): 'drained' | 'wait' | 'scale' {
  if (nodes === 0 && count === 0) return 'drained';
  return blockers.length ? 'wait' : 'scale';
}

async function readWorkloadPodsOnPool(config: Config, pool: string): Promise<string[]> {
  const nodeOutput = await kubectl(config, ['get', 'nodes', '-l', `kubernetes.azure.com/agentpool=${pool}`, '-o', 'json'],
    `Read ${pool} nodes before scale-down`);
  const nodeList = requireRecord(parseJson(nodeOutput, `${pool} nodes`), `${pool} nodes`);
  if (!Array.isArray(nodeList.items)) throw new Error(`${pool} node inventory is invalid.`);
  const blockers: string[] = [];
  for (const rawNode of nodeList.items) {
    const node = requireRecord(rawNode, `${pool} node`);
    const metadata = requireRecord(node.metadata, `${pool} node metadata`);
    if (typeof metadata.name !== 'string') throw new Error(`${pool} node name is invalid.`);
    const podOutput = await kubectl(config, ['get', 'pods', '--all-namespaces', '--field-selector',
      `spec.nodeName=${metadata.name}`, '-o', 'json'], `Read pods on ${pool} node`);
    const podList = requireRecord(parseJson(podOutput, `${pool} pods`), `${pool} pods`);
    if (!Array.isArray(podList.items)) throw new Error(`${pool} pod inventory is invalid.`);
    blockers.push(...poolBlockingPods(podList.items));
  }
  return blockers;
}

async function assertNoWorkloadPodsOnPool(config: Config, pool: string): Promise<void> {
  const blockers = await readWorkloadPodsOnPool(config, pool);
  if (blockers.length) throw new Error(`Non-DaemonSet pods remain on ${pool}; refusing to scale down: ${blockers.join(', ')}.`);
}

async function scalePoolToZero(config: Config, pool: string, maxCount = 1): Promise<void> {
  let blockers: string[] = [];
  const label = `${pool} safe scale-down`;
  let decision: 'drained' | 'scale';
  try {
    decision = await waitUntil(label, 25 * 60_000, async () => {
      const current = await readPool(config, pool);
      if (current.enableAutoScaling !== true || current.minCount !== 0 || current.maxCount !== maxCount) {
        throw new Error(`Workload pool ${pool} must retain autoscaling min 0/max ${maxCount}; refusing to change shared pool configuration.`);
      }
      const nodes = await poolNodeCount(config, pool);
      blockers = nodes === 0 ? [] : await readWorkloadPodsOnPool(config, pool);
      const state = poolCleanupDecision(nodes, current.count, blockers);
      return state === 'wait' ? undefined : state;
    }, 30_000);
  } catch (error) {
    if (error instanceof Error && error.message === `${label} timed out.` && blockers.length) {
      throw new Error(`Non-DaemonSet pods remain on ${pool}; refusing to scale down: ${blockers.join(', ')}.`, { cause: error });
    }
    throw error;
  }
  if (decision === 'drained') return;
  await assertNoWorkloadPodsOnPool(config, pool);
  let scaleError: Error | undefined;
  try {
    await command(['az', 'aks', 'nodepool', 'update', '--subscription', config.subscription,
      '--resource-group', config.group, '--cluster-name', config.cluster, '--name', pool,
      '--disable-cluster-autoscaler', '--only-show-errors', '--output', 'none'],
    `Temporarily disable ${pool} autoscaling`, 600);
    await assertNoWorkloadPodsOnPool(config, pool);
    await command(['az', 'aks', 'nodepool', 'scale', '--subscription', config.subscription,
      '--resource-group', config.group, '--cluster-name', config.cluster, '--name', pool,
      '--node-count', '0', '--only-show-errors', '--output', 'none'], `Scale ${pool} to zero`, 600);
  } catch (error) {
    scaleError = error instanceof Error ? error : new Error(String(error));
  }

  let restoreError: Error | undefined;
  try {
    await command(['az', 'aks', 'nodepool', 'update', '--subscription', config.subscription,
      '--resource-group', config.group, '--cluster-name', config.cluster, '--name', pool,
      '--enable-cluster-autoscaler', '--min-count', '0', '--max-count', String(maxCount),
      '--only-show-errors', '--output', 'none'], `Restore ${pool} autoscaling min 0/max ${maxCount}`, 600);
  } catch (error) {
    restoreError = error instanceof Error ? error : new Error(String(error));
  }

  if (scaleError && restoreError) throw new AggregateError([scaleError, restoreError], `${pool} scale-down and autoscaler restoration both failed.`);
  if (restoreError) throw restoreError;
  if (scaleError) throw scaleError;
}

export function errorMessages(error: Error): string[] {
  const messages: string[] = [];
  const seen = new Set<Error>();
  const visit = (item: Error): void => {
    if (seen.has(item)) return;
    seen.add(item);
    if (item.message) messages.push(item.message);
    if (item instanceof AggregateError) {
      for (const child of item.errors) if (child instanceof Error) visit(child);
    }
    if (item.cause instanceof Error) visit(item.cause);
  };
  visit(error);
  return messages;
}

async function waitPoolsAtZero(config: Config): Promise<void> {
  const errors: Error[] = [];
  for (const pool of workloadPools) {
    try {
      await runStage('restore ' + pool + ' workload pool', 35 * 60_000, async () => {
        await scalePoolToZero(config, pool);
        await waitUntil(pool + ' node pool scale-down', 30 * 60_000, async () => {
          const nodes = await poolNodeCount(config, pool);
          const configuration = await readPool(config, pool);
          return nodes === 0 && configuration.count === 0 && configuration.enableAutoScaling === true &&
            configuration.minCount === 0 && configuration.maxCount === 1 ? true : undefined;
        }, 30_000);
      });
    } catch (error) {
      errors.push(error instanceof Error ? error : new Error(String(error)));
    }
  }
  if (errors.length) throw new AggregateError(errors, 'One or more workload pools could not be restored to zero.');
}

async function waitDeployment(config: Config, namespace: string, name: string, timeoutMs = 10 * 60_000): Promise<void> {
  await waitUntil(`deployment/${name}`, timeoutMs, async () => {
    const deployment = await getObject(config, 'deployment', namespace, name);
    if (!deployment) return undefined;
    const status = isRecord(deployment.status) ? deployment.status : {};
    const ready = typeof status.availableReplicas === 'number' ? status.availableReplicas : 0;
    return ready > 0 ? true : undefined;
  });
}

async function waitJob(config: Config, namespace: string, name: string, timeoutMs: number, diagnosticsDirectory?: string): Promise<void> {
  await waitUntil(`job/${name}`, timeoutMs, async () => {
    const job = await getObject(config, 'job', namespace, name);
    if (!job) return undefined;
    const status = isRecord(job.status) ? job.status : {};
    if ((typeof status.failed === 'number' && status.failed > 0) ||
        (Array.isArray(status.conditions) && status.conditions.some(condition => isRecord(condition) &&
          condition.type === 'Failed' && condition.status === 'True'))) {
      if (diagnosticsDirectory) {
        const logPath = join(diagnosticsDirectory, `${name}.failure.log`);
        let logsCaptured = false;
        try {
          const logs = await kubectl(config, ['logs', `job/${name}`, '-n', namespace, '--all-containers=true'], `Capture failed job/${name} logs`, 60);
          await writePrivateFile(logPath, logs);
          logsCaptured = true;
        } catch (error) {
          try {
            await writePrivateFile(logPath, 'The job failed, but Kubernetes logs were unavailable at capture time.\n');
          } catch (writeError) {
            throw new AggregateError([error, writeError], `job/${name} failed and private diagnostics could not be saved.`);
          }
        }
        throw new Error(`job/${name} failed; private ${logsCaptured ? 'logs' : 'log-capture receipt'} saved at ${logPath}.`);
      }
      throw new Error(`job/${name} failed.`);
    }
    return status.succeeded === 1 ? true : undefined;
  }, 5000);
}

interface WriterIdentity { uid: string; restartCounts: number[] }

async function readWriter(config: Config, namespace: string): Promise<WriterIdentity | undefined> {
  const output = await kubectl(config, ['get', 'pods', '-n', namespace, '-l', 'app=arrowcol', '-o', 'json'], 'Read writer pod');
  const list = requireRecord(parseJson(output, 'Writer pod list'), 'Writer pod list');
  if (!Array.isArray(list.items) || list.items.length !== 1) return undefined;
  const pod = requireRecord(list.items[0], 'Writer pod');
  const metadata = requireRecord(pod.metadata, 'Writer pod metadata');
  const status = requireRecord(pod.status, 'Writer pod status');
  if (status.phase !== 'Running' || !Array.isArray(status.containerStatuses)) return undefined;
  const containers = status.containerStatuses.map(value => requireRecord(value, 'Writer container status'));
  if (!containers.length || containers.some(container => container.ready !== true)) return undefined;
  if (typeof metadata.uid !== 'string') throw new Error('Writer pod UID is missing.');
  return { uid: metadata.uid, restartCounts: containers.map(container => argNumber(container.restartCount, 'Writer restart count')) };
}

async function waitWriter(config: Config, namespace: string): Promise<WriterIdentity> {
  return waitUntil('writer readiness', 10 * 60_000, () => readWriter(config, namespace));
}

async function assertWriterStable(config: Config, namespace: string, expected: WriterIdentity): Promise<void> {
  const actual = await readWriter(config, namespace);
  if (!actual || actual.uid !== expected.uid || actual.restartCounts.length !== expected.restartCounts.length ||
      actual.restartCounts.some((count, index) => count !== expected.restartCounts[index])) {
    throw new Error('Writer pod UID, readiness, or restart count changed during the benchmark.');
  }
}

async function applyStage(config: Config, namespace: string, manifest: string, stage: string): Promise<void> {
  await kubectl(config, ['apply', '--server-side', '-n', namespace, '-f', manifest, '-l', `benchmark-stage=${stage}`], `Apply ${stage} stage`);
  console.log(`Applied ${stage} stage.`);
}

async function createSecret(config: Config, namespace: string, name: string, keys: Record<string, string>): Promise<void> {
  const args = ['kubectl', '--context', config.cluster, 'create', 'secret', 'generic', name, '-n', namespace, '--dry-run=client', '-o', 'yaml'];
  for (const [key, filename] of Object.entries(keys)) args.push(`--from-file=${key}=${filename}`);
  const pipeline = `${args.map(shellQuote).join(' ')} | ${['kubectl', '--context', config.cluster, 'apply', '-f', '-'].map(shellQuote).join(' ')}`;
  await command(['bash', '-o', 'pipefail', '-c', pipeline], `Create namespace Secret ${name}`);
}

async function createSecrets(config: Config, namespace: string, secretsPath: string): Promise<void> {
  await createSecret(config, namespace, 'arrowcol-generation', { generation: resolve(secretsPath, 'generation') });
  await createSecret(config, namespace, 'arrowcol-router-auth', {
    'edge-hmac': resolve(secretsPath, 'edge-hmac'), 'internal-hmac': resolve(secretsPath, 'internal-hmac'),
  });
  await createSecret(config, namespace, 'arrowcol-sql-admin', { password: resolve(secretsPath, 'sa-password') });
  await createSecret(config, namespace, 'arrowcol-sql-runtime', { password: resolve(secretsPath, 'app-password') });
  await createSecret(config, namespace, 'arrowcol-sql-migration', { password: resolve(secretsPath, 'migration-password') });
}

async function runFic(commandName: 'prepare-run-fic' | 'probe-run-fic' | 'cleanup-run-fic', runId: string,
  namespace: string, record: string, envFile: string, stampConfig: string): Promise<void> {
  const args = ['npx', '--no-install', 'tsx', 'infra/azure/bootstrap.ts', commandName,
    '--mode', 'stamped', '--env-file', envFile, '--stamp-config', stampConfig,
    '--run-id', runId, '--record', record];
  if (commandName !== 'cleanup-run-fic') args.push('--namespace', namespace, '--service-account', 'arrow-collector');
  if (commandName === 'probe-run-fic') args.push('--wait-seconds', '300');
  await command(args, commandName, commandName === 'probe-run-fic' ? 360 : 900);
}

async function checkDeltaRoot(config: Config, runId: string): Promise<string> {
  const relativePath = `direct-baseline-1/runs/${runId}`;
  const output = await command(['az', 'storage', 'fs', 'directory', 'exists', '--account-name', config.account,
    '--file-system', config.data, '--name', relativePath, '--auth-mode', 'login', '--subscription', config.subscription,
    '--output', 'json', '--only-show-errors'], 'Check run-scoped Delta root');
  const result = requireRecord(parseJson(output, 'Delta-root existence check'), 'Delta-root existence check');
  if (result.exists !== false) throw new Error('The generated run-scoped Delta root already exists; no existing data will be reused or deleted.');
  return `${relativePath}/dataflow`;
}

export function renderManifest(source: string, values: Record<string, string>): string {
  let rendered = source;
  for (const [key, value] of Object.entries(values)) rendered = rendered.replaceAll(key, value);
  if (/BENCH[A-Z0-9_]+/.test(rendered)) throw new Error('The benchmark manifest still contains an unresolved runtime placeholder.');
  return rendered;
}

export function assertRenderedManifest(rendered: string, runId: string, owner: string, registry: string, clientCount = 1): void {
  if (!uuidPattern.test(runId) || !/^[0-9a-f]{48}$/.test(owner) || !registry) throw new Error('Rendered manifest inputs are invalid.');
  const expectedNamespace = namespacePrefix + runId.replaceAll('-', '');
  const expirySentinels = rendered.match(/"expires_at_unix_ms"\s*:\s*9876543210000(?=\s*[,}])/g) ?? [];
  if (expirySentinels.length !== 1) throw new Error('Rendered manifest must contain exactly one initializer expiry sentinel.');
  const foundStages = new Set<string>();
  const images: string[] = [];
  let namespaceCount = 0;
  let loadDurationCount = 0;
  const collectImages = (value: unknown): void => {
    if (Array.isArray(value)) { value.forEach(collectImages); return; }
    if (!isRecord(value)) return;
    for (const [key, child] of Object.entries(value)) {
      if (key === 'image' && typeof child === 'string') images.push(child);
      else collectImages(child);
    }
  };
  for (const document of parseAllDocuments(rendered, { version: '1.1' })) {
    if (document.errors.length) throw new Error('Rendered manifest contains invalid YAML.');
    const value = document.toJSON();
    if (value === null || value === undefined) continue;
    const resource = requireRecord(value, 'Rendered Kubernetes resource');
    const metadata = requireRecord(resource.metadata, 'Rendered resource metadata');
    const labels = requireRecord(metadata.labels, 'Rendered resource labels');
    const stage = labels['benchmark-stage'];
    if (typeof stage !== 'string' || !manifestStages.includes(stage as typeof manifestStages[number]))
      throw new Error('Every rendered resource must have a recognized benchmark-stage label.');
    foundStages.add(stage);
    collectImages(resource);
    if (resource.kind === 'Namespace') {
      namespaceCount++;
      const annotations = requireRecord(metadata.annotations, 'Rendered namespace annotations');
      if (metadata.name !== expectedNamespace || labels[runLabel] !== runId || labels[ownerLabel] !== owner ||
          annotations[runLabel] !== runId || annotations[ownerLabel] !== owner) {
        throw new Error('Rendered namespace ownership markers do not match the fake run inputs.');
      }
    }
    if (resource.kind === 'Job' && (metadata.name === 'direct-load-confirmwide' ||
        scaledClientJobs(runId, clientCount).some(job => job.name === metadata.name))) {
      const spec = requireRecord(resource.spec, 'Rendered load Job spec');
      const template = requireRecord(spec.template, 'Rendered load Job template');
      const podSpec = requireRecord(template.spec, 'Rendered load pod spec');
      if (!Array.isArray(podSpec.containers)) throw new Error('Rendered load Job has no containers.');
      const durations = podSpec.containers.flatMap(container => {
        const args = requireRecord(container, 'Rendered load container').args;
        if (!Array.isArray(args)) return [];
        return args.flatMap((arg, index) => arg === '--duration-secs' ? [args[index + 1]] : []);
      });
      if (durations.length !== 1 || !['60', '600'].includes(String(durations[0])))
        throw new Error('Rendered load duration must be the 60-second smoke or 600-second full window.');
      loadDurationCount++;
    }
  }
  if (namespaceCount !== 1 || loadDurationCount !== clientCount || images.length === 0 ||
      manifestStages.some(stage => !foundStages.has(stage)) || foundStages.size !== manifestStages.length) {
    throw new Error('Rendered manifest is missing a namespace, load duration, or expected stage.');
  }
  if (images.some(image => !image.startsWith(registry + '/') || !/@sha256:[0-9a-f]{64}$/.test(image)))
    throw new Error('Every rendered image must use the stamped registry and a pinned SHA-256 digest.');
}

async function prepareNamespace(config: Config, namespace: string, runId: string, owner: string): Promise<string> {
  return waitUntil('owned namespace creation', 60_000, async () => {
    const resource = await readNamespace(config, namespace);
    if (!resource) return undefined;
    if (!namespaceMatches(resource, runId, owner)) throw new Error('Namespace creation did not preserve the run and owner markers.');
    return verifyNamespace(config, namespace, runId, owner);
  });
}

async function waitForReadyLoad(config: Config, namespace: string, writer: WriterIdentity): Promise<void> {
  await waitUntil('load client readiness', 7 * 60_000, async () => {
    await assertWriterStable(config, namespace, writer);
    const job = await getObject(config, 'job', namespace, 'direct-load-confirmwide');
    const status = job && isRecord(job.status) ? job.status : {};
    if ((typeof status.failed === 'number' && status.failed > 0) ||
        (Array.isArray(status.conditions) && status.conditions.some(condition => isRecord(condition) && condition.type === 'Failed' && condition.status === 'True'))) {
      throw new Error('The load Job failed before OAT_CLIENT_READY.');
    }
    try {
      const output = await kubectl(config, ['logs', 'job/direct-load-confirmwide', '-n', namespace, '--all-containers=true'],
        'Read load client readiness', 60);
      return output.includes('OAT_CLIENT_READY') ? true : undefined;
    } catch { return undefined; }
  }, 5000);
}

async function waitForLoad(config: Config, namespace: string, writer: WriterIdentity, timeoutMs: number): Promise<void> {
  const deadline = now() + timeoutMs;
  while (now() < deadline) {
    await assertWriterStable(config, namespace, writer);
    const job = await getObject(config, 'job', namespace, 'direct-load-confirmwide');
    if (job) {
      const status = isRecord(job.status) ? job.status : {};
      if ((typeof status.failed === 'number' && status.failed > 0) ||
          (Array.isArray(status.conditions) && status.conditions.some(condition => isRecord(condition) &&
            condition.type === 'Failed' && condition.status === 'True'))) throw new Error('The timed load Job failed.');
      if (status.succeeded === 1) return;
    }
    await sleep(10_000);
  }
  throw new Error('The timed load Job exceeded its ' + Math.ceil(timeoutMs / 60_000) + '-minute safety deadline.');
}

async function verifyRegistration(config: Config, namespace: string, runId: string, deltaRoot: string): Promise<void> {
  const output = await kubectl(config, ['logs', 'job/direct-catalog-verified', '-n', namespace, '--all-containers=true'],
    'Read signed catalog registration receipt');
  const receipts = output.split(/\r?\n/).flatMap(line => {
    try {
      const value: unknown = JSON.parse(line);
      return isRecord(value) && value.kind === 'oat_run_table_registered' ? [value] : [];
    } catch { return []; }
  });
  if (receipts.length !== 1) throw new Error('The registration Job did not emit exactly one signed one-table receipt.');
  const receipt = receipts[0]!;
  const expectedLocation = `${deltaRoot}/${tablePath}`;
  if (receipt.run_id !== runId || receipt.table !== table || receipt.location !== expectedLocation ||
      receipt.provider !== 'delta' || receipt.schema_version !== 1 || receipt.catalog_table_count !== 1 ||
      !isSchemaFingerprint(receipt.schema_fingerprint)) {
    throw new Error('The signed registration receipt does not prove the exact run-scoped table and schema.');
  }
}

async function deleteOwnedNamespace(config: Config, namespace: string, runId: string, owner: string, uid: string): Promise<void> {
  const current = await readNamespace(config, namespace);
  if (!current) return;
  await verifyNamespace(config, namespace, runId, owner, uid);
  await kubectl(config, ['delete', 'namespace', namespace, '--wait=false'], 'Request deletion of owned benchmark namespace');
  await waitUntil('owned namespace deletion', 10 * 60_000, async () =>
    await readNamespace(config, namespace) ? undefined : true, 10_000);
}

interface BenchmarkReport {
  schema_version: 1;
  run_id: string;
  tested_at_utc: string;
  delta_root: string;
  runbook_sha256: string;
  manifest_sha256: string;
  measurement_start_unix_ms: number;
  measurement_end_unix_ms: number;
  measurement_seconds: number;
  committed_rows: number;
  committed_rows_per_second: number;
  committed_files: number;
  committed_bytes: number;
  committed_delta_commits: number;
  first_delta_version: number | null;
  last_delta_version: number | null;
  final_delta_version: number;
  active_rows: number;
  writer_pod_uid: string;
  writer_restart_counts: number[];
  namespace_removed: boolean;
  run_fic_removed: boolean;
  workload_pools_at_zero: boolean;
  node_observation?: NodeResourceObservation;
  scaled_profile?: {
    client_count: number;
    collector_shape: CollectorShape;
    collector_pool: string;
    hardware: typeof collectorShapes[CollectorShape];
    workload: ReturnType<typeof effectiveWorkloadConfig>;
    expected_collector: ReturnType<typeof collectorSettings>;
    historical_collector_control: { dfe_core_allocation_count: unknown; receiver_rss_limit_bytes: unknown };
    collector_promotion?: Awaited<ReturnType<typeof applyCollectorPromotion>>['binding'];
    collector_startup_readback?: CollectorContract['observed'];
    collector_capacity_readback: ReturnType<typeof collectorCapacityReadback>;
    rendered_manifest_sha256: string;
    runner_sources_sha256: string;
    candidate_source_commit: string;
    image_binding: CandidateImageManifest;
    trusted_build_provenance_verified: boolean;
    trusted_signer_key_sha256: string;
    collector_receipt_window?: ReturnType<typeof reconcileCollectorWindow>;
    attested_platform_digests?: Record<string, string>;
    original_client_pool: ScaledPoolSettings;
    placement: ScaledRuntimeObservation;
    client_results: JsonObject[];
    collector_pool_removed: boolean;
    client_pool_restored: boolean;
  };
}

async function ensureFreshRunDirectory(runDir: string): Promise<void> {
  try {
    await lstat(runDir);
    throw new Error('Run ID was already used locally; choose a new UUID and retain existing evidence.');
  } catch (error) {
    if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ENOENT') return;
    throw error;
  }
}

async function executeCheck(options: { envFile: string; stampConfigPath: string; scaled?: ScaledOptions; outputPath?: string }): Promise<void> {
  const fileEnv = load_env(options.envFile);
  const missing = ['AZURE_SUBSCRIPTION_ID', 'AZURE_UAMI_CLIENT_ID'].filter(key => !fileEnv[key]?.trim());
  if (missing.length) throw new Error('Private .env is missing required keys: ' + missing.join(', ') + '.');
  const stamp = loadStampConfig(options.stampConfigPath);
  if (!stamp.subscription || !stamp.resourceGroup || !stamp.aks.namespace || !stamp.aks.serviceAccount)
    throw new Error('The private AKS stamp configuration is missing required fields.');
  const config = new Config({ ...fileEnv, ...process.env }, stamp);
  if (!config.identity_client_id) throw new Error('The approved stamp workload identity client ID is missing from private configuration.');
  const runId = '00000000-0000-4000-8000-000000000001';
  const owner = 'a'.repeat(48);
  const runHex = runId.replaceAll('-', '');
  const deltaRoot = 'abfss://' + config.data + '@' + config.account + '.dfs.core.windows.net/direct-baseline-1/runs/' + runId + '/dataflow';
  const source = await readFile(manifestPath, 'utf8');
  let rendered = renderManifest(source, {
    BENCHRUNUUID: runId, BENCHRUNHEX: runHex, BENCHOWNER: owner, BENCHCLIENTID: config.identity_client_id,
    BENCHREGISTRY: config.registryLoginServer, BENCHDELTA_ROOT: deltaRoot, BENCHSTAMP: config.stamp,
    BENCHSUBSCRIPTION: config.subscription, BENCHRESOURCEGROUP: config.group,
    BENCHSTORAGEACCOUNT: config.account, BENCHCHECKPOINTS: config.checkpoints, BENCHDURATIONSECS: '600',
  });
  const scaledRender = options.scaled ? await scaledInputs(rendered, runId, config.registryLoginServer, options.scaled) : undefined;
  if (scaledRender) rendered = scaledRender.manifest;
  assertRenderedManifest(rendered, runId, owner, config.registryLoginServer, options.scaled?.clientCount ?? 1);
  if (options.outputPath) {
    await writeFile(options.outputPath, rendered, { flag: 'wx', mode: 0o600 });
    if (scaledRender && !options.scaled?.imageManifestPath) {
      await writeFile(options.outputPath + '.images.json', JSON.stringify(scaledRender.binding, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    }
    if (scaledRender) {
      await writeFile(options.outputPath + '.collector.json', JSON.stringify({
        expected_collector: scaledRender.expectedCollector, historical_collector_control: scaledRender.historicalControl,
        collector_promotion: scaledRender.promotion.binding,
      }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    }
    console.log('PASS offline render; image binding is structural only, NOT trusted build provenance; output ' + options.outputPath);
    return;
  }
  const directory = await mkdtemp(join(tmpdir(), 'arrowcol-aks-check-'));
  await chmod(directory, 0o700);
  try {
    const fakeNamespace = namespacePrefix + runHex;
    const namespaced = rendered.replaceAll('namespace: ' + fakeNamespace, 'namespace: default');
    const dryRunManifest = replaceInitializerExpiry(namespaced, now() + 45 * 60_000);
    const manifestFile = join(directory, 'rendered.yaml');
    await writeFile(manifestFile, dryRunManifest, { flag: 'wx', mode: 0o600 });
    let mode: 'server' | 'client' = 'server';
    try {
      await runStage('check AKS access', 3 * 60_000, async () => {
        await configureKubeAuth(config);
        await command(['kubectl', '--context', config.cluster, 'get', '--raw=/version', '--request-timeout=10s'], 'Verify AKS API access', 20);
      });
    } catch {
      mode = 'client';
    }
    const args = ['kubectl'];
    if (mode === 'server') args.push('--context', config.cluster);
    args.push('apply', '--dry-run=' + mode, '--validate=false', '-f', manifestFile);
    let result: Awaited<ReturnType<typeof run_tree>>;
    try { result = await invokeTree(args, 90, ROOT); }
    catch { throw new Error('kubectl apply --dry-run=' + mode + ' could not complete; no resources were applied.'); }
    if (result.returncode !== 0) throw new Error('kubectl apply --dry-run=' + mode + ' failed (exit ' + result.returncode + '); no resources were applied.');
    console.log('PASS private config and rendered manifest invariants; kubectl apply --dry-run=' + mode + '.');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

interface ScaledOptions {
  clientCount: number;
  collectorShape: CollectorShape;
  candidateCommit: string;
  imageManifestPath?: string;
  provenanceKey?: string;
  provenanceBundle?: string;
  promotionPath?: string;
}

interface ScaledInputs {
  manifest: string;
  poolName: string;
  binding: CandidateImageManifest;
  platformDigests?: Record<string, string>;
  expectedCollector: ReturnType<typeof collectorSettings>;
  historicalControl: { dfe_core_allocation_count: unknown; receiver_rss_limit_bytes: unknown };
  promotion: Awaited<ReturnType<typeof applyCollectorPromotion>>;
  collectorReadback?: CollectorContract['observed'];
  evidenceDirectory?: string;
}

export async function projectCollectorContract(request: CollectorContractRequest): Promise<CollectorContract> {
  const directory = await mkdtemp(join(tmpdir(), 'collector-contract-'));
  await chmod(directory, 0o700);
  try {
    const input = join(directory, 'input.json');
    await writeFile(input, JSON.stringify(request), { mode: 0o600, flag: 'wx' });
    const result = await run_tree([resolve(ROOT, 'target/debug/arrowcol-collector-contract'), '--input', input], 90, ROOT);
    const value = requireRecord(parseJson(checkResult(result, 'Strict Rust collector contract'), 'Collector contract'), 'Collector contract');
    const environment = requireRecord(value.environment, 'Collector environment');
    if (value.schema_version !== 1 || typeof value.canonical_tunables_json !== 'string' ||
        typeof value.effective_tunables_json !== 'string' ||
        !Object.values(environment).every((entry): entry is string => typeof entry === 'string') ||
        (value.observed !== null && !isRecord(value.observed))) throw new Error('Invalid Rust collector contract result.');
    return { schema_version: 1, canonical_tunables_json: value.canonical_tunables_json,
      effective_tunables_json: value.effective_tunables_json,
      environment: Object.fromEntries(Object.entries(environment).map(([name, entry]) => [name, String(entry)])),
      observed: value.observed };
  } finally { await rm(directory, { recursive: true, force: true }); }
}

async function scaledInputs(base: string, runId: string, registry: string, options: ScaledOptions): Promise<ScaledInputs> {
  const historical = collectorSettings(base);
  const clients = renderScaledProfileManifest(base, runId, options.clientCount);
  const collector = renderScaledCollector(clients, runId, options.collectorShape);
  const promotion = await applyCollectorPromotion(collector.manifest,
    options.promotionPath ? parseJson(await readFile(options.promotionPath, 'utf8'), 'Collector promotion') : undefined,
    options.candidateCommit, executionScope.getStore()?.projectCollector ?? projectCollectorContract);
  const references = new Set(parseAllDocuments(promotion.manifest).flatMap(document => {
    const resource = document.toJSON();
    if (!isRecord(resource) || !isRecord(resource.spec) || !isRecord(resource.spec.template) || !isRecord(resource.spec.template.spec) ||
        !Array.isArray(resource.spec.template.spec.containers)) return [];
    return resource.spec.template.spec.containers.map(container => {
      const image = requireRecord(container, 'Binding image container').image;
      if (typeof image !== 'string' || !image.startsWith(registry + '/')) throw new Error('Binding image is outside candidate registry.');
      return image.slice(registry.length + 1);
    });
  }));
  const binding = options.imageManifestPath
    ? parseJson(await readFile(options.imageManifestPath, 'utf8'), 'Candidate image manifest') as CandidateImageManifest
    : { schemaVersion: 1 as const, sourceCommit: options.candidateCommit, workloadConfigSha256: effectiveWorkloadConfig(promotion.manifest).sha256,
      images: Object.fromEntries([...references].map(reference => [reference, 'sha256:' + '0'.repeat(64)])) };
  if (!isRecord(binding) || !isRecord(binding.images) || typeof binding.sourceCommit !== 'string') {
    throw new Error('Candidate image binding is invalid; it is not trusted build provenance.');
  }
  return { ...collector, binding, promotion, expectedCollector: collectorSettings(promotion.manifest),
    historicalControl: { dfe_core_allocation_count: historical.dfe_core_allocation_count, receiver_rss_limit_bytes: historical.receiver_rss_limit_bytes },
    manifest: applyCandidateImageManifest(promotion.manifest, registry, options.candidateCommit, binding) };
}

async function verifyCandidateBuilds(registry: string, options: ScaledOptions, inputs: ScaledInputs): Promise<string> {
  if (!options.provenanceKey) throw new Error('Scaled run requires --provenance-key from a trusted build signer; self-declared sourceCommit is not proof.');
  const publicKey = await readFile(options.provenanceKey);
  const keySha = createHash('sha256').update(publicKey).digest('hex');
  if (options.provenanceBundle) {
    const anchorPath = relative(await realpath(ROOT), await realpath(options.provenanceKey));
    if (!isAbsolute(anchorPath) && anchorPath !== '..' && !anchorPath.startsWith('../')) {
      throw new Error('Produced build trust anchor must be supplied outside candidate source.');
    }
    const bundle = requireRecord(parseJson(await readFile(options.provenanceBundle, 'utf8'), 'Produced build bundle'), 'Produced build bundle');
    if (bundle.schema_version !== 1 || bundle.scope !== 'candidate' || bundle.source_commit !== options.candidateCommit ||
        !Array.isArray(bundle.envelopes)) throw new Error('Local fixture or mismatched build bundle cannot authorize AKS.');
    const statements = bundle.envelopes.map(envelope => verifyBuildEnvelope(envelope, publicKey));
    inputs.platformDigests = {};
    for (const [reference, digest] of Object.entries(inputs.binding.images)) {
      const repository = registry + '/' + reference.split('@')[0];
      const matches = statements.filter(statement => Array.isArray(statement.subject) && statement.subject.some(subject =>
        isRecord(subject) && subject.name === repository && isRecord(subject.digest) && subject.digest.sha256 === digest.slice(7)));
      if (matches.length !== 1) throw new Error('Build bundle must contain exactly one verified result for each candidate role/digest.');
      const statement = matches[0]!;
      assertProducedBuildStatement(statement, digest, options.candidateCommit, repository);
      const output = requireRecord(requireRecord(statement.predicate, 'Produced predicate').output, 'Produced output');
      if (!Array.isArray(output.platforms)) throw new Error('Missing attested platform manifest.');
      const platform = requireRecord(output.platforms[0], 'Attested platform');
      if (typeof platform.digest !== 'string') throw new Error('Missing attested platform digest.');
      inputs.platformDigests[repository + '@' + digest] = platform.digest;
    }
    return keySha;
  }
  for (const [reference, digest] of Object.entries(inputs.binding.images)) {
    const image = registry + '/' + reference.split('@')[0] + '@' + digest;
    const output = await command(['cosign', 'verify-attestation', '--key', options.provenanceKey, '--type', 'slsaprovenance',
      '--output', 'json', image], 'Verify trusted candidate image attestation', 180);
    let values: unknown[];
    try {
      const value = parseJson(output, 'Verified cosign output');
      values = Array.isArray(value) ? value : [value];
    } catch {
      values = output.split(/\r?\n/).filter(Boolean).map(line => parseJson(line, 'Verified cosign envelope'));
    }
    const envelopes = values.flatMap(value => Array.isArray(value) ? value : [value]);
    let matches = 0;
    for (const envelope of envelopes) {
      if (!isRecord(envelope) || typeof envelope.payload !== 'string') throw new Error('Verified cosign output is missing its signed payload.');
      assertTrustedImageStatement(parseJson(Buffer.from(envelope.payload, 'base64').toString('utf8'), 'Signed build statement'),
        digest, options.candidateCommit, registry + '/' + reference.split('@')[0]);
      matches++;
    }
    if (!matches) throw new Error('No trusted build attestation was returned for a candidate image.');
  }
  return keySha;
}

function poolSettings(raw: JsonObject): ScaledPoolSettings {
  if (typeof raw.enableAutoScaling !== 'boolean') throw new Error('Pool autoscaler state is unknown.');
  return { enableAutoScaling: raw.enableAutoScaling, minCount: argNumber(raw.minCount, 'Pool min count'),
    maxCount: argNumber(raw.maxCount, 'Pool max count'), count: argNumber(raw.count, 'Pool count') };
}

function poolCommand(config: Config, pool: string): string[] {
  return ['az', 'aks', 'nodepool', '--subscription', config.subscription, '--resource-group', config.group,
    '--cluster-name', config.cluster, '--name', pool];
}

async function readCollectorPool(config: Config, pool: string): Promise<JsonObject | undefined> {
  const output = await command(['az', 'aks', 'nodepool', 'list', '--subscription', config.subscription,
    '--resource-group', config.group, '--cluster-name', config.cluster, '--output', 'json', '--only-show-errors'],
  'Read run-owned collector pool inventory');
  const pools = parseJson(output, 'Collector pool inventory');
  if (!Array.isArray(pools)) throw new Error('Collector pool inventory is invalid.');
  const matches = pools.filter(poolValue => isRecord(poolValue) && poolValue.name === pool);
  if (matches.length > 1) throw new Error('Duplicate run-owned collector pool.');
  return matches[0] === undefined ? undefined : requireRecord(matches[0], 'Collector pool');
}

async function assertScaledCapacity(config: Config, options: ScaledOptions): Promise<string> {
  const skuOutput = await command(['az', 'vm', 'list-skus', '--subscription', config.subscription, '--location', config.location,
    '--resource-type', 'virtualMachines', '--all', '--query',
    "[?name=='" + collectorShapes[options.collectorShape].vmSize + "' || name=='Standard_D64ds_v6'].{name:name,family:family,capabilities:capabilities,restrictions:restrictions}",
    '--output', 'json', '--only-show-errors'], 'Read scaled SKU feasibility', 180);
  const usageOutput = await command(['az', 'vm', 'list-usage', '--subscription', config.subscription,
    '--location', config.location, '--output', 'json', '--only-show-errors'], 'Read scaled vCPU quota');
  const skus = parseJson(skuOutput, 'SKU feasibility');
  const usage = parseJson(usageOutput, 'vCPU quota');
  if (!Array.isArray(skus) || !Array.isArray(usage)) throw new Error('Scaled capacity inventory is invalid.');
  for (const [skuName, cores, memory] of [
    [collectorShapes[options.collectorShape].vmSize, collectorShapes[options.collectorShape].vcpus, collectorShapes[options.collectorShape].memoryGiB],
    ['Standard_D64ds_v6', 64 * options.clientCount, 24],
  ] as const) {
    const sku = skus.find(value => isRecord(value) && value.name === skuName);
    if (!isRecord(sku) || !Array.isArray(sku.restrictions) || sku.restrictions.length !== 0 ||
        !Array.isArray(sku.capabilities) || typeof sku.family !== 'string') throw new Error('Selected scaled SKU is restricted or unknown: ' + skuName);
    const memoryCapability = sku.capabilities.find(value => isRecord(value) && value.name === 'MemoryGB');
    const cpuCapability = sku.capabilities.find(value => isRecord(value) && value.name === 'vCPUs');
    const cpuRequired = skuName === 'Standard_D64ds_v6' ? 64 : collectorShapes[options.collectorShape].vcpus;
    if (!isRecord(memoryCapability) || !Number.isFinite(Number(memoryCapability.value)) || Number(memoryCapability.value) <= memory ||
        !isRecord(cpuCapability) || Number(cpuCapability.value) !== cpuRequired) throw new Error('Selected scaled SKU has insufficient or unknown resources.');
    const quota = usage.find(value => isRecord(value) && isRecord(value.name) && value.name.value === sku.family);
    if (!isRecord(quota) || argNumber(quota.limit, 'Scaled SKU quota limit') -
        argNumber(quota.currentValue, 'Scaled SKU quota current usage') < cores) throw new Error('Insufficient scaled SKU family quota: ' + skuName);
  }
  const total = usage.find(value => isRecord(value) && isRecord(value.name) && value.name.value === 'cores');
  if (!isRecord(total) || argNumber(total.limit, 'Regional quota limit') -
      argNumber(total.currentValue, 'Regional quota current usage') < collectorShapes[options.collectorShape].vcpus + 64 * options.clientCount) {
    throw new Error('Insufficient regional scaled vCPU quota.');
  }
  const cluster = requireRecord(parseJson(await command(['az', 'aks', 'show', '--subscription', config.subscription,
    '--resource-group', config.group, '--name', config.cluster, '--output', 'json', '--only-show-errors'], 'Read selected cluster network'), 'Cluster'), 'Cluster');
  const pools = Array.isArray(cluster.agentPoolProfiles) ? cluster.agentPoolProfiles : [];
  const subnets = [...new Set(pools.flatMap(pool => isRecord(pool) && typeof pool.vnetSubnetId === 'string' ? [pool.vnetSubnetId] : []))];
  if (subnets.length !== 1) throw new Error('Scaled collector requires one proven AKS node subnet.');
  return subnets[0]!;
}

function assertCollectorOwnership(pool: JsonObject, runId: string, owner: string, shape: CollectorShape): void {
  const tags = requireRecord(pool.tags, 'Collector pool ownership tags');
  if (tags['arrowcol-oat-run-id'] !== runId || tags['arrowcol-oat-owner-token'] !== owner ||
      pool.vmSize !== collectorShapes[shape].vmSize) throw new Error('Collector pool ownership or selected shape changed; refusing mutation.');
}

async function assertNoQueuedForeignClients(config: Config): Promise<void> {
  const list = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '--all-namespaces', '-o', 'json'],
    'Read pending workloads before changing the shared client pool'), 'Pending pods'), 'Pending pods');
  if (!Array.isArray(list.items)) throw new Error('Pending pod inventory is invalid.');
  const blockers: string[] = [];
  for (const raw of list.items) {
    const pod = requireRecord(raw, 'Pending pod');
    const spec = requireRecord(pod.spec ?? {}, 'Pending pod spec');
    const status = requireRecord(pod.status ?? {}, 'Pending pod status');
    if (status.phase !== 'Pending' || spec.nodeName) continue;
    const metadata = requireRecord(pod.metadata, 'Pending pod metadata');
    const owners = Array.isArray(metadata.ownerReferences) ? metadata.ownerReferences : [];
    if (owners.some(owner => isRecord(owner) && owner.kind === 'DaemonSet')) continue;
    const selector = isRecord(spec.nodeSelector) ? spec.nodeSelector : {};
    const pool = selector['kubernetes.azure.com/agentpool'];
    const tolerations = Array.isArray(spec.tolerations) ? spec.tolerations : [];
    const couldTolerateClient = tolerations.some(value => isRecord(value) &&
      (value.key === '' || value.key === undefined || value.key === 'arrow-collector/role') &&
      (value.operator === 'Exists' || value.value === 'client'));
    if (pool === 'benchd64' || selector['arrow-collector/role'] === 'client' || (pool === undefined && couldTolerateClient)) {
      blockers.push(String(metadata.namespace) + '/' + String(metadata.name) + ' (Pending)');
    }
  }
  if (blockers.length) throw new Error('Unrelated pending client workloads prevent pool expansion: ' + blockers.join(', '));
}

async function captureScaledPods(config: Config, namespace: string, runId: string, jobs: string[], manifest: string, finished = false,
  inputs?: ScaledInputs): Promise<ScaledRuntimeObservation> {
  const platformDigests = inputs?.platformDigests;
  const list = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '-n', namespace, '-o', 'json'], 'Capture scaled pod placement'), 'Pod inventory'), 'Pod inventory');
  if (!Array.isArray(list.items)) throw new Error('Scaled pod inventory is invalid.');
  const measuredPods = list.items;
  const profiledClients = measuredPods.filter(value => isRecord(value) && isRecord(value.metadata) &&
    isRecord(value.metadata.labels) && value.metadata.labels['arrowcol-oat/client-profile'] === 'scaled');
  if (profiledClients.length !== jobs.length || profiledClients.some(value => !isRecord(value) || !isRecord(value.metadata) ||
      !isRecord(value.metadata.labels) || !jobs.includes(String(value.metadata.labels['job-name'])))) {
    throw new Error('Scaled runtime contains missing, duplicate or unexpected client pods.');
  }
  const podEvidence = (pod: unknown, containerName: string) => {
    const value = requireRecord(pod, 'Measured pod');
    const metadata = requireRecord(value.metadata, 'Measured pod metadata');
    const spec = requireRecord(value.spec, 'Measured pod spec');
    const status = requireRecord(value.status, 'Measured pod status');
    const labels = requireRecord(metadata.labels, 'Measured pod labels');
    if (labels[runLabel] !== runId || typeof metadata.name !== 'string' || typeof metadata.uid !== 'string' ||
        typeof spec.nodeName !== 'string' || !Array.isArray(status.containerStatuses)) throw new Error('Measured pod ownership/identity is missing.');
    const container = status.containerStatuses.find(value => isRecord(value) && value.name === containerName);
    const expectedPhase = containerName === 'generator' && finished ? 'Succeeded' : 'Running';
    if (!isRecord(container) || typeof container.imageID !== 'string' || container.restartCount !== 0 ||
        status.phase !== expectedPhase || (!finished && container.ready !== true)) throw new Error('Measured pod image, phase, readiness or restart state is invalid.');
    return { podName: metadata.name, podUid: metadata.uid, nodeName: spec.nodeName, imageId: container.imageID };
  };
  const collectors = measuredPods.filter(value => isRecord(value) && isRecord(value.metadata) && isRecord(value.metadata.labels) && value.metadata.labels.app === 'arrowcol');
  if (collectors.length !== 1) throw new Error('Exactly one measured collector is required.');
  const collector = podEvidence(collectors[0], 'dataflow');
  const clients = jobs.map(jobName => {
    const pods = measuredPods.filter(value => isRecord(value) && isRecord(value.metadata) && isRecord(value.metadata.labels) &&
      value.metadata.labels['job-name'] === jobName);
    if (pods.length !== 1) throw new Error('Missing or duplicated measured client pod for ' + jobName);
    return { jobName, ...podEvidence(pods[0], 'generator') };
  });
  const observation: ScaledRuntimeObservation = { collectorPodUid: collector.podUid, collectorNodeName: collector.nodeName,
    collectorImageId: collector.imageId, clients, nodeEvidence: [] };
  const documents = parseAllDocuments(manifest);
  const load = documents.find(document => document.getIn(['metadata', 'name']) === jobs[0]);
  const writer = documents.find(document => document.getIn(['metadata', 'name']) === 'arrowcol');
  const clientImage = load?.getIn(['spec', 'template', 'spec', 'containers', 0, 'image']);
  const collectorImage = writer?.getIn(['spec', 'template', 'spec', 'containers', 0, 'image']);
  if (typeof clientImage !== 'string' || typeof collectorImage !== 'string') throw new Error('Rendered candidate images are absent.');
  assertScaledRuntimeObservation(jobs, observation, platformDigests?.[clientImage] ?? clientImage.split('@')[1]!,
    platformDigests?.[collectorImage] ?? collectorImage.split('@')[1]!, platformDigests ? {
      client: clientImage.split('@')[1]!, collector: collectorImage.split('@')[1]!,
    } : undefined);
  const memoryBytes = (value: unknown): number => {
    if (typeof value !== 'string') throw new Error('Node memory quantity is unknown.');
    const match = /^(\d+)(Ki|Mi|Gi)?$/.exec(value);
    if (!match) throw new Error('Node memory quantity is unsupported.');
    return Number(match[1]) * ({ Ki: 1024, Mi: 1024 ** 2, Gi: 1024 ** 3 }[match[2] ?? ''] ?? 1);
  };
  for (const evidence of [collector, ...clients]) {
    const node = requireRecord(parseJson(await kubectl(config, ['get', 'node', evidence.nodeName, '-o', 'json'], 'Verify measured node resources'), 'Measured node'), 'Measured node');
    const allocatable = requireRecord(requireRecord(node.status, 'Node status').allocatable, 'Node allocatable resources');
    const nodeLabels = requireRecord(requireRecord(node.metadata, 'Node metadata').labels, 'Node labels');
    const isCollector = evidence.podUid === collector.podUid;
    const jobName = clients.find(client => client.podUid === evidence.podUid)?.jobName;
    const expected = isCollector ? writer : documents.find(document => document.getIn(['metadata', 'name']) === jobName);
    const livePod = measuredPods.find(value => isRecord(value) && isRecord(value.metadata) && value.metadata.uid === evidence.podUid);
    const liveContainers = requireRecord(requireRecord(livePod, 'Measured pod').spec, 'Measured pod spec').containers;
    const desiredPod = requireRecord(requireRecord(requireRecord(requireRecord(expected?.toJSON(), 'Desired resource').spec, 'Desired spec').template, 'Desired template').spec, 'Desired pod');
    if (!Array.isArray(desiredPod.containers)) throw new Error('Desired measured containers are unknown.');
    const desired = requireRecord(desiredPod.containers[0], 'Desired measured container');
    if (!Array.isArray(liveContainers)) throw new Error('Measured pod containers are unknown.');
    const actual = requireRecord(liveContainers[0], 'Actual measured container');
    if (isCollector && inputs?.promotion.binding) {
      assertPromotionEnvironment(inputs.promotion.binding.projected_environment, actual.env);
      const actualEnv = actual.env;
      const desiredEnv = desired.env;
      if (!Array.isArray(actualEnv) || !Array.isArray(desiredEnv)) throw new Error('Collector environment is missing.');
      if (
          !isDeepStrictEqual(actual.envFrom, desired.envFrom) ||
          desiredEnv.some(variable => {
            const matches = actualEnv.filter(entry =>
              requireRecord(entry, 'Actual collector env').name === requireRecord(variable, 'Desired collector env').name);
            return matches.length !== 1 || !isDeepStrictEqual(matches[0], variable);
          }) ||
          actualEnv.some((variable: unknown) => {
            const name = requireRecord(variable, 'Actual collector env').name;
            return typeof name !== 'string' || (/^(ARROWCOL_|PARQUET_|DFE_)/.test(name) &&
              !desiredEnv.some(entry => requireRecord(entry, 'Desired collector env').name === name));
          })) throw new Error('Actual promoted collector environment differs from rendered configuration.');
    }
    if (actual.image !== desired.image || !isDeepStrictEqual(actual.args, desired.args) ||
        (!isCollector && (!isDeepStrictEqual(actual.env, desired.env) || actual.envFrom !== undefined)) ||
        !isDeepStrictEqual(actual.resources, desired.resources)) {
      throw new Error('Actual measured pod image, arguments or resources differ from rendered candidate configuration.');
    }
    const cpu = String(allocatable.cpu);
    const cpuCores = cpu.endsWith('m') ? Number(cpu.slice(0, -1)) / 1000 : Number(cpu);
    const requested = Number(expected?.getIn(['spec', 'template', 'spec', 'containers', 0, 'resources', 'limits', 'cpu']));
    const expectedPool = expected?.getIn(['spec', 'template', 'spec', 'nodeSelector', 'kubernetes.azure.com/agentpool']);
    const memoryRequired = expected?.getIn(['spec', 'template', 'spec', 'containers', 0, 'resources', 'limits', 'memory']);
    const vmSize = nodeLabels['node.kubernetes.io/instance-type'];
    const expectedVm = isCollector ? (requested === collectorShapes.e192.runtimeCores ? collectorShapes.e192.vmSize : collectorShapes.e64.vmSize) : 'Standard_D64ds_v6';
    if (!Number.isFinite(cpuCores) || !Number.isFinite(requested) || cpuCores < requested ||
        memoryBytes(allocatable.memory) < memoryBytes(memoryRequired) ||
        nodeLabels['kubernetes.azure.com/agentpool'] !== expectedPool || vmSize !== expectedVm) {
      throw new Error('Measured node placement or allocatable CPU does not match selected shape.');
    }
    observation.nodeEvidence!.push({ nodeName: evidence.nodeName, vmSize: expectedVm,
      allocatableCpuCores: cpuCores, allocatableMemoryBytes: memoryBytes(allocatable.memory) });
  }
  const desiredDataflow = documents.find(document => document.getIn(['metadata', 'name']) === 'arrowcol-config')?.getIn(['data', 'dataflow.yaml']);
  const actualConfig = await getObject(config, 'configmap', namespace, 'arrowcol-config');
  if (!actualConfig || requireRecord(actualConfig.data, 'Actual dataflow config')['dataflow.yaml'] !== desiredDataflow) {
    throw new Error('Actual collector dataflow configuration differs from the rendered candidate.');
  }
  if (inputs?.promotion.request) {
    const logs = await kubectl(config, ['logs', 'pod/' + collector.podName, '-n', namespace, '-c', 'dataflow'],
      'Verify promoted collector startup readback');
    const result = await (executionScope.getStore()?.projectCollector ?? projectCollectorContract)({
      ...inputs.promotion.request, readback_logs: logs,
    });
    if (!result.observed) throw new Error('Promoted collector has no verified consumer startup readback.');
    inputs.collectorReadback = result.observed;
    if (inputs.evidenceDirectory) {
      await writePrivateFile(join(inputs.evidenceDirectory, 'collector-promotion-' + (finished ? 'finished' : 'startup') + '.json'),
        JSON.stringify({ pod_uid: collector.podUid, expected_collector_sha256: inputs.expectedCollector.sha256,
          promotion: inputs.promotion.binding, observed: result.observed }, null, 2) + '\n');
    }
  }
  return observation;
}

async function waitScaledClients(config: Config, namespace: string, writer: WriterIdentity, jobs: string[], ready: boolean): Promise<void> {
  await waitUntil(ready ? 'all scaled client readiness' : 'all scaled client completion', ready ? 7 * 60_000 : 22 * 60_000, async () => {
    await assertWriterStable(config, namespace, writer);
    let complete = true;
    for (const name of jobs) {
      const job = await getObject(config, 'job', namespace, name);
      if (!job) { complete = false; continue; }
      const status = requireRecord(job.status ?? {}, 'Scaled Job status');
      if (Number(status.failed ?? 0) > 0 || (Array.isArray(status.conditions) &&
          status.conditions.some(value => isRecord(value) && value.type === 'Failed' && value.status === 'True'))) {
        throw new Error('Scaled client ' + name + ' failed.');
      }
      if (ready) {
        const list = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '-n', namespace, '-l', 'job-name=' + name,
          '-o', 'json'], 'Read scaled client startup state'), 'Scaled client pods'), 'Scaled client pods');
        if (!Array.isArray(list.items) || list.items.length > 1) throw new Error('Scaled client pod inventory is invalid or duplicated.');
        if (list.items.length === 0) { complete = false; continue; }
        const pod = requireRecord(list.items[0], 'Scaled client pod');
        const podStatus = requireRecord(pod.status ?? {}, 'Scaled client pod status');
        if (podStatus.phase === 'Failed' || podStatus.phase === 'Succeeded') throw new Error('Scaled client ' + name + ' terminated before synchronized readiness.');
        const containers = Array.isArray(podStatus.containerStatuses) ? podStatus.containerStatuses : [];
        const generator = containers.find(value => isRecord(value) && value.name === 'generator');
        if (podStatus.phase !== 'Running' || !isRecord(generator) || generator.ready !== true) { complete = false; continue; }
        const logs = await kubectl(config, ['logs', 'job/' + name, '-n', namespace, '--all-containers=true'], 'Read scaled client readiness');
        if (!logs.includes('OAT_CLIENT_READY')) complete = false;
      } else if (status.succeeded !== 1) complete = false;
    }
    return complete ? true : undefined;
  });
}

async function executeRun(options: { envFile: string; stampConfigPath: string; runId?: string; mode: 'full' | 'smoke'; scaled?: ScaledOptions }): Promise<BenchmarkReport> {
  const runId = (options.runId ?? randomUUID()).toLowerCase();
  const measurementSeconds = options.mode === 'smoke' ? 60 : 600;
  if (!uuidPattern.test(runId)) throw new Error('--run-id must be a UUID. Generate a new UUID for every run.');
  const runHex = runId.replaceAll('-', '');
  const owner = randomBytes(24).toString('hex');
  const namespace = `${namespacePrefix}${runHex}`;
  const runDir = resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', runHex);
  const recordPath = resolve(runDir, 'run-fic.json');
  const reportPath = resolve(runDir, 'report.json');
  const secretsPath = resolve(runDir, 'secrets');
  await ensureFreshRunDirectory(runDir);
  const source = await readFile(manifestPath, 'utf8');
  const env = { ...load_env(options.envFile), ...process.env };
  const stamp = loadStampConfig(options.stampConfigPath);
  const config = new Config(env, stamp);
  if (!config.identity_client_id) throw new Error('The approved stamp workload identity client ID is missing from private configuration.');
  const { generation, relativePath } = await runStage('prepare run inputs', 10 * 60_000, async () => ({
    generation: await (executionScope.getStore()?.generation ?? capturedGeneration)(config, stageAwareRunTree),
    relativePath: await checkDeltaRoot(config, runId),
  }));
  const deltaRoot = `abfss://${config.data}@${config.account}.dfs.core.windows.net/${relativePath}`;
  let rendered = renderManifest(source, {
    BENCHRUNUUID: runId,
    BENCHRUNHEX: runHex,
    BENCHOWNER: owner,
    BENCHCLIENTID: config.identity_client_id,
    BENCHREGISTRY: config.registryLoginServer,
    BENCHDELTA_ROOT: deltaRoot,
    BENCHSTAMP: config.stamp,
    BENCHSUBSCRIPTION: config.subscription,
    BENCHRESOURCEGROUP: config.group,
    BENCHSTORAGEACCOUNT: config.account,
    BENCHCHECKPOINTS: config.checkpoints,
    BENCHDURATIONSECS: String(measurementSeconds),
  });
  const scaled = options.scaled ? await scaledInputs(rendered, runId, config.registryLoginServer, options.scaled) : undefined;
  if (scaled) rendered = scaled.manifest;
  assertRenderedManifest(rendered, runId, owner, config.registryLoginServer, options.scaled?.clientCount ?? 1);
  const manifestSha = createHash('sha256').update(source).digest('hex');
  const sourcePath = fileURLToPath(import.meta.url);
  const runbookSha = createHash('sha256').update(await readFile(sourcePath)).digest('hex');
  let scaledSourcesSha = '';
  if (scaled) {
    const hash = createHash('sha256');
    for (const path of ['projects/arrowcol-oat/aks-direct-benchmark.ts', 'projects/arrowcol-oat/aks-scaled-profile.ts',
      'projects/arrowcol-oat/aks-node-observation.ts', 'infra/azure/scaled-collector.bicep', 'projects/arrowcol-oat/publish-images.ts',
      'infra/azure/bootstrap.ts', 'projects/arrowcol-oat/aks-candidate-build.ts',
      'projects/arrowcol-oat/aks-delta-preservation.ts', 'projects/arrowcol-oat/aks-collector-window.ts',
      'projects/arrowcol-oat/aks-collector-promotion.ts', 'projects/arrowcol-oat-rust/src/runner_tunables.rs',
      'projects/arrowcol-oat-rust/src/runner_process_evidence.rs', 'crates/arrowcol-tunables/src/validation.rs',
      'projects/arrowcol-oat-rust/src/bin/arrowcol-collector-contract.rs']) {
      hash.update(path + '\0').update(await readFile(resolve(ROOT, path))).update('\0');
    }
    scaledSourcesSha = hash.digest('hex');
  }
  const parent = dirname(runDir);
  await mkdir(parent, { recursive: true, mode: 0o700 });
  await mkdir(runDir, { mode: 0o700 });
  await chmod(runDir, 0o700);
  if (scaled) scaled.evidenceDirectory = runDir;
  const info = await lstat(runDir);
  if (!info.isDirectory()) throw new Error('Benchmark private directory is not a directory.');
  const bootstrapEnv = resolve(runDir, 'bootstrap.env');
  const renderedPath = resolve(runDir, 'rendered.yaml');
  try {
    await writePrivateFile(bootstrapEnv, renderBootstrapEnv({}, generation));
    await prepareSecrets(secretsPath, bootstrapEnv);
    await writePrivateFile(renderedPath, rendered);
  } catch (error) {
    await rm(bootstrapEnv, { force: true });
    await rm(secretsPath, { recursive: true, force: true });
    throw error;
  }
  let namespaceUid: string | undefined;
  let primaryError: unknown;
  const cleanupErrors: Error[] = [];
  let kubeAuthReady = false;
  let namespaceRemoved = false;
  let ficRemoved = false;
  let poolsAtZero = false;
  let writer: WriterIdentity | undefined;
  let measurementStart = 0;
  let measurementEnd = 0;
  let metrics: Awaited<ReturnType<typeof readDeltaAddWindow>> | undefined;
  let clientPoolPlan: ScaledPoolPlan | undefined;
  let clientChangeRequested = false;
  let collectorRequested = false;
  let collectorRemoved = false;
  let clientRestored = false;
  let nodeSubnet = '';
  let signerKeySha = '';
  let placement: ScaledRuntimeObservation | undefined;
  let observer: NodeObservationHandle | undefined;
  let nodeResources: NodeResourceObservation | undefined;
  let collectorWindow: ReturnType<typeof reconcileCollectorWindow> | undefined;
  const clientResults: JsonObject[] = [];
  const clientJobs = options.scaled ? scaledClientJobs(runId, options.scaled.clientCount).map(job => job.name) : [];
  console.log('Run ' + runId + ': namespace ' + namespace + '; report ' + reportPath);
  try {
    await runStage('AKS preflight', (options.scaled ? 20 : 5) * 60_000, async () => {
      const activeSubscription = await command(['az', 'account', 'show', '--subscription', config.subscription, '--query', 'id', '--output', 'tsv', '--only-show-errors'],
        'Verify Azure login for the approved subscription');
      if (activeSubscription.toLowerCase() !== config.subscription.toLowerCase()) throw new Error('Azure login is not scoped to the approved stamp subscription.');
      await configureKubeAuth(config);
      kubeAuthReady = true;
      if (options.scaled && scaled) {
        signerKeySha = await verifyCandidateBuilds(config.registryLoginServer, options.scaled, scaled);
        const clientPool = await readPool(config, 'benchd64', true);
        if (clientPool.vmSize !== 'Standard_D64ds_v6') throw new Error('Scaled clients require the actual D64ds_v6 pool; refusing a hardware substitution.');
        clientPoolPlan = planScaledClientPool(poolSettings(clientPool), await poolNodeCount(config, 'benchd64'), options.scaled.clientCount);
        nodeSubnet = await assertScaledCapacity(config, options.scaled);
        await assertNoQueuedForeignClients(config);
        if (await readCollectorPool(config, scaled.poolName)) throw new Error('Run-owned collector pool name already exists; refusing reuse.');
      } else await requirePoolsAtZero(config);
    });
    namespaceUid = await runStage('identity', 5 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'identity');
      return prepareNamespace(config, namespace, runId, owner);
    });
    if (options.scaled && scaled && clientPoolPlan) {
      await runStage('scaled run-owned capacity', 30 * 60_000, async () => {
        const current = poolSettings(await readPool(config, 'benchd64'));
        if (JSON.stringify(current) !== JSON.stringify(clientPoolPlan!.original) || await poolNodeCount(config, 'benchd64') !== 0) {
          throw new Error('Client pool changed since preflight; refusing expansion.');
        }
        await assertNoQueuedForeignClients(config);
        clientChangeRequested = true;
        await command([...poolCommand(config, 'benchd64').slice(0, 3), 'update', ...poolCommand(config, 'benchd64').slice(3),
          '--enable-cluster-autoscaler', '--min-count', '0', '--max-count', String(options.scaled!.clientCount),
          '--output', 'none', '--only-show-errors'], 'Expand bounded client pool', 600);
        collectorRequested = true;
        if (await readCollectorPool(config, scaled.poolName)) throw new Error('Collector pool name became occupied; refusing to overwrite it.');
        const parameters = join(runDir, 'collector-parameters.json');
        await writePrivateFile(parameters, JSON.stringify({
          clusterName: { value: config.cluster }, nodeSubnetId: { value: nodeSubnet }, poolName: { value: scaled.poolName },
          vmSize: { value: collectorShapes[options.scaled!.collectorShape].vmSize }, runId: { value: runId }, ownerToken: { value: owner },
        }));
        try {
          await command(['az', 'deployment', 'group', 'create', '--subscription', config.subscription, '--resource-group', config.group,
            '--name', scaled.poolName, '--mode', 'Incremental', '--template-file', resolve(ROOT, 'infra/azure/scaled-collector.bicep'),
            '--parameters', '@' + parameters, '--output', 'none', '--only-show-errors'], 'Create run-owned collector pool', 1500);
        } finally { await rm(parameters, { force: true }); }
        const collector = await readCollectorPool(config, scaled.poolName);
        if (!collector) throw new Error('Run-owned collector pool was not created.');
        assertCollectorOwnership(collector, runId, owner, options.scaled!.collectorShape);
      });
    }
    await runStage('run FIC', 25 * 60_000, async () => {
      await runFic('prepare-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
      await runFic('probe-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
    });
    await runStage('install secrets', 12 * 60_000, () => createSecrets(config, namespace, secretsPath));
    await runStage('sql', 15 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'sql');
      await waitDeployment(config, namespace, 'arrowcol-sql');
    });
    await runStage('migration', 15 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'migration');
      await waitJob(config, namespace, 'arrowcol-migrate-v11-fresh', 10 * 60_000, dirname(reportPath));
    });
    await runStage('catalog', 40 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'catalog');
      for (const name of ['arrowcol-catalog', 'arrowcol-router', 'delta-maintenance']) await waitDeployment(config, namespace, name);
    });
    await runStage('writer', 15 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'writer');
      writer = await waitWriter(config, namespace);
    });
    await runStage('initializer', 6 * 60_000, async () => {
      if (!writer) throw new Error('Writer readiness was not established.');
      await writePrivateFile(renderedPath, replaceInitializerExpiry(rendered, now() + 45 * 60_000));
      await applyStage(config, namespace, renderedPath, 'initializer');
      await waitJob(config, namespace, 'direct-pin-live', 180_000, dirname(reportPath));
      await assertWriterStable(config, namespace, writer);
    });
    await runStage('registration', 8 * 60_000, async () => {
      if (!writer) throw new Error('Writer readiness was not established.');
      await applyStage(config, namespace, renderedPath, 'registration');
      await waitJob(config, namespace, 'direct-catalog-verified', 240_000, dirname(reportPath));
      await verifyRegistration(config, namespace, runId, deltaRoot);
      await assertWriterStable(config, namespace, writer);
    });
    await runStage('load and Delta reconciliation', 30 * 60_000, async () => {
      if (!writer || !namespaceUid) throw new Error('Writer or owned namespace readiness was not established.');
      await applyStage(config, namespace, renderedPath, 'load');
      if (scaled) {
        await waitScaledClients(config, namespace, writer, clientJobs, true);
        placement = await captureScaledPods(config, namespace, runId, clientJobs, rendered, false, scaled);
      } else await waitForReadyLoad(config, namespace, writer);
      const scheduleStart = now() + 130_000;
      if (scheduleStart - now() < 125_000) throw new Error('Load schedule must be at least 125 seconds in the future.');
      measurementStart = scheduleStart + 60_000;
      measurementEnd = measurementStart + measurementSeconds * 1000;
      if (placement) {
        const collectorPod = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '-n', namespace,
          '-l', 'app=arrowcol', '-o', 'json'], 'Read collector observation target'), 'Collector inventory'), 'Collector inventory');
        if (!Array.isArray(collectorPod.items) || collectorPod.items.length !== 1) throw new Error('Missing collector observation target.');
        const collectorName = requireRecord(requireRecord(collectorPod.items[0], 'Collector').metadata, 'Collector metadata').name;
        if (typeof collectorName !== 'string') throw new Error('Missing collector pod name.');
        observer = (executionScope.getStore()?.startObservation ?? startNodeObservation)({
          targets: [
            { pod_uid: placement.collectorPodUid, pod_name: collectorName, node_name: placement.collectorNodeName, role: 'collector', container_name: 'dataflow' },
            ...placement.clients.map(client => ({ pod_uid: client.podUid, pod_name: client.podName, node_name: client.nodeName, role: 'client' as const, container_name: 'generator' })),
          ],
          measurement_start_unix_ms: measurementStart, measurement_end_unix_ms: measurementEnd,
          read_node: createKubeletNodeResourceReader((path, signal) =>
            kubectl(config, ['get', '--raw', path], 'Read measured node Kubelet totals', 5, signal)),
          execute: async (target, sampleCommand, signal) => {
            const current = await getObject(config, 'pod', namespace, target.pod_name, signal);
            if (!current || requireRecord(current.metadata, 'Observed pod metadata').uid !== target.pod_uid) throw new Error('Observed pod UID changed.');
            return kubectl(config, ['exec', '-n', namespace, target.pod_name, '-c', target.container_name, '--', 'sh', '-c', sampleCommand],
              'Capture target pod resource sample', 5, signal);
          },
        });
      }
      if (scheduleStart - now() < 125_000) throw new Error('Scaled observation setup exceeded the common schedule lead time.');
      await kubectl(config, ['patch', 'configmap', 'direct-schedule-confirmwide', '-n', namespace, '--type', 'merge',
        '-p', JSON.stringify({ data: { start: String(scheduleStart) } })], 'Schedule benchmark load');
      if (scaled) {
        await waitScaledClients(config, namespace, writer, clientJobs, false);
        if (observer) { nodeResources = await observer.stop(); observer = undefined; }
        if (now() < measurementEnd) throw new Error('Scaled clients completed before the full common measurement window ended.');
        const finalPlacement = await captureScaledPods(config, namespace, runId, clientJobs, rendered, true, scaled);
        if (JSON.stringify(finalPlacement) !== JSON.stringify(placement)) throw new Error('Scaled client/collector pod UID, image or node changed during the window.');
        for (const jobName of clientJobs) {
          const clientPlacement = finalPlacement.clients.find(client => client.jobName === jobName)!;
          const logs = await kubectl(config, ['logs', 'pod/' + clientPlacement.podName, '-n', namespace, '-c', 'generator'], 'Read scaled client window evidence');
          await writePrivateFile(join(runDir, jobName + '.log'), logs);
          const loggedPod = await getObject(config, 'pod', namespace, clientPlacement.podName);
          if (!loggedPod || requireRecord(loggedPod.metadata, 'Logged client pod').uid !== clientPlacement.podUid) {
            throw new Error('Client pod UID changed while reading final measurement evidence.');
          }
          const windows = [...logs.matchAll(/measurement_window_start_unix_ms=(\d+) measurement_window_end_unix_ms=(\d+)/g)];
          const schedules = [...logs.matchAll(/OAT_CLIENT_SCHEDULE_RECEIVED client_id=(\S+) run_start_unix_ms=(\d+)/g)];
          const rowCounts = [...logs.matchAll(/(?:^|\n)measured_rows=(\d+) measured_seconds=([\d.]+)/g)];
          if (windows.length !== 1 || Number(windows[0]![1]) !== measurementStart || Number(windows[0]![2]) !== measurementEnd ||
              schedules.length !== 1 || Number(schedules[0]![2]) !== scheduleStart || rowCounts.length !== 1 ||
              Number(rowCounts[0]![2]) !== 600 || !Number.isSafeInteger(Number(rowCounts[0]![1]))) {
            throw new Error('Scaled client ' + jobName + ' did not prove the common schedule and full measurement window.');
          }
          const client = scaledClientJobs(runId, options.scaled!.clientCount).find(job => job.name === jobName)!;
          const clientId = client.clientId;
          if (schedules[0]![1] !== clientId) throw new Error('Scaled client source identity does not match its run-owned Job.');
          const effective = effectiveWorkloadConfig(rendered).performanceConfig;
          const summary = summarizeScaledClientWindow(logs, { podUid: clientPlacement.podUid, clientId, start: measurementStart,
            end: measurementEnd, completedRows: Number(rowCounts[0]![1]), workers: Number(effective.workers),
            streamsPerWorker: Number(effective['streams-per-worker']) });
          await writePrivateFile(join(runDir, jobName + '.measurement.json'), JSON.stringify(summary, null, 2) + '\n');
          clientResults.push({ job_name: jobName, client_id: clientId, pod_ordinal: client.podOrdinal, measurement_start_unix_ms: measurementStart,
            measurement_end_unix_ms: measurementEnd, client_completed_rows: Number(rowCounts[0]![1]),
            pod_uid: clientPlacement.podUid, final_measurement_summary: summary, succeeded: true, restart_count: 0 });
        }
      } else await waitForLoad(config, namespace, writer, 22 * 60_000);
      await assertWriterStable(config, namespace, writer);
      await verifyNamespace(config, namespace, runId, owner, namespaceUid);
      const azure = new Azure(config, () => boundedTimeoutSeconds(3600));
      metrics = await (executionScope.getStore()?.readDelta ?? readDeltaAddWindow)(azure, `${relativePath}/${tablePath}`, measurementStart, measurementEnd,
        async () => { await verifyNamespace(config, namespace, runId, owner, namespaceUid); }, scaled ? { retainCommitSummaries: true } : undefined);
      if (scaled && placement) {
        const pods = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '-n', namespace, '-l', 'app=arrowcol', '-o', 'json'],
          'Read collector receipt target'), 'Collector receipt target'), 'Collector receipt target');
        if (!Array.isArray(pods.items) || pods.items.length !== 1) throw new Error('Missing collector receipt target.');
        const metadata = requireRecord(requireRecord(pods.items[0], 'Collector receipt pod').metadata, 'Collector receipt identity');
        if (metadata.uid !== placement.collectorPodUid || typeof metadata.name !== 'string') throw new Error('Collector receipt UID changed.');
        const logs = await kubectl(config, ['logs', 'pod/' + metadata.name, '-n', namespace, '-c', 'dataflow'], 'Retain collector completion receipts');
        await writePrivateFile(join(runDir, 'collector-completion.log'), logs);
        const current = await getObject(config, 'pod', namespace, metadata.name);
        if (!current || requireRecord(current.metadata, 'Collector receipt identity').uid !== placement.collectorPodUid) {
          throw new Error('Collector UID changed while retaining completion receipts.');
        }
        collectorWindow = reconcileCollectorWindow(logs, metrics.table_id, metrics.commit_summaries,
          measurementStart, measurementEnd, placement.collectorPodUid);
        await writePrivateFile(join(runDir, 'collector-window.json'), JSON.stringify(collectorWindow, null, 2) + '\n');
      }
      if (metrics.window_rows <= 0 || metrics.window_commits <= 0) throw new Error('The timed Delta window contained no committed rows.');
    });
  } catch (error) {
    primaryError = error;
  } finally {
    const rememberCleanupError = (error: unknown) => cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
    if (observer) {
      try { nodeResources = await observer.stop(); observer = undefined; }
      catch (error) { rememberCleanupError(error); }
    }
    try {
      await runStage('cleanup owned namespace', 12 * 60_000, async () => {
        if (!kubeAuthReady) throw new Error('AKS API access was not established; namespace ownership cannot be verified.');
        const current = await readNamespace(config, namespace);
        if (current) {
          namespaceUid = await verifyNamespace(config, namespace, runId, owner, namespaceUid);
          await deleteOwnedNamespace(config, namespace, runId, owner, namespaceUid);
        }
        if (await readNamespace(config, namespace)) throw new Error('Owned namespace remains after deletion.');
        namespaceRemoved = true;
      });
    } catch (error) { rememberCleanupError(error); }
    if (namespaceRemoved) {
      try {
        await runStage('cleanup run FIC', 20 * 60_000, async () => {
          const record = await lstat(recordPath).then(() => true, error => {
            if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
            throw error;
          });
          if (record) await runFic('cleanup-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
          ficRemoved = true;
        });
      } catch (error) { rememberCleanupError(error); }
    } else {
      rememberCleanupError(new Error('Run FIC cleanup skipped because namespace absence was not verified.'));
    }
    if (kubeAuthReady && scaled && options.scaled) {
      if (namespaceRemoved) {
        if (collectorRequested) {
          try {
            await runStage('remove owned scaled collector', 35 * 60_000, async () => {
              const collector = await readCollectorPool(config, scaled.poolName);
              if (collector) {
                assertCollectorOwnership(collector, runId, owner, options.scaled!.collectorShape);
                await assertNoWorkloadPodsOnPool(config, scaled.poolName);
                await command([...poolCommand(config, scaled.poolName).slice(0, 3), 'delete', ...poolCommand(config, scaled.poolName).slice(3),
                  '--output', 'none', '--only-show-errors'], 'Delete only the run-owned collector pool', 1800);
              }
              if (await readCollectorPool(config, scaled.poolName)) throw new Error('Run-owned collector pool remains.');
              collectorRemoved = true;
            });
          } catch (error) { rememberCleanupError(error); }
        } else collectorRemoved = true;
        if (clientChangeRequested && clientPoolPlan) {
          try {
            await runStage('restore original scaled client pool', 40 * 60_000, async () => {
              const current = poolSettings(await readPool(config, 'benchd64'));
              const original = clientPoolPlan!.original;
              if (JSON.stringify(current) === JSON.stringify(original) && await poolNodeCount(config, 'benchd64') === 0) {
                clientRestored = true;
                return;
              }
              await scalePoolToZero(config, 'benchd64', clientPoolPlan!.active.maxCount);
              await waitUntil('scaled client pool drain', 30 * 60_000, async () =>
                (await readPool(config, 'benchd64')).count === 0 && await poolNodeCount(config, 'benchd64') === 0 ? true : undefined, 30_000);
              const restore = planScaledClientPoolRestore(clientPoolPlan!, poolSettings(await readPool(config, 'benchd64')), await poolNodeCount(config, 'benchd64'));
              await command([...poolCommand(config, 'benchd64').slice(0, 3), 'update', ...poolCommand(config, 'benchd64').slice(3),
                '--enable-cluster-autoscaler', '--min-count', String(restore.minCount), '--max-count', String(restore.maxCount),
                '--output', 'none', '--only-show-errors'], 'Restore original client pool settings', 600);
              if (JSON.stringify(poolSettings(await readPool(config, 'benchd64'))) !== JSON.stringify(original) ||
                  await poolNodeCount(config, 'benchd64') !== 0) throw new Error('Original client pool settings were not restored.');
              clientRestored = true;
            });
          } catch (error) { rememberCleanupError(error); }
        } else clientRestored = true;
        poolsAtZero = collectorRemoved && clientRestored;
      } else rememberCleanupError(new Error('Scaled pool cleanup refused until owned namespace absence is proven.'));
    } else if (kubeAuthReady) {
      try { await waitPoolsAtZero(config); poolsAtZero = true; } catch (error) { rememberCleanupError(error); }
    } else {
      rememberCleanupError(new Error('Workload pools could not be verified or restored without AKS API access.'));
    }
    try { await rm(bootstrapEnv, { force: true }); }
    catch (error) { rememberCleanupError(error); }
    try { await rm(secretsPath, { recursive: true, force: true }); }
    catch (error) { rememberCleanupError(error); }
  }

  const report: BenchmarkReport | undefined = metrics && writer ? {
    schema_version: 1,
    run_id: runId,
    tested_at_utc: new Date().toISOString(),
    delta_root: deltaRoot,
    runbook_sha256: runbookSha,
    manifest_sha256: manifestSha,
    measurement_start_unix_ms: measurementStart,
    measurement_end_unix_ms: measurementEnd,
    measurement_seconds: measurementSeconds,
    committed_rows: metrics.window_rows,
    committed_rows_per_second: metrics.window_rows / measurementSeconds,
    committed_files: metrics.window_files,
    committed_bytes: metrics.window_bytes,
    committed_delta_commits: metrics.window_commits,
    first_delta_version: metrics.first_version,
    last_delta_version: metrics.last_version,
    final_delta_version: metrics.delta_version,
    active_rows: metrics.active_rows,
    writer_pod_uid: writer.uid,
    writer_restart_counts: writer.restartCounts,
    namespace_removed: namespaceRemoved,
    run_fic_removed: ficRemoved,
    workload_pools_at_zero: poolsAtZero,
    ...(nodeResources ? { node_observation: nodeResources } : {}),
    ...(options.scaled && scaled && placement && clientPoolPlan ? { scaled_profile: {
      client_count: options.scaled.clientCount, collector_shape: options.scaled.collectorShape,
      collector_pool: scaled.poolName, hardware: collectorShapes[options.scaled.collectorShape],
      workload: effectiveWorkloadConfig(rendered), rendered_manifest_sha256: createHash('sha256').update(rendered).digest('hex'),
      runner_sources_sha256: scaledSourcesSha,
      candidate_source_commit: options.scaled.candidateCommit, image_binding: scaled.binding,
      expected_collector: scaled.expectedCollector, historical_collector_control: scaled.historicalControl,
      collector_promotion: scaled.promotion.binding, collector_startup_readback: scaled.collectorReadback,
      collector_capacity_readback: collectorCapacityReadback(scaled.collectorReadback),
      trusted_build_provenance_verified: !!signerKeySha, trusted_signer_key_sha256: signerKeySha,
      collector_receipt_window: collectorWindow, attested_platform_digests: scaled.platformDigests,
      original_client_pool: clientPoolPlan.original, placement, client_results: clientResults,
      collector_pool_removed: collectorRemoved, client_pool_restored: clientRestored,
    } } : {}),
  } : undefined;
  if (report) await writePrivateFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  if (primaryError || cleanupErrors.length || !report || !namespaceRemoved || !ficRemoved || !poolsAtZero) {
    const causes = [primaryError, ...cleanupErrors].filter((error): error is Error => error instanceof Error);
    if (!report) causes.push(new Error('No measured benchmark report was produced.'));
    if (!namespaceRemoved || !ficRemoved || !poolsAtZero) causes.push(new Error('Owned-resource cleanup or workload-pool scale-down is incomplete.'));
    const detail = causes.flatMap(errorMessages).map(message => config.redact(message)).filter(Boolean).join(' | ');
    throw new AggregateError(causes, `AKS benchmark run ${runId} did not complete successfully${detail ? `: ${detail}` : ''}; retain private evidence in ${runDir}.`);
  }
  return report;
}

export async function main(argv = process.argv.slice(2), dependencies: BenchmarkDependencies = {}): Promise<number> {
  return executionScope.run(dependencies, () => executeMain(argv));
}

async function executeMain(argv: string[]): Promise<number> {
  try {
    const { values, positionals } = parseArgs({ args: argv, allowPositionals: true, options: {
      'env-file': { type: 'string', default: resolve(ROOT, '.env') },
      'stamp-config': { type: 'string', default: resolve(ROOT, 'infra/azure/stamp.yaml') },
      'run-id': { type: 'string' },
      mode: { type: 'string', default: 'full' },
      profile: { type: 'string', default: 'default' },
      clients: { type: 'string' },
      'collector-shape': { type: 'string' },
      'candidate-commit': { type: 'string' },
      'image-manifest': { type: 'string' },
      'provenance-key': { type: 'string' },
      'provenance-bundle': { type: 'string' },
      'collector-promotion': { type: 'string' },
      output: { type: 'string' },
      help: { type: 'boolean', short: 'h' },
    } });
    if (values.help) {
      console.log('Usage: npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts check [--env-file PATH] [--stamp-config YAML]');
      console.log('       npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run [--mode full|smoke] [--env-file PATH] [--stamp-config YAML] [--run-id UUID]');
      console.log('Scaled opt-in: --profile scaled --clients 1..8 --collector-shape e64|e192 --candidate-commit SHA --image-manifest JSON [--provenance-key TRUSTED_PUBLIC_KEY]');
      console.log('Offline only: render --output PATH (exclusive create); scaled run requires trusted signed image attestations.');
      console.log('Qualified collector delta: --collector-promotion JSON; requires the locally built strict Rust collector-contract binary.');
      return 0;
    }
    if (positionals.length !== 1 || !['check', 'render', 'run'].includes(positionals[0]!)) throw new Error('Choose check, render or run.');
    const inputs = { envFile: resolve(values['env-file']!), stampConfigPath: resolve(values['stamp-config']!) };
    if (!['default', 'scaled'].includes(values.profile!)) throw new Error('--profile must be default or scaled.');
    let scaled: ScaledOptions | undefined;
    if (values.profile === 'scaled') {
      const count = Number(values.clients ?? '1');
      scaledClientJobs('00000000-0000-4000-8000-000000000001', count);
      const shape = values['collector-shape'] ?? 'e64';
      if (shape !== 'e64' && shape !== 'e192') throw new Error('--collector-shape must be e64 or e192.');
      if (!values['candidate-commit'] || !/^[0-9a-f]{40}$/.test(values['candidate-commit']) ||
          (positionals[0] !== 'render' && !values['image-manifest'])) {
        throw new Error('Scaled profile requires --candidate-commit and --image-manifest (render can generate an image-binding template).');
      }
      scaled = { clientCount: count, collectorShape: shape, candidateCommit: values['candidate-commit'],
        imageManifestPath: values['image-manifest'] ? resolve(values['image-manifest']) : undefined,
        provenanceKey: values['provenance-key'] ? resolve(values['provenance-key']) : undefined,
        promotionPath: values['collector-promotion'] ? resolve(values['collector-promotion']) : undefined,
        provenanceBundle: values['provenance-bundle'] ? resolve(values['provenance-bundle']) : undefined };
      if (values.mode !== 'full') throw new Error('Scaled holdout requires the common full 600-second window.');
      if (positionals[0] === 'run' && !scaled.provenanceKey) throw new Error('Scaled run requires a trusted --provenance-key; image bindings alone are not build proof.');
    } else if (values.clients || values['collector-shape'] || values['candidate-commit'] || values['image-manifest'] || values['provenance-key'] || values['provenance-bundle'] || values['collector-promotion']) {
      throw new Error('Scaled options require explicit --profile scaled; default behavior is unchanged.');
    }
    if (positionals[0] === 'check' || positionals[0] === 'render') {
      if (values.mode !== 'full' || values['run-id']) throw new Error('check/render does not accept --mode or --run-id.');
      if (positionals[0] === 'render' && !values.output) throw new Error('Offline render requires --output PATH.');
      if (positionals[0] === 'check' && values.output) throw new Error('--output is only accepted by render.');
      await executeCheck({ ...inputs, scaled, outputPath: values.output ? resolve(values.output) : undefined });
      return 0;
    }
    if (values.output) throw new Error('--output is only accepted by render.');
    if (values.mode !== 'full' && values.mode !== 'smoke') throw new Error('--mode must be full or smoke.');
    const report = await executeRun({ ...inputs, runId: values['run-id'], mode: values.mode, scaled });
    console.log('PASS ' + report.committed_rows_per_second.toFixed(2) + ' committed rows/s; ' + report.committed_rows +
      ' rows in ' + report.measurement_seconds + ' seconds; Delta v' + report.first_delta_version + '-v' + report.last_delta_version +
      '; report ' + resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', report.run_id.replaceAll('-', ''), 'report.json'));
    return 0;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  process.exitCode = await main();
}