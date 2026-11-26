#!/usr/bin/env -S npx --no-install tsx
import { AsyncLocalStorage } from 'node:async_hooks';
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { chmod, lstat, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { parseAllDocuments } from 'yaml';
import { Azure, Config, load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { capturedGeneration } from './aks-setup.js';
import { prepareSecrets, renderBootstrapEnv, writePrivateFile } from './runtime.js';
import { canonicalDeltaWindow, collectWriterCommitRecords, readDeltaAddWindow, type CanonicalDeltaWindow,
  type WriterCommitRecord } from './aks-delta-preservation.js';
import { DirectQapObserver, clockErrorBound } from './aks-qap-observer.js';

const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
const namespacePrefix = 'arrow-collector-';
const ownerLabel = 'arrowcol-oat/owner-token';
const runLabel = 'arrowcol-oat/run-id';
const tablePath = 'telemetry/sqlserver/sqlserver_availability_replica_states';
const table = tablePath.replaceAll('/', '.');
const uuidPattern = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
const workloadPools = ['benche64', 'benchd64'] as const;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

const stageScope = new AsyncLocalStorage<{ name: string; deadline: number; timeoutMs: number }>();
const manifestStages = ['identity', 'sql', 'migration', 'catalog', 'writer', 'initializer', 'registration', 'load'] as const;

function boundedTimeoutSeconds(timeout: number): number {
  const stage = stageScope.getStore();
  if (!stage) return timeout;
  const remaining = Math.ceil((stage.deadline - Date.now()) / 1000);
  if (remaining < 1) throw new Error(stage.name + ' stage timed out after ' + (stage.timeoutMs / 1000) + ' seconds.');
  return Math.min(timeout, remaining);
}

const stageAwareRunTree: typeof run_tree = (args, timeout, cwd, env, onStderr, maxOutputBytes) =>
  run_tree(args, boundedTimeoutSeconds(timeout), cwd, env, onStderr, maxOutputBytes);

async function runStage<T>(name: string, timeoutMs: number, action: () => Promise<T>): Promise<T> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw new Error(name + ' stage timeout is invalid.');
  const stage = { name, timeoutMs, deadline: Date.now() + timeoutMs };
  return stageScope.run(stage, async () => {
    try {
      const result = await action();
      if (Date.now() >= stage.deadline) throw new Error('stage deadline elapsed.');
      return result;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      const timedOut = Date.now() >= stage.deadline || /timed out|deadline elapsed/i.test(detail);
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

async function command(args: string[], label: string, timeout = 120): Promise<string> {
  return checkResult(await run_tree(args, boundedTimeoutSeconds(timeout), ROOT), label);
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

async function kubectl(config: Config, args: string[], label: string, timeout = 120): Promise<string> {
  return command(['kubectl', '--context', config.cluster, ...args], label, timeout);
}

async function getObject(config: Config, resource: string, namespace: string, name: string): Promise<JsonObject | undefined> {
  const output = await kubectl(config, ['get', resource, name, '-n', namespace, '-o', 'json', '--ignore-not-found'],
    `Read ${resource}/${name}`);
  if (!output) return undefined;
  return requireRecord(parseJson(output, `${resource}/${name}`), `${resource}/${name}`);
}

async function waitUntil<T>(label: string, timeoutMs: number, predicate: () => Promise<T | undefined>, intervalMs = 5000): Promise<T> {
  const localEnd = Date.now() + timeoutMs;
  const stage = stageScope.getStore();
  const end = Math.min(localEnd, stage?.deadline ?? Number.POSITIVE_INFINITY);
  while (Date.now() < end) {
    const value = await predicate();
    if (value !== undefined) return value;
    await sleep(Math.min(intervalMs, Math.max(0, end - Date.now())));
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

async function readPool(config: Config, pool: string): Promise<JsonObject> {
  const output = await command(['az', 'aks', 'nodepool', 'show', '--subscription', config.subscription,
    '--resource-group', config.group, '--cluster-name', config.cluster, '--name', pool,
    '--query', '{enableAutoScaling:enableAutoScaling,minCount:minCount,maxCount:maxCount,count:count}',
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

async function scalePoolToZero(config: Config, pool: string): Promise<void> {
  let blockers: string[] = [];
  const label = `${pool} safe scale-down`;
  let decision: 'drained' | 'scale';
  try {
    decision = await waitUntil(label, 25 * 60_000, async () => {
      const current = await readPool(config, pool);
      if (current.enableAutoScaling !== true || current.minCount !== 0 || current.maxCount !== 1) {
        throw new Error(`Workload pool ${pool} must retain autoscaling min 0/max 1; refusing to change shared pool configuration.`);
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
      '--enable-cluster-autoscaler', '--min-count', '0', '--max-count', '1',
      '--only-show-errors', '--output', 'none'], `Restore ${pool} autoscaling min 0/max 1`, 600);
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

interface WriterIdentity { name: string; uid: string; restartCounts: number[] }

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
  if (typeof metadata.uid !== 'string' || typeof metadata.name !== 'string') throw new Error('Writer pod identity is missing.');
  return { name: metadata.name, uid: metadata.uid,
    restartCounts: containers.map(container => argNumber(container.restartCount, 'Writer restart count')) };
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

export function assertRenderedManifest(rendered: string, runId: string, owner: string, registry: string): void {
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
    if (resource.kind === 'Deployment' || resource.kind === 'Job') {
      const spec = requireRecord(resource.spec, 'Rendered workload spec');
      const template = requireRecord(spec.template, 'Rendered workload template');
      const podSpec = requireRecord(template.spec, 'Rendered workload pod spec');
      const selector = requireRecord(podSpec.nodeSelector, 'Rendered workload node selector');
      const expectedPool = metadata.name === 'direct-load-confirmwide' ? 'benchd64' : 'benche64';
      if (selector['kubernetes.azure.com/agentpool'] !== expectedPool)
        throw new Error(`Rendered workload ${String(metadata.name)} must select agent pool ${expectedPool}.`);
    }
    if (resource.kind === 'Namespace') {
      namespaceCount++;
      const annotations = requireRecord(metadata.annotations, 'Rendered namespace annotations');
      if (metadata.name !== expectedNamespace || labels[runLabel] !== runId || labels[ownerLabel] !== owner ||
          annotations[runLabel] !== runId || annotations[ownerLabel] !== owner) {
        throw new Error('Rendered namespace ownership markers do not match the fake run inputs.');
      }
    }
    if (resource.kind === 'Job' && metadata.name === 'direct-load-confirmwide') {
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
  if (namespaceCount !== 1 || loadDurationCount !== 1 || images.length === 0 ||
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

export function verifyBenchmarkPlacement(pods: unknown[], nodes: unknown[]): { pod: string; node: string; pool: string }[] {
  const pools = new Map<string, string>();
  for (const value of nodes) {
    const node = requireRecord(value, 'Placement node');
    const metadata = requireRecord(node.metadata, 'Placement node metadata');
    const labels = isRecord(metadata.labels) ? metadata.labels : {};
    if (typeof metadata.name === 'string' && typeof labels['kubernetes.azure.com/agentpool'] === 'string')
      pools.set(metadata.name, labels['kubernetes.azure.com/agentpool']);
  }
  if (!pods.length) throw new Error('Benchmark placement has no pods.');
  return pods.map(value => {
    const pod = requireRecord(value, 'Placement pod');
    const metadata = requireRecord(pod.metadata, 'Placement pod metadata');
    const labels = isRecord(metadata.labels) ? metadata.labels : {};
    const spec = requireRecord(pod.spec, 'Placement pod spec');
    const pool = typeof spec.nodeName === 'string' ? pools.get(spec.nodeName) : undefined;
    const load = labels['batch.kubernetes.io/job-name'] === 'direct-load-confirmwide' ||
      labels['job-name'] === 'direct-load-confirmwide';
    const expectedPool = load ? 'benchd64' : 'benche64';
    if (typeof metadata.name !== 'string' || typeof spec.nodeName !== 'string' || pool !== expectedPool)
      throw new Error(`Benchmark pod ${String(metadata.name)} is on ${pool ?? 'unknown'}, expected ${expectedPool}.`);
    return { pod: metadata.name, node: spec.nodeName, pool };
  });
}

async function readBenchmarkPlacement(config: Config, namespace: string) {
  const pods = requireRecord(parseJson(await kubectl(config, ['get', 'pods', '-n', namespace, '-o', 'json'],
    'Read benchmark pod placement'), 'Benchmark pods'), 'Benchmark pods');
  const nodes = requireRecord(parseJson(await kubectl(config, ['get', 'nodes', '-o', 'json'],
    'Read benchmark node pool labels'), 'Benchmark nodes'), 'Benchmark nodes');
  if (!Array.isArray(pods.items) || !Array.isArray(nodes.items)) throw new Error('Benchmark placement inventory is invalid.');
  return verifyBenchmarkPlacement(pods.items, nodes.items);
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

async function waitForLoad(config: Config, namespace: string, writer: WriterIdentity, timeoutMs: number,
  onTick: () => Promise<void>): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await assertWriterStable(config, namespace, writer);
    await onTick();
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
  canonical_rows: number;
  canonical_rows_per_second: number;
  canonical_files: number;
  canonical_bytes: number;
  canonical_delta_commits: number;
  canonical_first_delta_version: number;
  canonical_last_delta_version: number;
  writer_commit_records: number;
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
  pod_placement: { pod: string; node: string; pool: string }[];
  namespace_removed: boolean;
  run_fic_removed: boolean;
  workload_pools_at_zero: boolean;
  qap?: {
    observation_count: number;
    terminal_state_ready: boolean;
    table_removed: boolean;
    clock_error_bound_ms: number | null;
    evidence_file: string;
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

async function executeCheck(options: { envFile: string; stampConfigPath: string }): Promise<void> {
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
  const rendered = renderManifest(source, {
    BENCHRUNUUID: runId, BENCHRUNHEX: runHex, BENCHOWNER: owner, BENCHCLIENTID: config.identity_client_id,
    BENCHREGISTRY: config.registryLoginServer, BENCHDELTA_ROOT: deltaRoot, BENCHSTAMP: config.stamp,
    BENCHSUBSCRIPTION: config.subscription, BENCHRESOURCEGROUP: config.group,
    BENCHSTORAGEACCOUNT: config.account, BENCHCHECKPOINTS: config.checkpoints, BENCHDURATIONSECS: '600',
  });
  assertRenderedManifest(rendered, runId, owner, config.registryLoginServer);
  const directory = await mkdtemp(join(tmpdir(), 'arrowcol-aks-check-'));
  await chmod(directory, 0o700);
  try {
    const fakeNamespace = namespacePrefix + runHex;
    const namespaced = rendered.replaceAll('namespace: ' + fakeNamespace, 'namespace: default');
    const dryRunManifest = replaceInitializerExpiry(namespaced, Date.now() + 45 * 60_000);
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
    try { result = await run_tree(args, 90, ROOT); }
    catch { throw new Error('kubectl apply --dry-run=' + mode + ' could not complete; no resources were applied.'); }
    if (result.returncode !== 0) throw new Error('kubectl apply --dry-run=' + mode + ' failed (exit ' + result.returncode + '); no resources were applied.');
    console.log('PASS private config and rendered manifest invariants; kubectl apply --dry-run=' + mode + '.');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

async function executeRun(options: { envFile: string; stampConfigPath: string; runId?: string;
  mode: 'full' | 'smoke'; qap?: boolean }): Promise<BenchmarkReport> {
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
    generation: await capturedGeneration(config, stageAwareRunTree),
    relativePath: await checkDeltaRoot(config, runId),
  }));
  const deltaRoot = `abfss://${config.data}@${config.account}.dfs.core.windows.net/${relativePath}`;
  const rendered = renderManifest(source, {
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
  assertRenderedManifest(rendered, runId, owner, config.registryLoginServer);
  const manifestSha = createHash('sha256').update(source).digest('hex');
  const sourcePath = fileURLToPath(import.meta.url);
  const runbookSha = createHash('sha256').update(await readFile(sourcePath)).digest('hex');
  const parent = dirname(runDir);
  await mkdir(parent, { recursive: true, mode: 0o700 });
  await mkdir(runDir, { mode: 0o700 });
  await chmod(runDir, 0o700);
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
  let canonical: CanonicalDeltaWindow | undefined;
  const writerCommits = new Map<number, WriterCommitRecord>();
  let podPlacement: BenchmarkReport['pod_placement'] = [];
  let qap: DirectQapObserver | undefined;
  let qapTerminalReady = false;
  let qapTableRemoved = false;
  const observeClock = async (loadWriter: WriterIdentity): Promise<void> => {
    if (!qap) return;
    const startedAtMs = Date.now();
    try {
      const output = await kubectl(config, ['exec', loadWriter.name, '-c', 'dataflow', '-n', namespace, '--',
        'date', '+%s%3N'], 'Observe producer clock', 30);
      const endedAtMs = Date.now();
      if (!/^\d{13}$/.test(output)) throw new Error('Producer clock was not a millisecond Unix timestamp.');
      qap.clocks.push({ startedAtMs, endedAtMs, producerAtMs: Number(output) });
    } catch (error) {
      console.warn('Producer clock bound unavailable; latency will remain unqualified.');
      await writePrivateFile(resolve(runDir, 'qap-clock-error.txt'),
        config.redact(error instanceof Error ? error.message : String(error)) + '\n');
    }
  };
  console.log('Run ' + runId + ': namespace ' + namespace + '; report ' + reportPath);
  try {
    await runStage('AKS preflight', 5 * 60_000, async () => {
      const activeSubscription = await command(['az', 'account', 'show', '--subscription', config.subscription, '--query', 'id', '--output', 'tsv', '--only-show-errors'],
        'Verify Azure login for the approved subscription');
      if (activeSubscription.toLowerCase() !== config.subscription.toLowerCase()) throw new Error('Azure login is not scoped to the approved stamp subscription.');
      await configureKubeAuth(config);
      kubeAuthReady = true;
      await requirePoolsAtZero(config);
    });
    namespaceUid = await runStage('identity', 5 * 60_000, async () => {
      await applyStage(config, namespace, renderedPath, 'identity');
      return prepareNamespace(config, namespace, runId, owner);
    });
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
      await writePrivateFile(renderedPath, replaceInitializerExpiry(rendered, Date.now() + 45 * 60_000));
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
    if (options.qap) {
      qap = new DirectQapObserver(new Azure(config, 90), runId, runDir);
      await runStage('create owned QAP table', 3 * 60_000, () => qap!.create(`${deltaRoot}/${tablePath}`));
    }
    await runStage('load and Delta reconciliation', 45 * 60_000, async () => {
      if (!writer || !namespaceUid) throw new Error('Writer or owned namespace readiness was not established.');
      const loadWriter = writer, ownedUid = namespaceUid;
      await applyStage(config, namespace, renderedPath, 'load');
      await waitForReadyLoad(config, namespace, loadWriter);
      podPlacement = await readBenchmarkPlacement(config, namespace);
      await writePrivateFile(resolve(runDir, 'pod-placement.json'), `${JSON.stringify(podPlacement, null, 2)}\n`);
      let logsReadAt: number | undefined;
      const sampleWriterCommits = async (required: boolean) => {
        const startedAt = Date.now();
        const since = logsReadAt === undefined ? [] : [`--since=${Math.ceil((startedAt - logsReadAt) / 1000) + 30}s`];
        let output: string;
        try {
          output = await kubectl(config, ['logs', `pod/${loadWriter.name}`, '-c', 'dataflow', '-n', namespace, ...since],
            'Read writer commit records', 120);
        } catch (error) {
          if (required) throw error;
          console.warn('Writer log sample unavailable; the next read will cover this interval. Missing commit evidence fails reconciliation.');
          return;
        }
        collectWriterCommitRecords(output, writerCommits);
        logsReadAt = startedAt;
      };
      await sampleWriterCommits(true);
      await observeClock(loadWriter);
      const scheduleStart = Date.now() + 130_000;
      if (scheduleStart - Date.now() < 125_000) throw new Error('Load schedule must be at least 125 seconds in the future.');
      await kubectl(config, ['patch', 'configmap', 'direct-schedule-confirmwide', '-n', namespace, '--type', 'merge',
        '-p', JSON.stringify({ data: { start: String(scheduleStart) } })], 'Schedule benchmark load');
      measurementStart = scheduleStart + 60_000;
      measurementEnd = measurementStart + measurementSeconds * 1000;
      await waitForLoad(config, namespace, loadWriter, 22 * 60_000, async () => {
        await sampleWriterCommits(false);
        if (qap) await qap.sample();
      });
      await observeClock(loadWriter);
      podPlacement = await readBenchmarkPlacement(config, namespace);
      await assertWriterStable(config, namespace, loadWriter);
      await verifyNamespace(config, namespace, runId, owner, ownedUid);
      const azure = new Azure(config, () => boundedTimeoutSeconds(3600));
      let verifiedAt = 0;
      const window = await readDeltaAddWindow(azure, `${relativePath}/${tablePath}`, measurementStart, measurementEnd,
        async () => {
          if (Date.now() - verifiedAt < 20_000) return;
          await verifyNamespace(config, namespace, runId, owner, ownedUid);
          verifiedAt = Date.now();
        }, Date.now() + 20 * 60_000);
      await verifyNamespace(config, namespace, runId, owner, ownedUid);
      await assertWriterStable(config, namespace, loadWriter);
      await sampleWriterCommits(true);
      await writePrivateFile(resolve(runDir, 'writer-commits.json'), `${JSON.stringify({
        measurement_start_unix_ms: measurementStart, measurement_end_unix_ms: measurementEnd, delta_adds: window.commits,
        writer_records: [...writerCommits.values()].sort((a, b) => a.version - b.version),
      })}\n`);
      if (window.window_rows <= 0 || window.window_commits <= 0) throw new Error('The timed Delta window contained no committed rows.');
      canonical = canonicalDeltaWindow(window.commits, writerCommits, measurementStart, measurementEnd);
      metrics = window;
      if (qap) {
        await writePrivateFile(resolve(runDir, 'qap-source.json'), JSON.stringify({
          measurementStartMs: measurementStart, measurementEndMs: measurementEnd,
          warmupMs: 60_000, steadyStateStartMs: measurementStart + 120_000,
          source: window, writerRecords: [...writerCommits.values()], clocks: qap.clocks,
        }) + '\n');
        qapTerminalReady = await qap.drain({ version: window.delta_version, rows: window.active_rows },
          options.mode === 'smoke' ? 5 * 60_000 : 15 * 60_000);
      }
    });
  } catch (error) {
    primaryError = error;
  } finally {
    const rememberCleanupError = (error: unknown) => cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
    if (qap) {
      try {
        await runStage('cleanup owned QAP table', 3 * 60_000, () => qap!.close());
        qapTableRemoved = true;
      } catch (error) { rememberCleanupError(error); }
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
    if (kubeAuthReady) {
      try { await waitPoolsAtZero(config); poolsAtZero = true; }
      catch (error) { rememberCleanupError(error); }
    } else {
      rememberCleanupError(new Error('Workload pools could not be verified or restored without AKS API access.'));
    }
    try { await rm(bootstrapEnv, { force: true }); }
    catch (error) { rememberCleanupError(error); }
    try { await rm(secretsPath, { recursive: true, force: true }); }
    catch (error) { rememberCleanupError(error); }
  }

  const report: BenchmarkReport | undefined = metrics && canonical && writer ? {
    schema_version: 1,
    run_id: runId,
    tested_at_utc: new Date().toISOString(),
    delta_root: deltaRoot,
    runbook_sha256: runbookSha,
    manifest_sha256: manifestSha,
    measurement_start_unix_ms: measurementStart,
    measurement_end_unix_ms: measurementEnd,
    measurement_seconds: measurementSeconds,
    canonical_rows: canonical.rows,
    canonical_rows_per_second: canonical.rows / measurementSeconds,
    canonical_files: canonical.files,
    canonical_bytes: canonical.bytes,
    canonical_delta_commits: canonical.commits,
    canonical_first_delta_version: canonical.first_version,
    canonical_last_delta_version: canonical.last_version,
    writer_commit_records: writerCommits.size,
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
    pod_placement: podPlacement,
    namespace_removed: namespaceRemoved,
    run_fic_removed: ficRemoved,
    workload_pools_at_zero: poolsAtZero,
    ...(qap ? { qap: {
      observation_count: qap.observations.length, terminal_state_ready: qapTerminalReady,
      table_removed: qapTableRemoved, clock_error_bound_ms: clockErrorBound(qap.clocks),
      evidence_file: 'qap-observations.jsonl',
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

export async function main(argv = process.argv.slice(2)): Promise<number> {
  try {
    const { values, positionals } = parseArgs({ args: argv, allowPositionals: true, options: {
      'env-file': { type: 'string', default: resolve(ROOT, '.env') },
      'stamp-config': { type: 'string', default: resolve(ROOT, 'infra/azure/stamp.yaml') },
      'run-id': { type: 'string' },
      mode: { type: 'string', default: 'full' },
      qap: { type: 'boolean', default: false },
      help: { type: 'boolean', short: 'h' },
    } });
    if (values.help) {
      console.log('Usage: npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts check [--env-file PATH] [--stamp-config YAML]');
      console.log('       npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run [--mode full|smoke] [--qap] [--env-file PATH] [--stamp-config YAML] [--run-id UUID]');
      return 0;
    }
    if (positionals.length !== 1) throw new Error('Choose check or run.');
    const inputs = { envFile: resolve(values['env-file']!), stampConfigPath: resolve(values['stamp-config']!) };
    if (positionals[0] === 'check') {
      if (values.mode !== 'full' || values['run-id'] || values.qap) throw new Error('check does not accept --mode, --qap or --run-id.');
      await executeCheck(inputs);
      return 0;
    }
    if (positionals[0] !== 'run') throw new Error('Choose check or run.');
    if (values.mode !== 'full' && values.mode !== 'smoke') throw new Error('--mode must be full or smoke.');
    const report = await executeRun({ ...inputs, runId: values['run-id'], mode: values.mode, qap: values.qap });
    console.log('PASS ' + report.canonical_rows_per_second.toFixed(2) + ' canonical durable rows/s; ' + report.canonical_rows +
      ' rows in ' + report.measurement_seconds + ' seconds; Delta v' + report.canonical_first_delta_version + '-v' +
      report.canonical_last_delta_version + '; commit-timestamp window ' + report.committed_rows_per_second.toFixed(2) +
      ' rows/s; report ' + resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', report.run_id.replaceAll('-', ''), 'report.json'));
    if (report.qap) console.log('QAP evidence collected; terminal state ready=' + report.qap.terminal_state_ready +
      '. This is not an accelerated-throughput or latency qualification; score the cohort evidence separately.');
    return 0;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  process.exitCode = await main();
}