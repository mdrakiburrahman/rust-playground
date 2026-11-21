#!/usr/bin/env -S npx --no-install tsx
import { randomBytes, randomUUID, createHash } from 'node:crypto';
import { chmod, lstat, mkdir, readFile, rm } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { Azure, Config, load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { capturedGeneration } from './aks-setup.js';
import { prepareSecrets, renderBootstrapEnv, writePrivateFile } from './runtime.js';
import { readDeltaAddWindow } from './aks-delta-preservation.js';

const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
const namespacePrefix = 'arrow-collector-';
const ownerLabel = 'arrowcol-oat/owner-token';
const runLabel = 'arrowcol-oat/run-id';
const table = 'telemetry.sqlserver.sqlserver_availability_replica_states';
const uuidPattern = /^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
const workloadPools = ['benche64', 'benchd64'] as const;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

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
  return checkResult(await run_tree(args, timeout, ROOT), label);
}

function parseJson(text: string, label: string): unknown {
  try { return JSON.parse(text); }
  catch { throw new Error(`${label} returned invalid JSON.`); }
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
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    const value = await predicate();
    if (value !== undefined) return value;
    await sleep(Math.min(intervalMs, Math.max(0, end - Date.now())));
  }
  throw new Error(`${label} timed out.`);
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

async function assertNoWorkloadPodsOnPool(config: Config, pool: string): Promise<void> {
  const nodeOutput = await kubectl(config, ['get', 'nodes', '-l', `kubernetes.azure.com/agentpool=${pool}`, '-o', 'json'],
    `Read ${pool} nodes before scale-down`);
  const nodeList = requireRecord(parseJson(nodeOutput, `${pool} nodes`), `${pool} nodes`);
  if (!Array.isArray(nodeList.items)) throw new Error(`${pool} node inventory is invalid.`);
  for (const rawNode of nodeList.items) {
    const node = requireRecord(rawNode, `${pool} node`);
    const metadata = requireRecord(node.metadata, `${pool} node metadata`);
    if (typeof metadata.name !== 'string') throw new Error(`${pool} node name is invalid.`);
    const podOutput = await kubectl(config, ['get', 'pods', '--all-namespaces', '--field-selector',
      `spec.nodeName=${metadata.name}`, '-o', 'json'], `Read pods on ${pool} node`);
    const podList = requireRecord(parseJson(podOutput, `${pool} pods`), `${pool} pods`);
    if (!Array.isArray(podList.items)) throw new Error(`${pool} pod inventory is invalid.`);
    const hasWorkloadPod = podList.items.some(rawPod => {
      if (!isRecord(rawPod)) return true;
      const podMetadata = isRecord(rawPod.metadata) ? rawPod.metadata : {};
      const owners = Array.isArray(podMetadata.ownerReferences) ? podMetadata.ownerReferences : [];
      return !owners.some(owner => isRecord(owner) && owner.kind === 'DaemonSet');
    });
    if (hasWorkloadPod) throw new Error(`Non-DaemonSet pods remain on ${pool}; refusing to scale down.`);
  }
}

async function scalePoolToZero(config: Config, pool: string): Promise<void> {
  const current = await readPool(config, pool);
  if (current.enableAutoScaling !== true || current.minCount !== 0 || current.maxCount !== 1) {
    throw new Error(`Workload pool ${pool} must retain autoscaling min 0/max 1; refusing to change shared pool configuration.`);
  }
  if ((await poolNodeCount(config, pool)) === 0 && current.count === 0) return;
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

async function waitPoolsAtZero(config: Config): Promise<void> {
  for (const pool of workloadPools) {
    await scalePoolToZero(config, pool);
    await waitUntil(`${pool} node pool scale-down`, 30 * 60_000, async () => {
      const nodes = await poolNodeCount(config, pool);
      const configuration = await readPool(config, pool);
      return nodes === 0 && configuration.count === 0 && configuration.enableAutoScaling === true &&
        configuration.minCount === 0 && configuration.maxCount === 1 ? true : undefined;
    }, 30_000);
  }
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

async function waitJob(config: Config, namespace: string, name: string, timeoutMs: number): Promise<void> {
  await waitUntil(`job/${name}`, timeoutMs, async () => {
    const job = await getObject(config, 'job', namespace, name);
    if (!job) return undefined;
    const status = isRecord(job.status) ? job.status : {};
    if ((typeof status.failed === 'number' && status.failed > 0) ||
        (Array.isArray(status.conditions) && status.conditions.some(condition => isRecord(condition) &&
          condition.type === 'Failed' && condition.status === 'True'))) {
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

function renderManifest(source: string, values: Record<string, string>): string {
  let rendered = source;
  for (const [key, value] of Object.entries(values)) rendered = rendered.replaceAll(key, value);
  if (/BENCH[A-Z0-9_]+/.test(rendered)) throw new Error('The benchmark manifest still contains an unresolved runtime placeholder.');
  return rendered;
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

async function waitForLoad(config: Config, namespace: string, writer: WriterIdentity): Promise<void> {
  const deadline = Date.now() + 22 * 60_000;
  while (Date.now() < deadline) {
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
  throw new Error('The timed load Job exceeded its 22-minute safety deadline.');
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
  const expectedLocation = `${deltaRoot}/telemetry/sqlserver/sqlserver_availability_replica_states`;
  if (receipt.run_id !== runId || receipt.table !== table || receipt.location !== expectedLocation ||
      receipt.provider !== 'delta' || receipt.schema_version !== 1 || receipt.catalog_table_count !== 1 ||
      typeof receipt.schema_fingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(receipt.schema_fingerprint)) {
    throw new Error('The signed registration receipt does not prove the exact run-scoped table and schema.');
  }
}

async function deleteOwnedNamespace(config: Config, namespace: string, runId: string, owner: string, uid: string): Promise<void> {
  const current = await readNamespace(config, namespace);
  if (!current) return;
  await verifyNamespace(config, namespace, runId, owner, uid);
  await kubectl(config, ['delete', 'namespace', namespace, '--wait=true', '--timeout=300s'], 'Delete owned benchmark namespace', 360);
  if (await readNamespace(config, namespace)) throw new Error('Owned namespace remains after deletion.');
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
}

async function executeRun(options: { envFile: string; stampConfigPath: string; runId?: string }): Promise<BenchmarkReport> {
  const runId = options.runId ?? randomUUID();
  if (!uuidPattern.test(runId)) throw new Error('--run-id must be a UUID. Generate a new UUID for every run.');
  const runHex = runId.replaceAll('-', '');
  const owner = randomBytes(24).toString('hex');
  const namespace = `${namespacePrefix}${runHex}`;
  const runDir = resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', runHex);
  const recordPath = resolve(runDir, 'run-fic.json');
  const reportPath = resolve(runDir, 'report.json');
  const secretsPath = resolve(runDir, 'secrets');
  const source = await readFile(manifestPath, 'utf8');
  const env = { ...load_env(options.envFile), ...process.env };
  const stamp = loadStampConfig(options.stampConfigPath);
  const config = new Config(env, stamp);
  if (!config.identity_client_id) throw new Error('The approved stamp workload identity client ID is missing from private configuration.');
  const generation = await capturedGeneration(config, run_tree);
  const relativePath = await checkDeltaRoot(config, runId);
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
  });
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
  let namespaceRemoved = false;
  let runStarted = false;
  let ficRemoved = false;
  let poolsAtZero = false;
  let writer: WriterIdentity | undefined;
  let measurementStart = 0;
  let measurementEnd = 0;
  let metrics: Awaited<ReturnType<typeof readDeltaAddWindow>> | undefined;
  console.log(`Run ${runId}: namespace ${namespace}; report ${reportPath}`);
  try {
    const activeSubscription = await command(['az', 'account', 'show', '--subscription', config.subscription, '--query', 'id', '--output', 'tsv', '--only-show-errors'],
      'Verify Azure login for the approved subscription');
    if (activeSubscription.toLowerCase() !== config.subscription.toLowerCase()) throw new Error('Azure login is not scoped to the approved stamp subscription.');
    await configureKubeAuth(config);
    await requirePoolsAtZero(config);
    runStarted = true;
    await applyStage(config, namespace, renderedPath, 'identity');
    namespaceUid = await prepareNamespace(config, namespace, runId, owner);
    await runFic('prepare-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
    await runFic('probe-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
    await createSecrets(config, namespace, secretsPath);

    await applyStage(config, namespace, renderedPath, 'sql');
    await waitDeployment(config, namespace, 'arrowcol-sql');
    await applyStage(config, namespace, renderedPath, 'migration');
    await waitJob(config, namespace, 'arrowcol-migrate-v11-fresh', 10 * 60_000);
    await applyStage(config, namespace, renderedPath, 'catalog');
    for (const name of ['arrowcol-catalog', 'arrowcol-router', 'delta-maintenance']) await waitDeployment(config, namespace, name);
    await applyStage(config, namespace, renderedPath, 'writer');
    writer = await waitWriter(config, namespace);

    const expiryManifest = replaceInitializerExpiry(rendered, Date.now() + 45 * 60_000);
    await writePrivateFile(renderedPath, expiryManifest);
    await applyStage(config, namespace, renderedPath, 'initializer');
    await waitJob(config, namespace, 'direct-pin-live', 180_000);
    await assertWriterStable(config, namespace, writer);
    await applyStage(config, namespace, renderedPath, 'registration');
    await waitJob(config, namespace, 'direct-catalog-verified', 240_000);
    await verifyRegistration(config, namespace, runId, deltaRoot);
    await assertWriterStable(config, namespace, writer);

    await applyStage(config, namespace, renderedPath, 'load');
    await waitForReadyLoad(config, namespace, writer);
    const scheduleStart = Date.now() + 130_000;
    if (scheduleStart - Date.now() < 125_000) throw new Error('Load schedule must be at least 125 seconds in the future.');
    await kubectl(config, ['patch', 'configmap', 'direct-schedule-confirmwide', '-n', namespace, '--type', 'merge',
      '-p', JSON.stringify({ data: { start: String(scheduleStart) } })], 'Schedule benchmark load');
    measurementStart = scheduleStart + 60_000;
    measurementEnd = measurementStart + 600_000;
    await waitForLoad(config, namespace, writer);
    await assertWriterStable(config, namespace, writer);
    await verifyNamespace(config, namespace, runId, owner, namespaceUid);
    const azure = new Azure(config);
    metrics = await readDeltaAddWindow(azure, relativePath, measurementStart, measurementEnd,
      async () => { await verifyNamespace(config, namespace, runId, owner, namespaceUid); });
    if (metrics.window_rows <= 0 || metrics.window_commits <= 0) throw new Error('The timed Delta window contained no committed rows.');
  } catch (error) {
    primaryError = error;
  }

  const cleanupErrors: Error[] = [];
  try {
    if (namespaceUid) {
      await deleteOwnedNamespace(config, namespace, runId, owner, namespaceUid);
      namespaceRemoved = true;
    } else {
      const current = await readNamespace(config, namespace);
      if (current) throw new Error('Namespace was created but its ownership UID was not captured; refusing deletion.');
      namespaceRemoved = true;
    }
    const record = await lstat(recordPath).then(() => true, error => {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
      throw error;
    });
    if (record) {
      await runFic('cleanup-run-fic', runId, namespace, recordPath, options.envFile, options.stampConfigPath);
      ficRemoved = true;
    } else {
      ficRemoved = true;
    }
    if (runStarted) {
      await waitPoolsAtZero(config);
      poolsAtZero = true;
    } else {
      poolsAtZero = (await Promise.all(workloadPools.map(pool => poolNodeCount(config, pool)))).every(count => count === 0);
    }
  } catch (error) {
    cleanupErrors.push(error instanceof Error ? error : new Error(String(error)));
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
    measurement_seconds: (measurementEnd - measurementStart) / 1000,
    committed_rows: metrics.window_rows,
    committed_rows_per_second: metrics.window_rows / ((measurementEnd - measurementStart) / 1000),
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
  } : undefined;
  if (report) await writePrivateFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  await rm(bootstrapEnv, { force: true });
  await rm(secretsPath, { recursive: true, force: true });
  if (primaryError || cleanupErrors.length || !report || !namespaceRemoved || !ficRemoved || !poolsAtZero) {
    const causes = [primaryError, ...cleanupErrors].filter((error): error is Error => error instanceof Error);
    if (!report) causes.push(new Error('No measured benchmark report was produced.'));
    if (!namespaceRemoved || !ficRemoved || !poolsAtZero) causes.push(new Error('Owned-resource cleanup or workload-pool scale-down is incomplete.'));
    const detail = causes.map(error => config.redact(error.message)).filter(Boolean).join(' | ').slice(0, 2000);
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
      help: { type: 'boolean', short: 'h' },
    } });
    if (values.help) {
      console.log('Usage: npx --no-install tsx projects/arrowcol-oat/aks-direct-benchmark.ts run [--env-file PATH] [--stamp-config YAML] [--run-id UUID]');
      return 0;
    }
    if (positionals.length !== 1 || positionals[0] !== 'run') throw new Error('The only command is run; each invocation creates a new run ID and preserves old Delta data.');
    const report = await executeRun({ envFile: resolve(values['env-file']!), stampConfigPath: resolve(values['stamp-config']!), runId: values['run-id'] });
    console.log(`PASS ${report.committed_rows_per_second.toFixed(2)} committed rows/s; ${report.committed_rows} rows in 600 seconds; Delta v${report.first_delta_version}-v${report.last_delta_version}; report ${resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', report.run_id.replaceAll('-', ''), 'report.json')}`);
    return 0;
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  process.exitCode = await main();
}