import { createHash, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { Azure, Config, load_env, preflight } from '../../../infra/azure/bootstrap.ts';
import { PublisherControl } from '../../../infra/azure/publisher_control.ts';
import { runProcess } from '../../../tools/libs/experiment-kit/src/experiment.js';

export interface Manifest {
  format: 1;
  runId: string;
  generation: string;
  environmentHash: string;
  createdAt: string;
  phase: 'planned' | 'deploying' | 'ready' | 'cleaning' | 'cleaned';
}

const canonicalUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

class ExperimentAzure extends Azure {
  constructor(config: Config, public signal?: AbortSignal) { super(config); }

  override async cli(...args: string[]): Promise<unknown> {
    const deadline = AbortSignal.timeout(90_000);
    const signal = this.signal ? AbortSignal.any([this.signal, deadline]) : deadline;
    const result = await runProcess('az', [...args, '--only-show-errors', '--output', 'json'], { signal });
    if (result.aborted || result.exitCode !== 0 || !result.outputClosed ||
        result.stdoutTruncated || result.stderrTruncated)
      throw new Error(this.c.redact(`Azure CLI failed or exceeded its bounded deadline: ${result.stderr}`));
    const parsed: unknown = result.stdout.trim() ? JSON.parse(result.stdout) : null;
    return parsed;
  }
}

export function names(runId: string): { database: string; data: string; checkpoints: string; deployment: string } {
  if (!canonicalUuid.test(runId)) throw new Error('Experiment run ID must be a canonical UUID.');
  const suffix = runId.replaceAll('-', '');
  return { database: `qapexp-${suffix}`, data: `qapdata-${suffix}`,
    checkpoints: `qapcontrol-${suffix}`, deployment: `qapexp-${suffix}` };
}

export function environmentHash(config: Config): string {
  return createHash('sha256').update(JSON.stringify([
    config.subscription, config.group, config.account, config.cluster, config.identity,
  ])).digest('hex');
}

export function validateManifest(value: unknown, config: Config): Manifest {
  if (!value || typeof value !== 'object') throw new Error('Malformed experiment ownership manifest.');
  const item = value as Record<string, unknown>;
  if (Object.keys(item).sort().join(',') !== 'createdAt,environmentHash,format,generation,phase,runId' ||
      item.format !== 1 || typeof item.runId !== 'string' || !canonicalUuid.test(item.runId) ||
      typeof item.generation !== 'string' || !canonicalUuid.test(item.generation) ||
      typeof item.createdAt !== 'string' || !Number.isFinite(Date.parse(item.createdAt)) ||
      item.environmentHash !== environmentHash(config) ||
      (item.phase !== 'planned' && item.phase !== 'deploying' && item.phase !== 'ready' &&
        item.phase !== 'cleaning' && item.phase !== 'cleaned'))
    throw new Error('Manifest is invalid or belongs to a different Azure environment; no resources changed.');
  return { format: 1, runId: item.runId, generation: item.generation,
    createdAt: item.createdAt, environmentHash: item.environmentHash, phase: item.phase };
}

export function saveManifest(filename: string, manifest: Manifest): void {
  fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
  const temporary = filename + '.' + randomUUID();
  const fd = fs.openSync(temporary, 'wx', 0o600);
  try {
    fs.writeFileSync(fd, JSON.stringify(manifest, null, 2) + '\n');
    fs.fsyncSync(fd);
  } finally { fs.closeSync(fd); }
  try { fs.renameSync(temporary, filename); }
  finally { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); }
}

export class Resources {
  readonly azure: ExperimentAzure;
  readonly config: Config;
  readonly manifest: Manifest;
  readonly filename: string;
  readonly values: Record<string, string>;
  readonly databaseId: string;
  readonly dataId: string;
  readonly checkpointsId: string;
  readonly deploymentId: string;

  constructor(envFile: string, filename: string, existing = false, signal?: AbortSignal) {
    this.values = load_env(envFile);
    const base = new Config(this.values);
    if (existing) {
      this.manifest = validateManifest(JSON.parse(fs.readFileSync(filename, 'utf8')), base);
    } else {
      if (fs.existsSync(filename)) throw new Error('Ownership manifest already exists; choose a fresh report directory.');
      this.manifest = { format: 1, runId: randomUUID(), generation: randomUUID(),
        environmentHash: environmentHash(base), createdAt: new Date().toISOString(), phase: 'planned' };
    }
    this.filename = filename;
    const owned = names(this.manifest.runId);
    this.config = new Config({ ...this.values, AZURE_KUSTO_DATABASE: owned.database,
      AZURE_STORAGE_CONTAINER: owned.data, AZURE_STORAGE_CHECKPOINT_CONTAINER: owned.checkpoints,
      ARROWCOL_AZURE_TABLE_ROOT: `abfss://${owned.data}@${base.account}.dfs.core.windows.net/` });
    this.azure = new ExperimentAzure(this.config, signal);
    this.databaseId = this.config.cluster_id + '/databases/' + owned.database;
    this.dataId = this.config.storage_id + '/blobServices/default/containers/' + owned.data;
    this.checkpointsId = this.config.storage_id + '/blobServices/default/containers/' + owned.checkpoints;
    this.deploymentId = this.config.base + '/providers/Microsoft.Resources/deployments/' + owned.deployment;
    if (!existing) saveManifest(filename, this.manifest);
  }

  environment(): NodeJS.ProcessEnv {
    return { ...process.env, ...this.values, ...this.config.generated(),
      ARROWCOL_RESET_GENERATION: this.manifest.generation,
      ARROWCOL_DELTA_ROOT: this.config.table_root, ARROWCOL_PARTITION_GRANULARITY: 'minute' };
  }

  useSignal(signal: AbortSignal): void { this.azure.signal = signal; }

  async deploy(signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    if (this.manifest.phase !== 'planned') throw new Error('An experiment cannot redeploy an existing authority.');
    await preflight(this.azure);
    for (const id of [this.databaseId, this.dataId, this.checkpointsId]) {
      signal.throwIfAborted();
      if (await this.azure.arm('GET', id, undefined, true,
        id === this.databaseId ? '2024-04-13' : '2023-05-01'))
        throw new Error('An experiment resource already exists; refusing to adopt or modify it.');
    }
    this.manifest.phase = 'deploying';
    saveManifest(this.filename, this.manifest);
    await this.azure.cli('deployment', 'group', 'create', '--subscription', this.config.subscription,
      '--resource-group', this.config.group, '--name', names(this.manifest.runId).deployment,
      '--no-wait',
      '--template-file', path.join(root, 'experiments/azure-qap/resources.bicep'),
      '--parameters', `runId=${this.manifest.runId}`, `generation=${this.manifest.generation}`,
      `sandboxId=${this.config.sandbox}`,
      `storageAccountName=${this.config.account}`, `clusterName=${this.config.cluster}`,
      `identityName=${this.config.identity}`, `location=${this.config.location}`);
    for (;;) {
      signal.throwIfAborted();
      const deployed: unknown = await this.azure.arm('GET', this.deploymentId, undefined, false, '2022-09-01');
      const properties = deployed && typeof deployed === 'object'
        ? (deployed as Record<string, unknown>).properties : undefined;
      const state = properties && typeof properties === 'object'
        ? (properties as Record<string, unknown>).provisioningState : undefined;
      if (state === 'Succeeded') break;
      if (state === 'Failed' || state === 'Canceled')
        throw new Error('Experiment resource deployment failed; inspect the owned deployment before cleanup.');
      if (typeof state !== 'string') throw new Error('Experiment deployment returned no provisioning state.');
      await sleep(2000, undefined, { signal });
    }
    signal.throwIfAborted();
    const [metadata, registry] = await new PublisherControl(this.azure).read();
    if (metadata.resetgeneration !== this.manifest.generation || metadata.resetstate !== 'ready-v1' ||
        Object.keys(registry).length || metadata.occupancy !== 'released')
      throw new Error('Experiment publisher authority did not initialize exactly as declared.');
    this.manifest.phase = 'ready';
    saveManifest(this.filename, this.manifest);
  }

  async cleanup(execute: boolean, signal?: AbortSignal): Promise<string[]> {
    signal?.throwIfAborted();
    if (this.manifest.phase === 'cleaned') return [];
    const plan = [this.databaseId, this.dataId, this.checkpointsId, this.deploymentId];
    if (!execute) return plan;
    const deployment: unknown = await this.azure.arm('GET', this.deploymentId, undefined, true, '2022-09-01');
    if (!deployment) {
      if (this.manifest.phase !== 'planned')
        throw new Error('Deployment ownership evidence is absent; automatic cleanup refused.');
      for (const id of [this.databaseId, this.dataId, this.checkpointsId]) {
        if (await this.azure.arm('GET', id, undefined, true,
          id === this.databaseId ? '2024-04-13' : '2023-05-01'))
          throw new Error('Unproven resources exist without deployment evidence; cleanup refused.');
      }
    } else {
      const outputs = deploymentOutputs(deployment);
      if (outputs.experimentRunId !== this.manifest.runId || outputs.databaseId !== this.databaseId ||
          outputs.dataId !== this.dataId || outputs.checkpointsId !== this.checkpointsId ||
          typeof outputs.readerRoleId !== 'string' ||
          !outputs.readerRoleId.startsWith(this.dataId + '/providers/Microsoft.Authorization/roleAssignments/') ||
          !canonicalUuid.test(outputs.readerRoleId.split('/').at(-1) ?? ''))
        throw new Error('Deployment ownership outputs do not match this run; cleanup refused.');
      const checkpoints: unknown = await this.azure.arm('GET', this.checkpointsId, undefined, true, '2023-05-01');
      const control = new PublisherControl(this.azure);
      if (checkpoints) {
        signal?.throwIfAborted();
        await this.assertContainerOwner(this.config.checkpoints);
        const [metadata] = await control.read();
        if (metadata.resetstate !== 'cleaning-v1') {
          await control.fence(this.manifest.generation);
          await control.claim_cleanup(this.manifest.runId, this.manifest.generation);
        }
        await control.cleanup_operator(this.manifest.runId, this.manifest.generation);
        this.manifest.phase = 'cleaning';
        saveManifest(this.filename, this.manifest);
      } else if (this.manifest.phase !== 'cleaning') {
        throw new Error('Publisher authority disappeared before owned cleanup; deletion refused.');
      }
      if (await this.azure.arm('GET', this.dataId, undefined, true, '2023-05-01'))
        await this.assertContainerOwner(this.config.data);
      signal?.throwIfAborted();
      await this.azure.arm('DELETE', this.databaseId, undefined, true);
      const deadline = Date.now() + 600_000;
      while (await this.azure.arm('GET', this.databaseId, undefined, true)) {
        signal?.throwIfAborted();
        if (Date.now() >= deadline) throw new Error('Database deletion is still pending; cleanup can be retried.');
        await sleep(2000, undefined, { signal });
      }
      if (checkpoints) await control.cleanup_operator(this.manifest.runId, this.manifest.generation);
      signal?.throwIfAborted();
      await this.azure.arm('DELETE', outputs.readerRoleId, undefined, true, '2022-04-01');
      signal?.throwIfAborted();
      await this.azure.arm('DELETE', this.dataId, undefined, true, '2023-05-01');
      if (await this.azure.arm('GET', this.dataId, undefined, true, '2023-05-01'))
        throw new Error('Data container deletion is still pending; cleanup can be retried.');
      if (checkpoints) await control.cleanup_operator(this.manifest.runId, this.manifest.generation);
      signal?.throwIfAborted();
      await this.azure.arm('DELETE', this.checkpointsId, undefined, true, '2023-05-01');
      if (await this.azure.arm('GET', this.checkpointsId, undefined, true, '2023-05-01'))
        throw new Error('Checkpoint container deletion is still pending; cleanup can be retried.');
      signal?.throwIfAborted();
      await this.azure.arm('DELETE', this.deploymentId, undefined, true, '2022-09-01');
    }
    signal?.throwIfAborted();
    this.manifest.phase = 'cleaned';
    saveManifest(this.filename, this.manifest);
    return plan;
  }

  private async assertContainerOwner(container: string): Promise<void> {
    const [, headers] = await this.azure.blob('HEAD', container, '', { restype: 'container' });
    if (headers['x-ms-meta-arrowcolexperiment'] !== this.manifest.runId)
      throw new Error('Container ownership marker differs; cleanup refused.');
  }
}

function deploymentOutputs(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object') throw new Error('Malformed deployment ownership response.');
  const properties = (value as Record<string, unknown>).properties;
  if (!properties || typeof properties !== 'object') throw new Error('Deployment properties are absent.');
  const outputs = (properties as Record<string, unknown>).outputs;
  if (!outputs || typeof outputs !== 'object') throw new Error('Deployment outputs are absent; inspect the failed deployment before cleanup.');
  return Object.fromEntries(Object.entries(outputs).map(([key, item]) =>
    [key, item && typeof item === 'object' ? (item as Record<string, unknown>).value : undefined]));
}
