import { randomUUID } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { observe_custody_idle_capacity } from '../../infra/azure/aks-safe-idle.js';
import { plan_run_fic } from '../../infra/azure/run_fic.js';
import { CampaignConflictError, CampaignFencedError, CampaignWriteOutcomeUnknownError, isInitialRunInventory, recordCleanupArmOperation, type CampaignState, type CampaignStore, type CleanupArmTarget } from './aks-campaign.js';
import { createOwnedRunCleanupActions, type CleanupMutationAuthority, type ExternalCleanupActions } from './aks-cleanup-executor.js';
import { assertCleaningActor, assertRunCleanupActor, discoverOwnedRunInventory, observeExactKubernetes,
  type CustodyCommandRunner } from './aks-setup-cleanup.js';
import { reconcilePendingArmOperations, verifySafeIdle, type SafeIdleObservation } from './aks-reconcile.js';
import { captureStampKustoBaseline, observeDataPreservation, validateStampKustoBaseline, type DataPreservationReceipt } from './aks-data-safety.js';
import { createDefaultLifecycleActions } from './aks-lifecycle.js';
import { createCellCleanupProviders, type CellStepProviders } from './aks-cell-cleanup.js';
import { createDefaultScaleAdapter, restoreCampaignScaleTrials } from './aks-scale-provider.js';
import { buildAksPoolScalePayload } from './aks-arm-pool.js';

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Provider observation is UNKNOWN.');
  return value as Record<string, unknown>;
}
function integer(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Provider measurement is UNKNOWN.');
  return value;
}
function text(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0) throw new Error('Provider identity is UNKNOWN.');
  return value;
}
const baselineBlob = (state: CampaignState) => `_arrowcol-custody/campaigns/${state.campaignId}/baseline.json`;
function activeOwner(state: CampaignState | undefined, ownerId: string, token: number): asserts state is CampaignState {
  const now = Date.now();
  if (!state || state.status !== 'active' || state.ownerId !== ownerId || state.fencingToken !== token ||
      now >= state.expiresAtMs || now < state.heartbeatAtMs || now - state.heartbeatAtMs >= 180_000 ||
      state.operations.length > 0) throw new Error('Baseline capture requires live producer custody before ARM mutations.');
}
export interface CampaignBaselineReceipt {
  status: 'captured'; campaign_id: string; owner_id: string; fencing_token: number;
  blob: string; etag: string; captured_at_unix_ms: number;
}
export async function captureCleanupBaseline(store: CampaignStore, azure: Azure, ownerId: string, token: number): Promise<CampaignBaselineReceipt> {
  const original = await store.read(), state = original?.state;
  const reader = (current: CampaignState | undefined) => {
    const now = Date.now();
    if (!current || current.ownerId !== ownerId || current.fencingToken !== token ||
        !['active', 'cleaning'].includes(current.status) ||
        current.status === 'active' && (now >= current.expiresAtMs || now < current.heartbeatAtMs ||
          now - current.heartbeatAtMs >= 180_000))
      throw new CampaignFencedError();
  };
  reader(state);
  if (!original || !state) throw new CampaignFencedError();
  if (!original.etag || original.etag === '*' || original.etag.startsWith('W/')) throw new CampaignConflictError();
  const [prior] = await azure.blob('GET', azure.c.checkpoints, baselineBlob(state), undefined, undefined, {}, true, true);
  if (prior !== null) {
    const receipt = await readCleanupBaseline(azure, state), current = await store.read();
    reader(current?.state);
    if (current?.etag !== original.etag) throw new CampaignConflictError();
    return receipt;
  }
  activeOwner(state, ownerId, token);
  if (state.mode !== 'observe' || !isInitialRunInventory(state.runInventory))
    throw new Error('First campaign baseline requires the original initial pre-provision inventory in observe mode.');
  const guard = async () => {
    const current = await store.read();
    activeOwner(current?.state, ownerId, token);
    if (!current || current.etag !== original.etag || current.state.mode !== 'observe' ||
        !isInitialRunInventory(current.state.runInventory) ||
        canonical_json(current.state.runInventory) !== canonical_json(state.runInventory))
      throw new CampaignConflictError();
  };
  await guard();
  const capacity = await observe_custody_idle_capacity(azure);
  if (capacity.status !== 'capacity_idle') throw new Error('Campaign baseline must be verified idle capacity before scale/load.');
  const nonRunKusto = await captureStampKustoBaseline(azure, state, guard);
  const value = { schema_version: 1, campaign_id: state.campaignId, owner_id: ownerId, producer_fencing_token: token,
    started_at_unix_ms: state.startedAtMs, captured_at_unix_ms: Date.now(), capacity, non_run_kusto: nonRunKusto };
  await guard();
  let etag: string;
  try {
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, baselineBlob(state), undefined, canonical_json(value),
      { 'x-ms-blob-type': 'BlockBlob', 'If-None-Match': '*' });
    const [confirmed, version] = await azure.blob('GET', azure.c.checkpoints, baselineBlob(state), undefined, undefined, {}, false, true);
    if (headers[':status'] !== '201' || !headers.etag || version.etag !== headers.etag ||
        confirmed === null || canonical_json(JSON.parse(confirmed.toString()) as unknown) !== canonical_json(value))
      throw new CampaignWriteOutcomeUnknownError();
    etag = headers.etag;
  } catch { throw new CampaignWriteOutcomeUnknownError(); }
  try {
    await guard();
    return await readCleanupBaseline(azure, state, etag);
  } catch { throw new CampaignWriteOutcomeUnknownError(); }
}
async function validatedCleanupBaseline(azure: Azure, state: CampaignState, expectedEtag?: string) {
  const [bytes, version] = await azure.blob('GET', azure.c.checkpoints, baselineBlob(state), undefined, undefined, {}, false, true);
  if (!bytes || bytes.length > 16_777_216 || !version.etag || version.etag.startsWith('W/') ||
      version.etag === '*' || expectedEtag !== undefined && version.etag !== expectedEtag)
    throw new Error('Original durable campaign baseline is UNKNOWN.');
  const value = object(JSON.parse(bytes.toString()) as unknown), capacity = object(value.capacity);
  const system = object(capacity.system_pool), kusto = object(capacity.kusto);
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  const expectedIds = [aksId, ...['systempool', 'benchd64', 'benche64'].map(name => aksId + '/agentPools/' + name), azure.c.cluster_id].sort();
  if (value.schema_version !== 1 || value.campaign_id !== state.campaignId || value.owner_id !== state.ownerId ||
      value.producer_fencing_token !== state.runInventory?.fencing_token || value.started_at_unix_ms !== state.startedAtMs ||
      typeof value.captured_at_unix_ms !== 'number' || !Number.isSafeInteger(value.captured_at_unix_ms) ||
      value.captured_at_unix_ms < state.startedAtMs || value.captured_at_unix_ms > (state.fencedAtMs ?? Date.now()) ||
      state.operations.some(operation => operation.submittedAtMs < Number(value.captured_at_unix_ms)) ||
      capacity.scope !== 'arm_capacity_only' || capacity.status !== 'capacity_idle' ||
      ![2, 3].includes(integer(system.count)) || system.vm_size !== 'Standard_D8ds_v6' ||
      system.autoscale_enabled !== false || system.autoscale_min !== null || system.autoscale_max !== null ||
      kusto.sku !== 'Standard_E4ads_v5' || kusto.capacity !== 2 || kusto.autoscale_min !== 2 || kusto.autoscale_max !== 4 ||
      kusto.autoscale_enabled !== true || kusto.auto_stop_enabled !== true || !Array.isArray(capacity.receipts))
    throw new Error('Original pre-campaign baseline does not authorize the safe-idle rollback target.');
  const ids = capacity.receipts.map(input => {
    const receipt = object(input), observedAt = integer(receipt.observed_at_unix_ms);
    if (receipt.http_status !== 200 || !text(receipt.request_id) || !/^[a-f0-9]{64}$/.test(text(receipt.body_sha256)) ||
        observedAt < state.startedAtMs || observedAt > Number(value.captured_at_unix_ms))
      throw new Error('Original pre-campaign baseline lacks positive, timely resource receipts.');
    return text(receipt.resource_id);
  }).sort();
  if (canonical_json(ids) !== canonical_json(expectedIds)) throw new Error('Original baseline targets another stamp or has incomplete ARM receipts.');
  validateStampKustoBaseline(value.non_run_kusto, azure, state, value.captured_at_unix_ms);
  return { systemCount: integer(system.count), value, etag: version.etag };
}
export async function baselineSystemCount(azure: Azure, state: CampaignState, expectedEtag?: string): Promise<number> {
  return (await validatedCleanupBaseline(azure, state, expectedEtag)).systemCount;
}
export async function readCleanupBaseline(azure: Azure, state: CampaignState, expectedEtag?: string): Promise<CampaignBaselineReceipt> {
  const { value, etag } = await validatedCleanupBaseline(azure, state, expectedEtag);
  return { status: 'captured', campaign_id: state.campaignId, owner_id: state.ownerId,
    fencing_token: integer(value.producer_fencing_token), blob: baselineBlob(state), etag,
    captured_at_unix_ms: integer(value.captured_at_unix_ms) };
}

export interface StampHealthReceipt {
  observed_at_unix_ms: number;
  pods: SafeIdleObservation['pods'];
  pdb_blocked: boolean;
  versions: Array<{ namespace: string; kind: string; resource_version: string }>;
}
export async function observeStampHealth(azure: Azure, state: CampaignState, authority: CleanupMutationAuthority,
  runCommand?: CustodyCommandRunner): Promise<StampHealthReceipt> {
  const plan = state.runInventory?.fic?.plan;
  if (!plan || plan.aksId !== azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster)
    throw new Error('Stamp health requires the exact persisted selected AKS identity.');
  if (canonical_json(plan) !== canonical_json(await plan_run_fic(azure, plan.runId, plan.namespace, plan.serviceAccount)))
    throw new Error('Stamp health server or authoritative ARM identity differs from the original plan.');
  const pods: StampHealthReceipt['pods'] = [], versions: StampHealthReceipt['versions'] = [];
  let blocked = false;
  for (const namespace of [...new Set(['kube-system', azure.c.kubernetesNamespace])]) {
    for (const kind of ['pods', 'poddisruptionbudgets.policy']) {
      const list = await observeExactKubernetes(plan.server, [kind, '--namespace', namespace], authority, runCommand);
      if (!list || !Array.isArray(list.items) || list.kind !== (kind === 'pods' ? 'PodList' : 'PodDisruptionBudgetList'))
        throw new Error('Full stamp pod/PDB observation is UNKNOWN.');
      versions.push({ namespace, kind, resource_version: text(object(list.metadata).resourceVersion) });
      for (const item of list.items) {
        const resource = object(item), metadata = object(resource.metadata), status = object(resource.status);
        if (metadata.namespace !== namespace) throw new Error('Stamp workload response namespace differs.');
        text(metadata.uid);
        if (kind === 'pods') {
          if (status.phase === 'Succeeded') continue;
          if (!Array.isArray(status.conditions)) throw new Error('Stamp pod conditions are UNKNOWN.');
          const conditions = status.conditions.map(object);
          pods.push({ name: namespace + '/' + text(metadata.name),
            ready: conditions.some(condition => condition.type === 'Ready' && condition.status === 'True'),
            scheduled: conditions.some(condition => condition.type === 'PodScheduled' && condition.status === 'True') });
        } else {
          if (integer(status.observedGeneration) < integer(metadata.generation)) throw new Error('PDB status has not observed its generation.');
          if (integer(status.disruptionsAllowed) === 0) blocked = true;
        }
      }
    }
  }
  if (pods.length === 0) throw new Error('No positive stamp pod readiness observation exists.');
  return { observed_at_unix_ms: Date.now(), pods, pdb_blocked: blocked, versions };
}

async function restoreOne(store: CampaignStore, azure: Azure, state: CampaignState, authority: CleanupMutationAuthority,
  resourceId: string, api: string, target: CleanupArmTarget): Promise<void> {
  const operationUrl = `https://management.azure.com${resourceId}?api-version=${api}`;
  if (state.operations.some(operation => operation.cleanupTarget && operation.resourceId.toLowerCase() === resourceId.toLowerCase() &&
      (operation.status !== 'succeeded' || canonical_json(operation.cleanupTarget) === canonical_json(target))))
    throw new Error('Prior restore target is unresolved or drifted; reconcile it instead of blindly replaying ARM.');
  const resource = object(await azure.arm('GET', resourceId, undefined, false, api));
  if (text(resource.id).toLowerCase() !== resourceId.toLowerCase()) throw new Error('Restore ARM resource identity changed.');
  let method: 'PUT' | 'PATCH', payload: Record<string, unknown>;
  if (target.kind === 'aks-pool') {
    payload = buildAksPoolScalePayload(resource, target);
    method = 'PUT';
  } else {
    payload = { sku: { name: target.sku, tier: 'Standard', capacity: target.capacity },
      properties: { optimizedAutoscale: { isEnabled: true, minimum: target.minimum, maximum: target.maximum },
        enableAutoStop: target.autoStop } };
    method = 'PATCH';
  }
  await authority.assertCurrent();
  await recordCleanupArmOperation(store, authority.ownerId, authority.fencingToken,
    { id: randomUUID(), resourceId, operationUrl, submittedAtMs: Date.now(), cleanupTarget: target }, azure.c.subscription);
  await authority.assertCurrent();
  await azure.request(method, operationUrl, 'https://management.azure.com/', canonical_json(payload),
    { 'Content-Type': 'application/json' }, false, false);
}

type DataMeasurements = Pick<SafeIdleObservation, 'qapNotHealthyReasons' | 'qapPendingBacklog' | 'previousQapPendingBacklog' |
  'deltaRowsBefore' | 'deltaRowsAfter' | 'kustoRowsBefore' | 'kustoRowsAfter'>;
export interface FullDataSafetyReceipt {
  campaign_id: string;
  cell_run_id: string;
  observed_at_unix_ms: number;
  evidence_etag: string;
  measurements: DataMeasurements;
}
type IndependentProviders = 'discoverExactInventory' | 'deleteNamespaceIfUidAndOwnerMatch' | 'deleteQapBindingIfOwned' |
  'deleteFicIfExact' | 'restoreSafeIdle' | 'verifySafeIdle';
export interface DefaultCleanupProviderOptions {
  azure: Azure;
  workDirectory: string;
  runCommand?: CustodyCommandRunner;
  lifecycle?: Omit<ExternalCleanupActions, IndependentProviders>;
  readDataSafety?: (state: CampaignState, authority: CleanupMutationAuthority) => Promise<FullDataSafetyReceipt | DataPreservationReceipt>;
  readHealth?: typeof observeStampHealth;
}
export function createDefaultCleanupProviders(store: CampaignStore, options: DefaultCleanupProviderOptions): ExternalCleanupActions {
  const { azure } = options, readHealth = options.readHealth ?? observeStampHealth;
  const lifecycle = options.lifecycle ?? createDefaultLifecycleActions(store, azure, options.runCommand);
  const readDataSafety = options.readDataSafety ?? ((state: CampaignState, authority: CleanupMutationAuthority) =>
    observeDataPreservation(store, azure, state, authority));
  const authorityFor = (state: CampaignState): CleanupMutationAuthority => ({
    mode: state.status === 'active' ? 'producer' : 'controller',
    campaignId: state.campaignId, ownerId: state.ownerId, fencingToken: state.fencingToken,
    assertCurrent: async () => {
      const current = (await store.read())?.state;
      if (!current || current.status !== state.status || !['active', 'cleaning'].includes(current.status) || current.campaignId !== state.campaignId ||
          current.ownerId !== state.ownerId || current.fencingToken !== state.fencingToken ||
          current.runInventory?.cell_run_id !== state.runInventory?.cell_run_id ||
          (current.status === 'cleaning' && !current.cleanupSteps.some(step => step.status === 'running')) ||
          (current.status === 'active' && (Date.now() >= current.expiresAtMs || Date.now() < current.heartbeatAtMs ||
            Date.now() - current.heartbeatAtMs >= 180_000)))
        throw new Error('Default provider cleanup authority changed.');
    },
  });
  const owned = async (authority: CleanupMutationAuthority) => {
    const inventory = await assertRunCleanupActor(store, authority);
    return createOwnedRunCleanupActions(store, azure, inventory.qap?.reference ?? null, options.workDirectory,
      inventory.fic?.plan ?? undefined);
  };
  return {
    ...lifecycle,
    captureTerminalState: async (inventory, key, authority) => {
      await assertCleaningActor(store, authority);
      const token = { ownerId: authority.ownerId, fencingToken: authority.fencingToken };
      const scaleAdapter = createDefaultScaleAdapter(store, { azure, token, runCommand: options.runCommand, authority });
      if (await restoreCampaignScaleTrials(store, scaleAdapter, token) === 'pending') return 'pending';
      return lifecycle.captureTerminalState(inventory, key, authority);
    },
    verifyCellPreservation: async (inventory, authority) => {
      await authority.assertCurrent();
      const state = (await store.read())?.state;
      if (!state || canonical_json(state.runInventory) !== canonical_json(inventory)) throw new Error('Final cell data inventory changed.');
      const result = await observeDataPreservation(store, azure, state, authority);
      if (result.status !== 'preserved') throw new Error('Final cell data preservation is ' + result.status + ': ' + result.reason);
      return 'complete';
    },
    discoverExactInventory: async inventory => {
      const state = (await store.read())?.state;
      if (!state || canonical_json(state.runInventory) !== canonical_json(inventory)) throw new Error('Discovery inventory/version changed.');
      return discoverOwnedRunInventory(azure, inventory, authorityFor(state), options.runCommand);
    },
    deleteNamespaceIfUidAndOwnerMatch: async (namespace, key, authority) => (await owned(authority)).deleteNamespaceIfUidAndOwnerMatch(namespace, key, authority),
    deleteQapBindingIfOwned: async (database, binding, campaignId, key, authority) =>
      (await owned(authority)).deleteQapBindingIfOwned(database, binding, campaignId, key, authority),
    deleteFicIfExact: async (fic, campaignId, key, authority) => (await owned(authority)).deleteFicIfExact(fic, campaignId, key, authority),
    restoreSafeIdle: async (_supplied, _key, authority) => {
      await assertCleaningActor(store, authority);
      await reconcilePendingArmOperations(store, async url => {
        await authority.assertCurrent();
        const [bytes, headers] = await azure.request('GET', url, 'https://management.azure.com/');
        await authority.assertCurrent();
        return { status: Number(headers[':status']), body: bytes ? JSON.parse(bytes.toString()) as unknown : undefined };
      }, azure.c.subscription);
      await assertCleaningActor(store, authority);
      const state = (await store.read())!.state;
      if (state.operations.some(operation => operation.status === 'pending' || operation.status === 'indeterminate')) return 'pending';
      if (state.operations.some(operation => operation.cleanupTarget && operation.status === 'failed'))
        throw new Error('ARM rollback failed; retained durable intent requires explicit reconciliation.');
      const baselineCount = await baselineSystemCount(azure, state), observed = await observe_custody_idle_capacity(azure);
      const health = await readHealth(azure, state, authority, options.runCommand);
      if (observed.kusto.state !== 'Running') throw new Error('Kusto is not Running; never stop/start it as guessed cleanup.');
      if (observed.aks_provisioning_state !== 'Succeeded' || observed.system_pool.provisioning_state !== 'Succeeded' ||
          observed.user_pools.some(pool => pool.provisioning_state !== 'Succeeded') || observed.kusto.provisioning_state !== 'Succeeded')
        return 'pending';
      if (observed.system_pool.vm_size !== 'Standard_D8ds_v6') throw new Error('System-pool SKU drift requires its explicit recorded restore plan.');
      const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
      for (const pool of observed.user_pools) if (pool.count !== 0 || !pool.autoscale_enabled || pool.autoscale_min !== 0 || pool.autoscale_max !== 1) {
        await restoreOne(store, azure, state, authority, aksId + '/agentPools/' + pool.name, '2024-10-01',
          { kind: 'aks-pool', count: 0, autoscale: true, minimum: 0, maximum: 1 });
        return 'pending';
      }
      const priorFallback = state.operations.some(operation => operation.resourceId.endsWith('/agentPools/systempool') &&
        operation.cleanupTarget?.kind === 'aks-pool' && operation.cleanupTarget.count === 3);
      const targetCount = priorFallback || health.pdb_blocked || health.pods.some(pod => !pod.ready || !pod.scheduled) ? baselineCount : 2;
      if ((health.pdb_blocked || health.pods.some(pod => !pod.ready || !pod.scheduled)) && observed.system_pool.count > targetCount)
        return 'pending';
      if (observed.system_pool.count !== targetCount || observed.system_pool.autoscale_enabled) {
        await restoreOne(store, azure, state, authority, aksId + '/agentPools/systempool', '2024-10-01',
          { kind: 'aks-pool', count: targetCount, autoscale: false, minimum: null, maximum: null });
        return 'pending';
      }
      if (observed.kusto.sku !== 'Standard_E4ads_v5' || observed.kusto.capacity !== 2 ||
          !observed.kusto.autoscale_enabled || observed.kusto.autoscale_min !== 2 || observed.kusto.autoscale_max !== 4 || !observed.kusto.auto_stop_enabled) {
        await restoreOne(store, azure, state, authority, azure.c.cluster_id, '2024-04-13',
          { kind: 'kusto', sku: 'Standard_E4ads_v5', capacity: 2, minimum: 2, maximum: 4, autoStop: true });
        return 'pending';
      }
      return observed.status === 'capacity_idle' ? 'complete' : 'pending';
    },
    verifySafeIdle: async state => {
      const authority = authorityFor(state);
      await assertCleaningActor(store, authority);
      await authority.assertCurrent();
      const capacity = await observe_custody_idle_capacity(azure), health = await readHealth(azure, state, authority, options.runCommand);
      if (capacity.status !== 'capacity_idle') return 'pending';
      const data = await readDataSafety(state, authority);
      if (data.campaign_id !== state.campaignId || data.cell_run_id !== state.runInventory?.cell_run_id ||
          !Number.isSafeInteger(data.observed_at_unix_ms) || data.observed_at_unix_ms < state.fencedAtMs! ||
          data.observed_at_unix_ms > Date.now() || !data.evidence_etag) throw new Error('Full data safety receipt is stale, incomplete or belongs to another run.');
      if ('scope' in data) {
        if (data.scope !== 'delta_and_non_run_kusto_preservation' || data.status !== 'preserved')
          throw new Error('Full stamp data preservation is ' + data.status + ': ' + data.reason);
        if (health.pdb_blocked || health.pods.some(pod => !pod.ready || !pod.scheduled)) return 'pending';
        await authority.assertCurrent();
        return 'complete';
      }
      const observation: SafeIdleObservation = { ...data.measurements,
        aksProvisioningState: capacity.aks_provisioning_state, kustoProvisioningState: capacity.kusto.provisioning_state,
        systemPoolCount: capacity.system_pool.count,
        userPools: capacity.user_pools.map(pool => ({ name: pool.name, nodeCount: pool.count, autoscaleMin: pool.autoscale_min, autoscaleMax: pool.autoscale_max })),
        pods: health.pods, kustoSku: capacity.kusto.sku, kustoCapacity: capacity.kusto.capacity,
        kustoAutoscaleMin: capacity.kusto.autoscale_min, kustoAutoscaleMax: capacity.kusto.autoscale_max,
        kustoAutoStopEnabled: capacity.kusto.auto_stop_enabled };
      await authority.assertCurrent();
      if (capacity.status !== 'capacity_idle' || !verifySafeIdle(observation).safe) return 'pending';
      return 'complete';
    },
  };
}

export function createDefaultCellCleanupProviders(actions: ExternalCleanupActions): CellStepProviders {
  const providers = createCellCleanupProviders(actions);
  for (const selector of ['stop_admission', 'bounded_drain', 'evidence_capture', 'stop_maintenance', 'terminal_capture'] as const) {
    providers[selector]!.recover = async (inventory, key, authority) => {
      if (!actions.recoverLifecycle || await actions.recoverLifecycle(selector, inventory,
        inventory.cleanup[selector].deadline_unix_ms, key, authority) !== 'complete')
        throw new Error('Lifecycle recovery remains pending or UNKNOWN; never execute as fallback.');
      return inventory;
    };
  }
  for (const selector of ['remove_namespace', 'remove_qap', 'remove_fic'] as const)
    providers[selector]!.recover = providers[selector]!.execute;
  return providers;
}
