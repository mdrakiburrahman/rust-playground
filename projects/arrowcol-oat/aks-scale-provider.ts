import { setTimeout as delay } from 'node:timers/promises';
import { canonical_json, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, processEffectRequest } from '../../infra/azure/effect-boundary.js';
import { assertCleaningActor, assertRunCleanupActor, observeExactKubernetes, type CustodyCommandRunner } from './aks-setup-cleanup.js';
import { buildAksPoolScalePayload } from './aks-arm-pool.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { assertRunInventoryReady } from './aks-campaign.js';
import type { CampaignState, CampaignStore, RunInventory, RunInventoryResource, ScaleResourceIdentity, ScaleTarget, ScaleTrialReceipt } from './aks-campaign.js';
import { newScaleTrialReceipt, persistScaleTrial, type ScaleAdapter, type ScaleCustodyToken, type ScaleMeasurement, type ScaleStep, type ScaleTrialResult } from './aks-scale.js';
import { validateRuntime, type ResourceRequirements, type RuntimeConfig } from './runtime.js';

interface Observation { identity: ScaleResourceIdentity; value: number | string; pending: boolean }
interface ScaleEffects {
  assertCustody(token: ScaleCustodyToken): Promise<{ state: NonNullable<Awaited<ReturnType<CampaignStore['read']>>>['state']; inventory: RunInventory }>;
  observe(target: ScaleTarget, receipt?: ScaleTrialReceipt): Promise<Observation>;
  apply(target: ScaleTarget, receipt: ScaleTrialReceipt): Promise<void>;
}

const phaseInFlight = new Set(['apply-intent', 'applied', 'restore-intent', 'unknown']);
const exact = (a: unknown, b: unknown): boolean => canonical_json(a) === canonical_json(b);
function object(value: unknown, label: string): Record<string, any> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(label + ' is UNKNOWN.');
  return value as Record<string, any>;
}
function oneResource(inventory: RunInventory, role: RunInventoryResource['role']): RunInventoryResource {
  const values = inventory.resources.filter(item => item.role === role && item.kind === 'Deployment');
  if (values.length !== 1 || values[0]!.state !== 'observed' || !values[0]!.uid)
    throw new Error('Scale target does not have one observed, UID-bound Deployment for role ' + role + '.');
  return values[0]!;
}
function quantityCpu(value: unknown): number {
  if (typeof value !== 'string') throw new Error('Observed CPU limit is UNKNOWN.');
  const match = /^(\d+)(m)?$/.exec(value);
  if (!match) throw new Error('Observed CPU limit is outside the integer-core scale contract.');
  const milli = Number(match[1]);
  if (!Number.isSafeInteger(milli) || match[2] && milli % 1000 !== 0) throw new Error('Observed CPU limit is not an integer core count.');
  return match[2] ? milli / 1000 : milli;
}
function quantityMemory(value: unknown): number {
  if (typeof value !== 'string') throw new Error('Observed memory limit is UNKNOWN.');
  const match = /^(\d+)(Gi|Mi)$/.exec(value);
  if (!match) throw new Error('Observed memory limit is outside the GiB scale contract.');
  const amount = Number(match[1]);
  if (!Number.isSafeInteger(amount) || match[2] === 'Mi' && amount % 1024 !== 0) throw new Error('Observed memory limit is not a whole GiB value.');
  return match[2] === 'Gi' ? amount : amount / 1024;
}
function resourceIdentity(transport: ScaleResourceIdentity['transport'], resourceId: string, apiVersion: string | null,
  kind: string, namespace: string | null, name: string, uid: string | null): ScaleResourceIdentity {
  return { transport, resource_id: resourceId, api_version: apiVersion, kind, namespace, name, uid };
}
function scaleReceipt(state: CampaignState, target: ScaleTarget): ScaleTrialReceipt {
  const matches = (state.scaleTrials ?? []).filter(item => exact(item.target, target) || exact(item.restore, target));
  const receipt = matches.at(-1);
  if (!receipt) throw new Error('Scale target has no durable intent receipt.');
  return receipt;
}
export function createDefaultScaleAdapter(store: CampaignStore, options: {
  azure: Azure; token: ScaleCustodyToken; measure?: () => Promise<ScaleMeasurement>; beginMeasurement?: () => void | Promise<void>; runtimeConfig?: unknown;
  runCommand?: CustodyCommandRunner; authority?: CleanupMutationAuthority; now?: () => number;
}): ScaleAdapter {
  const runCommand = options.runCommand ?? runtime.run_tree;
  const runtimeConfig = options.runtimeConfig === undefined ? undefined : validateRuntime(options.runtimeConfig);
  let authority = options.authority;
  const effects: ScaleEffects = {
    async assertCustody(token) {
      if (token.ownerId !== options.token.ownerId || token.fencingToken !== options.token.fencingToken)
        throw new Error('Scale actor differs from the configured custody token.');
      const current = await store.read();
      if (!current) throw new Error('Scale campaign record is absent.');
      const state = current.state;
      authority ??= { mode: 'producer', campaignId: state.campaignId, ownerId: token.ownerId, fencingToken: token.fencingToken,
        assertCurrent: async () => {
          const latest = (await store.read())?.state;
          if (!latest || latest.campaignId !== state.campaignId || latest.ownerId !== token.ownerId || latest.fencingToken !== token.fencingToken)
            throw new Error('Scale custody changed before provider admission.');
        } };
      let inventory: RunInventory;
      if (authority.mode === 'controller') inventory = await assertCleaningActor(store, authority);
      else {
        await assertRunCleanupActor(store, authority);
        const ready = await assertRunInventoryReady(store, token.ownerId, token.fencingToken, options.now?.() ?? Date.now());
        if (ready.campaignId !== authority.campaignId) throw new Error('Scale inventory belongs to another campaign.');
        inventory = ready.runInventory!;
      }
      if (authority.campaignId !== state.campaignId || authority.ownerId !== token.ownerId || authority.fencingToken !== token.fencingToken)
        throw new Error('Scale authority differs from the active campaign fence.');
      if (runtimeConfig && (runtimeConfig.namespace !== inventory.namespace.name || runtimeConfig.oatRunId !== inventory.cell_run_id ||
          runtimeConfig.subscriptionId !== options.azure.c.subscription || runtimeConfig.stamp !== options.azure.c.stamp)) throw new Error('Runtime resource contract differs from the selected RunInventory/stamp.');
      if (authority.mode !== 'controller') {
        if (state.status !== 'active' || state.mode !== 'armed') throw new Error('Scale requires an active armed campaign.');
      } else if (!state.cleanupSteps.some(step => ['record-terminal-state', 'restore-safe-idle'].includes(step.step) && step.status === 'running')) {
        throw new Error('Scale restoration requires the running terminal-state or safe-idle cleanup step.');
      }
      return { state, inventory };
    },
    async observe(target, receipt) {
      const { state, inventory } = await effects.assertCustody(options.token);
      if (receipt) {
        const latest = scaleReceipt(state, target);
        if (!exact(latest.resource, receipt.resource)) throw new Error('Scale receipt changed during observation.');
      }
      if (['collector-cpu-limit', 'collector-memory-limit', 'loadgen-max'].includes(target.dimension)) {
        if (!inventory.fic?.plan?.server) throw new Error('RunInventory has no exact Kubernetes API server binding.');
        const role = target.dimension === 'loadgen-max' ? 'client' : 'collector';
        const inventoryResource = oneResource(inventory, role);
        const expected = resourceIdentity('kubernetes', `apps/v1/Deployment/${inventory.namespace.name}/${inventoryResource.name}/${inventoryResource.uid}`,
          'apps/v1', 'Deployment', inventory.namespace.name, inventoryResource.name, inventoryResource.uid);
        if (receipt && !exact(receipt.resource, expected)) throw new Error('Scale Deployment identity differs from its immutable RunInventory binding.');
        const observed = await observeExactKubernetes(inventory.fic.plan.server,
          ['deployment', inventoryResource.name, '--namespace', inventory.namespace.name], authority!, runCommand);
        if (!observed) throw new Error('Scale Deployment is absent.');
        const metadata = object(observed.metadata, 'Scale Deployment metadata'), spec = object(observed.spec, 'Scale Deployment spec');
        if (metadata.uid !== inventoryResource.uid || metadata.namespace !== inventory.namespace.name || metadata.name !== inventoryResource.name)
          throw new Error('Scale Deployment UID/name/namespace differs from RunInventory.');
        const replicas = spec.replicas;
        if (!Number.isSafeInteger(replicas) || replicas < 1) throw new Error('Scale Deployment replica count is UNKNOWN.');
        let value: number;
        if (target.dimension === 'loadgen-max') value = replicas;
        else {
          if (role === 'collector' && replicas !== 1) throw new Error('The one-ArrowCol-writer replica invariant is not satisfied.');
          const podSpec = object(spec.template?.spec, 'Scale Deployment pod spec');
          const containers = podSpec.containers;
          if (!Array.isArray(containers) || containers.length !== 1 || containers[0].name !== 'dataflow')
            throw new Error('Collector Deployment does not have the exact single dataflow container.');
          const resources = object(containers[0].resources, 'Scale container resources');
          const limits = object(resources.limits, 'Scale container limits');
          const requests = object(resources.requests, 'Scale container requests');
          const requirements: ResourceRequirements = { requests: { cpu: String(requests.cpu), memory: String(requests.memory) },
            limits: { cpu: String(limits.cpu), memory: String(limits.memory) } };
          if (runtimeConfig) validateRuntime({ ...runtimeConfig, resources: { ...runtimeConfig.resources, collector: requirements } });
          value = target.dimension === 'collector-cpu-limit' ? quantityCpu(limits.cpu) : quantityMemory(limits.memory);
        }
        const status = object(observed.status ?? {}, 'Scale Deployment status');
        const pending = status.observedGeneration !== metadata.generation || status.availableReplicas !== replicas;
        return { identity: expected, value, pending };
      }
      const aksId = options.azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + options.azure.c.cluster;
      let id: string, api: string, kind: string, name: string;
      if (target.dimension === 'collector-node-max') {
        id = aksId + '/agentPools/benche64'; api = '2024-10-01'; kind = 'Microsoft.ContainerService/managedClusters/agentPools'; name = 'benche64';
      } else if (target.dimension === 'system-pool-count') {
        throw new Error('System-pool count is not an approved scale dimension; the system pool remains governed by safe-idle custody.');
      } else if (target.dimension === 'kusto-capacity' || target.dimension === 'kusto-sku') {
        id = options.azure.c.cluster_id; api = '2024-04-13'; kind = 'Microsoft.Kusto/clusters'; name = options.azure.c.cluster;
      } else throw new Error('Scale dimension has no production resource binding.');
      const actual = object(await options.azure.arm('GET', id, undefined, false, api), 'Scale ARM resource');
      if (actual.id !== id || actual.name !== name) throw new Error('Scale ARM identity differs from the selected stamp binding.');
      const properties = object(actual.properties, 'Scale ARM properties');
      const provisioning = properties.provisioningState;
      if (target.dimension === 'collector-node-max') {
        if (properties.enableAutoScaling !== true || properties.minCount !== 0 || !Number.isSafeInteger(properties.maxCount))
          throw new Error('Collector node-pool autoscale state is outside the recorded zero-minimum contract.');
        return { identity: resourceIdentity('azure-rest', id, api, kind, null, name, null), value: properties.maxCount as number,
          pending: provisioning !== 'Succeeded' };
      }
      const sku = object(actual.sku, 'Scale Kusto SKU');
      if (sku.tier !== 'Standard' || !Number.isSafeInteger(sku.capacity)) throw new Error('Kusto SKU/capacity state is UNKNOWN.');
      return { identity: resourceIdentity('azure-rest', id, api, kind, null, name, null),
        value: target.dimension === 'kusto-capacity' ? sku.capacity as number : String(sku.name), pending: provisioning !== 'Succeeded' };
    },
    async apply(target, receipt) {
      const { inventory } = await effects.assertCustody(options.token);
      if (authority?.mode === 'controller' && !exact(target, receipt.restore)) throw new Error('Cleanup may only apply the recorded restore target.');
      if (!exact(receipt.target, target) && !exact(receipt.restore, target)) throw new Error('Scale apply target differs from the durable receipt.');
      const current = await effects.observe(target, receipt);
      if (current.pending || current.value !== target.before) throw new Error('Scale target no longer matches the durable exact before-state.');
      if (['collector-cpu-limit', 'collector-memory-limit', 'loadgen-max'].includes(target.dimension)) {
        const runtimeConfigForTarget = runtimeConfig;
        if (!runtimeConfigForTarget && authority?.mode !== 'controller') throw new Error('Producer scale requires the typed D45 runtime resource contract.');
        const resource = target.dimension === 'loadgen-max' ? oneResource(inventory, 'client') : oneResource(inventory, 'collector');
        const deployment = await observeExactKubernetes(inventory.fic!.plan!.server!,
          ['deployment', resource.name, '--namespace', inventory.namespace.name], authority!, runCommand);
        if (!deployment) throw new Error('Scale Deployment disappeared before patch admission.');
        const metadata = object(deployment.metadata, 'Scale Deployment metadata');
        if (typeof metadata.resourceVersion !== 'string' || metadata.resourceVersion.length === 0)
          throw new Error('Scale Deployment resourceVersion is UNKNOWN.');
        const spec = object(deployment.spec, 'Scale Deployment spec');
        const patch = target.dimension === 'loadgen-max' ? [{ op: 'test', path: '/metadata/uid', value: resource.uid },
          { op: 'test', path: '/metadata/resourceVersion', value: metadata.resourceVersion },
          { op: 'replace', path: '/spec/replicas', value: target.after }] : (() => {
            const containers = object(spec.template?.spec, 'Scale pod spec').containers;
            if (!Array.isArray(containers) || containers.length !== 1 || containers[0].name !== 'dataflow')
              throw new Error('Collector container changed before patch admission.');
            const field = target.dimension === 'collector-cpu-limit' ? 'cpu' : 'memory';
            const oldRequirements = object(containers[0].resources, 'Scale resources');
            const oldLimits = object(oldRequirements.limits, 'Scale limits');
            const nextLimits = { ...oldLimits, [field]: target.dimension === 'collector-cpu-limit' ? String(target.after) : `${target.after}Gi` };
            const requests = object(oldRequirements.requests, 'Scale requests');
            if (runtimeConfigForTarget) validateRuntime({ ...runtimeConfigForTarget, resources: { ...runtimeConfigForTarget.resources,
              collector: { requests: { cpu: String(requests.cpu), memory: String(requests.memory) },
                limits: { cpu: String(nextLimits.cpu), memory: String(nextLimits.memory) } } } });
            return [{ op: 'test', path: '/metadata/uid', value: resource.uid },
              { op: 'test', path: '/metadata/resourceVersion', value: metadata.resourceVersion },
              { op: 'replace', path: `/spec/template/spec/containers/0/resources/limits/${field}`,
                value: nextLimits[field] }];
          })();
        const args = ['kubectl', '--server', inventory.fic!.plan!.server!, '--insecure-skip-tls-verify=false',
          '--request-timeout', '30s', 'patch', 'deployment', resource.name, '--namespace', inventory.namespace.name,
          '--type=json', '--patch', JSON.stringify(patch)];
        const response = await dispatchExternalEffect(processEffectRequest(args), () => runCommand(args, 35));
        if (response.returncode !== 0) throw new Error('Kubernetes scale patch failed with exit ' + response.returncode + '.');
        return;
      }
      const id = receipt.resource.resource_id, api = receipt.resource.api_version!;
      let body: Record<string, unknown> | undefined;
      let method: 'PUT' | 'PATCH' = 'PATCH';
      if (target.dimension === 'collector-node-max') {
        if (typeof target.after !== 'number' || typeof target.before !== 'number') throw new Error('Collector node maximum target is malformed.');
        const live = object(await options.azure.arm('GET', id, undefined, false, api), 'Scale agent pool');
        const properties = object(live.properties, 'Scale agent-pool properties');
        if (live.id !== id || properties.maxCount !== target.before || !Number.isSafeInteger(properties.count))
          throw new Error('Collector agent-pool exact before-state changed.');
        body = buildAksPoolScalePayload(live, { count: properties.count as number, autoscale: true, minimum: 0, maximum: target.after });
        method = 'PUT';
      } else if (target.dimension === 'kusto-capacity' || target.dimension === 'kusto-sku') {
        const live = object(await options.azure.arm('GET', id, undefined, false, api), 'Scale Kusto resource');
        if (live.id !== id || live.name !== options.azure.c.cluster) throw new Error('Kusto identity changed before scale patch.');
        const sku = object(live.sku, 'Scale Kusto SKU');
        if (sku.tier !== 'Standard' || !Number.isSafeInteger(sku.capacity)) throw new Error('Kusto SKU/capacity state is UNKNOWN.');
        if (target.dimension === 'kusto-capacity' && sku.capacity !== target.before ||
            target.dimension === 'kusto-sku' && sku.name !== target.before) throw new Error('Kusto before-state changed before patch admission.');
        body = { sku: { name: target.dimension === 'kusto-sku' ? target.after : sku.name, tier: sku.tier,
          capacity: target.dimension === 'kusto-capacity' ? target.after : sku.capacity } };
      }
      if (!body) throw new Error('Scale target has no approved ARM patch shape.');
      await options.azure.arm(method, id, body, false, api);
    },
  };
  const token = options.token;
  const transition = async (receipt: ScaleTrialReceipt, phase: ScaleTrialReceipt['phase'], result?: ScaleTrialReceipt['result']) => {
    const next = { ...receipt, phase, ...(result ? { result } : {}) };
    await persistScaleTrial(store, next, token, options.now?.() ?? Date.now());
    return next;
  };
  const find = async (target: ScaleTarget): Promise<ScaleTrialReceipt> => {
    const state = (await store.read())?.state;
    if (!state) throw new Error('Scale campaign record is absent.');
    return scaleReceipt(state, target);
  };
  return {
    async assertCustody(candidate) { await effects.assertCustody(candidate); },
    async persistIntent(target, candidate, trial) {
      await effects.assertCustody(candidate);
      const state = (await store.read())?.state;
      if (!state) throw new Error('Scale campaign record is absent.');
      const existing = (state.scaleTrials ?? []).filter(item => exact(item.target, target) || exact(item.restore, target)).at(-1);
      if (existing) {
        if (exact(existing.restore, target) && ['applied', 'kept', 'unknown'].includes(existing.phase))
          await transition(existing, 'restore-intent');
        return;
      }
      if (!trial || authority?.mode === 'controller') throw new Error('No matching durable scale intent exists for this target.');
      const observed = await effects.observe(target);
      if (observed.pending || observed.value !== target.before) throw new Error('Scale intent requires an exact observed before-state.');
      const receipt = newScaleTrialReceipt({ state, step: trial, resource: observed.identity, now: options.now?.() ?? Date.now() });
      await persistScaleTrial(store, receipt, candidate, options.now?.() ?? Date.now());
    },
    async apply(target, candidate) {
      const receipt = await find(target);
      if (candidate.ownerId !== token.ownerId || candidate.fencingToken !== token.fencingToken) throw new Error('Scale apply actor is fenced.');
      await effects.apply(target, receipt);
    },
    async reconcile(target) {
      let receipt = await find(target);
      while (true) {
        await effects.assertCustody(token);
        const observed = await effects.observe(target, receipt);
        if (!observed.pending) {
          if (observed.value === target.after) {
            const phase = exact(target, receipt.restore) ? 'restored' : 'applied';
            if (receipt.phase !== phase) receipt = await transition(receipt, phase);
            return 'after';
          }
          if (observed.value === target.before) return 'before';
          return 'unknown';
        }
        const cleanupDeadline = authority?.mode === 'controller' ? (await import('./aks-cleanup-budget.js')).cleanupBudget(authority)?.deadlineUnixMs : undefined;
        const deadline = cleanupDeadline ?? (exact(target, receipt.restore) ? receipt.restore_deadline_at_ms : receipt.deadline_at_ms);
        if ((options.now?.() ?? Date.now()) >= deadline) return 'pending';
        await delay(Math.min(1000, Math.max(1, deadline - (options.now?.() ?? Date.now()))));
      }
    },
    async beginMeasurement() { await options.beginMeasurement?.(); },
    async measure() {
      if (!options.measure) throw new Error('No qualified post-scale measurement source is configured.');
      return options.measure();
    },
    async finish(result) {
      const receipt = await findAny();
      const phase = result.outcome === 'keep' ? 'kept' : receipt.phase;
      await transition(receipt, phase, { outcome: result.outcome, committed_rows_per_second: result.committedRowsPerSecond,
        qap_pending_backlog: result.qapPendingBacklog, qap_p95_latency_ms: result.qapP95LatencyMs,
        ineffective_increments: result.ineffectiveIncrements, reason: result.reason });
    },
    async finishUnknown(reason) {
      const receipt = await findAny();
      await transition(receipt, receipt.phase, { outcome: 'unknown', committed_rows_per_second: null, qap_pending_backlog: null,
        qap_p95_latency_ms: null, ineffective_increments: 0, reason });
    },
  };
  async function findAny(): Promise<ScaleTrialReceipt> {
    const state = (await store.read())?.state;
    const receipt = state?.scaleTrials?.at(-1);
    if (!receipt) throw new Error('No scale trial is available for result persistence.');
    return receipt;
  }
}

export async function restoreCampaignScaleTrials(store: CampaignStore, adapter: ScaleAdapter, token: ScaleCustodyToken): Promise<'complete' | 'pending'> {
  const state = (await store.read())?.state;
  if (!state) throw new Error('Scale cleanup requires a campaign receipt.');
  for (let receipt of [...(state.scaleTrials ?? [])].reverse()) {
    if (['restored', 'reverted'].includes(receipt.phase)) continue;
    if (receipt.phase === 'apply-intent' || receipt.phase === 'unknown') {
      const current = await adapter.reconcile(receipt.target);
      if (current === 'before') {
        await persistScaleTrial(store, { ...receipt, phase: 'reverted' }, token);
        continue;
      }
      if (current !== 'after') return 'pending';
      const fresh = (await store.read())?.state.scaleTrials?.find(item => item.trial_id === receipt.trial_id);
      if (!fresh) throw new Error('Scale receipt disappeared during cleanup reconciliation.');
      receipt = fresh;
    }
    if (!exact(receipt.restore, receipt.target) && receipt.phase !== 'restore-intent') {
      await adapter.assertCustody(token);
      await adapter.persistIntent(receipt.restore, token);
      receipt = (await store.read())?.state.scaleTrials?.find(item => item.trial_id === receipt.trial_id)!;
    }
    const before = await adapter.reconcile(receipt.restore);
    if (before === 'after') continue;
    if (before !== 'before') return 'pending';
    await adapter.assertCustody(token);
    await adapter.apply(receipt.restore, token);
    if (await adapter.reconcile(receipt.restore) !== 'after') return 'pending';
  }
  return 'complete';
}
