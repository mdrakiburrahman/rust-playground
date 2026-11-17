import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { canonical_json, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, processEffectRequest } from '../../infra/azure/effect-boundary.js';
import { assertCleaningActor, assertRunCleanupActor, observeExactKubernetes, type CustodyCommandRunner } from './aks-setup-cleanup.js';
import { buildAksPoolScalePayload } from './aks-arm-pool.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { assertRunInventoryReady } from './aks-campaign.js';
import type { CampaignState, CampaignStore, RunInventory, RunInventoryResource, ScaleEffectDispatch, ScaleResourceIdentity, ScaleTarget, ScaleTrialReceipt } from './aks-campaign.js';
import { newScaleTrialReceipt, persistScaleTrial, type ScaleAdapter, type ScaleCustodyToken, type ScaleMeasurement, type ScaleStep, type ScaleTrialResult } from './aks-scale.js';
import { validateRuntime, type ResourceRequirements, type RuntimeConfig } from './runtime.js';

interface Observation { identity: ScaleResourceIdentity; value: number | string; pending: boolean }
interface ScaleEffects {
  assertCustody(token: ScaleCustodyToken): Promise<{ state: NonNullable<Awaited<ReturnType<CampaignStore['read']>>>['state']; inventory: RunInventory }>;
  observe(target: ScaleTarget, receipt?: ScaleTrialReceipt): Promise<Observation>;
  apply(target: ScaleTarget, receipt: ScaleTrialReceipt): Promise<void>;
}

const phaseInFlight = new Set(['apply-intent', 'apply-dispatch', 'applied', 'restore-intent', 'restore-dispatch', 'unknown']);
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
async function assertWriterBoundary(inventory: RunInventory, authority: CleanupMutationAuthority, runCommand: CustodyCommandRunner): Promise<void> {
  if (!inventory.fic?.plan?.server) throw new Error('Scale admission has no exact Kubernetes API server binding.');
  const collector = oneResource(inventory, 'collector');
  const deployment = await observeExactKubernetes(inventory.fic.plan.server,
    ['deployment', collector.name, '--namespace', inventory.namespace.name], authority, runCommand);
  if (!deployment) throw new Error('Scale admission cannot observe the collector Deployment.');
  const metadata = object(deployment.metadata, 'Scale Deployment metadata'), spec = object(deployment.spec, 'Scale Deployment spec');
  if (metadata.uid !== collector.uid || metadata.namespace !== inventory.namespace.name || metadata.name !== collector.name ||
      object(spec.strategy ?? {}, 'Scale Deployment strategy').type !== 'Recreate')
    throw new Error('Scale admission requires the UID-bound collector Deployment with Recreate strategy.');
  const selector = object(spec.selector, 'Scale Deployment selector').matchLabels;
  if (!selector || typeof selector !== 'object' || Array.isArray(selector) || !Object.keys(selector).length ||
      Object.values(selector).some(value => typeof value !== 'string' || !value))
    throw new Error('Scale admission has no exact collector pod selector.');
  const selectorText = Object.entries(selector).map(([key, value]) => key + '=' + value).join(',');
  const replicaSetList = await observeExactKubernetes(inventory.fic.plan.server,
    ['replicasets', '--namespace', inventory.namespace.name, '--selector', selectorText], authority, runCommand);
  const replicaSetItems = object(replicaSetList, 'Scale collector ReplicaSet list').items;
  const podList = await observeExactKubernetes(inventory.fic.plan.server,
    ['pods', '--namespace', inventory.namespace.name, '--selector', selectorText], authority, runCommand);
  const podItems = object(podList, 'Scale collector pod list').items;
  if (!Array.isArray(replicaSetItems) || !Array.isArray(podItems)) throw new Error('Scale admission cannot verify physical collector pods.');
  const replicaSets = replicaSetItems.map(value => object(value, 'Scale collector ReplicaSet'));
  const pods = podItems.map(value => object(value, 'Scale collector pod'));
  const ownedReplicaSets = replicaSets.filter(set => Array.isArray(set.metadata?.ownerReferences) && set.metadata.ownerReferences.some((owner: Record<string, unknown>) =>
    owner.kind === 'Deployment' && owner.uid === collector.uid && owner.controller === true));
  const activeReplicaSets = ownedReplicaSets.filter(set => (set.spec?.replicas ?? 0) > 0);
  const activePods = pods.filter(pod => !['Succeeded', 'Failed'].includes(String(pod.status?.phase)));
  const status = object(deployment.status ?? {}, 'Scale Deployment status');
  const replicas = spec.replicas;
  if (authority.mode === 'controller') {
    if (replicas !== 0 || activePods.length !== 0 || (status.availableReplicas ?? 0) !== 0)
      throw new Error('Scale cleanup requires a quiescent collector with zero desired, available and physical writer pods.');
  } else {
    if (pods.some(pod => pod.metadata?.deletionTimestamp !== undefined && pod.metadata.deletionTimestamp !== null))
      throw new Error('Scale admission rejects a deleting ArrowCol pod.');
    const activeSet = activeReplicaSets.length === 1 ? activeReplicaSets[0] : undefined;
    const activeOwner = activeSet?.metadata?.uid;
    const ownedPods = pods.filter(pod => Array.isArray(pod.metadata?.ownerReferences) && pod.metadata.ownerReferences.some((owner: Record<string, unknown>) =>
      owner.kind === 'ReplicaSet' && owner.uid === activeOwner && owner.controller === true));
    if (replicas !== 1 || metadata.generation !== status.observedGeneration || status.availableReplicas !== 1 ||
        status.readyReplicas !== 1 || activeReplicaSets.length !== 1 || pods.length !== 1 || ownedPods.length !== 1 || activePods.length !== 1 ||
        pods[0]?.status?.phase !== 'Running' || !Array.isArray(pods[0]?.status?.containerStatuses) ||
        pods[0].status.containerStatuses.length !== 1 || pods[0].status.containerStatuses[0]?.ready !== true)
      throw new Error('Scale admission requires exactly one ready physical ArrowCol writer and no overlapping rollout pods.');
  }
}
function scaleReceipt(state: CampaignState, target: ScaleTarget): ScaleTrialReceipt {
  const matches = (state.scaleTrials ?? []).filter(item => exact(item.target, target) || exact(item.restore, target));
  const receipt = matches.at(-1);
  if (!receipt) throw new Error('Scale target has no durable intent receipt.');
  return receipt;
}
export function createDefaultScaleAdapter(store: CampaignStore, options: {
  azure: Azure; token: ScaleCustodyToken; measure?: () => Promise<ScaleMeasurement>; beginMeasurement?: (deadlineAtMs: number) => void | Promise<void>; runtimeConfig?: unknown;
  runCommand?: CustodyCommandRunner; authority?: CleanupMutationAuthority; now?: () => number;
}): ScaleAdapter {
  const runCommand = options.runCommand ?? runtime.run_tree;
  const runtimeConfig = options.runtimeConfig === undefined ? undefined : validateRuntime(options.runtimeConfig);
  let authority = options.authority;
  const token = options.token;
  const now = () => options.now?.() ?? Date.now();
  const deadlineFor = async (receipt: ScaleTrialReceipt, target: ScaleTarget, minimumRemainingMs = 0): Promise<number> => {
    const restoring = exact(target, receipt.restore);
    const cleanupDeadline = authority?.mode === 'controller' ? (await import('./aks-cleanup-budget.js')).cleanupBudget(authority)?.deadlineUnixMs : undefined;
    const deadline = Math.min(restoring ? receipt.restore_deadline_at_ms : receipt.deadline_at_ms, cleanupDeadline ?? Number.POSITIVE_INFINITY);
    const remaining = deadline - now();
    if (remaining <= 0 || remaining < minimumRemainingMs) throw new Error('Scale effect admission is outside the original persisted deadline.');
    return remaining;
  };
  const transition = async (receipt: ScaleTrialReceipt, phase: ScaleTrialReceipt['phase'], result?: ScaleTrialReceipt['result'],
    dispatches?: ScaleTrialReceipt['dispatches']) => {
    const next = { ...receipt, phase, ...(result ? { result } : {}), ...(dispatches ? { dispatches } : {}) };
    await persistScaleTrial(store, next, token, now());
    return next;
  };
  const claimDispatch = async (receipt: ScaleTrialReceipt, target: ScaleTarget, dispatch: ScaleEffectDispatch): Promise<ScaleTrialReceipt> => {
    const current = (await store.read())?.state;
    if (!current) throw new Error('Scale campaign record is absent.');
    const latest = scaleReceipt(current, target);
    if (latest.trial_id !== receipt.trial_id) throw new Error('Scale dispatch trial changed before claim.');
    const restoring = exact(target, latest.restore), kind = restoring ? 'restore' : 'apply';
    if (latest.dispatches?.[kind] || latest.phase !== (restoring ? 'restore-intent' : 'apply-intent'))
      throw new Error('Scale dispatch is already claimed, uncertain, or ineligible for a new forward.');
    const next = { ...latest, phase: restoring ? 'restore-dispatch' as const : 'apply-dispatch' as const,
      dispatches: { ...latest.dispatches, [kind]: dispatch } };
    await persistScaleTrial(store, next, token, now());
    return next;
  };
  const recordDispatch = async (receipt: ScaleTrialReceipt, target: ScaleTarget,
    outcome: { state: 'confirmed' | 'unknown'; response_status?: number; returncode?: number; operation_url?: string },
    phase: ScaleTrialReceipt['phase']): Promise<ScaleTrialReceipt> => {
    const current = (await store.read())?.state;
    if (!current) throw new Error('Scale campaign record is absent.');
    const latest = scaleReceipt(current, target), restoring = exact(target, latest.restore), kind = restoring ? 'restore' : 'apply';
    const dispatch = latest.dispatches?.[kind];
    if (latest.trial_id !== receipt.trial_id || !dispatch || dispatch.state !== 'forwarding')
      throw new Error('Scale dispatch outcome no longer matches its durable claim.');
    const updated = { ...dispatch, ...outcome };
    const dispatches = { ...latest.dispatches, [kind]: updated };
    return transition(latest, phase, undefined, dispatches);
  };
  const requestHash = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
  const validOperationUrl = (value: string | undefined): string | undefined => {
    if (!value) return undefined;
    try { const url = new URL(value); return url.protocol === 'https:' && url.hostname === 'management.azure.com' ? value : undefined; }
    catch { return undefined; }
  };  const effects: ScaleEffects = {
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
      await assertWriterBoundary(inventory, authority, runCommand);
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
        if (!Number.isSafeInteger(replicas) || replicas < 0 || authority?.mode !== 'controller' && replicas < 1)
          throw new Error('Scale Deployment replica count is UNKNOWN.');
        let value: number;
        if (target.dimension === 'loadgen-max') {
          if (authority?.mode === 'controller' && replicas === 0) {
            const selector = object(spec.selector, 'Scale client Deployment selector').matchLabels;
            if (!selector || typeof selector !== 'object' || Array.isArray(selector) || !Object.keys(selector).length)
              throw new Error('Stopped load generator has no exact pod selector.');
            const selectorText = Object.entries(selector).map(([key, value]) => key + '=' + value).join(',');
            const list = await observeExactKubernetes(inventory.fic.plan.server,
              ['pods', '--namespace', inventory.namespace.name, '--selector', selectorText], authority!, runCommand);
            const items = object(list, 'Scale client pod list').items;
            if (!Array.isArray(items) || items.some(value => { const pod = object(value, 'Scale client pod');
              return !['Succeeded', 'Failed'].includes(String(pod.status?.phase)); }))
              throw new Error('Stopped load generator still has physical pods; scale cleanup remains pending.');
          }
          value = replicas;
        } else {
          if (role === 'collector' && replicas !== (authority?.mode === 'controller' ? 0 : 1)) throw new Error('The one-ArrowCol-writer replica invariant is not satisfied.');
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
        const pending = status.observedGeneration !== metadata.generation || (status.availableReplicas ?? 0) !== replicas;
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
      const { inventory } = await effects.assertCustody(token);
      if (authority?.mode === 'controller' && !exact(target, receipt.restore)) throw new Error('Cleanup may only apply the recorded restore target.');
      if (!exact(receipt.target, target) && !exact(receipt.restore, target)) throw new Error('Scale apply target differs from the durable receipt.');
      const restoring = exact(target, receipt.restore);
      if (receipt.phase !== (restoring ? 'restore-intent' : 'apply-intent'))
        throw new Error('Scale dispatch is not backed by a fresh durable intent.');
      await deadlineFor(receipt, target);
      const current = await effects.observe(target, receipt);
      if (current.pending || current.value !== target.before) throw new Error('Scale target no longer matches the durable exact before-state.');
      if (['collector-cpu-limit', 'collector-memory-limit', 'loadgen-max'].includes(target.dimension)) {
        if (authority?.mode === 'controller' && target.dimension === 'loadgen-max')
          throw new Error('Cleanup must preserve the stopped load generator and may not restore its replica count.');
        const runtimeConfigForTarget = runtimeConfig;
        if (!runtimeConfigForTarget && authority?.mode !== 'controller') throw new Error('Producer scale requires the typed D45 runtime resource contract.');
        const resource = target.dimension === 'loadgen-max' ? oneResource(inventory, 'client') : oneResource(inventory, 'collector');
        const server = inventory.fic?.plan?.server;
        if (!server) throw new Error('Scale Deployment has no exact API server binding.');
        const deployment = await observeExactKubernetes(server,
          ['deployment', resource.name, '--namespace', inventory.namespace.name], authority!, runCommand);
        if (!deployment) throw new Error('Scale Deployment disappeared before patch admission.');
        const metadata = object(deployment.metadata, 'Scale Deployment metadata'), spec = object(deployment.spec, 'Scale Deployment spec');
        if (metadata.uid !== resource.uid || metadata.namespace !== inventory.namespace.name || metadata.name !== resource.name ||
            typeof metadata.resourceVersion !== 'string' || metadata.resourceVersion.length === 0)
          throw new Error('Scale Deployment identity or resourceVersion changed before patch admission.');
        const replicas = spec.replicas;
        if (!Number.isSafeInteger(replicas) || replicas !== (target.dimension === 'loadgen-max' ? target.before : authority?.mode === 'controller' ? 0 : 1))
          throw new Error('Scale Deployment replica before-state changed before patch admission.');
        let patch: Array<Record<string, unknown>>;
        if (target.dimension === 'loadgen-max') {
          patch = [{ op: 'test', path: '/metadata/uid', value: resource.uid },
            { op: 'test', path: '/metadata/resourceVersion', value: metadata.resourceVersion },
            { op: 'test', path: '/spec/replicas', value: target.before },
            { op: 'replace', path: '/spec/replicas', value: target.after }];
        } else {
          if (object(spec.strategy ?? {}, 'Scale Deployment strategy').type !== 'Recreate')
            throw new Error('Collector scaling requires the live Recreate rollout strategy.');
          const containers = object(spec.template?.spec, 'Scale pod spec').containers;
          if (!Array.isArray(containers) || containers.length !== 1 || containers[0].name !== 'dataflow')
            throw new Error('Collector container changed before patch admission.');
          const field = target.dimension === 'collector-cpu-limit' ? 'cpu' : 'memory';
          const requirements = object(containers[0].resources, 'Scale resources');
          const limits = object(requirements.limits, 'Scale limits'), requests = object(requirements.requests, 'Scale requests');
          const observedValue = field === 'cpu' ? quantityCpu(limits[field]) : quantityMemory(limits[field]);
          if (observedValue !== target.before) throw new Error('Collector limit changed in the final Deployment observation.');
          const nextValue = field === 'cpu' ? String(target.after) : String(target.after) + 'Gi';
          if (runtimeConfigForTarget) validateRuntime({ ...runtimeConfigForTarget, resources: { ...runtimeConfigForTarget.resources,
            collector: { requests: { cpu: String(requests.cpu), memory: String(requests.memory) },
              limits: { cpu: field === 'cpu' ? nextValue : String(limits.cpu), memory: field === 'memory' ? nextValue : String(limits.memory) } } } });
          const path = '/spec/template/spec/containers/0/resources/limits/' + field;
          patch = [{ op: 'test', path: '/metadata/uid', value: resource.uid },
            { op: 'test', path: '/metadata/resourceVersion', value: metadata.resourceVersion },
            { op: 'test', path, value: limits[field] }, { op: 'replace', path, value: nextValue }];
        }
        await effects.assertCustody(token);
        await deadlineFor(receipt, target, 1_000);
        const dispatch: ScaleEffectDispatch = { state: 'forwarding', transport: 'kubernetes', method: 'kubectl-json-patch',
          target, before: target.before, started_at_ms: now(), resource_version: metadata.resourceVersion,
          request_sha256: requestHash({ server, namespace: inventory.namespace.name, resource: receipt.resource, patch }) };
        const claimed = await claimDispatch(receipt, target, dispatch);
        let outcomeRecorded = false, response: Awaited<ReturnType<CustodyCommandRunner>>;
        try {
          await authority!.assertCurrent();
          const timeoutSeconds = Math.min(35, Math.floor(await deadlineFor(receipt, target, 1_000) / 1_000));
          const args = ['kubectl', '--server', server, '--insecure-skip-tls-verify=false', '--request-timeout', Math.min(30, timeoutSeconds) + 's',
            'patch', 'deployment', resource.name, '--namespace', inventory.namespace.name, '--type=json', '--patch', JSON.stringify(patch)];
          response = await dispatchExternalEffect(processEffectRequest(args), () => runCommand(args, timeoutSeconds));
          if (response.returncode !== 0) {
            await recordDispatch(claimed, target, { state: 'unknown', returncode: response.returncode }, 'unknown');
            outcomeRecorded = true;
            throw new Error('Kubernetes scale patch failed with exit ' + response.returncode + '.');
          }
        } catch (error) {
          if (!outcomeRecorded) {
            try { await recordDispatch(claimed, target, { state: 'unknown' }, 'unknown'); }
            catch (journalError) { throw new AggregateError([error, journalError], 'Scale dispatch outcome and durable journal remain unresolved.'); }
          }
          throw error;
        }
        await recordDispatch(claimed, target, { state: 'confirmed', returncode: 0 }, restoring ? 'restore-dispatch' : 'apply-dispatch');
        return;
      }
      const id = receipt.resource.resource_id, api = receipt.resource.api_version!;
      const url = 'https://management.azure.com' + id + '?api-version=' + api;
      const [bytes, headers] = await options.azure.request('GET', url, 'https://management.azure.com/');
      if (!bytes) throw new Error('Scale ARM final read returned no resource body.');
      const live = object(JSON.parse(bytes.toString()), 'Scale ARM final resource');
      if (live.id !== id || live.name !== receipt.resource.name) throw new Error('Scale ARM identity changed before dispatch.');
      const etag = headers.etag;
      if (typeof etag !== 'string' || !etag) throw new Error('Scale ARM resource ETag is unavailable; conditional mutation is refused.');
      let body: Record<string, unknown> | undefined, method: 'PUT' | 'PATCH' = 'PATCH';
      if (target.dimension === 'collector-node-max') {
        if (typeof target.after !== 'number' || typeof target.before !== 'number') throw new Error('Collector node maximum target is malformed.');
        const properties = object(live.properties, 'Scale agent-pool properties');
        if (properties.enableAutoScaling !== true || properties.minCount !== 0 || properties.maxCount !== target.before || !Number.isSafeInteger(properties.count))
          throw new Error('Collector agent-pool exact before-state changed in the final ETag-bound observation.');
        body = buildAksPoolScalePayload(live, { count: properties.count as number, autoscale: true, minimum: 0, maximum: target.after });
        method = 'PUT';
      } else if (target.dimension === 'kusto-capacity' || target.dimension === 'kusto-sku') {
        const sku = object(live.sku, 'Scale Kusto SKU');
        if (sku.tier !== 'Standard' || !Number.isSafeInteger(sku.capacity) ||
            target.dimension === 'kusto-capacity' && sku.capacity !== target.before ||
            target.dimension === 'kusto-sku' && sku.name !== target.before)
          throw new Error('Kusto before-state changed in the final ETag-bound observation.');
        body = { sku: { name: target.dimension === 'kusto-sku' ? target.after : sku.name, tier: sku.tier,
          capacity: target.dimension === 'kusto-capacity' ? target.after : sku.capacity } };
      }
      if (!body) throw new Error('Scale target has no approved ARM patch shape.');
      await effects.assertCustody(token);
      await deadlineFor(receipt, target, 30_000);
      const dispatch: ScaleEffectDispatch = { state: 'forwarding', transport: 'azure-rest', method, target, before: target.before,
        started_at_ms: now(), etag, request_sha256: requestHash({ method, url, resource: receipt.resource, target, etag, body }) };
      const claimed = await claimDispatch(receipt, target, dispatch);
      let responseHeaders: Record<string, string>;
      try {
        await authority!.assertCurrent();
        await deadlineFor(receipt, target, 30_000);
        [, responseHeaders] = await options.azure.request(method, url, 'https://management.azure.com/', JSON.stringify(body),
          { 'Content-Type': 'application/json', 'If-Match': etag });
      } catch (error) {
        try { await recordDispatch(claimed, target, { state: 'unknown' }, 'unknown'); }
        catch (journalError) { throw new AggregateError([error, journalError], 'Scale dispatch outcome and durable journal remain unresolved.'); }
        throw error;
      }
      const status = Number(responseHeaders[':status']);
      const operationUrl = validOperationUrl(responseHeaders['azure-asyncoperation'] ?? responseHeaders['operation-location']);
      await recordDispatch(claimed, target, { state: 'confirmed', response_status: status,
        ...(operationUrl ? { operation_url: operationUrl } : {}) }, restoring ? 'restore-dispatch' : 'apply-dispatch');
    },
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
        if (exact(existing.restore, target) && ['applied', 'kept', 'unknown'].includes(existing.phase) && !existing.dispatches?.restore)
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
        const restoring = exact(target, receipt.restore);
        const dispatch = receipt.dispatches?.[restoring ? 'restore' : 'apply'];
        if (!observed.pending) {
          if (restoring && authority?.mode === 'controller' && target.dimension === 'loadgen-max' && observed.value === 0) {
            if (receipt.phase !== 'restore-stopped') receipt = await transition(receipt, 'restore-stopped');
            return 'after';
          }
          if (observed.value === target.after) {
            const phase = restoring ? 'restored' : 'applied';
            if (dispatch?.state === 'forwarding') {
              const dispatches = { ...receipt.dispatches, [restoring ? 'restore' : 'apply']: { ...dispatch, state: 'unknown' as const } };
              receipt = await transition(receipt, phase, undefined, dispatches);
            } else if (receipt.phase !== phase) receipt = await transition(receipt, phase);
            return 'after';
          }
          if (observed.value === target.before) return dispatch ? 'unknown' : 'before';
          return 'unknown';
        }
        const cleanupDeadline = authority?.mode === 'controller' ? (await import('./aks-cleanup-budget.js')).cleanupBudget(authority)?.deadlineUnixMs : undefined;
        const deadline = Math.min(restoring ? receipt.restore_deadline_at_ms : receipt.deadline_at_ms, cleanupDeadline ?? Number.POSITIVE_INFINITY);
        if ((options.now?.() ?? Date.now()) >= deadline) return 'pending';
        await delay(Math.min(1000, Math.max(1, deadline - (options.now?.() ?? Date.now()))));
      }
    },
    async deadlineFor(target) {
      const receipt = await find(target);
      return { trial: receipt.deadline_at_ms, restore: receipt.restore_deadline_at_ms };
    },
    async beginMeasurement(deadlineAtMs) { if (deadlineAtMs !== undefined) await options.beginMeasurement?.(deadlineAtMs); },
    async measure(deadlineAtMs) {
      if (!options.measure) throw new Error('No qualified post-scale measurement source is configured.');
      if (deadlineAtMs !== undefined && now() >= deadlineAtMs) throw new Error('Scale measurement deadline expired.');
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
    if (['restored', 'restore-stopped', 'reverted'].includes(receipt.phase)) continue;
    if (receipt.dispatches?.restore && ['restore-dispatch', 'unknown'].includes(receipt.phase)) {
      if (await adapter.reconcile(receipt.restore) !== 'after') return 'pending';
      continue;
    }
    if (['apply-intent', 'apply-dispatch', 'unknown'].includes(receipt.phase)) {
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
