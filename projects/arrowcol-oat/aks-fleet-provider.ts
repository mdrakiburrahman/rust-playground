import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { canonical_json, owned, runtime, type Azure, type HttpReceipt } from '../../infra/azure/bootstrap.js';
import { buildFleetPoolScalePayload, fleetPoolConfiguration } from './aks-arm-pool.js';
import { observeExactKubernetes, type CustodyCommandRunner } from './aks-setup-cleanup.js';
import { FleetUnsettledDispatchError, type FleetBinding, type FleetIntentAuthority, type FleetPoolObservation } from './aks-fleet-custody.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import type { AksFleetAdapter, FleetPoolSnapshot, FleetScaleResult } from './aks-fleet.js';

const api = '2024-10-01', arm = 'https://management.azure.com/';
const same = (a: unknown, b: unknown) => canonical_json(a) === canonical_json(b);
const terminalFailure = (state: unknown) => state === 'Failed' || state === 'Canceled';
class FleetObservationUnavailableError extends Error {
  constructor() { super('Exact fleet ARM observation is unavailable.'); }
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Fleet observation is UNKNOWN.');
  return value as Record<string, unknown>;
}
function integer(value: unknown, minimum = 0): value is number {
  return Number.isSafeInteger(value) && (value as number) >= minimum;
}
function clusterId(azure: Azure): string {
  if (azure.c.mode !== 'stamped') throw new Error('Fleet requires reviewed stamped infrastructure.');
  const id = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  if (!/^\/subscriptions\/[^/]+\/resourceGroups\/[^/]+\/providers\/Microsoft\.ContainerService\/managedClusters\/[^/]+$/.test(id))
    throw new Error('Fleet cluster identity is invalid.');
  return id;
}
async function observeCluster(azure: Azure): Promise<{ clusterId: string; server: string }> {
  const id = clusterId(azure), cluster = object(await azure.arm('GET', id, undefined, false, api));
  owned(cluster, azure.c);
  const properties = object(cluster.properties);
  if (typeof cluster.id !== 'string' || cluster.id.toLowerCase() !== id.toLowerCase() ||
      typeof cluster.type !== 'string' || cluster.type.toLowerCase() !== 'microsoft.containerservice/managedclusters' ||
      typeof properties.fqdn !== 'string' || !/^[a-zA-Z0-9.-]+$/.test(properties.fqdn) || !properties.fqdn.includes('.'))
    throw new Error('Fleet cluster identity or exact HTTPS server is UNKNOWN.');
  return { clusterId: id, server: 'https://' + properties.fqdn };
}
function selectedStampPools(azure: Azure, collectorPool: string) {
  const configured = azure.c.stampConfig.aks.userPools;
  const collectors = configured.filter(p => p.name === collectorPool && p.role === 'arrowcol');
  if (collectors.length !== 1) throw new Error('Fleet requires one explicit reviewed collector User pool.');
  const pools = configured.filter(p => p.name === collectorPool || p.role === 'client');
  if (new Set(pools.map(p => p.name)).size !== pools.length || !pools.some(p => p.role === 'client') ||
      pools.some(p => !/^[a-z][a-z0-9]{0,11}$/.test(p.name) || !integer(p.maxCount, 1) || p.minCount !== 0))
    throw new Error('Fleet reviewed membership or zero baseline bounds are invalid.');
  return pools;
}
function validateBinding(azure: Azure, binding: FleetBinding) {
  if (binding.clusterId !== clusterId(azure) || typeof binding.server !== 'string') throw new Error('Fleet cluster binding differs.');
  const collectors = binding.pools.filter(p => p.role === 'arrowcol');
  if (collectors.length !== 1) throw new Error('Fleet collector binding differs.');
  const configured = selectedStampPools(azure, collectors[0]!.name);
  if (configured.length !== binding.pools.length || configured.some(p => {
    const b = binding.pools.find(b => b.name === p.name);
    return !b || b.sku !== p.vmSize || b.role !== p.role || b.maxCount !== p.maxCount ||
      !integer(b.vcpu, 1) || !integer(b.count, 1) || b.count > b.maxCount;
  })) throw new Error('Fleet binding differs from exact reviewed membership.');
}
function validatePool(resource: unknown, id: string, pool: { name: string; sku: string; role: string }) {
  const value = object(resource), p = object(value.properties), labels = object(p.nodeLabels);
  fleetPoolConfiguration(value);
  if (typeof value.id !== 'string' || value.id.toLowerCase() !== (id + '/agentPools/' + pool.name).toLowerCase() ||
      value.name !== pool.name || typeof value.type !== 'string' ||
      value.type.toLowerCase() !== 'microsoft.containerservice/managedclusters/agentpools' ||
      p.mode !== 'User' || p.osType !== 'Linux' || p.vmSize !== pool.sku || p.enableAutoScaling !== true ||
      labels.type !== 'benchmark' || labels['arrow-collector/role'] !== pool.role ||
      labels['arrow-collector/sku'] === undefined && labels['node.kubernetes.io/instance-type'] === undefined &&
        !(pool.name === 'benche64' && pool.role === 'arrowcol' || pool.name === 'benchd64' && pool.role === 'client') ||
      labels['arrow-collector/sku'] !== undefined && labels['arrow-collector/sku'] !== pool.sku ||
      labels['node.kubernetes.io/instance-type'] !== undefined && labels['node.kubernetes.io/instance-type'] !== pool.sku ||
      !same(p.nodeTaints, ['arrow-collector/role=' + pool.role + ':NoSchedule']) ||
      !integer(p.count) || !integer(p.minCount) || !integer(p.maxCount, 1) ||
      typeof p.provisioningState !== 'string')
    throw new Error('Fleet User pool identity, role/SKU labels, taint or configuration drifted.');
  return { ...value, id: value.id, name: pool.name, properties: {
    ...p, count: p.count, minCount: p.minCount, maxCount: p.maxCount, provisioningState: p.provisioningState,
  } };
}
async function rawPool(azure: Azure, id: string, pool: { name: string; sku: string; role: string }) {
  let response: Awaited<ReturnType<Azure['request']>>;
  try { response = await azure.request('GET', arm.slice(0, -1) + id + '/agentPools/' + pool.name + '?api-version=' + api, arm); }
  catch { throw new FleetObservationUnavailableError(); }
  const [bytes, headers] = response;
  if (!bytes) throw new Error('Exact fleet pool is absent.');
  let parsed: unknown;
  try { parsed = JSON.parse(bytes.toString()); } catch { throw new FleetObservationUnavailableError(); }
  const value = validatePool(parsed, id, pool);
  const root = object(value), properties = object(value.properties);
  const etag = headers.etag ?? root.etag ?? root.eTag ?? properties.eTag;
  if (typeof etag !== 'string' || !/^(?:W\/)?"[^"\r\n]+"$/.test(etag) ||
      [root.etag, root.eTag, properties.eTag].some(tag => tag !== undefined && tag !== etag))
    throw new Error('Fleet pool ETag is absent, invalid or inconsistent.');
  const observation: FleetPoolObservation = { name: pool.name, resourceId: value.id, sku: pool.sku,
    count: value.properties.count, provisioningState: value.properties.provisioningState, etag,
    configurationHash: createHash('sha256').update(canonical_json(fleetPoolConfiguration(value))).digest('hex') };
  return { value, observation };
}

export async function observeFleetBinding(azure: Azure, collectorPool: string): Promise<FleetBinding> {
  const cluster = await observeCluster(azure), configured = selectedStampPools(azure, collectorPool);
  const pools: FleetBinding['pools'] = [];
  for (const pool of configured) {
    const inventory: unknown = await azure.cli('vm', 'list-skus', '--location', azure.c.location, '--size', pool.vmSize,
      '--all', '--subscription', azure.c.subscription);
    if (!Array.isArray(inventory)) throw new Error('Fleet Compute SKU inventory is UNKNOWN.');
    const matches = (inventory as unknown[]).map(object).filter(sku => sku.name === pool.vmSize && sku.resourceType === 'virtualMachines');
    const rawCapabilities = matches.length === 1 ? matches[0]!.capabilities : undefined;
    const capabilities = Array.isArray(rawCapabilities) ?
      (rawCapabilities as unknown[]).map(object).filter(c => c.name === 'vCPUs') : [];
    const rawVcpu = capabilities.length === 1 ? capabilities[0]!.value : undefined;
    const vcpu = typeof rawVcpu === 'string' && /^[1-9]\d*$/.test(rawVcpu) ? Number(rawVcpu) : NaN;
    if (!integer(vcpu, 1)) throw new Error('Fleet Compute SKU vCPU count is UNKNOWN.');
    await rawPool(azure, cluster.clusterId, { name: pool.name, sku: pool.vmSize, role: pool.role });
    pools.push({ name: pool.name, sku: pool.vmSize, role: pool.role, vcpu, count: 1, maxCount: pool.maxCount });
  }
  return { ...cluster, pools };
}

export function createFleetAdapter(azure: Azure, suppliedBinding: FleetBinding, authority: FleetIntentAuthority,
  runCommand: CustodyCommandRunner = runtime.run_tree): AksFleetAdapter & { establishBaseline(): Promise<void> } {
  const binding = structuredClone(suppliedBinding);
  validateBinding(azure, binding);
  const reviewed = structuredClone(azure.c.stampConfig.aks);
  const reviewedUsers = new Map(reviewed.userPools.map(p => [p.name, p]));
  if (!authority || !same(authority.binding, binding) ||
      ['assertCurrent', 'establishBaseline', 'recordObservation', 'beforeDispatch', 'afterDispatch'].some(k =>
        typeof (authority as unknown as Record<string, unknown>)[k] !== 'function'))
    throw new Error('Fleet requires exact durable intent authority.');
  const unsettled = new Map<string, number>();
  const check = async (supplied: CleanupMutationAuthority) => {
    if (supplied !== authority || !same(authority.binding, binding)) throw new Error('Fleet authority differs.');
    await authority.assertCurrent();
  };
  const pool = (name: string) => {
    const p = binding.pools.find(p => p.name === name);
    if (!p) throw new Error('Fleet pool is not an approved User pool.');
    return p;
  };
  const validateBounds = (p: Record<string, unknown>, approved: FleetBinding['pools'][number]) => {
    const idle = p.minCount === 0 && p.maxCount === approved.maxCount;
    const pinned = p.minCount === approved.count && p.maxCount === approved.count;
    if (p.provisioningState === 'Succeeded' &&
        !(p.count === 0 && idle) && !(p.count === approved.count && pinned) ||
        terminalFailure(p.provisioningState) && (!idle && !pinned || p.count !== 0 && p.count !== approved.count))
      throw new Error('Fleet autoscaler bounds differ from approved idle or pinned scale state.');
  };
  const membership = async (requireIdle = false) => {
    await check(authority);
    if (!same(await observeCluster(azure), { clusterId: binding.clusterId, server: binding.server }))
      throw new Error('Exact fleet cluster/server binding drifted.');
    const members: unknown = await azure.collection(binding.clusterId + '/agentPools', api);
    if (!Array.isArray(members)) throw new Error('Fleet User pool membership is UNKNOWN.');
    const names = new Set<string>();
    for (const item of members as unknown[]) {
      const m = object(item), p = object(m.properties);
      if (typeof m.name !== 'string' || names.has(m.name) ||
          typeof m.id !== 'string' || m.id.toLowerCase() !== (binding.clusterId + '/agentPools/' + m.name).toLowerCase() ||
          typeof p.mode !== 'string' || !['User', 'System'].includes(p.mode) || !integer(p.count))
        throw new Error('Fleet live pool membership identity is UNKNOWN.');
      names.add(m.name);
      if (typeof m.type !== 'string' || m.type.toLowerCase() !== 'microsoft.containerservice/managedclusters/agentpools')
        throw new Error('Fleet live pool membership type is UNKNOWN.');
      if (m.name === reviewed.systemPool.name) {
        const system = reviewed.systemPool;
        if (p.mode !== 'System' || p.vmSize !== system.vmSize || p.osType !== 'Linux' ||
            p.provisioningState !== 'Succeeded' || p.enableAutoScaling !== true ||
            p.minCount !== system.minCount || p.maxCount !== system.maxCount ||
            p.count < system.minCount || p.count > system.maxCount)
          throw new Error('Reviewed System pool identity or autoscaler policy drifted.');
        continue;
      }
      const configured = reviewedUsers.get(m.name);
      if (!configured) throw new Error('An undeclared pool is present in fleet membership.');
      const selected = binding.pools.find(p => p.name === m.name);
      if (selected) {
        const observed = validatePool(m, binding.clusterId, selected).properties;
        validateBounds(observed, selected);
        if (requireIdle && (observed.count !== 0 || observed.provisioningState !== 'Succeeded'))
          throw new Error('Selected User pool is not settled at zero during final idle verification.');
      }
      else {
        const preserved = validatePool(m, binding.clusterId,
          { name: configured.name, sku: configured.vmSize, role: configured.role }).properties;
        if (preserved.count !== 0 || preserved.provisioningState !== 'Succeeded' ||
            preserved.minCount !== configured.minCount || configured.minCount !== 0 ||
            preserved.maxCount !== configured.maxCount)
          throw new Error('Non-selected User pool must retain its approved settled-zero idle policy.');
      }
    }
    if (!names.has(reviewed.systemPool.name) || reviewed.userPools.some(p => !names.has(p.name)))
      throw new Error('An approved fleet System or User pool is missing.');
  };
  const read = async (name: string) => {
    await check(authority);
    const approved = pool(name), result = await rawPool(azure, binding.clusterId, approved);
    validateBounds(result.value.properties, approved);
    return result;
  };
  const reconcile = async (observation: FleetPoolObservation) => {
    await authority.recordObservation(observation);
    if (terminalFailure(observation.provisioningState)) {
      unsettled.delete(observation.name);
      return;
    }
    if (observation.provisioningState !== 'Succeeded' ||
        unsettled.has(observation.name) && unsettled.get(observation.name) !== observation.count)
      throw new FleetUnsettledDispatchError();
    unsettled.delete(observation.name);
  };
  const snapshot = (observation: FleetPoolObservation): FleetPoolSnapshot => ({
    name: observation.name, sku: observation.sku, vcpu: pool(observation.name).vcpu,
    count: observation.count, provisioningState: observation.provisioningState,
  });
  const kubernetes = async (args: string[], supplied: CleanupMutationAuthority) => {
    await check(supplied);
    if (!same(await observeCluster(azure), { clusterId: binding.clusterId, server: binding.server }))
      throw new Error('Exact fleet cluster/server binding drifted.');
    const value = await observeExactKubernetes(binding.server, args, authority, runCommand);
    if (!value || !Array.isArray(value.items)) throw new Error('Fleet Kubernetes list observation is UNKNOWN.');
    await check(supplied);
    return { ...value, items: value.items as unknown[] };
  };
  const validateNodeMembership = (nodes: unknown[], requireIdle = false) => {
    for (const node of nodes) {
      const labels = object(object(object(node).metadata).labels);
      const a = labels.agentpool, b = labels['kubernetes.azure.com/agentpool'];
      if (a !== undefined && b !== undefined && a !== b) throw new Error('Fleet node pool labels disagree.');
      const name = b ?? a;
      if (typeof name !== 'string' || name !== reviewed.systemPool.name && !reviewedUsers.has(name))
        throw new Error('Fleet node pool membership is UNKNOWN.');
      if (reviewedUsers.has(name) && (requireIdle || !binding.pools.some(p => p.name === name)))
        throw new Error('A User pool required to be idle still has physical nodes.');
    }
  };
  const adapter: AksFleetAdapter & { establishBaseline(): Promise<void> } = {
    async establishBaseline() {
      await membership();
      const observations: FleetPoolObservation[] = [];
      for (const p of binding.pools) {
        const { value, observation } = await read(p.name), config = value.properties;
        if (observation.count !== 0 || observation.provisioningState !== 'Succeeded' ||
            config.minCount !== 0 || config.maxCount !== p.maxCount)
          throw new Error('Fleet baseline requires settled zero pools and exact approved autoscaler bounds.');
        observations.push(observation);
      }
      const nodes = await kubernetes(['nodes'], authority);
      validateNodeMembership(nodes.items);
      if (nodes.items.some(node => {
        const labels = object(object(object(node).metadata).labels);
        const a = labels.agentpool, b = labels['kubernetes.azure.com/agentpool'];
        if (a !== undefined && b !== undefined && a !== b) throw new Error('Fleet node pool labels disagree.');
        if (typeof (b ?? a) !== 'string') throw new Error('Fleet node pool identity is UNKNOWN.');
        return binding.pools.some(p => p.name === (b ?? a));
      })) throw new Error('Fleet baseline requires zero selected physical User nodes.');
      await check(authority);
      await authority.establishBaseline(observations);
    },
    async listUserPools(supplied, requireIdle = false) {
      await check(supplied); await membership(requireIdle);
      for (const p of binding.pools) {
        const { observation } = await read(p.name);
        if (requireIdle && (observation.count !== 0 || observation.provisioningState !== 'Succeeded'))
          throw new Error('Selected User pool changed during final idle verification.');
      }
      validateNodeMembership((await kubernetes(['nodes'], supplied)).items, requireIdle);
      return binding.pools.map(p => p.name);
    },
    async readPool(name, supplied) {
      await check(supplied);
      const { observation } = await read(name);
      try { await reconcile(observation); }
      catch (error) { if (!(error instanceof FleetUnsettledDispatchError)) throw error; }
      return snapshot(observation);
    },
    async scalePool(name, count, supplied) {
      await check(supplied);
      const approved = pool(name);
      if (![0, approved.count].includes(count)) throw new Error('Fleet count is not approved.');
      await membership();
      const { value, observation } = await read(name);
      await reconcile(observation);
      if (terminalFailure(observation.provisioningState) && count !== 0)
        throw new Error('Terminal fleet pool failure permits only an owned repair to zero.');
      const body = buildFleetPoolScalePayload(value, {
        resourceId: binding.clusterId + '/agentPools/' + name, sku: approved.sku, maxCount: approved.maxCount,
      }, count);
      const dispatchId = await authority.beforeDispatch(name, count, observation.etag);
      unsettled.set(name, count);
      let receipt: HttpReceipt | undefined, result: FleetScaleResult = { state: 'uncertain' };
      try {
        await check(supplied);
        await azure.request('PUT', arm.slice(0, -1) + observation.resourceId + '?api-version=' + api, arm,
          JSON.stringify(body), { 'Content-Type': 'application/json', 'If-Match': observation.etag }, false, false,
          value => { receipt = value; });
        if (receipt && [200, 201, 202].includes(receipt.status)) result = { state: 'accepted' };
      } catch {
        if (receipt) {
          let code: unknown;
          try { code = object(object(JSON.parse(Buffer.from(receipt.bodyBase64, 'base64').toString())).error).code; }
          catch { /* Unknown response remains uncertain. */ }
          if (receipt.status === 409 && code === 'OperationNotAllowed')
            result = { state: 'conflict', status: 409, code: 'OperationNotAllowed' };
          else if ([400, 403, 409, 422].includes(receipt.status) && typeof code === 'string') {
            const reasons: Record<string, 'quota_exceeded' | 'allocation_failed' | 'sku_capacity_unavailable'> = {
              QuotaExceeded: 'quota_exceeded', ResourceQuotaExceeded: 'quota_exceeded',
              AllocationFailed: 'allocation_failed', ZonalAllocationFailed: 'allocation_failed',
              SkuNotAvailable: 'sku_capacity_unavailable', SkuCapacityUnavailable: 'sku_capacity_unavailable',
            };
            result = { state: 'rejected', reason: reasons[code] ?? 'request_rejected' };
          }
        }
      }
      await authority.afterDispatch(name, dispatchId, result.state);
      if (result.state === 'rejected' || result.state === 'conflict') unsettled.delete(name);
      try { await reconcile((await read(name)).observation); }
      catch (error) {
        if (error instanceof FleetObservationUnavailableError) return { state: 'uncertain' };
        if (!(error instanceof FleetUnsettledDispatchError)) throw error;
      }
      return result;
    },
    async waitForPool(name, count, deadlineAtMs, supplied) {
      pool(name);
      if (!Number.isFinite(deadlineAtMs)) throw new Error('Fleet poll requires the original finite deadline.');
      while (Date.now() < deadlineAtMs) {
        await check(supplied);
        const { observation } = await read(name);
        try {
          await reconcile(observation);
          if ((observation.count === count || terminalFailure(observation.provisioningState)) && Date.now() < deadlineAtMs)
            return snapshot(observation);
        } catch (error) { if (!(error instanceof FleetUnsettledDispatchError)) throw error; }
        const remaining = deadlineAtMs - Date.now();
        if (remaining > 0) await delay(Math.min(1000, remaining));
      }
      throw new Error('Fleet pool did not settle before the original deadline.');
    },
    listNodes: supplied => kubernetes(['nodes'], supplied),
    listPods: supplied => kubernetes(['pods', '--all-namespaces'], supplied),
  };
  return adapter;
}

export async function observeFleetImageArchitectures(azure: Azure, images: string[]): Promise<Array<{ image: string; architectures: string[] }>> {
  const observations: Array<{ image: string; architectures: string[] }> = [];
  const server = azure.c.registryLoginServer;
  if (server !== azure.c.registry + '.azurecr.io') throw new Error('Fleet image registry is not the exact stamp ACR.');
  for (const image of new Set(images)) {
    const reference = /^([^/]+)\/([a-z0-9]+(?:[._/-][a-z0-9]+)*)@(sha256:[a-f0-9]{64})$/.exec(image);
    if (!reference || reference[1] !== server) throw new Error('Fleet image must be digest-pinned in the stamp ACR.');
    const metadata = object(await azure.cli('acr', 'manifest', 'show-metadata', '--registry', azure.c.registry,
      '--name', reference[2] + '@' + reference[3], '--subscription', azure.c.subscription));
    if (metadata.digest !== reference[3]) throw new Error('Fleet image manifest digest differs.');
    const platforms = Array.isArray(metadata.manifests) ?
      (metadata.manifests as unknown[]).map(m => object(object(m).platform)) : [metadata];
    const architectures = new Set<string>();
    if (!platforms.length) throw new Error('Fleet image platform metadata is UNKNOWN.');
    for (const platform of platforms) {
      if (platform.os !== 'linux' || typeof platform.architecture !== 'string' ||
          !['amd64', 'arm64', 'arm', '386', 'ppc64le', 's390x', 'riscv64'].includes(platform.architecture))
        throw new Error('Fleet image Linux architecture metadata is UNKNOWN or unsupported.');
      architectures.add(platform.architecture);
    }
    observations.push({ image, architectures: [...architectures].sort() });
  }
  return observations;
}
