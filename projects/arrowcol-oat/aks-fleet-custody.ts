import { randomUUID } from 'node:crypto';
import { TextDecoder } from 'node:util';
import { canonical_json, LEASE_SECONDS, type Azure } from '../../infra/azure/bootstrap.js';
import { HttpFailure } from '../../infra/azure/authority.js';
import { CampaignConflictError, heartbeatCampaign, newCampaignState, validateCampaignState, type CampaignState, type CampaignStore } from './aks-campaign.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

export interface FleetBinding {
  clusterId: string; server: string;
  pools: Array<{ name: string; sku: string; vcpu: number; role: 'arrowcol' | 'client'; count: number; maxCount: number }>;
}
export interface FleetPoolObservation {
  name: string; resourceId: string; sku: string; count: number; provisioningState: string; etag: string; configurationHash: string;
}
export interface FleetIntentAuthority extends CleanupMutationAuthority {
  binding: FleetBinding;
  readonly baselineEstablished?: boolean;
  establishBaseline(observations: FleetPoolObservation[]): Promise<void>;
  recordObservation(observation: FleetPoolObservation): Promise<void>;
  beforeDispatch(name: string, count: number, etag: string): Promise<string>;
  afterDispatch(name: string, dispatchId: string, state: 'accepted' | 'conflict' | 'rejected' | 'uncertain'): Promise<void>;
  complete(): Promise<void>;
  close(): Promise<void>;
}

export class FleetUnsettledDispatchError extends Error {
  constructor() { super('Dispatch outcome remains uncertain.'); this.name = 'FleetUnsettledDispatchError'; }
}

type DispatchState = 'intent' | 'accepted' | 'conflict' | 'rejected' | 'uncertain';
interface PoolRecord { baseline: FleetPoolObservation; observed: FleetPoolObservation; dispatch: null | {
  id: string; count: number; etag: string; state: DispatchState;
}; }
interface Journal {
  version: 1; campaignId: string; ownerId: string; fencingToken: number; binding: FleetBinding;
  status: 'active' | 'complete'; pools: Record<string, PoolRecord>;
}
const journalPath = 'oat-fleet/active.v1.json';
const campaignPath = 'active.json';
const isRecord = (x: unknown): x is Record<string, unknown> => !!x && typeof x === 'object' && !Array.isArray(x);
const exact = (x: unknown, keys: string[]) => isRecord(x) && Object.keys(x).sort().join(',') === keys.sort().join(',');
const text = (x: unknown): x is string => typeof x === 'string' && x.length > 0 && x.length <= 2048;
const integer = (x: unknown): x is number => Number.isSafeInteger(x) && (x as number) >= 0;
const terminalFailure = (state: string) => state === 'Failed' || state === 'Canceled';
const uuid = (x: unknown): x is string => typeof x === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(x);
function bindingValue(x: unknown): FleetBinding {
  if (!exact(x, ['clusterId', 'server', 'pools']) || !isRecord(x) || !text(x.clusterId) || !/^\/subscriptions\/[^/]+\/resourceGroups\/[^/]+\/providers\/Microsoft\.ContainerService\/managedClusters\/[^/]+$/i.test(x.clusterId) ||
      !text(x.server) || !Array.isArray(x.pools) || x.pools.length < 2 || x.pools.length > 129) throw new Error('Invalid fleet binding.');
  const names = new Set<string>();
  for (const pool of x.pools) {
    if (!exact(pool, ['name', 'sku', 'vcpu', 'role', 'count', 'maxCount']) || !isRecord(pool) ||
        typeof pool.name !== 'string' || !/^[a-z][a-z0-9]{0,11}$/.test(pool.name) || names.has(pool.name) ||
        !text(pool.sku) || !integer(pool.vcpu) || !pool.vcpu || !['arrowcol', 'client'].includes(String(pool.role)) ||
        !integer(pool.count) || pool.count < 1 || !integer(pool.maxCount) || pool.maxCount < pool.count) throw new Error('Invalid approved fleet pool.');
    names.add(pool.name);
  }
  const pools = x.pools as Array<{ role: string; count: number }>;
  const collectors = pools.filter(pool => pool.role === 'arrowcol');
  const clients = pools.filter(pool => pool.role === 'client');
  const clientNodes = clients.reduce((sum, pool) => sum + pool.count, 0);
  if (collectors.length !== 1 || collectors[0]!.count !== 1 || clients.length < 1 || clientNodes > 128)
    throw new Error('Fleet requires exactly one collector and 1..128 client nodes.');
  return structuredClone(x) as unknown as FleetBinding;
}
function observationValue(x: unknown, binding: FleetBinding): FleetPoolObservation {
  if (!exact(x, ['name', 'resourceId', 'sku', 'count', 'provisioningState', 'etag', 'configurationHash']) || !isRecord(x) ||
      !text(x.name) || !text(x.resourceId) || !text(x.sku) || !integer(x.count) || !text(x.provisioningState) ||
      !text(x.etag) || !text(x.configurationHash)) throw new Error('Invalid fleet observation.');
  const pool = binding.pools.find(p => p.name === x.name);
  if (!pool || x.sku !== pool.sku || x.resourceId.toLowerCase() !== (binding.clusterId + '/agentPools/' + pool.name).toLowerCase())
    throw new Error('Observation is not an approved pool identity.');
  return structuredClone(x) as unknown as FleetPoolObservation;
}
function parseJournal(value: unknown): Journal {
  if (!exact(value, ['version', 'campaignId', 'ownerId', 'fencingToken', 'binding', 'status', 'pools']) || !isRecord(value) ||
      value.version !== 1 || !uuid(value.campaignId) || !uuid(value.ownerId) || !integer(value.fencingToken) || value.fencingToken < 1 ||
      !['active', 'complete'].includes(String(value.status)) || !isRecord(value.pools)) throw new Error('Invalid fleet journal.');
  const binding = bindingValue(value.binding);
  const keys = Object.keys(value.pools);
  if (keys.length && (keys.length !== binding.pools.length || binding.pools.some(p => !keys.includes(p.name)))) throw new Error('Incomplete fleet ownership.');
  for (const name of keys) {
    const entry = value.pools[name];
    if (!exact(entry, ['baseline', 'observed', 'dispatch']) || !isRecord(entry)) throw new Error('Invalid pool journal.');
    const baseline = observationValue(entry.baseline, binding), observed = observationValue(entry.observed, binding);
    if (baseline.name !== name || observed.name !== name || baseline.count !== 0 || baseline.provisioningState !== 'Succeeded' ||
        baseline.configurationHash !== observed.configurationHash || ![0, binding.pools.find(p => p.name === name)!.count].includes(observed.count) ||
        observed.provisioningState !== 'Succeeded' && !terminalFailure(observed.provisioningState)) throw new Error('Invalid fleet ownership baseline.');
    if (entry.dispatch !== null) {
      const d = entry.dispatch;
      if (!exact(d, ['id', 'count', 'etag', 'state']) || !isRecord(d) || !uuid(d.id) || !integer(d.count) ||
          !text(d.etag) || !['intent', 'accepted', 'conflict', 'rejected', 'uncertain'].includes(String(d.state)) ||
          ![0, binding.pools.find(p => p.name === name)!.count].includes(d.count) ||
          d.count === observed.count && !(d.count === 0 && terminalFailure(observed.provisioningState)) ||
          terminalFailure(observed.provisioningState) && d.count !== 0 || d.etag !== observed.etag) throw new Error('Invalid fleet dispatch.');
    }
    if (value.status === 'complete' && (observed.count !== 0 || observed.provisioningState !== 'Succeeded' || entry.dispatch !== null))
      throw new Error('Completed fleet is not settled at zero.');
  }
  if (value.status === 'complete' && !keys.length) throw new Error('Completed fleet lacks ownership.');
  return structuredClone(value) as unknown as Journal;
}

export async function acquireFleetAuthority(azure: Azure, input: FleetBinding, options: { ownerId: string; resume: boolean }): Promise<FleetIntentAuthority> {
  const binding = bindingValue(input);
  if (!uuid(options.ownerId) || typeof options.resume !== 'boolean') throw new Error('Invalid fleet owner.');
  const leaseId = randomUUID();
  let acquired = false, leaseUntil = 0;
  let released = false, poisoned = false, completed = false;
  let journal!: Journal, journalEtag = '';
  let queue: Promise<void> = Promise.resolve();
  const read = async (path: string) => {
    const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, {}, true, true);
    if (!bytes) return undefined;
    if (!headers.etag || bytes.length > 1024 * 1024) throw new Error('Invalid custody blob or ETag.');
    return { value: JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as unknown, etag: headers.etag };
  };
  // Lease the journal, not the container used by native publisher control.
  const journalLease = async (action: 'acquire' | 'renew' | 'release', etag?: string) => {
    const started = performance.now();
    if (action === 'acquire' && !etag) throw new Error('Fleet journal lease requires an exact ETag.');
    if (action === 'renew' && started >= leaseUntil) throw new Error('Fleet journal lease expired.');
    await azure.blob('PUT', azure.c.checkpoints, journalPath, { comp: 'lease' }, Buffer.alloc(0),
      { 'x-ms-lease-action': action, ...(action === 'acquire' ? { 'x-ms-lease-duration': String(LEASE_SECONDS),
        'x-ms-proposed-lease-id': leaseId, 'if-match': etag ?? '' } : { 'x-ms-lease-id': leaseId }) });
    if (action === 'acquire') acquired = true;
    if (action !== 'release') {
      leaseUntil = started + LEASE_SECONDS * 1000;
      if (performance.now() >= leaseUntil) throw new Error('Fleet journal lease confirmation arrived too late.');
    }
  };
  const write = async (path: string, value: unknown, etag?: string, lease?: string) => {
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, path, undefined, canonical_json(value),
      { 'x-ms-blob-type': 'BlockBlob', 'content-type': 'application/json', ...(etag ? { 'if-match': etag } : { 'if-none-match': '*' }),
        ...(lease ? { 'x-ms-lease-id': lease } : {}) });
    if (!headers.etag) throw new Error('Custody write has no ETag.');
    return headers.etag;
  };
  const store: CampaignStore = {
    async read() { const result = await read(campaignPath); return result && { state: validateCampaignState(result.value), etag: result.etag }; },
    async write(state, etag) {
      try { return await write(campaignPath, validateCampaignState(state), etag); }
      catch (error) { if (error instanceof HttpFailure && error.code === 412) throw new CampaignConflictError(); throw error; }
    },
  };
  const save = async (next: Journal) => {
    parseJournal(next);
    try {
      if (!acquired || !journalEtag || performance.now() >= leaseUntil) throw new Error('Fleet journal lease is not current.');
      journalEtag = await write(journalPath, next, journalEtag, leaseId); journal = next;
    }
    catch (error) { poisoned = true; throw error; }
  };
  const exclusive = <T>(fn: () => Promise<T>): Promise<T> => {
    const operation = queue.then(fn); queue = operation.then(() => undefined, () => undefined); return operation;
  };
  const check = async () => {
    if (released || poisoned || completed) throw new Error('Fleet authority is closed, fenced or uncertain.');
    try {
      await journalLease('renew');
      const currentJournal = await read(journalPath);
      if (!currentJournal || currentJournal.etag !== journalEtag || canonical_json(parseJournal(currentJournal.value)) !== canonical_json(journal)) throw new Error('Fleet journal fencing changed.');
      const current = await store.read();
      if (!current || current.state.campaignId !== journal.campaignId || current.state.ownerId !== journal.ownerId ||
          current.state.fencingToken !== journal.fencingToken || current.state.mode !== 'observe' || current.state.status !== 'active' ||
          current.state.runInventory || current.state.operations.length || current.state.scaleTrials?.length ||
          Date.now() < current.state.heartbeatAtMs || Date.now() >= current.state.expiresAtMs || Date.now() - current.state.heartbeatAtMs >= 180_000) throw new Error('Fleet campaign is fenced.');
      await journalLease('renew');
    } catch (error) { poisoned = true; throw error; }
  };
  let timer: ReturnType<typeof setInterval> | undefined;
  try {
    const existing = await read(journalPath);
    const old = existing && parseJournal(existing.value);
    if (options.resume && !old || !options.resume && old && old.status !== 'complete') throw new Error('Fleet journal requires exact recovery.');
    if (options.resume && canonical_json(old!.binding) !== canonical_json(binding)) throw new Error('Fleet recovery binding differs.');
    if (options.resume && old!.ownerId !== options.ownerId) throw new Error('Fleet recovery requires the retained owner.');
    const campaign = await store.read();
    if (options.resume && campaign &&
        (campaign.state.campaignId !== old!.campaignId || campaign.state.ownerId !== old!.ownerId))
      throw new Error('Another campaign or owner holds fleet recovery.');
    if (campaign && !(campaign.state.status === 'complete' && campaign.state.cleanupStatus === 'verified') &&
        !(options.resume && campaign.state.campaignId === old!.campaignId && campaign.state.ownerId === old!.ownerId &&
          campaign.state.mode === 'observe' &&
          campaign.state.status === 'active' && !campaign.state.runInventory && !campaign.state.operations.length && !campaign.state.scaleTrials?.length))
      throw new Error('Another campaign owns the stamp.');
    const id = options.resume ? old!.campaignId : randomUUID();
    const token = options.resume ? Math.max(old!.fencingToken, campaign?.state.campaignId === id ? campaign.state.fencingToken : 0) + 1 : 1;
    const candidate: Journal = options.resume ? { ...old!, ownerId: options.ownerId, fencingToken: token } :
      { version: 1, campaignId: id, ownerId: options.ownerId, fencingToken: token, binding, status: 'active', pools: {} };
    parseJournal(candidate);
    // Only the acknowledged conditional creator may use a newly created journal on this attempt.
    journalEtag = existing?.etag ?? await write(journalPath, candidate);
    await journalLease('acquire', journalEtag);
    const locked = await read(journalPath);
    if (!locked || locked.etag !== journalEtag ||
        canonical_json(parseJournal(locked.value)) !== canonical_json(old ?? candidate)) throw new Error('Fleet journal changed before acquisition.');
    if (existing) await save(candidate);
    else journal = candidate;
    await journalLease('renew');
    const next: CampaignState = { ...newCampaignState(id, options.ownerId), fencingToken: token };
    await store.write(next, campaign?.etag);
    await check();
    let lastHeartbeat = Date.now();
    timer = setInterval(() => { void exclusive(async () => {
      await check();
      if (Date.now() - lastHeartbeat >= 30_000) { await heartbeatCampaign(store, options.ownerId, token); lastHeartbeat = Date.now(); }
    }).catch(() => { poisoned = true; }); }, 5_000);
    timer.unref();
    const authority: FleetIntentAuthority = {
      mode: 'producer', campaignId: id, ownerId: options.ownerId, fencingToken: token, binding: structuredClone(binding),
      get baselineEstablished() { return Object.keys(journal.pools).length === binding.pools.length; },
      assertCurrent: () => exclusive(check),
      establishBaseline: observations => exclusive(async () => {
        await check();
        if (journal.status !== 'active' || Object.keys(journal.pools).length || observations.length !== binding.pools.length) throw new Error('Baseline already established or incomplete.');
        const pools: Record<string, PoolRecord> = {};
        for (const item of observations) {
          const obs = observationValue(item, binding);
          if (pools[obs.name] || obs.count !== 0 || obs.provisioningState !== 'Succeeded') throw new Error('Fleet baseline must be unique, settled and zero.');
          pools[obs.name] = { baseline: obs, observed: obs, dispatch: null };
        }
        await save({ ...journal, pools });
      }),
      recordObservation: observation => exclusive(async () => {
        await check();
        const obs = observationValue(observation, binding), pool = journal.pools[obs.name];
        if (!pool || obs.configurationHash !== pool.baseline.configurationHash) throw new Error('Pool identity or configuration is uncertain.');
        const d = pool.dispatch;
        const approved = binding.pools.find(p => p.name === obs.name)!;
        if (![0, approved.count].includes(obs.count)) throw new Error('Unowned pool count change.');
        if (d && ['intent', 'accepted', 'uncertain'].includes(d.state)) {
          if (terminalFailure(obs.provisioningState)) {
            if (obs.etag === d.etag || ![pool.observed.count, d.count].includes(obs.count)) throw new FleetUnsettledDispatchError();
          } else if (obs.provisioningState !== 'Succeeded' || obs.count !== d.count ||
              d.count === pool.observed.count && obs.etag === d.etag) throw new FleetUnsettledDispatchError();
        } else {
          if (obs.provisioningState !== 'Succeeded' && !terminalFailure(obs.provisioningState)) throw new Error('Pool provisioning state is uncertain.');
          if (obs.count !== pool.observed.count) throw new Error('Unowned or rejected pool count change.');
        }
        await save({ ...journal, pools: { ...journal.pools, [obs.name]: { ...pool, observed: obs, dispatch: null } } });
      }),
      beforeDispatch: (name, count, etag) => exclusive(async () => {
        await check();
        const pool = journal.pools[name], approved = binding.pools.find(p => p.name === name);
        if (journal.status !== 'active' || !pool || !approved || pool.dispatch || etag !== pool.observed.etag ||
            ![0, approved.count].includes(count) || options.resume && count !== 0 ||
            terminalFailure(pool.observed.provisioningState) && count !== 0 ||
            count === pool.observed.count && !(count === 0 && terminalFailure(pool.observed.provisioningState))) throw new Error('Fleet dispatch is not authorized by settled ownership.');
        const id = randomUUID();
        await save({ ...journal, pools: { ...journal.pools, [name]: { ...pool, dispatch: { id, count, etag, state: 'intent' } } } });
        await check();
        return id;
      }),
      afterDispatch: (name, dispatchId, state) => exclusive(async () => {
        await check();
        const pool = journal.pools[name];
        if (!pool?.dispatch || pool.dispatch.id !== dispatchId || pool.dispatch.state !== 'intent' || !['accepted', 'conflict', 'rejected', 'uncertain'].includes(state)) throw new Error('Unknown or already settled fleet dispatch.');
        await save({ ...journal, pools: { ...journal.pools, [name]: { ...pool, dispatch: { ...pool.dispatch, state } } } });
      }),
      complete: () => exclusive(async () => {
        await check();
        if (Object.keys(journal.pools).length !== binding.pools.length || Object.values(journal.pools).some(p => p.dispatch || p.observed.count !== 0 || p.observed.provisioningState !== 'Succeeded')) throw new Error('Fleet is not proven settled at zero.');
        if (journal.status !== 'complete') await save({ ...journal, status: 'complete' });
        const campaign = await store.read();
        if (!campaign || campaign.state.campaignId !== id || campaign.state.ownerId !== options.ownerId || campaign.state.fencingToken !== token || campaign.state.mode !== 'observe' || campaign.state.status !== 'active') throw new Error('Fleet campaign changed before release.');
        try { await journalLease('renew'); await azure.blob('DELETE', azure.c.checkpoints, campaignPath, undefined, undefined, { 'if-match': campaign.etag }); }
        catch (error) { poisoned = true; throw error; }
        completed = true;
      }),
      close: () => exclusive(async () => { if (timer) clearInterval(timer); if (!released) { released = true; await journalLease('release'); } }),
    };
    return authority;
  } catch (error) {
    if (timer) clearInterval(timer);
    if (acquired) try { await journalLease('release'); } catch { /* preserve acquisition error */ }
    throw error;
  }
}
