import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { parseArgs } from 'node:util';
import { Azure, canonical_json, type Config } from '../../infra/azure/bootstrap.js';
import { readRelease } from '../../infra/azure/benchmark-direct-cli.js';
import { inspectDirectRecovery } from '../../infra/azure/benchmark-direct-recovery.js';
import { EffectBoundary, readEffectEnvelope, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { assert_run_fic_plan, read_run_fic_record, type RunFicRecord } from '../../infra/azure/run_fic.js';
import { createBlobCampaignStore, getAccessToken, MAX_CAMPAIGN_LIFETIME_MS,
  type CampaignStore, type StoredCampaign, type Fetcher, type TokenProvider } from './aks-campaign.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { reconcileOnce } from './aks-reconcile.js';

const uuid = /^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const dnsLabel = /^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/;
const ACTIVE_RECORD = 'active.json';
const READ_LIMIT = 4 * 1024 * 1024;
const IO_MS = 15_000;

function absoluteTime(value: string | undefined) {
  const ms = value ? Date.parse(value) : NaN;
  if (!Number.isSafeInteger(ms) || new Date(ms).toISOString() !== value)
    throw new Error('Expected canonical UTC expiry (YYYY-MM-DDTHH:mm:ss.sssZ).');
  return ms;
}

function pin(filename: string, value: unknown) {
  const serialized = canonical_json(value);
  if (fs.existsSync(filename)) {
    const stat = fs.lstatSync(filename);
    if (!stat.isFile() || (stat.mode & 0o077) !== 0 || fs.readFileSync(filename, 'utf8') !== serialized)
      throw new Error('Scoped watchdog identity changed; refusing to rebind.');
    return;
  }
  const fd = fs.openSync(filename, 'wx', 0o600);
  try { fs.writeFileSync(fd, serialized); fs.fsyncSync(fd); }
  finally { fs.closeSync(fd); }
  const dir = fs.openSync(path.dirname(filename), 'r');
  try { fs.fsyncSync(dir); } finally { fs.closeSync(dir); }
}

type Selection = { ownerId: string; namespace: string; namespaceOwner: string; expiresAtMs: number };
type ScopedEvent = 'waiting_campaign' | 'waiting_inventory' | 'waiting_binding' | 'bound_observe_only' | 'fence_budget_blocked' |
  'bound_fence_only' | 'fenced_cleanup_blocked' | 'selected_complete' | 'selected_complete_unsettled' |
  'direct_recovery_unsealed' | 'direct_recovery_seal_observed';

function validateEffects(envelope: EffectEnvelope, inspect: boolean) {
  const origin = `https://${envelope.storage_account}.blob.core.windows.net/${envelope.state_container}`;
  const blobPath = `/${envelope.state_container}/${ACTIVE_RECORD}`;
  const tokenPath = JSON.stringify(['account', 'get-access-token', '--subscription', envelope.subscription_id,
    '--resource', 'https://storage.azure.com/', '--query', 'accessToken', '--output', 'tsv', '--only-show-errors']);
  const one = (transport: string, method: string, resource: string, target: string) =>
    envelope.effects.find(rule => rule.transport === transport && rule.method === method &&
      rule.resource === resource && rule.path === target && rule.body_sha256 === undefined);
  const put = one('campaign-blob', 'PUT', origin, blobPath);
  const baseGet = one('campaign-blob', 'GET', origin, blobPath);
  const baseToken = one('process', 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', 'az', tokenPath);
  const extra = envelope.effects.filter(effect => effect !== baseGet && effect !== put && effect !== baseToken);
  const permittedRead = (effect: (typeof extra)[number]) =>
    effect.transport === 'azure-rest' && ['GET', 'HEAD'].includes(effect.method) ||
    effect.transport === 'process' && effect.method === 'KUBECTL_GET' && effect.resource === 'kubectl' ||
    effect.transport === 'process' && effect.method === 'AZURE_ACCOUNT_GET-ACCESS-TOKEN' && effect.resource === 'az';
  if ((!inspect && extra.length > 0) || extra.some(effect => !permittedRead(effect)) ||
      !baseGet || put?.max_calls !== 1 ||
      !one('process', 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', 'az', tokenPath))
    throw new Error('Allocate only the exact active record GET, one conditional PUT, and the stock storage token request.');
  return put.id;
}

// The only mutable target in this mode is the selected campaign's own ETag-conditional record.
// Kubernetes, FIC, publisher, and shared capacity effects are not reachable.
function scopedFenceTick(store: CampaignStore, scope: Selection, envelope: EffectEnvelope,
  record: () => RunFicRecord | undefined, pinIdentity: (name: string, value: unknown) => void,
  hasPin: (name: string) => boolean, canFence: () => boolean, signal: AbortSignal,
  inspect?: (current: StoredCampaign, scopedRead: CampaignStore['read']) => Promise<boolean>) {
  const event = (scopeStatus: ScopedEvent, now: number) => ({ event: 'scoped_campaign_custody_evaluation',
    scopeStatus, campaignId: envelope.campaign_id, runId: envelope.run_id, observedAtMs: now });
  const deadline = () => {
    signal.throwIfAborted();
    if (Date.now() >= scope.expiresAtMs) throw new Error('Scoped watchdog allocation expired.');
  };
  const read = () => {
    deadline();
    return store.read({ maxResponseBodyBytes: READ_LIMIT, deadlineUnixMs: Math.min(Date.now() + IO_MS, scope.expiresAtMs) });
  };
  return async () => {
    const now = Date.now();
    const current = await read();
    deadline();
    if (!current) {
      if (hasPin('campaign.json')) throw new Error('Original selected campaign disappeared.');
      return event('waiting_campaign', now);
    }
    if (!hasPin('campaign.json') && current.state.campaignId !== envelope.campaign_id && current.state.status === 'complete')
      return event('waiting_campaign', now);
    const assertCampaign = ({ state }: NonNullable<typeof current>) => {
      if (state.campaignId !== envelope.campaign_id || state.ownerId !== scope.ownerId)
        throw new Error('Active campaign is not the selected campaign/owner; refusing adoption.');
      pinIdentity('campaign.json', { startedAtMs: state.startedAtMs });
    };
    assertCampaign(current);
    if (!current.state.runInventory) {
      if (hasPin('identity.json')) throw new Error('Original bound RunInventory disappeared.');
      return event('waiting_inventory', now);
    }
    const assertInventory = ({ state }: typeof current) => {
      assertCampaign({ state, etag: '' });
      const inv = state.runInventory;
      if (!inv || inv.campaign_id !== envelope.campaign_id || inv.owner_id !== scope.ownerId ||
          inv.cell_run_id !== envelope.run_id || inv.namespace.name !== scope.namespace ||
          inv.namespace.owner_token !== scope.namespaceOwner || inv.fencing_token !== 1 ||
          state.fencingToken !== (state.status === 'active' ? 1 : 2) &&
            !(state.status === 'complete' && state.fencingToken === 1))
        throw new Error('Selected run, namespace owner or producer/controller fence changed.');
      return inv;
    };
    const inventory = assertInventory(current);
    if (inspect && ['fenced', 'cleaning'].includes(current.state.status)) {
      const observedSeal = await inspect(current, async () => {
        const observed = await read();
        if (observed) assertInventory(observed);
        return observed;
      });
      return event(observedSeal ? 'direct_recovery_seal_observed' : 'direct_recovery_unsealed', now);
    }
    const fic = record();
    if (!fic || !inventory.namespace.uid || !inventory.fic?.plan) {
      if (hasPin('identity.json')) throw new Error('Original bound FIC or namespace identity disappeared.');
      return event('waiting_binding', now);
    }
    const resourceId = `${fic.identityId}/federatedIdentityCredentials/${fic.credentialName}`;
    if (fic.phase !== 'ready' || fic.runId !== envelope.run_id || fic.namespace !== scope.namespace ||
        fic.namespaceOwner !== scope.namespaceOwner || fic.namespaceUid !== inventory.namespace.uid ||
        inventory.fic.resource_id !== resourceId || inventory.fic.name !== fic.credentialName)
      throw new Error('Original FIC record and live RunInventory identity do not match.');
    assert_run_fic_plan(inventory.fic.plan, fic);
    pinIdentity('identity.json', fic);
    const status = (state: typeof current.state) => {
      if (state.status === 'complete') return 'selected_complete';
      if (state.status === 'complete_unsettled') return 'selected_complete_unsettled';
      if (state.status !== 'active') return 'fenced_cleanup_blocked';
      return undefined;
    };
    const terminal = status(current.state);
    if (terminal) return event(terminal, now);
    if (current.state.mode !== 'armed' || inventory.fic.state !== 'observed') return event('bound_observe_only', now);
    if (!canFence()) return event('fence_budget_blocked', now);
    const assertBound = (value: typeof current) => {
      const inv = assertInventory(value);
      if (inv.namespace.uid !== fic.namespaceUid || inv.fic?.resource_id !== resourceId ||
          inv.fic.name !== fic.credentialName || value.state.status === 'active' &&
          (inv.fic.state !== 'observed' || value.state.mode !== 'armed'))
        throw new Error('Original bound identity changed before the fence or its readback.');
      assert_run_fic_plan(inv.fic.plan, fic);
    };
    let authorityETag: string | undefined;
    let wrote = false;
    const fencedStore: CampaignStore = {
      async read() {
        const selected = await read();
        deadline();
        if (!selected) throw new Error('Selected campaign disappeared during fence evaluation.');
        assertBound(selected);
        authorityETag = selected.etag;
        return selected;
      },
      async write(state, etag) {
        deadline();
        if (!etag || etag !== authorityETag || state.status !== 'fenced' || state.fencingToken !== 2)
          throw new Error('Scoped watchdog can dispatch only the original conditional producer fence.');
        assertBound({ state, etag });
        const result = await store.write(state, etag);
        wrote = true;
        return result;
      },
    };
    const outcome = await reconcileOnce(fencedStore, now, true);
    if (wrote || outcome.status === 'terminal') {
      const after = await fencedStore.read();
      if (!after || after.state.status === 'active') throw new Error('Conditional fence or terminal state was not confirmed by durable readback.');
      return event(status(after.state)!, now);
    }
    return event('bound_fence_only', now);
  };
}

export async function watchSelectedRun(args: string[], signal: AbortSignal,
  supervise: (signal: AbortSignal, evaluate: () => Promise<Record<string, unknown>>) => Promise<number>,
  fixture?: { config: Config; fetcher: Fetcher; token: TokenProvider; azure?: Azure }) {
  const { values, tokens } = parseArgs({ args, allowPositionals: false, tokens: true, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, 'effect-envelope': { type: 'string' },
    'state-dir': { type: 'string' }, 'owner-id': { type: 'string' }, namespace: { type: 'string' },
    'namespace-owner': { type: 'string' }, 'expires-at': { type: 'string' }, 'fic-record': { type: 'string' }, 'release-file': { type: 'string' },
  } });
  if (new Set(tokens.map(token => token.kind === 'option' ? token.name : '')).size !== tokens.length)
    throw new Error('Duplicate scoped watchdog option.');
  const env = values['env-file'], stamp = values['stamp-config'], envelopePath = values['effect-envelope'],
    stateDir = values['state-dir'], ownerId = values['owner-id'], namespace = values.namespace,
    namespaceOwner = values['namespace-owner'], ficPath = values['fic-record'];
  if (!env || !stamp || !envelopePath || !stateDir || !ownerId || !uuid.test(ownerId) || !namespace ||
      !dnsLabel.test(namespace) || !namespaceOwner || !dnsLabel.test(namespaceOwner) || !ficPath ||
      !path.isAbsolute(stateDir) || !path.isAbsolute(ficPath))
    throw new Error('Scoped watchdog requires the exact owner, namespace/owner token, private paths and expiry.');
  const expiresAtMs = absoluteTime(values['expires-at']);
  if (expiresAtMs <= Date.now() || expiresAtMs - Date.now() > MAX_CAMPAIGN_LIFETIME_MS)
    throw new Error('Scoped watchdog expiry must be in the next six hours.');
  const scope = { ownerId, namespace, namespaceOwner, expiresAtMs };
  const config = fixture?.config ?? loadCustodyConfig(['--env-file', env, '--stamp-config', stamp]).config;
  const envelope = readEffectEnvelope(envelopePath);
  if (envelope.subscription_id !== config.subscription || envelope.resource_group !== config.group ||
      envelope.storage_account !== config.account || envelope.state_container !== config.checkpoints)
    throw new Error('Scoped effect allocation does not match the explicitly selected stamp.');
  const release = values['release-file'] ? readRelease(values['release-file']) : undefined;
  const fenceEffectId = validateEffects(envelope, !!release);
  const stat = fs.lstatSync(stateDir);
  if (!stat.isDirectory() || (stat.mode & 0o077) !== 0 || fs.realpathSync(stateDir) !== stateDir)
    throw new Error('Scoped watchdog requires a private canonical local state directory.');
  const lock = path.join(stateDir, 'watch.lock');
  fs.mkdirSync(lock, { mode: 0o700 });
  const expires = AbortSignal.timeout(Math.max(0, expiresAtMs - Date.now()));
  const stop = AbortSignal.any([signal, expires]);
  try {
    const journal = path.join(stateDir, 'effects.jsonl');
    const scopePath = path.join(stateDir, 'scope.json');
    if (fs.existsSync(scopePath) && !fs.existsSync(journal))
      throw new Error('Scoped effect journal is missing; refusing to reset the allocation.');
    if (!fs.existsSync(journal)) {
      const fd = fs.openSync(journal, 'wx', 0o600);
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    }
    pin(scopePath, { ...scope, envelopeHash: createHash('sha256').update(canonical_json(envelope)).digest('hex'),
      ficPath, cluster: config.cluster, ...(release ? { release } : {}) });
    const boundary = new EffectBoundary(envelope, journal);
    const store = createBlobCampaignStore({ account: config.account, container: config.checkpoints,
      subscriptionId: config.subscription, recordPath: ACTIVE_RECORD }, (input, init) => {
      stop.throwIfAborted();
      return (fixture?.fetcher ?? fetch)(input, { ...init, signal: AbortSignal.any([stop, AbortSignal.timeout(IO_MS),
        ...(init?.signal ? [init.signal] : [])]) });
    }, fixture?.token ?? (resource => getAccessToken(resource, config.subscription, {})));
    const hasPin = (name: string) => fs.existsSync(path.join(stateDir, name));
    const tick = scopedFenceTick(store, scope, envelope, () => {
      if (!fs.existsSync(ficPath)) {
        return undefined;
      }
      return read_run_fic_record(ficPath, config);
    }, (name, value) => pin(path.join(stateDir, name), value), hasPin, () => !boundary.hasDispatched(fenceEffectId), stop,
    release ? async (current, read) => {
      const inspected = await inspectDirectRecovery(fixture?.azure ?? new Azure(config),
        { read, async write() { throw new Error('Direct recovery inspection may not write campaign state.'); } },
        current, release, stop, stateDir);
      return !!inspected.seal;
    } : undefined);
    const result = await supervise(stop, () => withEffectBoundary(boundary, tick, undefined, stop));
    if (expires.aborted) throw new Error('Scoped watchdog allocation expired.');
    return result;
  } finally { fs.rmdirSync(lock); }
}
