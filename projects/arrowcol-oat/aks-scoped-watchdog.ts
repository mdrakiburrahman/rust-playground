import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { parseArgs } from 'node:util';
import { Azure, canonical_json, type Config } from '../../infra/azure/bootstrap.js';
import { readRelease } from '../../infra/azure/benchmark-direct-cli.js';
import { inspectDirectRecovery } from '../../infra/azure/benchmark-direct-recovery.js';
import { recoverInterruptedDirectRun } from '../../infra/azure/benchmark-interrupted-custody.js';
import { directConditionalStopArgv } from '../../infra/azure/benchmark-stop-patch.js';
import { EffectBoundary, readEffectEnvelope, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { assert_run_fic_plan, read_run_fic_record, run_fic_ledger_path, type RunFicRecord } from '../../infra/azure/run_fic.js';
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
  'direct_recovery_unsealed' | 'direct_recovery_seal_observed' | 'direct_recovery_preserved_not_scored';

export function validateEffects(envelope: EffectEnvelope, inspect: boolean, recover: boolean) {
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
  const recoverable = (effect: (typeof extra)[number]) =>
    effect.transport === 'azure-rest' && ['PUT', 'DELETE'].includes(effect.method) ||
    effect.transport === 'process' && effect.resource === 'kubectl' &&
      (effect.method === 'KUBECTL_DIRECT_STOP' && effect.max_calls === 1 ||
        effect.method === 'KUBECTL_DELETE' && effect.max_calls === 1 && !!effect.body_sha256 ||
        ['KUBECTL_DIRECT_EXIT_WATCH', 'KUBECTL_DIRECT_SQL_READ'].includes(effect.method));
  if ((!inspect && extra.length > 0) || extra.some(effect => !permittedRead(effect) && !(recover && recoverable(effect))) ||
      !baseGet || put?.max_calls !== 1 ||
      !one('process', 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', 'az', tokenPath))
    throw new Error('Allocate only the exact active record GET, one conditional PUT, and the stock storage token request.');
  return put.id;
}

// The fence phase may mutate only the selected campaign record. Recovery is an explicit,
// separately allocated action on the original workload after the durable fence.
function scopedFenceTick(store: CampaignStore, scope: Selection, envelope: EffectEnvelope,
  record: () => RunFicRecord | undefined, pinIdentity: (name: string, value: unknown) => void,
  hasPin: (name: string) => boolean, canFence: () => boolean, signal: AbortSignal,
  inspect?: (current: StoredCampaign, scopedRead: CampaignStore['read']) => Promise<ScopedEvent>) {
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
      const observedStatus = await inspect(current, async () => {
        const observed = await read();
        if (observed) assertInventory(observed);
        return observed;
      });
      return event(observedStatus, now);
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
      if (inspect && after.state.status === 'fenced') {
        return event(await inspect(after, async () => {
          const observed = await read();
          if (observed) assertInventory(observed);
          return observed;
        }), now);
      }
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
    'namespace-owner': { type: 'string' }, 'expires-at': { type: 'string' }, 'fic-record': { type: 'string' }, 'release-file': { type: 'string' }, 'recover-interrupted-context': { type: 'string' },
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
  const recoveryContext = values['recover-interrupted-context'];
  if (recoveryContext !== undefined && (!release || !/^[A-Za-z0-9._:-]{1,128}$/.test(recoveryContext)))
    throw new Error('Interrupted recovery requires an original release and exact Kubernetes context.');
  const fenceEffectId = validateEffects(envelope, !!release, !!recoveryContext);
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
      ficPath, cluster: config.cluster, ...(release ? { release } : {}), ...(recoveryContext ? { recoveryContext } : {}) });
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
      const azure = fixture?.azure ?? new Azure(config);
      const inspected = await inspectDirectRecovery(azure,
        { read, async write() { throw new Error('Direct recovery may not write campaign state.'); } },
        current, release, stop, stateDir, !!recoveryContext);
      if (!recoveryContext || inspected.seal)
        return inspected.seal ? 'direct_recovery_seal_observed' : 'direct_recovery_unsealed';
      const { arm, fic } = inspected;
      pin(path.join(stateDir, 'direct-recovery-origin.json'), { arm, aks: fic.aksId, server: fic.server,
        identity: fic.identityId, credential: fic.credentialName, ownershipNonce: fic.ownershipNonce });
      validateRecoveryTargets(envelope, config, arm, fic, recoveryContext);
      return recoverInterruptedDirectRun(azure, inspected, recoveryContext, stateDir, stop);
    } : undefined);
    const result = await supervise(stop, () => withEffectBoundary(boundary, tick, undefined, stop));
    if (expires.aborted) throw new Error('Scoped watchdog allocation expired.');
    return result;
  } finally { fs.rmdirSync(lock); }
}

export function validateRecoveryTargets(envelope: EffectEnvelope, config: Config,
  arm: Awaited<ReturnType<typeof inspectDirectRecovery>>['arm'], fic: RunFicRecord, recoveryContext: string) {
  for (const [role, uid] of [['arrowcol-router', arm.routerUid], ['arrowcol', arm.writer.deploymentUid],
    ['delta-maintenance', arm.maintenance.deploymentUid]] as const) {
    const argv = directConditionalStopArgv(recoveryContext, fic.server, arm.origin, role);
    if (!envelope.effects.some(rule => rule.transport === 'process' && rule.method === 'KUBECTL_DIRECT_STOP' &&
        rule.resource === 'kubectl' && rule.path === JSON.stringify({ argv, uid }) && rule.max_calls === 1))
      throw new Error('Missing exact conditional stop allocation for the original workload.');
  }
  const checkpointPath = '/' + config.checkpoints;
  const storage = 'https://' + config.account + '.blob.core.windows.net|https://storage.azure.com/';
  const direct = checkpointPath + '/_arrowcol-custody/direct/' + arm.origin.runId + '/' + arm.origin.namespaceUid;
  const ledger = checkpointPath + '/' + run_fic_ledger_path(config);
  const namespaceJournal = checkpointPath + '/_arrowcol-custody/namespaces/' + arm.origin.runId + '/' + arm.origin.namespaceUid + '.json';
  const ficPath = fic.identityId + '/federatedIdentityCredentials/' + fic.credentialName + '?api-version=2023-01-31';
  const namespaceDelete = JSON.stringify(['--server', fic.server, '--insecure-skip-tls-verify=false',
    '--request-timeout', '15s', 'delete', '--raw', '/api/v1/namespaces/' + fic.namespace, '-f', '-']);
  const namespaceDigest = createHash('sha256').update(canonical_json({ apiVersion: 'v1', kind: 'DeleteOptions',
    preconditions: { uid: fic.namespaceUid } })).digest('hex');
  for (const rule of envelope.effects) {
    if (rule.transport === 'azure-rest' && rule.method === 'DELETE' &&
        (rule.resource !== 'https://management.azure.com|https://management.azure.com/' || rule.path !== ficPath || rule.max_calls !== 1))
      throw new Error('Recovery DELETE allocation is not the exact original FIC.');
    if (rule.transport === 'process' && rule.method === 'KUBECTL_DELETE' &&
        (rule.path !== namespaceDelete || rule.body_sha256 !== namespaceDigest))
      throw new Error('Recovery DELETE allocation is not the exact original namespace UID.');
    if (rule.transport !== 'azure-rest' || rule.method !== 'PUT') continue;
    const fixed = [ledger, ledger + '?comp=lease', namespaceJournal, checkpointPath + '?comp=lease&restype=container',
      checkpointPath + '?restype=container&comp=metadata', direct + '/interrupted/seal.json',
      ...['arrowcol-router', 'arrowcol', 'delta-maintenance'].map(role => direct + '/stop-' + role + '.json'),
      ...['arrowcol', 'delta-maintenance'].map(role => direct + '/exit-' + role + '.json')];
    const commit = rule.path.startsWith(direct + '/interrupted/') && /^delta-[0-9]{20}[.]json$/.test(rule.path.slice((direct + '/interrupted/').length));
    if (rule.resource !== storage || (!fixed.includes(rule.path) && !commit))
      throw new Error('Recovery PUT allocation is outside the original custody targets.');
  }
}
