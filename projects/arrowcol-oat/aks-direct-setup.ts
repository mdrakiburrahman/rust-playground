import { createHash, randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync, readdirSync, rmdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { canonical_json, run_tree_with_input, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { create_namespace_deletion_journal } from '../../infra/azure/aks-namespace-custody.js';
import { assert_owned_namespace_absent, cleanup_run_fic, delete_owned_namespace, find_run_fic_record, plan_run_fic,
  prepare_run_fic, recover_run_fic_record, observe_run_fic_cleanup, type RunFicRecord,
  type RunCleanupAuthority, type RunNamespaceIdentity } from '../../infra/azure/run_fic.js';
import { CampaignConflictError, directPoolOperationUrl, HEARTBEAT_INTERVAL_MS, MISSED_HEARTBEAT_LIMIT, writeDirectSetupOnce,
  type CampaignStore, type DirectSetupObjectAttempt, type DirectSetupPoolAttempt, type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { reconcileArmOperation } from './aks-reconcile.js';
import { buildAksPoolScalePayload } from './aks-arm-pool.js';
import { readCleanupBaseline } from './aks-default-providers.js';

const etagPattern = /^"[^"\r\n]+"$/;
const ownershipTag = 'arrowcol-direct-setup-attempt';
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
const arm = 'https://management.azure.com/';
const poolUrl = (resourceId: string) => `https://management.azure.com${resourceId}?api-version=2024-10-01`;

async function assertVersion(store: CampaignStore, expected: StoredCampaign, freshProducer = false) {
  const current = await store.read(), state = expected.state, now = Date.now();
  if (current?.etag !== expected.etag || canonical_json(current.state) !== canonical_json(state) ||
      freshProducer && (state.status !== 'active' || now < state.heartbeatAtMs || now >= state.expiresAtMs ||
        now - state.heartbeatAtMs >= HEARTBEAT_INTERVAL_MS * MISSED_HEARTBEAT_LIMIT)) throw new CampaignConflictError();
}

async function readPool(azure: Azure, resourceId: string) {
  const [raw, headers] = await azure.request('GET', poolUrl(resourceId), arm);
  const pool = JSON.parse(raw?.toString() ?? 'null'), etag = headers.etag;
  const properties = pool?.properties;
  if (headers[':status'] !== '200' || !etag || !etagPattern.test(etag) ||
      typeof pool?.id !== 'string' || pool.id.toLowerCase() !== resourceId.toLowerCase() ||
      properties?.provisioningState !== 'Succeeded' || properties?.mode !== 'User' ||
      properties.vmSize !== 'Standard_E192ids_v6' || properties.enableAutoScaling !== true || properties.maxCount !== 1)
    throw new Error('Exact versioned setup pool is unavailable.');
  return { pool, properties, etag };
}

async function submit(azure: Azure, attempt: DirectSetupPoolAttempt, body: string): Promise<DirectSetupPoolAttempt> {
  if (hash(body) !== attempt.bodySha256) throw new Error('Setup pool dispatch payload differs from the original intent.');
  const [, ack] = await azure.request('PUT', poolUrl(attempt.resourceId), arm, body,
    { 'Content-Type': 'application/json', 'If-Match': attempt.expectedEtag, 'x-ms-client-request-id': attempt.requestId }, false, false);
  const status = Number(ack[':status']), requestId = ack['x-ms-request-id'], operation = ack['azure-asyncoperation'];
  if (!(status === 200 || status === 201 || status === 202) || !requestId)
    throw new Error('Setup pool acknowledgement is unresolved; retain dispatch custody.');
  return { ...attempt, response: { status, requestId,
    ...(directPoolOperationUrl(operation, attempt.resourceId) ? { operationUrl: operation } : {}) } };
}

export async function dispatchFirstDirectPool(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  baselineReader = readCleanupBaseline): Promise<StoredCampaign> {
  const { state } = expected, original = state.directSetup;
  const plan = state.runInventory?.fic?.plan;
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  if (!original || original.firstPool !== null || state.status !== 'active' || !plan ||
      plan.aksId !== aksId || plan.identityId !== azure.c.identity_id)
    throw new Error('No unused original first-pool slot; do not replay a possibly dispatched setup operation.');
  const baseline = await baselineReader(azure, state);
  if (!etagPattern.test(baseline.etag) || baseline.campaign_id !== state.campaignId ||
      baseline.owner_id !== state.ownerId || baseline.fencing_token !== state.fencingToken)
    throw new Error('Original immutable idle baseline could not be bound.');
  const resourceId = `${aksId}/agentPools/arrowe192`;
  const { pool, properties, etag } = await readPool(azure, resourceId);
  if (properties.count !== 0 || properties.minCount !== 0 || Object.hasOwn(properties.tags ?? {}, ownershipTag))
    throw new Error('First setup pool is not the exact versioned idle 0/0/1 pool.');
  const requestId = randomUUID();
  const body = canonical_json(buildAksPoolScalePayload({ ...pool, properties: { ...properties,
    tags: { ...properties.tags, [ownershipTag]: requestId } } },
  { count: 0, autoscale: true, minimum: 1, maximum: 1 }, 'direct-first-floor'));
  const intent: DirectSetupPoolAttempt = { resourceId, expectedEtag: etag, bodySha256: hash(body), requestId, response: null };
  const originalPoolBodySha256 = hash(canonical_json(buildAksPoolScalePayload(pool,
    { count: 0, autoscale: true, minimum: 0, maximum: 1 }, 'direct-restore-floor')));
  await assertVersion(store, expected, true);
  const dispatch = await writeDirectSetupOnce(store, expected, { ...state,
    directSetup: { ...original, baselineEtag: baseline.etag, firstPool: intent,
      originalPoolBodySha256, originalPoolHasTags: Object.hasOwn(properties, 'tags') } });
  await assertVersion(store, dispatch, true);
  const submitted = await submit(azure, intent, body);
  return writeDirectSetupOnce(store, dispatch, { ...dispatch.state, directSetup: {
    ...dispatch.state.directSetup!, firstPool: submitted,
  } });
}

export async function inspectFirstDirectPool(store: CampaignStore, azure: Azure, expected: StoredCampaign, reversal = false):
Promise<'not_dispatched' | 'unknown' | 'original_operation_pending' | 'original_operation_succeeded' | 'original_operation_failed'> {
  const { state } = expected, original = state.directSetup;
  const plan = state.runInventory?.fic?.plan;
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  if (!original || !plan || plan.aksId !== aksId || plan.identityId !== azure.c.identity_id)
    throw new Error('Original direct setup does not match the selected stamp.');
  await assertVersion(store, expected);
  const pool = reversal ? original.reversal : original.firstPool;
  if (!pool) return 'not_dispatched';
  const operationUrl = pool.response?.operationUrl;
  if (!directPoolOperationUrl(operationUrl, pool.resourceId)) return 'unknown';
  const result = await reconcileArmOperation({ id: pool.requestId, resourceId: pool.resourceId,
    operationUrl, submittedAtMs: state.startedAtMs, status: 'pending' }, async url => {
    const [raw, headers] = await azure.request('GET', url, 'https://management.azure.com/', undefined, {}, true);
    const status = Number(headers[':status'] ?? 404);
    if (status !== 200 || !raw || raw.length > 1024 ** 2) return { status: status === 200 ? 422 : status };
    try { return { status, body: JSON.parse(raw.toString()) }; }
    catch { return { status: 422 }; }
  }, azure.c.subscription);
  await assertVersion(store, expected);
  if (result.status === 'succeeded') return 'original_operation_succeeded';
  if (result.status === 'failed') return 'original_operation_failed';
  return result.status === 'pending' ? 'original_operation_pending' : 'unknown';
}

export async function readSetupKubernetes(server: string, args: string[]): Promise<unknown> {
  const response = await runtime.run_tree(['kubectl', '--server', server, '--insecure-skip-tls-verify=false',
    '--request-timeout=15s', ...args, '--chunk-size=0', '-o', 'json'], 15);
  if (response.returncode !== 0) throw new Error('Setup Kubernetes read is unresolved.');
  return JSON.parse(response.stdout.trim() || 'null');
}

async function createSetupObject(server: string, path: string, body: string): Promise<unknown> {
  const response = await run_tree_with_input(['kubectl', '--server', server, '--insecure-skip-tls-verify=false',
    '--request-timeout=15s', 'create', '--raw', path, '-f', '-'], Buffer.from(body), 15, 65_536);
  if (response.returncode !== 0) throw new Error('Original setup Kubernetes CREATE outcome is unknown; never replay.');
  return JSON.parse(response.stdout);
}

async function removeSetupNamespace(azure: Azure, identity: RunNamespaceIdentity, path: string,
  authority: RunCleanupAuthority): Promise<void> {
  const journal = create_namespace_deletion_journal(azure, identity, authority);
  const receipt = await delete_owned_namespace(azure, identity, path, authority, journal, true);
  await journal.complete(receipt);
  await journal.stopped();
}

async function removeSetupFic(azure: Azure, fic: RunFicRecord, path: string, authority: RunCleanupAuthority): Promise<void> {
  await recover_run_fic_record(azure, fic, path, authority);
  await cleanup_run_fic(azure, fic.runId, path, authority);
  if (await observe_run_fic_cleanup(azure, fic, authority) !== 'deleted')
    throw new Error('Exact FIC deletion has not settled.');
}

export const setupNamespaceProviders = { plan: plan_run_fic, create: createSetupObject, prepare: prepare_run_fic,
  find: find_run_fic_record, removeNamespace: removeSetupNamespace, removeFic: removeSetupFic };

async function withPrivatePath<T>(work: (path: string) => Promise<T>): Promise<T> {
  const dir = mkdtempSync(join(tmpdir(), 'arrowcol-direct-setup-'));
  let result: T;
  try { result = await work(dir); }
  catch (error) {
    console.error(`Setup outcome unresolved; preserve private receipts at ${dir}.`);
    throw error;
  }
  for (const name of readdirSync(dir)) rmSync(join(dir, name), { force: true });
  rmdirSync(dir);
  return result;
}

const runLabel = 'arrowcol-oat/run-id', ownerLabel = 'arrowcol-oat/owner-token', attemptAnnotation = 'arrowcol-oat/setup-attempt';
function responseUid(value: unknown, kind: string, name: string, runId: string, owner: string, nonce: string,
  scope?: { namespace: string; clientId: string }): string {
  if (!value || typeof value !== 'object' || !('kind' in value) || value.kind !== kind ||
      !('metadata' in value) || !value.metadata || typeof value.metadata !== 'object')
    throw new Error('Original setup object response is unrecognized.');
  const metadata = value.metadata as { name?: string; uid?: string; namespace?: string; deletionTimestamp?: string;
    labels?: Record<string, string>; annotations?: Record<string, string> };
  if (metadata.name !== name || metadata.deletionTimestamp ||
      !metadata.uid || !/^[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/.test(metadata.uid) ||
      metadata.labels?.[runLabel] !== runId || metadata.labels?.[ownerLabel] !== owner ||
      metadata.annotations?.[runLabel] !== runId || metadata.annotations?.[ownerLabel] !== owner ||
      metadata.annotations?.[attemptAnnotation] !== nonce ||
      (scope && (metadata.namespace !== scope.namespace ||
        metadata.annotations?.['azure.workload.identity/client-id'] !== scope.clientId)))
    throw new Error('Original setup object identity or attempt changed.');
  return metadata.uid;
}

export async function provisionSetupNamespace(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  injected?: Partial<typeof setupNamespaceProviders>, read = readSetupKubernetes): Promise<string> {
  const provider = { ...setupNamespaceProviders, ...injected }, { state } = expected, original = state.directSetup;
  const plan = state.runInventory?.fic?.plan;
  if (!original || !plan || original.namespaceAttempt || original.reversal || original.namespaceCleanup ||
      await inspectFirstDirectPool(store, azure, expected) !== 'original_operation_succeeded')
    throw new Error('No unused original namespace slot with a settled pool. Never replay a provisioning attempt.');
  await readOwnedFloor(azure, original.firstPool!);
  if (canonical_json(await provider.plan(azure, plan.runId, plan.namespace, plan.serviceAccount)) !== canonical_json(plan) ||
      await read(plan.server, ['get', 'namespace', plan.namespace, '--ignore-not-found']) !== null ||
      await provider.find(azure, plan, { assertCurrent: () => assertVersion(store, expected, true) }))
    throw new Error('Original setup namespace and FIC must be initially absent.');
  let version = expected;
  const persist = async (change: Partial<NonNullable<StoredCampaign['state']['directSetup']>>) => {
    version = await writeDirectSetupOnce(store, version, { ...version.state, directSetup: { ...version.state.directSetup!, ...change } });
  };
  const scope = { namespace: plan.namespace, clientId: plan.clientId };
  async function create(slot: 'namespaceAttempt' | 'accountAttempt', kind: 'Namespace' | 'ServiceAccount',
    name: string, endpoint: string, account?: typeof scope): Promise<string> {
    const nonce = randomUUID(), identity = { [runLabel]: plan!.runId, [ownerLabel]: state.ownerId };
    const body = canonical_json({ apiVersion: 'v1', kind, metadata: { name, labels: identity,
      ...(account ? { namespace: account.namespace, ownerReferences: [{ apiVersion: 'v1', kind: 'Namespace',
        name: account.namespace, uid: version.state.directSetup!.namespaceAttempt!.uid }] } : {}), annotations: { ...identity, [attemptAnnotation]: nonce,
        ...(account ? { 'azure.workload.identity/client-id': account.clientId } : {}) } } });
    const attempt: DirectSetupObjectAttempt = { nonce, bodySha256: hash(body), uid: null };
    await assertVersion(store, version, true);
    await persist({ [slot]: attempt });
    await assertVersion(store, version, true);
    const uid = responseUid(await provider.create(plan!.server, endpoint, body), kind, name, plan!.runId, state.ownerId, nonce, account);
    await persist({ [slot]: { ...attempt, uid } });
    return uid;
  }
  const namespaceUid = await create('namespaceAttempt', 'Namespace', plan.namespace, '/api/v1/namespaces');
  if (responseUid(await read(plan.server, ['get', 'namespace', plan.namespace]), 'Namespace', plan.namespace,
      plan.runId, state.ownerId, version.state.directSetup!.namespaceAttempt!.nonce) !== namespaceUid)
    throw new Error('Original namespace UID changed before ServiceAccount dispatch.');
  const accountUid = await create('accountAttempt', 'ServiceAccount', plan.serviceAccount,
    `/api/v1/namespaces/${plan.namespace}/serviceaccounts`, scope);
  await persist({ ficAttempt: true });
  const assertCurrent = () => assertVersion(store, version, true);
  const assertFicIdentity = (record: RunFicRecord) => {
    if (record.namespaceUid !== namespaceUid || record.namespaceOwner !== state.ownerId || record.serviceAccountUid !== accountUid)
      throw new Error('Original namespace or ServiceAccount UID changed before FIC binding.');
  };
  return withPrivatePath(async dir => {
    await assertCurrent();
    await provider.prepare(azure, plan.runId, plan.namespace, plan.serviceAccount, join(dir, 'fic.json'), plan,
      async record => { assertFicIdentity(record); await assertCurrent(); });
    const record = await provider.find(azure, plan, { assertCurrent });
    if (!record || record.phase !== 'ready') throw new Error('Original definitive FIC creation is not available.');
    assertFicIdentity(record);
    await assertCurrent();
    return 'namespace_fic_owned_not_admitted';
  });
}

export async function cancelSetupNamespace(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  injected?: Partial<typeof setupNamespaceProviders>, read = readSetupKubernetes): Promise<StoredCampaign> {
  const provider = { ...setupNamespaceProviders, ...injected }, { state } = expected, setup = state.directSetup;
  const inv = state.runInventory, plan = inv?.fic?.plan, attempt = setup?.namespaceAttempt;
  if (!attempt) return expected;
  if (!plan || !setup || setup.heldStaging || state.status !== 'active' || state.mode !== 'observe' || inv?.load_admission !== 'not_authorized' ||
      !attempt.uid || (setup.accountAttempt && !setup.accountAttempt.uid))
    throw new Error('Original namespace or ServiceAccount creation is unresolved; retain custody.');
  if (setup.namespaceCleanup === 'complete') return expected;
  if (await inspectFirstDirectPool(store, azure, expected) !== 'original_operation_succeeded')
    throw new Error('Original pool outcome is unresolved; retain setup custody.');
  if (canonical_json(await provider.plan(azure, plan.runId, plan.namespace, plan.serviceAccount)) !== canonical_json(plan))
    throw new Error('Original FIC plan changed.');
  let current = expected;
  const authority = { assertCurrent: () => assertVersion(store, current) };
  const fic = await provider.find(azure, plan, authority);
  if (setup.ficAttempt ? !fic || fic.namespaceUid !== attempt.uid || fic.namespaceOwner !== state.ownerId ||
      fic.serviceAccountUid !== setup.accountAttempt?.uid || fic.phase !== 'ready' &&
      !(fic.phase === 'deleted' && setup.namespaceCleanup === 'started') : fic !== undefined)
    throw new Error('Original FIC attempt has no matching settled ledger proof; never replay or adopt.');
  const assertUnused = async () => {
    const check = [
      ['pods', 'PodList'], ['persistentvolumeclaims', 'PersistentVolumeClaimList'],
      ['deployments', 'DeploymentList'], ['replicasets', 'ReplicaSetList'], ['statefulsets', 'StatefulSetList'],
      ['daemonsets', 'DaemonSetList'], ['jobs', 'JobList'], ['cronjobs', 'CronJobList'],
      ['replicationcontrollers', 'ReplicationControllerList'],
    ];
    for (const [resource, kind] of check)
      if (list(await read(plan.server, ['get', resource, '-n', plan.namespace]), kind).length)
        throw new Error('Namespace has workloads or storage; use existing sealed held-arm recovery, not setup cancellation.');
    if (list(await read(plan.server, ['get', 'persistentvolumes']), 'PersistentVolumeList')
      .some(volume => volume.spec?.claimRef?.namespace === plan.namespace))
      throw new Error('Original namespace has persistent storage; setup cancellation will not remove it.');
  };
  if (setup.namespaceCleanup !== 'started') {
    if (responseUid(await read(plan.server, ['get', 'namespace', plan.namespace]), 'Namespace', plan.namespace,
        plan.runId, state.ownerId, attempt.nonce) !== attempt.uid)
      throw new Error('Original namespace UID changed.');
    await assertUnused();
    current = await writeDirectSetupOnce(store, current, { ...current.state, directSetup: { ...setup, namespaceCleanup: 'started' } });
  }
  if (await read(plan.server, ['get', 'namespace', plan.namespace, '--ignore-not-found']) !== null) await assertUnused();
  const identity: RunNamespaceIdentity = { version: 1, runId: plan.runId, namespace: plan.namespace,
    namespaceUid: attempt.uid, namespaceOwner: state.ownerId, aksId: plan.aksId, server: plan.server };
  await withPrivatePath(async dir => {
    await provider.removeNamespace(azure, identity, join(dir, 'namespace.json'), authority);
    if (fic) await provider.removeFic(azure, fic, join(dir, 'fic.json'), authority);
  });
  await authority.assertCurrent();
  return writeDirectSetupOnce(store, current, { ...current.state, directSetup: { ...setup, namespaceCleanup: 'complete' } });
}

export async function resolveSetupFic(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  injected?: Partial<typeof setupNamespaceProviders>, read = readSetupKubernetes): Promise<RunFicRecord> {
  const provider = { ...setupNamespaceProviders, ...injected }, { state } = expected, setup = state.directSetup;
  const plan = state.runInventory?.fic?.plan, namespace = setup?.namespaceAttempt, account = setup?.accountAttempt;
  if (!plan || !namespace?.uid || !account?.uid || !setup?.ficAttempt || setup.namespaceCleanup || setup.reversal)
    throw new Error('Original namespace and FIC attempts are not available for held setup.');
  const assertCurrent = () => assertVersion(store, expected, true);
  await assertCurrent();
  const scope = { namespace: plan.namespace, clientId: plan.clientId };
  if (responseUid(await read(plan.server, ['get', 'namespace', plan.namespace]), 'Namespace', plan.namespace,
      plan.runId, state.ownerId, namespace.nonce) !== namespace.uid ||
      responseUid(await read(plan.server, ['get', 'serviceaccount', plan.serviceAccount, '-n', plan.namespace]),
        'ServiceAccount', plan.serviceAccount, plan.runId, state.ownerId, account.nonce, scope) !== account.uid)
    throw new Error('Held setup Kubernetes identity changed.');
  const fic = await provider.find(azure, plan, { assertCurrent });
  if (!fic || fic.phase !== 'ready' || fic.namespaceUid !== namespace.uid || fic.namespaceOwner !== state.ownerId ||
      fic.serviceAccountUid !== account.uid)
    throw new Error('No matching definitive original FIC ownership.');
  await assertCurrent();
  return fic;
}

export async function stageHeldSetup(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  injected?: Partial<typeof setupNamespaceProviders>, read = readSetupKubernetes): Promise<void> {
  const { state } = expected;
  if (!state.directSetup || state.directSetup.heldStaging || state.mode !== 'observe' ||
      await inspectFirstDirectPool(store, azure, expected) !== 'original_operation_succeeded')
    throw new Error('Held workload staging is already pinned or the original pool is unsettled.');
  await readOwnedFloor(azure, state.directSetup.firstPool!);
  await resolveSetupFic(store, azure, expected, injected, read);
  await writeDirectSetupOnce(store, expected, { ...state, directSetup: { ...state.directSetup, heldStaging: true } });
}

export async function verifySetupHeldInventory(store: CampaignStore, azure: Azure, expected: StoredCampaign, inv: RunInventory,
  injected?: Partial<typeof setupNamespaceProviders>, read = readSetupKubernetes): Promise<void> {
  const fic = await resolveSetupFic(store, azure, expected, injected, read);
  for (const resource of inv.resources) {
    const raw = await read(fic.server, ['get', resource.kind, resource.name, '-n', fic.namespace]);
    if (!raw || typeof raw !== 'object' || !('metadata' in raw) || !raw.metadata || typeof raw.metadata !== 'object')
      throw new Error('An original held workload has no positive Kubernetes readback.');
    const meta = raw.metadata as { name?: string; namespace?: string; uid?: string; deletionTimestamp?: string;
      labels?: Record<string, string>; annotations?: Record<string, string> };
    if (!('apiVersion' in raw) || raw.apiVersion !== resource.api_version || !('kind' in raw) || raw.kind !== resource.kind ||
        meta.name !== resource.name || meta.namespace !== fic.namespace || meta.uid !== resource.uid || meta.deletionTimestamp ||
        ![meta.labels, meta.annotations].every(markers => markers?.[runLabel] === fic.runId && markers?.[ownerLabel] === fic.namespaceOwner))
      throw new Error('Original held workload UID or ownership changed.');
  }
  await assertVersion(store, expected, true);
}

function list(value: unknown, kind: string): Record<string, any>[] {
  if (!value || typeof value !== 'object' || !('kind' in value) || (value.kind !== kind && value.kind !== 'List') ||
      !('metadata' in value) || !value.metadata || typeof value.metadata !== 'object' ||
      'continue' in value.metadata && value.metadata.continue || !('items' in value) || !Array.isArray(value.items) ||
      value.items.some(item => !item?.metadata || typeof item.metadata.name !== 'string' ||
        value.kind === 'List' && item.kind !== kind.slice(0, -4)))
    throw new Error('Setup Kubernetes inventory is incomplete.');
  return value.items;
}

async function assertPoolUnused(server: string, namespace: string, read: typeof readSetupKubernetes, physicallyIdle = false) {
  if (await read(server, ['get', 'namespace', namespace, '--ignore-not-found']) !== null)
    throw new Error('Original setup namespace must be positively absent before restoring the floor.');
  const nodes = list(await read(server, ['get', 'nodes']), 'NodeList');
  const poolName = (node: Record<string, any>) => {
    const labels = node.metadata.labels, primary = labels?.['kubernetes.azure.com/agentpool'], legacy = labels?.agentpool;
    return typeof primary === 'string' && primary && (!legacy || legacy === primary) ? primary : undefined;
  };
  if (nodes.some(node => !poolName(node)))
    throw new Error('Pool identity for a Kubernetes node is unknown.');
  const names = new Set(nodes.filter(node => poolName(node) === 'arrowe192')
    .map(node => node.metadata.name));
  if (physicallyIdle && names.size)
    throw new Error('Physical setup pool nodes still exist.');
  const pods = list(await read(server, ['get', 'pods', '--all-namespaces']), 'PodList');
  const volumes = list(await read(server, ['get', 'volumeattachments']), 'VolumeAttachmentList');
  const persistent = list(await read(server, ['get', 'persistentvolumes']), 'PersistentVolumeList');
  if (pods.some(pod => pod.metadata.namespace === namespace || names.has(pod.spec?.nodeName) &&
      !(pod.metadata.namespace === 'kube-system' && pod.metadata.ownerReferences?.some((ref: {kind: string; controller?: boolean}) =>
        ref.kind === 'DaemonSet' && ref.controller === true) && Array.isArray(pod.spec.volumes) &&
        pod.spec.volumes.every((volume: object) => !('persistentVolumeClaim' in volume) && !('csi' in volume) && !('ephemeral' in volume)))) ||
      volumes.some(volume => !volume.spec?.nodeName || !nodes.some(node => node.metadata.name === volume.spec.nodeName) ||
        names.has(volume.spec.nodeName)) ||
      persistent.some(volume => volume.spec?.claimRef?.namespace === namespace))
    throw new Error('Setup pool still has workloads or storage attachments.');
}

async function readOwnedFloor(azure: Azure, first: DirectSetupPoolAttempt) {
  const loaded = await readPool(azure, first.resourceId), { pool, properties, etag } = loaded;
  if (etag === first.expectedEtag || properties.minCount !== 1 || ![0, 1].includes(properties.count) ||
      properties.tags?.[ownershipTag] !== first.requestId || hash(canonical_json(buildAksPoolScalePayload(pool,
        { count: 0, autoscale: true, minimum: 1, maximum: 1 }, 'direct-first-floor'))) !== first.bodySha256)
    throw new Error('Original scoped pool ownership or configuration drifted.');
  return loaded;
}

export async function verifyHeldSetupSettlement(store: CampaignStore, azure: Azure, current: StoredCampaign,
  context: string, releasePath: string): Promise<void> {
  const { readRelease } = await import('../../infra/azure/benchmark-direct-cli.js');
  const { inspectDirectRecovery } = await import('../../infra/azure/benchmark-direct-recovery.js');
  const { recoverInterruptedDirectRun } = await import('../../infra/azure/benchmark-interrupted-custody.js');
  const signal = AbortSignal.timeout(180_000), release = readRelease(releasePath);
  await withPrivatePath(async directory => {
    const binding = await inspectDirectRecovery(azure, store, current, release, signal, directory, true);
    if (binding.arm.scheduleDisposition !== 'held' || !binding.arm.heldBarrier)
      throw new Error('Original held barrier proof is unavailable.');
    await recoverInterruptedDirectRun(azure, binding, context, directory, signal, true);
  });
}

export async function settleFirstDirectPool(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  baselineReader = readCleanupBaseline, read = readSetupKubernetes, verifyHeld?: () => Promise<void>): Promise<string> {
  const { state } = expected, original = state.directSetup;
  const first = original?.firstPool, inv = state.runInventory, plan = inv?.fic?.plan;
  if (!original || !plan || !first || !original.baselineEtag ||
      (verifyHeld ? state.status !== 'fenced' || state.mode !== 'armed' || !original.heldStaging :
        state.status !== 'active' || state.mode !== 'observe' || original.heldStaging))
    throw new Error('No bounded original setup floor to settle.');
  const baseline = await baselineReader(azure, state, original.baselineEtag);
  if (baseline.etag !== original.baselineEtag || baseline.campaign_id !== state.campaignId ||
      baseline.owner_id !== state.ownerId || baseline.fencing_token !== inv?.fencing_token)
    throw new Error('Original immutable idle baseline could not be bound.');
  if (await inspectFirstDirectPool(store, azure, expected) !== 'original_operation_succeeded')
    throw new Error('Original pool outcome is not positively settled as succeeded; retain custody.');
  await verifyHeld?.();
  if (original.reversal) {
    if (await inspectFirstDirectPool(store, azure, expected, true) !== 'original_operation_succeeded')
      throw new Error('Original reversal outcome is unresolved; never redispatch.');
    const { pool, etag, properties } = await readPool(azure, first.resourceId);
    if (properties.count !== 0 || properties.minCount !== 0 || etag === original.reversal.expectedEtag ||
        Object.hasOwn(properties.tags ?? {}, ownershipTag) || hash(canonical_json(buildAksPoolScalePayload(pool,
          { count: 0, autoscale: true, minimum: 0, maximum: 1 }, 'direct-restore-floor'))) !== original.reversal.bodySha256)
      throw new Error('Original reversal has not read back the exact idle configuration.');
    await assertPoolUnused(plan.server, plan.namespace, read, true);
    await writeDirectSetupOnce(store, expected, { ...state, status: 'setup_retired', directSetup: { ...original, reversalVerified: true } });
    return 'setup_retired';
  }
  const { pool, etag, properties } = await readOwnedFloor(azure, first);
  await assertPoolUnused(plan.server, plan.namespace, read);
  const { [ownershipTag]: _owned, ...tags } = properties.tags;
  const restored = { ...properties };
  delete restored.tags;
  if (original.originalPoolHasTags) restored.tags = tags;
  const body = canonical_json(buildAksPoolScalePayload({ ...pool, properties: restored },
    { count: 0, autoscale: true, minimum: 0, maximum: 1 }, 'direct-restore-floor'));
  if (hash(body) !== original.originalPoolBodySha256) throw new Error('Reversal would not restore the immutable original pool configuration.');
  const reversal: DirectSetupPoolAttempt = { resourceId: first.resourceId, expectedEtag: etag, bodySha256: hash(body),
    requestId: randomUUID(), response: null };
  const dispatch = await writeDirectSetupOnce(store, expected, { ...state, directSetup: { ...original, reversal } });
  await assertPoolUnused(plan.server, plan.namespace, read);
  await assertVersion(store, dispatch);
  const submitted = await submit(azure, reversal, body);
  await writeDirectSetupOnce(store, dispatch, { ...dispatch.state, directSetup: { ...original, reversal: submitted } });
  return 'reversal_acknowledged_not_verified';
}
