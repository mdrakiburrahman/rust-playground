import { createHash, randomUUID } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, directPoolOperationUrl, HEARTBEAT_INTERVAL_MS, MISSED_HEARTBEAT_LIMIT, writeDirectSetupOnce,
  type CampaignStore, type DirectSetup, type StoredCampaign } from './aks-campaign.js';
import { reconcileArmOperation } from './aks-reconcile.js';
import { buildAksPoolScalePayload } from './aks-arm-pool.js';
import { readCleanupBaseline } from './aks-default-providers.js';

const etagPattern = /^"[^"\r\n]+"$/;

export async function dispatchFirstDirectPool(store: CampaignStore, azure: Azure, expected: StoredCampaign,
  baselineReader = readCleanupBaseline): Promise<StoredCampaign> {
  const { state } = expected, original = state.directSetup;
  const plan = state.runInventory?.fic?.plan;
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  if (!original || original.firstPool !== null || state.status !== 'active' || !plan ||
      plan.aksId !== aksId || plan.identityId !== azure.c.identity_id)
    throw new Error('No unused original first-pool slot; do not replay a possibly dispatched setup operation.');
  const assertCurrent = async (version: StoredCampaign) => {
    const current = await store.read(), now = Date.now(), pinned = version.state;
    if (current?.etag !== version.etag || canonical_json(current.state) !== canonical_json(pinned) ||
        pinned.status !== 'active' || now < pinned.heartbeatAtMs || now >= pinned.expiresAtMs ||
        now - pinned.heartbeatAtMs >= HEARTBEAT_INTERVAL_MS * MISSED_HEARTBEAT_LIMIT) throw new CampaignConflictError();
  };
  const baseline = await baselineReader(azure, state);
  if (!etagPattern.test(baseline.etag) || baseline.campaign_id !== state.campaignId ||
      baseline.owner_id !== state.ownerId || baseline.fencing_token !== state.fencingToken)
    throw new Error('Original immutable idle baseline could not be bound.');
  const resourceId = `${aksId}/agentPools/arrowe192`;
  const url = `https://management.azure.com${resourceId}?api-version=2024-10-01`;
  const [raw, headers] = await azure.request('GET', url, 'https://management.azure.com/');
  const pool = JSON.parse(raw?.toString() ?? 'null'), poolEtag = headers.etag;
  const properties = pool?.properties;
  if (headers[':status'] !== '200' || !poolEtag || !etagPattern.test(poolEtag) ||
      typeof pool?.id !== 'string' || pool.id.toLowerCase() !== resourceId.toLowerCase() ||
      properties?.provisioningState !== 'Succeeded' || properties?.mode !== 'User' || properties.vmSize !== 'Standard_E192ids_v6' ||
      properties?.count !== 0 || properties.enableAutoScaling !== true || properties.minCount !== 0 || properties.maxCount !== 1)
    throw new Error('First setup pool is not the exact versioned idle 0/0/1 pool.');
  const body = canonical_json(buildAksPoolScalePayload(pool, { count: 0, autoscale: true, minimum: 1, maximum: 1 }, 'direct-first-floor'));
  const intent: NonNullable<DirectSetup['firstPool']> = {
    resourceId, expectedEtag: poolEtag, bodySha256: createHash('sha256').update(body).digest('hex'),
    requestId: randomUUID(), response: null,
  };
  await assertCurrent(expected);
  const dispatch = await writeDirectSetupOnce(store, expected, { ...state,
    directSetup: { ...original, baselineEtag: baseline.etag, firstPool: intent } });
  await assertCurrent(dispatch);
  const [, ack] = await azure.request('PUT', url, 'https://management.azure.com/', body,
    { 'Content-Type': 'application/json', 'If-Match': poolEtag, 'x-ms-client-request-id': intent.requestId }, false, false);
  const status = Number(ack[':status']), requestId = ack['x-ms-request-id'];
  if (!(status === 200 || status === 201 || status === 202) || !requestId)
    throw new Error('Setup pool acknowledgement is unresolved; retain dispatch custody.');
  const operation = ack['azure-asyncoperation'];
  return writeDirectSetupOnce(store, dispatch, { ...dispatch.state, directSetup: {
    ...original, baselineEtag: baseline.etag, firstPool: { ...intent, response: { status, requestId,
      ...(directPoolOperationUrl(operation, resourceId) ? { operationUrl: operation } : {}) } },
  } });
}

export async function inspectFirstDirectPool(store: CampaignStore, azure: Azure, expected: StoredCampaign):
Promise<'not_dispatched' | 'unknown' | 'original_operation_pending' | 'original_operation_succeeded' | 'original_operation_failed'> {
  const { state } = expected, original = state.directSetup;
  const plan = state.runInventory?.fic?.plan;
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  if (!original || !plan || plan.aksId !== aksId || plan.identityId !== azure.c.identity_id)
    throw new Error('Original direct setup does not match the selected stamp.');
  const assertCurrent = async () => {
    const current = await store.read();
    if (current?.etag !== expected.etag || canonical_json(current.state) !== canonical_json(state))
      throw new CampaignConflictError();
  };
  await assertCurrent();
  const pool = original.firstPool;
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
  await assertCurrent();
  if (result.status === 'succeeded') return 'original_operation_succeeded';
  if (result.status === 'failed') return 'original_operation_failed';
  return result.status === 'pending' ? 'original_operation_pending' : 'unknown';
}
