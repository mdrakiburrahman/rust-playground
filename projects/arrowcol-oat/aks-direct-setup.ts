import { createHash, randomUUID } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, HEARTBEAT_INTERVAL_MS, MISSED_HEARTBEAT_LIMIT, writeDirectSetupOnce,
  type CampaignStore, type DirectSetup, type StoredCampaign } from './aks-campaign.js';
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
  return writeDirectSetupOnce(store, dispatch, { ...dispatch.state, directSetup: {
    ...original, baselineEtag: baseline.etag, firstPool: { ...intent, response: { status, requestId } },
  } });
}
