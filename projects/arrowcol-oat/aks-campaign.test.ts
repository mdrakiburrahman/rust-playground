import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CampaignConflictError, MAX_CAMPAIGN_LIFETIME_MS, MISSED_HEARTBEAT_LIMIT, HEARTBEAT_INTERVAL_MS,
  armCampaign, heartbeatCampaign, mutateCampaign, newCampaignState, startCampaign, validateCampaignState,
  type CampaignState, type CampaignStore, type StoredCampaign, recordArmOperation } from './aks-campaign.js';
import { evaluateCustody, reconcileArmOperation, reconcileOnce, reconcilePendingArmOperations } from './aks-reconcile.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = 'c0ffee00-0000-4000-8000-000000000003';
const resourceId = `/subscriptions/${subscriptionId}/resourceGroups/perf69/providers/Microsoft.ContainerService/managedClusters/bench`;
const operationUrl = `https://management.azure.com/subscriptions/${subscriptionId}/providers/Microsoft.ContainerService/locations/westus/operations/123?api-version=2024-10-01`;

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  async read() { return this.current ? structuredClone(this.current) : undefined; }
  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = `etag-${++this.revision}`;
    this.current = { state: structuredClone(validateCampaignState(state)), etag: next };
    return next;
  }
}

const seed = async (now = 1_000_000) => {
  const store = new MemoryStore();
  await startCampaign(store, campaignId, ownerId, now);
  return store;
};

test('new campaign is observe-only with fixed heartbeat and lifetime bounds', () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  assert.equal(state.mode, 'observe');
  assert.equal(state.expiresAtMs, 100 + MAX_CAMPAIGN_LIFETIME_MS);
  assert.equal(HEARTBEAT_INTERVAL_MS, 60_000);
  assert.equal(MISSED_HEARTBEAT_LIMIT, 3);
});

test('heartbeat cannot revive expired custody and stale fencing tokens are rejected', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const beat = await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 1);
  assert.equal(beat.state.heartbeatSequence, 1);
  assert.equal(beat.state.expiresAtMs, started.expiresAtMs);
  await assert.rejects(heartbeatCampaign(store, ownerId, started.fencingToken, started.expiresAtMs), /maximum lifetime/);
});

test('observer records a would-fence event without fencing; armed controller fences once at three missed beats', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const expiredAt = started.heartbeatAtMs + 3 * HEARTBEAT_INTERVAL_MS;
  assert.equal(evaluateCustody(started, expiredAt).wouldFence, true);
  const observed = await reconcileOnce(store, expiredAt);
  assert.equal(observed.status, 'observed');
  assert.equal((await store.read())!.state.status, 'active');
  await armCampaign(store, ownerId, started.fencingToken);
  const fenced = await reconcileOnce(store, expiredAt);
  assert.equal(fenced.status, 'fenced');
  const state = (await store.read())!.state;
  assert.equal(state.status, 'fenced');
  assert.equal(state.fencingToken, started.fencingToken + 1);
  assert.equal(state.cleanupStatus, 'pending');
  assert.equal((await reconcileOnce(store, expiredAt + HEARTBEAT_INTERVAL_MS)).status, 'terminal');
  assert.equal((await store.read())!.state.fencingToken, started.fencingToken + 1);
});

test('max lifetime fences even when heartbeat is current', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  await armCampaign(store, ownerId, started.fencingToken);
  await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 10);
  const expired = (await store.read())!.state;
  const result = await reconcileOnce(store, expired.expiresAtMs);
  assert.equal(result.fenceReason, 'max-lifetime');
});

test('campaign ownership cannot be replaced until cleanup is verified', async () => {
  const store = await seed();
  await assert.rejects(startCampaign(store, 'c0ffee00-0000-4000-8000-000000000004', ownerId, 1_000_100), /unfinished campaign/);
});

test('ARM operation receipts reject out-of-scope URLs and reconcile terminal public status values', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  await assert.rejects(recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-000000000005', resourceId,
      operationUrl: 'https://example.com/operation', submittedAtMs: state.startedAtMs }, subscriptionId), /management.azure.com/);
  await recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-000000000006', resourceId, operationUrl, submittedAtMs: state.startedAtMs }, subscriptionId);
  const pending = (await store.read())!.state.operations[0]!;
  const completed = await reconcileArmOperation(pending, async url => {
    assert.equal(url, operationUrl);
    return { status: 200, body: { status: 'Succeeded' } };
  }, subscriptionId, state.startedAtMs + 1);
  assert.equal(completed.status, 'succeeded');
  const reconciled = await reconcilePendingArmOperations(store, async () => ({ status: 202 }), subscriptionId, state.startedAtMs + 1);
  assert.equal(reconciled!.operations[0]!.status, 'pending');
  const hostile = { ...pending, operationUrl: 'https://127.0.0.1/latest/meta-data' };
  await assert.rejects(reconcileArmOperation(hostile, async () => ({ status: 200 }), subscriptionId, state.startedAtMs + 1), /outside the configured subscription/);
});

test('strict state schema rejects additional persisted fields', async () => {
  const state = newCampaignState(campaignId, ownerId, 1);
  await assert.rejects(async () => validateCampaignState({ ...state, unexpected: true }), /unknown fields/);
  const store = new MemoryStore();
  await mutateCampaign(store, () => state);
  assert.equal((await store.read())!.state.campaignId, campaignId);
});
