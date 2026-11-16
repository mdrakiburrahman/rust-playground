import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, armCampaign, startCampaign, type CampaignState, type CampaignStore, type StoredCampaign } from './aks-campaign.js';
import { watchdogOnce } from './aks-watchdog.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = 'c0ffee00-0000-4000-8000-000000000003';

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  async read() { return this.current ? structuredClone(this.current) : undefined; }
  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = `etag-${++this.revision}`;
    this.current = { state: structuredClone(state), etag: next };
    return next;
  }
}

async function armedStore(): Promise<MemoryStore> {
  const store = new MemoryStore();
  await startCampaign(store, campaignId, ownerId, 1_000_000);
  await armCampaign(store, ownerId, 1, 1_000_000);
  return store;
}

test('watchdog expiry fences first, refuses missing cleanup, and resumes idempotent cleanup', async () => {
  const env = { ARM_SUBSCRIPTION_ID: subscriptionId };
  const blockedStore = await armedStore();
  await assert.rejects(watchdogOnce(env, undefined, blockedStore), /external cleanup executor/);
  assert.equal((await blockedStore.read())!.state.status, 'fenced');

  const store = await armedStore();
  const executed: string[] = [];
  const execute = async (step: typeof CUSTODY_CLEANUP_STEPS[number]) => {
    executed.push(step);
    return 'complete' as const;
  };
  let evaluation = await watchdogOnce(env, execute, store);
  while ((await store.read())!.state.status !== 'complete') evaluation = await watchdogOnce(env, execute, store);
  assert.equal(evaluation.cleanupProgress, 'complete');
  assert.deepEqual(executed, CUSTODY_CLEANUP_STEPS);
  assert.equal((await store.read())!.state.cleanupStatus, 'verified');
});
