import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, armCampaign, publishRunInventory, startCampaign, type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { watchdogOnce } from './aks-watchdog.js';
import { createExternalCleanupExecutor, type ExternalCleanupActions } from './aks-cleanup-executor.js';
import { reconcileCleanupOnce, reconcileOnce } from './aks-reconcile.js';

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

const inventoryFor = (state: CampaignState): RunInventory => {
  const cellRunId = 'c0ffee00-0000-4000-8000-000000000004';
  const namespace = 'arrowcol-' + cellRunId.replace(/-/g, '');
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  return {
    schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: state.fencingToken,
    orchestration_run_id: 'c0ffee00-0000-4000-8000-000000000005', cell_run_id: cellRunId,
    namespace: { name: namespace, uid: 'c0ffee00-0000-4000-8000-000000000009', owner_token: ownerId },
    resources: [{ api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'arrowcol',
      uid: 'c0ffee00-0000-4000-8000-000000000006', absence: null, role: 'collector', state: 'observed' }],
    fic: { resource_id: '/subscriptions/' + subscriptionId + '/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector/federatedIdentityCredentials/arrowcol-run-' + cellRunId,
      name: 'arrowcol-run-' + cellRunId, state: 'observed', absence: null },
    qap: { database: 'perf69-qap', bindings: [{ external_table: 'arrowcol_data', relative_path: 'perf69/' + cellRunId }], state: 'observed', absence: [] },
    cleanup: { stop_admission: { ...step }, bounded_drain: { ...step }, evidence_capture: { ...step }, terminal_capture: { ...step }, handoff: { ...step } },
  };
};
async function armedStore(): Promise<MemoryStore> {
  const store = new MemoryStore();
  await startCampaign(store, campaignId, ownerId, 1_000_000);
  const state = (await store.read())!.state;
  await publishRunInventory(store, inventoryFor(state), state.startedAtMs);
  await armCampaign(store, ownerId, state.fencingToken, state.startedAtMs);
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


test('external executor invokes safe-idle restoration exactly once under the cleanup fence', async () => {
  const store = await armedStore();
  await reconcileOnce(store, 1_000_000 + 3 * 60_000);
  for (const _step of CUSTODY_CLEANUP_STEPS.slice(0, CUSTODY_CLEANUP_STEPS.indexOf('restore-safe-idle'))) {
    await reconcileCleanupOnce(store, async () => 'complete');
  }
  let restoreCalls = 0;
  const complete = async () => 'complete' as const;
  const actions: ExternalCleanupActions = {
    discoverExactInventory: async inventory => inventory,
    stopAdmission: complete,
    boundedDrain: complete,
    preserveEvidence: complete,
    stopMaintenanceIfUidAndOwnerMatch: complete,
    captureTerminalState: complete,
    deleteNamespaceIfUidAndOwnerMatch: complete,
    deleteQapBindingIfOwned: complete,
    deleteFicIfExact: complete,
    restoreSafeIdle: async (_state, _key, authority) => { await authority.assertCurrent(); restoreCalls++; return 'complete'; },
    verifySafeIdle: complete,
  };
  const result = await reconcileCleanupOnce(store, createExternalCleanupExecutor(store, actions));
  assert.equal(result, 'progress');
  assert.equal(restoreCalls, 1);
});
