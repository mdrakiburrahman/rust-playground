import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, MAX_CAMPAIGN_LIFETIME_MS, KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS, assessCellLifetimeBudget,
  MISSED_HEARTBEAT_LIMIT, HEARTBEAT_INTERVAL_MS,
  createBlobCampaignStore,
  armCampaign, assertCampaignEnvironment, assertRunInventoryReady, campaignStartReceipt, heartbeatCampaign, mutateCampaign, newCampaignState, publishCleanupRunInventory, publishRunInventory, runInventoryPublishReceipt, startCampaign,
  validateCampaignState, type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign,
  CampaignWriteOutcomeUnknownError, recordArmOperation } from './aks-campaign.js';
import { evaluateCustody, reconcileArmOperation, reconcileCleanupOnce, reconcileOnce, reconcilePendingArmOperations } from './aks-reconcile.js';
import { newScaleTrialReceipt, SCALE_RESTORE_TIMEOUT_MS, type ScaleStep } from './aks-scale.js';

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

const inventoryFor = (state: CampaignState, observed = false): RunInventory => {
  const cellRunId = 'c0ffee00-0000-4000-8000-000000000004';
  const orchestrationRunId = 'c0ffee00-0000-4000-8000-000000000005';
  const namespace = 'arrowcol-' + cellRunId.replace(/-/g, '');
  const names = ['arrowcol', 'arrowcol-client', 'delta-maintenance'];
  const uids = ['c0ffee00-0000-4000-8000-000000000006', 'c0ffee00-0000-4000-8000-000000000007',
    'c0ffee00-0000-4000-8000-000000000008'];
  const stateName = observed ? 'observed' : 'planned';
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  return {
    schema_version: 1, campaign_id: state.campaignId, owner_id: state.ownerId, fencing_token: state.runInventory?.fencing_token ?? state.fencingToken,
    orchestration_run_id: orchestrationRunId, cell_run_id: cellRunId, load_admission: state.runInventory?.load_admission ?? 'not_authorized',
    planned_cell_duration_ms: 60_000, effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    namespace: { name: namespace, uid: observed ? 'c0ffee00-0000-4000-8000-000000000009' : null, owner_token: state.ownerId },
    resources: names.map((name, index) => ({ api_version: 'apps/v1', kind: 'Deployment', namespace, name,
      uid: observed ? uids[index]! : null, absence: null, role: index === 0 ? 'collector' : index === 1 ? 'client' : 'maintenance', state: stateName })),
    fic: { resource_id: '/subscriptions/' + subscriptionId + '/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector/federatedIdentityCredentials/arrowcol-run-' + cellRunId,
      name: 'arrowcol-run-' + cellRunId, state: stateName, absence: null },
    qap: { database: 'perf69-qap', bindings: [{ external_table: 'arrowcol_data', relative_path: 'perf69/' + cellRunId }], state: stateName, absence: [] },
    cleanup: { stop_admission: { ...step }, bounded_drain: { ...step }, evidence_capture: { ...step }, stop_maintenance: { ...step }, terminal_capture: { ...step }, handoff: { ...step } },
  };
};
const armForTest = async (store: MemoryStore, now = 1_000_000, ready = true) => {
  const state = (await store.read())!.state;
  await publishRunInventory(store, inventoryFor(state), now);
  await armCampaign(store, ownerId, state.fencingToken, now);
  const planned = (await store.read())!.state.runInventory!;
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, now);
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, now);
  if (ready) await publishRunInventory(store, inventoryFor((await store.read())!.state, true), now + 1);
  return (await store.read())!.state;
};

test('load admission requires a pre-provisioning initial record and cannot skip or reopen authorization', async () => {
  const store = await seed(), started = (await store.read())!.state, planned = inventoryFor(started);
  const { load_admission: _legacy, ...legacy } = planned;
  await assert.rejects(publishRunInventory(store, legacy, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'invalid' }, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs), /before external provisioning/);
  await assert.rejects(publishRunInventory(store, inventoryFor(started, true), started.startedAtMs), /before external provisioning/);
  await publishRunInventory(store, planned, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs), /authorization_pending/);
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, started.startedAtMs);
  assert.equal((await store.read())!.state.runInventory!.load_admission, 'authorization_pending');
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, planned, started.startedAtMs), /monotonically/);
  assert.equal((await store.read())!.state.runInventory!.load_admission, 'authorized');
});

test('Rust RunInventory timing fields are strictly parsed and legacy inventory cannot arm', async () => {
  const store = await seed(), started = (await store.read())!.state;
  const rustInventory = JSON.parse(JSON.stringify(inventoryFor(started))) as RunInventory;
  await publishRunInventory(store, rustInventory, started.startedAtMs);
  const invalidBudgets = [
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_199_999 },
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 5_100_001 },
    { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000, extra: true },
  ];
  for (const effective_stop_budgets of invalidBudgets)
    await assert.rejects(publishRunInventory(store, { ...rustInventory, effective_stop_budgets }, started.startedAtMs),
      /effective|timeout/);

  const legacyStore = await seed(), legacyStarted = (await legacyStore.read())!.state;
  const inventory = inventoryFor(legacyStarted);
  const { planned_cell_duration_ms: _duration, effective_stop_budgets: _budgets, ...legacy } = inventory;
  legacyStore.current = { state: validateCampaignState({ ...legacyStarted, runInventory: legacy }), etag: 'legacy-etag' };
  await assert.rejects(armCampaign(legacyStore, ownerId, legacyStarted.fencingToken, legacyStarted.startedAtMs),
    /without a new-cell duration and maintenance stop budget/);
});

test('bounded lifetime arithmetic admits equality and rejects a one-millisecond shortfall', async () => {
  const stopTimeout = 1_200_000;
  const duration = MAX_CAMPAIGN_LIFETIME_MS - stopTimeout - KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS;
  assert.deepEqual(assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS, duration, stopTimeout,
    KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS), { status: 'ready', required_ms: MAX_CAMPAIGN_LIFETIME_MS });
  assert.deepEqual(assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS - 1, duration, stopTimeout,
    KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS), { status: 'insufficient', required_ms: MAX_CAMPAIGN_LIFETIME_MS });
});

test('cell admission retains UNKNOWN for unbounded cleanup completion stages', async () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  const store = new MemoryStore(); store.current = { state, etag: 'etag-1' };
  const assessment = assessCellLifetimeBudget(MAX_CAMPAIGN_LIFETIME_MS, 0, 1_200_000);
  assert.equal(assessment.status, 'unknown');
  if (assessment.status === 'unknown') {
    assert.equal(assessment.required_ms, 1_200_000 + KNOWN_LIFECYCLE_CLEANUP_RESERVE_MS);
    assert.match(assessment.reason, /preserve-evidence.*remove-run-namespace/);
  }
  await publishRunInventory(store, { ...inventoryFor(state), planned_cell_duration_ms: 0 }, state.startedAtMs);
  assert.equal((await store.read())!.state.runInventory!.planned_cell_duration_ms, 0);
});

test('zero-duration rehearsal stays not-authorized and cell duration/stop budgets cannot change', async () => {
  const store = await seed(), started = (await store.read())!.state;
  const rowFree = { ...inventoryFor(started), planned_cell_duration_ms: 0 };
  await publishRunInventory(store, rowFree, started.startedAtMs);
  await assert.rejects(publishRunInventory(store, { ...rowFree, load_admission: 'authorization_pending' }, started.startedAtMs), /malformed/);
  await assert.rejects(publishRunInventory(store, { ...rowFree, planned_cell_duration_ms: 1 }, started.startedAtMs), /immutable/);
  await assert.rejects(publishRunInventory(store, { ...rowFree,
    effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_001 } }, started.startedAtMs), /immutable/);
});

test('maintenance milestone is required, bounded, monotonic and required for handoff', async () => {
  const store = await seed(), inventory = inventoryFor((await store.read())!.state);
  await publishRunInventory(store, inventory, 1_000_000);
  const { stop_maintenance: _omitted, ...legacy } = inventory.cleanup;
  await assert.rejects(publishRunInventory(store, { ...inventory, cleanup: legacy }, 1_000_000), /malformed/);
  for (const deadline of [null, 120_000]) {
    const invalid = structuredClone(inventory);
    invalid.cleanup.stop_maintenance = { state: 'running', deadline_unix_ms: deadline, reason_code: null };
    await assert.rejects(publishRunInventory(store, invalid, 1_000_000), /absolute deadline/);
  }
  const running = structuredClone(inventory);
  running.cleanup.stop_maintenance = { state: 'running', deadline_unix_ms: 1_120_000, reason_code: null };
  await publishRunInventory(store, running, 1_000_000);
  const complete = structuredClone(running);
  complete.cleanup.stop_maintenance.state = 'complete';
  await publishRunInventory(store, complete, 1_000_000);
  await assert.rejects(publishRunInventory(store, running, 1_000_000), /Completed cleanup state/);
  const unresolved = inventoryFor((await store.read())!.state);
  unresolved.resources = []; unresolved.fic = null; unresolved.qap = null;
  for (const [name, step] of Object.entries(unresolved.cleanup)) if (name !== 'stop_maintenance') step.state = 'complete';
  await assert.rejects(publishRunInventory(store, unresolved, 1_000_000), /handoff cannot complete/);
});

test('new campaign is observe-only with fixed heartbeat and lifetime bounds', () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  assert.equal(state.mode, 'observe');
  assert.equal(state.expiresAtMs, 100 + MAX_CAMPAIGN_LIFETIME_MS);
  assert.equal(HEARTBEAT_INTERVAL_MS, 60_000);
  assert.equal(MISSED_HEARTBEAT_LIMIT, 3);
});

test('campaign start receipt provides strict runner environment identities', () => {
  const state = newCampaignState(campaignId, ownerId, 100);
  const receipt = campaignStartReceipt(state);
  assert.deepEqual(receipt.environment, { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' });
  assert.equal(receipt.status, 'started');
  assert.doesNotThrow(() => assertCampaignEnvironment(receipt.environment, state));
  assert.throws(() => assertCampaignEnvironment({ CAMPAIGN_ID: campaignId }, state), /does not match the durable fence/);
});

test('heartbeat cannot revive expired custody and stale fencing tokens are rejected', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const beat = await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 1);
  assert.equal(beat.state.heartbeatSequence, 1);
  assert.equal(beat.state.expiresAtMs, started.expiresAtMs);
  await assert.rejects(heartbeatCampaign(store, ownerId, started.fencingToken, started.expiresAtMs), /expired/);
});

test('observer does not fence; armed controller fences once at three missed beats', async () => {
  const observedStore = await seed();
  const observedStart = (await observedStore.read())!.state;
  await publishRunInventory(observedStore, inventoryFor(observedStart), observedStart.startedAtMs);
  const expiredAt = observedStart.heartbeatAtMs + 3 * HEARTBEAT_INTERVAL_MS;
  assert.equal(evaluateCustody(observedStart, expiredAt).wouldFence, true);
  assert.equal((await reconcileOnce(observedStore, expiredAt)).status, 'observed');
  assert.equal((await observedStore.read())!.state.status, 'active');

  const armedStore = await seed();
  const started = await armForTest(armedStore);
  const fenced = await reconcileOnce(armedStore, expiredAt);
  assert.equal(fenced.status, 'fenced');
  const state = (await armedStore.read())!.state;
  assert.equal(state.status, 'fenced');
  assert.equal(state.fencingToken, started.fencingToken + 1);
  assert.equal(state.runInventory!.fencing_token, started.fencingToken);
  assert.equal(state.cleanupStatus, 'pending');
  assert.equal((await reconcileOnce(armedStore, expiredAt + HEARTBEAT_INTERVAL_MS)).status, 'terminal');
  assert.equal((await armedStore.read())!.state.fencingToken, started.fencingToken + 1);
});

test('max lifetime fences even when heartbeat is current', async () => {
  const store = await seed();
  const started = await armForTest(store);
  await heartbeatCampaign(store, ownerId, started.fencingToken, started.heartbeatAtMs + 10);
  const expired = (await store.read())!.state;
  const result = await reconcileOnce(store, expired.expiresAtMs);
  assert.equal(result.fenceReason, 'max-lifetime');
});

test('expired heartbeat blocks owner mutations before the watchdog fences', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const expiredAt = state.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await assert.rejects(heartbeatCampaign(store, ownerId, state.fencingToken, expiredAt), /heartbeat is expired/);
  await assert.rejects(armCampaign(store, ownerId, state.fencingToken, expiredAt), /heartbeat is expired/);
});

test('campaign ownership cannot be replaced until cleanup is verified', async () => {
  const store = await seed();
  await assert.rejects(startCampaign(store, 'c0ffee00-0000-4000-8000-000000000004', ownerId, 1_000_100), /unfinished campaign/);
});

test('fenced cleanup retries the same idempotency key and completes ordered steps once', async () => {
  const store = await seed();
  await armForTest(store);
  const started = (await store.read())!.state;
  const expiredAt = started.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await reconcileOnce(store, expiredAt);
  await assert.rejects(heartbeatCampaign(store, ownerId, started.fencingToken, expiredAt + 1), /fenced, stale/);
  const seen: Array<{ step: string; key: string }> = [];
  let firstCall = true;
  const execute = async (step: typeof CUSTODY_CLEANUP_STEPS[number], _state: CampaignState, key: string) => {
    seen.push({ step, key });
    if (step === 'stop-admission' && firstCall) { firstCall = false; return 'pending' as const; }
    return 'complete' as const;
  };
  assert.equal(await reconcileCleanupOnce(store, execute), 'pending');
  assert.equal((await store.read())!.state.cleanupSteps[0]!.status, 'running');
  let progress = await reconcileCleanupOnce(store, execute);
  while (progress !== 'complete') progress = await reconcileCleanupOnce(store, execute);
  const completed = (await store.read())!.state;
  assert.equal(completed.status, 'complete');
  assert.deepEqual(completed.cleanupSteps.map(receipt => receipt.step), CUSTODY_CLEANUP_STEPS);
  assert.ok(completed.cleanupSteps.every(receipt => receipt.status === 'complete'));
  assert.equal(seen[0]!.key, seen[1]!.key);
  assert.equal(seen[0]!.key, completed.campaignId + ':' + completed.fencingToken + ':stop-admission');
  assert.deepEqual(seen.filter((item, index) => index === 0 || item.step !== seen[index - 1]!.step).map(item => item.step), CUSTODY_CLEANUP_STEPS);
});

test('ARM operation receipts reject out-of-scope URLs and reconcile terminal public status values', async () => {
  const store = await seed();
  const state = await armForTest(store);
  await assert.rejects(recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000a', resourceId,
      operationUrl: 'https://example.com/operation', submittedAtMs: state.startedAtMs }, subscriptionId, state.startedAtMs + 2), /management.azure.com/);
  await recordArmOperation(store, ownerId, state.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000b', resourceId, operationUrl, submittedAtMs: state.startedAtMs }, subscriptionId, state.startedAtMs + 2);
  const pending = (await store.read())!.state.operations[0]!;
  const completed = await reconcileArmOperation(pending, async url => {
    assert.equal(url, operationUrl);
    return { status: 200, body: { status: 'Succeeded' } };
  }, subscriptionId, state.startedAtMs + 3);
  assert.equal(completed.status, 'succeeded');
  const reconciled = await reconcilePendingArmOperations(store, async () => ({ status: 202 }), subscriptionId, state.startedAtMs + 3);
  assert.equal(reconciled!.operations[0]!.status, 'pending');
  const hostile = { ...pending, operationUrl: 'https://127.0.0.1/latest/meta-data' };
  await assert.rejects(reconcileArmOperation(hostile, async () => ({ status: 200 }), subscriptionId, state.startedAtMs + 3), /outside the configured subscription/);
});

test('RunInventory fenced merge preserves heartbeat and ARM receipts and blocks load until observed', async () => {
  const store = await seed();
  const started = (await store.read())!.state;
  const planned = inventoryFor(started);
  await publishRunInventory(store, planned, started.startedAtMs);
  await assert.rejects(assertRunInventoryReady(store, ownerId, started.fencingToken, started.startedAtMs), /blocked/);
  await armCampaign(store, ownerId, started.fencingToken, started.startedAtMs);
  await publishRunInventory(store, { ...planned, load_admission: 'authorization_pending' }, started.startedAtMs);
  await publishRunInventory(store, { ...planned, load_admission: 'authorized' }, started.startedAtMs);
  const observed = inventoryFor((await store.read())!.state, true);
  await publishRunInventory(store, observed, started.startedAtMs + 1);
  await assertRunInventoryReady(store, ownerId, started.fencingToken, started.startedAtMs + 2);
  await heartbeatCampaign(store, ownerId, started.fencingToken, started.startedAtMs + 3);
  await recordArmOperation(store, ownerId, started.fencingToken,
    { id: 'c0ffee00-0000-4000-8000-00000000000c', resourceId, operationUrl, submittedAtMs: started.startedAtMs },
    subscriptionId, started.startedAtMs + 4);
  const changed = { ...observed, cleanup: { ...observed.cleanup, stop_admission: { state: 'complete' as const, deadline_unix_ms: null, reason_code: null } } };
  await publishRunInventory(store, changed, started.startedAtMs + 5);
  const merged = (await store.read())!.state;
  assert.equal(merged.heartbeatSequence, 1);
  assert.equal(merged.operations.length, 1);
  assert.equal(merged.runInventory!.cleanup.stop_admission.state, 'complete');
  const nextCellId = 'c0ffee00-0000-4000-8000-00000000000d';
  const nextNamespace = 'arrowcol-' + nextCellId.replace(/-/g, '');
  const nextCell = { ...changed, cell_run_id: nextCellId, namespace: { ...changed.namespace, name: nextNamespace },
    resources: changed.resources.map(resource => ({ ...resource, namespace: nextNamespace })) };
  await assert.rejects(publishRunInventory(store, nextCell, started.startedAtMs + 6), /unresolved RunInventory/);
});

test('RunInventory rejects unknown identities and reports an unconfirmed Blob write', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const planned = inventoryFor(state);
  await assert.rejects(publishRunInventory(store, { ...planned, extra: true }, state.startedAtMs), /malformed/);
  class FailsBeforeWrite extends MemoryStore {
    failNext = false;
    override async write(next: CampaignState, etag?: string): Promise<string> {
      if (this.failNext) { this.failNext = false; throw new Error('connection reset'); }
      return super.write(next, etag);
    }
  }
  const failing = new FailsBeforeWrite();
  await startCampaign(failing, campaignId, ownerId, state.startedAtMs);
  failing.failNext = true;
  await assert.rejects(publishRunInventory(failing, planned, state.startedAtMs), CampaignWriteOutcomeUnknownError);
});

test('successful inventory CAS receipt supports unknown FIC and QAP', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const inventory = { ...inventoryFor(state), fic: null, qap: null };
  const written = await publishRunInventory(store, inventory, state.startedAtMs);
  const receipt = runInventoryPublishReceipt(written);
  assert.equal(receipt.status, 'persisted');
  assert.equal(receipt.fic_state, null);
  assert.equal(receipt.qap_state, null);
  assert.equal(receipt.resource_count, inventory.resources.length);
  await assert.rejects(assertRunInventoryReady(store, ownerId, state.fencingToken, state.startedAtMs), /blocked/);
});

test('RunInventory absent state requires immutable exact not-found receipts', async () => {
  const store = await seed();
  const state = (await store.read())!.state;
  const planned = inventoryFor(state);
  const first = planned.resources[0]!;
  await publishRunInventory(store, planned, state.startedAtMs);
  const absentResource = { ...first, state: 'absent' as const, absence: { api_version: first.api_version, kind: first.kind,
    namespace: first.namespace, name: first.name, namespace_uid: planned.namespace.uid, observed_at_unix_ms: state.startedAtMs + 1 } };
  const fic = planned.fic!;
  const qap = planned.qap!;
  const absent = { ...planned, resources: [absentResource, ...planned.resources.slice(1)],
    fic: { ...fic, state: 'absent' as const, absence: { resource_id: fic.resource_id!, name: fic.name!, observed_at_unix_ms: state.startedAtMs + 2 } },
    qap: { ...qap, state: 'absent' as const, absence: qap.bindings.map(binding => ({ database: qap.database,
      external_table: binding.external_table, relative_path: binding.relative_path, observed_at_unix_ms: state.startedAtMs + 3 })) } };
  await publishRunInventory(store, absent, state.startedAtMs + 4);
  const malformedResource = { ...absent, resources: [{ ...absentResource, absence: { ...absentResource.absence, name: 'other' } }, ...absent.resources.slice(1)] };
  await assert.rejects(publishRunInventory(await seed(), malformedResource, state.startedAtMs + 4), /malformed|FIC|QAP/);
  const missingQapReceipt = { ...absent, qap: { ...absent.qap!, absence: [] } };
  await assert.rejects(publishRunInventory(await seed(), missingQapReceipt, state.startedAtMs + 4), /not-found receipt/);
  const duplicateQapReceipt = { ...absent, qap: { ...absent.qap!, absence: [...absent.qap!.absence, absent.qap!.absence[0]!] } };
  await assert.rejects(publishRunInventory(await seed(), duplicateQapReceipt, state.startedAtMs + 4), /duplicate absence/);
});

test('watchdog cleanup CAS retains producer token and rejects stale producer writes', async () => {
  const store = await seed();
  const armed = await armForTest(store);
  const expiredAt = armed.heartbeatAtMs + MISSED_HEARTBEAT_LIMIT * HEARTBEAT_INTERVAL_MS;
  await reconcileOnce(store, expiredAt);
  const fenced = (await store.read())!.state;
  assert.equal(fenced.fencingToken, armed.fencingToken + 1);
  assert.equal(fenced.runInventory!.fencing_token, armed.fencingToken);
  await assert.rejects(publishRunInventory(store, fenced.runInventory, expiredAt + 1), /blocked after campaign fencing/);
  await reconcileCleanupOnce(store, async () => 'pending');
  const cleaning = (await store.read())!.state;
  const inventory = cleaning.runInventory!;
  const updated = { ...inventory, cleanup: { ...inventory.cleanup,
    stop_admission: { state: 'pending' as const, deadline_unix_ms: expiredAt + 60_000, reason_code: null } } };
  await publishCleanupRunInventory(store, updated, ownerId, cleaning.fencingToken);
  const merged = (await store.read())!.state;
  assert.equal(merged.runInventory!.fencing_token, armed.fencingToken);
  assert.equal(merged.fencingToken, cleaning.fencingToken);
  assert.equal(merged.runInventory!.cleanup.stop_admission.state, 'pending');
  await assert.rejects(publishCleanupRunInventory(store, updated, ownerId, armed.fencingToken), /fenced|cleaning token/);
});

test('scale trial journal rejects malformed target bindings and duplicate ids', () => {
  const state = newCampaignState(campaignId, ownerId, 1_000_000);
  const inventory = inventoryFor(state, true);
  const armed = { ...state, mode: 'armed' as const, runInventory: inventory };
  const target = { dimension: 'collector-cpu-limit' as const, before: 2, after: 3 };
  const step: ScaleStep = { ...target, restore: { dimension: target.dimension, before: 3, after: 2 }, evidence: ['collector-cpu-saturated'],
    baselineCommittedRowsPerSecond: 100, baselineQapPendingBacklog: 1, baselineQapP95LatencyMs: 10 };
  const generated = newScaleTrialReceipt({ state: armed, step, resource: { transport: 'kubernetes',
    resource_id: 'apps/v1/Deployment/' + inventory.namespace.name + '/arrowcol/' + inventory.resources[0]!.uid, api_version: 'apps/v1',
    kind: 'Deployment', namespace: inventory.namespace.name, name: 'arrowcol', uid: inventory.resources[0]!.uid }, now: armed.startedAtMs });
  assert.equal(generated.restore_deadline_at_ms - generated.deadline_at_ms, SCALE_RESTORE_TIMEOUT_MS);
  assert.throws(() => newScaleTrialReceipt({ state: armed, step, resource: generated.resource,
    now: armed.expiresAtMs - SCALE_RESTORE_TIMEOUT_MS }), /bounded remaining lifetime/);
  const receipt = {
    schema_version: 1 as const, trial_id: 'c0ffee00-0000-4000-8000-000000000010', campaign_id: campaignId, owner_id: ownerId,
    fencing_token: 1, cell_run_id: inventory.cell_run_id, started_at_ms: armed.startedAtMs, deadline_at_ms: armed.startedAtMs + 60_000,
    restore_deadline_at_ms: armed.startedAtMs + 360_000,
    target, restore: { dimension: target.dimension, before: 3, after: 2 }, evidence: ['collector-cpu-saturated'],
    baseline: { committed_rows_per_second: 100, qap_pending_backlog: 1, qap_p95_latency_ms: 10 },
    resource: { transport: 'kubernetes' as const, resource_id: 'apps/v1/Deployment/' + inventory.namespace.name + '/arrowcol/' + inventory.resources[0]!.uid,
      api_version: 'apps/v1', kind: 'Deployment', namespace: inventory.namespace.name, name: 'arrowcol', uid: inventory.resources[0]!.uid },
    phase: 'apply-intent' as const,
  };
  assert.equal(validateCampaignState({ ...armed, scaleTrials: [receipt] }).scaleTrials?.length, 1);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [receipt, receipt] }), /duplicated/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore: { ...receipt.restore, after: 4 } }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore_deadline_at_ms: receipt.deadline_at_ms }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, restore_deadline_at_ms: receipt.restore_deadline_at_ms + 1 }] }), /malformed/);
  assert.throws(() => validateCampaignState({ ...armed, scaleTrials: [{ ...receipt, resource: { ...receipt.resource, uid: 'c0ffee00-0000-4000-8000-000000000099' } }] }), /malformed/);
});

test('strict state schema rejects additional persisted fields', async () => {
  const state = newCampaignState(campaignId, ownerId, 1);
  await assert.rejects(async () => validateCampaignState({ ...state, unexpected: true }), /unknown fields/);
  const store = new MemoryStore();
  await mutateCampaign(store, () => state);
  assert.equal((await store.read())!.state.campaignId, campaignId);
});


test('campaign Blob store keeps the default record path and supports isolated paths', async () => {
  const paths: string[] = [];
  const fetcher: typeof fetch = async input => {
    paths.push(new URL(String(input)).pathname);
    return new Response(null, { status: 404 });
  };
  const config = { account: 'arrowcollector12345', container: 'checkpoints', subscriptionId };
  await createBlobCampaignStore(config, fetcher, async () => 'fixture-token').read();
  await createBlobCampaignStore({ ...config, recordPath: '_arrowcol-custody-rehearsal/rehearsal-1/active.json' },
    fetcher, async () => 'fixture-token').read();
  assert.deepEqual(paths, ['/checkpoints/active.json',
    '/checkpoints/_arrowcol-custody-rehearsal/rehearsal-1/active.json']);
  assert.throws(() => createBlobCampaignStore({ ...config, recordPath: '../active.json' }, fetcher,
    async () => 'fixture-token'), /Campaign store record path is invalid/);
});
