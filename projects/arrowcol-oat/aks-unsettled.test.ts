import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CampaignConflictError, CUSTODY_CLEANUP_STEPS, DRAIN_UNSETTLED_REASON, hasVerifiedUnsettledDrain, newCampaignState,
  validateCampaignState, type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { createExternalCleanupExecutor, type ExternalCleanupActions } from './aks-cleanup-executor.js';
import { executeCellCleanupStep, type CellStepJournal, type CellStepRecord } from './aks-cell-cleanup.js';
import { reconcileCleanupOnce, recordUnsettledDrain } from './aks-reconcile.js';
import { runWatchdog, watchdogOnce } from './aks-watchdog.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001', ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const cellId = 'c0ffee00-0000-4000-8000-000000000003', namespace = 'oat-' + cellId.replaceAll('-', '');
class Store implements CampaignStore {
  revision = 1;
  afterWrite?: () => void;
  constructor(public state: CampaignState) { validateCampaignState(state); }
  async read(): Promise<StoredCampaign> { return { state: structuredClone(this.state), etag: `"${this.revision}"` }; }
  async write(state: CampaignState, expected?: string) {
    if (expected !== `"${this.revision}"`) throw new CampaignConflictError();
    this.state = structuredClone(validateCampaignState(state));
    this.revision++;
    this.afterWrite?.();
    return `"${this.revision}"`;
  }
}
function fixture() {
  const now = Date.now(), started = now - 300_000, step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, namespace: { name: namespace, uid: cellId, owner_token: ownerId },
    resources: ['collector', 'maintenance'].map((name, index) => ({ api_version: 'apps/v1', kind: 'Deployment', namespace, name,
      uid: index === 0 ? campaignId : ownerId, role: index === 0 ? 'collector' : 'maintenance', state: 'observed', absence: null })),
    fic: null, qap: null, evidence_destination: { account: 'fixtureaccount', container: 'checkpoints', blob_prefix: 'evidence/' + cellId },
    cleanup: { stop_admission: { ...step, state: 'complete' }, bounded_drain: { ...step, state: 'running', deadline_unix_ms: now - 1_000 },
      evidence_capture: { ...step }, stop_maintenance: { ...step }, terminal_capture: { ...step }, handoff: { ...step } } };
  const store = new Store({ ...newCampaignState(campaignId, ownerId, started), mode: 'armed', status: 'cleaning',
    fencingToken: 2, fenceReason: 'heartbeat-expired', fencedAtMs: now - 120_000, cleanupStatus: 'running', runInventory: inventory,
    cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
      status: index === 0 ? 'complete' : 'pending' })) });
  return { store, now };
}

test('only an expired current controller with positive admission and the same original drain may record unsettled expiry', async () => {
  const { store, now } = fixture();
  await assert.rejects(recordUnsettledDrain(store, ownerId, 1, now), /current cleaning controller/);
  await assert.rejects(recordUnsettledDrain(store, ownerId, 2, now - 1_001), /not expired/);
  const original = structuredClone(store.state);
  store.state.runInventory!.cleanup.stop_admission.state = 'pending';
  await assert.rejects(recordUnsettledDrain(store, ownerId, 2, now), /admission is not stopped/);
  store.state = structuredClone(original);
  store.state.runInventory!.cleanup.bounded_drain = { state: 'failed', deadline_unix_ms: now - 1_000, reason_code: 'unrelated_storage_failure' };
  let effects = 0;
  await assert.rejects(recordUnsettledDrain(store, ownerId, 2, now), /unrelated failure/);
  assert.equal(await reconcileCleanupOnce(store, async () => { effects++; return 'complete'; }, now), 'pending');
  assert.equal(effects, 0);
  store.state = structuredClone(original);
  store.state = { ...store.state, status: 'active', fencingToken: 1, cleanupStatus: 'not-started', cleanupSteps: [],
    heartbeatAtMs: now, fenceReason: undefined, fencedAtMs: undefined };
  await assert.rejects(recordUnsettledDrain(store, ownerId, 1, now), /current cleaning controller/);
  assert.equal(store.revision, 1);
});

test('expiry evidence is durably reread, exact, idempotent and survives restart without inventing row counts', async () => {
  const { store, now } = fixture(), confirmed = await recordUnsettledDrain(store, ownerId, 2, now);
  assert.equal(confirmed.state.cleanupSteps[1].status, 'unsettled');
  assert.equal(confirmed.state.runInventory!.cleanup.bounded_drain.state, 'uncertain');
  assert.equal(confirmed.state.runInventory!.cleanup.bounded_drain.reason_code, DRAIN_UNSETTLED_REASON);
  const proof = confirmed.state.cleanupSteps[1].unsettled!;
  assert.equal(proof.observedAtUnixMs, now);
  assert.equal(proof.deadlineUnixMs, now - 1_000);
  assert.equal(proof.producerFencingToken, 1);
  assert.equal(proof.actorFencingToken, 2);
  assert.equal(proof.deltaRows, null);
  assert.equal(proof.kustoRows, null);
  const restarted = new Store(JSON.parse(JSON.stringify(confirmed.state)) as CampaignState);
  await recordUnsettledDrain(restarted, ownerId, 2, now);
  assert.deepEqual(restarted.state.cleanupSteps[1].unsettled, proof);
  assert.equal(restarted.revision, 1);
  for (const patch of [{ deadlineUnixMs: now + 60_000 }, { observedAtUnixMs: now - 1_001 },
    { producerFencingToken: 2 }, { actorFencingToken: 3 }, { cellRunId: ownerId }, { namespaceUid: ownerId },
    { deltaRows: 0 }, { kustoRows: 0 }]) {
    assert.throws(() => validateCampaignState({ ...confirmed.state, cleanupSteps: confirmed.state.cleanupSteps.map((step, index) =>
      index === 1 ? { ...step, unsettled: { ...proof, ...patch } } : step) }), /unsettled drain/);
  }
  assert.throws(() => validateCampaignState({ ...confirmed.state, status: 'complete', cleanupStatus: 'verified' }), /cleanup state|unsettled/);
});

test('fencing between durable failed evidence and terminal receipt cannot advance or manufacture proof', async () => {
  const { store, now } = fixture();
  store.afterWrite = () => {
    store.afterWrite = undefined;
    store.state.status = 'fenced';
    store.state.cleanupStatus = 'pending';
  };
  await assert.rejects(recordUnsettledDrain(store, ownerId, 2, now), /current cleaning controller|fenced/);
  assert.notEqual(store.state.cleanupSteps[1].status, 'unsettled');
  assert.equal(store.state.runInventory!.cleanup.bounded_drain.reason_code, DRAIN_UNSETTLED_REASON);
});

test('expired drain continues ordered cleanup to verified idle while keeping the cell failed and terminal-unsettled', async t => {
  const { store } = fixture(), events: string[] = [];
  let maintenance = true, recreation = true, idle = false;
  const forbidden = async (): Promise<never> => { throw new Error('Expired admission/drain or nonexistent QAP/FIC must not execute.'); };
  const actions: ExternalCleanupActions = { discoverExactInventory: async inventory => structuredClone(inventory),
    stopAdmission: forbidden, boundedDrain: forbidden,
    preserveEvidence: async inventory => {
      assert.equal(maintenance, true);
      assert.equal(inventory.cleanup.bounded_drain.reason_code, DRAIN_UNSETTLED_REASON);
      events.push('evidence'); return 'complete';
    },
    stopMaintenanceIfUidAndOwnerMatch: async (_resource, _owner, _key, authority) => {
      await authority.assertCurrent();
      recreation = false; maintenance = false;
      assert.equal(maintenance || recreation, false);
      events.push('maintenance'); return 'complete';
    },
    captureTerminalState: async inventory => {
      assert.equal(maintenance || recreation, false);
      assert.equal(inventory.cleanup.bounded_drain.state, 'uncertain');
      events.push('terminal'); return 'complete';
    },
    deleteNamespaceIfUidAndOwnerMatch: async ns => { assert.equal(ns.uid, cellId); events.push('namespace'); return 'complete'; },
    deleteQapBindingIfOwned: forbidden, deleteFicIfExact: forbidden,
    restoreSafeIdle: async () => { idle = true; events.push('restore'); return 'complete'; },
    verifySafeIdle: async state => {
      assert.equal(idle, true);
      assert.ok(state.runInventory!.resources.every(resource => resource.state === 'removed'));
      events.push('verify'); return 'complete';
    } };
  const execute = createExternalCleanupExecutor(store, actions);
  let progress = await reconcileCleanupOnce(store, execute);
  for (let iterations = 0; progress !== 'complete_unsettled'; iterations++) {
    assert.ok(iterations < 12);
    progress = await reconcileCleanupOnce(store, execute);
  }
  assert.deepEqual(events, ['evidence', 'maintenance', 'terminal', 'namespace', 'restore', 'verify']);
  assert.equal(store.state.status, 'complete_unsettled');
  assert.equal(store.state.cleanupStatus, 'verified_with_unsettled');
  assert.equal(store.state.runInventory!.cleanup.handoff.state, 'failed');
  assert.equal(store.state.runInventory!.cleanup.handoff.reason_code, DRAIN_UNSETTLED_REASON);
  assert.equal(store.state.runInventory!.cleanup.bounded_drain.state, 'uncertain');
  assert.equal(store.state.runInventory!.fencing_token, 1);
  assert.ok(hasVerifiedUnsettledDrain(store.state));
  assert.ok(store.state.cleanupSteps.every(step => step.step === 'bounded-drain' ? step.status === 'unsettled' : step.status === 'complete'));
  assert.throws(() => validateCampaignState({ ...store.state, runInventory: { ...store.state.runInventory,
    resources: store.state.runInventory!.resources.map(resource => ({ ...resource, state: 'observed' })) } }), /cannot finalize/);
  assert.throws(() => validateCampaignState({ ...store.state, runInventory: { ...store.state.runInventory,
    cleanup: { ...store.state.runInventory!.cleanup, handoff: { state: 'complete', deadline_unix_ms: null, reason_code: null } } } }),
  /handoff cannot complete/);
  const terminal = await watchdogOnce({ ARM_SUBSCRIPTION_ID: campaignId }, undefined, store);
  assert.equal(terminal.cleanupProgress, 'complete_unsettled');
  assert.equal(terminal.campaignStatus, 'complete_unsettled');
  let polls = 0;
  t.mock.method(console, 'log', () => {});
  assert.equal(await runWatchdog(new AbortController().signal, async () => { polls++; return terminal; }), 'complete_unsettled');
  assert.equal(polls, 1);
  assert.equal(await reconcileCleanupOnce(store, execute), 'complete_unsettled');
  assert.equal(events.length, 6);
});

test('only validated controller expiry can bypass the unresolved drain journal for later cell cleanup', async () => {
  const { store, now } = fixture();
  await recordUnsettledDrain(store, ownerId, 2, now);
  store.state.cleanupSteps[2].status = 'running';
  let effects = 0, record: { value: CellStepRecord; etag: string } | undefined;
  const journal: CellStepJournal = { read: async selector => selector === 'evidence_capture' ? record : undefined,
    write: async value => { record = { value: structuredClone(value), etag: `"journal-${effects}-${value.phase}"` }; return record.etag; } };
  const current = await store.read();
  const input = { schema_version: 1, inventory: current.state.runInventory!, expected_etag: current.etag,
    actor: { mode: 'controller', owner_id: ownerId, fencing_token: 2 } };
  const receipt = await executeCellCleanupStep(store, input, 'evidence_capture', { evidence_capture: {
    execute: async inventory => { effects++; return inventory; },
  } }, () => journal);
  assert.equal(receipt.fencing_token, 1);
  assert.equal(receipt.actor.fencing_token, 2);
  assert.equal(effects, 1);
  assert.equal(store.state.runInventory!.cleanup.bounded_drain.state, 'uncertain');
});
