import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { test, type TestContext } from 'node:test';
import { Azure, Config } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CampaignWriteOutcomeUnknownError, cleanupFailureCode, CUSTODY_CLEANUP_STEPS, newCampaignState,
  type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';
import { CELL_CLEANUP_SELECTORS, createBlobCellStepJournal, createCellCleanupProviders, executeCellCleanupStep,
  readCellRunInventory, type CellCleanupEnvelope, type CellCleanupSelector, type CellStepProviders, type ReadInventoryEnvelope } from './aks-cell-cleanup.js';
import type { CleanupMutationAuthority, ExternalCleanupActions } from './aks-cleanup-executor.js';
import { cleanupBudget } from './aks-cleanup-budget.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001', ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const cellId = 'c0ffee00-0000-4000-8000-000000000003', namespace = 'oat-' + cellId.replaceAll('-', '');
class Store implements CampaignStore {
  revision = 1;
  fault?: 'before' | 'after';
  failConfirmOnce = false;
  constructor(public state: CampaignState) {}
  async read(): Promise<StoredCampaign> {
    if (this.failConfirmOnce && this.state.runInventory?.cleanup.stop_admission.state === 'complete') {
      this.failConfirmOnce = false;
      throw new Error('injected failed post-write read');
    }
    return { state: structuredClone(this.state), etag: `"${this.revision}"` };
  }
  async write(state: CampaignState, expected?: string) {
    if (expected !== `"${this.revision}"`) throw new CampaignConflictError();
    const fault = state.runInventory?.cleanup.stop_admission.state === 'complete' ? this.fault : undefined;
    if (fault) this.fault = undefined;
    if (fault === 'before') throw new Error('injected missing write response before commit');
    this.state = structuredClone(state);
    this.revision++;
    if (fault === 'after') throw new Error('injected missing write response after commit');
    return `"${this.revision}"`;
  }
}
function fixture(t: TestContext, evidence = true) {
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, namespace: { name: namespace, uid: cellId, owner_token: ownerId },
    resources: [{ api_version: 'apps/v1', kind: 'Deployment', namespace, name: 'maintenance', uid: campaignId,
      role: 'maintenance', state: 'observed', absence: null }],
    fic: { resource_id: '/subscriptions/' + campaignId + '/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/fixture/federatedIdentityCredentials/arrowcol-run-' + cellId,
      name: 'arrowcol-run-' + cellId, state: 'observed', absence: null },
    qap: { database: 'qap', bindings: [{ external_table: 'collector', relative_path: 'runs/' + cellId }], state: 'observed', absence: [] },
    evidence_destination: evidence ? { account: 'fixtureaccount', container: 'checkpoints', blob_prefix: 'evidence/' + cellId } : null,
    cleanup: { stop_admission: { ...step }, bounded_drain: { ...step }, evidence_capture: { ...step }, stop_maintenance: { ...step }, terminal_capture: { ...step }, handoff: { ...step } } };
  const store = new Store({ ...newCampaignState(campaignId, ownerId), mode: 'armed', runInventory: inventory });
  const calls: string[] = [], blobs = new Map<string, { bytes: Buffer; etag: string }>();
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: campaignId }));
  let version = 0, afterJournal: (() => void) | undefined, beforeJournalRead: (() => void) | undefined;
  t.mock.method(azure, 'blob', async (method: string, _container: string, name: string, _query: unknown,
    body: string | Buffer | null, headers: Record<string, string>) => {
    const prior = blobs.get(name);
    if (method === 'GET') {
      beforeJournalRead?.();
      return [prior?.bytes ?? null, prior ? { etag: prior.etag } : {}];
    }
    if (headers[prior ? 'If-Match' : 'If-None-Match'] !== (prior?.etag ?? '*')) throw new CampaignConflictError();
    const etag = `"journal-${++version}"`;
    blobs.set(name, { bytes: Buffer.from(String(body)), etag });
    afterJournal?.();
    return [null, { ':status': '201', etag }];
  });
  const effect = (name: string) => async (...args: unknown[]) => {
    const authority = args.at(-1) as CleanupMutationAuthority;
    await authority.assertCurrent();
    if (name === 'maintenance') {
      assert.equal(store.state.runInventory!.cleanup.stop_maintenance.state, 'running');
      assert.ok(store.state.runInventory!.cleanup.stop_maintenance.deadline_unix_ms! > Date.now());
    }
    calls.push(name);
    return 'complete' as const;
  };
  const actions: ExternalCleanupActions = { discoverExactInventory: async value => structuredClone(value),
    stopAdmission: effect('admission'), boundedDrain: effect('drain'), preserveEvidence: effect('evidence'),
    stopMaintenanceIfUidAndOwnerMatch: effect('maintenance'), captureTerminalState: effect('terminal'),
    deleteNamespaceIfUidAndOwnerMatch: effect('namespace'), deleteQapBindingIfOwned: effect('qap'), deleteFicIfExact: effect('fic'),
    restoreSafeIdle: async () => { throw new Error('Per-cell path must never restore stamp capacity.'); },
    verifySafeIdle: async () => { throw new Error('Per-cell path must never claim campaign-terminal full safety.'); } };
  const envelope = async (mode: 'producer' | 'controller' = 'producer'): Promise<CellCleanupEnvelope> => ({
    schema_version: 1, inventory: (await store.read()).state.runInventory!, expected_etag: `"${store.revision}"`,
    actor: { mode, owner_id: ownerId, fencing_token: mode === 'producer' ? 1 : store.state.fencingToken },
  });
  const invoke = async (selector: CellCleanupSelector, providers = createCellCleanupProviders(actions), input?: CellCleanupEnvelope) => {
    const value = input ?? await envelope();
    return executeCellCleanupStep(store, value, selector, providers,
      assertCurrent => createBlobCellStepJournal(azure, value, assertCurrent));
  };
  return { store, actions, calls, blobs, envelope, invoke, afterJournal: (value: () => void) => { afterJournal = value; },
    beforeJournalRead: (value?: () => void) => { beforeJournalRead = value; } };
}

test('per-cell selectors execute each provider once, retain active origin, and hand off without stamp cleanup', async t => {
  const f = fixture(t);
  for (const selector of Object.keys(CELL_CLEANUP_SELECTORS) as CellCleanupSelector[]) {
    const receipt = await f.invoke(selector);
    assert.equal(receipt.step, selector);
    assert.equal(receipt.etag, `"${f.store.revision}"`);
    assert.equal(receipt.actor.mode, 'producer');
    assert.equal(receipt.fencing_token, 1);
    assert.equal(f.store.state.status, 'active');
  }
  assert.deepEqual(f.calls, ['admission', 'drain', 'evidence', 'maintenance', 'terminal', 'namespace', 'qap', 'fic']);
  const revision = f.store.revision;
  await f.invoke('stop_admission');
  assert.equal(f.store.revision, revision + 1);
  assert.equal(f.calls.filter(call => call === 'admission').length, 1);
  assert.equal(f.store.state.runInventory!.cleanup.handoff.state, 'complete');
  assert.equal(f.store.state.fencingToken, 1);
  assert.deepEqual(f.store.state.operations, []);
});

test('stale input, missing provider, skipped order and changed actor fail before effects', async t => {
  const f = fixture(t), stale = await f.envelope();
  stale.expected_etag = '"old"';
  await assert.rejects(f.invoke('stop_admission', undefined, stale), CampaignConflictError);
  await assert.rejects(f.invoke('stop_admission', {}), /No concrete/);
  await assert.rejects(f.invoke('remove_namespace'), /Prior cell cleanup/);
  assert.equal(f.store.revision, 1);
  assert.equal(f.blobs.size, 0);
  f.afterJournal(() => { f.store.state.status = 'fenced'; f.store.state.fencingToken = 2; });
  await assert.rejects(f.invoke('stop_admission'), /fenced/);
  assert.deepEqual(f.calls, []);
});

test('capture without a durable destination is refused before a journal claim or effect', async t => {
  const f = fixture(t, false);
  await f.invoke('stop_admission');
  await f.invoke('bounded_drain');
  const revision = f.store.revision, journals = f.blobs.size;
  await assert.rejects(f.invoke('evidence_capture'), /Persist the exact evidence destination/);
  assert.deepEqual(f.calls, ['admission', 'drain']);
  assert.equal(f.store.revision, revision);
  assert.equal(f.blobs.size, journals);
});

test('only pre-effect version refusals are stale_version; a conflict after effect dispatch is UNKNOWN', async t => {
  const f = fixture(t), stale = await f.envelope();
  stale.expected_etag = '"old"';
  await assert.rejects(f.invoke('stop_admission', undefined, stale), error => {
    assert.equal(cleanupFailureCode(error), 'stale_version'); return true;
  });
  assert.deepEqual(f.calls, []);
  await assert.rejects(f.invoke('stop_admission', { stop_admission: { execute: async () => {
    f.calls.push('attempted'); throw new CampaignConflictError();
  } } }), error => {
    assert.equal(cleanupFailureCode(error), 'write_outcome_unknown'); return true;
  });
  assert.deepEqual(f.calls, ['attempted']);
  await assert.rejects(f.invoke('stop_admission'), /UNKNOWN/);
  assert.deepEqual(f.calls, ['attempted']);
});

test('saved-running recovery cannot pass a same-actor inventory/version change during journal reads', async t => {
    const f = fixture(t);
    let executions = 0, recoveries = 0;
    const providers: CellStepProviders = { stop_admission: {
      execute: async () => { executions++; throw new Error('lost effect response'); },
      recover: async inventory => { recoveries++; return inventory; },
    } };
    await assert.rejects(f.invoke('stop_admission', providers), /lost effect response/);
    const input = await f.envelope();
    f.beforeJournalRead(() => {
      f.beforeJournalRead();
      f.store.state.runInventory!.evidence_destination!.blob_prefix += '/changed';
      f.store.revision++;
    });
    await assert.rejects(f.invoke('stop_admission', providers, input), CampaignWriteOutcomeUnknownError);
    assert.equal(executions, 1);
    assert.equal(recoveries, 0);
  });

test('recovery pins its confirmed current inventory version through provider authority checks', async t => {
    const f = fixture(t);
    const providers: CellStepProviders = { stop_admission: {
      execute: async () => { throw new Error('lost effect response'); },
      recover: async (_inventory, _key, authority) => {
        f.store.revision++;
        await authority.assertCurrent();
        assert.fail('Changed recovery version must be refused before a mutation.');
      },
    } };
    await assert.rejects(f.invoke('stop_admission', providers), /lost effect response/);
    await assert.rejects(f.invoke('stop_admission', providers), CampaignWriteOutcomeUnknownError);
  });

test('maintenance retains its original budget and refuses a second stop after the first exhausts it', async t => {
    const f = fixture(t), original = f.store.state.runInventory!.resources[0];
    f.store.state.runInventory!.resources.push({ ...original, name: 'maintenance-two', uid: ownerId });
    await f.invoke('stop_admission');
    await f.invoke('bounded_drain');
    await f.invoke('evidence_capture');
    let now = Date.now();
    t.mock.method(Date, 'now', () => now);
    const confirmed: string[] = [];
    const providers = createCellCleanupProviders({ ...f.actions,
      stopMaintenanceIfUidAndOwnerMatch: async (resource, _owner, _key, authority) => {
        const budget = cleanupBudget(authority)!;
        assert.equal(budget.deadlineUnixMs, f.store.state.runInventory!.cleanup.stop_maintenance.deadline_unix_ms);
        assert.equal(budget.remainingMs, 120_000);
        await authority.assertCurrent();
        confirmed.push(resource.uid!);
        now += budget.remainingMs + 1;
        return 'complete';
      } });
    await assert.rejects(f.invoke('stop_maintenance', providers), /deadline expired/);
    assert.deepEqual(confirmed, [original.uid]);
    const deadline = f.store.state.runInventory!.cleanup.stop_maintenance.deadline_unix_ms;
    assert.equal(deadline, now - 1);
    assert.equal(f.store.state.runInventory!.cleanup.stop_maintenance.state, 'running');
    await assert.rejects(f.invoke('stop_maintenance', providers), /UNKNOWN/);
    assert.deepEqual(confirmed, [original.uid]);
    assert.equal(f.store.state.runInventory!.cleanup.stop_maintenance.deadline_unix_ms, deadline);
  });

for (const fault of ['before', 'after'] as const) test(`positive effect survives ${fault}-commit write loss without executing twice`, async t => {
  const f = fixture(t);
  f.store.fault = fault;
  await assert.rejects(f.invoke('stop_admission'), CampaignWriteOutcomeUnknownError);
  assert.deepEqual(f.calls, ['admission']);
  assert.equal(JSON.parse([...f.blobs.values()][0].bytes.toString()).phase, 'effect-confirmed');
  const receipt = await f.invoke('stop_admission');
  assert.equal(receipt.etag, `"${f.store.revision}"`);
  assert.deepEqual(f.calls, ['admission']);
});

test('a failed post-effect confirmation read is UNKNOWN and completes later without replay', async t => {
  const f = fixture(t);
  f.store.failConfirmOnce = true;
  await assert.rejects(f.invoke('stop_admission'), CampaignWriteOutcomeUnknownError);
  assert.deepEqual(f.calls, ['admission']);
  await f.invoke('stop_admission');
  assert.deepEqual(f.calls, ['admission']);
});

test('unknown effect is never replayed and recovery uses current controller while retaining producer origin', async t => {
  const f = fixture(t);
  const providers: CellStepProviders = { stop_admission: { execute: async () => { f.calls.push('effect'); throw new Error('lost effect response'); } } };
  await assert.rejects(f.invoke('stop_admission', providers), /lost effect/);
  await assert.rejects(f.invoke('stop_admission', providers), /UNKNOWN.*never replay/);
  assert.deepEqual(f.calls, ['effect']);
  f.store.state = { ...f.store.state, status: 'cleaning', fencingToken: 2, fencedAtMs: Date.now(),
    fenceReason: 'heartbeat-expired', cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index === 0 ? 'running' : 'pending',
    })) };
  const recovering: CellStepProviders = { stop_admission: { execute: async () => { throw new Error('must not execute'); },
    recover: async (inventory, _key, authority) => { await authority.assertCurrent(); f.calls.push('verified-durable-recovery'); return inventory; } } };
  const receipt = await f.invoke('stop_admission', recovering, await f.envelope('controller'));
  assert.equal(receipt.fencing_token, 1);
  assert.equal(receipt.actor.fencing_token, 2);
  assert.equal(receipt.actor.mode, 'controller');
  assert.equal(f.store.state.runInventory!.namespace.owner_token, ownerId);
  assert.equal(f.store.state.status, 'cleaning');
  f.store.state.cleanupSteps[0].status = 'complete';
  f.store.state.cleanupSteps[1].status = 'running';
  await assert.rejects(f.invoke('stop_admission', recovering, await f.envelope('controller')), /running outer step/);
});

test('handoff rejects unresolved resources even with all earlier cell receipts', async t => {
  const f = fixture(t);
  for (const selector of (Object.keys(CELL_CLEANUP_SELECTORS) as CellCleanupSelector[]).filter(value => value !== 'handoff'))
    await f.invoke(selector);
  f.store.state.runInventory!.resources[0].state = 'observed';
  await assert.rejects(f.invoke('handoff'), /exact resource removal/);
  assert.equal(f.store.state.runInventory!.cleanup.handoff.state, 'not_started');
});

test('cleanup CLI rejects unknown selectors with exactly one nonzero JSON error and no cloud access', () => {
  const result = spawnSync(process.execPath, ['--import', 'tsx', 'projects/arrowcol-oat/aks-campaign.ts', 'cleanup-step', 'unknown'], {
    encoding: 'utf8', env: { ...process.env, ARM_SUBSCRIPTION_ID: campaignId, CAMPAIGN_STORAGE_ACCOUNT: 'fixtureaccount',
      CAMPAIGN_STATE_CONTAINER: 'checkpoints' },
  });
  assert.notEqual(result.status, 0);
  const lines = result.stdout.trim().split('\n');
  assert.equal(lines.length, 1);
  assert.deepEqual(JSON.parse(lines[0]), { ok: false, code: 'invalid_or_storage_error', message: 'Unknown cleanup-step selector.' });
});

test('guarded inventory refresh pairs the full inventory and strong ETag from exactly one read without proving an unknown effect', async t => {
  const f = fixture(t), input: ReadInventoryEnvelope = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId,
    cell_run_id: cellId, actor: { mode: 'producer', owner_id: ownerId, fencing_token: 1 } };
  const env = { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' };
  const unknown: CellStepProviders = { stop_admission: { execute: async () => { throw new Error('unknown positive response'); } } };
  await assert.rejects(f.invoke('stop_admission', unknown), /unknown positive response/);
  const read = f.store.read.bind(f.store);
  let reads = 0;
  t.mock.method(f.store, 'read', async () => { reads++; return read(); });
  const result = await readCellRunInventory(f.store, input, env);
  assert.equal(reads, 1);
  assert.deepEqual(result, { ok: true, inventory: f.store.state.runInventory, etag: `"${f.store.revision}"` });
  assert.equal(result.inventory.cleanup.stop_admission.state, 'running');
  await assert.rejects(f.invoke('stop_admission', unknown), /UNKNOWN.*never replay/);
  assert.deepEqual(f.calls, []);
});

test('guarded inventory refresh rejects stale producer, wrong run/cell, environment and current controller fence', async t => {
  const f = fixture(t), input: ReadInventoryEnvelope = { schema_version: 1, campaign_id: campaignId, orchestration_run_id: campaignId,
    cell_run_id: cellId, actor: { mode: 'producer', owner_id: ownerId, fencing_token: 1 } };
  const env = { CAMPAIGN_ID: campaignId, CAMPAIGN_OWNER_ID: ownerId, CAMPAIGN_FENCING_TOKEN: '1' };
  await assert.rejects(readCellRunInventory(f.store, { ...input, cell_run_id: ownerId }, env), /requested run\/cell/);
  await assert.rejects(readCellRunInventory(f.store, { ...input, orchestration_run_id: ownerId }, env), /requested run\/cell/);
  await assert.rejects(readCellRunInventory(f.store, input, { ...env, CAMPAIGN_ID: ownerId }), /environment identity/);
  const heartbeat = f.store.state.heartbeatAtMs;
  f.store.state.heartbeatAtMs -= 180_001;
  f.store.state.startedAtMs = f.store.state.heartbeatAtMs;
  f.store.state.expiresAtMs -= 180_001;
  await assert.rejects(readCellRunInventory(f.store, input, env), /actor, immutable origin/);
  f.store.state.startedAtMs = f.store.state.heartbeatAtMs = heartbeat;
  f.store.state.expiresAtMs += 180_001;
  f.store.state = { ...f.store.state, status: 'cleaning', fencingToken: 2, fencedAtMs: Date.now(),
    fenceReason: 'heartbeat-expired', cleanupStatus: 'running', cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({
      step, idempotencyKey: `${campaignId}:2:${step}`, status: index === 0 ? 'running' : 'pending',
    })) };
  const controller = { ...input, actor: { ...input.actor, mode: 'controller' as const, fencing_token: 2 } };
  const currentEnv = { ...env, CAMPAIGN_FENCING_TOKEN: '2' };
  const result = await readCellRunInventory(f.store, controller, currentEnv);
  assert.equal(result.inventory.fencing_token, 1);
  assert.equal(result.inventory.namespace.owner_token, ownerId);
  await assert.rejects(readCellRunInventory(f.store, { ...controller, actor: { ...controller.actor, fencing_token: 1 } }, currentEnv), /actor, immutable origin/);
  assert.equal(f.store.revision, 1);
  assert.deepEqual(f.calls, []);
  assert.equal(f.blobs.size, 0);
});
