import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test, type TestContext } from 'node:test';
import { Azure, Config, runtime, TAGS } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, EffectBoundary, processEffectRequest, withEffectBoundary,
  type EffectEnvelope, type ExternalEffect } from '../../infra/azure/effect-boundary.js';
import { plan_run_fic } from '../../infra/azure/run_fic.js';
import { assessCellLifetimeBudget, CUSTODY_CLEANUP_STEPS, newCampaignState, validateCampaignState,
  type CampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { createDefaultLifecycleActions } from './aks-lifecycle.js';
import { recordUnsettledDrain } from './aks-reconcile.js';
import { createDefaultCellCleanupProviders, createDefaultCleanupProviders } from './aks-default-providers.js';
import { createExternalCleanupExecutor, type CleanupMutationAuthority, type ExternalCleanupActions } from './aks-cleanup-executor.js';

const campaignId = '11111111-1111-4111-8111-111111111111';
const ownerId = '22222222-2222-4222-8222-222222222222';
const cellId = '33333333-3333-4333-8333-333333333333';
const workloadUid = '44444444-4444-4444-8444-444444444444';
async function fixture(t: TestContext, deployment = false) {
  const c = new Config({ AZURE_SUBSCRIPTION_ID: campaignId }), azure = new Azure(c);
  const namespace = 'oat-' + cellId.replaceAll('-', ''), issuer = 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/';
  t.mock.method(azure, 'request', async () => { throw new Error('Tests may not reach cloud APIs.'); });
  t.mock.method(azure, 'arm', async (_method: string, id: string) => {
    const tags = { ...TAGS, sandboxId: c.sandbox };
    if (id === c.identity_id) return { tags, properties: { clientId: campaignId, tenantId: campaignId } };
    if (id.includes('/managedClusters/')) return { tags, properties: { fqdn: 'fixture.azmk8s.io',
      oidcIssuerProfile: { issuerURL: issuer, enabled: true }, securityProfile: { workloadIdentity: { enabled: true } } } };
    throw new Error('Unexpected mocked ARM target');
  });
  t.mock.method(azure, 'collection', async () => [{ name: 'azure-qap-' + c.cluster, properties: { issuer,
    subject: `system:serviceaccount:${c.kubernetesNamespace}:${c.kubernetesServiceAccount}`, audiences: ['api://AzureADTokenExchange'] } }]);
  const plan = await plan_run_fic(azure, cellId, namespace, 'arrowcol-oat');
  const step = { state: 'not_started' as const, deadline_unix_ms: null, reason_code: null };
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: campaignId, cell_run_id: cellId, load_admission: 'authorized',
    planned_cell_duration_ms: 60_000, effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    namespace: { name: namespace, uid: cellId, owner_token: ownerId },
    resources: [{ api_version: deployment ? 'apps/v1' : 'batch/v1', kind: deployment ? 'Deployment' : 'Job', namespace, name: 'maintenance', uid: workloadUid,
      role: 'maintenance', state: 'observed', absence: null }],
    fic: { resource_id: plan.resourceId, name: plan.credentialName, state: 'observed', absence: null, plan }, qap: null,
    evidence_destination: { account: c.account, container: c.checkpoints, blob_prefix: 'evidence/' + cellId },
    cleanup: { stop_admission: step, bounded_drain: step, evidence_capture: step,
      stop_maintenance: { state: 'running', deadline_unix_ms: Date.now() + 120_000, reason_code: null },
      terminal_capture: step, handoff: step } };
  let state: CampaignState = { ...newCampaignState(campaignId, ownerId), mode: 'armed', runInventory: inventory };
  let revision = 1;
  const store: CampaignStore = { read: async () => ({ state: structuredClone(state), etag: `"inventory-${revision}"` }),
    write: async (next, expected) => { assert.equal(expected, `"inventory-${revision}"`);
      state = structuredClone(validateCampaignState(next)); return `"inventory-${++revision}"`; } };
  const authority: CleanupMutationAuthority = { mode: 'producer', campaignId, ownerId, fencingToken: 1, assertCurrent: async () => {
    assert.equal(state.fencingToken, 1, 'fenced actors may not act');
  } };
  const blobs = new Map<string, { bytes: Buffer; etag: string }>();
  const effectCalls: ExternalEffect[] = [];
  let fault: 'none' | 'patch_unknown' | 'completion_unknown' = 'none', patched = false, patchCount = 0, wrongUid = false;
  let onPatch: (() => void) | undefined, onIntent: (() => void) | undefined;
  let podShape: 'regular' | 'init_sidecar' | 'missing' | 'ephemeral' | 'terminal_auxiliary' = 'regular';
  const timeouts: number[] = [];
  t.mock.method(azure, 'blob', async (...args: Parameters<Azure['blob']>) => {
    const [method, container, name = '', , body, headers] = args;
    const effect: ExternalEffect = { transport: 'campaign-blob', method: method.toUpperCase(),
      resource: `https://${c.account}.blob.core.windows.net/${container}`,
      path: `/${container}/${name.split('/').map(encodeURIComponent).join('/')}`, ...(body !== undefined ? { body } : {}) };
    effectCalls.push(effect);
    return dispatchExternalEffect(effect, async () => {
      assert.equal(container, c.checkpoints);
      const previous = blobs.get(name);
      if (method === 'GET') return [previous?.bytes ?? null, previous ? { etag: previous.etag } : {}];
      assert.equal(method, 'PUT');
      assert.equal(headers?.['If-Match'] ?? headers?.['If-None-Match'], previous?.etag ?? '*');
      const bytes = Buffer.from(String(body)), etag = `"${blobs.size}-${previous?.etag.length ?? 0}-${bytes.length}"`;
      blobs.set(name, { bytes, etag });
      if (JSON.parse(bytes.toString()).phase === 'intent') onIntent?.();
      if (fault === 'completion_unknown' && JSON.parse(bytes.toString()).phase === 'complete')
        throw new Error('Lost completed journal response');
      return [null, { ':status': '201', etag }];
    });
  });
  const marks = { 'arrowcol-oat/run-id': cellId, 'arrowcol-oat/owner-token': ownerId };
  const metadata = (name: string, uid: string) => ({ name, namespace, uid, resourceVersion: patched ? '2' : '1',
    generation: patched ? 2 : 1, labels: marks, annotations: marks });
  const job = () => ({ apiVersion: deployment ? 'apps/v1' : 'batch/v1', kind: deployment ? 'Deployment' : 'Job',
    metadata: metadata('maintenance', wrongUid ? ownerId : workloadUid),
    spec: { ...(deployment ? { replicas: patched ? 0 : 1 } : { parallelism: 1, suspend: patched }), selector: { matchLabels: { job: 'maintenance' } } },
    status: { observedGeneration: patched ? 2 : 1, conditions: patched ? [{ type: 'Suspended', status: 'True' }] : [] } });
  t.mock.method(runtime, 'run_tree', async (args: string[], timeout?: number) => {
    effectCalls.push(processEffectRequest(args));
    timeouts.push(timeout ?? -1);
    if (args.includes('patch')) {
      patchCount++;
      assert.equal(JSON.parse([...blobs.values()][0]!.bytes.toString()).phase, 'intent', 'durable intent must precede PATCH');
      const patch = JSON.parse(args[args.indexOf('--patch') + 1]!);
      assert.deepEqual(patch.slice(0, 2), [{ op: 'test', path: '/metadata/uid', value: workloadUid },
        { op: 'test', path: '/metadata/resourceVersion', value: '1' }]);
      patched = true;
      onPatch?.();
      if (fault === 'patch_unknown') throw new Error('Lost PATCH response');
      return { returncode: 0, stdout: JSON.stringify(job()), stderr: '' };
    }
    if (args.includes('logs')) return { returncode: 0,
      stdout: 'secret=must-not-be-persisted\n2026-01-01T00:00:00Z {"rows":9,"token":"must-not-be-persisted"}\n', stderr: '' };
    const kind = args[args.indexOf('get') + 1];
    let response: unknown;
    if (kind === 'namespace') response = { apiVersion: 'v1', kind: 'Namespace', metadata: metadata(namespace, cellId) };
    else if (kind === 'job.batch' || kind === 'deployment.apps') response = job();
    else if (kind === 'replicaset.apps') response = { kind: 'ReplicaSetList', metadata: { resourceVersion: '1' }, items: [{
      metadata: { uid: campaignId, ownerReferences: [{ uid: workloadUid, kind: 'Deployment', controller: true }] },
    }] };
    else if (kind === 'horizontalpodautoscalers.autoscaling') response = { kind: 'HorizontalPodAutoscalerList', metadata: { resourceVersion: '1' }, items: [] };
    else if (kind === 'pods') response = { kind: 'PodList', metadata: { resourceVersion: '1' }, items: [{
      apiVersion: 'v1', kind: 'Pod', metadata: { ...metadata('maintenance-pod', ownerId),
        ownerReferences: [{ uid: deployment ? campaignId : workloadUid, kind: deployment ? 'ReplicaSet' : 'Job', controller: true }] },
      spec: { containers: [{ name: 'main' }, ...(podShape === 'missing' ? [{ name: 'unobserved-writer' }] : [])],
        ...(['init_sidecar', 'terminal_auxiliary'].includes(podShape) ? { initContainers: [{ name: 'sidecar', restartPolicy: 'Always' }] } : {}),
        ...(['ephemeral', 'terminal_auxiliary'].includes(podShape) ? { ephemeralContainers: [{ name: 'debug-writer' }] } : {}) },
      status: { containerStatuses: [{ name: 'main', state: patched ? { terminated: { exitCode: 0, finishedAt: new Date().toISOString() } } : { running: {} } }],
        ...(podShape === 'init_sidecar' ? { initContainerStatuses: [{ name: 'sidecar', state: { running: {} } }] } : {}),
        ...(podShape === 'ephemeral' ? { ephemeralContainerStatuses: [{ name: 'debug-writer', state: { running: {} } }] } : {}),
        ...(podShape === 'terminal_auxiliary' ? {
          initContainerStatuses: [{ name: 'sidecar', state: { terminated: { exitCode: 0, finishedAt: new Date().toISOString() } } }],
          ephemeralContainerStatuses: [{ name: 'debug-writer', state: { terminated: { exitCode: 0, finishedAt: new Date().toISOString() } } }],
        } : {}) },
    }] };
    else throw new Error('Unexpected mock kubectl command: ' + args.join(' '));
    return { returncode: 0, stdout: JSON.stringify(response), stderr: '' };
  });
  const controller = async () => {
    const now = Date.now();
    inventory.load_admission = 'not_authorized';
    inventory.resources[0]!.role = 'collector';
    inventory.cleanup.stop_admission = { ...step, state: 'complete' };
    inventory.cleanup.bounded_drain = { state: 'running', deadline_unix_ms: now - 1000, reason_code: null };
    inventory.cleanup.stop_maintenance = { ...step, state: 'complete' };
    inventory.cleanup.evidence_capture = { ...step, state: 'complete' };
    inventory.cleanup.terminal_capture = { state: 'running', deadline_unix_ms: now + 120_000, reason_code: null };
    state = { ...newCampaignState(campaignId, ownerId, now - 300_000), mode: 'armed', status: 'cleaning',
      fencingToken: 2, fencedAtMs: now - 120_000, fenceReason: 'heartbeat-expired', cleanupStatus: 'running', runInventory: inventory,
      cleanupSteps: CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step, idempotencyKey: `${campaignId}:2:${step}`,
        status: index === 0 ? 'complete' : 'pending' })) };
    await recordUnsettledDrain(store, ownerId, 2, now);
    state.cleanupSteps = state.cleanupSteps.map((value, index) => index === 1 ? value : {
      ...value, status: index < 4 ? 'complete' : index === 4 ? 'running' : 'pending' });
    const guarded: CleanupMutationAuthority = { mode: 'controller', campaignId, ownerId, fencingToken: 2,
      assertCurrent: async () => { assert.equal(state.fencingToken, 2, 'actor changed'); } };
    return { inventory: state.runInventory!, authority: guarded };
  };
  return { azure, store, inventory, authority, blobs, timeouts, effectCalls, actions: createDefaultLifecycleActions(store, azure), controller,
    onPatch: (value: () => void) => { onPatch = value; }, onIntent: (value: () => void) => { onIntent = value; },
    alreadyStopped: () => { patched = true; }, touchVersion: () => { revision++; },
    setPodShape: (value: typeof podShape) => { podShape = value; },
    fence: () => { state.fencingToken++; }, get state() { return state; },
    get patchCount() { return patchCount; }, setFault: (value: typeof fault) => { fault = value; }, wrongUid: () => { wrongUid = true; } };
}
function envelopeFromTrace(f: Awaited<ReturnType<typeof fixture>>, calls: ExternalEffect[]): EffectEnvelope {
  const grouped = new Map<string, { request: ExternalEffect; count: number }>();
  for (const request of calls) {
    const key = [request.transport, request.method.toUpperCase(), request.resource, request.path].join('\0');
    const entry = grouped.get(key);
    if (entry) entry.count++;
    else grouped.set(key, { request, count: 1 });
  }
  return { schema_version: 1, campaign_id: campaignId, run_id: cellId, subscription_id: f.azure.c.subscription,
    resource_group: f.azure.c.group, storage_account: f.azure.c.account, state_container: f.azure.c.checkpoints,
    effects: [...grouped.values()].map(({ request, count }, index) => ({ id: `effect-${index}`,
      transport: request.transport, method: request.method.toUpperCase(), resource: request.resource,
      path: request.path, max_calls: count })) };
}
test('default lifecycle confirms exact Job suspension, physical stop and sanitized actual terminal/log receipts', async t => {
  const f = await fixture(t);
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.' });
  const selectors = createDefaultCellCleanupProviders(defaults);
  for (const selector of Object.values(selectors)) {
    assert.equal(typeof selector.execute, 'function');
    assert.equal(typeof selector.recover, 'function');
  }
  assert.equal(await defaults.stopMaintenanceIfUidAndOwnerMatch(f.inventory.resources[0]!, ownerId, 'key', f.authority), 'complete');
  const journal = [...f.blobs.values()][0]!.bytes.toString();
  assert.equal(f.patchCount, 1);
  assert.equal(JSON.parse(journal).terminal_pods[0].containers[0].exit_code, 0);
  assert.ok(!journal.includes('must-not-be-persisted'));
  assert.ok(JSON.parse(journal).logs.some((value: { numeric_samples: unknown[] }) => value.numeric_samples.length === 1));
  assert.equal(await f.actions.recoverLifecycle!('stop_maintenance', f.inventory, f.inventory.cleanup.stop_maintenance.deadline_unix_ms, 'key', f.authority), 'complete');
  assert.equal(f.patchCount, 1, 'confirmed recovery must never PATCH again');
});
test('default cell terminal capture preserves the active producer path when the scale journal is empty', async t => {
  const f = await fixture(t);
  let captures = 0;
  const lifecycle = { ...f.actions, captureTerminalState: async () => { captures++; return 'complete' as const; } };
  const defaults = createDefaultCleanupProviders(f.store, { azure: f.azure, workDirectory: '.', lifecycle });
  const cellProviders = createDefaultCellCleanupProviders({ ...defaults,
    discoverExactInventory: async inventory => structuredClone(inventory) });
  await cellProviders.terminal_capture!.execute(f.inventory, 'terminal-capture', f.authority);
  assert.equal(captures, 1);
  assert.equal(f.state.scaleTrials, undefined);
  assert.equal(f.state.status, 'active');
});

test('default lifecycle unknown PATCH/empty recovery journals never dispatch an execute fallback', async t => {
  const f = await fixture(t);
  await assert.rejects(f.actions.recoverLifecycle!('stop_maintenance', f.inventory, null, 'key', f.authority), /no durable/);
  f.setFault('patch_unknown');
  await assert.rejects(f.actions.stopMaintenanceIfUidAndOwnerMatch(f.inventory.resources[0]!, ownerId, 'key', f.authority), /Lost PATCH/);
  await assert.rejects(f.actions.recoverLifecycle!('stop_maintenance', f.inventory, null, 'key', f.authority), /outcome UNKNOWN/);
  assert.equal(f.patchCount, 1);
});
test('default lifecycle resumes a positively persisted completed response without replaying and rejects UID replacements', async t => {
  const f = await fixture(t); f.setFault('completion_unknown');
  await assert.rejects(f.actions.stopMaintenanceIfUidAndOwnerMatch(f.inventory.resources[0]!, ownerId, 'key', f.authority), /Lost completed/);
  f.setFault('none');
  assert.equal(await f.actions.recoverLifecycle!('stop_maintenance', f.inventory, null, 'key', f.authority), 'complete');
  assert.equal(f.patchCount, 1);
});
test('default lifecycle rejects a different workload UID before any intent or PATCH', async t => {
  const f = await fixture(t); f.wrongUid();
  await assert.rejects(f.actions.stopMaintenanceIfUidAndOwnerMatch(f.inventory.resources[0]!, ownerId, 'key', f.authority), /UID/);
  assert.equal(f.patchCount, 0); assert.equal(f.blobs.size, 0);
});
test('controller terminal capture stops a residual collector after drain expiry without reopening drain or deleting resources', async t => {
  const f = await fixture(t, true), current = await f.controller();
  const deadline = current.inventory.cleanup.terminal_capture.deadline_unix_ms;
  assert.equal(await f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), 'complete');
  assert.equal(f.patchCount, 1);
  assert.equal(f.state.runInventory!.cleanup.bounded_drain.state, 'uncertain');
  assert.equal(f.state.cleanupSteps[1]!.status, 'unsettled');
  assert.equal(f.state.runInventory!.cleanup.terminal_capture.deadline_unix_ms, deadline);
  assert.equal(await f.actions.recoverLifecycle!('terminal_capture', current.inventory, deadline, 'terminal', current.authority), 'complete');
  assert.equal(f.patchCount, 1);
});
test('unknown residual PATCH is recovered only by positive exact stopped state through the guarded adapter', async t => {
  const dry = await fixture(t);
  const exercise = async (value: Awaited<ReturnType<typeof fixture>>) => {
    const current = await value.controller(), deadline = current.inventory.cleanup.terminal_capture.deadline_unix_ms;
    value.setFault('patch_unknown');
    await assert.rejects(value.actions.captureTerminalState(current.inventory, 'terminal', current.authority), /Lost PATCH/);
    value.setFault('none');
    const restartedActions = createDefaultLifecycleActions(value.store, value.azure);
    assert.equal(await restartedActions.recoverLifecycle!('terminal_capture', current.inventory, deadline, 'terminal', current.authority), 'complete');
    assert.equal(value.patchCount, 1);
    const stops = [...value.blobs.values()].map(blob => JSON.parse(blob.bytes.toString())).filter(blob => blob.phase === 'complete');
    assert.equal(stops[0].response.source, 'exact_state_recovery');
    assert.equal(stops[0].deadline_unix_ms, deadline);
  };
  await exercise(dry);
  assert.ok(dry.effectCalls.some(request => request.transport === 'process' && request.method === 'KUBECTL_PATCH'));
  const envelope = envelopeFromTrace(dry, dry.effectCalls);
  t.mock.restoreAll();
  const f = await fixture(t);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-lifecycle-effect-boundary-'));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const receiptPath = path.join(directory, 'effects.jsonl');
  await withEffectBoundary(new EffectBoundary(envelope, receiptPath), () => exercise(f));
  const receipts = fs.readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const patch = receipts.filter(row => row.transport === 'process' && row.method === 'KUBECTL_PATCH');
  assert.deepEqual(patch.map(row => row.event), ['dispatch', 'result']);
  assert.equal(patch[1].outcome, 'unknown_after_forward');
  assert.equal(f.patchCount, 1);
});
for (const shape of ['init_sidecar', 'missing', 'ephemeral'] as const) {
  test(`terminal recovery and freeze refuse incomplete physical stop: ${shape}`, async t => {
    const f = await fixture(t, true), current = await f.controller();
    f.setFault('patch_unknown');
    await assert.rejects(f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), /Lost PATCH/);
    f.setFault('none'); f.setPodShape(shape);
    await assert.rejects(f.actions.recoverLifecycle!('terminal_capture', current.inventory, null, 'terminal', current.authority),
      shape === 'missing' ? /container coverage is UNKNOWN/ : /no positive exact stopped state/);
    assert.equal(f.patchCount, 1, 'incomplete observations must not authorize UNKNOWN PATCH replay');
    assert.ok(![...f.blobs.keys()].some(blob => blob.endsWith('/terminal_capture.json')));
    f.setPodShape('regular');
    assert.equal(await f.actions.recoverLifecycle!('terminal_capture', current.inventory, null, 'terminal', current.authority), 'complete');
    f.setPodShape(shape);
    await assert.rejects(f.actions.recoverLifecycle!('terminal_capture', current.inventory, null, 'terminal', current.authority),
      shape === 'missing' ? /container coverage is UNKNOWN/ : /no longer physically stopped/);
    assert.equal(f.patchCount, 1);
  });
}
test('terminal freeze shares complete container proof for maintenance writers', async t => {
  const f = await fixture(t, true), current = await f.controller();
  assert.equal(await f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), 'complete');
  f.state.runInventory!.resources[0]!.role = 'maintenance';
  f.setPodShape('ephemeral');
  await assert.rejects(f.actions.captureTerminalState(current.inventory, 'terminal', current.authority),
    /physical writer stop/);
  assert.equal(f.patchCount, 1);
});
test('positive terminal proof retains observed regular, init-sidecar and ephemeral container receipts', async t => {
  const f = await fixture(t, true), current = await f.controller();
  f.alreadyStopped(); f.setPodShape('terminal_auxiliary');
  assert.equal(await f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), 'complete');
  const stop = [...f.blobs.values()].map(value => JSON.parse(value.bytes.toString())).find(value => value.phase === 'complete');
  assert.deepEqual(stop.terminal_pods[0].containers.map((value: { name: string }) => value.name), ['main', 'sidecar', 'debug-writer']);
  assert.equal(f.patchCount, 0);
});
test('expired terminal budget cannot admit a residual collector stop or reset its persisted deadline', async t => {
  const f = await fixture(t), current = await f.controller(), deadline = current.inventory.cleanup.terminal_capture.deadline_unix_ms!;
  t.mock.method(Date, 'now', () => deadline + 1);
  await assert.rejects(f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), /deadline expired/);
  assert.equal(f.patchCount, 0);
  assert.equal(f.state.runInventory!.cleanup.terminal_capture.deadline_unix_ms, deadline);
});
test('already stopped residual Deployment is proved without another PATCH and missing response is never inferred as a drain success', async t => {
  const f = await fixture(t, true), current = await f.controller(); f.alreadyStopped();
  assert.equal(await f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), 'complete');
  assert.equal(f.patchCount, 0);
  const stop = [...f.blobs.values()].map(value => JSON.parse(value.bytes.toString())).find(value => value.phase === 'complete');
  assert.equal(stop.response.source, 'already_stopped_observation');
  assert.equal(f.state.runInventory!.cleanup.bounded_drain.state, 'uncertain');
});
test('residual stop retains genuine late response evidence, leaves terminal pending and cannot reset budget on restart', async t => {
  const f = await fixture(t), current = await f.controller(), deadline = current.inventory.cleanup.terminal_capture.deadline_unix_ms!;
  const originalNow = Date.now;
  let now = originalNow();
  t.mock.method(Date, 'now', () => now);
  f.onPatch(() => { now = deadline + 1; });
  assert.equal(await f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), 'pending');
  const stop = [...f.blobs.values()].map(value => JSON.parse(value.bytes.toString())).find(value => value.phase === 'responded');
  assert.equal(stop.response.source, 'patch_response');
  assert.equal(stop.deadline_unix_ms, deadline);
  await assert.rejects(f.actions.recoverLifecycle!('terminal_capture', current.inventory, deadline, 'terminal', current.authority), /deadline expired/);
  assert.equal(f.patchCount, 1);
});
test('residual stop refuses a fencing race between durable intent and PATCH', async t => {
  const f = await fixture(t), current = await f.controller(); f.onIntent(f.fence);
  await assert.rejects(f.actions.captureTerminalState(current.inventory, 'terminal', current.authority), /actor changed/);
  assert.equal(f.patchCount, 0);
});
test('delayed unbounded cleanup remains pending and leaves lifetime reserve UNKNOWN', async t => {
  const f = await fixture(t);
  await f.controller();
  f.state.runInventory!.cleanup.evidence_capture = { state: 'not_started', deadline_unix_ms: null, reason_code: null };
  f.state.cleanupSteps = CUSTODY_CLEANUP_STEPS.map((step, index) => ({ step,
    idempotencyKey: `${campaignId}:2:${step}`, status: index < 2 ? 'complete' : index === 2 ? 'running' : 'pending' }));
  let settled = false;
  const complete = async () => 'complete' as const;
  const actions: ExternalCleanupActions = {
    discoverExactInventory: async inventory => inventory,
    stopAdmission: complete, boundedDrain: complete,
    preserveEvidence: async () => { await new Promise(resolve => setTimeout(resolve, 20)); settled = true; return 'pending'; },
    stopMaintenanceIfUidAndOwnerMatch: complete, captureTerminalState: complete,
    deleteNamespaceIfUidAndOwnerMatch: complete, deleteQapBindingIfOwned: complete, deleteFicIfExact: complete,
    restoreSafeIdle: complete, verifySafeIdle: complete,
  };
  const execute = createExternalCleanupExecutor(f.store, actions,
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  assert.equal(await execute('preserve-evidence', f.state, `${campaignId}:2:preserve-evidence`), 'pending');
  assert.equal(settled, true);
  const saved = (await f.store.read())!.state.runInventory!.cleanup.evidence_capture;
  assert.equal(saved.state, 'pending');
  assert.equal(saved.deadline_unix_ms, null);
  assert.equal(assessCellLifetimeBudget(6 * 60 * 60 * 1000, 0, 5_100_000).status, 'unknown');
});

test('controller executor pins inventory ETag across residual stop intent even when the actor and origin are unchanged', async t => {
  const f = await fixture(t), current = await f.controller();
  f.state.runInventory!.cleanup.terminal_capture.state = 'not_started';
  f.onIntent(f.touchVersion);
  const forbidden = async (): Promise<never> => { throw new Error('Unrelated effect must not run.'); };
  const actions: ExternalCleanupActions = { ...f.actions, discoverExactInventory: async inventory => inventory,
    deleteNamespaceIfUidAndOwnerMatch: forbidden, deleteQapBindingIfOwned: forbidden, deleteFicIfExact: forbidden,
    restoreSafeIdle: forbidden, verifySafeIdle: forbidden };
  const execute = createExternalCleanupExecutor(f.store, actions,
    { maintenanceGraceSeconds: 900, maintenanceStopTimeoutMs: 1_200_000, budget_source: 'effective_stop_budgets' });
  await assert.rejects(execute('record-terminal-state', f.state, `${campaignId}:2:record-terminal-state`), /fencing changed/);
  assert.equal(f.patchCount, 0);
});
