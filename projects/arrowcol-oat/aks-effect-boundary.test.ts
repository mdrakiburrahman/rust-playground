import assert from 'node:assert/strict';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { mkdtempSync, readFileSync, rmSync, chmodSync, writeFileSync, writeSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { stringify } from 'yaml';
import { Azure, Config, exchange_workload_identity_assertion, runtime } from '../../infra/azure/bootstrap.js';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { EffectBoundary, EffectBoundaryRefused, ResponseWithholdingSupervisor, dispatchExternalEffect, withEffectBoundary,
  type EffectEnvelope, type EffectRule } from '../../infra/azure/effect-boundary.js';
import { createBlobCampaignStore, managedIdentityToken, publishRunInventory, readArmOperationStatus, startCampaign,
  type CampaignState, type CampaignStore } from './aks-campaign.js';
import { isCellCleanupSelector } from './aks-cell-cleanup.js';
import { boundedEffectRehearsal, type BoundedEffectRehearsalDependencies } from './aks-effect-rehearsal.js';
import { createSyntheticArmedCampaignState } from './aks-rehearsal.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const runId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = '68fa0545-661c-4152-93f2-fc8ae1c231b8';
const stamp = { subscription_id: subscriptionId, resource_group: 'arrow-collector-rg',
  storage_account: 'arrowcollector68590', state_container: 'checkpoints' };
function tempDir(): string {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-effect-boundary-'));
  chmodSync(directory, 0o700);
  return directory;
}
function envelope(effects: EffectRule[]): EffectEnvelope {
  return { schema_version: 1, campaign_id: campaignId, run_id: runId, ...stamp, effects };
}
function rule(id: string, transport: EffectRule['transport'], method: string, resource: string, pathValue: string,
  max_calls = 1): EffectRule {
  return { id, transport, method, resource, path: pathValue, max_calls };
}

function inMemoryCampaignStore(initial: CampaignState): CampaignStore {
  let state = initial;
  let revision = 0;
  return {
    async read() { return { state, etag: String(revision) }; },
    async write(next, expectedEtag) {
      if (expectedEtag !== String(revision)) throw new Error('in-memory campaign CAS conflict');
      state = next;
      revision++;
      return String(revision);
    },
  };
}


function emptyCampaignStore(): CampaignStore {
  let current: { state: CampaignState; etag: string } | undefined;
  let revision = 0;
  return {
    async read() { return current && structuredClone(current); },
    async write(state, expectedEtag) {
      if (expectedEtag !== current?.etag) throw new Error('in-memory campaign CAS conflict');
      const etag = String(++revision);
      current = { state: structuredClone(state), etag };
      return etag;
    },
  };
}

function initialRunInventory(ownerId: string): Record<string, unknown> {
  const step = { state: 'not_started', deadline_unix_ms: null, reason_code: null };
  const namespace = 'arrowcol-' + runId.replace(/-/g, '');
  return {
    schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: 'c0ffee00-0000-4000-8000-000000000003', cell_run_id: runId,
    load_admission: 'not_authorized', planned_cell_duration_ms: 60_000,
    effective_stop_budgets: { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_000 },
    drain_budget: { budget_ms: 120_000, collector_grace_ms: 300_000, pre_stop_capture_margin_ms: 30_000 },
    namespace: { name: namespace, uid: null, owner_token: ownerId },
    resources: ['arrowcol', 'arrowcol-client', 'delta-maintenance'].map((name, index) => ({
      api_version: 'apps/v1', kind: 'Deployment', namespace, name, uid: null, absence: null, state: 'planned',
      role: index === 0 ? 'collector' : index === 1 ? 'client' : 'maintenance',
    })),
    fic: { resource_id: '/subscriptions/' + subscriptionId + '/resourceGroups/perf69/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector/federatedIdentityCredentials/arrowcol-run-' + runId,
      name: 'arrowcol-run-' + runId, state: 'planned', absence: null },
    qap: { database: 'perf69-qap', bindings: [{ external_table: 'arrowcol_data', relative_path: 'perf69/' + runId }],
      state: 'planned', absence: [] },
    cleanup: { stop_admission: { ...step }, bounded_drain: { ...step }, evidence_capture: { ...step },
      stop_maintenance: { ...step }, terminal_capture: { ...step }, handoff: { ...step } },
  };
}

function writeRehearsalFixture(directory: string, ownerId: string, cellRunId: string) {
  const stampPath = path.join(directory, 'stamp.yaml'), envPath = path.join(directory, 'campaign.env');
  const allocationPath = path.join(directory, 'allocation.json'), receiptPath = path.join(directory, 'effects.jsonl');
  const inventoryPath = path.join(directory, 'inventory.json');
  writeFileSync(stampPath, stringify(defaultStampConfig({ AZURE_SUBSCRIPTION_ID: subscriptionId,
    AZURE_STAMP: '12345', AZURE_LOCATION: 'canadacentral' })), { mode: 0o600 });
  writeFileSync(envPath, [
    'AZURE_SUBSCRIPTION_ID=' + subscriptionId, 'ARM_SUBSCRIPTION_ID=' + subscriptionId,
    'CAMPAIGN_STORAGE_ACCOUNT=arrowcollector12345', 'CAMPAIGN_STATE_CONTAINER=checkpoints',
    'CAMPAIGN_ID=' + campaignId, 'CAMPAIGN_OWNER_ID=' + ownerId, 'CAMPAIGN_FENCING_TOKEN=1',
  ].join('\n'), { mode: 0o600 });
  const account = 'arrowcollector12345';
  writeFileSync(allocationPath, JSON.stringify({ schema_version: 1, campaign_id: campaignId, run_id: cellRunId,
    subscription_id: subscriptionId, resource_group: 'arrow-collector-rg', storage_account: account,
    state_container: 'checkpoints', effects: [rule('unused', 'campaign-blob', 'GET',
      'https://' + account + '.blob.core.windows.net/checkpoints', '/checkpoints/not-active.json')] }), { mode: 0o600 });
  writeFileSync(inventoryPath, '{}', { mode: 0o600 });
  return { stampPath, envPath, allocationPath, receiptPath, inventoryPath };
}

test('exact effect allocations refuse target/method changes before forwarding and persist attributable receipts', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const receipts = path.join(directory, 'effects.jsonl');
  const boundary = new EffectBoundary(envelope([rule('read-active', 'campaign-blob', 'GET',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/active.json')]), receipts);
  let forwarded = 0;
  await withEffectBoundary(boundary, async () => {
    await dispatchExternalEffect({ transport: 'campaign-blob', method: 'GET',
      resource: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', path: '/checkpoints/active.json' }, async () => {
      forwarded++; return { status: 404 };
    });
    await assert.rejects(dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
      resource: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', path: '/checkpoints/active.json' }, async () => {
      forwarded++; return { status: 201 };
    }), /refused before forwarding/);
    await assert.rejects(dispatchExternalEffect({ transport: 'campaign-blob', method: 'GET',
      resource: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', path: '/checkpoints/other.json' }, async () => {
      forwarded++; return { status: 200 };
    }), /refused before forwarding/);
  });
  assert.equal(forwarded, 1);
  const rows = readFileSync(receipts, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(rows.map(row => row.event), ['dispatch', 'result', 'refused', 'refused']);
  assert.ok(rows.every(row => row.campaign_id === campaignId && row.run_id === runId));
  assert.equal(rows[1].status, 404);
});

test('effect envelopes reject missing identities and ambiguous duplicate allocations', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const receiptPath = path.join(directory, 'effects.jsonl');
  const allocation = rule('active-read', 'campaign-blob', 'GET',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/active.json');
  const valid = envelope([allocation]);
  assert.throws(() => new EffectBoundary({ ...valid, run_id: undefined }, receiptPath), /identity or schema is invalid/);
  assert.throws(() => new EffectBoundary(envelope([allocation, { ...allocation, id: 'active-read-copy' }]), receiptPath),
    /duplicate or ambiguous rule identities/);
});

test('lost response consumes the durable one-forward allocation across boundary restart', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const receipts = path.join(directory, 'effects.jsonl');
  const effects = [rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/_arrowcol-custody/campaigns/' + campaignId + '/baseline.json', 2)];
  let providerWrites = 0;
  const first = new EffectBoundary(envelope(effects), receipts);
  await assert.rejects(withEffectBoundary(first, () => dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
    resource: effects[0]!.resource, path: effects[0]!.path, body: '{"baseline":true}' }, async () => {
    providerWrites++;
    throw new Error('simulated lost acknowledgement after provider write');
  })), /simulated lost acknowledgement/);
  const restarted = new EffectBoundary(envelope(effects), receipts);
  await assert.rejects(withEffectBoundary(restarted, () => dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
    resource: effects[0]!.resource, path: effects[0]!.path, body: '{"baseline":true}' }, async () => {
    providerWrites++;
    return { status: 201 };
  })), /refused before forwarding/);
  assert.equal(providerWrites, 1);
  const rows = readFileSync(receipts, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(rows.map(row => row.event), ['dispatch', 'result', 'refused']);
  assert.equal(rows[1].outcome, 'unknown_after_forward');
  assert.match(rows[0].body_sha256, /^[0-9a-f]{64}$/);
  assert.equal(JSON.stringify(rows).includes('baseline":true'), false);
});

test('a bounded mutation sequence resumes only after a durably confirmed result', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const receipts = path.join(directory, 'effects.jsonl');
  const effect = rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/baseline.json', 2);
  const allocation = envelope([effect]);
  let writes = 0;
  const first = new EffectBoundary(allocation, receipts);
  await withEffectBoundary(first, () => dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
    resource: effect.resource, path: effect.path, body: '{"phase":"intent"}' }, async () => {
    writes++; return { status: 201 };
  }));
  const restarted = new EffectBoundary(allocation, receipts);
  await withEffectBoundary(restarted, () => dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
    resource: effect.resource, path: effect.path, body: '{"phase":"complete"}' }, async () => {
    writes++; return { status: 201 };
  }));
  await assert.rejects(withEffectBoundary(restarted, () => dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT',
    resource: effect.resource, path: effect.path, body: '{"phase":"extra"}' }, async () => {
    writes++; return { status: 201 };
  })), /refused before forwarding/);
  assert.equal(writes, 2);
});

test('campaign Blob adapter is guarded and refuses a second exact read after its allocation is spent', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const resource = 'https://arrowcollector68590.blob.core.windows.net/checkpoints';
  const pathValue = '/checkpoints/active.json';
  const boundary = new EffectBoundary(envelope([rule('active-read', 'campaign-blob', 'GET', resource, pathValue)]),
    path.join(directory, 'effects.jsonl'));
  let fetches = 0;
  const store = createBlobCampaignStore({ account: stamp.storage_account, container: stamp.state_container,
    subscriptionId, recordPath: 'active.json' }, async () => {
      fetches++; return new Response(null, { status: 404 });
    }, async () => 'simulated-test-token');
  await withEffectBoundary(boundary, async () => {
    assert.equal(await store.read(), undefined);
    await assert.rejects(store.read(), /refused before forwarding/);
  });
  assert.equal(fetches, 1);
  const refused = readFileSync(path.join(directory, 'effects.jsonl'), 'utf8').trim().split('\n')
    .map(line => JSON.parse(line)).filter(row => row.event === 'refused');
  assert.equal(refused.length, 1);
});

test('bounded rehearsal constructs default campaign providers inside the fail-closed boundary', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const stampPath = path.join(directory, 'stamp.yaml'), envPath = path.join(directory, 'campaign.env');
  const allocationPath = path.join(directory, 'allocation.json'), receiptPath = path.join(directory, 'effects.jsonl');
  writeFileSync(stampPath, stringify(defaultStampConfig({ AZURE_SUBSCRIPTION_ID: subscriptionId,
    AZURE_STAMP: '12345', AZURE_LOCATION: 'canadacentral' })), { mode: 0o600 });
  writeFileSync(envPath, [
    'AZURE_SUBSCRIPTION_ID=' + subscriptionId, 'ARM_SUBSCRIPTION_ID=' + subscriptionId,
    'CAMPAIGN_STORAGE_ACCOUNT=arrowcollector12345', 'CAMPAIGN_STATE_CONTAINER=checkpoints',
    'CAMPAIGN_ID=' + campaignId, 'CAMPAIGN_OWNER_ID=' + runId, 'CAMPAIGN_FENCING_TOKEN=1',
  ].join('\n'), { mode: 0o600 });
  const allocation = { schema_version: 1, campaign_id: campaignId, run_id: runId,
    subscription_id: subscriptionId, resource_group: 'arrow-collector-rg', storage_account: 'arrowcollector12345',
    state_container: 'checkpoints', effects: [rule('unrelated', 'campaign-blob', 'GET',
      'https://arrowcollector12345.blob.core.windows.net/checkpoints', '/checkpoints/not-active.json')] };
  writeFileSync(allocationPath, JSON.stringify(allocation), { mode: 0o600 });
  const boundary = new EffectBoundary(allocation, receiptPath);
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response(null, { status: 200 }); });
  await assert.rejects(withEffectBoundary(boundary, () => boundedEffectRehearsal([
    '--mode', 'watchdog-once', '--env-file', envPath, '--stamp-config', stampPath,
    '--effect-envelope', allocationPath, '--receipt-file', receiptPath,
  ])), /refused before forwarding/);
  assert.equal(fetches, 0);
  const rows = readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(rows.map(row => row.event), ['refused']);
});

test('producer rehearsal dispatches an accepted underscore cleanup selector', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const state = createSyntheticArmedCampaignState(campaignId, runId);
  const cellRunId = state.runInventory!.cell_run_id;
  const fixture = writeRehearsalFixture(directory, runId, cellRunId);
  const store = inMemoryCampaignStore(state);
  let dispatched: string | undefined;
  const result = await boundedEffectRehearsal([
    '--mode', 'producer-cleanup-step', '--selector', 'stop_admission', '--env-file', fixture.envPath,
    '--stamp-config', fixture.stampPath, '--effect-envelope', fixture.allocationPath,
    '--receipt-file', fixture.receiptPath, '--inventory-file', fixture.inventoryPath,
  ], { store, campaignMain: async (args) => {
    assert.equal(args[0], 'cleanup-step');
    assert.equal(args[1], 'stop_admission');
    assert.ok(isCellCleanupSelector(args[1]!));
    dispatched = args[1];
  } });
  assert.equal(dispatched, 'stop_admission');
  assert.equal(result, undefined);
});

test('watchdog rehearsal returns its actual custody evaluation', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const state = createSyntheticArmedCampaignState(campaignId, runId);
  const fixture = writeRehearsalFixture(directory, runId, state.runInventory!.cell_run_id);
  const evaluation = await boundedEffectRehearsal([
    '--mode', 'watchdog-once', '--env-file', fixture.envPath, '--stamp-config', fixture.stampPath,
    '--effect-envelope', fixture.allocationPath, '--receipt-file', fixture.receiptPath,
  ], { store: inMemoryCampaignStore(state) });
  assert.equal(evaluation?.event, 'campaign_custody_evaluation');
  assert.equal(evaluation?.status, 'observed');
  assert.equal(evaluation?.campaignStatus, 'active');
  assert.equal(evaluation?.cleanupStatus, 'not-started');
  assert.equal(evaluation?.pendingArmOperations, 0);
  assert.equal(evaluation?.cleanupProgress, undefined);
});

test('one-shot response withholding records the real Blob response and blocks restart replay', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  let forwards = 0;
  const server = createServer((request, response) => {
    forwards++;
    request.resume();
    request.on('end', () => {
      response.writeHead(201, { etag: '"baseline-etag"', connection: 'close' });
      response.end('created-by-local-provider');
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  });
  const address = server.address();
  assert.ok(address && typeof address === 'object');
  const localUrl = 'http://127.0.0.1:' + address.port;
  const fetcher: typeof fetch = async (_input, init) => fetch(localUrl, init);
  const effect = rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/baseline.json');
  const allocation = envelope([effect]), receiptPath = path.join(directory, 'effects.jsonl');
  const evidencePath = path.join(directory, 'withheld-response.json');
  const target = { transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path };
  const supervisor = new ResponseWithholdingSupervisor(campaignId, runId, target, evidencePath);
  const store = createBlobCampaignStore({ account: stamp.storage_account, container: stamp.state_container,
    subscriptionId, recordPath: 'baseline.json' }, fetcher, async () => 'test-only-token');
  const state = createSyntheticArmedCampaignState(campaignId, runId);
  await assert.rejects(withEffectBoundary(new EffectBoundary(allocation, receiptPath), () => store.write(state), supervisor),
    /durably captured and withheld/);
  assert.equal(supervisor.wasTriggered, true);
  assert.equal(forwards, 1);
  const evidence = JSON.parse(readFileSync(evidencePath, 'utf8'));
  assert.equal(evidence.event, 'provider_response_withheld');
  assert.equal(evidence.status, 201);
  assert.match(evidence.response_sha256, /^[0-9a-f]{64}$/);
  assert.equal(JSON.stringify(evidence).includes('created-by-local-provider'), false);

  const restartedStore = createBlobCampaignStore({ account: stamp.storage_account, container: stamp.state_container,
    subscriptionId, recordPath: 'baseline.json' }, fetcher, async () => 'test-only-token');
  await assert.rejects(withEffectBoundary(new EffectBoundary(allocation, receiptPath), () => restartedStore.write(state)),
    EffectBoundaryRefused);
  assert.equal(forwards, 1);
  const receipts = readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(receipts.find(row => row.event === 'result')?.outcome, 'unknown_after_forward');
});


test('setup-baseline sequences the immutable inventory and resumes after withheld Blob acknowledgement', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const fixture = writeRehearsalFixture(directory, runId, runId);
  const resource = 'https://arrowcollector12345.blob.core.windows.net/checkpoints';
  const baselinePath = '/checkpoints/_arrowcol-custody/campaigns/' + campaignId + '/baseline.json';
  const allocation = { ...envelope([rule('baseline-lookup', 'campaign-blob', 'GET', resource, baselinePath, 2),
    rule('baseline-write', 'campaign-blob', 'PUT', resource, baselinePath)]), storage_account: 'arrowcollector12345' };
  writeFileSync(fixture.allocationPath, JSON.stringify(allocation), { mode: 0o600 });
  const initialInventoryPath = path.join(directory, 'initial-inventory.json');
  writeFileSync(initialInventoryPath, JSON.stringify(initialRunInventory(runId)), { mode: 0o600 });
  const planPath = path.join(directory, 'withhold-plan.json');
  const evidencePath = path.join(directory, 'withheld-response.json');
  writeFileSync(planPath, JSON.stringify({ schema_version: 1, campaign_id: campaignId, run_id: runId,
    target: { transport: 'campaign-blob', method: 'PUT', resource, path: baselinePath },
    evidence_file: path.basename(evidencePath) }), { mode: 0o600 });

  const store = emptyCampaignStore();
  const calls: string[] = [];
  let lookupCount = 0, writeCount = 0;
  const campaignMain: NonNullable<BoundedEffectRehearsalDependencies['campaignMain']> = async (args, dependencies) => {
    calls.push(args[0]!);
    if (args[0] === 'start') await startCampaign(store, args[1]!, args[2]!, 1_000_000);
    else if (args[0] === 'publish-inventory') await publishRunInventory(store, dependencies?.runInventoryInput, 1_000_001);
    else if (args[0] === 'capture-baseline') {
      const existing = await dispatchExternalEffect({ transport: 'campaign-blob', method: 'GET', resource, path: baselinePath },
        async () => new Response(null, { status: lookupCount++ === 0 ? 404 : 200 }));
      if (existing instanceof Response && existing.status === 404) {
        await dispatchExternalEffect({ transport: 'campaign-blob', method: 'PUT', resource, path: baselinePath,
          body: Buffer.from('provider-generated-baseline') }, async () => {
          writeCount++;
          return new Response('created', { status: 201, headers: { etag: '"baseline"' } });
        });
      }
    } else throw new Error('Unexpected campaign action: ' + args[0]);
  };
  const firstArgs = ['--mode', 'setup-baseline', '--env-file', fixture.envPath, '--stamp-config', fixture.stampPath,
    '--effect-envelope', fixture.allocationPath, '--receipt-file', fixture.receiptPath,
    '--initial-inventory-file', initialInventoryPath, '--withhold-response', planPath];
  await assert.rejects(boundedEffectRehearsal(firstArgs, { store, campaignMain }), /durably captured and withheld/);
  assert.deepEqual(calls, ['start', 'publish-inventory', 'capture-baseline']);
  assert.equal(writeCount, 1);
  assert.equal(JSON.parse(readFileSync(evidencePath, 'utf8')).status, 201);

  await boundedEffectRehearsal([...firstArgs.slice(0, -2), '--resume'], { store, campaignMain });
  assert.deepEqual(calls, ['start', 'publish-inventory', 'capture-baseline', 'capture-baseline']);
  assert.equal(lookupCount, 2);
  assert.equal(writeCount, 1);
  const receipts = readFileSync(fixture.receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(receipts.filter(row => row.event === 'result' && row.method === 'PUT' && row.outcome === 'unknown_after_forward').length, 1);
});

test('Azure REST and process adapters reject unallocated targets before transport', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const resource = 'https://management.azure.com/';
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01';
  const request = rule('arm-put', 'azure-rest', 'PUT', 'https://management.azure.com|' + resource,
    '/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01');
  const boundary = new EffectBoundary(envelope([request]), path.join(directory, 'effects.jsonl'));
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscriptionId, AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo-test' }, 'schema-evolution');
  const azure = new Azure(config);
  azure.tokens.set(resource, { at: runtime.now(), token: 'test-token', lifetime: 240 });
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response(null, { status: 200 }); });
  await withEffectBoundary(boundary, async () => {
    await azure.request('PUT', url, resource, '{"phase":"intent"}', {}, false, true);
    await assert.rejects(azure.request('GET', url + '&unexpected=true', resource), /refused before forwarding/);
    const command = ['kubectl', '--server', 'https://example.invalid', 'delete', 'namespace', 'unallocated'];
    await assert.rejects(runtime.run_tree(command, 1), /refused before forwarding/);
  });
  assert.equal(fetches, 1);
  const refused = readFileSync(path.join(directory, 'effects.jsonl'), 'utf8').trim().split('\n')
    .map(line => JSON.parse(line)).filter(row => row.event === 'refused');
  assert.equal(refused.length, 2);
});

test('workload identity refusal is preserved and does not retry transport', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const receiptPath = path.join(directory, 'effects.jsonl');
  const boundary = new EffectBoundary(envelope([rule('wrong-token-target', 'azure-auth', 'POST',
    'https://login.microsoftonline.com', '/not-the-token-endpoint')]), receiptPath);
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response(null, { status: 200 }); });
  await assert.rejects(withEffectBoundary(boundary, () => exchange_workload_identity_assertion(
    'https://management.azure.com/', 'client-id', 'tenant-id', 'simulated-assertion')), EffectBoundaryRefused);
  assert.equal(fetches, 0);
  const rows = readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(rows.map(row => row.event), ['refused']);
});


test('uncertain mutation responses do not unlock another dispatch but authoritative 412 does', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const effect = rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/baseline.json', 2);
  const receipts = path.join(directory, 'effects.jsonl');
  const boundary = new EffectBoundary(envelope([effect]), receipts);
  let forwards = 0;
  await withEffectBoundary(boundary, async () => {
    await dispatchExternalEffect({ transport: effect.transport, method: effect.method, resource: effect.resource,
      path: effect.path }, async () => { forwards++; return { status: 500 }; });
    await assert.rejects(dispatchExternalEffect({ transport: effect.transport, method: effect.method, resource: effect.resource,
      path: effect.path }, async () => { forwards++; return { status: 201 }; }), EffectBoundaryRefused);
  });
  assert.equal(forwards, 1);
  assert.equal(JSON.parse(readFileSync(receipts, 'utf8').trim().split('\n')[1]!).outcome, 'unknown_after_forward');

  const casReceipts = path.join(directory, 'cas-effects.jsonl');
  const cas = new EffectBoundary(envelope([effect]), casReceipts);
  await withEffectBoundary(cas, async () => {
    assert.deepEqual(await dispatchExternalEffect({ transport: effect.transport, method: effect.method,
      resource: effect.resource, path: effect.path }, async () => ({ status: 412 })), { status: 412 });
    assert.deepEqual(await dispatchExternalEffect({ transport: effect.transport, method: effect.method,
      resource: effect.resource, path: effect.path }, async () => ({ status: 201 })), { status: 201 });
  });
  const casRows = readFileSync(casReceipts, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(casRows.filter(row => row.event === 'result').map(row => row.outcome), ['definite_no_effect', 'confirmed_success']);
});

test('journal recovery rejects changed allocations and request bindings before effects', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const effect = rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/baseline.json', 2);
  const allocation = envelope([effect]), receipts = path.join(directory, 'effects.jsonl');
  await withEffectBoundary(new EffectBoundary(allocation, receipts), () => dispatchExternalEffect({
    transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path,
  }, async () => ({ status: 201 })));
  assert.throws(() => new EffectBoundary(envelope([{ ...effect, max_calls: 3 }]), receipts), /allocation/);
  const changedRows = readFileSync(receipts, 'utf8').trim().split('\n').map(line => {
    const row = JSON.parse(line); if (row.event === 'dispatch' || row.event === 'result') row.path = '/checkpoints/other.json';
    return JSON.stringify(row);
  });
  writeFileSync(receipts, changedRows.join('\n') + '\n', { mode: 0o600 });
  assert.throws(() => new EffectBoundary(allocation, receipts), /binding changed/);
});

test('receipt journal completes short writes and refuses a zero-byte dispatch write', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const effect = rule('active-read', 'campaign-blob', 'GET',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/active.json');
  const shortReceipts = path.join(directory, 'short.jsonl');
  const shortWriter = (fd: number, data: Buffer, offset: number, length: number) =>
    writeSync(fd, data, offset, Math.min(length, 7));
  let forwards = 0;
  await withEffectBoundary(new EffectBoundary(envelope([effect]), shortReceipts, shortWriter), () =>
    dispatchExternalEffect({ transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path },
      async () => { forwards++; return { status: 200 }; }));
  assert.equal(forwards, 1);
  assert.equal(readFileSync(shortReceipts, 'utf8').trim().split('\n').length, 2);

  const failedReceipts = path.join(directory, 'zero.jsonl');
  const zeroWriter = () => 0;
  let failedForwards = 0;
  await assert.rejects(withEffectBoundary(new EffectBoundary(envelope([effect]), failedReceipts, zeroWriter), () =>
    dispatchExternalEffect({ transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path },
      async () => { failedForwards++; return { status: 200 }; })), /Dispatch receipt could not be durably recorded/);
  assert.equal(failedForwards, 0);
});

test('ARM operation status network call requires its own allocation after allocated identity auth', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const endpoint = new URL('https://identity.test/metadata');
  endpoint.searchParams.set('api-version', '2019-08-01');
  endpoint.searchParams.set('resource', 'https://management.azure.com/');
  endpoint.searchParams.set('client_id', 'client-id');
  const identityRule = rule('identity-token', 'azure-auth', 'GET', endpoint.origin, endpoint.pathname + endpoint.search);
  const boundary = new EffectBoundary(envelope([identityRule]), path.join(directory, 'effects.jsonl'));
  const env = { IDENTITY_ENDPOINT: 'https://identity.test/metadata', IDENTITY_HEADER: 'test-header', AZURE_CLIENT_ID: 'client-id' };
  let identityFetches = 0, statusFetches = 0, redirectMode: RequestRedirect | undefined;
  const fetcher = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const url = new URL(String(input));
    if (url.origin === endpoint.origin) {
      identityFetches++; redirectMode = init?.redirect;
      return new Response(JSON.stringify({ access_token: 'simulated-token' }), { status: 200 });
    }
    statusFetches++;
    return new Response('{}', { status: 200 });
  };
  const operationUrl = 'https://management.azure.com/subscriptions/' + subscriptionId +
    '/providers/Microsoft.ContainerService/locations/westus/operations/op-1?api-version=2024-10-01';
  await assert.rejects(withEffectBoundary(boundary, () => readArmOperationStatus(operationUrl, subscriptionId, fetcher, env)),
    EffectBoundaryRefused);
  assert.equal(identityFetches, 1);
  assert.equal(statusFetches, 0);
  assert.equal(redirectMode, 'error');
});

test('identity and campaign Blob transports reject redirects only while the boundary is active', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const env = { IDENTITY_ENDPOINT: 'https://identity.test/metadata', IDENTITY_HEADER: 'test-header', AZURE_CLIENT_ID: 'client-id' };
  const endpoint = new URL(env.IDENTITY_ENDPOINT);
  endpoint.searchParams.set('api-version', '2019-08-01');
  endpoint.searchParams.set('resource', 'https://management.azure.com/');
  endpoint.searchParams.set('client_id', 'client-id');
  const boundary = new EffectBoundary(envelope([rule('identity-token', 'azure-auth', 'GET', endpoint.origin,
    endpoint.pathname + endpoint.search)]), path.join(directory, 'identity.jsonl'));
  let activeRedirect: RequestRedirect | undefined, ordinaryRedirect: RequestRedirect | undefined;
  const identityFetcher = async (_input: string | URL | Request, init?: RequestInit) => {
    activeRedirect ??= init?.redirect;
    return new Response(JSON.stringify({ access_token: 'simulated-token' }), { status: 200 });
  };
  await withEffectBoundary(boundary, () => managedIdentityToken('https://management.azure.com/', env, identityFetcher));
  await managedIdentityToken('https://management.azure.com/', env, async (_input, init) => {
    ordinaryRedirect = init?.redirect; return new Response(JSON.stringify({ access_token: 'simulated-token' }), { status: 200 });
  });
  assert.equal(activeRedirect, 'error');
  assert.equal(ordinaryRedirect, undefined);

  const blobPath = '/checkpoints/active.json';
  const blobRule = rule('active-read', 'campaign-blob', 'GET',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', blobPath);
  const blobBoundary = new EffectBoundary(envelope([blobRule]), path.join(directory, 'blob.jsonl'));
  let blobActiveRedirect: RequestRedirect | undefined, blobOrdinaryRedirect: RequestRedirect | undefined;
  const activeStore = createBlobCampaignStore({ account: stamp.storage_account, container: stamp.state_container,
    subscriptionId, recordPath: 'active.json' }, async (_input, init) => {
      blobActiveRedirect = init?.redirect; return new Response(null, { status: 404 });
    }, async () => 'simulated-token');
  await withEffectBoundary(blobBoundary, () => activeStore.read());
  const ordinaryStore = createBlobCampaignStore({ account: stamp.storage_account, container: stamp.state_container,
    subscriptionId, recordPath: 'active.json' }, async (_input, init) => {
      blobOrdinaryRedirect = init?.redirect; return new Response(null, { status: 404 });
    }, async () => 'simulated-token');
  await ordinaryStore.read();
  assert.equal(blobActiveRedirect, 'error');
  assert.equal(blobOrdinaryRedirect, undefined);
});

test('Azure REST response-body loss cannot replay a mutation on its retry path', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const resource = 'https://management.azure.com/';
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId +
    '/resourceGroups/arrow-collector-rg?api-version=2024-10-01';
  const effect = rule('arm-put', 'azure-rest', 'PUT', 'https://management.azure.com|' + resource,
    '/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01', 3);
  const boundary = new EffectBoundary(envelope([effect]), path.join(directory, 'effects.jsonl'));
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscriptionId, AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo-test' }, 'schema-evolution'));
  azure.tokens.set(resource, { at: runtime.now(), token: 'test-token', lifetime: 240 });
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    fetches++;
    const response = new Response('body', { status: 200 });
    response.arrayBuffer = async () => { throw new Error('simulated response-body loss'); };
    return response;
  });
  await assert.rejects(withEffectBoundary(boundary, () => azure.request('PUT', url, resource, '{"phase":"intent"}', {}, false, true)), EffectBoundaryRefused);
  assert.equal(fetches, 1);
  const rows = readFileSync(path.join(directory, 'effects.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(rows.find(row => row.event === 'result')?.outcome, 'unknown_after_forward');
});


test('Azure REST HTTP 5xx cannot unlock a retrying mutation', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const resource = 'https://management.azure.com/';
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId +
    '/resourceGroups/arrow-collector-rg?api-version=2024-10-01';
  const effect = rule('arm-put', 'azure-rest', 'PUT', 'https://management.azure.com|' + resource,
    '/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01', 3);
  const receiptPath = path.join(directory, 'effects.jsonl');
  const boundary = new EffectBoundary(envelope([effect]), receiptPath);
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscriptionId, AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo-test' }, 'schema-evolution'));
  azure.tokens.set(resource, { at: runtime.now(), token: 'test-token', lifetime: 240 });
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response('server failed', { status: 500 }); });
  await assert.rejects(withEffectBoundary(boundary, () => azure.request('PUT', url, resource, '{"phase":"intent"}', {}, false, true)),
    EffectBoundaryRefused);
  assert.equal(fetches, 1);
  const rows = readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(rows.find(row => row.event === 'result')?.outcome, 'unknown_after_forward');
  assert.equal(rows.find(row => row.event === 'result')?.status, 500);
});

test('nonzero process exits remain unresolved and cannot replay a command', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const command = ['node', '-e', 'process.exit(17)'];
  const effect = rule('nonzero-command', 'process', 'PROCESS_NODE', 'node', JSON.stringify(command.slice(1)), 2);
  const receiptPath = path.join(directory, 'effects.jsonl');
  const boundary = new EffectBoundary(envelope([effect]), receiptPath);
  await withEffectBoundary(boundary, async () => {
    assert.equal((await runtime.run_tree(command, 5)).returncode, 17);
    await assert.rejects(runtime.run_tree(command, 5), EffectBoundaryRefused);
  });
  const rows = readFileSync(receiptPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.equal(rows.find(row => row.event === 'result')?.outcome, 'unknown_after_forward');
  assert.equal(rows.find(row => row.event === 'result')?.returncode, 17);
});


test('recovery rejects success receipts without transport acknowledgement or with prior errors', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const effect = rule('baseline-put', 'campaign-blob', 'PUT',
    'https://arrowcollector68590.blob.core.windows.net/checkpoints', '/checkpoints/baseline.json');
  const allocation = envelope([effect]);
  const missingAckPath = path.join(directory, 'missing-ack.jsonl');
  await withEffectBoundary(new EffectBoundary(allocation, missingAckPath), () => dispatchExternalEffect({
    transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path,
  }, async () => ({ status: 201 })));
  const missingAckRows = readFileSync(missingAckPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const missingAckResult = missingAckRows.find(row => row.event === 'result');
  delete missingAckResult.status;
  missingAckResult.outcome = 'confirmed_success';
  writeFileSync(missingAckPath, missingAckRows.map(row => JSON.stringify(row)).join('\n') + '\n', { mode: 0o600 });
  assert.throws(() => new EffectBoundary(allocation, missingAckPath), /outcome does not match/);

  const relabelledPath = path.join(directory, 'relabelled-error.jsonl');
  await assert.rejects(withEffectBoundary(new EffectBoundary(allocation, relabelledPath), () => dispatchExternalEffect({
    transport: effect.transport, method: effect.method, resource: effect.resource, path: effect.path,
  }, async () => { throw new Error('acknowledgement lost'); })));
  const relabelledRows = readFileSync(relabelledPath, 'utf8').trim().split('\n').map(line => JSON.parse(line));
  relabelledRows.find(row => row.event === 'result').outcome = 'confirmed_success';
  writeFileSync(relabelledPath, relabelledRows.map(row => JSON.stringify(row)).join('\n') + '\n', { mode: 0o600 });
  assert.throws(() => new EffectBoundary(allocation, relabelledPath), /outcome does not match/);
});
