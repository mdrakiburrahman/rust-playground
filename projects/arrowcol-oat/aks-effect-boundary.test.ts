import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, chmodSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { stringify } from 'yaml';
import { Azure, Config, exchange_workload_identity_assertion, runtime } from '../../infra/azure/bootstrap.js';
import { defaultStampConfig } from '../../infra/azure/stamp.js';
import { EffectBoundary, EffectBoundaryRefused, dispatchExternalEffect, withEffectBoundary,
  type EffectEnvelope, type EffectRule } from '../../infra/azure/effect-boundary.js';
import { createBlobCampaignStore } from './aks-campaign.js';
import { boundedEffectRehearsal } from './aks-effect-rehearsal.js';

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

test('Azure REST and process adapters reject unallocated targets before transport', async t => {
  const directory = tempDir(); t.after(() => rmSync(directory, { recursive: true, force: true }));
  const resource = 'https://management.azure.com/';
  const url = 'https://management.azure.com/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01';
  const request = rule('arm-get', 'azure-rest', 'GET', 'https://management.azure.com|' + resource,
    '/subscriptions/' + subscriptionId + '/resourceGroups/arrow-collector-rg?api-version=2024-10-01');
  const boundary = new EffectBoundary(envelope([request]), path.join(directory, 'effects.jsonl'));
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscriptionId, AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo-test' }, 'schema-evolution');
  const azure = new Azure(config);
  azure.tokens.set(resource, { at: runtime.now(), token: 'test-token', lifetime: 240 });
  let fetches = 0;
  t.mock.method(globalThis, 'fetch', async () => { fetches++; return new Response(null, { status: 200 }); });
  await withEffectBoundary(boundary, async () => {
    await azure.request('GET', url, resource);
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
