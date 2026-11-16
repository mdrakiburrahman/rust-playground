import assert from 'node:assert/strict';
import { test, type TestContext } from 'node:test';
import { Azure, Config, TAGS, type HttpReceipt } from './bootstrap.js';
import { observe_custody_idle_capacity } from './aks-safe-idle.js';

function fixture(t: TestContext, change: (id: string, value: Record<string, unknown>) => void = () => {}) {
  const c = new Config({ AZURE_SUBSCRIPTION_ID: '22222222-2222-4222-8222-222222222222' }), azure = new Azure(c);
  const calls: string[] = [];
  t.mock.method(azure, 'request', async (method: string, url: string, resource: string, body?: unknown,
    _headers?: unknown, _missing?: boolean, _idempotent?: boolean, capture?: (receipt: HttpReceipt) => void) => {
    assert.equal(method, 'GET');
    assert.equal(body, undefined);
    assert.equal(resource, 'https://management.azure.com/');
    assert.equal(new URL(url).pathname.split('/')[2], c.subscription);
    calls.push(url);
    const id = new URL(url).pathname, pool = id.split('/agentPools/')[1];
    const value: Record<string, unknown> = {
      id, type: pool ? 'Microsoft.ContainerService/managedClusters/agentPools' :
        id === c.cluster_id ? 'Microsoft.Kusto/clusters' : 'Microsoft.ContainerService/managedClusters',
      tags: { ...TAGS, sandboxId: c.sandbox },
      properties: pool ? { provisioningState: 'Succeeded', vmSize: 'Standard_D8ds_v6',
        count: pool === 'systempool' ? 2 : 0, enableAutoScaling: pool !== 'systempool', minCount: 0, maxCount: 1 } :
        id === c.cluster_id ? { provisioningState: 'Succeeded', state: 'Running', enableAutoStop: true,
          optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 4 } } : { provisioningState: 'Succeeded',
          agentPoolProfiles: ['systempool', 'benchd64', 'benche64'].map(name => ({ name })) },
      ...(id === c.cluster_id ? { sku: { name: 'Standard_E4ads_v5', capacity: 2 } } : {}),
    };
    change(id, value);
    const bytes = Buffer.from(JSON.stringify(value)), headers = { 'x-ms-request-id': 'provider-read-1' };
    capture?.({ status: 200, headers, bodyBase64: bytes.toString('base64') });
    return [bytes, { ':status': '200', ...headers }];
  });
  return { azure, calls };
}

test('safe-idle capacity observation uses only exact-resource GETs with positive receipts and never claims workload/data safety', async t => {
  const f = fixture(t), result = await observe_custody_idle_capacity(f.azure);
  assert.equal(result.status, 'capacity_idle');
  assert.equal(result.workload_and_data_verification, 'unknown');
  assert.equal(result.scope, 'arm_capacity_only');
  assert.equal(f.calls.length, 5);
  assert.equal(result.receipts.length, 5);
  assert.ok(result.receipts.every(receipt => receipt.http_status === 200 && receipt.request_id === 'provider-read-1' &&
    /^[a-f0-9]{64}$/.test(receipt.body_sha256)));
  assert.deepEqual(result.user_pools.map(pool => [pool.name, pool.count, pool.autoscale_min, pool.autoscale_max]),
    [['benchd64', 0, 0, 1], ['benche64', 0, 0, 1]]);
  assert.equal(result.system_pool.autoscale_enabled, false);
});

test('scaled, stopped or provisioning capacity is not idle; incomplete and mismatched observations fail UNKNOWN', async t => {
  for (const patch of [
    { count: 1 }, { provisioningState: 'Updating' }, { enableAutoScaling: false },
  ]) {
    const f = fixture(t, (id, value) => {
      if (id.endsWith('/agentPools/benche64')) Object.assign(value.properties as object, patch);
    });
    assert.equal((await observe_custody_idle_capacity(f.azure)).status, 'capacity_not_idle');
  }
  for (const bad of [undefined, Number.NaN, 0.5, -1, '0']) {
    const f = fixture(t, (id, value) => {
      if (id.endsWith('/agentPools/benchd64')) Object.assign(value.properties as object, { count: bad });
    });
    await assert.rejects(observe_custody_idle_capacity(f.azure), /UNKNOWN/);
  }
  const stopped = fixture(t, (id, value) => {
    if (id.includes('Microsoft.Kusto')) Object.assign(value.properties as object, { state: 'Stopped' });
  });
  assert.equal((await observe_custody_idle_capacity(stopped.azure)).status, 'capacity_not_idle');
  const wrong = fixture(t, (_id, value) => { value.id = '/subscriptions/a-different-stamp'; });
  await assert.rejects(observe_custody_idle_capacity(wrong.azure), /UNKNOWN.*identity mismatch/);
  const missingReceipt = fixture(t);
  t.mock.method(missingReceipt.azure, 'request', async () => [Buffer.from('{}'), { ':status': '200' }]);
  await assert.rejects(observe_custody_idle_capacity(missingReceipt.azure), /UNKNOWN.*response receipt/);
  const extraPool = fixture(t, (id, value) => {
    if (id.includes('managedClusters') && !id.includes('agentPools'))
      Object.assign(value.properties as object, { agentPoolProfiles: [{ name: 'another-pool' }] });
  });
  await assert.rejects(observe_custody_idle_capacity(extraPool.azure), /UNKNOWN.*pool inventory/);
  const expandingSystem = fixture(t, (id, value) => {
    if (id.endsWith('/agentPools/systempool')) Object.assign(value.properties as object, { enableAutoScaling: true, minCount: 2, maxCount: 10 });
  });
  assert.equal((await observe_custody_idle_capacity(expandingSystem.azure)).status, 'capacity_not_idle');
  const unknownSystem = fixture(t, (id, value) => {
    if (id.endsWith('/agentPools/systempool')) delete (value.properties as Record<string, unknown>).enableAutoScaling;
  });
  await assert.rejects(observe_custody_idle_capacity(unknownSystem.azure), /UNKNOWN.*system autoscaler/);
});
