import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { Azure, Config, deploy, fresh_authority, preflight, SCHEMA_EVOLUTION_GROUP } from './bootstrap.js';
import { defaultStampConfig } from './stamp.js';

const subscription = '00000000-0000-0000-0000-000000000001';

for (const network of ['azure', 'cilium'] as const) {
  test(`ARM what-if uses the actual ${network} deployment parameters without creation or data-plane calls`, async () => {
    const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
    stamp.subscription = subscription;
    stamp.aks = { ...stamp.aks, networkDataplane: network };
    const config = new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp);
    config.operator = subscription;
    const azure = new Azure(config);
    const calls: string[][] = [];
    azure.arm = async (method, resource) => {
      assert.equal(method, 'GET');
      assert.ok([config.base, config.storage_id, config.cluster_id, config.identity_id, config.workspace_id].includes(resource));
      return null;
    };
    let parameterFile = '';
    azure.cli = async (...args) => {
      calls.push(args);
      assert.deepEqual(args.slice(0, 3), ['deployment', 'sub', 'what-if']);
      parameterFile = args.at(-1)!.slice(1);
      assert.equal(fs.statSync(parameterFile).mode & 0o777, 0o600);
      const { parameters } = JSON.parse(fs.readFileSync(parameterFile, 'utf8'));
      assert.equal(parameters.aksConfig.value.networkDataplane, network);
      assert.equal(parameters.storageAccountName.value, 'arrowcollector89152');
      assert.equal(parameters.clusterName.value, 'arrow-collector-89152');
      assert.equal(parameters.identityName.value, 'arrow-collector');
      assert.equal(parameters.checkpointExists.value, false);
      assert.equal(parameters.controlMetadata.value.resetstate, 'fenced');
      return { changes: [] };
    };
    await deploy(azure, fresh_authority(), true);
    assert.equal(calls.length, 1);
    assert.equal(fs.existsSync(parameterFile), false);
  });
}

test('read-only preflight never registers missing Azure providers', async () => {
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscription, AZURE_RESOURCE_GROUP: SCHEMA_EVOLUTION_GROUP }, 'schema-evolution');
  const azure = new Azure(config);
  const calls: string[][] = [];
  azure.cli = async (...args) => {
    calls.push(args);
    if (args[0] === 'account') return { id: subscription };
    assert.deepEqual(args.slice(0, 2), ['provider', 'show']);
    return { registrationState: 'NotRegistered' };
  };
  await assert.rejects(preflight(azure, true), /read-only preflight cannot register/);
  assert.equal(calls.some(args => args[1] === 'register'), false);
});

test('ordinary preflight retains provider registration behavior', async () => {
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscription, AZURE_RESOURCE_GROUP: SCHEMA_EVOLUTION_GROUP }, 'schema-evolution');
  config.operator = subscription;
  const azure = new Azure(config);
  const registrations: string[] = [];
  azure.cli = async (...args) => {
    if (args[0] === 'account') return { id: subscription };
    if (args[0] === 'provider' && args[1] === 'show') return { registrationState: 'NotRegistered' };
    if (args[0] === 'provider' && args[1] === 'register') { registrations.push(args[3]!); return {}; }
    if (args[0] === 'resource') return [];
    assert.fail(`Unexpected operation ${args.join(' ')}`);
  };
  azure.arm = async method => { assert.equal(method, 'GET'); return null; };
  await preflight(azure);
  assert.equal(registrations.length, 5);
});
