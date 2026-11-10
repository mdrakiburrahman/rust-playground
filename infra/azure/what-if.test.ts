import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { ARM_WHAT_IF_EXTENSION_TYPES, ARM_WHAT_IF_RESOURCE_TYPES, ArmPreviewFailure, Azure, Config, Failure, deploy, fresh_authority, preflight, provision, SCHEMA_EVOLUTION_GROUP } from './bootstrap.js';
import { defaultStampConfig } from './stamp.js';
import { FENCED } from './publisher_control.js';

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
      assert.deepEqual(args.slice(3, 6), ['--result-format', 'FullResourcePayloads', '--no-pretty-print']);
      parameterFile = args.at(-1)!.slice(1);
      assert.equal(fs.statSync(parameterFile).mode & 0o777, 0o600);
      const { parameters } = JSON.parse(fs.readFileSync(parameterFile, 'utf8'));
      assert.equal(parameters.aksConfig.value.networkDataplane, network);
      assert.equal(parameters.storageAccountName.value, 'arrowcollector89152');
      assert.equal(parameters.clusterName.value, 'arrow-collector-89152');
      assert.equal(parameters.identityName.value, 'arrow-collector');
      assert.equal(parameters.checkpointExists.value, false);
      assert.equal(parameters.controlMetadata.value.resetstate, FENCED);
      return { changes: [] };
    };
    await deploy(azure, fresh_authority(), true);
    assert.equal(calls.length, 1);
    assert.equal(fs.existsSync(parameterFile), false);
  });
}

test('provision previews and creates with the identical parameter file and resolved deployment arguments', async () => {
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  const config = new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp);
  config.operator = subscription;
  const azure = new Azure(config);
  azure.arm = async () => null;
  const calls: { args: string[]; parameterPath: string; payload: Buffer }[] = [];
  const logs: string[] = [];
  const originalLog = console.log;
  console.log = (...values) => logs.push(values.join(' '));
  azure.cli = async (...args) => {
    const parameterArg = args.at(-1)!;
    assert.ok(parameterArg.startsWith('@'));
    const parameterPath = parameterArg.slice(1);
    calls.push({ args, parameterPath, payload: fs.readFileSync(parameterPath) });
    if (args[2] === 'what-if') return { changes: [{ changeType: 'Create',
      resourceId: '/subscriptions/' + subscription + '/resourceGroups/arrow-collector-rg/providers/Microsoft.Kusto/clusters/arrow-collector-89152' }] };
    assert.equal(args[2], 'create');
    throw new Failure('DeploymentFailed: simulated create rejection');
  };
  try { await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), /DeploymentFailed/); }
  finally { console.log = originalLog; }
  assert.deepEqual(calls.map(call => call.args[2]), ['what-if', 'create']);
  assert.equal(calls[0]!.parameterPath, calls[1]!.parameterPath);
  assert.deepEqual(calls[0]!.payload, calls[1]!.payload);
  assert.deepEqual(calls[0]!.args.slice(0, 2), calls[1]!.args.slice(0, 2));
  assert.deepEqual(calls[0]!.args.slice(6), calls[1]!.args.slice(3));
  const parameters = JSON.parse(calls[0]!.payload.toString('utf8')).parameters;
  assert.equal(parameters.controlMetadata.value.resetstate, FENCED);
  assert.equal(parameters.checkpointExists.value, false);
  assert.equal(parameters.aksConfig.value.networkDataplane, 'azure');
  assert.equal(fs.existsSync(calls[0]!.parameterPath), false);
  const parameterHash = createHash('sha256').update(calls[0]!.payload).digest('hex');
  const templatePath = calls[0]!.args[calls[0]!.args.indexOf('--template-file') + 1]!;
  const templateHash = createHash('sha256').update(fs.readFileSync(templatePath)).digest('hex');
  assert.ok(logs.includes('ARM_PREVIEW_VERIFIED parametersSha256=' + parameterHash + ' templateSha256=' + templateHash + ' changes=1'));
});

const resourceGroupId = '/subscriptions/' + subscription + '/resourceGroups/arrow-collector-rg';
const identityId = resourceGroupId + '/providers/Microsoft.ManagedIdentity/userAssignedIdentities/arrow-collector';
const clusterId = resourceGroupId + '/providers/Microsoft.Kusto/clusters/arrow-collector-89152';
const microsoftRestTagCreateModify = JSON.parse(fs.readFileSync(
  new URL('./fixtures/what-if-rest-tag-create.synthetic.json', import.meta.url), 'utf8'));

test('what-if allowlist covers every resource type in the compiled main Bicep template', () => {
  const compiled = spawnSync('az', ['bicep', 'build', '--file', 'infra/azure/main.bicep', '--stdout'],
    { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  assert.ifError(compiled.error);
  assert.equal(compiled.status, 0, compiled.stderr);
  const template = JSON.parse(compiled.stdout);
  const types = new Set<string>();
  const visit = (value: any): void => {
    for (const resource of value.resources ?? []) {
      if (typeof resource.type === 'string') types.add(resource.type.toLowerCase());
      if (resource.properties?.template && typeof resource.properties.template === 'object') visit(resource.properties.template);
    }
    for (const definition of Object.values(value.definitions ?? {}) as any[]) {
      if (definition?.template && typeof definition.template === 'object') visit(definition.template);
    }
  };
  visit(template);
  assert.ok(types.size > 0);
  assert.deepEqual([...types].filter(type => !ARM_WHAT_IF_RESOURCE_TYPES.has(type)), []);
  for (const type of ARM_WHAT_IF_EXTENSION_TYPES) assert.ok(ARM_WHAT_IF_RESOURCE_TYPES.has(type));
});

for (const scope of [
  { name: 'identity', id: identityId + '/providers/Microsoft.Authorization/roleAssignments/identity-role' },
  { name: 'subnet', id: resourceGroupId + '/providers/Microsoft.Network/virtualNetworks/vnet/subnets/node/providers/Microsoft.Authorization/roleAssignments/subnet-role' },
  { name: 'registry', id: resourceGroupId + '/providers/Microsoft.ContainerRegistry/registries/arrowcollector89152/providers/Microsoft.Authorization/roleAssignments/registry-role' },
]) {
  for (const changeType of ['Create', 'Ignore'] as const) {
    test('what-if accepts ' + changeType + ' for ' + scope.name + '-scoped role assignment IDs', async () => {
      const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
      stamp.subscription = subscription;
      const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
      azure.arm = async () => null;
      const calls: string[] = [];
      const originalLog = console.log;
      console.log = () => {};
      azure.cli = async (...args) => { calls.push(args[2]!); return { changes: [{ changeType, resourceId: scope.id }] }; };
      try { await deploy(azure, fresh_authority(), true); }
      finally { console.log = originalLog; }
      assert.deepEqual(calls, ['what-if']);
    });
  }
}

for (const scenario of [
  { name: 'missing change list', result: {} },
  { name: 'unsupported resource change', result: { changes: [{ changeType: 'Unsupported', resourceId: clusterId }] } },
  { name: 'Deploy-only resource change', result: { changes: [{ changeType: 'Deploy', resourceId: clusterId }] } },
  { name: 'missing resource identifier', result: { changes: [{ changeType: 'Create' }] } },
  { name: 'incomplete preview status', result: { status: 'Running', changes: [] } },
  { name: 'Delete change', result: { changes: [{ changeType: 'Delete', resourceId: clusterId }] } },
  { name: 'resource in another resource group', result: { changes: [{ changeType: 'Create', resourceId: '/subscriptions/' + subscription + '/resourceGroups/other-rg/providers/Microsoft.Kusto/clusters/other' }] } },
  { name: 'resource in another subscription', result: { changes: [{ changeType: 'Create', resourceId: '/subscriptions/00000000-0000-0000-0000-000000000002/resourceGroups/arrow-collector-rg/providers/Microsoft.Kusto/clusters/other' }] } },
  { name: 'unexpected resource type', result: { changes: [{ changeType: 'Create', resourceId: resourceGroupId + '/providers/Microsoft.Test/items/x' }] } },
  { name: 'Modify without snapshots', result: { changes: [{ changeType: 'Modify', resourceId: identityId, delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new' }] }] } },
  { name: 'non-tag snapshot change hidden by a tag-only delta', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: { location: 'canadacentral', tags: { owner: 'old' } }, after: { location: 'eastus2', tags: { owner: 'new' } },
    delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new' }] }] } },
  { name: 'tag removal', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: { tags: { retired: 'old' } }, after: { tags: {} },
    delta: [{ path: 'tags.retired', propertyChangeType: 'Delete', before: 'old' }] }] } },
  { name: 'whole tags object removal', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: { tags: { retired: 'old' } }, after: { tags: {} },
    delta: [{ path: 'tags', propertyChangeType: 'Modify', before: { retired: 'old' }, after: {} }] }] } },
  { name: 'replacement-sensitive Modify', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: { location: 'eastus2', tags: { owner: 'old' } }, after: { location: 'canadacentral', tags: { owner: 'new' } },
    delta: [{ path: 'location', propertyChangeType: 'Modify', before: 'eastus2', after: 'canadacentral' }] }] } },
]) {
  test('provision refuses ' + scenario.name + ' before create', async () => {
    const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
    stamp.subscription = subscription;
    const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
    azure.arm = async () => null;
    const calls: string[] = [];
    const logs: string[] = [];
    const originalLog = console.log;
    console.log = (...values) => logs.push(values.join(' '));
    azure.cli = async (...args) => { calls.push(args[2]!); return scenario.result; };
    try {
      await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), error => {
        if (!(error instanceof ArmPreviewFailure)) return false;
        assert.doesNotMatch(error.message, /\/subscriptions\//);
        return true;
      });
    } finally { console.log = originalLog; }
    assert.deepEqual(calls, ['what-if']);
    assert.equal(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED')), false);
  });
}

test('provision permits Microsoft REST-shaped tag-only Modify with tag creation lacking before', async () => {
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
  azure.arm = async () => null;
  const calls: string[] = [];
  const logs: string[] = [];
  const originalLog = console.log;
  console.log = (...values) => logs.push(values.join(' '));
  azure.cli = async (...args) => {
    calls.push(args[2]!);
    if (args[2] === 'what-if') return { changes: [microsoftRestTagCreateModify] };
    throw new Failure('DeploymentFailed: simulated create rejection');
  };
  try { await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), /DeploymentFailed/); }
  finally { console.log = originalLog; }
  assert.deepEqual(calls, ['what-if', 'create']);
  assert.ok(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED ') && line.endsWith(' changes=1')));
});

test('provision allows complete Microsoft REST-shaped tag Modify deltas on the resource group and shared UAMI', async () => {
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
  azure.arm = async () => null;
  const calls: string[] = [];
  const logs: string[] = [];
  const originalLog = console.log;
  console.log = (...values) => logs.push(values.join(' '));
  azure.cli = async (...args) => {
    calls.push(args[2]!);
    if (args[2] === 'what-if') return { changes: [
      { changeType: 'Modify', resourceId: resourceGroupId,
        before: { location: 'canadacentral', tags: { owner: 'previous' } },
        after: { location: 'canadacentral', tags: { owner: 'current' } },
        delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'previous', after: 'current' }] },
      { changeType: 'Modify', resourceId: identityId,
        before: { location: 'canadacentral', tags: { owner: 'previous' } },
        after: { location: 'canadacentral', tags: { owner: 'current' } },
        delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'previous', after: 'current' }] },
    ] };
    throw new Failure('DeploymentFailed: simulated create rejection');
  };
  try { await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), /DeploymentFailed/); }
  finally { console.log = originalLog; }
  assert.deepEqual(calls, ['what-if', 'create']);
  assert.ok(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED ') && line.endsWith(' changes=2')));
});

test('provision treats a timed-out ARM what-if as a preview failure before create', async () => {
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
  azure.arm = async () => null;
  const calls: string[] = [];
  azure.cli = async (...args) => { calls.push(args[2]!); throw new Failure('Azure what-if command timed out'); };
  await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true),
    error => error instanceof ArmPreviewFailure);
  assert.deepEqual(calls, ['what-if']);
});

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
