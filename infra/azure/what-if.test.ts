import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import test from 'node:test';
import { ARM_WHAT_IF_EXTENSION_TYPES, ARM_WHAT_IF_RESOURCE_TYPES, ArmPreviewFailure, Azure, Config, Failure, arm_what_if_runtime_roles, deploy, fresh_authority, preflight, provision, SCHEMA_EVOLUTION_GROUP } from './bootstrap.js';
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
const observedCliPreview = JSON.parse(fs.readFileSync(
  new URL('./fixtures/what-if-cli-observed.sanitized.json', import.meta.url), 'utf8'));
const fullSnapshot = (id: string, tags: Record<string, string>) => ({
  id, name: id.split('/').at(-1)!, apiVersion: '2023-01-31', location: 'canadacentral', tags,
  type: id === resourceGroupId ? 'Microsoft.Resources/resourceGroups' : 'Microsoft.ManagedIdentity/userAssignedIdentities',
});
const tagModify = () => ({
  changeType: 'Modify', resourceId: identityId,
  before: fullSnapshot(identityId, { owner: 'old' }), after: fullSnapshot(identityId, { owner: 'new' }),
  delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new' }],
});

for (const scenario of [
  { name: 'minimized tag-only snapshots', change: { ...tagModify(),
    before: { tags: { owner: 'old' } }, after: { tags: { owner: 'new' } } } },
  { name: 'conflicting nested tag child', change: { ...tagModify(), delta: [
    { path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new',
      children: [{ path: 'properties.principalId', propertyChangeType: 'Delete', before: 'principal' }] },
  ] } },
  { name: 'malformed nested tag children', change: { ...tagModify(), delta: [
    { path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new', children: {} },
  ] } },
  { name: 'snapshot with mismatched identity', change: { ...tagModify(),
    before: fullSnapshot(identityId + '-other', { owner: 'old' }),
    after: fullSnapshot(identityId + '-other', { owner: 'new' }) } },
  { name: 'NoEffect resource type', change: { changeType: 'NoEffect', resourceId: clusterId } },
  { name: 'dangling provider suffix', change: { changeType: 'Create', resourceId: clusterId + '/providers' } },
  { name: 'potential deletion', change: { changeType: 'Delete', resourceId: clusterId }, potential: true },
  { name: 'potential out-of-scope change', change: { changeType: 'Create',
    resourceId: resourceGroupId.replace('arrow-collector-rg', 'other-rg') + '/providers/Microsoft.Kusto/clusters/other' }, potential: true },
]) {
  test('R3 refuses ' + scenario.name + ' without a verified marker or create', async () => {
    const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
    stamp.subscription = subscription;
    const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
    azure.arm = async () => null;
    const variants = scenario.name === 'dangling provider suffix' ? [
      scenario.change,
      { changeType: 'Ignore', resourceId: identityId + '/providers//roleAssignments/role' },
      { changeType: 'Create', resourceId: identityId + '/providers/Microsoft.Authorization' },
      { changeType: 'Create', resourceId: identityId + '/providers/Microsoft.Authorization/roleAssignments' },
    ] : [scenario.change];
    for (const change of variants) {
      const calls: string[] = [], logs: string[] = [];
      const originalLog = console.log;
      console.log = (...values) => logs.push(values.join(' '));
      azure.cli = async (...args) => {
        calls.push(args[2]!);
        return scenario.potential ? { status: 'Succeeded', changes: [], potentialChanges: [change] } :
          { status: 'Succeeded', changes: [change] };
      };
      try { await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), ArmPreviewFailure); }
      finally { console.log = originalLog; }
      assert.deepEqual(calls, ['what-if']);
      assert.equal(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED')), false);
    }
  });
}

for (const properties of [
  { changes: [{ changeType: 'Create', resourceId: clusterId }] },
  { changes: [], potentialChanges: [{ changeType: 'Ignore', resourceId: identityId +
    '/providers/Microsoft.Authorization/roleAssignments/identity-role' }] },
]) {
  test('R3 accepts the documented REST properties envelope and validates both lists', async () => {
    const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
    stamp.subscription = subscription;
    const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
    azure.arm = async () => null;
    const calls: string[] = [], logs: string[] = [];
    const originalLog = console.log;
    console.log = (...values) => logs.push(values.join(' '));
    azure.cli = async (...args) => {
      calls.push(args[2]!);
      if (args[2] === 'what-if') return { status: 'Succeeded', properties };
      throw new Failure('DeploymentFailed: simulated create rejection');
    };
    try { await assert.rejects(provision(azure, '', 0, '/tmp/arrowcol-preview-test-output', true, {}, true), /DeploymentFailed/); }
    finally { console.log = originalLog; }
    assert.deepEqual(calls, ['what-if', 'create']);
    assert.ok(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED ') && line.endsWith(' changes=1')));
  });
}

test('R3 verifies the observed role reference against the live identity and rejects changes without that proof', async () => {
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
  const role = observedCliPreview.changes.find((item: { changeType: string }) => item.changeType === 'Modify');
  let livePrincipal: unknown = role.before.properties.principalId;
  let identityReads = 0;
  azure.arm = async (method, resource, _body, optional) => {
    assert.equal(method, 'GET');
    if (!optional) {
      assert.equal(resource, identityId);
      identityReads++;
      return { properties: { principalId: livePrincipal } };
    }
    return null;
  };
  const originalLog = console.log;
  const logs: string[] = [];
  console.log = (...values) => logs.push(values.join(' '));
  try {
    for (const nested of [false, true]) {
      const calls: string[] = [];
      identityReads = 0;
      logs.length = 0;
      azure.cli = async (...args) => {
        calls.push(args[2]!);
        if (args[2] === 'create') throw new Failure('DeploymentFailed: simulated create rejection');
        if (nested) {
          const { changes, potentialChanges, ...rest } = observedCliPreview;
          return { ...rest, properties: { changes, potentialChanges } };
        }
        return observedCliPreview;
      };
      await assert.rejects(deploy(azure, fresh_authority(), false, true), /DeploymentFailed/);
      assert.deepEqual(calls, ['what-if', 'create']);
      assert.equal(identityReads, 1);
      assert.ok(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED ')));
    }
    for (const fault of ['principal mismatch', 'unexpected assignment', 'role definition delta', 'Delete', 'missing before',
      'nested delta', 'different identity expression', 'changed snapshot scope']) {
      const item = structuredClone(role);
      livePrincipal = role.before.properties.principalId;
      if (fault === 'principal mismatch') livePrincipal = subscription;
      if (fault === 'unexpected assignment') {
        item.resourceId += '-other';
        for (const snapshot of [item.before, item.after]) {
          snapshot.id = item.resourceId;
          snapshot.name += '-other';
        }
      }
      if (fault === 'role definition delta') {
        item.after.properties.roleDefinitionId += '-other';
        item.delta.push({ path: 'properties.roleDefinitionId', propertyChangeType: 'Modify',
          before: item.before.properties.roleDefinitionId, after: item.after.properties.roleDefinitionId });
      }
      if (fault === 'Delete') item.changeType = 'Delete';
      if (fault === 'missing before') delete item.before;
      if (fault === 'nested delta') item.delta[0].children = [
        { path: 'properties.roleDefinitionId', propertyChangeType: 'Delete', before: 'role' },
      ];
      if (fault === 'different identity expression') {
        item.after.properties.principalId = item.after.properties.principalId.replace('arrow-collector', 'other');
        item.delta[0].after = item.after.properties.principalId;
      }
      if (fault === 'changed snapshot scope') item.after.properties.scope = resourceGroupId;
      const calls: string[] = [];
      logs.length = 0;
      azure.cli = async (...args) => {
        calls.push(args[2]!);
        return { status: 'Succeeded', changes: [item] };
      };
      await assert.rejects(deploy(azure, fresh_authority(), false, true), ArmPreviewFailure, fault);
      assert.deepEqual(calls, ['what-if'], fault);
      assert.equal(logs.some(line => line.startsWith('ARM_PREVIEW_VERIFIED')), false, fault);
    }
  } finally { console.log = originalLog; }
});

test('what-if allowlist covers every resource type in the compiled main Bicep template', () => {
  const compiled = spawnSync('az', ['bicep', 'build', '--file', 'infra/azure/main.bicep', '--stdout'],
    { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  assert.ifError(compiled.error);
  assert.equal(compiled.status, 0, compiled.stderr);
  const template = JSON.parse(compiled.stdout);
  const types = new Set<string>();
  const runtimeRoles: string[] = [];
  const visit = (value: any): void => {
    for (const resource of value.resources ?? []) {
      if (typeof resource.type === 'string') types.add(resource.type.toLowerCase());
      if (['microsoft.resources/resourcegroups', 'microsoft.managedidentity/userassignedidentities'].includes(resource.type?.toLowerCase())) {
        assert.deepEqual(Object.keys(resource).sort(), ['apiVersion', 'location', 'name', 'tags', 'type'],
          'Shared-resource Modify verification must be revisited if Bicep manages additional fields.');
      }
      if (resource.type?.toLowerCase() === 'microsoft.authorization/roleassignments' &&
          /(?:\.principalId|runtimePrincipalId)/.test(resource.properties?.principalId ?? '')) {
        const seed = resource.name.match(/'([^']+)'\)\]$/)?.[1];
        const roleExpression = resource.properties.roleDefinitionId;
        const variable = roleExpression.match(/^\[variables\('([^']+)'\)\]$/)?.[1];
        const role = (variable ? value.variables[variable] : roleExpression).match(/[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}/)?.[0];
        assert.ok(seed && role, 'Runtime role assignments must retain deterministic names and explicit role definitions.');
        runtimeRoles.push(seed + ':' + role);
      }
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
  const stamp = defaultStampConfig({ AZURE_STAMP: '89152' });
  stamp.subscription = subscription;
  stamp.storageReadLogDestination = 'storage';
  const expectedRoles = arm_what_if_runtime_roles(new Config({ AZURE_SUBSCRIPTION_ID: subscription }, stamp));
  assert.deepEqual(runtimeRoles.sort(), expectedRoles.map(role => role.seed + ':' + role.role.split('/').at(-1)).sort());
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
    before: fullSnapshot(identityId, { owner: 'old' }), after: { ...fullSnapshot(identityId, { owner: 'new' }), location: 'eastus2' },
    delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'old', after: 'new' }] }] } },
  { name: 'tag removal', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: fullSnapshot(identityId, { retired: 'old' }), after: fullSnapshot(identityId, {}),
    delta: [{ path: 'tags.retired', propertyChangeType: 'Delete', before: 'old' }] }] } },
  { name: 'whole tags object removal', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: fullSnapshot(identityId, { retired: 'old' }), after: fullSnapshot(identityId, {}),
    delta: [{ path: 'tags', propertyChangeType: 'Modify', before: { retired: 'old' }, after: {} }] }] } },
  { name: 'replacement-sensitive Modify', result: { changes: [{ changeType: 'Modify', resourceId: identityId,
    before: { ...fullSnapshot(identityId, { owner: 'old' }), location: 'eastus2' }, after: fullSnapshot(identityId, { owner: 'new' }),
    delta: [{ path: 'location', propertyChangeType: 'Modify', before: 'eastus2', after: 'canadacentral' }] }] } },
  { name: 'ambiguous flat and properties envelopes', result: { changes: [],
    properties: { changes: [{ changeType: 'Delete', resourceId: clusterId }] } } },
  { name: 'malformed potential changes list', result: { changes: [], potentialChanges: {} } },
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

test('provision permits synthetic REST tag creation; this Modify shape is not provider-observed', async () => {
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
    if (args[2] === 'what-if') return { status: 'Succeeded', properties: { changes: [
      { changeType: 'Modify', resourceId: resourceGroupId,
        before: fullSnapshot(resourceGroupId, { owner: 'previous' }),
        after: fullSnapshot(resourceGroupId, { owner: 'current' }),
        delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'previous', after: 'current' }] },
      { changeType: 'Modify', resourceId: identityId,
        before: fullSnapshot(identityId, { owner: 'previous' }),
        after: fullSnapshot(identityId, { owner: 'current' }),
        delta: [{ path: 'tags.owner', propertyChangeType: 'Modify', before: 'previous', after: 'current' }] },
    ] } };
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
