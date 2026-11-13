import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { assertNoStampNetworkOverlap, defaultStampConfig, parseStampConfig } from './stamp.js';

const example = fs.readFileSync(new URL('./stamp.example.yaml', import.meta.url), 'utf8');

test('loads the approved resource group, stamp and AKS baseline from YAML', () => {
  const config = parseStampConfig(example);
  assert.equal(config.stamp, '1');
  assert.equal(config.resourceGroup, 'arrow-collector-rg');
  assert.equal(config.location, 'eastus2');
  assert.equal(config.aks.version, '1.35.6');
  assert.equal(config.aks.systemPool.vmSize, 'Standard_D8ds_v6');
  const defaults = defaultStampConfig();
  assert.equal(defaults.aks.systemPool.vmSize, config.aks.systemPool.vmSize);
  assert.equal(defaults.aks.systemPool.maxCount, config.aks.systemPool.maxCount);
  assert.equal(defaults.aks.network.vnetIpv4Cidr, config.aks.network.vnetIpv4Cidr);
  assert.deepEqual(config.aks.userPools.map(pool => [pool.name, pool.initialCount, pool.minCount, pool.maxCount]),
    [['benchd64', 0, 0, 1], ['benche64', 0, 0, 1]]);
  assert.equal(config.aks.network.dnsServiceIp, '10.247.0.10');
  assert.equal(config.storageReadLogDestination, 'storage');
  assert.equal(defaultStampConfig().storageReadLogDestination, 'log-analytics');
  assert.throws(() => parseStampConfig(example.replace('storageReadLogDestination: storage', 'storageReadLogDestination: metrics')), /invalid/);
});

test('network-policy enforcement is an explicit validated Cilium opt-in', () => {
  assert.equal(parseStampConfig(example).aks.networkDataplane, 'azure');
  assert.equal(defaultStampConfig().aks.networkDataplane, 'azure');
  const configured = example.replace('aks:', 'aks:\n  networkDataplane: cilium');
  assert.equal(parseStampConfig(configured).aks.networkDataplane, 'cilium');
  assert.throws(() => parseStampConfig(configured.replace('networkDataplane: cilium', 'networkDataplane: invalid')), /networkDataplane is invalid/);
  const template = fs.readFileSync(new URL('./aks.bicep', import.meta.url), 'utf8');
  assert.ok(template.includes("networkDataplane: aksConfig.?networkDataplane ?? 'azure'"));
  assert.ok(template.includes("networkPolicy: (aksConfig.?networkDataplane ?? 'azure') == 'cilium' ? 'cilium' : 'none'"));
});

test('validates zero-scaled benchmark user pools and rejects invalid pool configuration', () => {
  const config = parseStampConfig(example);
  assert.deepEqual(config.aks.userPools.map(pool => pool.vmSize), ['Standard_D64ds_v6', 'Standard_E64ds_v6']);
  assert.equal(defaultStampConfig().aks.userPools[0]?.minCount, 0);
  assert.throws(() => parseStampConfig(example.replace('initialCount: 0', 'initialCount: 2')), /counts must satisfy/);
  assert.throws(() => parseStampConfig(example.replace('name: benche64', 'name: benchd64')), /names must be unique/);
  assert.throws(() => parseStampConfig(example.replace('maxCount: 1', 'maxCount: 0')), /maxCount must be an integer/);
  assert.deepEqual(config.aks.userPools.map(pool => pool.role), ['client', 'arrowcol']);
  assert.throws(() => parseStampConfig(example.replace('role: client', 'role: system')), /role is invalid/);
  const template = fs.readFileSync(new URL('./aks.bicep', import.meta.url), 'utf8');
  assert.ok(template.includes("nodeTaints: ['arrow-collector/role=${userPool.role}:NoSchedule']"));
  assert.ok(template.includes("nodeLabels: { type: 'benchmark', 'arrow-collector/role': userPool.role }"));
  assert.equal(parseStampConfig(example.replace('maxCount: 1', 'maxCount: 100')).aks.userPools[0]?.maxCount, 100);
  assert.throws(() => parseStampConfig(example.replaceAll('maxCount: 1', 'maxCount: 100')),
    /need 270 nodes; the node subnet fits 251 and the pod CIDR fits 256/);
});

test('rejects invalid stamps, resource-group scope and network overlaps', () => {
  assert.throws(() => parseStampConfig(example.replace('stamp: 1', 'stamp: -1')), /stamp must be an integer/);
  assert.throws(() => parseStampConfig(example.replace('arrow-collector-rg', 'rust-playground')), /resourceGroup is invalid/);
  assert.throws(() => parseStampConfig(example.replace('apiServerSubnetIpv4Cidr: 10.245.1.0/28',
    'apiServerSubnetIpv4Cidr: 10.245.0.0/28')), /non-overlapping/);
  assert.throws(() => parseStampConfig(example.replace('serviceIpv4Cidr: 10.247.0.0/16',
    'serviceIpv4Cidr: 10.247.0.0/24')), /prefix 16\.\.16/);
  assert.throws(() => parseStampConfig(example.replace('serviceIpv4Cidr: 10.247.0.0/16',
    'serviceIpv4Cidr: 172.31.0.0/16')), /platform-reserved/);
  assert.throws(() => parseStampConfig(example.replace('podIpv4Cidr: 10.246.0.0/16',
    'podIpv4Cidr: 172.30.0.0/16')), /platform-reserved/);
  assert.throws(() => parseStampConfig(example.replace('podIpv4Cidr: 10.246.0.0/16',
    'podIpv4Cidr: 10.245.0.0/16')), /must not overlap/);
});

test('bounds YAML and environment stamps to the Kusto cluster name limit', () => {
  assert.equal(parseStampConfig(example.replace('stamp: 1', 'stamp: 999999')).stamp, '999999');
  assert.equal(defaultStampConfig({ AZURE_STAMP: '999999' }).stamp, '999999');
  assert.equal(parseStampConfig(example.replace('stamp: 1', "stamp: '00000'")).stamp, '00000');
  assert.equal(defaultStampConfig({ AZURE_STAMP: '00000' }).stamp, '00000');
  assert.throws(() => parseStampConfig(example.replace('stamp: 1', 'stamp: 00000')), /must be quoted as '00000'/);
  for (const stamp of ['0', '000000', '01']) assert.throws(() => defaultStampConfig({ AZURE_STAMP: stamp }), /AZURE_STAMP must be/);
  for (const stamp of [1000000, 99999999999]) {
    assert.throws(() => parseStampConfig(example.replace('stamp: 1', `stamp: ${stamp}`)), /stamp must be an integer in 1\.\.999999/);
    assert.throws(() => defaultStampConfig({ AZURE_STAMP: String(stamp) }), /AZURE_STAMP must be an integer in 1\.\.999999/);
  }
});

test('accepts both approved stamp regions without broadening the resource-group boundary', () => {
  assert.equal(defaultStampConfig().stamp, '1');
  for (const location of ['eastus2', 'canadacentral']) {
    assert.equal(defaultStampConfig({ AZURE_LOCATION: location }).location, location);
    assert.equal(parseStampConfig(example.replace('location: eastus2', 'location: ' + location)).location, location);
  }
  assert.throws(() => defaultStampConfig({ AZURE_RESOURCE_GROUP: 'rust-playground' }), /exact arrow-collector-rg/);
  assert.throws(() => defaultStampConfig({ AZURE_LOCATION: 'westus2' }), /approved eastus2 and canadacentral/);
  assert.throws(() => parseStampConfig(example.replace('location: eastus2', 'location: westus2')), /location is invalid/);
});
test('validates independently configured IPv4 and IPv6 egress allocation tags', () => {
  const configured = example.replace('egressIpv4IpTags: []',
    'egressIpv4IpTags:\n      - ipTagType: FirstPartyUsage\n        tag: /TestAllocation');
  const config = parseStampConfig(configured);
  assert.deepEqual(config.aks.network.egressIpv4IpTags, [{ ipTagType: 'FirstPartyUsage', tag: '/TestAllocation' }]);
  assert.deepEqual(config.aks.network.egressIpv6IpTags, []);
  assert.throws(() => parseStampConfig(configured.replace('/TestAllocation', 'invalid tag')), /tag is invalid/);
  assert.throws(() => parseStampConfig(configured.replace('ipTagType: FirstPartyUsage', 'ipTagType: FirstPartyUsage\n        bypass: true')), /unsupported key/);
  assert.throws(() => parseStampConfig(configured.replace('tag: /TestAllocation',
    'tag: /TestAllocation\n      - ipTagType: FirstPartyUsage\n        tag: /TestAllocation')), /duplicate IP tags/);
  assert.deepEqual(parseStampConfig(example.replace('    egressIpv4IpTags: []\n', '')).aks.network.egressIpv4IpTags, []);
});
test('validates publisher federation and rejects IPv4 or IPv6 overlap with Azure networks', () => {
  const publisher = example.replace('publisherFederatedCredentials: []',
    'publisherFederatedCredentials:\n  - name: github-publish\n    issuer: https://token.actions.githubusercontent.com\n    subject: repo:org@123/project@456:environment:publish');
  const config = parseStampConfig(publisher);
  assert.equal(config.publisherFederatedCredentials[0]?.subject, 'repo:org@123/project@456:environment:publish');
  assert.throws(() => assertNoStampNetworkOverlap(config, ['10.245.64.0/18']), /overlaps an existing Azure VNet/);
  assert.throws(() => assertNoStampNetworkOverlap(config, ['fd20:245:2::/64']), /overlaps an existing Azure VNet/);
  assert.doesNotThrow(() => assertNoStampNetworkOverlap(config, ['10.0.0.0/16', 'fd00:0:0040::/56']));
});

test('empty publisher allowlist is valid without relaxing configured-entry validation', () => {
  assert.deepEqual(parseStampConfig(example).publisherFederatedCredentials, []);
  const entry = '\n  - name: github-publish\n    issuer: https://token.actions.githubusercontent.com\n    subject: repo:org/project:environment:publish';
  const configured = example.replace('publisherFederatedCredentials: []', 'publisherFederatedCredentials:' + entry);
  assert.equal(parseStampConfig(configured).publisherFederatedCredentials.length, 1);
  assert.throws(() => parseStampConfig(configured.replace('issuer: https:', 'issuer: http:')), /issuer is invalid/);
  assert.throws(() => parseStampConfig(configured.replace('subject: repo:org/project:environment:publish', 'subject: ""')), /subject/);
  assert.throws(() => parseStampConfig(example.replace('publisherFederatedCredentials: []',
    'publisherFederatedCredentials:' + entry + entry)), /unique names and issuer\/subject pairs/);
  assert.throws(() => parseStampConfig(configured.replace('name: github-publish', 'name: github-publish\n    allowAnySubject: true')), /unsupported key/);
});