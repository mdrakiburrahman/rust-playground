import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { assertNoStampNetworkOverlap, defaultStampConfig, parseStampConfig } from './stamp.js';

const example = fs.readFileSync(new URL('./stamp.example.yaml', import.meta.url), 'utf8');

test('loads the approved resource group, stamp and AKS baseline from YAML', () => {
  const config = parseStampConfig(example);
  assert.equal(config.stamp, 1);
  assert.equal(config.resourceGroup, 'arrow-collector-rg');
  assert.equal(config.location, 'eastus2');
  assert.equal(config.aks.version, '1.35.6');
  assert.equal(config.aks.systemPool.vmSize, 'Standard_D8s_v6');
  assert.deepEqual(config.aks.userPools.map(pool => [pool.name, pool.initialCount, pool.minCount, pool.maxCount]),
    [['benchd64', 0, 0, 1], ['benche64', 0, 0, 1]]);
  assert.equal(config.aks.network.dnsServiceIp, '10.242.0.10');
  assert.equal(config.storageReadLogDestination, 'storage');
  assert.equal(defaultStampConfig().storageReadLogDestination, 'log-analytics');
  assert.throws(() => parseStampConfig(example.replace('storageReadLogDestination: storage', 'storageReadLogDestination: metrics')), /invalid/);
});

test('validates zero-scaled benchmark user pools and rejects invalid pool configuration', () => {
  const config = parseStampConfig(example);
  assert.deepEqual(config.aks.userPools.map(pool => pool.vmSize), ['Standard_D64ds_v6', 'Standard_E64ds_v6']);
  assert.equal(defaultStampConfig().aks.userPools[0]?.minCount, 0);
  assert.throws(() => parseStampConfig(example.replace('initialCount: 0', 'initialCount: 2')), /counts must satisfy/);
  assert.throws(() => parseStampConfig(example.replace('name: benche64', 'name: benchd64')), /names must be unique/);
  assert.throws(() => parseStampConfig(example.replace('maxCount: 1', 'maxCount: 0')), /maxCount must be an integer/);
});

test('rejects invalid stamps, resource-group scope and network overlaps', () => {
  assert.throws(() => parseStampConfig(example.replace('stamp: 1', 'stamp: 0')), /stamp must be an integer/);
  assert.throws(() => parseStampConfig(example.replace('arrow-collector-rg', 'rust-playground')), /resourceGroup is invalid/);
  assert.throws(() => parseStampConfig(example.replace('apiServerSubnetIpv4Cidr: 10.240.1.0/28',
    'apiServerSubnetIpv4Cidr: 10.240.0.0/28')), /non-overlapping/);
  assert.throws(() => parseStampConfig(example.replace('serviceIpv4Cidr: 10.242.0.0/16',
    'serviceIpv4Cidr: 10.242.0.0/24')), /prefix 16\.\.16/);
  assert.throws(() => parseStampConfig(example.replace('serviceIpv4Cidr: 10.242.0.0/16',
    'serviceIpv4Cidr: 172.31.0.0/16')), /platform-reserved/);
  assert.throws(() => parseStampConfig(example.replace('podIpv4Cidr: 10.241.0.0/16',
    'podIpv4Cidr: 172.30.0.0/16')), /platform-reserved/);
  assert.throws(() => parseStampConfig(example.replace('podIpv4Cidr: 10.241.0.0/16',
    'podIpv4Cidr: 10.240.0.0/16')), /must not overlap/);
});

test('bounds YAML and environment stamps to the Kusto cluster name limit', () => {
  assert.equal(parseStampConfig(example.replace('stamp: 1', 'stamp: 999999')).stamp, 999999);
  assert.equal(defaultStampConfig({ AZURE_STAMP: '999999' }).stamp, 999999);
  for (const stamp of [1000000, 99999999999]) {
    assert.throws(() => parseStampConfig(example.replace('stamp: 1', `stamp: ${stamp}`)), /stamp must be an integer in 1\.\.999999/);
    assert.throws(() => defaultStampConfig({ AZURE_STAMP: String(stamp) }), /Kusto cluster name limit/);
  }
});

test('requires explicit target defaults and rejects a different group or region', () => {
  assert.equal(defaultStampConfig().stamp, 1);
  assert.throws(() => defaultStampConfig({ AZURE_RESOURCE_GROUP: 'rust-playground' }), /exact arrow-collector-rg/);
  assert.throws(() => defaultStampConfig({ AZURE_LOCATION: 'westus2' }), /qualified eastus2/);
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
  assert.throws(() => assertNoStampNetworkOverlap(config, ['10.240.64.0/18']), /overlaps an existing Azure VNet/);
  assert.throws(() => assertNoStampNetworkOverlap(config, ['fd20:240:2::/64']), /overlaps an existing Azure VNet/);
  assert.doesNotThrow(() => assertNoStampNetworkOverlap(config, ['10.0.0.0/16', 'fd00:0:0040::/56']));
});
