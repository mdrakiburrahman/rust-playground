import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import { assertNoStampNetworkOverlap, defaultStampConfig, parseStampConfig } from './stamp.js';
import { generatePools, poolName } from './sku-pools.js';
import { stringify } from 'yaml';

const example = fs.readFileSync(new URL('./stamp.example.yaml', import.meta.url), 'utf8');

test('loads the approved resource group, stamp and AKS baseline from YAML', () => {
  const config = parseStampConfig(example);
  assert.equal(config.stamp, '1');
  assert.equal(config.resourceGroup, 'arrow-collector-rg');
  assert.equal(config.location, 'canadacentral');
  assert.equal(config.aks.version, '1.35.6');
  assert.equal(config.aks.systemPool.vmSize, 'Standard_D8ds_v6');
  const defaults = defaultStampConfig();
  assert.equal(defaults.aks.systemPool.vmSize, config.aks.systemPool.vmSize);
  assert.equal(defaults.aks.systemPool.maxCount, config.aks.systemPool.maxCount);
  assert.equal(defaults.aks.network.vnetIpv4Cidr, config.aks.network.vnetIpv4Cidr);
  assert.deepEqual(config.aks.userPools.map(pool => [pool.name, pool.initialCount, pool.minCount, pool.maxCount]),
    [['benchd64', 0, 0, 4], ['benche64', 0, 0, 4]]);
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
  assert.throws(() => parseStampConfig(example.replace('initialCount: 0', 'initialCount: 5')), /counts must satisfy/);
  assert.throws(() => parseStampConfig(example.replace('name: benche64', 'name: benchd64')), /names must be unique/);
  assert.throws(() => parseStampConfig(example.replace('maxCount: 4', 'maxCount: 0')), /maxCount must be an integer/);
  assert.deepEqual(config.aks.userPools.map(pool => pool.role), ['client', 'arrowcol']);
  assert.throws(() => parseStampConfig(example.replace('role: client', 'role: system')), /role is invalid/);
  const template = fs.readFileSync(new URL('./aks.bicep', import.meta.url), 'utf8');
  assert.ok(template.includes("nodeTaints: ['arrow-collector/role=${userPool.role}:NoSchedule']"));
  assert.ok(template.includes("'arrow-collector/role': userPool.role, 'arrow-collector/sku': userPool.vmSize"));
  assert.equal(parseStampConfig(example.replace('maxCount: 4', 'maxCount: 100')).aks.userPools[0]?.maxCount, 100);
  assert.throws(() => parseStampConfig(example.replaceAll('maxCount: 4', 'maxCount: 100')),
    /need 270 nodes; the node subnet fits 251 and the pod CIDR fits 256/);
});

test('generates deterministic quota-budgeted pools without modifying preserved pools', () => {
  const config = parseStampConfig(example);
  const sku = (vmSize: string, family: string, vcpus: number, quotaVcpus = vcpus) => ({
    vmSize, family, vcpus, quotaVcpus, memoryGiB: vcpus * 4, networkGbps: 10,
    zones: ['1', '2', '3'], restricted: false, aksEligible: true, architecture: 'x64',
    familyVcpuLimit: 10000, familyVcpuUsed: 0,
  });
  const inventory = { schema: 'arrowcol-sku-inventory/v1', region: config.location, capturedAt: '2026-01-01T00:00:00Z',
    regionalVcpu: { limit: 10000, used: 0 }, skus: [
      { ...sku('Standard_D8ds_v6', 'd', 8), aksEligible: false }, sku('Standard_D64ds_v6', 'd', 64), sku('Standard_E64ds_v6', 'e', 64),
      sku('Standard_LongSkuName128_v1', 'x', 128), { ...sku('Standard_LongSkuName128-64_v1', 'x', 64, 128), networkGbps: 20 },
      { ...sku('Standard_Bad_v1', 'x', 32), restricted: true },
      { ...sku('Standard_Arm_v1', 'x', 32), architecture: 'arm64' },
    ] };
  const result = generatePools(inventory, config);
  assert.deepEqual(result.userPools.slice(0, 2), config.aks.userPools);
  assert.equal(result.collector.vmSize, 'Standard_LongSkuName128_v1');
  assert.equal(result.newPools.filter(pool => pool.role === 'arrowcol').length, 1);
  assert.equal(result.newPools.length, 2);
  assert.ok(result.newPools.every(pool => /^[a-z][a-z0-9]{0,11}$/.test(pool.name) && pool.initialCount === 0 && pool.minCount === 0));
  assert.equal(new Set(result.newPools.map(pool => pool.name)).size, 2);
  assert.equal(result.charges[1]?.quotaVcpus, 128);
  for (const quotaVcpus of [undefined, null, 63]) {
    const invalid = { ...inventory, skus: inventory.skus.map((s, i) => i === 4 ? { ...s, quotaVcpus } : s) };
    assert.throws(() => generatePools(invalid, config), /quotaVcpus must be an integer >= 64/);
  }
  const { quotaVcpus: _omitted, ...withoutQuota } = inventory.skus[4]!;
  const missing = { ...inventory, skus: inventory.skus.map((s, i) => i === 4 ? withoutQuota : s) };
  assert.throws(() => generatePools(missing, config), /quotaVcpus must be an integer >= 64/);
  assert.deepEqual(generatePools({ ...inventory, skus: [...inventory.skus].reverse() }, config).newPools, result.newPools);
  assert.equal(generatePools(inventory, config, { arrowcolSku: 'Standard_LongSkuName128-64_v1' }).collector.vmSize, 'Standard_LongSkuName128-64_v1');
  assert.throws(() => generatePools(inventory, config, { arrowcolSku: 'Standard_Arm_v1' }), /collector fits/);
  assert.ok(result.dropped.some(entry => entry.reason === 'x64 architecture not established'));
  assert.equal(generatePools(inventory, config, { maxPools: 3 }).newPools.length, 1);
  assert.equal(generatePools(inventory, config, { maxPerPool: 2 }).newPools[0]?.maxCount, 2);
  const ordered = structuredClone(inventory);
  ordered.skus[4]!.vcpus = 128;
  assert.equal(generatePools(ordered, config).collector.vmSize, 'Standard_LongSkuName128-64_v1');
  const missingArchitecture = { ...ordered, skus: ordered.skus.map((s, i) => i === 4 ? { ...s, architecture: undefined } : s) };
  assert.equal(generatePools(missingArchitecture, config).collector.vmSize, 'Standard_LongSkuName128_v1');
  const collisionConfig = structuredClone(config);
  collisionConfig.aks.userPools[0]!.name = poolName('Standard_LongSkuName128_v1');
  assert.throws(() => generatePools(inventory, collisionConfig), /collision/);

  const constrained = structuredClone(inventory);
  constrained.skus.filter(s => s.family === 'x').forEach(s => { s.familyVcpuLimit = 300; });
  assert.equal(generatePools(constrained, config).newPools.length, 1);
  constrained.skus.filter(s => s.family === 'x').forEach(s => { s.familyVcpuLimit = 200; });
  assert.throws(() => generatePools(constrained, config, { arrowcolSku: 'Standard_LongSkuName128-64_v1' }), /collector fits/);
  constrained.skus.filter(s => s.family === 'x').forEach(s => { s.familyVcpuLimit = 384; });
  assert.equal(generatePools(constrained, config).newPools.length, 2);
  constrained.skus.filter(s => s.family === 'x').forEach(s => { s.familyVcpuLimit = 383; });
  assert.equal(generatePools(constrained, config).newPools.length, 1);
  const regional = structuredClone(inventory);
  regional.regionalVcpu.limit = 850;
  assert.equal(generatePools(regional, config).newPools.length, 1);
  constrained.regionalVcpu.limit = 200;
  assert.throws(() => generatePools(constrained, config), /quota/);
});

test('accepts 99 zero-minimum pools while accounting for their upgrade surge', () => {
  const config = parseStampConfig(example);
  const pools = Array.from({ length: 99 }, (_, index) => ({
    name: `p${index}`, role: index === 0 ? 'arrowcol' : 'client', vmSize: 'Standard_D8ds_v6',
    initialCount: 0, minCount: 0, maxCount: 1, maxPods: 250, availabilityZones: [],
  }));
  assert.equal(parseStampConfig(stringify({ ...config, aks: { ...config.aks, userPools: pools } })).aks.userPools.length, 99);
  pools[0]!.maxCount = 100;
  assert.throws(() => parseStampConfig(stringify({ ...config, aks: { ...config.aks, userPools: pools } })), /upgrade surge/);
  const template = fs.readFileSync(new URL('./sku-pools.bicep', import.meta.url), 'utf8');
  assert.ok(template.includes("nodeTaints: ['arrow-collector/role=${pool.role}:NoSchedule']"));
  assert.ok(template.includes("'arrow-collector/sku': pool.vmSize"));
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
    assert.equal(parseStampConfig(example.replace('location: canadacentral', 'location: ' + location)).location, location);
  }
  assert.throws(() => defaultStampConfig({ AZURE_RESOURCE_GROUP: 'rust-playground' }), /exact arrow-collector-rg/);
  assert.throws(() => defaultStampConfig({ AZURE_LOCATION: 'westus2' }), /approved eastus2 and canadacentral/);
  assert.throws(() => parseStampConfig(example.replace('location: canadacentral', 'location: westus2')), /location is invalid/);
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