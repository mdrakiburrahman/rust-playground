import fs from 'node:fs';
import path from 'node:path';
import { isIP } from 'node:net';
import { parse as parseYaml } from 'yaml';
import { fileURLToPath } from 'node:url';
import { Failure } from './authority.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');

export interface StampConfig {
  subscription: string;
  stamp: number;
  resourceGroup: 'arrow-collector-rg';
  location: 'eastus2';
  aks: {
    version: string;
    skuTier: 'Free' | 'Premium';
    namespace: string;
    serviceAccount: string;
    systemPool: { name: string; vmSize: string; initialCount: number; minCount: number; maxCount: number; maxPods: number; availabilityZones: string[] };
    network: {
      vnetIpv4Cidr: string; vnetIpv6Cidr: string; nodeSubnetIpv4Cidr: string; nodeSubnetIpv6Cidr: string;
      apiServerSubnetIpv4Cidr: string; podIpv4Cidr: string; podIpv6Cidr: string;
      serviceIpv4Cidr: string; serviceIpv6Cidr: string; dnsServiceIp: string;
      egressIpv4IpTags: Array<{ ipTagType: string; tag: string }>;
      egressIpv6IpTags: Array<{ ipTagType: string; tag: string }>;
    };
  };
  publisherFederatedCredentials: Array<{ name: string; issuer: string; subject: string }>;
}

const NAME = /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/;
const FIC_NAME = /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/;
const DEFAULT_AKS: StampConfig['aks'] = {
  version: '1.35.6', skuTier: 'Free', namespace: 'arrow-collector', serviceAccount: 'arrow-collector',
  systemPool: { name: 'systempool', vmSize: 'Standard_D8s_v6', initialCount: 3, minCount: 3, maxCount: 6, maxPods: 250, availabilityZones: ['1', '2', '3'] },
  network: { vnetIpv4Cidr: '10.240.0.0/16', vnetIpv6Cidr: 'fd20:240::/48', nodeSubnetIpv4Cidr: '10.240.0.0/24',
    nodeSubnetIpv6Cidr: 'fd20:240:0:1::/64', apiServerSubnetIpv4Cidr: '10.240.1.0/28', podIpv4Cidr: '172.30.0.0/16',
    podIpv6Cidr: 'fd20:240:1::/64', serviceIpv4Cidr: '172.31.0.0/16', serviceIpv6Cidr: 'fd20:240:2::/108',
    dnsServiceIp: '172.31.0.10', egressIpv4IpTags: [], egressIpv6IpTags: [] },
};

function object(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure(`${name} must be a mapping.`);
  return value as Record<string, unknown>;
}

function keys(value: Record<string, unknown>, allowed: readonly string[], name: string): void {
  const extra = Object.keys(value).filter(key => !allowed.includes(key));
  if (extra.length) throw new Failure(`${name} contains unsupported key(s): ${extra.join(', ')}.`);
}

function text(value: unknown, name: string, pattern: RegExp, maximum = 255): string {
  if (typeof value !== 'string' || value.length > maximum || !pattern.test(value)) throw new Failure(`${name} is invalid.`);
  return value;
}

function integer(value: unknown, name: string, minimum: number, maximum: number): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum || value > maximum)
    throw new Failure(`${name} must be an integer in ${minimum}..${maximum}.`);
  return value;
}

function cidr(value: unknown, name: string, version: 4 | 6, prefixMinimum: number, prefixMaximum: number): string {
  if (typeof value !== 'string') throw new Failure(`${name} must be a CIDR string.`);
  const [address, rawPrefix, extra] = value.split('/');
  const prefix = Number(rawPrefix);
  if (extra !== undefined || !address || isIP(address) !== version || !/^\d+$/.test(rawPrefix ?? '') ||
      prefix < prefixMinimum || prefix > prefixMaximum)
    throw new Failure(`${name} must be a valid IPv${version} CIDR with prefix ${prefixMinimum}..${prefixMaximum}.`);
  return value;
}

function ipv4Number(address: string): bigint {
  return address.split('.').reduce((result, part) => result * 256n + BigInt(part), 0n);
}

function ipv4Range(value: string): [bigint, bigint] {
  const [address, prefixText] = value.split('/');
  const hostBits = 32n - BigInt(prefixText!);
  const mask = ((1n << 32n) - 1n) ^ ((1n << hostBits) - 1n);
  const start = ipv4Number(address!) & mask;
  return [start, start + ((1n << hostBits) - 1n)];
}

function contains(outer: string, inner: string): boolean {
  const [outerStart, outerEnd] = ipv4Range(outer);
  const [innerStart, innerEnd] = ipv4Range(inner);
  return outerStart <= innerStart && innerEnd <= outerEnd;
}

function withinRange(outer: string, inner: string): boolean {
  const left = range(outer), right = range(inner);
  return left.version === right.version && left.start <= right.start && right.end <= left.end;
}

function cidrsOverlap(leftCidr: string, rightCidr: string): boolean {
  const left = range(leftCidr), right = range(rightCidr);
  return left.version === right.version && left.start <= right.end && right.start <= left.end;
}

function overlaps(left: string, right: string): boolean {
  const [leftStart, leftEnd] = ipv4Range(left);
  const [rightStart, rightEnd] = ipv4Range(right);
  return leftStart <= rightEnd && rightStart <= leftEnd;
}

function ipv6Number(address: string): bigint {
  const [leftText, rightText] = address.toLowerCase().split('::');
  const left = leftText ? leftText.split(':') : [];
  const right = rightText ? rightText.split(':') : [];
  const words = address.includes('::') ? [...left, ...Array(8 - left.length - right.length).fill('0'), ...right] : left;
  if (words.length !== 8) throw new Failure('Invalid IPv6 address in network configuration.');
  return words.reduce((value, word) => value * 65536n + BigInt(parseInt(word, 16)), 0n);
}

function range(value: string): { version: 4 | 6; start: bigint; end: bigint } {
  const [address, prefixText] = value.split('/');
  const version = isIP(address!) as 4 | 6;
  const bits = version === 4 ? 32n : 128n;
  const prefix = BigInt(prefixText!);
  const hostBits = bits - prefix;
  const addressNumber = version === 4 ? ipv4Number(address!) : ipv6Number(address!);
  const mask = ((1n << bits) - 1n) ^ ((1n << hostBits) - 1n);
  const start = addressNumber & mask;
  return { version, start, end: start + ((1n << hostBits) - 1n) };
}

export function assertNoStampNetworkOverlap(config: StampConfig, externalPrefixes: string[]): void {
  const network = config.aks.network;
  const requested = [network.vnetIpv4Cidr, network.nodeSubnetIpv4Cidr, network.apiServerSubnetIpv4Cidr,
    network.podIpv4Cidr, network.serviceIpv4Cidr, network.vnetIpv6Cidr, network.nodeSubnetIpv6Cidr,
    network.podIpv6Cidr, network.serviceIpv6Cidr];
  for (const candidate of requested) {
    const left = range(candidate);
    for (const existing of externalPrefixes) {
      if (!existing || isIP(existing.split('/')[0] ?? '') === 0) continue;
      const right = range(existing);
      if (left.version === right.version && left.start <= right.end && right.start <= left.end)
        throw new Failure('Configured AKS network range overlaps an existing Azure VNet range: ' + candidate + ' conflicts with ' + existing + '.');
    }
  }
}

export function defaultStampConfig(values: Record<string, string | undefined> = {}): StampConfig {
  const rawStamp = values.AZURE_STAMP ?? '1';
  if (!/^\d+$/.test(rawStamp) || !Number.isSafeInteger(Number(rawStamp)) || Number(rawStamp) < 1 || Number(rawStamp) > 99_999_999_999)
    throw new Failure('AZURE_STAMP must be a positive integer no greater than 99999999999.');
  const group = values.AZURE_RESOURCE_GROUP ?? 'arrow-collector-rg';
  if (group !== 'arrow-collector-rg') throw new Failure('Only the exact arrow-collector-rg resource group is authorized.');
  const location = values.AZURE_LOCATION ?? 'eastus2';
  if (location !== 'eastus2') throw new Failure('Only the qualified eastus2 region is authorized.');
  const aks = { ...DEFAULT_AKS, namespace: values.AZURE_KUBERNETES_NAMESPACE ?? DEFAULT_AKS.namespace,
    serviceAccount: values.AZURE_KUBERNETES_SERVICE_ACCOUNT ?? DEFAULT_AKS.serviceAccount };
  return { subscription: values.AZURE_SUBSCRIPTION_ID ?? '', stamp: Number(rawStamp), resourceGroup: group, location, aks, publisherFederatedCredentials: [] };
}

export function parseStampConfig(source: string): StampConfig {
  let parsed: unknown;
  try { parsed = parseYaml(source); }
  catch (error) { throw new Failure('AKS stamp configuration is not valid YAML.', { cause: error }); }
  const raw = object(parsed, 'AKS stamp configuration');
  keys(raw, ['subscription', 'stamp', 'resourceGroup', 'location', 'aks', 'publisherFederatedCredentials'], 'AKS stamp configuration');
  const subscription = text(raw.subscription, 'subscription', /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i).toLowerCase();
  const stamp = integer(raw.stamp, 'stamp', 1, 99_999_999_999);
  const resourceGroup = text(raw.resourceGroup, 'resourceGroup', /^arrow-collector-rg$/) as 'arrow-collector-rg';
  const location = text(raw.location, 'location', /^eastus2$/) as 'eastus2';
  const aksRaw = object(raw.aks, 'aks');
  keys(aksRaw, ['version', 'skuTier', 'namespace', 'serviceAccount', 'systemPool', 'network'], 'aks');
  const version = text(aksRaw.version, 'aks.version', /^\d+\.\d+\.\d+$/);
  const skuTier = text(aksRaw.skuTier, 'aks.skuTier', /^(Free|Premium)$/) as 'Free' | 'Premium';
  const namespace = text(aksRaw.namespace, 'aks.namespace', NAME, 63);
  const serviceAccount = text(aksRaw.serviceAccount, 'aks.serviceAccount', NAME, 63);
  const poolRaw = object(aksRaw.systemPool, 'aks.systemPool');
  keys(poolRaw, ['name', 'vmSize', 'initialCount', 'minCount', 'maxCount', 'maxPods', 'availabilityZones'], 'aks.systemPool');
  const systemPool = { name: text(poolRaw.name, 'aks.systemPool.name', /^[a-z][a-z0-9]{0,11}$/),
    vmSize: text(poolRaw.vmSize, 'aks.systemPool.vmSize', /^Standard_[A-Za-z0-9_]+$/),
    initialCount: integer(poolRaw.initialCount, 'aks.systemPool.initialCount', 1, 100),
    minCount: integer(poolRaw.minCount, 'aks.systemPool.minCount', 1, 100),
    maxCount: integer(poolRaw.maxCount, 'aks.systemPool.maxCount', 1, 100),
    maxPods: integer(poolRaw.maxPods, 'aks.systemPool.maxPods', 30, 250),
    availabilityZones: Array.isArray(poolRaw.availabilityZones) ? poolRaw.availabilityZones.map((zone, index) =>
      text(zone, `aks.systemPool.availabilityZones[${index}]`, /^[1-9]$/)) : (() => { throw new Failure('aks.systemPool.availabilityZones must be a list of supported zone numbers.'); })() };
  if (new Set(systemPool.availabilityZones).size !== systemPool.availabilityZones.length)
    throw new Failure('aks.systemPool.availabilityZones must not contain duplicates.');
  if (systemPool.minCount > systemPool.initialCount || systemPool.initialCount > systemPool.maxCount)
    throw new Failure('aks.systemPool counts must satisfy minCount <= initialCount <= maxCount.');
  const networkRaw = object(aksRaw.network, 'aks.network');
  const networkKeys = ['vnetIpv4Cidr', 'vnetIpv6Cidr', 'nodeSubnetIpv4Cidr', 'nodeSubnetIpv6Cidr',
    'apiServerSubnetIpv4Cidr', 'podIpv4Cidr', 'podIpv6Cidr', 'serviceIpv4Cidr', 'serviceIpv6Cidr', 'dnsServiceIp'] as const;
  keys(networkRaw, [...networkKeys, 'egressIpv4IpTags', 'egressIpv6IpTags'], 'aks.network');
  const ipTags = (value: unknown, name: string): Array<{ ipTagType: string; tag: string }> => {
    if (value === undefined) return [];
    if (!Array.isArray(value) || value.length > 10) throw new Failure(`${name} must be a list of at most 10 IP tags.`);
    const result = value.map((entry, index) => {
      const item = object(entry, `${name}[${index}]`);
      keys(item, ['ipTagType', 'tag'], `${name}[${index}]`);
      return {
        ipTagType: text(item.ipTagType, `${name}[${index}].ipTagType`, /^[A-Za-z][A-Za-z0-9]*$/, 64),
        tag: text(item.tag, `${name}[${index}].tag`, /^\/[A-Za-z0-9/_-]+$/, 128),
      };
    });
    if (new Set(result.map(item => `${item.ipTagType}\0${item.tag}`)).size !== result.length)
      throw new Failure(`${name} must not contain duplicate IP tags.`);
    return result;
  };
  const network = {
    vnetIpv4Cidr: cidr(networkRaw.vnetIpv4Cidr, 'aks.network.vnetIpv4Cidr', 4, 16, 24),
    vnetIpv6Cidr: cidr(networkRaw.vnetIpv6Cidr, 'aks.network.vnetIpv6Cidr', 6, 48, 64),
    nodeSubnetIpv4Cidr: cidr(networkRaw.nodeSubnetIpv4Cidr, 'aks.network.nodeSubnetIpv4Cidr', 4, 16, 24),
    nodeSubnetIpv6Cidr: cidr(networkRaw.nodeSubnetIpv6Cidr, 'aks.network.nodeSubnetIpv6Cidr', 6, 64, 64),
    apiServerSubnetIpv4Cidr: cidr(networkRaw.apiServerSubnetIpv4Cidr, 'aks.network.apiServerSubnetIpv4Cidr', 4, 16, 28),
    podIpv4Cidr: cidr(networkRaw.podIpv4Cidr, 'aks.network.podIpv4Cidr', 4, 16, 16),
    podIpv6Cidr: cidr(networkRaw.podIpv6Cidr, 'aks.network.podIpv6Cidr', 6, 64, 64),
    serviceIpv4Cidr: cidr(networkRaw.serviceIpv4Cidr, 'aks.network.serviceIpv4Cidr', 4, 16, 16),
    serviceIpv6Cidr: cidr(networkRaw.serviceIpv6Cidr, 'aks.network.serviceIpv6Cidr', 6, 108, 108),
    dnsServiceIp: text(networkRaw.dnsServiceIp, 'aks.network.dnsServiceIp', /^(?:\d{1,3}\.){3}\d{1,3}$/),
    egressIpv4IpTags: ipTags(networkRaw.egressIpv4IpTags, 'aks.network.egressIpv4IpTags'),
    egressIpv6IpTags: ipTags(networkRaw.egressIpv6IpTags, 'aks.network.egressIpv6IpTags'),
  };
  if (!contains(network.vnetIpv4Cidr, network.nodeSubnetIpv4Cidr) ||
      !contains(network.vnetIpv4Cidr, network.apiServerSubnetIpv4Cidr) ||
      overlaps(network.nodeSubnetIpv4Cidr, network.apiServerSubnetIpv4Cidr) ||
      overlaps(network.podIpv4Cidr, network.serviceIpv4Cidr))
    throw new Failure('AKS IPv4 VNet, subnet, pod and service CIDRs must be contained and non-overlapping.');
  const [serviceStart, serviceEnd] = ipv4Range(network.serviceIpv4Cidr);
  const dns = ipv4Number(network.dnsServiceIp);
  if (isIP(network.dnsServiceIp) !== 4 || dns < serviceStart || dns > serviceEnd) throw new Failure('aks.network.dnsServiceIp must be inside serviceIpv4Cidr.');
  const credentials = raw.publisherFederatedCredentials ?? [];
  if (!Array.isArray(credentials) || credentials.length > 20) throw new Failure('publisherFederatedCredentials must be a list of at most 20 entries.');
  const publisherFederatedCredentials = credentials.map((value, index) => {
    const fic = object(value, `publisherFederatedCredentials[${index}]`);
    keys(fic, ['name', 'issuer', 'subject'], `publisherFederatedCredentials[${index}]`);
    const name = text(fic.name, `publisherFederatedCredentials[${index}].name`, FIC_NAME, 120);
    const issuer = text(fic.issuer, `publisherFederatedCredentials[${index}].issuer`, /^https:\/\/[A-Za-z0-9.-]+(?:\/[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]*)?$/);
    const subject = text(fic.subject, `publisherFederatedCredentials[${index}].subject`, /^[A-Za-z0-9._:@/-]+$/);
    return { name, issuer, subject };
  });
  if (new Set(publisherFederatedCredentials.map(fic => fic.name)).size !== publisherFederatedCredentials.length ||
      new Set(publisherFederatedCredentials.map(fic => `${fic.issuer}\0${fic.subject}`)).size !== publisherFederatedCredentials.length)
    throw new Failure('publisherFederatedCredentials must have unique names and issuer/subject pairs.');
  return { subscription, stamp, resourceGroup, location, aks: { version, skuTier, namespace, serviceAccount, systemPool, network },
    publisherFederatedCredentials };
}

export function loadStampConfig(filename: string): StampConfig {
  try { return parseStampConfig(fs.readFileSync(path.resolve(ROOT, filename), 'utf8')); }
  catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT')
      throw new Failure(`AKS stamp config is missing at ${filename}; copy infra/azure/stamp.example.yaml and fill in the approved network and publisher entries.`);
    throw error;
  }
}

export const defaultStamp = DEFAULT_AKS;