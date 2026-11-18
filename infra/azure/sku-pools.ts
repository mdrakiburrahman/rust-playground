import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { pathToFileURL } from 'node:url';
import { stringify } from 'yaml';
import { Failure } from './authority.js';
import { loadStampConfig, parseStampConfig, type StampConfig } from './stamp.js';

type Pool = StampConfig['aks']['userPools'][number];
type Sku = {
  vmSize: string; family: string; vcpus: number; quotaVcpus: number; memoryGiB: number;
  networkGbps: number | null; zones: string[]; restricted: boolean; aksEligible: boolean;
  familyVcpuLimit: number; familyVcpuUsed: number; architecture?: string;
};
type Inventory = { schema: string; region: string; capturedAt: string; regionalVcpu: { limit: number; used: number }; skus: Sku[] };
type Options = { arrowcolSku?: string; maxPerPool?: number; maxPools?: number };

function number(value: unknown, name: string, min = 0): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min)
    throw new Failure(`${name} must be an integer >= ${min}.`);
  return value;
}

function inventoryFrom(input: unknown): Inventory {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Failure('Inventory must be an object.');
  const raw = input as Record<string, unknown>;
  if (raw.schema !== 'arrowcol-sku-inventory/v1' || typeof raw.region !== 'string' ||
      typeof raw.capturedAt !== 'string' || !Number.isFinite(Date.parse(raw.capturedAt)) ||
      !raw.regionalVcpu || typeof raw.regionalVcpu !== 'object' || !Array.isArray(raw.skus))
    throw new Failure('Invalid inventory metadata.');
  const regional = raw.regionalVcpu as Record<string, unknown>;
  const regionalVcpu = { limit: number(regional.limit, 'regional limit'), used: number(regional.used, 'regional used') };
  const skus = raw.skus.map((value, i): Sku => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure(`Invalid SKU ${i}.`);
    const s = value as Record<string, unknown>;
    if (typeof s.vmSize !== 'string' || !/^Standard_[A-Za-z0-9_-]+$/.test(s.vmSize) ||
        typeof s.family !== 'string' || !s.family || typeof s.restricted !== 'boolean' || typeof s.aksEligible !== 'boolean' ||
        !Array.isArray(s.zones) || s.zones.some(z => typeof z !== 'string' || !/^[1-9]$/.test(z)) ||
        new Set(s.zones).size !== s.zones.length ||
        (s.networkGbps !== null && (typeof s.networkGbps !== 'number' || !Number.isFinite(s.networkGbps) || s.networkGbps <= 0)) ||
        typeof s.memoryGiB !== 'number' || !Number.isFinite(s.memoryGiB) || s.memoryGiB <= 0 ||
        (s.architecture !== undefined && typeof s.architecture !== 'string'))
      throw new Failure(`Invalid SKU record ${i}.`);
    const vcpus = number(s.vcpus, `${s.vmSize} vcpus`, 1);
    return { vmSize: s.vmSize, family: s.family, vcpus, quotaVcpus: number(s.quotaVcpus ?? vcpus, `${s.vmSize} quotaVcpus`, vcpus),
      memoryGiB: s.memoryGiB, networkGbps: s.networkGbps as number | null, zones: s.zones as string[],
      restricted: s.restricted, aksEligible: s.aksEligible, familyVcpuLimit: number(s.familyVcpuLimit, `${s.vmSize} family limit`),
      familyVcpuUsed: number(s.familyVcpuUsed, `${s.vmSize} family used`), architecture: s.architecture as string | undefined };
  });
  if (new Set(skus.map(s => s.vmSize.toLowerCase())).size !== skus.length) throw new Failure('Duplicate VM SKU in inventory.');
  return { schema: raw.schema, region: raw.region, capturedAt: raw.capturedAt, regionalVcpu, skus };
}

export function poolName(vmSize: string): string {
  return `p${createHash('sha256').update(vmSize.toLowerCase()).digest('hex').slice(0, 11)}`;
}

export function generatePools(input: unknown, config: StampConfig, options: Options = {}) {
  const inventory = inventoryFrom(input);
  if (inventory.region.toLowerCase() !== config.location) throw new Failure('Inventory region differs from stamp region.');
  const maxPerPool = number(options.maxPerPool ?? 1, 'maxPerPool', 1);
  const maxPools = number(options.maxPools ?? 99, 'maxPools', 1);
  if (maxPerPool > 100 || maxPools > 99 || config.aks.userPools.length >= maxPools)
    throw new Failure('Pool limits exceed AKS/stamp limits or leave no room for a new pool.');
  const ledger = new Map<string, { limit: number; used: number; remaining: number; surge: number }>();
  for (const sku of inventory.skus) {
    const family = sku.family.toLowerCase();
    const prior = ledger.get(family);
    if (prior && (prior.limit !== sku.familyVcpuLimit || prior.used !== sku.familyVcpuUsed))
      throw new Failure(`Inconsistent quota for family ${sku.family}.`);
    ledger.set(family, { limit: sku.familyVcpuLimit, used: sku.familyVcpuUsed,
      remaining: sku.familyVcpuLimit - sku.familyVcpuUsed, surge: 0 });
  }
  let regional = inventory.regionalVcpu.limit - inventory.regionalVcpu.used;
  let regionalSurge = 0;
  const bySku = new Map(inventory.skus.map(s => [s.vmSize.toLowerCase(), s]));
  const preservedFamilies = new Set<string>();
  const surge = (count: number) => Math.ceil(count * 33 / 100);
  const costs = (sku: Sku, nodes: number, surgeNodes: number) => {
    const family = ledger.get(sku.family.toLowerCase())!;
    const base = nodes * sku.quotaVcpus;
    const peak = surgeNodes * sku.quotaVcpus;
    return { family: base + Math.max(0, peak - family.surge), regional: base + Math.max(0, peak - regionalSurge), peak };
  };
  const reserve = (sku: Sku, nodes: number, surgeNodes: number) => {
    const family = ledger.get(sku.family.toLowerCase())!;
    const charge = costs(sku, nodes, surgeNodes);
    family.remaining -= charge.family;
    family.surge = Math.max(family.surge, charge.peak);
    regional -= charge.regional;
    regionalSurge = Math.max(regionalSurge, charge.peak);
  };
  for (const pool of [config.aks.systemPool, ...config.aks.userPools]) {
    const sku = bySku.get(pool.vmSize.toLowerCase());
    if (!sku) throw new Failure(`Inventory lacks preserved pool SKU ${pool.vmSize}; cannot reserve quota.`);
    preservedFamilies.add(sku.family.toLowerCase());
    reserve(sku, pool.maxCount - pool.initialCount, surge(pool.maxCount));
  }
  if (regional < 0 || [...preservedFamilies].some(family => ledger.get(family)!.remaining < 0))
    throw new Failure('Insufficient quota to reserve preserved pool maxima and upgrade surge.');
  const existing = new Set(config.aks.userPools.map(p => p.vmSize.toLowerCase()));
  const dropped: Array<{ vmSize: string; reason: string }> = [];
  const candidates = inventory.skus.filter(s => {
    let reason = '';
    if (s.restricted) reason = 'restricted';
    else if (!s.aksEligible) reason = 'not AKS eligible';
    else if (s.architecture !== 'x64') reason = 'x64 architecture not established';
    else if (existing.has(s.vmSize.toLowerCase())) reason = 'preserved pool already uses SKU';
    if (reason) dropped.push({ vmSize: s.vmSize, reason });
    return !reason;
  }).sort((a, b) => b.vcpus - a.vcpus || (b.networkGbps ?? -1) - (a.networkGbps ?? -1) ||
    b.memoryGiB - a.memoryGiB || a.vmSize.localeCompare(b.vmSize, 'en'));
  const fits = (s: Sku, n: number) => {
    const charge = costs(s, n, surge(n));
    return charge.regional <= regional && charge.family <= ledger.get(s.family.toLowerCase())!.remaining;
  };
  const collector = options.arrowcolSku
    ? candidates.find(s => s.vmSize.toLowerCase() === options.arrowcolSku!.toLowerCase())
    : candidates.find(s => fits(s, 1));
  if (!collector || !fits(collector, 1)) throw new Failure('No requested/eligible collector fits family and regional quota including surge.');
  const ordered = [collector, ...candidates.filter(s => s !== collector)];
  const names = new Set([config.aks.systemPool.name, ...config.aks.userPools.map(p => p.name)]);
  const newPools: Pool[] = [];
  const charges: Array<{ vmSize: string; quotaVcpus: number; nodes: number; surgeNodes: number }> = [];
  for (const sku of ordered) {
    if (config.aks.userPools.length + newPools.length >= maxPools) {
      dropped.push({ vmSize: sku.vmSize, reason: 'pool limit' }); continue;
    }
    let count = maxPerPool;
    while (count > 0 && !fits(sku, count)) count--;
    if (!count) { dropped.push({ vmSize: sku.vmSize, reason: 'family or regional quota including surge' }); continue; }
    const name = poolName(sku.vmSize);
    if (names.has(name)) throw new Failure(`Pool name collision for ${sku.vmSize}: ${name}.`);
    names.add(name);
    const pool: Pool = { name, role: sku === collector ? 'arrowcol' : 'client', vmSize: sku.vmSize,
      initialCount: 0, minCount: 0, maxCount: count, maxPods: 250, availabilityZones: [] };
    newPools.push(pool);
    charges.push({ vmSize: sku.vmSize, quotaVcpus: sku.quotaVcpus, nodes: count, surgeNodes: surge(count) });
    reserve(sku, count, surge(count));
    if (count < maxPerPool) dropped.push({ vmSize: sku.vmSize, reason: `maxCount reduced to ${count} by quota including surge` });
  }
  const userPools = [...config.aks.userPools, ...newPools];
  parseStampConfig(stringify({ ...config, aks: { ...config.aks, userPools } }));
  return { userPools, newPools, collector: { name: newPools[0]!.name, vmSize: collector.vmSize },
    charges, regionalRemaining: regional, dropped };
}

function main(args: string[]) {
  const values = new Map<string, string>();
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i], value = args[i + 1];
    if (!key || !['--inventory', '--stamp-config', '--arrowcol-sku', '--max-per-pool', '--max-pools'].includes(key) ||
        !value || value.startsWith('--') || values.has(key)) throw new Failure(`Invalid argument ${key ?? ''}.`);
    values.set(key, value);
  }
  const inventory = values.get('--inventory'), stamp = values.get('--stamp-config');
  if (!inventory || !stamp) throw new Failure('Usage: sku-pools.ts --inventory FILE --stamp-config FILE [--arrowcol-sku SKU] [--max-per-pool N] [--max-pools N]');
  const readNumber = (key: string) => {
    const value = values.get(key);
    if (value === undefined) return undefined;
    if (!/^[1-9]\d*$/.test(value)) throw new Failure(`${key} must be a positive integer.`);
    return Number(value);
  };
  const result = generatePools(JSON.parse(fs.readFileSync(inventory, 'utf8')), loadStampConfig(stamp), {
    arrowcolSku: values.get('--arrowcol-sku'), maxPerPool: readNumber('--max-per-pool'), maxPools: readNumber('--max-pools'),
  });
  process.stdout.write(JSON.stringify(result, null, 2) + '\n');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { main(process.argv.slice(2)); }
  catch (error) { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; }
}
