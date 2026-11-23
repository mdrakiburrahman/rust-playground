import { createHash } from 'node:crypto';

export type CollectorBuildProfile = {
  allocator: 'system' | 'mimalloc';
  lto: 'off' | 'thin' | 'fat';
  codegen_units: number;
  opt_level: 2 | 3;
  target_cpu: 'generic' | 'x86-64-v2' | 'x86-64-v3' | 'x86-64-v4';
};

const defaults: CollectorBuildProfile = {
  allocator: 'system', lto: 'off', codegen_units: 16, opt_level: 3, target_cpu: 'generic',
};
const fields = Object.keys(defaults) as Array<keyof CollectorBuildProfile>;

export function parseCollectorBuildProfile(value: unknown): CollectorBuildProfile | undefined {
  if (value == null) return undefined;
  if (typeof value !== 'object' || Array.isArray(value)) throw new Error('collector must be an object');
  const input = value as Record<string, unknown>;
  const profile = { ...defaults };
  let configured = false;
  for (const field of fields) {
    if (!Object.hasOwn(input, field)) continue;
    const item = input[field];
    if (item === null) throw new Error(`collector.${field} must not be null`);
    if (item === undefined) continue;
    configured = true;
    if (field === 'allocator' && (item === 'system' || item === 'mimalloc')) profile.allocator = item;
    else if (field === 'lto' && (item === 'off' || item === 'thin' || item === 'fat')) profile.lto = item;
    else if (field === 'codegen_units' && Number.isInteger(item) && (item as number) >= 1 && (item as number) <= 256) profile.codegen_units = item as number;
    else if (field === 'opt_level' && (item === 2 || item === 3)) profile.opt_level = item;
    else if (field === 'target_cpu' && (item === 'generic' || item === 'x86-64-v2' || item === 'x86-64-v3' || item === 'x86-64-v4')) profile.target_cpu = item;
    else if (field === 'codegen_units') throw new Error('collector.codegen_units must be an integer in 1..=256');
    else if (field === 'opt_level') throw new Error('collector.opt_level must be integer 2 or 3');
    else if (field === 'allocator') throw new Error('collector.allocator must be system or mimalloc');
    else if (field === 'lto') throw new Error('collector.lto must be off, thin or fat');
    else throw new Error('collector.target_cpu must be generic, x86-64-v2, x86-64-v3 or x86-64-v4');
  }
  return configured ? profile : undefined;
}

export function collectorBuildProfileKey(profile: CollectorBuildProfile): string {
  return ['allocator=', profile.allocator, ';lto=', profile.lto, ';codegen_units=', String(profile.codegen_units),
    ';opt_level=', String(profile.opt_level), ';target_cpu=', profile.target_cpu].join('');
}

export function collectorBuildProfileHash(profile: CollectorBuildProfile): string {
  return createHash('sha256').update(collectorBuildProfileKey(profile)).digest('hex');
}
