import { randomUUID } from 'node:crypto';

export class Failure extends Error {}
export class HttpFailure extends Failure {
  constructor(public code: number, message: string) { super(message); }
}
export class ControlConflict extends Failure {}
export class NotReady extends Failure {}
export class DeploymentFailed extends Failure {}
export type Metadata = Record<string, string>;
export type Registry = Record<string, { generation: string; state: 'active' | 'uncertain' }>;
export const canonical = (value: unknown): string => {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(value))
    throw new Failure('Publisher authority requires canonical UUIDs.');
  return value;
};
export const uuid = randomUUID;
export function canonical_json(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical_json).join(',') + ']';
  if (value !== null && typeof value === 'object')
    return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' +
      canonical_json((value as Record<string, unknown>)[key])).join(',') + '}';
  return JSON.stringify(value);
}
export function require_released(metadata: Metadata): void {
  if (metadata.occupancy !== 'released')
    throw new Failure('Cloud tests or a stop hold own the sandbox; wait for explicit release.');
}
export function publisher_registry(metadata: Metadata): Registry {
  if (metadata.publisherregistryversion !== '1')
    throw new Failure('Publisher registry is missing or has an unsupported version.');
  const raw = metadata.publishers;
  if (typeof raw !== 'string' || Buffer.byteLength(raw) > 4096)
    throw new Failure('Publisher registry exceeds its bounded metadata budget or is missing.');
  let registry: Registry;
  try { registry = JSON.parse(raw); } catch { throw new Failure('Publisher registry JSON is malformed.'); }
  if (!registry || Array.isArray(registry) || typeof registry !== 'object' || Object.keys(registry).length > 16)
    throw new Failure('Publisher registry must be a map with at most 16 slots.');
  for (const [publisher, entry] of Object.entries(registry)) {
    canonical(publisher);
    if (!entry || typeof entry !== 'object' || Object.keys(entry).sort().join(',') !== 'generation,state' ||
        !['active', 'uncertain'].includes(entry.state))
      throw new Failure('Publisher registry contains an invalid publisher entry.');
    canonical(entry.generation);
  }
  // Canonical comparison rejects duplicate JSON keys, whitespace and unsorted members.
  if (canonical_json(registry) !== raw)
    throw new Failure('Publisher registry is not canonical compact JSON.');
  return registry;
}
export function require_drained_publishers(metadata: Metadata): void {
  if (Object.keys(publisher_registry(metadata)).length)
    throw new Failure('Registered active or uncertain publishers block destructive cleanup; no expiry or force-clear is permitted.');
}
