import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

export type EffectTransport = 'azure-rest' | 'azure-auth' | 'campaign-blob' | 'process';
export interface EffectRule {
  id: string;
  transport: EffectTransport;
  method: string;
  resource: string;
  path: string;
  max_calls: number;
  body_sha256?: string;
}
export interface EffectEnvelope {
  schema_version: 1;
  campaign_id: string;
  run_id: string;
  subscription_id: string;
  resource_group: string;
  storage_account: string;
  state_container: string;
  effects: EffectRule[];
}
export interface ExternalEffect {
  transport: EffectTransport;
  method: string;
  resource: string;
  path: string;
  body?: string | Buffer | null;
}

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const name = /^[a-z][a-z0-9-]{1,62}$/;
const transports = new Set<EffectTransport>(['azure-rest', 'azure-auth', 'campaign-blob', 'process']);
const readMethods = new Set(['GET', 'HEAD', 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', 'KUBECTL_GET', 'KUBECTL_LOGS']);
interface BoundaryContext { boundary: EffectBoundary; forwarding: Set<string> }
const active = new AsyncLocalStorage<BoundaryContext>();

export class EffectBoundaryRefused extends Error {
  constructor() {
    super('External effect refused before forwarding: no exact unused campaign/run/resource/path/method allocation.');
    this.name = 'EffectBoundaryRefused';
  }
}

function exactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  return Object.keys(value).sort().join(',') === expected.slice().sort().join(',');
}
function safeTarget(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 8192 &&
    !/[\r\n\0]/.test(value) && !value.includes('*') &&
    !/(?:sig|access_token|refresh_token|client_secret)=/i.test(value);
}
function hash(value: string | Buffer | null | undefined): string | undefined {
  if (value == null) return undefined;
  return createHash('sha256').update(value).digest('hex');
}
function parseEnvelope(input: unknown): EffectEnvelope {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Effect envelope must be an object.');
  const value = input as Record<string, unknown>;
  if (!exactKeys(value, ['schema_version', 'campaign_id', 'run_id', 'subscription_id', 'resource_group',
      'storage_account', 'state_container', 'effects']) || value.schema_version !== 1 ||
      typeof value.campaign_id !== 'string' || !uuid.test(value.campaign_id) ||
      typeof value.run_id !== 'string' || !uuid.test(value.run_id) ||
      typeof value.subscription_id !== 'string' || !uuid.test(value.subscription_id) ||
      typeof value.resource_group !== 'string' || !name.test(value.resource_group) ||
      typeof value.storage_account !== 'string' || !/^[a-z0-9]{3,24}$/.test(value.storage_account) ||
      typeof value.state_container !== 'string' || !/^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$/.test(value.state_container) ||
      !Array.isArray(value.effects) || value.effects.length < 1 || value.effects.length > 512)
    throw new Error('Effect envelope identity or schema is invalid.');
  const effects: EffectRule[] = [];
  const ids = new Set<string>(), signatures = new Set<string>(), targetBinding = new Map<string, boolean>();
  for (const raw of value.effects) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) throw new Error('Effect envelope has a malformed rule.');
    const rule = raw as Record<string, unknown>;
    const ruleKeys = ['id', 'transport', 'method', 'resource', 'path', 'max_calls'];
    const bodyBound = Object.hasOwn(rule, 'body_sha256');
    if ((!exactKeys(rule, ruleKeys) && !exactKeys(rule, [...ruleKeys, 'body_sha256'])) ||
        bodyBound && (typeof rule.body_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(rule.body_sha256)) ||
        typeof rule.id !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/.test(rule.id) ||
        typeof rule.transport !== 'string' || !transports.has(rule.transport as EffectTransport) ||
        typeof rule.method !== 'string' || !/^[A-Z][A-Z0-9_-]{0,63}$/.test(rule.method) ||
        !safeTarget(rule.resource) || !safeTarget(rule.path) || !Number.isSafeInteger(rule.max_calls) ||
        (rule.max_calls as number) < 1 || (rule.max_calls as number) > 10_000 ||
        !readMethods.has(rule.method) && (rule.max_calls as number) > 10)
      throw new Error('Effect envelope contains an invalid or replayable rule.');
    const target = [rule.transport, rule.method, rule.resource, rule.path].join('\0');
    const signature = [target, bodyBound ? rule.body_sha256 : ''].join('\0');
    if (ids.has(rule.id) || signatures.has(signature) || targetBinding.has(target) && targetBinding.get(target) !== bodyBound)
      throw new Error('Effect envelope has duplicate or ambiguous rule identities.');
    ids.add(rule.id); signatures.add(signature); targetBinding.set(target, bodyBound);
    effects.push({ id: rule.id, transport: rule.transport as EffectTransport, method: rule.method,
      resource: rule.resource, path: rule.path, max_calls: rule.max_calls as number,
      ...(bodyBound ? { body_sha256: rule.body_sha256 as string } : {}) });
  }
  return { schema_version: 1, campaign_id: value.campaign_id, run_id: value.run_id,
    subscription_id: value.subscription_id, resource_group: value.resource_group,
    storage_account: value.storage_account, state_container: value.state_container, effects };
}

export function readEffectEnvelope(filename: string): EffectEnvelope {
  const stat = fs.lstatSync(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
    throw new Error('Effect envelope must be a private regular file.');
  let value: unknown;
  try { value = JSON.parse(fs.readFileSync(filename, 'utf8')); }
  catch { throw new Error('Effect envelope is not valid JSON.'); }
  return parseEnvelope(value);
}

export function processEffectRequest(command: string[]): ExternalEffect {
  const executable = path.basename(command[0] ?? '').toLowerCase();
  const verbs = new Set(['get', 'logs', 'patch', 'delete', 'apply', 'create', 'replace', 'exec']);
  const verb = executable === 'kubectl' ? command.find(value => verbs.has(value)) ?? 'unknown' : '';
  const method = executable === 'kubectl' ? 'KUBECTL_' + verb.toUpperCase() : executable === 'az' ?
    'AZURE_' + (command[1] ?? 'unknown').toUpperCase() + '_' + (command[2] ?? 'unknown').toUpperCase() :
    'PROCESS_' + (executable.toUpperCase() || 'UNKNOWN');
  return { transport: 'process', method, resource: command[0] ?? '', path: JSON.stringify(command.slice(1)) };
}

export class EffectBoundary {
  readonly envelope: EffectEnvelope;
  private readonly counts = new Map<string, number>();
  private readonly completed = new Map<string, number>();
  private sequence = 0;

  constructor(input: EffectEnvelope | unknown, private readonly receiptPath: string) {
    this.envelope = parseEnvelope(input);
    const parent = fs.lstatSync(path.dirname(receiptPath));
    if (!parent.isDirectory() || parent.isSymbolicLink() || (parent.mode & 0o077) !== 0)
      throw new Error('Effect receipt directory must be private and not a symlink.');
    if (fs.existsSync(receiptPath)) {
      const stat = fs.lstatSync(receiptPath);
      if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
        throw new Error('Effect receipt must be a private regular file.');
      for (const line of fs.readFileSync(receiptPath, 'utf8').split(/\r?\n/).filter(Boolean)) {
        let receipt: unknown;
        try { receipt = JSON.parse(line); } catch { throw new Error('Effect receipt journal is malformed; refusing dispatch.'); }
        if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt)) throw new Error('Effect receipt journal is malformed; refusing dispatch.');
        const row = receipt as Record<string, unknown>;
        if (row.schema_version !== 1 || row.campaign_id !== this.envelope.campaign_id || row.run_id !== this.envelope.run_id ||
            !['dispatch', 'result', 'refused'].includes(String(row.event)))
          throw new Error('Effect receipt journal has a different identity or schema.');
        this.sequence = Math.max(this.sequence, Number.isSafeInteger(row.sequence) ? row.sequence as number : 0);
        if (row.event === 'dispatch') {
          if (typeof row.effect_id !== 'string' || !this.envelope.effects.some(effect => effect.id === row.effect_id))
            throw new Error('Effect receipt journal names an unallocated effect.');
          const count = (this.counts.get(row.effect_id) ?? 0) + 1;
          const effect = this.envelope.effects.find(value => value.id === row.effect_id)!;
          if (count > effect.max_calls || !readMethods.has(effect.method) &&
              count > (this.completed.get(row.effect_id) ?? 0) + 1)
            throw new Error('Effect receipt journal exceeds or replays an allocation.');
          this.counts.set(row.effect_id, count);
        } else if (row.event === 'result') {
          if (typeof row.effect_id !== 'string' || !this.envelope.effects.some(effect => effect.id === row.effect_id) ||
              (this.counts.get(row.effect_id) ?? 0) <= (this.completed.get(row.effect_id) ?? 0))
            throw new Error('Effect receipt journal contains an unpaired result.');
          if (row.outcome === 'returned')
            this.completed.set(row.effect_id, (this.completed.get(row.effect_id) ?? 0) + 1);
          else if (row.outcome !== 'unknown_after_forward')
            throw new Error('Effect receipt journal contains an invalid outcome.');
        }
      }
    }
  }

  async dispatch<T>(request: ExternalEffect, forward: () => Promise<T>): Promise<T> {
    const method = request.method.toUpperCase();
    const bodyHash = hash(request.body);
    const rule = this.envelope.effects.find(effect => effect.transport === request.transport && effect.method === method &&
      effect.resource === request.resource && effect.path === request.path &&
      (effect.body_sha256 === undefined || effect.body_sha256 === bodyHash));
    const used = rule ? this.counts.get(rule.id) ?? 0 : 0;
    const completed = rule ? this.completed.get(rule.id) ?? 0 : 0;
    if (!rule || used >= rule.max_calls || !readMethods.has(method) && used > completed) {
      this.writeReceipt('refused', undefined, request, bodyHash, 'no_matching_or_remaining_allocation');
      throw new EffectBoundaryRefused();
    }
    this.counts.set(rule.id, (this.counts.get(rule.id) ?? 0) + 1);
    this.writeReceipt('dispatch', rule.id, request, bodyHash);
    try {
      const result = await forward();
      const outcome = result && typeof result === 'object' ? result as { status?: unknown; returncode?: unknown } : {};
      this.writeReceipt('result', rule.id, request, bodyHash, 'returned', {
        ...(Number.isInteger(outcome.status) ? { status: outcome.status } : {}),
        ...(Number.isInteger(outcome.returncode) ? { returncode: outcome.returncode } : {}),
      });
      this.completed.set(rule.id, completed + 1);
      return result;
    } catch (error) {
      this.writeReceipt('result', rule.id, request, bodyHash, 'unknown_after_forward', {
        error_type: error instanceof Error ? error.name : 'unknown',
      });
      throw error;
    }
  }

  private writeReceipt(event: 'dispatch' | 'result' | 'refused', effectId: string | undefined,
    request: ExternalEffect, bodyHash: string | undefined, outcome?: string, details: Record<string, unknown> = {}): void {
    const receipt = { schema_version: 1, sequence: ++this.sequence, observed_at: new Date().toISOString(), event,
      campaign_id: this.envelope.campaign_id, run_id: this.envelope.run_id,
      ...(effectId ? { effect_id: effectId } : {}), transport: request.transport, method: request.method.toUpperCase(),
      resource: request.resource, path: request.path, ...(bodyHash ? { body_sha256: bodyHash } : {}),
      ...(outcome ? { outcome } : {}), ...details };
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0);
    const fd = fs.openSync(this.receiptPath, flags, 0o600);
    try { fs.writeSync(fd, JSON.stringify(receipt) + '\n'); fs.fsyncSync(fd); }
    finally { fs.closeSync(fd); }
  }
}

export function dispatchExternalEffect<T>(request: ExternalEffect, forward: () => Promise<T>): Promise<T> {
  const context = active.getStore();
  if (!context) return forward();
  const signature = [request.transport, request.method.toUpperCase(), request.resource, request.path, hash(request.body) ?? ''].join('\0');
  if (context.forwarding.has(signature)) return forward();
  return context.boundary.dispatch(request, () => active.run({ boundary: context.boundary,
    forwarding: new Set([...context.forwarding, signature]) }, forward));
}

export function withEffectBoundary<T>(boundary: EffectBoundary, work: () => Promise<T>): Promise<T> {
  return active.run({ boundary, forwarding: new Set() }, work);
}
