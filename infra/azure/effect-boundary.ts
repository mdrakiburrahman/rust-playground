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
interface BoundaryContext {
  boundary: EffectBoundary;
  forwarding: Set<string>;
  responseWithholding?: ResponseWithholdingSupervisor;
}
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

export type EffectOutcome = 'confirmed_success' | 'definite_no_effect' | 'read_response' | 'unknown_after_forward';
interface ReceiptRequest { transport: EffectTransport; method: string; resource: string; path: string; body_sha256?: string }
interface PendingDispatch extends ReceiptRequest { effect_id: string; sequence: number }
type ReceiptChunkWriter = (fd: number, data: Buffer, offset: number, length: number) => number;
const receiptSchemaVersion = 2;
const effectOutcomes = new Set<EffectOutcome>(['confirmed_success', 'definite_no_effect', 'read_response', 'unknown_after_forward']);
function freezeEnvelope(envelope: EffectEnvelope): EffectEnvelope {
  for (const effect of envelope.effects) Object.freeze(effect);
  Object.freeze(envelope.effects);
  return Object.freeze(envelope);
}
function allocationFingerprint(envelope: EffectEnvelope): string {
  return createHash('sha256').update(JSON.stringify(envelope)).digest('hex');
}
function receiptKeys(row: Record<string, unknown>, required: string[], optional: string[] = []): boolean {
  const allowed = new Set([...required, ...optional]);
  return required.every(key => Object.hasOwn(row, key)) && Object.keys(row).every(key => allowed.has(key));
}
function parseReceiptRequest(row: Record<string, unknown>): ReceiptRequest {
  if (typeof row.transport !== 'string' || !transports.has(row.transport as EffectTransport) ||
      typeof row.method !== 'string' || !/^[A-Z][A-Z0-9_-]{0,63}$/.test(row.method) ||
      !safeTarget(row.resource) || !safeTarget(row.path) ||
      row.body_sha256 !== undefined && (typeof row.body_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(row.body_sha256)))
    throw new Error('Effect receipt journal request is malformed.');
  return { transport: row.transport as EffectTransport, method: row.method, resource: row.resource, path: row.path,
    ...(row.body_sha256 === undefined ? {} : { body_sha256: row.body_sha256 as string }) };
}
export interface ResponseWithholdingTarget {
  transport: EffectTransport;
  method: string;
  resource: string;
  path: string;
  body_sha256?: string;
}

interface WithheldResponseSummary { status?: number; returncode?: number; response_sha256: string }

async function summarizeWithheldResponse(value: unknown): Promise<WithheldResponseSummary> {
  if (value instanceof Response) {
    const body = Buffer.from(await value.clone().arrayBuffer());
    return { status: value.status, response_sha256: createHash('sha256').update(String(value.status)).update('\0').update(body).digest('hex') };
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const result = value as { status?: unknown; returncode?: unknown; stdout?: unknown; stderr?: unknown };
    if (Number.isSafeInteger(result.returncode)) {
      const stdout = Buffer.isBuffer(result.stdout) ? result.stdout : Buffer.from(typeof result.stdout === 'string' ? result.stdout : '');
      const stderr = Buffer.isBuffer(result.stderr) ? result.stderr : Buffer.from(typeof result.stderr === 'string' ? result.stderr : '');
      return { returncode: result.returncode as number, response_sha256: createHash('sha256').update(String(result.returncode))
        .update('\0').update(stdout).update('\0').update(stderr).digest('hex') };
    }
    if (Number.isSafeInteger(result.status)) {
      const status = result.status as number;
      return { status, response_sha256: createHash('sha256').update(JSON.stringify({ status })).digest('hex') };
    }
  }
  throw new Error('Response-withholding supervisor cannot safely summarize this provider response.');
}

function supportedWithholdingTarget(target: ReceiptRequest): boolean {
  if (target.transport === 'campaign-blob' && target.method === 'PUT' && /\/baseline\.json(?:\?|$)/.test(target.path)) return true;
  if (target.transport !== 'process' || target.method !== 'KUBECTL_PATCH') return false;
  try {
    const argv: unknown = JSON.parse(target.path);
    if (!Array.isArray(argv) || argv.some(value => typeof value !== 'string')) return false;
    const serialized = JSON.stringify(argv);
    return argv.includes('patch') && argv.includes('--type=json') && serialized.includes('\"uid\"') &&
      serialized.includes('\"resourceVersion\"') && serialized.includes('/spec/replicas');
  } catch { return false; }
}

export class ResponseWithholdingSupervisor {
  private triggered = false;
  private readonly target: ReceiptRequest;
  private readonly evidencePath: string;

  constructor(readonly campaignId: string, readonly runId: string, target: ResponseWithholdingTarget, evidencePath: string) {
    if (!uuid.test(campaignId) || !uuid.test(runId)) throw new Error('Response-withholding campaign/run identity is invalid.');
    this.target = parseReceiptRequest({ transport: target.transport, method: target.method, resource: target.resource,
      path: target.path, ...(target.body_sha256 === undefined ? {} : { body_sha256: target.body_sha256 }) });
    if (!supportedWithholdingTarget(this.target))
      throw new Error('Response withholding is limited to the exact campaign baseline PUT or UID/RV-tested replica PATCH.');
    this.evidencePath = path.resolve(evidencePath);
    const parent = fs.lstatSync(path.dirname(this.evidencePath));
    if (!parent.isDirectory() || parent.isSymbolicLink() || (parent.mode & 0o077) !== 0)
      throw new Error('Response-withholding evidence directory must be private and non-symlink.');
    if (fs.existsSync(this.evidencePath)) throw new Error('Response-withholding evidence already exists; fault is single-use.');
  }

  get wasTriggered(): boolean { return this.triggered; }
  get transport(): EffectTransport { return this.target.transport; }

  assertConsumed(): void {
    if (!this.triggered) throw new Error('Response-withholding target was not forwarded; fault rehearsal is incomplete.');
  }

  matches(request: ExternalEffect): boolean {
    return request.transport === this.target.transport && request.method.toUpperCase() === this.target.method &&
      request.resource === this.target.resource && request.path === this.target.path &&
      (this.target.body_sha256 === undefined || hash(request.body) === this.target.body_sha256);
  }

  async forward<T>(request: ExternalEffect, forward: () => Promise<T>): Promise<T> {
    const result = await forward();
    if (!this.matches(request)) return result;
    const response = await summarizeWithheldResponse(result);
    const bodyHash = hash(request.body);
    const evidence = { schema_version: 1, campaign_id: this.campaignId, run_id: this.runId,
      observed_at: new Date().toISOString(), event: 'provider_response_withheld',
      transport: request.transport, method: request.method.toUpperCase(), resource: request.resource,
      path: request.path, ...(bodyHash ? { body_sha256: bodyHash } : {}), ...response };
    const data = Buffer.from(JSON.stringify(evidence) + '\n');
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0);
    const fd = fs.openSync(this.evidencePath, flags, 0o600);
    try {
      let offset = 0;
      while (offset < data.length) {
        const written = fs.writeSync(fd, data, offset, data.length - offset);
        if (!Number.isSafeInteger(written) || written <= 0 || written > data.length - offset)
          throw new Error('Response-withholding evidence write returned an invalid byte count.');
        offset += written;
      }
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    const directory = fs.openSync(path.dirname(this.evidencePath), fs.constants.O_RDONLY);
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
    this.triggered = true;
    throw new Error('Provider response was durably captured and withheld; evidence is at ' + this.evidencePath + '.');
  }
}

function receiptMatchesRule(request: ReceiptRequest, rule: EffectRule): boolean {
  return request.transport === rule.transport && request.method === rule.method && request.resource === rule.resource &&
    request.path === rule.path && (rule.body_sha256 === undefined || request.body_sha256 === rule.body_sha256);
}
function sameReceiptRequest(left: ReceiptRequest, right: ReceiptRequest): boolean {
  return left.transport === right.transport && left.method === right.method && left.resource === right.resource &&
    left.path === right.path && left.body_sha256 === right.body_sha256;
}
function resultStatus(request: ExternalEffect, result: unknown): number | undefined {
  if (result && typeof result === 'object' && !Array.isArray(result)) {
    const status = (result as { status?: unknown }).status;
    if (Number.isSafeInteger(status)) return status as number;
  }
  if (request.transport === 'campaign-blob' && Array.isArray(result) && result.length === 2 &&
      result[1] && typeof result[1] === 'object' && !Array.isArray(result[1])) {
    const status = (result[1] as Record<string, unknown>)[":status"];
    if (typeof status === 'string' && /^[1-5][0-9]{2}$/.test(status)) return Number(status);
  }
  return undefined;
}
function classifyOutcome(request: ExternalEffect, result: unknown): EffectOutcome {
  if (readMethods.has(request.method)) return 'read_response';
  const status = resultStatus(request, result);
  if (status !== undefined) {
    if (status >= 200 && status < 300) return 'confirmed_success';
    if (status === 412) return 'definite_no_effect';
    return 'unknown_after_forward';
  }
  if (request.transport === 'process' && result && typeof result === 'object' &&
      Number.isSafeInteger((result as { returncode?: unknown }).returncode) &&
      (result as { returncode: number }).returncode === 0) return 'confirmed_success';
  return 'unknown_after_forward';
}

export class EffectBoundary {
  readonly envelope: EffectEnvelope;
  private readonly counts = new Map<string, number>();
  private readonly resolved = new Map<string, number>();
  private readonly pending = new Map<number, PendingDispatch>();
  private readonly allocationSha256: string;
  private sequence = 0;
  private poisoned = false;

  constructor(input: EffectEnvelope | unknown, private readonly receiptPath: string,
    private readonly writeChunk: ReceiptChunkWriter = (fd, data, offset, length) => fs.writeSync(fd, data, offset, length)) {
    this.envelope = freezeEnvelope(parseEnvelope(input));
    this.allocationSha256 = allocationFingerprint(this.envelope);
    const parent = fs.lstatSync(path.dirname(receiptPath));
    if (!parent.isDirectory() || parent.isSymbolicLink() || (parent.mode & 0o077) !== 0)
      throw new Error('Effect receipt directory must be private and not a symlink.');
    if (fs.existsSync(receiptPath)) {
      const stat = fs.lstatSync(receiptPath);
      if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
        throw new Error('Effect receipt must be a private regular file.');
      const journal = fs.readFileSync(receiptPath, 'utf8');
      if (journal && !journal.endsWith('\n')) throw new Error('Effect receipt journal is incomplete; refusing dispatch.');
      const lines = journal ? journal.split(/\r?\n/).slice(0, -1) : [];
      if (lines.some(line => !line)) throw new Error('Effect receipt journal contains an empty or malformed record.');
      for (const line of lines) {
        let receipt: unknown;
        try { receipt = JSON.parse(line); } catch { throw new Error('Effect receipt journal is malformed; refusing dispatch.'); }
        if (!receipt || typeof receipt !== 'object' || Array.isArray(receipt))
          throw new Error('Effect receipt journal is malformed; refusing dispatch.');
        const row = receipt as Record<string, unknown>;
        if (row.schema_version !== receiptSchemaVersion || row.campaign_id !== this.envelope.campaign_id ||
            row.run_id !== this.envelope.run_id || row.allocation_sha256 !== this.allocationSha256 ||
            !Number.isSafeInteger(row.sequence) || row.sequence !== this.sequence + 1 ||
            typeof row.observed_at !== 'string' || !Number.isFinite(Date.parse(row.observed_at)) ||
            new Date(row.observed_at).toISOString() !== row.observed_at ||
            !['dispatch', 'result', 'refused'].includes(String(row.event)))
          throw new Error('Effect receipt journal has a different identity, allocation, sequence or schema.');
        this.sequence = row.sequence as number;
        const request = parseReceiptRequest(row);
        if (row.event === 'dispatch') {
          if (!receiptKeys(row, ['schema_version', 'allocation_sha256', 'sequence', 'observed_at', 'event',
              'campaign_id', 'run_id', 'effect_id', 'transport', 'method', 'resource', 'path'], ['body_sha256']) ||
              typeof row.effect_id !== 'string')
            throw new Error('Effect receipt dispatch record is malformed.');
          const effect = this.envelope.effects.find(value => value.id === row.effect_id);
          if (!effect || !receiptMatchesRule(request, effect))
            throw new Error('Effect receipt journal dispatch binding changed.');
          const count = (this.counts.get(effect.id) ?? 0) + 1;
          const resolved = this.resolved.get(effect.id) ?? 0;
          if (count > effect.max_calls || !readMethods.has(effect.method) && count > resolved + 1)
            throw new Error('Effect receipt journal exceeds or replays an allocation.');
          this.counts.set(effect.id, count);
          this.pending.set(this.sequence, { ...request, effect_id: effect.id, sequence: this.sequence });
        } else if (row.event === 'result') {
          if (!receiptKeys(row, ['schema_version', 'allocation_sha256', 'sequence', 'observed_at', 'event',
              'campaign_id', 'run_id', 'effect_id', 'dispatch_sequence', 'transport', 'method', 'resource', 'path', 'outcome'],
              ['body_sha256', 'status', 'returncode', 'error_type']) || typeof row.effect_id !== 'string' ||
              !Number.isSafeInteger(row.dispatch_sequence) || typeof row.outcome !== 'string' ||
              !effectOutcomes.has(row.outcome as EffectOutcome) ||
              row.status !== undefined && (!Number.isSafeInteger(row.status) || (row.status as number) < 100 || (row.status as number) > 599) ||
              row.returncode !== undefined && !Number.isSafeInteger(row.returncode) ||
              row.error_type !== undefined && (typeof row.error_type !== 'string' || !row.error_type))
            throw new Error('Effect receipt result record is malformed.');
          const dispatched = this.pending.get(row.dispatch_sequence as number);
          if (!dispatched || dispatched.effect_id !== row.effect_id || !sameReceiptRequest(request, dispatched))
            throw new Error('Effect receipt journal result is unpaired or has changed request binding.');
          const outcome = row.outcome as EffectOutcome;
          const confirmedAcknowledgement = request.transport === 'process' ?
            row.status === undefined && row.returncode === 0 :
            row.returncode === undefined && Number.isSafeInteger(row.status) &&
              (row.status as number) >= 200 && (row.status as number) < 300;
          if (outcome === 'read_response' && !readMethods.has(request.method) ||
              (outcome === 'confirmed_success' || outcome === 'definite_no_effect') && readMethods.has(request.method) ||
              outcome === 'confirmed_success' && (!confirmedAcknowledgement || row.error_type !== undefined) ||
              outcome === 'definite_no_effect' &&
                (request.transport === 'process' || row.status !== 412 || row.returncode !== undefined || row.error_type !== undefined))
            throw new Error('Effect receipt result outcome does not match the recorded response.');
          this.pending.delete(row.dispatch_sequence as number);
          if (outcome === 'confirmed_success' || outcome === 'definite_no_effect')
            this.resolved.set(dispatched.effect_id, (this.resolved.get(dispatched.effect_id) ?? 0) + 1);
        } else {
          if (!receiptKeys(row, ['schema_version', 'allocation_sha256', 'sequence', 'observed_at', 'event',
              'campaign_id', 'run_id', 'transport', 'method', 'resource', 'path', 'outcome'], ['body_sha256']) ||
              row.outcome !== 'no_matching_or_remaining_allocation')
            throw new Error('Effect receipt refusal record is malformed.');
          const effect = this.envelope.effects.find(value => value.transport === request.transport &&
            value.method === request.method && value.resource === request.resource && value.path === request.path &&
            (value.body_sha256 === undefined || value.body_sha256 === request.body_sha256));
          if (effect && (this.counts.get(effect.id) ?? 0) < effect.max_calls &&
              (readMethods.has(effect.method) || (this.counts.get(effect.id) ?? 0) <= (this.resolved.get(effect.id) ?? 0)))
            throw new Error('Effect receipt refusal does not match an exhausted or unresolved allocation.');
        }
      }
    }
  }

  matchesAllocation(input: unknown): boolean {
    try { return this.allocationSha256 === effectAllocationFingerprint(input); }
    catch { return false; }
  }

  async dispatch<T>(request: ExternalEffect, forward: () => Promise<T>): Promise<T> {
    if (this.poisoned) throw new Error('Effect boundary journal is not durable; refusing all further dispatch.');
    const method = request.method.toUpperCase();
    const bodyHash = hash(request.body);
    const rule = this.envelope.effects.find(effect => effect.transport === request.transport && effect.method === method &&
      effect.resource === request.resource && effect.path === request.path &&
      (effect.body_sha256 === undefined || effect.body_sha256 === bodyHash));
    const used = rule ? this.counts.get(rule.id) ?? 0 : 0;
    const resolved = rule ? this.resolved.get(rule.id) ?? 0 : 0;
    if (!rule || used >= rule.max_calls || !readMethods.has(method) && used > resolved) {
      try { this.writeReceipt('refused', undefined, request, bodyHash, 'no_matching_or_remaining_allocation'); }
      catch (error) {
        this.poisoned = true;
        throw new Error('Refusal receipt could not be durably recorded; no effect was forwarded.', { cause: error });
      }
      throw new EffectBoundaryRefused();
    }
    this.counts.set(rule.id, used + 1);
    let dispatchSequence: number;
    try { dispatchSequence = this.writeReceipt('dispatch', rule.id, request, bodyHash); }
    catch (error) {
      this.poisoned = true;
      throw new Error('Dispatch receipt could not be durably recorded; effect was not forwarded.', { cause: error });
    }
    let result: T;
    try { result = await forward(); }
    catch (error) {
      try { this.writeReceipt('result', rule.id, request, bodyHash, 'unknown_after_forward', {
        dispatch_sequence: dispatchSequence, error_type: error instanceof Error ? error.name : 'unknown',
      }); }
      catch (receiptError) {
        this.poisoned = true;
        throw new AggregateError([error, receiptError], 'Effect failed after forwarding and its unknown outcome could not be durably recorded.');
      }
      throw error;
    }
    const outcome = classifyOutcome({ ...request, method }, result);
    const details: Record<string, unknown> = { dispatch_sequence: dispatchSequence };
    const status = resultStatus({ ...request, method }, result);
    if (status !== undefined && status >= 100 && status <= 599) details.status = status;
    if (result && typeof result === 'object' && !Array.isArray(result)) {
      const returncode = (result as { returncode?: unknown }).returncode;
      if (Number.isSafeInteger(returncode)) details.returncode = returncode;
    }
    try { this.writeReceipt('result', rule.id, request, bodyHash, outcome, details); }
    catch (error) {
      this.poisoned = true;
      throw new Error('Effect returned but its outcome receipt could not be durably recorded; refusing replay.', { cause: error });
    }
    if (outcome === 'confirmed_success' || outcome === 'definite_no_effect')
      this.resolved.set(rule.id, resolved + 1);
    return result;
  }

  private writeReceipt(event: 'dispatch' | 'result' | 'refused', effectId: string | undefined,
    request: ExternalEffect, bodyHash: string | undefined, outcome?: string, details: Record<string, unknown> = {}): number {
    const sequence = this.sequence + 1;
    const receipt = { schema_version: receiptSchemaVersion, allocation_sha256: this.allocationSha256,
      sequence, observed_at: new Date().toISOString(), event,
      campaign_id: this.envelope.campaign_id, run_id: this.envelope.run_id,
      ...(effectId ? { effect_id: effectId } : {}), transport: request.transport, method: request.method.toUpperCase(),
      resource: request.resource, path: request.path, ...(bodyHash ? { body_sha256: bodyHash } : {}),
      ...(outcome ? { outcome } : {}), ...details };
    const data = Buffer.from(JSON.stringify(receipt) + '\n');
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0);
    const fd = fs.openSync(this.receiptPath, flags, 0o600);
    try {
      let offset = 0;
      while (offset < data.length) {
        const remaining = data.length - offset;
        const written = this.writeChunk(fd, data, offset, remaining);
        if (!Number.isSafeInteger(written) || written <= 0 || written > remaining)
          throw new Error('Effect receipt write returned an invalid byte count.');
        offset += written;
      }
      fs.fsyncSync(fd);
      this.sequence = sequence;
      return sequence;
    } finally { fs.closeSync(fd); }
  }
}
export function effectAllocationFingerprint(input: unknown): string {
  return allocationFingerprint(parseEnvelope(input));
}
export function isEffectBoundaryActive(): boolean { return active.getStore() !== undefined; }
export function isEffectBoundaryActiveFor(envelope: unknown): boolean {
  return active.getStore()?.boundary.matchesAllocation(envelope) ?? false;
}

export function dispatchExternalEffect<T>(request: ExternalEffect, forward: () => Promise<T>): Promise<T> {
  const context = active.getStore();
  if (!context) return forward();
  const signature = [request.transport, request.method.toUpperCase(), request.resource, request.path, hash(request.body) ?? ''].join('\0');
  if (context.forwarding.has(signature)) return forward();
  return context.boundary.dispatch(request, () => active.run({ boundary: context.boundary,
    forwarding: new Set([...context.forwarding, signature]), responseWithholding: context.responseWithholding },
    () => context.responseWithholding ? context.responseWithholding.forward(request, forward) : forward()));
}

export function withEffectBoundary<T>(boundary: EffectBoundary, work: () => Promise<T>,
  responseWithholding?: ResponseWithholdingSupervisor): Promise<T> {
  return active.run({ boundary, forwarding: new Set(), responseWithholding }, work);
}
