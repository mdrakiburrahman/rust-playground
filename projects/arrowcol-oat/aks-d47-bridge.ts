import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawn, type ChildProcess } from 'node:child_process';
import { TextDecoder } from 'node:util';
import { Azure, type ProcessResult } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, EffectBoundaryRefused, effectAllocationFingerprint, processEffectRequest,
  type EffectEnvelope, type ExternalEffect, isEffectBoundaryActiveFor } from '../../infra/azure/effect-boundary.js';
import { assertCampaignCustodyState, campaignMain, validateInitialRunInventory, type CampaignStore,
  type CampaignStoreOperationOptions, type RunInventory } from './aks-campaign.js';

const MAX_FRAME = 4 * 1024 * 1024;
const MAX_OUTPUT = 1024 * 1024;
const MAX_STORAGE_BODY = 1024 * 1024;
const STORAGE_RESOURCE = 'https://storage.azure.com/';
const VOLATILE_STORAGE_EFFECT_HEADERS = new Set(['x-ms-client-request-id', 'date', 'x-ms-date']);
type Obj = Record<string, unknown>;

export interface FixtureAuthority { campaign_id: string; cell_run_id: string; owner_id: string; fencing_token: number;
  namespace: string; server: string; context: string }
export interface FixtureCommandRequest { schema_version: 1; kind: 'command_request'; request_id: number; authority: FixtureAuthority;
  original_deadline_unix_ms: number; remaining_timeout_ms: number; executable: string; argv: string[];
  current_dir: string | null; stdin: string | null; stdin_sha256: string; request_sha256: string }
export interface FixtureCommandResponse { schema_version: 1; kind: 'command_response'; request_id: number; request_sha256: string;
  exit_code: number; stdout: string; stderr: string; truncated: false }
export interface FixtureSetupInput { schema_version: 1; authority: FixtureAuthority; original_deadline_unix_ms: number;
  inventory: RunInventory; service_account: string; manifests: unknown[]; runtime_directory: string; stamp_config_path: string; bootstrap_env_file: string }
export interface FixtureReceipt { schema_version: 1; kind: 'fixture_setup_receipt'; inventory: RunInventory;
  namespace_response: Obj; command_responses: FixtureCommandResponse[]; load_admitted: false }
export interface FixtureDispatcherOptions { input: FixtureSetupInput; rootDirectory: string; ledgerPath: string; envelope: EffectEnvelope;
  store: CampaignStore; azure: Azure; env: NodeJS.ProcessEnv; campaignMain?: typeof campaignMain; now?: () => number;
  runCommand?: (command: string[], stdin: Buffer | undefined, timeoutMs: number, cwd: string) => Promise<ProcessResult> }

export interface StorageBridgeAuthority { campaign_id: string; cell_run_id: string; owner_id: string; fencing_token: number;
  storage_account: string; table_root_url: string; control_container_url: string; sandbox_id: string; reset_generation: string }
export interface StorageBridgeRequest { schema_version: 1; kind: 'storage_http_request'; request_id: number;
  authority: StorageBridgeAuthority; original_deadline_unix_ms: number; remaining_timeout_ms: number; target: 'control' | 'object_store';
  method: string; url: string; headers: [string, string][]; body: string; body_sha256: string; request_sha256: string }
export type StorageBridgeReply =
  | { schema_version: 1; kind: 'storage_http_response'; request_id: number; request_sha256: string; outcome: 'response';
      status: number; headers: [string, string][]; body: string; body_sha256: string; truncated: false }
  | { schema_version: 1; kind: 'storage_http_response'; request_id: number; request_sha256: string;
      outcome: 'definitely_not_sent' | 'unknown'; error_code: 'admission_refused' | 'deadline_exceeded' | 'invalid_request' |
        'protocol' | 'response_lost' | 'transport_unavailable' };
export interface StorageDispatcherOptions { authority: StorageBridgeAuthority; originalDeadlineUnixMs: number;
  envelope: EffectEnvelope; azure: Azure; store: CampaignStore; ledgerPath: string; phase?: string; planIdentity?: unknown;
  controlPlan?: PlannedControlPlan; fetcher?: typeof fetch; now?: () => number }
interface PlannedControlPlan { publisher_id: string; generation: string; control_lease_ids: readonly [string, string] }
interface PlannedControlProgress { stage: 0 | 1; phase: 'acquire' | 'metadata' | 'release' | 'blocked' | 'done';
  registered: boolean; metadataState?: 'active' | 'uncertain' | 'removed'; complete: boolean }
interface PlannedControlStep { kind: 'acquire' | 'metadata' | 'release'; stage: 0 | 1; leaseId: string;
  metadataState?: 'active' | 'uncertain' | 'removed' }

function object(value: unknown): value is Obj { return value !== null && typeof value === 'object' && !Array.isArray(value); }
function exact(value: Obj, keys: string[]): boolean { return Object.keys(value).sort().join(',') === keys.slice().sort().join(','); }
function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (object(value)) return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  const result = JSON.stringify(value); if (result === undefined) throw new Error('Bridge frame is not JSON.'); return result;
}
function sha(value: string | Buffer): string { return createHash('sha256').update(value).digest('hex'); }
function equal(left: unknown, right: unknown): boolean { return canonical(left) === canonical(right); }
function integer(value: unknown, min: number, max: number, label: string): asserts value is number {
  if (!Number.isSafeInteger(value) || Number(value) < min || Number(value) > max) throw new Error(`Bridge ${label} is invalid.`);
}
function uuid(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value) ||
      value === '00000000-0000-0000-0000-000000000000') throw new Error(`Bridge ${label} is invalid.`);
}
function uuidV4(value: unknown, label: string): asserts value is string {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value))
    throw new Error(`Bridge ${label} must be a canonical lowercase UUID v4.`);
}
function singleHeader(headers: [string, string][], name: string): string | undefined {
  const values = headers.filter(([key]) => key.toLowerCase() === name).map(([, value]) => value);
  if (values.length > 1) throw new Error(`Bridge ${name} header is duplicated.`);
  return values[0];
}
function checkDeadline(deadline: number, remaining: number, now: number, max: number): void {
  integer(deadline, 1, Number.MAX_SAFE_INTEGER, 'deadline'); integer(remaining, 1, max, 'remaining timeout');
  if (now >= deadline || remaining > deadline - now) throw new Error('Bridge original deadline expired or timeout was reset.');
}
function checkDigest(value: Obj, stdin?: string): void {
  const digest = value.request_sha256;
  if (typeof digest !== 'string' || !/^[0-9a-f]{64}$/.test(digest)) throw new Error('Bridge request digest is malformed.');
  const unsigned = { ...value }; delete unsigned.request_sha256;
  if (sha(canonical(unsigned)) !== digest || stdin !== undefined && value.stdin_sha256 !== sha(stdin))
    throw new Error('Bridge request digest does not match the canonical frame/body.');
}
function fixtureAuthority(value: unknown): asserts value is FixtureAuthority {
  const keys = ['campaign_id', 'cell_run_id', 'owner_id', 'fencing_token', 'namespace', 'server', 'context'];
  if (!object(value) || !exact(value, keys)) throw new Error('Fixture authority schema is invalid.');
  uuid(value.campaign_id, 'campaign'); uuid(value.cell_run_id, 'run'); uuid(value.owner_id, 'owner');
  integer(value.fencing_token, 1, Number.MAX_SAFE_INTEGER, 'fencing token');
  if (typeof value.namespace !== 'string' || !/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/.test(value.namespace) ||
      typeof value.context !== 'string' || !value.context || value.context.length > 120 || /[\u0000-\u001f]/.test(value.context))
    throw new Error('Fixture namespace or context is invalid.');
  let server: URL; try { server = new URL(String(value.server)); } catch { throw new Error('Fixture server is invalid.'); }
  if (server.protocol !== 'https:' || !server.hostname || server.username || server.password || server.search || server.hash || server.href !== value.server)
    throw new Error('Fixture server must be canonical credential-free HTTPS.');
}
function validateFixtureInput(input: FixtureSetupInput, now: number): void {
  fixtureAuthority(input.authority); integer(input.original_deadline_unix_ms, 1, Number.MAX_SAFE_INTEGER, 'original deadline');
  checkDeadline(input.original_deadline_unix_ms, input.original_deadline_unix_ms - now, now, 600_000);
  const inventory = validateInitialRunInventory(input.inventory, input.authority.campaign_id, input.authority.owner_id);
  if (input.schema_version !== 1 || inventory.cell_run_id !== input.authority.cell_run_id ||
      inventory.fencing_token !== input.authority.fencing_token || inventory.load_admission !== 'not_authorized' ||
      inventory.planned_cell_duration_ms !== 0 || inventory.namespace.name !== input.authority.namespace || inventory.namespace.uid !== null ||
      inventory.resources.length !== 0 || !inventory.fic?.plan || inventory.fic.plan.runId !== inventory.cell_run_id ||
      inventory.fic.plan.namespace !== input.authority.namespace || inventory.fic.plan.server !== input.authority.server ||
        inventory.fic.plan.serviceAccount !== input.service_account || !Array.isArray(input.manifests) || input.manifests.length !== 5 ||
      ![input.runtime_directory, input.stamp_config_path, input.bootstrap_env_file].every(value => typeof value === 'string' && path.isAbsolute(value)))
    throw new Error('Fixture input is not the exact fresh zero-row allocation.');
}
function fixtureRequest(value: unknown, authority: FixtureAuthority, deadline: number, now: number): FixtureCommandRequest {
  const keys = ['schema_version', 'kind', 'request_id', 'authority', 'original_deadline_unix_ms', 'remaining_timeout_ms',
    'executable', 'argv', 'current_dir', 'stdin', 'stdin_sha256', 'request_sha256'];
  if (!object(value) || !exact(value, keys) || value.schema_version !== 1 || value.kind !== 'command_request' ||
      typeof value.executable !== 'string' || !value.executable || !Array.isArray(value.argv) || value.argv.some(item => typeof item !== 'string') ||
      !(value.current_dir === null || typeof value.current_dir === 'string') || !(value.stdin === null || typeof value.stdin === 'string') ||
      typeof value.stdin_sha256 !== 'string') throw new Error('Fixture request schema is invalid.');
  integer(value.request_id, 1, Number.MAX_SAFE_INTEGER, 'request ID'); fixtureAuthority(value.authority);
  if (!equal(value.authority, authority) || value.original_deadline_unix_ms !== deadline) throw new Error('Fixture request changed immutable authority/deadline.');
  checkDeadline(deadline, Number(value.remaining_timeout_ms), now, 600_000);
  checkDigest(value, value.stdin ?? '');
  if (value.stdin === null && value.stdin_sha256 !== sha('') || Buffer.byteLength(canonical(value)) > MAX_FRAME)
    throw new Error('Fixture stdin digest or frame bound is invalid.');
  return value as unknown as FixtureCommandRequest;
}
function addOriginMarkers(value: unknown, authority: FixtureAuthority, podTemplate: boolean): Obj {
  if (!object(value)) throw new Error('Fixture manifest is not a JSON object.');
  const result = structuredClone(value) as Obj;
  const mark = (metadata: unknown) => {
    if (!object(metadata)) throw new Error('Fixture manifest metadata is invalid.');
    for (const field of ['labels', 'annotations']) {
      if (metadata[field] === undefined) metadata[field] = {};
      if (!object(metadata[field])) throw new Error('Fixture manifest labels or annotations are invalid.');
      metadata[field]['arrowcol-oat/run-id'] = authority.cell_run_id;
      metadata[field]['arrowcol-oat/owner-token'] = authority.owner_id;
    }
  };
  mark(result.metadata);
  if (podTemplate) {
    const spec = result.spec;
    if (!object(spec) || !object(spec.template)) throw new Error('Fixture Deployment template is invalid.');
    mark(spec.template.metadata);
  }
  return result;
}
function fixtureRuntimeList(input: FixtureSetupInput): string {
  const items = input.manifests.slice(1).map(manifest => addOriginMarkers(manifest, input.authority,
    object(manifest) && manifest.kind === 'Deployment'));
  return canonical({ apiVersion: 'v1', kind: 'List', items });
}
function fixtureNamespace(input: FixtureSetupInput): string {
  return canonical(addOriginMarkers(input.manifests[0], input.authority, false));
}
function storageAuthority(value: unknown): asserts value is StorageBridgeAuthority {
  const keys = ['campaign_id', 'cell_run_id', 'owner_id', 'fencing_token', 'storage_account', 'table_root_url',
    'control_container_url', 'sandbox_id', 'reset_generation'];
  if (!object(value) || !exact(value, keys)) throw new Error('Storage authority schema is invalid.');
  uuid(value.campaign_id, 'campaign'); uuid(value.cell_run_id, 'run'); uuid(value.owner_id, 'owner'); uuid(value.reset_generation, 'reset generation');
  integer(value.fencing_token, 1, Number.MAX_SAFE_INTEGER, 'fencing token');
  if (typeof value.storage_account !== 'string' || !/^[a-z0-9]{3,24}$/.test(value.storage_account) ||
      typeof value.sandbox_id !== 'string' || !value.sandbox_id || value.sandbox_id.length > 256)
    throw new Error('Storage authority account or sandbox is invalid.');
  let root: URL, control: URL; try { root = new URL(String(value.table_root_url)); control = new URL(String(value.control_container_url)); }
  catch { throw new Error('Storage authority URL is invalid.'); }
  if (root.protocol !== 'abfss:' || !root.pathname.endsWith('/') || root.pathname === '/' || root.search || root.hash ||
      root.password || root.port || root.hostname !== `${value.storage_account}.dfs.core.windows.net` || root.href !== value.table_root_url ||
      !/^https:$/.test(control.protocol) || control.origin !== `https://${value.storage_account}.blob.core.windows.net` ||
      control.username || control.password || control.search || control.hash || control.pathname.endsWith('/') ||
      control.pathname.split('/').filter(Boolean).length !== 1 || control.href !== value.control_container_url)
    throw new Error('Storage authority must bind canonical one-table and one-container URLs.');
}
interface LedgerBinding { schema_version: 1; kind: 'binding'; scope: string; authority_sha256: string;
  original_deadline_unix_ms: number; phase: string; plan_sha256: string; allocation_sha256: string }
interface StorageLedgerRequest { target: 'control' | 'object_store'; method: string; url: string;
  headers: [string, string][]; body_sha256: string }
interface LedgerRecord { schema_version: 1; scope: string; authority_sha256: string; request_id: number;
  request_sha256: string; state: 'pending' | 'response'; request_evidence?: StorageLedgerRequest;
  response?: unknown; response_sha256?: string }
interface LedgerTerminalRecord extends Omit<LedgerBinding, 'kind'> { kind: 'terminal_receipt'; request_count: number;
  transcript_sha256: string; receipt: unknown; receipt_sha256: string }
class DurableBridgeLedger {
  private readonly records = new Map<number, LedgerRecord>();
  private maxId = 0;
  private terminal?: LedgerTerminalRecord;
  private readonly scope: string;
  private readonly authorityHash: string;
  private readonly binding: LedgerBinding;
  constructor(private readonly filename: string, binding: LedgerBinding) {
    this.binding = binding; this.scope = binding.scope; this.authorityHash = binding.authority_sha256;
    const directory = path.dirname(path.resolve(filename)); fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    const dir = fs.lstatSync(directory);
    if (!dir.isDirectory() || dir.isSymbolicLink() || (dir.mode & 0o077) !== 0) throw new Error('Bridge ledger directory must be private and non-symlink.');
    if (!fs.existsSync(filename)) { const fd = fs.openSync(filename, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600); fs.fsyncSync(fd); fs.closeSync(fd); }
    const stat = fs.lstatSync(filename);
    if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) throw new Error('Bridge ledger must be a private regular file.');
    const bytes = fs.readFileSync(filename, 'utf8');
    if (bytes && !bytes.endsWith('\n')) throw new Error('Bridge ledger ends with an incomplete record; outcome is unknown.');
    const lines = bytes.split('\n').filter(Boolean);
    if (lines.length === 0) this.append(binding);
    else {
      const persisted = JSON.parse(lines[0]!) as unknown;
      if (!object(persisted) || !exact(persisted, ['schema_version','kind','scope','authority_sha256','original_deadline_unix_ms',
          'phase','plan_sha256','allocation_sha256']) || !equal(persisted, binding))
        throw new Error('Bridge ledger immutable deadline, phase, plan or allocation identity changed after restart.');
      for (let index = 1; index < lines.length; index++) {
        const value = JSON.parse(lines[index]!) as unknown;
        if (object(value) && value.kind === 'terminal_receipt') {
          if (index !== lines.length - 1) throw new Error('Bridge ledger has records after its terminal receipt.');
          this.loadTerminal(value);
        } else {
          if (this.terminal) throw new Error('Bridge ledger has records after its terminal receipt.');
          this.load(value);
        }
      }
    }
  }
  private loadTerminal(value: Obj): void {
    const keys = ['schema_version','kind','scope','authority_sha256','original_deadline_unix_ms','phase','plan_sha256',
      'allocation_sha256','request_count','transcript_sha256','receipt','receipt_sha256'];
    if (!exact(value, keys) || value.schema_version !== 1 || value.kind !== 'terminal_receipt' ||
        value.scope !== this.scope || value.authority_sha256 !== this.binding.authority_sha256 ||
        value.original_deadline_unix_ms !== this.binding.original_deadline_unix_ms || value.phase !== this.binding.phase ||
        value.plan_sha256 !== this.binding.plan_sha256 || value.allocation_sha256 !== this.binding.allocation_sha256 ||
        value.request_count !== this.maxId || this.maxId < 1 || [...this.records.values()].some(record => record.state !== 'response') ||
        value.transcript_sha256 !== this.transcriptSha256() || typeof value.receipt_sha256 !== 'string' ||
        sha(canonical(value.receipt)) !== value.receipt_sha256 || Buffer.byteLength(canonical(value.receipt)) > MAX_FRAME)
      throw new Error('Bridge ledger terminal receipt is unpaired, changed, or not bound to completed exchanges.');
    this.terminal = value as unknown as LedgerTerminalRecord;
  }
  private load(value: unknown): void {
    const keys = ['schema_version', 'scope', 'authority_sha256', 'request_id', 'request_sha256', 'state'];
    if (!object(value) || value.schema_version !== 1 || value.scope !== this.scope || value.authority_sha256 !== this.authorityHash ||
        !Number.isSafeInteger(value.request_id) || typeof value.request_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(value.request_sha256))
      throw new Error('Bridge ledger record is malformed or bound to another authority.');
    const hasEvidence = Object.hasOwn(value, 'request_evidence');
    if ((this.scope === 'storage-http') !== hasEvidence) throw new Error('Bridge ledger request evidence is missing or unexpected.');
    if (hasEvidence) validateStorageLedgerRequest(value.request_evidence);
    const id = Number(value.request_id), prior = this.records.get(id);
    if (!prior) {
      if (!exact(value, hasEvidence ? [...keys, 'request_evidence'] : keys) || id !== this.maxId + 1 || value.state !== 'pending')
        throw new Error('Bridge ledger request sequence is invalid.');
      this.maxId = id; this.records.set(id, value as unknown as LedgerRecord); return;
    }
    if (!exact(value, [...keys, ...(hasEvidence ? ['request_evidence'] : []), 'response', 'response_sha256']) || prior.state !== 'pending' ||
        value.request_sha256 !== prior.request_sha256 || value.state !== 'response' || typeof value.response_sha256 !== 'string' ||
        hasEvidence && !equal(value.request_evidence, prior.request_evidence) || sha(canonical(value.response)) !== value.response_sha256)
      throw new Error('Bridge ledger response transition is invalid.');
    this.records.set(id, value as unknown as LedgerRecord);
  }
  private transcriptSha256(): string {
    const exchanges = [...this.records.values()].sort((left, right) => left.request_id - right.request_id).map(record => ({
      request_id: record.request_id, request_sha256: record.request_sha256, request_evidence: record.request_evidence,
      response_sha256: record.response_sha256,
    }));
    return sha(canonical(exchanges));
  }
  private append(record: LedgerBinding | LedgerRecord | LedgerTerminalRecord): void {
    const data = Buffer.from(canonical(record) + '\n'), fd = fs.openSync(this.filename,
      fs.constants.O_WRONLY | fs.constants.O_APPEND | (fs.constants.O_NOFOLLOW ?? 0));
    try { let offset = 0; while (offset < data.length) { const n = fs.writeSync(fd, data, offset, data.length - offset);
      if (n <= 0) throw new Error('Bridge ledger write failed.'); offset += n; } fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  }
  peek(id: number, digest: string, requestEvidence?: StorageLedgerRequest): unknown | undefined {
    if (this.terminal) throw new Error('Bridge ledger already contains a terminal receipt; no further exchange is permitted.');
    if ((this.scope === 'storage-http') !== (requestEvidence !== undefined))
      throw new Error('Bridge ledger request evidence does not match its scope.');
    if (requestEvidence) validateStorageLedgerRequest(requestEvidence);
    const prior = this.records.get(id);
    if (prior) {
      if (prior.request_sha256 !== digest || (requestEvidence !== undefined) !== (prior.request_evidence !== undefined) ||
          requestEvidence !== undefined && !equal(prior.request_evidence, requestEvidence))
        throw new Error('Bridge request ID reused with a different digest or raw request evidence.');
      if (prior.state !== 'response') throw new Error('Bridge request has an unresolved prior forward; never replay.');
      return prior.response;
    }
    if (id !== this.maxId + 1) throw new Error('Bridge request ID is not the next durable sequence.');
    return undefined;
  }
  begin(id: number, digest: string, requestEvidence?: StorageLedgerRequest): unknown | undefined {
    if (this.terminal) throw new Error('Bridge ledger already contains a terminal receipt; no further exchange is permitted.');
    if ((this.scope === 'storage-http') !== (requestEvidence !== undefined))
      throw new Error('Bridge ledger request evidence does not match its scope.');
    if (requestEvidence) validateStorageLedgerRequest(requestEvidence);
    const prior = this.records.get(id);
    if (prior) {
      if (prior.request_sha256 !== digest || (requestEvidence !== undefined) !== (prior.request_evidence !== undefined) ||
          requestEvidence !== undefined && !equal(prior.request_evidence, requestEvidence))
        throw new Error('Bridge request ID reused with a different digest or raw request evidence.');
      if (prior.state !== 'response') throw new Error('Bridge request has an unresolved prior forward; never replay.');
      return prior.response;
    }
    if (id !== this.maxId + 1) throw new Error('Bridge request ID is not the next durable sequence.');
    const record: LedgerRecord = { schema_version: 1, scope: this.scope, authority_sha256: this.authorityHash,
      request_id: id, request_sha256: digest, state: 'pending', ...(requestEvidence ? { request_evidence: requestEvidence } : {}) };
    this.append(record); this.records.set(id, record); this.maxId = id; return undefined;
  }
  complete(id: number, digest: string, response: unknown): void {
    if (this.terminal) throw new Error('Bridge ledger already contains a terminal receipt; no further exchange is permitted.');
    const prior = this.records.get(id);
    if (!prior || prior.state !== 'pending' || prior.request_sha256 !== digest) throw new Error('Bridge response does not match pending durable request.');
    if (Buffer.byteLength(canonical(response)) > MAX_FRAME) throw new Error('Bridge response exceeds durable frame limit.');
    const record: LedgerRecord = { ...prior, state: 'response', response, response_sha256: sha(canonical(response)) };
    this.append(record); this.records.set(id, record);
  }
  recordTerminal(receipt: unknown): void {
    if (this.scope !== 'storage-http' || this.maxId < 1 || [...this.records.values()].some(record => record.state !== 'response') ||
        Buffer.byteLength(canonical(receipt)) > MAX_FRAME)
      throw new Error('Bridge terminal receipt requires a bounded, fully resolved StorageBridge transcript.');
    const record: LedgerTerminalRecord = { ...this.binding, kind: 'terminal_receipt', request_count: this.maxId,
      transcript_sha256: this.transcriptSha256(), receipt, receipt_sha256: sha(canonical(receipt)) };
    if (this.terminal) {
      if (!equal(this.terminal, record)) throw new Error('Bridge terminal receipt changed after restart.');
      return;
    }
    this.append(record); this.terminal = record;
  }
  terminalReceipt(): unknown | undefined { return this.terminal?.receipt; }
  get requestCount(): number { return this.maxId; }
}
function ledgerBinding(scope: string, authority: unknown, deadline: number, phase: string, plan: unknown,
  envelope: EffectEnvelope): LedgerBinding {
  integer(deadline, 1, Number.MAX_SAFE_INTEGER, 'original deadline');
  if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(phase)) throw new Error('Bridge phase identity is invalid.');
  return { schema_version: 1, kind: 'binding', scope, authority_sha256: sha(canonical(authority)),
    original_deadline_unix_ms: deadline, phase, plan_sha256: sha(canonical(plan)),
    allocation_sha256: effectAllocationFingerprint(envelope) };
}
function assertBridgeLive(envelope: EffectEnvelope, deadline: number, now: () => number): void {
  if (!isEffectBoundaryActiveFor(envelope)) throw new Error('Bridge dispatch requires its exact active effect allocation.');
  if (now() >= deadline) throw new Error('Bridge original deadline expired.');
}
async function withinBridgeDeadline<T>(promise: Promise<T>, deadline: number, now: () => number, label: string): Promise<T> {
  const remaining = deadline - now();
  if (remaining <= 0) throw new Error(`${label} exceeded the original deadline.`);
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([promise, new Promise<T>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded the original deadline.`)), remaining);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}
function boundedCampaignStore(base: CampaignStore, identity: { campaign_id: string; cell_run_id: string; owner_id: string;
  fencing_token: number }, deadline: number, envelope: EffectEnvelope, now: () => number,
  allowUnpublishedRun: boolean): CampaignStore {
  const operationOptions = (beforeDispatch?: () => Promise<void>): CampaignStoreOperationOptions => ({
    deadlineUnixMs: deadline,
    maxResponseBodyBytes: 4_194_304,
    beforeDispatch: async () => { assertBridgeLive(envelope, deadline, now); await beforeDispatch?.(); },
  });
  const read = async () => {
    assertBridgeLive(envelope, deadline, now);
    const stored = await withinBridgeDeadline(base.read(operationOptions()), deadline, now, 'Campaign store read');
    assertBridgeLive(envelope, deadline, now);
    return assertCampaignCustodyState(stored, { campaignId: identity.campaign_id, cellRunId: identity.cell_run_id,
      ownerId: identity.owner_id, fencingToken: identity.fencing_token }, now(), allowUnpublishedRun);
  };
  const beforeMutation = async () => { await read(); assertBridgeLive(envelope, deadline, now); };
  return {
    read,
    async write(state, expectedEtag, callOptions) {
      assertBridgeLive(envelope, deadline, now);
      const write = base.write(state, expectedEtag, operationOptions(async () => {
        await callOptions?.beforeDispatch?.();
        await beforeMutation();
      }));
      const etag = await withinBridgeDeadline(write, deadline, now, 'Campaign store write');
      assertBridgeLive(envelope, deadline, now);
      return etag;
    },
  };
}
function fixtureEffect(request: FixtureCommandRequest): ExternalEffect {
  const base = processEffectRequest([request.executable, ...request.argv]);
  return request.stdin === null ? base : { ...base, body: Buffer.from(request.stdin, 'utf8') };
}
function assertAllocated(envelope: EffectEnvelope, effect: ExternalEffect): void {
  const bodyHash = effect.body == null ? undefined : sha(effect.body);
  if (!envelope.effects.some(rule => rule.transport === effect.transport && rule.method === effect.method.toUpperCase() &&
      rule.resource === effect.resource && rule.path === effect.path && rule.body_sha256 === bodyHash))
    throw new Error('Bridge request differs from every exact original effect allocation.');
}
function assertFixtureCommand(request: FixtureCommandRequest, input: FixtureSetupInput): 'kubectl' | 'campaign' {
  const executable = path.posix.basename(request.executable);
  if (request.current_dir !== null) throw new Error('Fixture command unexpectedly changed its production working directory.');
  if (executable === 'kubectl' && request.argv.length >= 5 && request.argv[0] === '--context' &&
      request.argv[1] === input.authority.context && request.argv[2] === '--server' && request.argv[3] === input.authority.server) {
    const args = request.argv.slice(4), verb = args[0];
    const getNamespace = verb === 'get' && args.length === 6 && args[1] === 'namespace' && args[2] === input.authority.namespace &&
      args[3] === '--ignore-not-found' && args[4] === '-o' && args[5] === 'json';
    const getResourceCollision = verb === 'get' && args.length === 8 && args[1] === '--namespace' &&
      args[2] === input.authority.namespace && args[3] === '-f' && args[4] === '-' &&
      args[5] === '--ignore-not-found' && args[6] === '-o' && args[7] === 'json' && request.stdin === fixtureRuntimeList(input);
    const getResourceReadback = verb === 'get' && args.length === 7 && args[1] === '--namespace' &&
      args[2] === input.authority.namespace && args[3] === '-f' && args[4] === '-' &&
      args[5] === '-o' && args[6] === 'json' && request.stdin === fixtureRuntimeList(input);
    const createNamespace = verb === 'create' && args.length === 5 && args[1] === '-f' && args[2] === '-' &&
      args[3] === '-o' && args[4] === 'json' && request.stdin === fixtureNamespace(input);
    const createResources = verb === 'create' && args.length === 7 && args[1] === '--namespace' &&
      args[2] === input.authority.namespace && args[3] === '-f' && args[4] === '-' &&
      args[5] === '-o' && args[6] === 'json' && request.stdin === fixtureRuntimeList(input);
    if ((getNamespace && request.stdin === null) || getResourceCollision || getResourceReadback || createNamespace || createResources) return 'kubectl';
  }
  if (executable === 'npx' && request.stdin !== null && request.argv.length === 8 && request.argv[0] === '--no-install' &&
      request.argv[1] === 'tsx' && request.argv[2] === 'projects/arrowcol-oat/aks-campaign.ts' &&
      request.argv[3] === 'publish-inventory' && request.argv[4] === '--env-file' && request.argv[5] === input.bootstrap_env_file &&
      request.argv[6] === '--stamp-config' && request.argv[7] === input.stamp_config_path) return 'campaign';
  throw new Error('Fixture command is outside the fresh-create/read-only/publish-inventory allowlist.');
}
async function readJsonObject(text: string, label: string): Promise<Obj> {
  let value: unknown; try { value = JSON.parse(text) as unknown; } catch { throw new Error(`${label} is not valid JSON.`); }
  if (!object(value)) throw new Error(`${label} must be a JSON object.`);
  return value;
}
function parseJsonValues(text: string): unknown[] {
  const values: unknown[] = [];
  let start = -1, depth = 0, inString = false, escaped = false;
  for (let index = 0; index < text.length; index++) {
    const character = text[index]!;
    if (start < 0) {
      if (/\s/.test(character)) continue;
      start = index;
      if (character !== '{' && character !== '[') throw new Error('Create response is not a JSON object document.');
      depth = 1;
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (character === '\\') escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') inString = true;
    else if (character === '{' || character === '[') depth++;
    else if (character === '}' || character === ']') {
      if (--depth < 0) throw new Error('Create response JSON nesting is invalid.');
      if (depth === 0) {
        let value: unknown;
        try { value = JSON.parse(text.slice(start, index + 1)) as unknown; }
        catch { throw new Error('Create response contains malformed JSON.'); }
        values.push(value);
        start = -1;
      }
    }
  }
  if (start >= 0 || inString || values.length === 0) throw new Error('Create response is truncated or empty.');
  return values;
}
function publishReceipt(stdout: string, inventory: RunInventory): void {
  let value: unknown;
  try { value = JSON.parse(stdout) as unknown; } catch { throw new Error('Inventory publisher response is malformed JSON.'); }
  if (!object(value) || !exact(value, ['ok', 'receipt']) || value.ok !== true || !object(value.receipt) ||
      !exact(value.receipt, ['status', 'campaign_id', 'owner_id', 'fencing_token', 'orchestration_run_id', 'cell_run_id',
        'namespace_uid', 'resource_count', 'fic_state', 'qap_state', 'etag'])) throw new Error('Inventory publisher receipt schema is invalid.');
  const receipt = value.receipt;
  if (receipt.status !== 'persisted' || receipt.campaign_id !== inventory.campaign_id || receipt.owner_id !== inventory.owner_id ||
      receipt.fencing_token !== inventory.fencing_token || receipt.orchestration_run_id !== inventory.orchestration_run_id ||
      receipt.cell_run_id !== inventory.cell_run_id || receipt.namespace_uid !== inventory.namespace.uid ||
      receipt.resource_count !== inventory.resources.length || receipt.fic_state !== (inventory.fic?.state ?? null) ||
      receipt.qap_state !== (inventory.qap?.state ?? null) || typeof receipt.etag !== 'string' || receipt.etag.length === 0)
    throw new Error('Inventory publisher receipt does not bind the exact inventory and actual persisted state.');
}
async function runProcess(command: string[], stdin: Buffer | undefined, timeoutMs: number, cwd: string): Promise<ProcessResult> {
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 600_000) throw new Error('Process timeout is outside the original deadline.');
  return new Promise((resolve, reject) => {
    const child = spawn(command[0]!, command.slice(1), { detached: true, cwd, env: process.env,
      stdio: [stdin ? 'pipe' : 'ignore', 'pipe', 'pipe'] });
    let stdout: Buffer<ArrayBufferLike> = Buffer.alloc(0), stderr: Buffer<ArrayBufferLike> = Buffer.alloc(0);
    let failure: Error | undefined, settled = false;
    const kill = (message: string) => {
      if (failure) return; failure = new Error(message);
      if (child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') failure = new Error('Process group could not be terminated; effect outcome is unknown.');
      } }
    };
    const timer = setTimeout(() => kill('Bridge process exceeded its bound; effect outcome is unknown.'), timeoutMs);
    const collect = (target: Buffer, chunk: Buffer, label: string): Buffer => {
      const next = Buffer.concat([target, chunk]); if (next.length > MAX_OUTPUT) { kill(`Bridge process ${label} exceeded its output bound.`); return target; }
      return next;
    };
    const stdoutStream = child.stdout, stderrStream = child.stderr;
    if (!stdoutStream || !stderrStream) { clearTimeout(timer); child.kill('SIGKILL'); reject(new Error('Bridge process output channels are unavailable.')); return; }
    stdoutStream.on('data', chunk => { stdout = collect(stdout, Buffer.from(chunk), 'stdout'); });
    stderrStream.on('data', chunk => { stderr = collect(stderr, Buffer.from(chunk), 'stderr'); });
    child.once('error', error => { clearTimeout(timer); if (!settled) { settled = true; reject(new Error('Bridge process could not start.', { cause: error })); } });
    child.once('close', code => {
      clearTimeout(timer); if (settled) return; settled = true;
      if (failure) { reject(failure); return; }
      try { resolve({ returncode: code ?? -1, stdout: new TextDecoder('utf-8', { fatal: true }).decode(stdout),
        stderr: new TextDecoder('utf-8', { fatal: true }).decode(stderr) }); }
      catch (error) { reject(new Error('Bridge process output was not valid UTF-8.', { cause: error })); }
    });
    if (stdin) { child.stdin!.on('error', error => { if ((error as NodeJS.ErrnoException).code !== 'EPIPE') kill('Bridge process stdin failed; outcome is unknown.'); }); child.stdin!.end(stdin); }
  });
}
async function callCampaignMain(options: FixtureDispatcherOptions, request: FixtureCommandRequest, store: CampaignStore): Promise<ProcessResult> {
  const input = await readJsonObject(request.stdin!, 'RunInventory stdin');
  const args = request.argv.slice(3);
  const logs: string[] = [], original = console.log;
  console.log = (...values: unknown[]) => { logs.push(values.map(value => typeof value === 'string' ? value : String(value)).join(' ')); };
  try { await (options.campaignMain ?? campaignMain)(args, { store, azure: options.azure, env: options.env, runInventoryInput: input,
    runInventoryWriteOptions: { deadlineUnixMs: options.input.original_deadline_unix_ms } }); }
  finally { console.log = original; }
  const stdout = logs.length ? logs.join('\n') + '\n' : '';
  if (Buffer.byteLength(stdout) > MAX_OUTPUT) throw new Error('Campaign publisher output exceeded the fixture response bound.');
  return { returncode: 0, stdout, stderr: '' };
}
export class FixtureCommandDispatcher {
  private readonly now: () => number;
  private readonly ledger: DurableBridgeLedger;
  private readonly envelope: EffectEnvelope;
  private readonly campaignStore: CampaignStore;
  private readonly seen = new Map<number, FixtureCommandRequest>();
  private readonly responses = new Map<number, FixtureCommandResponse>();
  constructor(private readonly options: FixtureDispatcherOptions) {
    this.now = options.now ?? Date.now; validateFixtureInput(options.input, this.now());
    this.envelope = structuredClone(options.envelope);
    if (this.envelope.campaign_id !== options.input.authority.campaign_id || this.envelope.run_id !== options.input.authority.cell_run_id ||
        options.env.CAMPAIGN_ID !== options.input.authority.campaign_id || options.env.CAMPAIGN_OWNER_ID !== options.input.authority.owner_id ||
        options.env.CAMPAIGN_FENCING_TOKEN !== String(options.input.authority.fencing_token))
      throw new Error('Fixture setup does not match the selected campaign/effect authority.');
    this.ledger = new DurableBridgeLedger(options.ledgerPath, ledgerBinding('fixture-command', options.input.authority,
      options.input.original_deadline_unix_ms, 'fixture-setup', options.input, this.envelope));
    this.campaignStore = boundedCampaignStore(options.store, options.input.authority, options.input.original_deadline_unix_ms,
      this.envelope, this.now, true);
  }
  async exchange(value: unknown): Promise<FixtureCommandResponse> {
    const request = fixtureRequest(value, this.options.input.authority, this.options.input.original_deadline_unix_ms, this.now());
    const kind = assertFixtureCommand(request, this.options.input), effect = fixtureEffect(request);
    assertAllocated(this.envelope, effect); this.seen.set(request.request_id, request);
    assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
    await this.campaignStore.read();
    const cached = this.ledger.begin(request.request_id, request.request_sha256);
    if (cached !== undefined) { this.responses.set(request.request_id, cached as FixtureCommandResponse); return cached as FixtureCommandResponse; }
    let result: ProcessResult;
    if (kind === 'campaign') result = await dispatchExternalEffect(effect, async () => {
      assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
      await this.campaignStore.read();
      assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
      return callCampaignMain(this.options, request, this.campaignStore);
    });
    else result = await dispatchExternalEffect(effect, async () => {
      assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
      await this.campaignStore.read();
      assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
      return (this.options.runCommand ?? runProcess)([request.executable, ...request.argv],
        request.stdin === null ? undefined : Buffer.from(request.stdin),
        Math.min(request.remaining_timeout_ms, request.original_deadline_unix_ms - this.now()),
        request.current_dir ?? this.options.rootDirectory);
    });
    assertBridgeLive(this.envelope, request.original_deadline_unix_ms, this.now);
    if (!Number.isSafeInteger(result.returncode) || result.stdout.length > MAX_OUTPUT || result.stderr.length > MAX_OUTPUT)
      throw new Error('Fixture command result is invalid or oversized; outcome is not reusable.');
    if (kind === 'campaign') {
      const inventory = JSON.parse(request.stdin!) as RunInventory; publishReceipt(result.stdout, inventory);
      const stored = await this.campaignStore.read();
      if (!stored || !equal(stored.state.runInventory, inventory)) throw new Error('Campaign store readback does not match the actual published inventory.');
    }
    const response: FixtureCommandResponse = { schema_version: 1, kind: 'command_response', request_id: request.request_id,
      request_sha256: request.request_sha256, exit_code: result.returncode, stdout: result.stdout, stderr: result.stderr, truncated: false };
    this.ledger.complete(request.request_id, request.request_sha256, response); this.responses.set(request.request_id, response);
    return response;
  }
  async validateReceipt(value: unknown): Promise<FixtureReceipt> {
    const keys = ['schema_version', 'kind', 'inventory', 'namespace_response', 'command_responses', 'load_admitted'];
    if (!object(value) || !exact(value, keys) || value.schema_version !== 1 || value.kind !== 'fixture_setup_receipt' ||
        value.load_admitted !== false || !object(value.namespace_response) || !Array.isArray(value.command_responses))
      throw new Error('Fixture final receipt schema or load disposition is invalid.');
    const receipt = value as unknown as FixtureReceipt;
    if (receipt.inventory.campaign_id !== this.options.input.authority.campaign_id ||
        receipt.inventory.cell_run_id !== this.options.input.authority.cell_run_id || receipt.inventory.owner_id !== this.options.input.authority.owner_id ||
        receipt.inventory.fencing_token !== this.options.input.authority.fencing_token || receipt.inventory.load_admission !== 'not_authorized' ||
        receipt.inventory.planned_cell_duration_ms !== 0 || receipt.inventory.namespace.name !== this.options.input.authority.namespace ||
        !receipt.inventory.namespace.uid || receipt.inventory.resources.length !== 3 || receipt.command_responses.length !== this.responses.size ||
        !equal(receipt.command_responses, [...this.responses.values()].sort((a, b) => a.request_id - b.request_id)))
      throw new Error('Fixture final receipt does not bind actual created responses and the zero-row authority.');
    const publisher = [...this.seen.values()].find(request => request.argv[3] === 'publish-inventory');
    if (!publisher || !equal(JSON.parse(publisher.stdin!) as unknown, receipt.inventory)) throw new Error('Fixture inventory lacks its exact publish-inventory request.');
    const publishResponse = this.responses.get(publisher.request_id);
    if (!publishResponse || publishResponse.exit_code !== 0) throw new Error('Fixture inventory publication did not return success.');
    publishReceipt(publishResponse.stdout, receipt.inventory);
    const created: Obj[] = [];
    for (const request of this.seen.values()) if (request.executable.endsWith('kubectl') && request.argv.includes('create')) {
      const response = this.responses.get(request.request_id); if (!response || response.exit_code !== 0) throw new Error('Fixture create response is incomplete.');
      for (const resource of parseJsonValues(response.stdout)) { if (!object(resource)) throw new Error('Fixture create response is not a resource object.'); created.push(resource); }
    }
    const namespace = created.find(resource => resource.kind === 'Namespace' && object(resource.metadata) && resource.metadata.name === receipt.inventory.namespace.name);
    if (!namespace || !equal(namespace, receipt.namespace_response) || !object(namespace.metadata) ||
        namespace.metadata.uid !== receipt.inventory.namespace.uid || typeof namespace.metadata.resourceVersion !== 'string')
      throw new Error('Fixture namespace identity is not derived from its actual create response.');
    const seenUids = new Set<string>();
    for (const resource of receipt.inventory.resources) {
      const actual = created.find(item => object(item.metadata) && item.kind === resource.kind && item.metadata.name === resource.name &&
        item.metadata.namespace === resource.namespace);
      if (!actual || !object(actual.metadata) || !actual.metadata.uid || actual.metadata.uid !== resource.uid ||
          typeof actual.metadata.resourceVersion !== 'string' || !actual.metadata.resourceVersion || !Number.isSafeInteger(actual.metadata.generation) ||
          !resource.uid || resource.state !== 'observed' || resource.absence !== null || seenUids.has(resource.uid))
        throw new Error('Fixture resource identity is not derived from a unique actual create response.');
      seenUids.add(resource.uid);
    }
    assertBridgeLive(this.envelope, this.options.input.original_deadline_unix_ms, this.now);
    const stored = await this.campaignStore.read();
    if (!stored || !equal(stored.state.runInventory, receipt.inventory)) throw new Error('Fixture final inventory differs from the durable campaign store.');
    return receipt;
  }
}
function storageRequest(value: unknown, authority: StorageBridgeAuthority, deadline: number, now: number): StorageBridgeRequest {
  const keys = ['schema_version', 'kind', 'request_id', 'authority', 'original_deadline_unix_ms', 'remaining_timeout_ms',
    'target', 'method', 'url', 'headers', 'body', 'body_sha256', 'request_sha256'];
  if (!object(value) || !exact(value, keys) || value.schema_version !== 1 || value.kind !== 'storage_http_request' ||
      !['control', 'object_store'].includes(String(value.target)) || typeof value.method !== 'string' || typeof value.url !== 'string' ||
      !Array.isArray(value.headers) || value.headers.some(pair => !Array.isArray(pair) || pair.length !== 2 || pair.some(part => typeof part !== 'string')) ||
      typeof value.body !== 'string' || typeof value.body_sha256 !== 'string') throw new Error('Storage request schema is invalid.');
  integer(value.request_id, 1, Number.MAX_SAFE_INTEGER, 'request ID'); storageAuthority(value.authority);
  if (!equal(value.authority, authority) || value.original_deadline_unix_ms !== deadline) throw new Error('Storage request changed immutable authority/deadline.');
  checkDeadline(deadline, Number(value.remaining_timeout_ms), now, 10_000);
  checkDigest(value);
  const body = Buffer.from(value.body, 'utf8');
  if (sha(body) !== value.body_sha256 || body.length > MAX_STORAGE_BODY || Buffer.byteLength(canonical(value)) > MAX_FRAME)
    throw new Error('Storage request body digest or frame bound is invalid.');
  const url = new URL(value.url);
  if (url.href !== value.url || url.username || url.password || url.hash || /(?:sig|access_token|refresh_token|client_secret)=/i.test(value.url))
    throw new Error('Storage request URL is not canonical or contains credentials.');
  const seen = new Set<string>();
  for (const [name, content] of value.headers as [string, string][]) {
    const key = name.toLowerCase();
    if (seen.has(key) || !storageHeaderAllowed(key, false) || /[\r\n\0]/.test(content)) throw new Error('Storage request header is duplicated or not permitted.');
    seen.add(key);
  }
  validateStorageEffectHeaders(value.headers as [string, string][], now);
  validateStorageRoute(value.target as StorageBridgeRequest['target'], value.method, url, value.headers as [string, string][], body, authority);
  return value as unknown as StorageBridgeRequest;
}
function validateStorageEffectHeaders(headers: [string, string][], now: number): void {
  const requestIds = headers.filter(([name]) => name.toLowerCase() === 'x-ms-client-request-id');
  if (requestIds.length !== 1 || !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(requestIds[0]![1]))
    throw new Error('Storage request must carry one canonical lowercase UUID v4 client request ID.');
  for (const [name, value] of headers) if (name.toLowerCase() === 'date' || name.toLowerCase() === 'x-ms-date') {
    const timestamp = Date.parse(value);
    if (!Number.isFinite(timestamp) || new Date(timestamp).toUTCString() !== value || Math.abs(timestamp - now) > 300_000)
      throw new Error('Storage request date header must be a current IMF-fixdate within five minutes.');
  }
}
function validateStorageLedgerRequest(value: unknown): asserts value is StorageLedgerRequest {
  if (!object(value) || !exact(value, ['target', 'method', 'url', 'headers', 'body_sha256']) ||
      !['control', 'object_store'].includes(String(value.target)) || typeof value.method !== 'string' ||
      !/^[A-Z]+$/.test(value.method) || typeof value.url !== 'string' || typeof value.body_sha256 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(value.body_sha256) || !Array.isArray(value.headers) ||
      value.headers.some(pair => !Array.isArray(pair) || pair.length !== 2 || pair.some(part => typeof part !== 'string')))
    throw new Error('Bridge ledger raw storage request evidence is malformed.');
  const seen = new Set<string>();
  for (const [name, content] of value.headers as [string, string][]) {
    const key = name.toLowerCase();
    if (seen.has(key) || !storageHeaderAllowed(key, false) || /[\r\n\0]/.test(content))
      throw new Error('Bridge ledger raw storage request headers are invalid.');
    seen.add(key);
  }
  const url = new URL(value.url);
  if (url.href !== value.url || url.username || url.password || url.hash ||
      /(?:sig|access_token|refresh_token|client_secret)=/i.test(value.url))
    throw new Error('Bridge ledger raw storage request URL is invalid.');
}
function storageHeaderAllowed(name: string, response: boolean): boolean {
  return name.startsWith('x-ms-meta-') || ['content-length','content-type','content-md5','date','x-ms-version','x-ms-date',
    'x-ms-client-request-id','x-ms-request-id','x-ms-lease-id', ...(response ? ['etag','last-modified','server','accept-ranges',
      'content-range','x-ms-error-code','x-ms-blob-type','x-ms-blob-content-md5','x-ms-lease-status','x-ms-lease-state',
      'x-ms-lease-duration','x-ms-creation-time','x-ms-server-encrypted','transfer-encoding'] : ['if-match','if-none-match',
      'if-modified-since','if-unmodified-since','range','x-ms-range','x-ms-lease-action','x-ms-lease-duration',
      'x-ms-proposed-lease-id','x-ms-blob-type','user-agent','accept','accept-encoding'])].includes(name);
}
function validateStorageRoute(target: StorageBridgeRequest['target'], method: string, url: URL,
  headers: [string, string][], body: Buffer, authority: StorageBridgeAuthority): void {
  const query = new Map<string, string>(); for (const [key, value] of url.searchParams) {
    if (query.has(key)) throw new Error('Storage request has a duplicate query key.'); query.set(key, value);
  }
  const header = (name: string) => headers.find(([key]) => key.toLowerCase() === name)?.[1];
  const control = new URL(authority.control_container_url);
  if (target === 'control') {
    const endpoint = new URL(url.href); endpoint.search = '';
    if (endpoint.href !== authority.control_container_url || query.get('restype') !== 'container' || body.length !== 0 ||
        method === 'HEAD' && (query.size !== 1 || query.has('comp')) ||
        method === 'PUT' && (query.size !== 2 || !['lease','metadata'].includes(query.get('comp') ?? '') ||
          query.get('comp') === 'lease' && !['acquire','release'].includes(header('x-ms-lease-action') ?? '') ||
          query.get('comp') === 'metadata' && header('x-ms-lease-action') !== undefined) || !['HEAD','PUT'].includes(method))
      throw new Error('Storage control request is outside the exact container operation allowlist.');
    return;
  }
  const root = new URL(authority.table_root_url), container = root.username, origin = `https://${authority.storage_account}.blob.core.windows.net`;
  const endpoint = `${origin}/${container}`, prefix = `/${container}${root.pathname}`;
  const listingPrefix = query.get('prefix') ?? '';
  const allowedListKeys = ['restype','comp','prefix','marker','maxresults','startFrom'];
  const startFrom = query.get('startFrom'), expectedLogPrefix = `${root.pathname.slice(1)}_delta_log/`;
  const startFromValid = startFrom === undefined || query.size === 4 && query.get('prefix') === expectedLogPrefix &&
    (startFrom === expectedLogPrefix.slice(0, -1) || startFrom.startsWith(expectedLogPrefix) &&
      /^[0-9]{20}$/.test(startFrom.slice(expectedLogPrefix.length))) &&
    url.search.slice(1) === new URLSearchParams([['restype', 'container'], ['comp', 'list'],
      ['prefix', expectedLogPrefix], ['startFrom', startFrom]]).toString();
  const isList = method === 'GET' && url.origin === origin && url.pathname === `/${container}` && query.size >= 3 && query.size <= 5 &&
    query.get('restype') === 'container' && query.get('comp') === 'list' && listingPrefix.startsWith(root.pathname.slice(1)) &&
    startFromValid && [...query.keys()].every(key => allowedListKeys.includes(key));
  const isLogRead = ['GET','HEAD'].includes(method) && url.origin === origin && url.pathname.startsWith(`${prefix}_delta_log/`) && query.size === 0;
  const isCommit = method === 'PUT' && url.origin === origin && url.pathname === `${prefix}_delta_log/00000000000000000000.json` &&
    query.size === 0 && header('if-none-match') === '*' && body.length > 0;
  if (!url.href.startsWith(endpoint) || url.username || url.password || control.origin !== origin || !(isList || isLogRead || isCommit))
    throw new Error('Storage object-store request is outside the exact fresh-table/Delta-log allowlist.');
}
function storageEffect(request: StorageBridgeRequest): ExternalEffect {
  const url = new URL(request.url), body = Buffer.from(request.body, 'utf8');
  const allocationHeaders = request.headers.filter(([name]) => !VOLATILE_STORAGE_EFFECT_HEADERS.has(name.toLowerCase()));
  return { transport: 'azure-rest', method: request.method, resource: `${url.origin}|${STORAGE_RESOURCE}`,
    path: `${url.pathname}${url.search}|headers-sha256=${sha(canonical(allocationHeaders))}`, body };
}
async function readBody(response: Response, maxBytes: number): Promise<Buffer> {
  if (!response.body) return Buffer.alloc(0);
  const reader = response.body.getReader(), chunks: Buffer[] = []; let size = 0;
  for (;;) { const { done, value } = await reader.read(); if (done) break;
    size += value.byteLength; if (size > maxBytes) { await reader.cancel(); throw new Error('Storage response exceeds its bounded body size.'); }
    chunks.push(Buffer.from(value)); }
  return Buffer.concat(chunks, size);
}
export class StorageBridgeDispatcher {
  private readonly now: () => number;
  private readonly ledger: DurableBridgeLedger;
  private readonly envelope: EffectEnvelope;
  private readonly campaignStore: CampaignStore;
  private readonly recoveredLedger: boolean;
  private readonly controlProgress?: PlannedControlProgress;
  constructor(private readonly options: StorageDispatcherOptions) {
    this.now = options.now ?? Date.now; storageAuthority(options.authority);
    this.envelope = structuredClone(options.envelope);
    if (options.azure.c.account !== options.authority.storage_account || options.envelope.campaign_id !== options.authority.campaign_id ||
        options.envelope.run_id !== options.authority.cell_run_id) throw new Error('Storage bridge does not match the selected stamp/effect envelope.');
    if (options.controlPlan) validatePlannedControlPlan(options.controlPlan);
    this.ledger = new DurableBridgeLedger(options.ledgerPath, ledgerBinding('storage-http', options.authority,
      options.originalDeadlineUnixMs, options.phase ?? 'storage-http', options.planIdentity ?? options.authority, this.envelope));
    this.recoveredLedger = this.ledger.requestCount > 0;
    this.controlProgress = options.controlPlan ? { stage: 0, phase: 'acquire', registered: false, complete: false } : undefined;
    this.campaignStore = boundedCampaignStore(options.store, { campaign_id: options.authority.campaign_id,
      cell_run_id: options.authority.cell_run_id, owner_id: options.authority.owner_id,
      fencing_token: options.authority.fencing_token }, options.originalDeadlineUnixMs, this.envelope, this.now, false);
  }
  terminalEvidence(): unknown | undefined { return this.ledger.terminalReceipt(); }
  get recordedRequestCount(): number { return this.ledger.requestCount; }
  recordTerminalEvidence(value: unknown): void { this.ledger.recordTerminal(value); }
  assertControlPlanComplete(): void {
    if (this.options.controlPlan && (this.recoveredLedger || !this.controlProgress?.complete))
      throw new Error('Owned-run initializer did not consume both planned control identities in the exact registration/drain trace.');
  }
  private plannedControlStep(request: StorageBridgeRequest): PlannedControlStep | undefined {
    const plan = this.options.controlPlan, progress = this.controlProgress;
    if (!plan || !progress || request.method === 'GET' || request.method === 'HEAD') return undefined;
    if (this.recoveredLedger) throw new Error('A recovered bridge ledger permits cached replies and bounded reads only; mutating replay is forbidden.');
    if (request.target === 'object_store') {
      if (!progress.registered || progress.stage !== 1 || progress.phase !== 'acquire')
        throw new Error('Object-store effects require the exact planned publisher registration before terminal drain.');
      return undefined;
    }
    const url = new URL(request.url), leaseId = plan.control_lease_ids[progress.stage];
    if (url.searchParams.get('comp') === 'lease') {
      const action = singleHeader(request.headers, 'x-ms-lease-action');
      if (action === 'acquire') {
        const proposed = singleHeader(request.headers, 'x-ms-proposed-lease-id');
        if (progress.phase !== 'acquire' || proposed !== leaseId)
          throw new Error('Control lease acquisition does not match the next immutable plan identity.');
        return { kind: 'acquire', stage: progress.stage, leaseId };
      }
      if (action === 'release') {
        const supplied = singleHeader(request.headers, 'x-ms-lease-id');
        if (progress.phase !== 'release' || supplied !== leaseId)
          throw new Error('Control lease release does not match the active immutable plan identity.');
        return { kind: 'release', stage: progress.stage, leaseId };
      }
      throw new Error('Planned control lease action is not permitted.');
    }
    if (url.searchParams.get('comp') === 'metadata') {
      const supplied = singleHeader(request.headers, 'x-ms-lease-id');
      if (progress.phase !== 'metadata' || supplied !== leaseId)
        throw new Error('Control metadata write does not match the active immutable plan identity.');
      const metadataState = plannedPublisherState(request.headers, plan, progress.stage);
      return { kind: 'metadata', stage: progress.stage, leaseId, metadataState };
    }
    throw new Error('Unplanned control mutation is refused before provider dispatch.');
  }
  private recordPlannedControlResponse(step: PlannedControlStep | undefined, reply: StorageBridgeReply): void {
    if (!step || !this.controlProgress) return;
    const progress = this.controlProgress;
    if (step.kind === 'acquire') {
      if (reply.outcome === 'response' && reply.status === 201) progress.phase = 'metadata';
      else if (reply.outcome === 'unknown' || reply.outcome === 'response' && reply.status >= 500) progress.phase = 'release';
      else progress.phase = 'blocked';
      return;
    }
    if (step.kind === 'metadata') {
      progress.metadataState = reply.outcome === 'response' && reply.status === 200 ? step.metadataState : undefined;
      progress.phase = 'release';
      return;
    }
    if (reply.outcome !== 'response' || reply.status < 200 || reply.status >= 300) {
      progress.phase = 'blocked';
      return;
    }
    if (step.stage === 0 && progress.metadataState === 'active') {
      progress.registered = true; progress.stage = 1; progress.phase = 'acquire'; progress.metadataState = undefined;
      return;
    }
    if (step.stage === 1 && progress.metadataState === 'removed') {
      progress.complete = true; progress.phase = 'done';
      return;
    }
    progress.phase = 'blocked';
  }
  async exchange(value: unknown): Promise<StorageBridgeReply> {
    const request = storageRequest(value, this.options.authority, this.options.originalDeadlineUnixMs, this.now());
    const effect = storageEffect(request);
    assertBridgeLive(this.envelope, this.options.originalDeadlineUnixMs, this.now);
    await this.campaignStore.read();
    const requestEvidence: StorageLedgerRequest = { target: request.target, method: request.method, url: request.url,
      headers: request.headers.map(([name, value]) => [name, value] as [string, string]), body_sha256: request.body_sha256 };
    const cached = this.ledger.peek(request.request_id, request.request_sha256, requestEvidence);
    if (cached !== undefined) return cached as StorageBridgeReply;
    if (this.recoveredLedger && request.method !== 'GET' && request.method !== 'HEAD')
      throw new Error('A recovered bridge ledger permits cached replies and bounded reads only; mutating replay is forbidden.');
    const controlStep = this.plannedControlStep(request);
    this.ledger.begin(request.request_id, request.request_sha256, requestEvidence);
    let reply: StorageBridgeReply;
    try {
      const response = await dispatchExternalEffect(effect, async () => {
        assertBridgeLive(this.envelope, this.options.originalDeadlineUnixMs, this.now);
        const token = await this.options.azure.token(STORAGE_RESOURCE);
        await this.campaignStore.read();
        assertBridgeLive(this.envelope, this.options.originalDeadlineUnixMs, this.now);
        const headers = Object.fromEntries(request.headers);
        headers.Authorization = `Bearer ${token}`;
        if (!Object.keys(headers).some(name => name.toLowerCase() === 'x-ms-date')) headers['x-ms-date'] = new Date(this.now()).toUTCString();
        const timeout = Math.min(request.remaining_timeout_ms, this.options.originalDeadlineUnixMs - this.now(), 10_000);
        const received = await (this.options.fetcher ?? fetch)(request.url, { method: request.method, headers, body: request.body || undefined,
          redirect: 'error', signal: AbortSignal.timeout(timeout) });
        const bytes = await readBody(received, MAX_STORAGE_BODY), text = new TextDecoder('utf-8', { fatal: true }).decode(bytes);
        assertBridgeLive(this.envelope, this.options.originalDeadlineUnixMs, this.now);
        const allowed = [...received.headers.entries()].filter(([name]) => storageHeaderAllowed(name.toLowerCase(), true));
        return { status: received.status, headers: allowed, body: text };
      });
      if (!object(response) || !Number.isInteger(response.status) || typeof response.body !== 'string' || !Array.isArray(response.headers))
        throw new Error('Storage provider returned an invalid response.');
      const headers = response.headers as [string, string][];
      reply = { schema_version: 1, kind: 'storage_http_response', request_id: request.request_id,
        request_sha256: request.request_sha256, outcome: 'response', status: response.status,
        headers, body: response.body, body_sha256: sha(response.body), truncated: false };
    } catch (error) {
      const refused = error instanceof EffectBoundaryRefused;
      reply = { schema_version: 1, kind: 'storage_http_response', request_id: request.request_id, request_sha256: request.request_sha256,
        outcome: refused ? 'definitely_not_sent' : 'unknown', error_code: refused ? 'admission_refused' : 'response_lost' };
    }
    this.ledger.complete(request.request_id, request.request_sha256, reply);
    this.recordPlannedControlResponse(controlStep, reply);
    return reply;
  }
}async function* ndjsonLines(stream: NodeJS.ReadableStream): AsyncGenerator<Buffer> {
  let pending = Buffer.alloc(0);
  for await (const raw of stream as AsyncIterable<Buffer>) {
    pending = Buffer.concat([pending, Buffer.from(raw)]);
    if (pending.length > MAX_FRAME && !pending.includes(0x0a)) throw new Error('Bridge child emitted an oversized frame.');
    for (;;) { const index = pending.indexOf(0x0a); if (index < 0) break;
      if (index > MAX_FRAME) throw new Error('Bridge child emitted an oversized frame.');
      yield pending.subarray(0, index); pending = pending.subarray(index + 1); }
    if (pending.length > MAX_FRAME) throw new Error('Bridge child emitted an oversized frame.');
  }
  if (pending.length) throw new Error('Bridge child closed with a truncated NDJSON frame.');
}
function privateJson(filename: string, limit: number): unknown {
  const metadata = fs.lstatSync(filename);
  if (!metadata.isFile() || metadata.isSymbolicLink() || metadata.size > limit || (metadata.mode & 0o077) !== 0)
    throw new Error('Bridge input must be a bounded private regular file.');
  try { return JSON.parse(fs.readFileSync(filename, 'utf8')) as unknown; } catch { throw new Error('Bridge input is not valid JSON.'); }
}
function bridgeChildEnvironment(): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = {};
  for (const key of ['PATH','HOME','TMPDIR','TMP','TEMP','LANG','LC_ALL']) if (process.env[key] !== undefined) environment[key] = process.env[key];
  return environment;
}
async function writeFrame(child: ChildProcess, value: unknown): Promise<void> {
  if (!child.stdin || child.stdin.destroyed) throw new Error('Bridge child response channel is closed.');
  const frame = Buffer.from(JSON.stringify(value) + '\n'); if (frame.length > MAX_FRAME) throw new Error('Bridge response frame exceeds its bound.');
  if (!child.stdin.write(frame)) await new Promise<void>((resolve, reject) => {
    child.stdin!.once('drain', resolve); child.stdin!.once('error', reject);
  });
}
async function runBridgeChild(executable: string, args: string[], cwd: string, deadline: number,
  requestKind: string, finalKind: string, exchange: (request: unknown) => Promise<unknown>): Promise<{ receipt: unknown; exchangeCount: number }> {
  if (!path.isAbsolute(executable) || !path.isAbsolute(cwd) || Date.now() >= deadline) throw new Error('Bridge child path or original deadline is invalid.');
  const child = spawn(executable, args, { cwd, env: bridgeChildEnvironment(), detached: true, stdio: ['pipe','pipe','pipe'] });
  let stderr = Buffer.alloc(0), final: unknown, finalSeen = false, failure: Error | undefined, exchangeCount = 0;
  const kill = () => { if (!child.pid) return; try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ESRCH') failure = new Error('Bridge child could not be stopped; effect outcome is unknown.'); } };
  child.stderr!.on('data', chunk => { const next = Buffer.concat([stderr, Buffer.from(chunk)]);
    if (next.length > MAX_OUTPUT) { failure = new Error('Bridge child diagnostics exceeded their bound.'); kill(); } else stderr = next; });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
    child.once('error', reject); child.once('close', (code, signal) => resolve({ code, signal }));
  });
  const timeout = setTimeout(() => { failure = new Error('Bridge child exceeded the original deadline; outcome is unknown.'); kill(); }, Math.max(1, deadline - Date.now()));
  try {
    for await (const bytes of ndjsonLines(child.stdout!)) {
      if (finalSeen) throw new Error('Bridge child emitted output after its final receipt.');
      let value: unknown; try { value = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)) as unknown; }
      catch { throw new Error('Bridge child emitted a malformed/non-UTF8 frame.'); }
      if (!object(value) || typeof value.kind !== 'string') throw new Error('Bridge child frame has no typed kind.');
      if (value.kind === requestKind) { await writeFrame(child, await exchange(value)); exchangeCount++; }
      else if (value.kind === finalKind) { final = value; finalSeen = true; child.stdin!.end(); }
      else throw new Error('Bridge child emitted an unexpected frame kind.');
    }
    const result = await closed;
    if (failure) throw failure;
    if (result.code !== 0 || !finalSeen) throw new Error('Bridge child failed or omitted its final actual receipt; no fallback is permitted.');
    if (exchangeCount === 0) throw new Error('Bridge child emitted a final receipt without any actual bridge exchange.');
    return { receipt: final, exchangeCount };
  } catch (error) { kill(); await closed.catch(() => undefined); throw error; }
  finally { clearTimeout(timeout); }
}
export async function runFixtureSetupChild(executable: string, inputPath: string, options: FixtureDispatcherOptions): Promise<FixtureReceipt> {
  if (!path.isAbsolute(inputPath) || !equal(privateJson(inputPath, MAX_OUTPUT), options.input)) throw new Error('Fixture child input file differs from its bound setup allocation.');
  const dispatcher = new FixtureCommandDispatcher(options);
  const result = await runBridgeChild(executable, ['--fixture-setup', inputPath, '--fixture-bridge-stdio'],
    options.rootDirectory, options.input.original_deadline_unix_ms, 'command_request', 'fixture_setup_receipt', request => dispatcher.exchange(request));
  return dispatcher.validateReceipt(result.receipt);
}
interface OwnedRunTablePlan { account: string; container: string; campaign_root: string; run_root: string; campaign_id: string; run_id: string;
  generation: string; publisher_id: string; control_lease_ids: [string, string]; table: string; partition_by_ingestion_date: boolean;
  owner_id: string; fencing_token: number }
export interface OwnedRunTableReceipt { allocation: OwnedRunTablePlan; delta: { table_id: string; version: number; schema_json: string;
  partition_columns: string[]; active_files: number }; publisher_drained: true }
export interface OwnedRunTableBridgeReceipt { schema_version: 1; kind: 'owned_run_table_receipt'; authority: StorageBridgeAuthority;
  original_deadline_unix_ms: number; receipt: OwnedRunTableReceipt }
export interface OwnedRunTableArtifactIdentity { source_commit: string; executable_sha256: string }
export interface OwnedRunTableParentReceipt { schema_version: 1; kind: 'owned_run_table_parent_receipt';
  artifact_identity: OwnedRunTableArtifactIdentity; exchange_count: number; bridge_receipt: OwnedRunTableBridgeReceipt }
interface OwnedRunBridgeInput { authority: StorageBridgeAuthority; original_deadline_unix_ms: number }
const APPROVED_OWNED_RUN_SOURCE = '2d329ab907f15c8123a08b933c01d98670bfd755';
const OWNED_RUN_TABLE = 'telemetry.sqlserver.sqlserver_availability_replica_states';
const OWNED_RUN_PARTITION = '__arrowcol_ingestion';
const OWNED_RUN_TABLE_RELATIVE_PATH = 'dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/';
export interface OwnedRunInitializerOptions extends Omit<StorageDispatcherOptions,
  'authority' | 'originalDeadlineUnixMs' | 'ledgerPath' | 'phase' | 'planIdentity'> {
  executable: string; artifactIdentity: OwnedRunTableArtifactIdentity; planPath: string; bridgeInputPath: string;
  ledgerPath: string; rootDirectory: string; now?: () => number
}
function validateOwnedRunTablePlan(value: unknown): asserts value is OwnedRunTablePlan {
  const keys = ['account','container','campaign_root','run_root','campaign_id','run_id','generation','publisher_id',
    'control_lease_ids','table','partition_by_ingestion_date','owner_id','fencing_token'];
  if (!object(value) || !exact(value, keys) || typeof value.account !== 'string' || !/^[a-z0-9]{3,24}$/.test(value.account) ||
      typeof value.container !== 'string' || !/^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$/.test(value.container) ||
      typeof value.campaign_root !== 'string' || typeof value.run_root !== 'string' || typeof value.table !== 'string' ||
      value.table !== OWNED_RUN_TABLE || value.partition_by_ingestion_date !== true)
    throw new Error('Owned-run table plan schema or frozen table selection is invalid.');
  uuid(value.campaign_id, 'campaign'); uuid(value.run_id, 'run'); uuid(value.generation, 'generation'); uuid(value.owner_id, 'owner');
  uuidV4(value.publisher_id, 'planned publisher');
  if (!Array.isArray(value.control_lease_ids) || value.control_lease_ids.length !== 2)
    throw new Error('Owned-run table plan requires exactly two ordered control lease identities.');
  value.control_lease_ids.forEach((id, index) => uuidV4(id, 'planned control lease ' + index));
  if (new Set([value.publisher_id, ...value.control_lease_ids]).size !== 3)
    throw new Error('Owned-run publisher and control lease identities must be distinct.');
  integer(value.fencing_token, 1, Number.MAX_SAFE_INTEGER, 'fencing token');
  const root = (raw: string): URL => {
    let url: URL; try { url = new URL(raw); } catch { throw new Error('Owned-run table plan root is invalid.'); }
    if (url.protocol !== 'abfss:' || url.username !== value.container || url.hostname !== `${value.account}.dfs.core.windows.net` ||
        url.password || url.port || url.search || url.hash || !url.pathname.endsWith('/') || url.href !== raw ||
        raw.includes('%') || raw.includes('\\') || url.pathname.split('/').some(part => part === '.' || part === '..') ||
        /[\u0000-\u0020]/.test(url.pathname)) throw new Error('Owned-run table plan root is not canonical and credential-free.');
    return url;
  };
  const campaign = root(value.campaign_root), run = root(value.run_root);
  if (!campaign.pathname.endsWith(`/campaigns/${value.campaign_id}/`) ||
      run.href !== new URL(`runs/${value.run_id}/`, campaign).href)
    throw new Error('Owned-run table roots do not bind the exact campaign and run.');
}
function validatePlannedControlPlan(value: PlannedControlPlan): void {
  uuidV4(value.publisher_id, 'planned publisher');
  uuid(value.generation, 'generation');
  if (!Array.isArray(value.control_lease_ids) || value.control_lease_ids.length !== 2)
    throw new Error('Storage bridge requires exactly two ordered planned control lease IDs.');
  value.control_lease_ids.forEach((id, index) => uuidV4(id, 'planned control lease ' + index));
  if (new Set([value.publisher_id, ...value.control_lease_ids]).size !== 3)
    throw new Error('Planned publisher and control lease identities must be distinct.');
}
function plannedPublisherState(headers: [string, string][], plan: PlannedControlPlan, stage: 0 | 1): 'active' | 'uncertain' | 'removed' {
  const raw = singleHeader(headers, 'x-ms-meta-publishers');
  if (raw === undefined) throw new Error('Planned control metadata omits the publisher registry.');
  let registry: unknown;
  try { registry = JSON.parse(raw) as unknown; } catch { throw new Error('Planned publisher registry metadata is invalid JSON.'); }
  if (!object(registry)) throw new Error('Planned publisher registry metadata is not an object.');
  const member = registry[plan.publisher_id];
  if (stage === 0 && object(member) && exact(member, ['generation', 'state']) &&
      member.generation === plan.generation && member.state === 'active') return 'active';
  if (stage === 1 && member === undefined) return 'removed';
  if (stage === 1 && object(member) && exact(member, ['generation', 'state']) &&
      member.generation === plan.generation && member.state === 'uncertain') return 'uncertain';
  throw new Error('Control publisher metadata does not match the planned registration or terminal transition.');
}
function validateOwnedRunBridgeInput(value: unknown, plan: OwnedRunTablePlan, now: number): asserts value is OwnedRunBridgeInput {
  if (!object(value) || !exact(value, ['authority','original_deadline_unix_ms']))
    throw new Error('Owned-run bridge sidecar schema is invalid.');
  storageAuthority(value.authority);
  integer(value.original_deadline_unix_ms, 1, Number.MAX_SAFE_INTEGER, 'original deadline');
  checkDeadline(value.original_deadline_unix_ms, value.original_deadline_unix_ms - now, now, 600_000);
  const tableRoot = new URL(OWNED_RUN_TABLE_RELATIVE_PATH, plan.run_root).href;
  if (value.authority.campaign_id !== plan.campaign_id || value.authority.cell_run_id !== plan.run_id ||
      value.authority.owner_id !== plan.owner_id || value.authority.fencing_token !== plan.fencing_token ||
      value.authority.reset_generation !== plan.generation || value.authority.storage_account !== plan.account ||
      new URL(value.authority.table_root_url).username !== plan.container || value.authority.table_root_url !== tableRoot ||
      value.authority.control_container_url !== `https://${plan.account}.blob.core.windows.net/${plan.container}`)
    throw new Error('Owned-run bridge authority differs from the exact selected one-table allocation.');
}
function validateOwnedRunArtifact(executable: string, identity: OwnedRunTableArtifactIdentity): OwnedRunTableArtifactIdentity {
  if (!object(identity) || !exact(identity, ['source_commit','executable_sha256']) ||
      identity.source_commit !== APPROVED_OWNED_RUN_SOURCE || typeof identity.executable_sha256 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(identity.executable_sha256))
    throw new Error('Owned-run initializer artifact identity is not the approved integrated source.');
  const stat = fs.lstatSync(executable);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size === 0 || stat.size > 512 * 1024 * 1024)
    throw new Error('Owned-run initializer executable must be a bounded regular non-symlink file.');
  fs.accessSync(executable, fs.constants.X_OK);
  if (sha(fs.readFileSync(executable)) !== identity.executable_sha256)
    throw new Error('Owned-run initializer executable does not match its pinned build identity.');
  return identity;
}
function validateOwnedRunReceipt(value: unknown, plan: OwnedRunTablePlan, authority: StorageBridgeAuthority,
  deadline: number): asserts value is OwnedRunTableBridgeReceipt {
  if (!object(value) || !exact(value, ['schema_version','kind','authority','original_deadline_unix_ms','receipt']) ||
      value.schema_version !== 1 || value.kind !== 'owned_run_table_receipt' || !equal(value.authority, authority) ||
      value.original_deadline_unix_ms !== deadline || !object(value.receipt) ||
      !exact(value.receipt, ['allocation','delta','publisher_drained']) || !equal(value.receipt.allocation, plan) ||
      value.receipt.publisher_drained !== true || !object(value.receipt.delta) ||
      !exact(value.receipt.delta, ['table_id','version','schema_json','partition_columns','active_files']))
    throw new Error('Owned-run initializer terminal receipt is malformed or not bound to the exact authority/plan.');
  const delta = value.receipt.delta;
  uuid(delta.table_id, 'actual Delta table ID');
  if (delta.version !== 0 || delta.active_files !== 0 || typeof delta.schema_json !== 'string' || !Array.isArray(delta.partition_columns) ||
      !equal(delta.partition_columns, [OWNED_RUN_PARTITION]))
    throw new Error('Owned-run initializer receipt is not a fresh zero-file version-zero Delta readback.');
  let schema: unknown; try { schema = JSON.parse(delta.schema_json) as unknown; } catch { throw new Error('Owned-run Delta schema readback is invalid.'); }
  if (!object(schema) || !Array.isArray(schema.fields) || !schema.fields.some(field => object(field) && field.name === OWNED_RUN_PARTITION &&
      field.type === 'string' && field.nullable === false)) throw new Error('Owned-run Delta schema lacks its exact ingestion partition column.');
}
function validateOwnedRunParentReceipt(value: unknown, artifactIdentity: OwnedRunTableArtifactIdentity,
  plan: OwnedRunTablePlan, authority: StorageBridgeAuthority, deadline: number, requestCount: number): asserts value is OwnedRunTableParentReceipt {
  if (!object(value) || !exact(value, ['schema_version','kind','artifact_identity','exchange_count','bridge_receipt']) ||
      value.schema_version !== 1 || value.kind !== 'owned_run_table_parent_receipt' || !equal(value.artifact_identity, artifactIdentity) ||
      value.exchange_count !== requestCount || !Number.isSafeInteger(value.exchange_count) || requestCount < 1)
    throw new Error('Owned-run parent receipt is not bound to the actual durable bridge transcript and build.');
  validateOwnedRunReceipt(value.bridge_receipt, plan, authority, deadline);
}
export async function initializeOwnedRunTable(options: OwnedRunInitializerOptions): Promise<OwnedRunTableParentReceipt> {
  const now = options.now ?? Date.now;
  if (![options.executable, options.planPath, options.bridgeInputPath, options.rootDirectory, options.ledgerPath].every(path.isAbsolute))
    throw new Error('Owned-run initializer paths must be absolute.');
  const planValue = privateJson(options.planPath, MAX_OUTPUT), bridgeValue = privateJson(options.bridgeInputPath, MAX_OUTPUT);
  validateOwnedRunTablePlan(planValue);
  validateOwnedRunBridgeInput(bridgeValue, planValue, now());
  const plan = planValue, authority = bridgeValue.authority, deadline = bridgeValue.original_deadline_unix_ms;
  if (options.envelope.campaign_id !== plan.campaign_id || options.envelope.run_id !== plan.run_id ||
      options.envelope.storage_account !== plan.account || options.azure.c.account !== plan.account)
    throw new Error('Owned-run initializer does not match the exact active effect envelope and selected stamp.');
  const artifactIdentity = validateOwnedRunArtifact(options.executable, options.artifactIdentity);
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope: options.envelope,
    azure: options.azure, store: options.store, ledgerPath: options.ledgerPath, phase: 'owned-run-table-initialization',
    planIdentity: { plan, artifact_identity: artifactIdentity },
    controlPlan: { publisher_id: plan.publisher_id, generation: plan.generation, control_lease_ids: plan.control_lease_ids },
    fetcher: options.fetcher, now });
  const cached = dispatcher.terminalEvidence();
  if (cached !== undefined) {
    validateOwnedRunParentReceipt(cached, artifactIdentity, plan, authority, deadline, dispatcher.recordedRequestCount);
    return cached;
  }
  assertBridgeLive(options.envelope, deadline, now);
  const child = await withinBridgeDeadline(runBridgeChild(options.executable,
    ['initialize-owned-run-table', '--plan', options.planPath, '--storage-bridge-input', options.bridgeInputPath, '--storage-bridge-stdio'],
    options.rootDirectory, deadline, 'storage_http_request', 'owned_run_table_receipt', request => dispatcher.exchange(request)),
    deadline, now, 'Owned-run initializer');
  validateOwnedRunReceipt(child.receipt, plan, authority, deadline);
  if (child.exchangeCount !== dispatcher.recordedRequestCount)
    throw new Error('Owned-run child exchanges are not a one-to-one match for the retained durable transcript.');
  dispatcher.assertControlPlanComplete();
  const result: OwnedRunTableParentReceipt = { schema_version: 1, kind: 'owned_run_table_parent_receipt',
    artifact_identity: artifactIdentity, exchange_count: child.exchangeCount, bridge_receipt: child.receipt };
  validateOwnedRunParentReceipt(result, artifactIdentity, plan, authority, deadline, dispatcher.recordedRequestCount);
  dispatcher.recordTerminalEvidence(result);
  return result;
}