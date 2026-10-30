#!/usr/bin/env -S npx --no-install tsx
/** Reconcile the isolated ArrowCol Azure sandbox using Azure CLI credentials. */
import fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';
import { spawn, spawnSync } from 'node:child_process';
import { parseArgs } from 'node:util';
import { Failure, HttpFailure, ControlConflict, NotReady, DeploymentFailed, canonical,
  canonical_json, publisher_registry, require_released, uuid, type Metadata } from './authority.js';
import { PublisherControl, READY, FENCED, CLEANING, UnknownTransition } from './publisher_control.js';
export * from './authority.js';

export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
export const API = '2024-04-13';
export const LEASE_SECONDS = 15;
export const TAGS = { project: 'rust-playground', purpose: 'disposable-arrowcol-sandbox' };
export const DEFAULT_STOP_COMMAND = ['npx', '--no-install', 'tsx', 'projects/loadtest/src/main.ts', '--stop'];
export const DELETE_PARALLELISM = 32;
export const READ_ATTEMPTS = 5;
export const TRANSIENT_HTTP = new Set([408, 429, 500, 502, 503, 504]);
export const runtime = {
  sleep: (seconds: number) => new Promise<void>(resolve => setTimeout(resolve, seconds * 1000)),
  now: () => performance.now() / 1000,
  run_tree,
};
export function load_env(filename: string): Record<string, string> {
  if (!fs.existsSync(filename) || !fs.statSync(filename).isFile())
    throw new Failure('Private env file is missing; deliver it using repo-bootstrapper sync.');
  const values: Record<string, string> = {};
  fs.readFileSync(filename, 'utf8').split(/\r?\n/).forEach((raw, index) => {
    let line = raw.trim();
    if (!line || line.startsWith('#')) return;
    if (line.startsWith('export ')) line = line.slice(7);
    const equal = line.indexOf('=');
    if (equal < 0) throw new Failure(`Invalid env assignment on line ${index + 1}.`);
    const key = line.slice(0, equal);
    let value = line.slice(equal + 1);
    if (!/^[A-Z][A-Z0-9_]*$/.test(key)) throw new Failure(`Invalid env key on line ${index + 1}.`);
    if (value.startsWith("'") || value.startsWith('"')) {
      if (value.length < 2 || value.at(-1) !== value[0])
        throw new Failure(`Unclosed env quote on line ${index + 1}.`);
      value = value.slice(1, -1);
    }
    values[key] = value;
  });
  return values;
}
export function cluster_tags(raw: string): Record<string, string> {
  let tags: Record<string, string>;
  try { tags = JSON.parse(raw); } catch { throw new Failure('AZURE_KUSTO_CLUSTER_TAGS must be a JSON object of string tags.'); }
  const reserved = [...Object.keys(TAGS), 'sandboxId'].map(key => key.toLowerCase());
  if (!tags || typeof tags !== 'object' || Array.isArray(tags) || Object.keys(tags).length > 15 ||
      Object.entries(tags).some(([k, v]) => typeof v !== 'string' || !k.length || k.length > 512 ||
        v.length > 256 || reserved.includes(k.toLowerCase())))
    throw new Failure('AZURE_KUSTO_CLUSTER_TAGS must be at most 15 string tags that do not override sandbox ownership tags.');
  return tags;
}
function normalized_uuid(value: string): string {
  const raw = value.replace(/^urn:uuid:/i, '').replace(/^\{|\}$/g, '').replaceAll('-', '').toLowerCase();
  if (!/^[0-9a-f]{32}$/.test(raw)) throw new Failure('Invalid UUID.');
  return `${raw.slice(0, 8)}-${raw.slice(8, 12)}-${raw.slice(12, 16)}-${raw.slice(16, 20)}-${raw.slice(20)}`;
}
export class Config {
  subscription: string; group: string; sandbox: string; location: string; account: string;
  cluster: string; identity: string; database: string; data: string; checkpoints: string;
  sku: string; minimum: number; maximum: number; hot: number; cluster_tags: Record<string, string>;
  operator: string; operator_type: string; operator_application: string; base: string;
  storage_id: string; cluster_id: string; identity_id: string; uri: string; table_root: string; table_prefix: string;
  constructor(values: Record<string, string | undefined>) {
    const get = (key: string, fallback = '') => values[key] || fallback;
    try { this.subscription = normalized_uuid(get('AZURE_SUBSCRIPTION_ID')); }
    catch { throw new Failure('AZURE_SUBSCRIPTION_ID must be a private subscription UUID.'); }
    this.group = get('AZURE_RESOURCE_GROUP', 'rust-playground');
    if (this.group !== 'rust-playground') throw new Failure('Only the exact rust-playground resource group is authorized.');
    this.sandbox = createHash('sha256').update(this.subscription + ':' + this.group).digest('hex').slice(0, 12);
    this.location = get('AZURE_LOCATION', 'eastus2');
    this.account = get('AZURE_STORAGE_ACCOUNT', 'arrowcol' + this.sandbox);
    this.cluster = get('AZURE_KUSTO_CLUSTER', 'arrowcol-' + this.sandbox);
    this.identity = get('AZURE_KUSTO_IDENTITY_NAME', 'arrowcol-kusto-reader');
    this.database = get('AZURE_KUSTO_DATABASE', 'arrowcol');
    this.data = get('AZURE_STORAGE_CONTAINER', 'arrowcol');
    this.checkpoints = get('AZURE_STORAGE_CHECKPOINT_CONTAINER', 'checkpoints');
    for (const value of [this.location, this.identity, this.cluster, this.database, this.data, this.checkpoints])
      if (!/^[a-z][a-z0-9-]{1,62}$/.test(value)) throw new Failure('Resource names must be lowercase alphanumeric sandbox names.');
    if (!/^[a-z0-9]{3,24}$/.test(this.account)) throw new Failure('Invalid storage account name.');
    if (this.data === this.checkpoints) throw new Failure('Data and checkpoint containers must be distinct.');
    this.sku = get('AZURE_KUSTO_SKU', 'Standard_E4ads_v5');
    const integer = (key: string, fallback: string) => {
      const raw = get(key, fallback);
      if (!/^[+-]?\d+$/.test(raw.trim())) throw new Failure('Invalid integer configuration.');
      return Number(raw);
    };
    this.minimum = integer('AZURE_KUSTO_MIN_INSTANCES', '2');
    this.maximum = integer('AZURE_KUSTO_MAX_INSTANCES', '4');
    this.hot = integer('AZURE_KUSTO_QAP_HOT_DAYS', '7');
    if (!(2 <= this.minimum && this.minimum <= this.maximum && this.maximum <= 8 && 1 <= this.hot && this.hot <= 365))
      throw new Failure('Require 2 <= cluster minimum <= maximum <= 8 and QAP days 1..365.');
    this.cluster_tags = cluster_tags(get('AZURE_KUSTO_CLUSTER_TAGS', '{}'));
    this.operator = get('AZURE_OPERATOR_OBJECT_ID');
    this.operator_type = get('AZURE_OPERATOR_TYPE', 'User');
    this.operator_application = get('AZURE_OPERATOR_APPLICATION_ID');
    if (!['User', 'App'].includes(this.operator_type)) throw new Failure('AZURE_OPERATOR_TYPE must be User or App.');
    if (this.operator_type === 'App') {
      try { this.operator_application = normalized_uuid(this.operator_application); }
      catch { throw new Failure('App operators require an explicit AZURE_OPERATOR_APPLICATION_ID.'); }
    }
    this.base = `/subscriptions/${this.subscription}/resourceGroups/${this.group}`;
    this.storage_id = this.base + '/providers/Microsoft.Storage/storageAccounts/' + this.account;
    this.cluster_id = this.base + '/providers/Microsoft.Kusto/clusters/' + this.cluster;
    this.identity_id = this.base + '/providers/Microsoft.ManagedIdentity/userAssignedIdentities/' + this.identity;
    this.uri = `https://${this.cluster}.${this.location}.kusto.windows.net`;
    const root = get('ARROWCOL_AZURE_TABLE_ROOT', `abfss://${this.data}@${this.account}.dfs.core.windows.net/`);
    // Validate raw authority/path rather than URL's normalized path (which erases traversal).
    const match = /^abfss:\/\/([^/?#]+)([^?#]*)(\?[^#]*)?(#.*)?$/.exec(root);
    if (!match || match[1] !== `${this.data}@${this.account}.dfs.core.windows.net` || match[3] || match[4])
      throw new Failure('Azure table root must stay inside the exact owned account and data container.');
    this.table_prefix = match[2].replace(/^\/+|\/+$/g, '');
    if (this.table_prefix && !/^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/.test(this.table_prefix))
      throw new Failure('Azure table root has an unsafe relative prefix.');
    this.table_root = `abfss://${match[1]}/` + (this.table_prefix ? this.table_prefix + '/' : '');
  }
  redact(text: string): string {
    for (const value of [this.subscription, this.sandbox, this.account, this.cluster, this.identity,
      this.uri, this.database, this.data, this.checkpoints]) text = text.replaceAll(value, '[private]');
    return text.replace(/https?:\/\/[^\s"'<>]+/g, '[endpoint]')
      .replace(/\b[\w.+-]+@[\w.-]+\.[A-Za-z]+\b/g, '[principal]')
      .replace(/\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b/gi, '[identifier]')
      .replace(/bearer\s+\S+/gi, '******').slice(0, 2000);
  }
  generated(): Record<string, string> {
    return { AZURE_LOCATION: this.location, AZURE_STORAGE_ACCOUNT: this.account, AZURE_STORAGE_CONTAINER: this.data,
      AZURE_STORAGE_CHECKPOINT_CONTAINER: this.checkpoints, AZURE_KUSTO_CLUSTER: this.cluster,
      AZURE_KUSTO_DATABASE: this.database, AZURE_KUSTO_IDENTITY_NAME: this.identity, AZURE_KUSTO_URI: this.uri,
      ARROWCOL_AZURE_TABLE_ROOT: this.table_root,
      ARROWCOL_AZURE_CHECKPOINT_ROOT: `abfss://${this.checkpoints}@${this.account}.dfs.core.windows.net/` };
  }
}
function private_file(filename: string, bytes: string | Buffer): string {
  fs.mkdirSync(path.dirname(filename), { recursive: true });
  const temporary = path.join(path.dirname(filename), '.env.azure.' + uuid());
  const fd = fs.openSync(temporary, 'wx', 0o600);
  try { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); }
  catch (error) { fs.unlinkSync(temporary); throw error; }
  finally { fs.closeSync(fd); }
  return temporary;
}
export function write_private(filename: string, values: Record<string, string>): void {
  const temporary = private_file(filename, Object.entries(values).map(([k, v]) => `${k}=${v}\n`).join(''));
  try { fs.renameSync(temporary, filename); } finally { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); }
}
export interface ProcessResult { returncode: number; stdout: string; stderr: string }
export async function run_tree(command: string[], timeout: number, cwd?: string,
  env?: NodeJS.ProcessEnv): Promise<ProcessResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command[0], command.slice(1), { detached: true, cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '', failure: Failure | undefined;
    const kill = (message: string) => {
      failure = new Failure(message);
      if (child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ESRCH') failure = new Failure('Process group could not be terminated; outcome unknown.');
      } }
    };
    const interrupt = () => kill('Command interrupted; outcome unknown and process group killed. Authority stays blocking; no automatic clear.');
    const timer = setTimeout(() => kill('Command timed out; outcome unknown and process group killed. Authority stays blocking; no automatic clear.'), timeout * 1000);
    process.on('SIGINT', interrupt); process.on('SIGTERM', interrupt);
    child.stdout.on('data', data => { stdout += data.toString(); });
    child.stderr.on('data', data => { stderr += data.toString(); });
    const cleanup = () => { clearTimeout(timer); process.off('SIGINT', interrupt); process.off('SIGTERM', interrupt); };
    child.once('error', error => { cleanup(); reject(new Failure('Command could not start.', { cause: error })); });
    child.once('close', code => { cleanup(); if (failure) reject(failure); else resolve({ returncode: code ?? -1, stdout, stderr }); });
  });
}
type Headers = Record<string, string>;
export type BlobEntry = { name: string; snapshot?: string | null; version?: string | null; etag?: string | null;
  deleted?: boolean; directory?: boolean };
export class Azure {
  tokens = new Map<string, { at: number; token: string }>();
  token_pending = new Map<string, Promise<string>>();
  snapshots = new Map<string, Metadata>();
  constructor(public c: Config) {}
  async cli(...args: string[]): Promise<any> {
    const result = await runtime.run_tree(['az', ...args, '--only-show-errors', '--output', 'json'], 3600);
    if (result.returncode) throw new Failure(this.c.redact(result.stderr));
    return result.stdout.trim() ? JSON.parse(result.stdout) : null;
  }
  async token(resource: string): Promise<string> {
    const cached = this.tokens.get(resource);
    if (cached && runtime.now() - cached.at <= 240) return cached.token;
    const pending = this.token_pending.get(resource);
    if (pending) return pending;
    const refresh = (async () => {
      const token = (await this.cli('account', 'get-access-token', '--subscription', this.c.subscription, '--resource', resource)).accessToken;
      this.tokens.set(resource, { at: runtime.now(), token });
      return token;
    })();
    this.token_pending.set(resource, refresh);
    try { return await refresh; } finally { this.token_pending.delete(resource); }
  }
  async request(method: string, url: string, resource: string, body?: string | Buffer | null,
    headers: Headers = {}, missing_ok = false): Promise<[Buffer | null, Headers]> {
    const attempts = ['GET', 'HEAD'].includes(method) ? READ_ATTEMPTS : 1;
    for (let attempt = 0; attempt < attempts; attempt++) {
      try { return await this._request_once(method, url, resource, body, headers, missing_ok); }
      catch (error) {
        if (!(error instanceof Failure) || attempt + 1 === attempts ||
            (error instanceof HttpFailure && !TRANSIENT_HTTP.has(error.code))) throw error;
      }
      await runtime.sleep(Math.min(2 ** attempt, 15));
    }
    throw new Error('unreachable');
  }
  async _request_once(method: string, url: string, resource: string, body?: string | Buffer | null,
    headers: Headers = {}, missing_ok = false): Promise<[Buffer | null, Headers]> {
    const token = await this.token(resource);
    let response: Response;
    try {
      response = await fetch(url, { method, body: body == null ? undefined : body as BodyInit,
        signal: AbortSignal.timeout(30_000), redirect: 'error',
        headers: { Authorization: 'Bearer ' + token, ...(resource === 'https://storage.azure.com/' ?
          { 'x-ms-date': new Date().toUTCString() } : {}), ...headers } });
      const bytes = Buffer.from(await response.arrayBuffer());
      if (missing_ok && response.status === 404) return [null, {}];
      if (!response.ok) throw new HttpFailure(response.status, `HTTP ${response.status}: ${this.c.redact(bytes.toString())}`);
      return [bytes, { ...Object.fromEntries(response.headers.entries()), ':status': String(response.status) }];
    } catch (error) {
      if (error instanceof HttpFailure) throw error;
      throw new Failure(this.c.redact(String(error)), { cause: error });
    }
  }
  async arm(method: string, resourcePath: string, body?: any, missing_ok = false, api = API): Promise<any> {
    const [data] = await this.request(method, 'https://management.azure.com' + resourcePath +
      (resourcePath.includes('?') ? '&' : '?') + 'api-version=' + api, 'https://management.azure.com/',
      body == null ? undefined : JSON.stringify(body), { 'Content-Type': 'application/json' }, missing_ok);
    return data?.length ? JSON.parse(data.toString()) : null;
  }
  async delete_database(): Promise<[number, string | undefined]> {
    const [, headers] = await this.request('DELETE', 'https://management.azure.com' + this.c.cluster_id +
      '/databases/' + this.c.database + '?api-version=' + API, 'https://management.azure.com/', undefined, {}, true);
    return [Object.keys(headers).length ? Number(headers[':status']) : 404, headers['azure-asyncoperation']];
  }
  async provider_operation(url: string): Promise<string> {
    operation_url(url);
    const [data, headers] = await this.request('GET', url, 'https://management.azure.com/');
    if (headers[':status'] !== '200') return 'Unknown';
    try {
      const status = JSON.parse(data!.toString())?.status;
      return typeof status === 'string' ? PROVIDER_STATUSES[status.toLowerCase()] ?? 'Unknown' : 'Unknown';
    } catch { return 'Unknown'; }
  }
  async blob(method: string, container: string, name = '', query?: Record<string, string>,
    body?: string | Buffer | null, headers: Headers = {}, missing_ok = false): Promise<[Buffer | null, Headers]> {
    let url = `https://${this.c.account}.blob.core.windows.net/${container}`;
    if (name) url += '/' + name.split('/').map(encodeURIComponent).join('/');
    if (query && Object.keys(query).length) url += '?' + new URLSearchParams(query);
    return this.request(method, url, 'https://storage.azure.com/', body, { 'x-ms-version': '2023-11-03', ...headers }, missing_ok);
  }
  async blobs(container: string): Promise<BlobEntry[]> {
    const entries: BlobEntry[] = [];
    let marker = '';
    do {
      const [data] = await this.blob('GET', container, '', { restype: 'container', comp: 'list',
        include: 'snapshots,versions,deleted,metadata', marker });
      const root = parse_xml(data!.toString());
      if (root.name !== 'EnumerationResults') throw new Failure('Storage inventory XML is malformed.');
      const text = (node: XmlNode, name: string) => node.children.find(child => child.name === name)?.text ?? null;
      const blobs = root.children.find(child => child.name === 'Blobs');
      for (const entry of blobs?.children.filter(child => child.name === 'Blob') ?? []) {
        const properties = entry.children.find(child => child.name === 'Properties');
        entries.push({ name: text(entry, 'Name') ?? '', snapshot: text(entry, 'Snapshot'),
          version: text(entry, 'VersionId'), etag: properties ? text(properties, 'Etag') : null,
          deleted: text(entry, 'Deleted') === 'true' });
      }
      marker = text(root, 'NextMarker') ?? '';
    } while (marker);
    return entries;
  }
  async paths(container: string): Promise<any[]> {
    const paths: any[] = [];
    let continuation = '';
    do {
      const query = new URLSearchParams({ resource: 'filesystem', recursive: 'true', maxResults: '5000', continuation });
      const [data, headers] = await this.request('GET', `https://${this.c.account}.dfs.core.windows.net/${container}?${query}`,
        'https://storage.azure.com/', undefined, { 'x-ms-version': '2023-11-03' });
      paths.push(...(JSON.parse(data!.toString()).paths ?? []));
      continuation = headers['x-ms-continuation'] ?? '';
    } while (continuation);
    return paths;
  }
  async delete_directory(container: string, name: string, etag: string): Promise<void> {
    const encoded = name.split('/').map(encodeURIComponent).join('/');
    await this.request('DELETE', `https://${this.c.account}.dfs.core.windows.net/${container}/${encoded}?recursive=false`,
      'https://storage.azure.com/', undefined, { 'x-ms-version': '2023-11-03', 'If-Match': etag }, true);
  }
  async kql(command: string, database?: string): Promise<any> {
    const [data] = await this.request('POST', this.c.uri + '/v1/rest/mgmt', 'https://kusto.kusto.windows.net',
      JSON.stringify({ db: database || this.c.database, csl: command }), { 'Content-Type': 'application/json' });
    const value = JSON.parse(data!.toString());
    if (value.error || value.HasErrors || (Array.isArray(value.Exceptions) ? value.Exceptions.length : value.Exceptions))
      throw new Failure(this.c.redact(JSON.stringify(value)));
    return value;
  }
  async head_control(): Promise<[Metadata, string]> {
    const [, headers] = await this.blob('HEAD', this.c.checkpoints, '', { restype: 'container' });
    const metadata = Object.fromEntries(Object.entries(headers).filter(([key]) => key.startsWith('x-ms-meta-'))
      .map(([key, value]) => [key.slice(10).toLowerCase(), value]));
    if (!headers.etag) throw new Failure('Reset authority ETag is unavailable.');
    this.snapshots.set(headers.etag, { ...metadata });
    return [metadata, headers.etag];
  }
  async control(): Promise<[Metadata, string]> {
    const [metadata, , etag] = await new PublisherControl(this).read();
    return [metadata, etag];
  }
  async lease(action: string, lease_id: string, proposed = false): Promise<void> {
    await this.blob('PUT', this.c.checkpoints, '', { comp: 'lease', restype: 'container' }, Buffer.alloc(0),
      { 'x-ms-lease-action': action, ...(proposed ? { 'x-ms-lease-duration': String(LEASE_SECONDS),
        'x-ms-proposed-lease-id': lease_id } : { 'x-ms-lease-id': lease_id }) });
  }
  async set_control(metadata: Metadata, etag: string): Promise<void> {
    const expected = this.snapshots.get(etag);
    if (!expected) throw new ControlConflict('Authority write has no matching prior read; nothing written.');
    const lease = uuid();
    try { await this.lease('acquire', lease, true); }
    catch (error) { throw new ControlConflict('Authority lease unavailable; nothing written.', { cause: error }); }
    try {
      const [current] = await this.head_control();
      if (canonical_json(current) !== canonical_json(expected))
        throw new ControlConflict('Authority changed since it was read; nothing written.');
      try {
        await this.blob('PUT', this.c.checkpoints, '', { restype: 'container', comp: 'metadata' }, Buffer.alloc(0),
          { 'x-ms-lease-id': lease, ...Object.fromEntries(Object.entries(metadata).map(([k, v]) => ['x-ms-meta-' + k, v])) });
      } catch (error) {
        if (error instanceof HttpFailure && error.code === 412)
          throw new ControlConflict('Authority lease lost before write; nothing written.', { cause: error });
        throw error;
      }
    } finally { try { await this.lease('release', lease); } catch { /* Expiry bounds an unreleased lease. */ } }
  }
  async collection(resourcePath: string, api = API): Promise<any[]> {
    let page = await this.arm('GET', resourcePath, undefined, false, api);
    const items = [...page.value];
    while (page.nextLink) {
      if (!page.nextLink.startsWith('https://management.azure.com' + resourcePath + '?'))
        throw new Failure('Unexpected ARM pagination endpoint; refusing unsafe inventory.');
      const [data] = await this.request('GET', page.nextLink, 'https://management.azure.com/');
      page = JSON.parse(data!.toString()); items.push(...page.value);
    }
    return items;
  }
}
interface XmlNode { name: string; text: string; children: XmlNode[] }
function xml_decode(value: string): string {
  return value.replace(/&([^;]*);|&/g, (entity, code: string | undefined) => {
    const named: Record<string, string> = { lt: '<', gt: '>', quot: '"', apos: "'", amp: '&' };
    if (code && Object.hasOwn(named, code)) return named[code];
    if (code && /^(#\d+|#x[0-9a-fA-F]+)$/.test(code)) {
      const point = parseInt(code.slice(code[1] === 'x' ? 2 : 1), code[1] === 'x' ? 16 : 10);
      if (point === 9 || point === 10 || point === 13 || (point >= 32 && point <= 0x10ffff && !(point >= 0xd800 && point <= 0xdfff)))
        return String.fromCodePoint(point);
    }
    throw new Failure('Storage inventory XML contains an invalid entity: ' + entity.slice(0, 20));
  });
}
/** Strict, non-DTD XML parser for Azure's flat inventory schema; malformed input never means empty. */
export function parse_xml(xml: string): XmlNode {
  const document: XmlNode = { name: '', text: '', children: [] }, stack = [document];
  let at = 0;
  const malformed = () => { throw new Failure('Storage inventory XML is malformed.'); };
  while (at < xml.length) {
    if (xml[at] !== '<') {
      const next = xml.indexOf('<', at), end = next < 0 ? xml.length : next;
      const value = xml.slice(at, end);
      if ((stack.length === 1 && value.trim()) || value.includes(']]>')) malformed();
      stack.at(-1)!.text += xml_decode(value); at = end; continue;
    }
    if (xml.startsWith('<!--', at)) {
      const end = xml.indexOf('-->', at + 4);
      if (end < 0 || xml.slice(at + 4, end).includes('--')) malformed();
      at = end + 3; continue;
    }
    if (xml.startsWith('<![CDATA[', at)) {
      const end = xml.indexOf(']]>', at + 9);
      if (end < 0 || stack.length === 1) malformed();
      stack.at(-1)!.text += xml.slice(at + 9, end); at = end + 3; continue;
    }
    if (xml.startsWith('<?', at)) {
      const end = xml.indexOf('?>', at + 2);
      if (end < 0) malformed();
      at = end + 2; continue;
    }
    const token = /^<\/?([A-Za-z_][\w:.-]*)(\s+(?:[^>"']|"[^"]*"|'[^']*')*)?\s*\/?>/.exec(xml.slice(at));
    if (!token) malformed();
    const raw = token![0], name = token![1];
    if (raw.startsWith('</')) {
      if (!/^<\/[A-Za-z_][\w:.-]*\s*>$/.test(raw) || stack.length === 1 || stack.at(-1)!.name !== name) malformed();
      stack.pop();
    } else {
      const attributes = raw.slice(name.length + 1).replace(/\/?>$/, '');
      let rest = attributes, match: RegExpExecArray | null;
      const keys = new Set<string>();
      while (rest.trim()) {
        match = /^\s+([A-Za-z_][\w:.-]*)\s*=\s*("([^"<]*)"|'([^'<]*)')/.exec(rest);
        if (!match || keys.has(match[1])) malformed();
        keys.add(match![1]); xml_decode(match![3] ?? match![4]); rest = rest.slice(match![0].length);
      }
      const node = { name, text: '', children: [] };
      stack.at(-1)!.children.push(node);
      if (!raw.endsWith('/>')) stack.push(node);
    }
    at += raw.length;
  }
  if (stack.length !== 1 || document.children.length !== 1) malformed();
  return document.children[0];
}
export function owned(resource: any, config: Config): void {
  if (!resource || Object.entries({ ...TAGS, sandboxId: config.sandbox }).some(([k, v]) => resource.tags?.[k] !== v))
    throw new Failure('Exact sandbox ownership tags do not match; refusing adoption/mutation.');
}
export async function retry_ready<T>(action: () => Promise<T>, label: string, timeout = 600): Promise<T> {
  const deadline = runtime.now() + timeout;
  for (;;) {
    try { return await action(); } catch (error) {
      if (!(error instanceof Failure)) throw error;
      console.error(`${label} not ready; retrying: ${String(error.message).slice(0, 400)}`);
      if (runtime.now() >= deadline) throw new Failure(`${label} readiness timed out: ${error.message}`, { cause: error });
    }
    await runtime.sleep(10);
  }
}
export async function preflight(azure: Azure): Promise<void> {
  const c = azure.c;
  const account = await azure.cli('account', 'show', '--subscription', c.subscription);
  if (account.id.toLowerCase() !== c.subscription) throw new Failure('CLI subscription mismatch; no default-subscription fallback is permitted.');
  for (const provider of ['Microsoft.Kusto', 'Microsoft.Storage', 'Microsoft.ManagedIdentity']) {
    const state = (await azure.cli('provider', 'show', '--namespace', provider, '--subscription', c.subscription)).registrationState;
    if (state !== 'Registered') await azure.cli('provider', 'register', '--namespace', provider, '--subscription', c.subscription, '--wait');
  }
  if (!c.operator) {
    const token = await azure.token('https://management.azure.com/');
    c.operator = normalized_uuid(JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString()).oid);
  } else c.operator = normalized_uuid(c.operator);
  console.log('Azure subscription and signed-in provisioning identity: ready');
}
export async function guard_existing(azure: Azure): Promise<void> {
  const c = azure.c;
  for (const resourcePath of [c.base, c.storage_id, c.cluster_id, c.identity_id]) {
    const resource = await azure.arm('GET', resourcePath, undefined, true,
      resourcePath === c.base ? '2022-09-01' : resourcePath === c.storage_id ? '2023-05-01' :
        resourcePath === c.identity_id ? '2023-01-31' : API);
    if (resource) owned(resource, c);
  }
}
export function fresh_authority(): Metadata {
  return { resetstate: FENCED, resetgeneration: uuid(), occupancy: 'released', publisherregistryversion: '1', publishers: '{}' };
}
export async function checkpoint_container(azure: Azure): Promise<any> {
  return azure.arm('GET', azure.c.storage_id + '/blobServices/default/containers/' + azure.c.checkpoints, undefined, true, '2023-05-01');
}
export async function deploy(azure: Azure, fresh?: Metadata): Promise<void> {
  const c = azure.c;
  await guard_existing(azure);
  const params = { location: c.location, resourceGroupName: c.group, sandboxId: c.sandbox, storageAccountName: c.account,
    clusterName: c.cluster, identityName: c.identity, databaseName: c.database, dataContainer: c.data, checkpointContainer: c.checkpoints,
    operatorObjectId: c.operator, operatorKustoPrincipalId: c.operator_type === 'App' ? c.operator_application : c.operator,
    controlMetadata: fresh ?? {}, checkpointExists: fresh === undefined, operatorType: c.operator_type,
    clusterSku: c.sku, minimumInstances: c.minimum, maximumInstances: c.maximum, clusterTags: c.cluster_tags };
  const filename = private_file(path.join(ROOT, '.env.azure.parameters.json'),
    JSON.stringify({ parameters: Object.fromEntries(Object.entries(params).map(([k, v]) => [k, { value: v }])) }));
  try {
    try {
      await azure.cli('deployment', 'sub', 'create', '--subscription', c.subscription, '--location', c.location,
        '--name', 'arrowcol-sandbox', '--template-file', path.join(ROOT, 'infra/azure/main.bicep'), '--parameters', '@' + filename);
    } catch (error) {
      if (error instanceof Failure && error.message.includes('DeploymentFailed')) throw new DeploymentFailed(error.message, { cause: error });
      throw error;
    }
  } finally { fs.unlinkSync(filename); }
  c.uri = (await azure.arm('GET', c.cluster_id)).properties.uri.replace(/\/+$/, '');
  await retry_ready(() => azure.blobs(c.data), 'Storage RBAC');
  console.log('Declarative resources and storage access: ready');
}
export async function ensure_running(azure: Azure): Promise<void> {
  const c = azure.c, cluster = await azure.arm('GET', c.cluster_id);
  owned(cluster, c); c.uri = cluster.properties.uri.replace(/\/+$/, '');
  if (cluster.properties.state === 'Stopped')
    await azure.cli('rest', '--method', 'post', '--url', 'https://management.azure.com' + c.cluster_id + '/start?api-version=' + API);
  await retry_ready(() => azure.kql('.show cluster'), 'Kusto');
  console.log('Kusto cluster: running');
}
export async function held<T>(control: PublisherControl, publisher: string, generation: string, work: () => Promise<T>): Promise<T> {
  try { return await work(); } catch (error) {
    if (error instanceof NotReady || error instanceof DeploymentFailed) await control.clean_release(publisher, generation, true);
    else { try { await control.uncertain(publisher, generation); } catch { /* Active still blocks. */ } }
    throw error;
  }
}
export async function exclusive(control: PublisherControl, generation: string, steps: (() => Promise<unknown>)[]): Promise<void> {
  const operator = uuid();
  await control.claim_exclusive(operator, generation);
  try {
    for (const step of steps) { await control.bootstrap_owner(operator, generation); await step(); }
  } catch (error) {
    if (error instanceof NotReady || error instanceof DeploymentFailed) await control.complete_bootstrap(operator, generation, true);
    else { try { await control.uncertain(operator, generation); } catch { /* Active still blocks. */ } }
    throw error;
  }
  await control.complete_bootstrap(operator, generation, true);
}
export function initializer_env(values: Record<string, string>, c: Config, tables: string, generation: string): NodeJS.ProcessEnv {
  return { ...process.env, ...values, ...c.generated(), ARROWCOL_AZURE_TABLE_MANIFEST: path.resolve(tables),
    ARROWCOL_EXPECTED_RESET_GENERATION: generation };
}
export const initializerEnv = initializer_env;
export async function provision(azure: Azure, tables: string, wait_seconds: number, output: string,
  infra_only: boolean, values: Record<string, string>): Promise<void> {
  const c = azure.c, control = new PublisherControl(azure);
  await guard_existing(azure);
  let generation: string;
  if (await checkpoint_container(azure) === null) {
    const fresh = fresh_authority();
    await deploy(azure, fresh);
    const [metadata, registry] = await control.read();
    if (metadata.resetgeneration !== fresh.resetgeneration || metadata.resetstate !== FENCED ||
        Object.keys(registry).length || metadata.occupancy !== 'released')
      throw new Failure('New checkpoint authority does not match its initialized generation; concurrent first provisioning is unsupported.');
    generation = fresh.resetgeneration;
  } else {
    const [metadata, registry] = await control.read();
    require_released(metadata); generation = metadata.resetgeneration;
    const member = uuid();
    if (metadata.resetstate === READY) {
      const steps: (() => Promise<unknown>)[] = [() => deploy(azure), () => ensure_running(azure)];
      if (!infra_only) steps.push(() => reconcile(azure, tables, wait_seconds));
      await exclusive(control, generation, steps);
      write_private(output, { ...c.generated(), ARROWCOL_RESET_GENERATION: generation });
      console.log('Ready sandbox reconciled under exclusive operator custody; authority preserved.');
      return;
    }
    if (metadata.resetstate !== FENCED || Object.keys(registry).length)
      throw new Failure('Authority is cleanup-owned or has registered publishers; provisioning refused.');
    await control.admit(member, generation, true);
    await held(control, member, generation, () => deploy(azure));
    await control.clean_release(member, generation, true);
  }
  write_private(output, c.generated());
  if (infra_only) { console.log('INFRA-ONLY: Delta initialization and external-table/QAP reconciliation NOT verified.'); return; }
  const initializer = path.join(ROOT, 'infra/azure/initialize-delta.sh');
  if (!fs.existsSync(initializer)) throw new Failure('Delta initializer is unavailable; infra exists, full bootstrap is NOT complete.');
  const result = await runtime.run_tree(['bash', initializer], 1800, ROOT, initializer_env(values, c, tables, generation));
  if (result.returncode) throw new Failure('Delta initialization failed; KQL reconciliation was not attempted. ' + c.redact(result.stderr));
  const parent = uuid();
  await control.admit(parent, generation, true);
  await held(control, parent, generation, async () => {
    await control.bootstrap_owner(parent, generation); await ensure_running(azure);
    await control.bootstrap_owner(parent, generation); await reconcile(azure, tables, wait_seconds);
    await control.bootstrap_owner(parent, generation);
  });
  await control.complete_bootstrap(parent, generation, true);
  const [metadata] = await control.read();
  if (metadata.resetstate !== READY || metadata.resetgeneration !== generation)
    throw new Failure('Authority changed after bootstrap completion; not exporting readiness.');
  write_private(output, { ...c.generated(), ARROWCOL_RESET_GENERATION: generation });
  console.log('Full bootstrap ready-v1 at the captured generation.');
}
export async function external_locations(azure: Azure): Promise<Record<string, string[]>> {
  const result = await azure.kql('.show external tables | project TableName, ConnectionStrings');
  return Object.fromEntries((result?.Tables?.[0]?.Rows ?? []).map(([name, connections]: [string, unknown]) =>
    [name, typeof connections === 'string' ? JSON.parse(connections) : connections ?? []]));
}
export async function reconcile(azure: Azure, manifest: string, wait_seconds = 0): Promise<void> {
  const c = azure.c;
  let tables: any = JSON.parse(fs.readFileSync(manifest, 'utf8'));
  if (!Array.isArray(tables) || !tables.length) throw new Failure('Initialization manifest must be a nonempty array of table bindings.');
  tables = tables.map((table: any) => typeof table === 'string' ? { name: table, relative_path: table } : table);
  if (tables.some((t: any) => !t || typeof t !== 'object' || Object.keys(t).sort().join(',') !== 'name,relative_path'))
    throw new Failure('Each table binding must contain exactly name and relative_path.');
  if (tables.some((t: any) => typeof t.name !== 'string' || typeof t.relative_path !== 'string'))
    throw new Failure('Table names and relative paths must be strings.');
  if (new Set(tables.map((t: any) => t.name)).size !== tables.length) throw new Failure('Initialization manifest has duplicate tables.');
  for (const table of tables) {
    if (!/^[A-Za-z][A-Za-z0-9_]{0,127}$/.test(table.name)) throw new Failure('Initialization manifest contains an invalid table name.');
    if (!/^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/.test(table.relative_path)) throw new Failure('Initialization manifest contains an unsafe relative path.');
    table.storage_path = [c.table_prefix, table.relative_path].filter(Boolean).join('/');
  }
  const deadline = runtime.now() + wait_seconds;
  for (;;) {
    let missing = 0;
    for (const table of tables) {
      const [data] = await azure.blob('GET', c.data, table.storage_path + '/_delta_log/00000000000000000000.json', undefined, undefined, {}, true);
      if (data === null) { missing++; continue; }
      let actions: any[];
      try { actions = data.toString().split(/\r?\n/).filter(line => line.trim()).map(line => JSON.parse(line)); }
      catch { throw new Failure('Delta initialization log is malformed.'); }
      if (!actions.some(action => action && 'metaData' in action) || !actions.some(action => action && 'protocol' in action))
        throw new Failure('Delta initialization did not publish metadata and protocol.');
    }
    if (!missing) break;
    if (runtime.now() >= deadline) throw new NotReady(`Declared Delta logs are absent: ${missing}; no KQL was attempted.`);
    await runtime.sleep(Math.min(1, Math.max(0, deadline - runtime.now())));
  }
  const identity = (await azure.arm('GET', c.identity_id, undefined, false, '2023-01-31')).properties.principalId;
  const policy = JSON.stringify([{ ObjectId: identity, AllowedUsages: 'ExternalTable,AutomatedFlows' }]);
  await azure.kql(`.alter-merge database ['${c.database}'] policy managed_identity '${policy}'`);
  const qap = JSON.stringify({ IsEnabled: true, Hot: `${c.hot}.00:00:00`, ManagedIdentity: identity });
  const existing = await external_locations(azure);
  for (const table of tables) {
    const connection = `abfss://${c.data}@${c.account}.dfs.core.windows.net/${table.storage_path};managed_identity=${identity}`;
    if (table.name in existing && JSON.stringify(existing[table.name]) !== JSON.stringify([connection]))
      await azure.kql(`.delete external table ['${table.name}'] policy query_acceleration`);
    await retry_ready(() => azure.kql(`.create-or-alter external table ['${table.name}'] kind=delta (h@'${connection}')`), 'External Delta table');
    await azure.kql(`.alter external table ['${table.name}'] policy query_acceleration '${qap}'`);
  }
  console.log(`External Delta tables and QAP reconciled: ${tables.length}`);
}

export async function reset_inventory(azure: Azure): Promise<[any[], Record<string, BlobEntry[]>]> {
  const c = azure.c;
  await guard_existing(azure);
  owned(await azure.arm('GET', c.cluster_id), c);
  const storage = await azure.arm('GET', c.storage_id, undefined, false, '2023-05-01');
  owned(storage, c);
  if (!storage.properties.isHnsEnabled) throw new Failure('Reset requires the declared HNS storage account.');
  const service = (await azure.arm('GET', c.storage_id + '/blobServices/default', undefined, false, '2023-05-01')).properties;
  if (service.isVersioningEnabled || service.deleteRetentionPolicy?.enabled || service.containerDeleteRetentionPolicy?.enabled || service.restorePolicy?.enabled)
    throw new Failure('Retained copies are enabled; refusing a reset that cannot guarantee deletion.');
  const databases = await azure.collection(c.cluster_id + '/databases');
  if (databases.some(item => item.name.split('/').at(-1) !== c.database))
    throw new Failure('Unexpected Kusto database present; refusing shared-resource reset.');
  const containers = await azure.collection(c.storage_id + '/blobServices/default/containers', '2023-05-01');
  if (canonical_json(containers.map(item => item.name.split('/').at(-1)).sort()) !== canonical_json([c.data, c.checkpoints].sort()))
    throw new Failure('Unexpected or missing container; refusing shared-resource reset.');
  for (const name of [c.data, c.checkpoints]) {
    const container = await azure.arm('GET', c.storage_id + '/blobServices/default/containers/' + name, undefined, false, '2023-05-01');
    const metadata = Object.fromEntries(Object.entries(container.properties.metadata ?? {}).map(([k, v]) => [k.toLowerCase(), v]));
    if (metadata.sandboxid !== c.sandbox) throw new Failure('Container ownership metadata mismatch.');
  }
  const inventory: Record<string, BlobEntry[]> = {};
  for (const name of [c.data, c.checkpoints]) {
    inventory[name] = await azure.blobs(name);
    const byName = new Map(inventory[name].map(item => [item.name, item]));
    for (const item of await azure.paths(name)) {
      const directory = String(item.isDirectory ?? false).toLowerCase() === 'true';
      const entry = byName.get(item.name);
      if (entry) { entry.directory = directory; entry.etag ||= item.etag; }
      else inventory[name].push({ name: item.name, directory, etag: item.etag, deleted: false, version: null, snapshot: null });
    }
  }
  for (const entries of Object.values(inventory)) for (const entry of entries) {
    if (entry.deleted) throw new Failure('Soft-deleted objects remain; reset cannot truthfully claim physical emptiness.');
    if (typeof entry.name !== 'string' || !entry.name || entry.name.startsWith('/') || entry.name.includes('\\') ||
        entry.name.split('/').some(part => ['.', '..'].includes(part)))
      throw new Failure('Storage inventory contains a malformed or unscoped path.');
  }
  return [databases, inventory];
}
export async function reset_preview(azure: Azure): Promise<void> {
  const [databases, inventory] = await reset_inventory(azure);
  console.log(`Reset preview: owned databases=${databases.length}, containers=2, stored objects=${Object.values(inventory).reduce((n, items) => n + items.length, 0)}`);
  console.log('Preserve cluster, account, containers and managed identity.');
}
export const RESET_RECORD = path.join(ROOT, '.env.azure.reset.json');
export const RECEIPT_FIELDS = { stopped: true, scope: 'workspace-owned-loadtest', dataPreserved: true };
export interface ResetRecord { sandbox: string; operator: string; generation: string; next: string }
export async function require_running(azure: Azure): Promise<void> {
  const cluster = await azure.arm('GET', azure.c.cluster_id);
  owned(cluster, azure.c);
  if (cluster.properties.state !== 'Running')
    throw new Failure('Reset requires the owned cluster to be Running; refused without starting it (no reset-owned start/stop).');
}
export function read_reset_record(filename: string): ResetRecord | null {
  let record: ResetRecord;
  try { record = JSON.parse(fs.readFileSync(filename, 'utf8')); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw new Failure('Cleanup custody record is unreadable; refusing.', { cause: error });
  }
  if (!record || typeof record !== 'object' || Object.keys(record).sort().join(',') !== 'generation,next,operator,sandbox')
    throw new Failure('Cleanup custody record is malformed; refusing.');
  for (const key of ['operator', 'generation', 'next'] as const) canonical(record[key]);
  return record;
}
export const record_bytes = (record: unknown): Buffer => Buffer.from(canonical_json(record));
export function fsync_directory(directory: string): void {
  try {
    const fd = fs.openSync(directory, fs.constants.O_RDONLY | fs.constants.O_DIRECTORY);
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  } catch (error) {
    throw new Failure('Cleanup custody record directory could not be made durable; no claim or deletion attempted.', { cause: error });
  }
}
export async function reset_lock<T>(filename: string, work: () => Promise<T>): Promise<T> {
  fs.mkdirSync(path.dirname(filename), { recursive: true });
  const fd = fs.openSync(filename + '.lock', 'a+', 0o600);
  try {
    // flock locks the inherited open-file description, held by our descriptor until close.
    // Unlike a PID/lockfile sentinel this is released by the kernel on process death.
    const result = spawnSync('flock', ['--exclusive', '--nonblock', '3'], { stdio: ['ignore', 'pipe', 'pipe', fd] });
    if (result.error || result.status !== 0)
      throw new Failure('Another reset process holds the local custody lock; refusing.');
    return await work();
  } finally { fs.closeSync(fd); }
}
export function publish_private(filename: string, record: unknown, previous?: unknown): void {
  fs.mkdirSync(path.dirname(filename), { recursive: true });
  if (previous !== undefined && !fs.readFileSync(filename).equals(record_bytes(previous)))
    throw new Failure("Custody file is not this operator's; never overwriting it.");
  const temporary = private_file(filename, record_bytes(record));
  try {
    if (previous === undefined) {
      try { fs.linkSync(temporary, filename); } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'EEXIST')
          throw new Failure('A cleanup custody record already exists; never overwriting it.');
        throw error;
      }
    } else fs.renameSync(temporary, filename);
  } finally { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); }
  fsync_directory(path.dirname(filename));
}
export const write_reset_record = (filename: string, record: ResetRecord): void => publish_private(filename, record);
export function remove_reset_record(filename: string, record: ResetRecord): void {
  let current: Buffer;
  try { current = fs.readFileSync(filename); }
  catch (error) { throw new Failure('Cleanup custody record is unreadable; not removing it.', { cause: error }); }
  if (!current.equals(record_bytes(record)))
    throw new Failure("Cleanup custody record is not this operator's; never removing it.");
  fs.unlinkSync(filename); fsync_directory(path.dirname(filename));
}
export const DATABASE_STATES = new Set(['submitting', 'accepted', 'completed']);
export const database_intent_path = (filename: string): string => path.join(path.dirname(filename), '.env.azure.reset.database.json');
export const PROVIDER_STATUSES: Record<string, string> = {
  succeeded: 'Succeeded', failed: 'Failed', canceled: 'Canceled', inprogress: 'InProgress',
  running: 'InProgress', accepted: 'InProgress', deleting: 'InProgress',
};
export const MAX_OPERATION_URL = 8192;
export function operation_url(value: unknown): string {
  if (typeof value !== 'string' || value.length > MAX_OPERATION_URL || !/^[A-Za-z0-9/._~%=&?:+,;@!$'()*-]*$/.test(value))
    throw new Failure('Database delete operation URL is malformed; retaining custody.');
  // Inspect the raw authority/path: WHATWG URL normalizes dot segments and default ports.
  const match = /^https:\/\/([^/?#]+)(\/[^?#]*)(\?[^#]*)?$/.exec(value);
  if (!match || match[1] !== 'management.azure.com' || match[2].split('/').includes('..') || value.includes('\\'))
    throw new Failure('Database delete operation URL is not an ARM operation; retaining custody.');
  return value;
}
export interface DatabaseIntent { operator: string; generation: string; state: string; operation: string | null }
export function read_database_intent(filename: string, record: ResetRecord): DatabaseIntent | null {
  let raw: Buffer, intent: DatabaseIntent;
  try { raw = fs.readFileSync(database_intent_path(filename)); intent = JSON.parse(raw.toString()); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null;
    throw new Failure('Database delete intent is unreadable; refusing.', { cause: error });
  }
  if (!intent || typeof intent !== 'object' || Object.keys(intent).sort().join(',') !== 'generation,operation,operator,state' ||
      !DATABASE_STATES.has(intent.state) || !raw.equals(record_bytes(intent)))
    throw new Failure('Database delete intent is malformed; refusing.');
  if (intent.operator !== record.operator || intent.generation !== record.generation)
    throw new Failure('Database delete intent belongs to other custody; never adopting it.');
  if (intent.state === 'accepted' || intent.operation !== null) operation_url(intent.operation);
  if (intent.state === 'submitting' && intent.operation !== null) throw new Failure('Database delete intent is malformed; refusing.');
  return intent;
}
export const UNKNOWN_DATABASE_DELETE = 'Owned database DELETE submission outcome is unknown and no provider operation receipt was captured. A late request could delete a successor database after rotation, so this custody is UNRESUMABLE: cleaning-v1, the custody record and the intent are retained; absence in a listing does not reconcile it.';
export async function settle_database(azure: Azure, record: ResetRecord, filename: string, databases: any[]): Promise<void> {
  const intentPath = database_intent_path(filename);
  let intent = read_database_intent(filename, record);
  if (intent === null) {
    if (!databases.length) return;
    if (databases[0].properties?.provisioningState === 'Deleting')
      throw new Failure("Owned database is Deleting without this custody's intent; refusing.");
    intent = { operator: record.operator, generation: record.generation, state: 'submitting', operation: null };
    publish_private(intentPath, intent);
    let status: number, operation: string | undefined;
    try { [status, operation] = await azure.delete_database(); }
    catch (error) { throw new Failure(UNKNOWN_DATABASE_DELETE, { cause: error }); }
    let update: DatabaseIntent;
    if ([200, 204, 404].includes(status)) update = { ...intent, state: 'completed' };
    else if (status === 202 && operation != null) {
      try { operation_url(operation); } catch (error) { throw new Failure(UNKNOWN_DATABASE_DELETE, { cause: error }); }
      update = { ...intent, state: 'accepted', operation };
    } else throw new Failure(UNKNOWN_DATABASE_DELETE);
    publish_private(intentPath, update, intent); intent = update;
  }
  if (intent.state === 'submitting') throw new Failure(UNKNOWN_DATABASE_DELETE);
  if (intent.state === 'accepted') {
    operation_url(intent.operation);
    const status = await retry_ready(async () => {
      const status = await azure.provider_operation(intent!.operation!);
      if (status === 'InProgress') throw new Failure('Database delete provider operation is still in progress.');
      return status;
    }, 'Database delete operation');
    if (['Failed', 'Canceled'].includes(status)) {
      remove_custody_file(intentPath, intent);
      throw new Failure(`Provider reported the database delete ${status}; resume may resubmit.`);
    }
    if (status !== 'Succeeded')
      throw new Failure('Database delete provider receipt is unknown; accepted intent and custody retained, never cleared or resubmitted.');
    publish_private(intentPath, { ...intent, state: 'completed' }, intent);
  }
}
export function remove_custody_file(filename: string, record: unknown): void {
  if (!fs.readFileSync(filename).equals(record_bytes(record)))
    throw new Failure("Custody file is not this operator's; never removing it.");
  fs.unlinkSync(filename); fsync_directory(path.dirname(filename));
}
export function verified_stop_receipt(stdout: string, generation: string): boolean {
  return stdout.split(/\r?\n/).some(line => {
    try {
      const receipt = JSON.parse(line);
      return receipt !== null && typeof receipt === 'object' &&
        Object.entries(RECEIPT_FIELDS).every(([k, v]) => receipt[k] === v) && receipt.resetGeneration === generation;
    } catch { return false; }
  });
}
export async function reset(azure: Azure, execute: boolean, stop_command: string[], resume = false): Promise<void> {
  if (resume) return _unqualified_reset_resume(azure);
  if (execute) return _unqualified_reset_sequence(azure, stop_command);
  return reset_preview(azure);
}
export async function _unqualified_reset_sequence(azure: Azure, stop_command: string[], filename = RESET_RECORD): Promise<void> {
  return reset_lock(filename, () => _locked_reset_sequence(azure, stop_command, filename));
}
export async function _locked_reset_sequence(azure: Azure, stop_command: string[], filename: string): Promise<void> {
  const c = azure.c;
  if (read_reset_record(filename) !== null || fs.existsSync(database_intent_path(filename)))
    throw new Failure('A cleanup custody record exists; resume it instead of a fresh claim.');
  const control = new PublisherControl(azure);
  await reset_preview(azure);
  if (!stop_command.length) throw new Failure("Execution requires the runner's --stop-command to stop local writers/consumers.");
  await require_running(azure);
  const [metadata] = await control.read();
  require_released(metadata);
  if (![READY, FENCED].includes(metadata.resetstate))
    throw new Failure('Authority is not ready-v1 or fenced-v1; use reset --resume for existing cleanup custody. Nothing was stopped or deleted.');
  const generation = metadata.resetgeneration;
  if (metadata.resetstate === READY) await control.fence(generation);
  const result = await runtime.run_tree(stop_command, 180, ROOT, { ...process.env, ARROWCOL_RESET_GENERATION: generation });
  if (result.returncode) throw new Failure('Writer/consumer stop failed; reset fence retained, no workload data deleted. ' + c.redact(result.stderr));
  if (!verified_stop_receipt(result.stdout, generation))
    throw new Failure('Runner did not verify workspace-owned services stopped for this reset generation; reset fence retained.');
  console.log('Distributed reset fence active; waiting 60s for bounded in-flight publications.');
  await runtime.sleep(60);
  const record = { sandbox: c.sandbox, operator: uuid(), generation, next: uuid() };
  write_reset_record(filename, record);
  try { await control.claim_cleanup(record.operator, generation); }
  catch (error) {
    if (error instanceof UnknownTransition) throw new Failure('Cleanup claim outcome unknown; custody record kept for read-only resume.');
    if (!(error instanceof Failure)) throw error;
    remove_reset_record(filename, record);
    throw new Failure('Reset authority changed during quiescence or publishers remain; refusing data deletion.', { cause: error });
  }
  await running_or_abort(azure, control, record, filename);
  await cleanup_with_custody(azure, control, record, filename);
}
export async function running_or_abort(azure: Azure, control: PublisherControl, record: ResetRecord, filename: string): Promise<void> {
  try { await require_running(azure); } catch (error) {
    if (!(error instanceof Failure)) throw error;
    await control.abort_cleanup(record.operator, record.generation, true);
    remove_reset_record(filename, record);
    throw new Failure('Cluster is not Running after the cleanup claim (a stop won); custody aborted before any deletion. Start via provision, then retry.');
  }
}
export async function _unqualified_reset_resume(azure: Azure, filename = RESET_RECORD): Promise<void> {
  return reset_lock(filename, () => _locked_reset_resume(azure, filename));
}
export async function _locked_reset_resume(azure: Azure, filename: string): Promise<void> {
  const record = read_reset_record(filename);
  if (!record) throw new Failure('No cleanup custody record; nothing to resume.');
  if (!fs.readFileSync(filename).equals(record_bytes(record))) throw new Failure('Cleanup custody record is not in canonical form; refusing.');
  if (record.sandbox !== azure.c.sandbox) throw new Failure('Cleanup custody record belongs to another sandbox; refusing.');
  fsync_directory(path.dirname(filename));
  const control = new PublisherControl(azure), [metadata, registry] = await control.read();
  const { operator, generation } = record;
  if (metadata.resetgeneration === record.next) {
    const intent = read_database_intent(filename, record);
    if (intent) {
      if (intent.state !== 'completed') throw new Failure('Rotated authority with an incomplete database intent; refusing.');
      remove_custody_file(database_intent_path(filename), intent);
    }
    remove_reset_record(filename, record);
    console.log('Earlier cleanup already finished and rotated authority; record cleared.'); return;
  }
  if (metadata.resetgeneration !== generation) throw new Failure("Authority generation is neither this record's nor its successor; refusing.");
  if (metadata.resetstate === FENCED && !Object.keys(registry).length) {
    await require_running(azure);
    try { await control.claim_cleanup(operator, generation); }
    catch (error) {
      if (error instanceof UnknownTransition) throw new Failure('Cleanup claim outcome unknown again; custody record kept.');
      throw error;
    }
    await running_or_abort(azure, control, record, filename);
  } else if (metadata.resetstate !== CLEANING || canonical_json(registry) !== canonical_json({ [operator]: { generation, state: 'active' } }))
    throw new Failure("Authority does not show this record's exact active cleanup custody; never taking over or clearing another or uncertain member.");
  await cleanup_with_custody(azure, control, record, filename);
}
export async function cleanup_with_custody(azure: Azure, control: PublisherControl, record: ResetRecord, filename: string): Promise<void> {
  const c = azure.c, { operator, generation } = record;
  await control.cleanup_operator(operator, generation); await require_running(azure);
  const [databases, inventory] = await reset_inventory(azure);
  if (Object.values(inventory).flat().some(blob => !(blob.version || blob.snapshot || blob.etag)))
    throw new Failure('Inventoried object has no ETag; refusing an unconditional delete.');
  await control.cleanup_operator(operator, generation);
  await settle_database(azure, record, filename, databases);
  for (const [container, blobs] of Object.entries(inventory)) {
    await control.cleanup_operator(operator, generation);
    const files = blobs.filter(blob => !blob.directory), directories = blobs.filter(blob => blob.directory);
    const depth = (blob: BlobEntry) => blob.name.split('/').length - 1;
    const waves = [files, ...[...new Set(directories.map(depth))].sort((a, b) => b - a)
      .map(d => directories.filter(blob => depth(blob) === d))];
    for (const wave of waves) await parallel_each(blob => delete_inventoried(azure, container, blob), wave);
  }
  const remaining = [];
  for (const name of [c.data, c.checkpoints]) remaining.push(...await azure.blobs(name), ...await azure.paths(name));
  if (remaining.length) throw new Failure('Reset left workload state behind; cleanup authority retained.');
  await settle_database(azure, record, filename, []);
  const intent = read_database_intent(filename, record);
  if (intent && intent.state !== 'completed') throw new Failure(UNKNOWN_DATABASE_DELETE);
  await retry_ready(() => no_databases(azure), 'Database deletion');
  await control.finish_cleanup(operator, generation, record.next, true);
  if (intent) remove_custody_file(database_intent_path(filename), intent);
  remove_reset_record(filename, record);
  console.log('Reset verified: zero databases and zero blobs, versions or snapshots; authority is fenced-v1 at a fresh generation.');
}
export async function parallel_each<T>(action: (item: T) => Promise<unknown>, items: T[]): Promise<void> {
  let index = 0, failed = false, first: unknown;
  await Promise.all(Array.from({ length: Math.min(DELETE_PARALLELISM, items.length) }, async () => {
    while (!failed && index < items.length) {
      const item = items[index++];
      try { await action(item); } catch (error) { if (!failed) first = error; failed = true; }
    }
  }));
  // All physically started work has settled before callers may release authority.
  if (failed) throw first;
}
export async function delete_inventoried(azure: Azure, container: string, blob: BlobEntry): Promise<void> {
  const query: Record<string, string> = {};
  if (blob.version) query.versionid = blob.version;
  else if (blob.snapshot) query.snapshot = blob.snapshot;
  if (Object.keys(query).length) { await azure.blob('DELETE', container, blob.name, query, undefined, {}, true); return; }
  if (blob.directory) await azure.delete_directory(container, blob.name, blob.etag!);
  else await azure.blob('DELETE', container, blob.name, undefined, undefined,
    { 'x-ms-delete-snapshots': 'include', 'If-Match': blob.etag! }, true);
}
export async function no_databases(azure: Azure): Promise<void> {
  if ((await azure.collection(azure.c.cluster_id + '/databases')).length) throw new Failure('Database deletion is still in progress.');
}
export function require_released_or_owner(metadata: Metadata, owner: string): void {
  if (metadata.occupancy === 'released') return;
  if (metadata.occupancy !== 'testing' || !owner || metadata.occupancyid !== owner)
    throw new Failure('Only the active cloud test owner may reconcile isolated table bindings.');
}
export async function reconcile_command(azure: Azure, tables: string, owner: string, wait_seconds = 0, expected?: string): Promise<void> {
  const control = new PublisherControl(azure), [metadata] = await control.read();
  require_released_or_owner(metadata, owner);
  if (metadata.occupancy === 'testing' && !expected)
    throw new Failure('Reconcile under test occupancy requires the captured ARROWCOL_RESET_GENERATION (or --reset-generation).');
  const generation = expected || metadata.resetgeneration;
  if (generation !== metadata.resetgeneration) throw new Failure('Reset generation changed since occupancy; reconcile refused before I/O.');
  if (metadata.occupancy === 'released') {
    await exclusive(control, generation, [() => ensure_running(azure), () => reconcile(azure, tables, wait_seconds)]); return;
  }
  const member = uuid();
  await control.admit(member, generation, false, owner);
  await held(control, member, generation, async () => { await ensure_running(azure); await reconcile(azure, tables, wait_seconds); });
  await control.clean_release(member, generation, true);
}
export async function occupancy(azure: Azure, action: string, owner: string, output?: string, generation?: string): Promise<void> {
  try { owner = normalized_uuid(owner); } catch { throw new Failure("Occupancy requires the runner's UUID --occupancy-id."); }
  const [metadata, registry, etag] = await new PublisherControl(azure).read();
  const current = metadata.occupancy;
  if (action === 'occupy') {
    if (metadata.resetstate !== READY) throw new Failure('Cannot start cloud tests unless authority is ready-v1.');
    if (current === 'stopping') throw new Failure('A cluster stop holds the sandbox; tests cannot start.');
    if (current !== 'released' && (current !== 'testing' || metadata.occupancyid !== owner))
      throw new Failure('Another cloud test owns the sandbox.');
    metadata.occupancy = 'testing'; metadata.occupancyid = owner;
  } else {
    if (current !== 'testing' || metadata.occupancyid !== owner) throw new Failure('Only the exact occupancy owner can release cloud tests.');
    if (!generation || metadata.resetgeneration !== generation) throw new Failure('Release requires the captured ARROWCOL_RESET_GENERATION of this test.');
    if (Object.keys(registry).length) throw new Failure('Registered active or uncertain publishers remain; occupancy retained.');
    metadata.occupancy = 'released';
  }
  await azure.set_control(metadata, etag);
  if (action === 'occupy' && output !== undefined) {
    const [current] = await azure.control();
    if (current.occupancyid !== owner || current.occupancy !== 'testing' || current.resetstate !== READY || current.resetgeneration !== metadata.resetgeneration)
      throw new Failure('Authority changed after occupancy; do not admit collectors.');
    write_private(output, { ...azure.c.generated(), ARROWCOL_RESET_GENERATION: current.resetgeneration });
  }
  console.log('Cloud test occupancy: ' + (action === 'occupy' ? 'TESTING' : 'RELEASED'));
}
export async function recover(azure: Azure, execute: boolean, stop_command: string[]): Promise<void> {
  const control = new PublisherControl(azure), [metadata, registry] = await control.read();
  console.log(`Recover preview: state=${metadata.resetstate}, occupancy=${metadata.occupancy}, registered publishers=${Object.keys(registry).length}`);
  if (!execute) { console.log('Recover --execute stops local writers, fences, rotates the generation, clears the registry and releases occupancy. Then run reset --execute and provision.'); return; }
  const generation = metadata.resetgeneration;
  if (![READY, FENCED].includes(metadata.resetstate) || metadata.occupancy === 'stopping')
    throw new Failure('Cleanup or a cluster stop owns authority; recovery refused before any stop.');
  const result = await runtime.run_tree(stop_command, 180, ROOT, { ...process.env, ARROWCOL_RESET_GENERATION: generation });
  if (result.returncode || !verified_stop_receipt(result.stdout, generation))
    throw new Failure('Runner did not verify workspace-owned services stopped; nothing recovered. ' + azure.c.redact(result.stderr));
  console.log('Local writers stopped; waiting 60s for bounded in-flight publications.');
  await runtime.sleep(60);
  try { await control.abandon(generation, uuid(), true); }
  catch (error) {
    if (error instanceof UnknownTransition) throw new Failure('Recovery CAS outcome unknown; re-read authority with recover (preview).');
    throw error;
  }
  console.log('Recovered: fenced-v1, released, empty registry, fresh generation. Next: reset --execute, then provision.');
}
export async function stop_cluster(azure: Azure): Promise<void> {
  const [metadata, registry, etag] = await new PublisherControl(azure).read();
  if (metadata.occupancy === 'stopping')
    throw new Failure('A prior stop hold may have an ambiguous ARM submission; not replaying or releasing it. Reconcile the old request explicitly.');
  if (metadata.occupancy !== 'released') throw new Failure('Cloud tests own the sandbox; wait for explicit occupancy release.');
  if (![READY, FENCED].includes(metadata.resetstate)) throw new Failure('Cleanup owns the authority; stop refused.');
  if (Object.keys(registry).length) throw new Failure('Registered publishers or bootstrap owners protect the cluster; stop refused.');
  const hold = uuid();
  metadata.occupancy = 'stopping'; metadata.occupancyid = hold;
  await azure.set_control(metadata, etag);
  await azure.cli('rest', '--method', 'post', '--url', 'https://management.azure.com' + azure.c.cluster_id + '/stop?api-version=' + API);
  await retry_ready(() => stopped(azure), 'Cluster stop', 1200);
  const [current, currentEtag] = await azure.control();
  if (current.occupancy !== 'stopping' || current.occupancyid !== hold)
    throw new Failure('Stop hold changed unexpectedly; reconcile occupancy before reuse.');
  current.occupancy = 'released'; await azure.set_control(current, currentEtag);
}
export async function stopped(azure: Azure): Promise<void> {
  if ((await azure.arm('GET', azure.c.cluster_id)).properties.state !== 'Stopped') throw new Failure('Cluster is still stopping.');
}
export async function main(argv = process.argv.slice(2)): Promise<number> {
  let c: Config | undefined;
  try {
    const stopIndex = argv.indexOf('--stop-command');
    const stopCommand = stopIndex < 0 || stopIndex === argv.length - 1 ? DEFAULT_STOP_COMMAND : argv.slice(stopIndex + 1);
    const { values: args, positionals } = parseArgs({ args: stopIndex < 0 ? argv : argv.slice(0, stopIndex),
      allowPositionals: true, options: {
        'env-file': { type: 'string', default: path.join(ROOT, '.env') }, output: { type: 'string', default: path.join(ROOT, '.env.azure') },
        tables: { type: 'string', default: path.join(ROOT, '.env.azure.tables.json') }, 'infra-only': { type: 'boolean', default: false },
        execute: { type: 'boolean', default: false }, resume: { type: 'boolean', default: false },
        'occupancy-id': { type: 'string', default: '' }, 'reset-generation': { type: 'string', default: '' },
        'wait-seconds': { type: 'string', default: '0' }, help: { type: 'boolean', short: 'h' },
      } });
    if (args.help) { console.log('Usage: bootstrap.ts <config|preflight|provision|reconcile|reset|recover|status|stop|occupy|release> [--env-file PATH] [--output PATH] [--tables PATH] [--infra-only] [--execute] [--resume] [--occupancy-id UUID] [--reset-generation UUID] [--wait-seconds 0..600] [--stop-command COMMAND ...]'); return 0; }
    const command = positionals[0];
    if (positionals.length !== 1 || !['config', 'preflight', 'provision', 'reconcile', 'reset', 'recover', 'status', 'stop', 'occupy', 'release'].includes(command))
      throw new Failure('A valid Azure bootstrap command is required.');
    const values = { ...load_env(args['env-file']!), ...process.env } as Record<string, string>;
    c = new Config(values);
    const wait = Number(args['wait-seconds']);
    if (!/^\d+$/.test(args['wait-seconds']!) || !Number.isInteger(wait) || wait < 0 || wait > 600)
      throw new Failure('--wait-seconds must be in the range 0..600.');
    const azure = new Azure(c), generation = args['reset-generation'] || values.ARROWCOL_RESET_GENERATION;
    switch (command) {
      case 'config': write_private(args.output!, c.generated()); console.log('Private generated Azure configuration written (values withheld).'); break;
      case 'preflight': await preflight(azure); break;
      case 'provision': await preflight(azure); await provision(azure, args.tables!, wait, args.output!, args['infra-only']!, values); break;
      case 'reconcile': await reconcile_command(azure, args.tables!, args['occupancy-id']!, wait, generation); break;
      case 'reset': await reset(azure, args.execute!, stopCommand, args.resume!); break;
      case 'recover': await recover(azure, args.execute!, stopCommand); break;
      case 'status': {
        const [databases] = await reset_inventory(azure);
        if (!databases.length) console.log('Owned infrastructure: present; Kusto database absent (data plane reset). Run provision to recreate it.');
        else { await azure.kql('.show capacity'); console.log('Owned infrastructure and Kusto capacity: responsive'); }
        break;
      }
      case 'stop':
        owned(await azure.arm('GET', c.cluster_id), c);
        if (args.execute) { await stop_cluster(azure); console.log('Owned Kusto cluster stopped; infrastructure preserved.'); }
        else console.log('Stop preview: preserve cluster; --execute requests stop after test occupancy release.');
        break;
      case 'occupy': case 'release': await occupancy(azure, command, args['occupancy-id']!, command === 'occupy' ? args.output : undefined, generation); break;
    }
    return 0;
  } catch (error) {
    if (error instanceof NotReady) {
      console.error(JSON.stringify({ status: 'not_ready', code: 'DELTA_LOGS_ABSENT', retryable: true, kql_attempted: false })); return 3;
    }
    console.error('ERROR: ' + (c ? c.redact(error instanceof Error ? error.message : String(error)) : 'Invalid or missing private Azure configuration.'));
    return 1;
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href)
  process.exitCode = await main();
