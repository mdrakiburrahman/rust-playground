import { createHash } from 'node:crypto';
import { tableFromIPC } from 'apache-arrow';
import { readParquet } from 'parquet-wasm';
import { canonical_json, parse_xml, type Azure, type HttpReceipt } from '../../infra/azure/bootstrap.js';

type Json = Record<string, unknown>;
export interface DeltaActiveFile {
  path: string;
  size: number;
  num_records: number | null;
  partition_values: Record<string, string | null>;
}
export interface DeltaSourceObject {
  name: string;
  etag: string;
  bytes: number;
  sha256: string;
}
export interface DeltaPresentReference {
  schema_version: 1;
  observation: 'table';
  account: string;
  container: string;
  table_root: string;
  table_id: string;
  version: number;
  protocol: Record<string, unknown>;
  active_files: DeltaActiveFile[];
  active_file_count: number;
  active_bytes: number;
  row_count: number | null;
  observed_at_unix_ms: number;
  provenance: {
    reader: 'aks-delta-replay-v1';
    metadata_sha256: string;
    partition_columns: string[];
    checkpoint_version: number | null;
    source_objects: DeltaSourceObject[];
    inventory_sha256: string;
    reference_sha256: string;
  };
}
export interface DeltaAbsenceObservation {
  status: 200;
  prefix: string;
  request_id: string;
  response_sha256: string;
  observed_at_unix_ms: number;
  completed: true;
  object_count: 0;
}
export interface DeltaAbsentReference {
  schema_version: 1;
  observation: 'observed_absent';
  account: string;
  container: string;
  table_root: string;
  table_id: null;
  version: null;
  protocol: null;
  active_files: null;
  active_file_count: null;
  active_bytes: null;
  row_count: null;
  observed_at_unix_ms: number;
  provenance: {
    reader: 'aks-delta-absence-v1';
    log_prefix: string;
    observations: [DeltaAbsenceObservation, DeltaAbsenceObservation];
    reference_sha256: string;
  };
}
export type DeltaReplayReference = DeltaPresentReference | DeltaAbsentReference;
/** Trusted runner configuration, not a policy inferred from DELETE's name or watermark. */
export interface DeltaRetentionPolicy {
  retention_ms: number;
  granularity: 'minute' | 'hour' | 'day';
}
export interface DeltaPreservationResult {
  status: 'preserved' | 'unknown' | 'failed';
  disposition: 'table_preserved' | 'absence_preserved' | null;
  reason: string | null;
  current: DeltaReplayReference | null;
  purge_explained_rows: number | null;
}

const MAX_OBJECT_BYTES = 64 * 1024 * 1024;
const MAX_TOTAL_BYTES = 256 * 1024 * 1024;
const MAX_OBJECTS = 20_000;
const MAX_ACTIONS = 1_000_000;
const MAX_PAGES = 100;
const MAX_VERSIONS = 20_000;
const HASH = /^[a-f0-9]{64}$/;
const REQUEST_ID = /^[A-Za-z0-9-]{1,128}$/;
const ETAG = /^"[^"\r\n]+"$/;
const JSON_LOG = /^(\d{20})\.json$/;
const CHECKPOINT = /^(\d{20})\.checkpoint(?:\.(\d{10})\.(\d{10}))?\.parquet$/;
const hash = (value: string | Buffer) => createHash('sha256').update(value).digest('hex');
const same = (left: unknown, right: unknown) => canonical_json(left) === canonical_json(right);
class ReplayError extends Error {
  constructor(message: string, readonly status: 'unknown' | 'failed' = 'unknown') { super(message); }
}
function requireThat(value: unknown, reason: string): asserts value {
  if (!value) throw new ReplayError(reason);
}
function object(value: unknown, what: string): Json {
  requireThat(value !== null && typeof value === 'object' && !Array.isArray(value), `${what} must be an object`);
  return value as Json;
}
function integer(value: unknown, what: string): number {
  requireThat(typeof value === 'number' && Number.isSafeInteger(value) && value >= 0, `${what} is not a measured safe integer`);
  return value;
}
function sum(values: number[], what: string): number {
  return integer(values.reduce((total, value) => total + value, 0), what);
}
function text(value: unknown, what: string): string {
  requireThat(typeof value === 'string' && value.length > 0 && value.length <= 4096, `${what} is missing or oversized`);
  return value;
}
function exact(value: Json, names: string[], what: string): void {
  requireThat(same(Object.keys(value).sort(), [...names].sort()), `${what} has an unexpected schema`);
}
function safePath(value: unknown): string {
  const path = text(value, 'relative path');
  // Percent escapes are deliberately not accepted: double decoding must never hide traversal or SAS.
  requireThat(path.length <= 2048 && path.split('/').every(part =>
    /^[A-Za-z0-9_=-][A-Za-z0-9_.=-]*$/.test(part) && part !== '.' && part !== '..'),
  'unsafe relative path (URI, traversal, escapes or secret components)');
  return path;
}
function binding(azure: Azure, relativePath: string) {
  const root = safePath(relativePath);
  requireThat(/^[a-z0-9]{3,24}$/.test(azure.c.account), 'invalid storage account');
  requireThat(/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/.test(azure.c.data), 'invalid data container');
  const prefix = azure.c.table_prefix;
  if (prefix) requireThat(root.startsWith(safePath(prefix) + '/'), 'table root is outside configured data prefix');
  return { account: azure.c.account, container: azure.c.data, table_root: root };
}
function protocol(value: unknown): Json {
  const p = object(value, 'protocol');
  requireThat(Object.keys(p).every(key => ['minReaderVersion', 'minWriterVersion', 'readerFeatures', 'writerFeatures'].includes(key)),
    'unsupported protocol fields');
  requireThat(integer(p.minReaderVersion, 'minReaderVersion') === 1 &&
    [1, 2].includes(integer(p.minWriterVersion, 'minWriterVersion')), 'unsupported Delta protocol');
  for (const key of ['readerFeatures', 'writerFeatures']) {
    if (p[key] !== undefined) requireThat(Array.isArray(p[key]) && p[key].length === 0, 'unsupported Delta protocol features');
  }
  return p;
}
function columns(value: unknown): string[] {
  requireThat(Array.isArray(value) && value.length <= 100 && value.every(v => typeof v === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(v)),
    'invalid partition columns');
  requireThat(new Set(value).size === value.length, 'duplicate partition columns');
  return value;
}
function metadata(value: unknown): { id: string; fingerprint: string; partitions: string[] } {
  const m = object(value, 'metaData'), format = object(m.format, 'metadata format');
  requireThat(format.provider === 'parquet', 'unsupported Delta data format');
  const config = object(m.configuration, 'metadata configuration');
  requireThat(!config['delta.columnMapping.mode'] || config['delta.columnMapping.mode'] === 'none', 'unsupported column mapping');
  requireThat(!config['delta.enableDeletionVectors'] || config['delta.enableDeletionVectors'] === 'false', 'unsupported deletion vectors');
  requireThat(typeof m.schemaString === 'string' && m.schemaString.length > 0 && m.schemaString.length <= MAX_OBJECT_BYTES,
    'metadata schema is missing or oversized');
  const schema = object(JSON.parse(m.schemaString), 'metadata schema');
  requireThat(schema.type === 'struct' && Array.isArray(schema.fields), 'unsupported metadata schema');
  return { id: text(m.id, 'table ID'), fingerprint: hash(canonical_json(m)), partitions: columns(m.partitionColumns) };
}
function partitionValues(value: unknown): Record<string, string | null> {
  const result = object(value, 'partition values');
  requireThat(Object.keys(result).length <= 100 && Object.entries(result).every(([key, val]) =>
    /^[A-Za-z_][A-Za-z0-9_]*$/.test(key) && (val === null || typeof val === 'string' && val.length <= 1024)),
  'invalid partition values');
  return result as Record<string, string | null>;
}
function addFile(value: unknown): DeltaActiveFile {
  const a = object(value, 'add');
  requireThat(a.deletionVector == null && a.baseRowId == null && a.defaultRowCommitVersion == null, 'unsupported add features');
  requireThat(typeof a.dataChange === 'boolean', 'add lacks dataChange');
  let records: number | null = null;
  if (a.stats !== undefined && a.stats !== null) {
    requireThat(typeof a.stats === 'string' && a.stats.length > 0 && a.stats.length <= MAX_OBJECT_BYTES, 'add stats is invalid');
    const stats = object(JSON.parse(a.stats), 'add stats');
    if (stats.numRecords !== undefined && stats.numRecords !== null) records = integer(stats.numRecords, 'numRecords');
  } else if (a.stats_parsed !== undefined && a.stats_parsed !== null) {
    const stats = object(a.stats_parsed, 'parsed add stats');
    if (stats.numRecords !== undefined && stats.numRecords !== null) records = integer(stats.numRecords, 'numRecords');
  }
  return { path: safePath(a.path), size: integer(a.size, 'file size'), num_records: records,
    partition_values: partitionValues(a.partitionValues) };
}
function totals(files: DeltaActiveFile[]) {
  return { active_file_count: files.length, active_bytes: sum(files.map(f => f.size), 'active bytes'),
    row_count: files.some(f => f.num_records === null) ? null : sum(files.map(f => f.num_records!), 'row count') };
}
function fingerprint(reference: DeltaReplayReference): string {
  const { reference_sha256: _, ...provenance } = reference.provenance;
  return hash(canonical_json({ ...reference, provenance }));
}
function versionName(version: number): string { return version.toString().padStart(20, '0') + '.json'; }
function versionOf(value: string): number { return integer(Number(value), 'Delta version'); }
interface Entry { name: string; etag: string; bytes: number }
interface State {
  files: Map<string, DeltaActiveFile>;
  tableId: string | null;
  metadataHash: string | null;
  partitions: string[];
  protocol: Json | null;
}
const freshState = (): State => ({ files: new Map(), tableId: null, metadataHash: null, partitions: [], protocol: null });
function apply(state: State, actions: Json[], checkpoint = false): void {
  const seen = new Set<string>();
  for (const action of actions) {
    const keys = Object.keys(action).filter(key => action[key] !== null && action[key] !== undefined);
    requireThat(keys.length === 1, 'Delta action must have exactly one populated field');
    const key = keys[0], value = action[key];
    requireThat(['add', 'remove', 'metaData', 'protocol', 'txn', 'commitInfo'].includes(key), 'unsupported Delta action');
    if (key === 'metaData') {
      const m = metadata(value);
      if (state.tableId && state.tableId !== m.id) throw new ReplayError('Delta table ID changed', 'failed');
      state.tableId = m.id; state.metadataHash = m.fingerprint; state.partitions = m.partitions;
    } else if (key === 'protocol') {
      state.protocol = protocol(value);
    } else if (key === 'remove') {
      const r = object(value, 'remove');
      safePath(r.path);
      requireThat(!r.deletionVector && typeof r.dataChange === 'boolean', 'unsupported or incomplete remove');
      if (!checkpoint) {
        requireThat(state.files.has(String(r.path)), 'remove references an unknown active file');
        state.files.delete(String(r.path));
      }
    } else if (key === 'add') {
      const file = addFile(value);
      requireThat(!seen.has(file.path), 'duplicate add path');
      requireThat(!state.files.has(file.path), 'add overwrites an active file');
      seen.add(file.path); state.files.set(file.path, file);
    } else object(value, key);
    requireThat(state.files.size <= MAX_ACTIONS, 'active file limit exceeded');
  }
}
function normalizeArrow(value: any): any {
  if (typeof value === 'bigint') {
    requireThat(value >= 0n && value <= BigInt(Number.MAX_SAFE_INTEGER), 'checkpoint integer overflow');
    return Number(value);
  }
  if (value === null || typeof value !== 'object') return value;
  if (typeof value.toJSON === 'function') return normalizeArrow(value.toJSON());
  if (value instanceof Map) return Object.fromEntries([...value].map(([key, val]) => [key, normalizeArrow(val)]));
  if (Array.isArray(value)) return value.map(normalizeArrow);
  return Object.fromEntries(Object.entries(value).map(([key, val]) => [key, normalizeArrow(val)]));
}
async function capture(azure: Azure, relativePath: string, assertCurrent: () => Promise<void>, historyAfter?: number):
  Promise<{ reference: DeltaReplayReference; commits: Map<number, Json[]> }> {
  const bound = binding(azure, relativePath), prefix = bound.table_root + '/_delta_log/';
  const absenceObservations: DeltaAbsenceObservation[] = [];
  let bytesRead = 0, actionCount = 0;
  const invoke = async (name: string, query?: Record<string, string>, headers: Record<string, string> = {}) => {
    await assertCurrent();
    let receipt: HttpReceipt | undefined;
    const [bytes, responseHeaders] = await azure.blob('GET', bound.container, name, query, undefined, headers,
      false, true, value => { receipt = value; });
    await assertCurrent();
    requireThat(receipt && [200, 206].includes(receipt.status) && bytes, 'storage GET has no positive complete response');
    const h = Object.fromEntries(Object.entries(responseHeaders).map(([key, val]) => [key.toLowerCase(), val]));
    if (receipt.status === 206) {
      requireThat(h['content-range'] === `bytes 0-${bytes.length - 1}/${bytes.length}`, 'storage object exceeds bounded GET');
    }
    bytesRead += bytes.length;
    requireThat(bytes.length <= MAX_OBJECT_BYTES && bytesRead <= MAX_TOTAL_BYTES, 'Delta read byte limit exceeded');
    return { bytes, headers: h, status: receipt.status, observed_at_unix_ms: Date.now() };
  };
  const inventory = async (): Promise<Entry[]> => {
    const result: Entry[] = [], markers = new Set<string>();
    let marker = '';
    for (let page = 0; page < MAX_PAGES; page++) {
      const response = await invoke('', { restype: 'container', comp: 'list', prefix, marker, maxresults: '1000' },
        { Range: `bytes=0-${MAX_OBJECT_BYTES}`, 'Cache-Control': 'no-cache' });
      const root = parse_xml(response.bytes.toString());
      requireThat(root.name === 'EnumerationResults', 'invalid Delta listing');
      const child = (node: typeof root, name: string) => node.children.find(c => c.name === name);
      requireThat(['Prefix', 'Blobs', 'NextMarker'].every(name => root.children.filter(c => c.name === name).length === 1) &&
        child(root, 'Prefix')!.text === prefix && !child(root, 'Delimiter')?.text, 'listing does not confirm the exact undelimited Delta prefix');
      const blobs = child(root, 'Blobs');
      requireThat(blobs, 'Delta listing lacks Blobs');
      for (const blob of blobs.children) {
        requireThat(blob.name === 'Blob', 'unsupported listing entry');
        const name = child(blob, 'Name')?.text ?? '', props = child(blob, 'Properties');
        requireThat(name.startsWith(prefix), 'listing escaped the exact Delta prefix');
        safePath(name);
        const etag = props && child(props, 'Etag')?.text;
        const size = props && child(props, 'Content-Length')?.text;
        requireThat(etag && ETAG.test(etag) && size && /^\d+$/.test(size), 'listing lacks strong ETag or size');
        requireThat(!child(blob, 'Snapshot') && !child(blob, 'VersionId') && !child(blob, 'Deleted'), 'unexpected historical blob');
        result.push({ name, etag, bytes: integer(Number(size), 'listed size') });
        requireThat(result.length <= MAX_OBJECTS, 'Delta listing object limit exceeded');
      }
      const next = child(root, 'NextMarker');
      requireThat(next, 'Delta listing lacks completion marker');
      marker = next.text;
      if (!marker) {
        result.sort((a, b) => a.name.localeCompare(b.name));
        requireThat(new Set(result.map(e => e.name)).size === result.length, 'duplicate listing object');
        if (result.length === 0) {
          requireThat(page === 0 && response.status === 200 && REQUEST_ID.test(response.headers['x-ms-request-id'] ?? ''),
            'empty listing lacks authoritative complete observation provenance');
          absenceObservations.push({ status: 200, prefix, request_id: response.headers['x-ms-request-id'],
            response_sha256: hash(response.bytes), observed_at_unix_ms: response.observed_at_unix_ms,
            completed: true, object_count: 0 });
        }
        return result;
      }
      requireThat(!markers.has(marker), 'listing marker cycle');
      markers.add(marker);
    }
    throw new ReplayError('Delta listing page limit exceeded');
  };
  const before = await inventory(), byName = new Map(before.map(entry => [entry.name, entry]));
  if (before.length === 0) {
    const after = await inventory();
    requireThat(after.length === 0 && absenceObservations.length === 2, 'Delta listing changed during absence observation');
    requireThat(absenceObservations[0].request_id !== absenceObservations[1].request_id,
      'absence observations reused a storage request receipt');
    const reference: DeltaAbsentReference = { schema_version: 1, observation: 'observed_absent', ...bound,
      table_id: null, version: null, protocol: null, active_files: null, active_file_count: null, active_bytes: null,
      row_count: null, observed_at_unix_ms: absenceObservations[1].observed_at_unix_ms,
      provenance: { reader: 'aks-delta-absence-v1', log_prefix: prefix,
        observations: absenceObservations as [DeltaAbsenceObservation, DeltaAbsenceObservation], reference_sha256: '' } };
    reference.provenance.reference_sha256 = fingerprint(reference);
    return { reference, commits: new Map() };
  }
  const objects = new Map<string, DeltaSourceObject>();
  const get = async (name: string): Promise<Buffer> => {
    const entry = byName.get(prefix + name);
    requireThat(entry && entry.bytes > 0 && entry.bytes <= MAX_OBJECT_BYTES, 'required Delta history/object is missing or oversized');
    const response = await invoke(entry.name, undefined,
      { 'If-Match': entry.etag, Range: `bytes=0-${MAX_OBJECT_BYTES}` });
    requireThat(response.headers.etag === entry.etag && ETAG.test(response.headers.etag), 'Delta object ETag changed or is not strong');
    requireThat(response.bytes.length === entry.bytes, 'incomplete Delta object');
    objects.set(entry.name, { ...entry, sha256: hash(response.bytes) });
    return response.bytes;
  };
  const json = new Map<number, string>(), checkpoints = new Map<number, Array<{ name: string; part: number; total: number }>>();
  for (const entry of before) {
    const name = entry.name.slice(prefix.length), log = JSON_LOG.exec(name), cp = CHECKPOINT.exec(name);
    requireThat(!name.includes('/') && !name.includes('sidecar') &&
      (!name.includes('.checkpoint.') || cp), 'unsupported checkpoint-v2/sidecar layout');
    if (log) json.set(versionOf(log[1]), name);
    else if (cp) {
      const version = versionOf(cp[1]), parts = checkpoints.get(version) ?? [];
      parts.push({ name, part: cp[2] ? versionOf(cp[2]) : 1, total: cp[3] ? versionOf(cp[3]) : 1 });
      checkpoints.set(version, parts);
    } else requireThat(name === '_last_checkpoint' || /^\..*\.crc$/.test(name), 'unsupported Delta log object');
  }
  if (byName.has(prefix + '_last_checkpoint')) {
    const last = object(JSON.parse((await get('_last_checkpoint')).toString()), 'last checkpoint');
    requireThat(!last.v2Checkpoint && !last.checkpointSchema && !last.sidecarFiles, 'unsupported checkpoint-v2');
    const version = integer(last.version, 'last checkpoint version');
    requireThat(checkpoints.has(version), 'last checkpoint is incomplete');
    if (last.parts !== undefined) requireThat(integer(last.parts, 'checkpoint parts') === checkpoints.get(version)!.length,
      'last checkpoint parts mismatch');
  }
  const versions = [...json.keys(), ...checkpoints.keys()];
  requireThat(versions.length > 0, 'Delta log is empty');
  const latest = Math.max(...versions), checkpointVersion = checkpoints.size ? Math.max(...checkpoints.keys()) : null;
  const state = freshState(), commits = new Map<number, Json[]>();
  if (checkpointVersion !== null) {
    const parts = checkpoints.get(checkpointVersion)!.sort((a, b) => a.part - b.part);
    requireThat(parts.length === parts[0].total && parts.every((p, index) => p.part === index + 1 && p.total === parts.length),
      'incomplete multipart checkpoint');
    for (const part of parts) {
      const table = tableFromIPC(readParquet(await get(part.name)).intoIPCStream());
      actionCount += table.numRows;
      requireThat(actionCount <= MAX_ACTIONS, 'Delta action limit exceeded');
      apply(state, table.toArray().map(row => normalizeArrow(row)), true);
    }
    requireThat(state.tableId && state.protocol, 'checkpoint lacks metadata/protocol');
  }
  const start = checkpointVersion === null ? 0 : checkpointVersion + 1;
  const historyStart = historyAfter === undefined ? start : Math.min(start, historyAfter + 1);
  requireThat(latest - historyStart < MAX_VERSIONS, 'Delta history version limit exceeded');
  for (let version = historyStart; version <= latest; version++) {
    const name = json.get(version);
    requireThat(name, 'contiguous JSON history is missing');
    const lines = (await get(name)).toString('utf8').trim().split('\n');
    requireThat(lines.length > 0, 'empty Delta commit');
    actionCount += lines.length;
    requireThat(actionCount <= MAX_ACTIONS, 'Delta action limit exceeded');
    const actions = lines.map(line => object(JSON.parse(line), 'Delta JSON action'));
    commits.set(version, actions);
    if (version >= start) apply(state, actions);
  }
  requireThat(state.tableId && state.protocol && state.metadataHash, 'Delta replay lacks metadata/protocol');
  const after = await inventory();
  requireThat(same(before, after), 'Delta listing changed during replay');
  const active = [...state.files.values()].sort((a, b) => a.path.localeCompare(b.path));
  const reference: DeltaPresentReference = { schema_version: 1, observation: 'table', ...bound, table_id: state.tableId, version: latest,
    protocol: state.protocol, active_files: active, ...totals(active), observed_at_unix_ms: Date.now(),
    provenance: { reader: 'aks-delta-replay-v1', metadata_sha256: state.metadataHash,
      partition_columns: state.partitions, checkpoint_version: checkpointVersion,
      source_objects: [...objects.values()].sort((a, b) => a.name.localeCompare(b.name)),
      inventory_sha256: hash(canonical_json(before)), reference_sha256: '' } };
  reference.provenance.reference_sha256 = fingerprint(reference);
  return { reference, commits };
}

/** Fresh replay or two authoritative empty-prefix observations; errors never manufacture absence or zero rows. */
export async function readDeltaReference(azure: Azure, relativePath: string,
  assertCurrent: () => Promise<void>): Promise<DeltaReplayReference> {
  return (await capture(azure, relativePath, assertCurrent)).reference;
}

export interface DeltaAddWindow {
  total_rows: number;
  total_files: number;
  total_bytes: number;
  total_commits: number;
  window_rows: number;
  window_files: number;
  window_bytes: number;
  window_commits: number;
  first_version: number | null;
  last_version: number | null;
}

export function reconcileDeltaAdds(commits: Map<number, Json[]>, startUnixMs: number, endUnixMs: number): DeltaAddWindow {
  requireThat(Number.isSafeInteger(startUnixMs) && Number.isSafeInteger(endUnixMs) && startUnixMs < endUnixMs,
    'invalid Delta measurement window');
  const result: DeltaAddWindow = { total_rows: 0, total_files: 0, total_bytes: 0, total_commits: 0,
    window_rows: 0, window_files: 0, window_bytes: 0, window_commits: 0, first_version: null, last_version: null };
  for (const [version, actions] of [...commits].sort(([a], [b]) => a - b)) {
    const records = (key: string): Json[] => actions.flatMap(action => {
      const value = action[key];
      return value !== null && typeof value === 'object' && !Array.isArray(value) ? [value as Json] : [];
    });
    const infos = records('commitInfo');
    const adds = records('add');
    const removes = records('remove');
    requireThat(infos.length <= 1, 'Delta commit has duplicate commitInfo actions');
    if (removes.some(action => action.dataChange === true))
      throw new ReplayError('Data-changing Delta removals are outside the append-only benchmark contract');
    if (!adds.length) continue;
    requireThat(infos.length === 1, 'Delta adds lack a single commit timestamp');
    const timestamp = integer(infos[0]!.timestamp, 'Delta commit timestamp');
    let rows = 0, files = 0, bytes = 0;
    for (const action of adds) {
      const file = addFile(action);
      if (action.dataChange !== true) continue;
      requireThat(file.num_records !== null, 'Delta Add lacks numRecords statistics');
      rows = sum([rows, file.num_records], 'commit rows');
      files = sum([files, 1], 'commit files');
      bytes = sum([bytes, file.size], 'commit bytes');
    }
    if (!files) continue;
    result.total_rows = sum([result.total_rows, rows], 'total Delta Add rows');
    result.total_files = sum([result.total_files, files], 'total Delta Add files');
    result.total_bytes = sum([result.total_bytes, bytes], 'total Delta Add bytes');
    result.total_commits = sum([result.total_commits, 1], 'total Delta Add commits');
    if (timestamp >= startUnixMs && timestamp < endUnixMs) {
      result.window_rows = sum([result.window_rows, rows], 'measured Delta Add rows');
      result.window_files = sum([result.window_files, files], 'measured Delta Add files');
      result.window_bytes = sum([result.window_bytes, bytes], 'measured Delta Add bytes');
      result.window_commits = sum([result.window_commits, 1], 'measured Delta Add commits');
      result.first_version ??= version;
      result.last_version = version;
    }
  }
  return result;
}

export async function readDeltaAddWindow(azure: Azure, relativePath: string, startUnixMs: number, endUnixMs: number,
  assertCurrent: () => Promise<void>): Promise<DeltaAddWindow & { delta_version: number; active_rows: number }> {
  const { reference, commits } = await capture(azure, relativePath, assertCurrent, -1);
  if (reference.observation !== 'table' || reference.row_count === null)
    throw new ReplayError('Delta table or complete active-file row statistics are unavailable');
  const adds = reconcileDeltaAdds(commits, startUnixMs, endUnixMs);
  if (adds.total_rows !== reference.row_count)
    throw new ReplayError('Delta Add row totals do not reconcile with the final active-file snapshot');
  return { ...adds, delta_version: reference.version, active_rows: reference.row_count };
}

export function parseDeltaReplayReference(input: unknown, azure: Azure, relativePath: string): DeltaReplayReference {
  const r = object(input, 'Delta reference');
  exact(r, ['schema_version', 'observation', 'account', 'container', 'table_root', 'table_id', 'version', 'protocol', 'active_files',
    'active_file_count', 'active_bytes', 'row_count', 'observed_at_unix_ms', 'provenance'], 'Delta reference');
  const bound = binding(azure, relativePath);
  requireThat(r.schema_version === 1 && Object.entries(bound).every(([key, value]) => r[key] === value),
    'Delta reference storage binding mismatch');
  integer(r.observed_at_unix_ms, 'observation time');
  if (r.observation === 'observed_absent') {
    requireThat(['table_id', 'version', 'protocol', 'active_files', 'active_file_count', 'active_bytes', 'row_count']
      .every(key => r[key] === null), 'absent Delta reference must not manufacture table identity or measured totals');
    const p = object(r.provenance, 'absence provenance');
    exact(p, ['reader', 'log_prefix', 'observations', 'reference_sha256'], 'absence provenance');
    requireThat(p.reader === 'aks-delta-absence-v1' && p.log_prefix === bound.table_root + '/_delta_log/' &&
      typeof p.reference_sha256 === 'string' && HASH.test(p.reference_sha256), 'invalid absence provenance binding');
    requireThat(Array.isArray(p.observations) && p.observations.length === 2, 'absence requires two complete observations');
    let previous = 0;
    for (const value of p.observations) {
      const observation = object(value, 'absence observation');
      exact(observation, ['status', 'prefix', 'request_id', 'response_sha256', 'observed_at_unix_ms', 'completed', 'object_count'],
        'absence observation');
      requireThat(observation.status === 200 && observation.prefix === p.log_prefix && observation.completed === true &&
        observation.object_count === 0 && typeof observation.request_id === 'string' && REQUEST_ID.test(observation.request_id) &&
        typeof observation.response_sha256 === 'string' && HASH.test(observation.response_sha256),
      'absence observation is not positive, bounded and complete');
      const at = integer(observation.observed_at_unix_ms, 'absence observation time');
      requireThat(at >= previous && at <= Number(r.observed_at_unix_ms), 'absence observation chronology is inconsistent');
      previous = at;
    }
    requireThat(previous === r.observed_at_unix_ms, 'absence reference time is not its final observation');
    requireThat(p.observations[0].request_id !== p.observations[1].request_id, 'absence observations reused a storage request receipt');
    const reference = r as unknown as DeltaAbsentReference;
    requireThat(fingerprint(reference) === p.reference_sha256, 'Delta absence provenance digest mismatch');
    return JSON.parse(JSON.stringify(reference)) as DeltaAbsentReference;
  }
  requireThat(r.observation === 'table', 'unsupported Delta observation discriminant');
  text(r.table_id, 'table ID'); integer(r.version, 'version'); integer(r.observed_at_unix_ms, 'observation time');
  protocol(r.protocol);
  requireThat(Array.isArray(r.active_files) && r.active_files.length <= MAX_ACTIONS, 'invalid active file manifest');
  const files: DeltaActiveFile[] = r.active_files.map(value => {
    const file = object(value, 'reference active file');
    exact(file, ['path', 'size', 'num_records', 'partition_values'], 'reference active file');
    return { path: safePath(file.path), size: integer(file.size, 'file size'),
      num_records: file.num_records === null ? null : integer(file.num_records, 'file records'),
      partition_values: partitionValues(file.partition_values) };
  });
  requireThat(new Set(files.map(file => file.path)).size === files.length &&
    same(files, [...files].sort((a, b) => a.path.localeCompare(b.path))), 'active manifest must be unique and sorted');
  for (const [key, value] of Object.entries(totals(files))) requireThat(r[key] === value, 'inconsistent measured file totals');
  const p = object(r.provenance, 'reference provenance');
  exact(p, ['reader', 'metadata_sha256', 'partition_columns', 'checkpoint_version', 'source_objects', 'inventory_sha256',
    'reference_sha256'], 'reference provenance');
  requireThat(p.reader === 'aks-delta-replay-v1' && [p.metadata_sha256, p.inventory_sha256, p.reference_sha256]
    .every(value => typeof value === 'string' && HASH.test(value)), 'invalid replay provenance');
  columns(p.partition_columns);
  const cp = p.checkpoint_version === null ? null : integer(p.checkpoint_version, 'checkpoint version');
  requireThat(cp === null || cp <= Number(r.version), 'checkpoint version exceeds table version');
  requireThat(Array.isArray(p.source_objects) && p.source_objects.length > 0 && p.source_objects.length <= MAX_OBJECTS,
    'invalid source object provenance');
  const names = new Set<string>(), checkpointParts: Array<{ part: number; total: number }> = [];
  for (const value of p.source_objects) {
    const source = object(value, 'source object');
    exact(source, ['name', 'etag', 'bytes', 'sha256'], 'source object');
    const name = safePath(source.name), prefix = bound.table_root + '/_delta_log/';
    requireThat(name.startsWith(prefix) && !names.has(name), 'invalid or duplicate source binding');
    names.add(name);
    const short = name.slice(prefix.length), log = JSON_LOG.exec(short), checkpoint = CHECKPOINT.exec(short);
    requireThat(short === '_last_checkpoint' || log || checkpoint, 'invalid replay source');
    if (log) requireThat(versionOf(log[1]) <= Number(r.version), 'source log exceeds version');
    if (checkpoint) {
      requireThat(cp === versionOf(checkpoint[1]), 'checkpoint source version mismatch');
      checkpointParts.push({ part: checkpoint[2] ? versionOf(checkpoint[2]) : 1, total: checkpoint[3] ? versionOf(checkpoint[3]) : 1 });
    }
    requireThat(typeof source.etag === 'string' && ETAG.test(source.etag) && typeof source.sha256 === 'string' &&
      HASH.test(source.sha256) && integer(source.bytes, 'source bytes') > 0 && Number(source.bytes) <= MAX_OBJECT_BYTES,
    'source object lacks strong provenance');
  }
  if (cp !== null) {
    checkpointParts.sort((a, b) => a.part - b.part);
    requireThat(checkpointParts.length > 0 && checkpointParts.every((part, index) =>
      part.part === index + 1 && part.total === checkpointParts.length), 'missing checkpoint source provenance');
  } else requireThat(checkpointParts.length === 0, 'unexpected checkpoint sources');
  requireThat(Number(r.version) - (cp ?? -1) <= MAX_VERSIONS, 'reference replay range exceeds bound');
  for (let version = cp === null ? 0 : cp + 1; version <= Number(r.version); version++)
    requireThat(names.has(bound.table_root + '/_delta_log/' + versionName(version)), 'reference lacks contiguous source provenance');
  const reference = r as unknown as DeltaReplayReference;
  requireThat(fingerprint(reference) === p.reference_sha256, 'Delta reference provenance digest mismatch');
  return JSON.parse(JSON.stringify(reference)) as DeltaReplayReference;
}

function stateMatches(state: State, reference: DeltaPresentReference): boolean {
  return state.tableId === reference.table_id && state.metadataHash === reference.provenance.metadata_sha256 &&
    same(state.protocol, reference.protocol) && same(state.partitions, reference.provenance.partition_columns) &&
    same([...state.files.values()].sort((a, b) => a.path.localeCompare(b.path)), reference.active_files);
}
function checkCommit(state: State, actions: Json[], policy?: DeltaRetentionPolicy): number {
  const infos = actions.filter(action => action.commitInfo != null);
  requireThat(infos.length === 1, 'commit operation provenance is missing or ambiguous');
  const info = object(infos[0].commitInfo, 'commitInfo');
  const adds = actions.filter(action => action.add != null).map(action => object(action.add, 'add'));
  const removes = actions.filter(action => action.remove != null).map(action => object(action.remove, 'remove'));
  const operation = info.operation;
  for (const action of actions) {
    if (action.metaData != null) {
      const m = metadata(action.metaData);
      if (m.id !== state.tableId) throw new ReplayError('Delta table ID changed', 'failed');
      requireThat(m.fingerprint === state.metadataHash, 'metadata changed during preservation interval');
    }
    if (action.protocol != null) requireThat(same(protocol(action.protocol), state.protocol), 'protocol changed during preservation interval');
  }
  if (removes.some(r => r.dataChange === true)) {
    if (operation !== 'DELETE' || info.engineInfo !== 'arrow-deltalake-maintenance')
      throw new ReplayError('unexplained dataChange=true removal', 'failed');
    requireThat(removes.every(r => r.dataChange === true) && adds.length === 0, 'unsupported PURGE action mixture');
    // Rust persists a retention watermark, but not the configured retention/anchor. Never infer policy from it.
    requireThat(policy, 'PURGE lacks trusted configured retention evidence');
    const retention = integer(policy.retention_ms, 'configured retention');
    requireThat(retention > 0 && ['minute', 'hour', 'day'].includes(policy.granularity), 'invalid retention policy');
    const timestamp = integer(info.timestamp, 'PURGE timestamp');
    const width = { minute: 60_000, hour: 3_600_000, day: 86_400_000 }[policy.granularity];
    const cutoff = Math.floor((timestamp - retention) / width) * width;
    const marks = actions.filter(action => action.txn != null)
      .map(action => object(action.txn, 'txn')).filter(txn => txn.appId === 'arrowcol.purge.before-ms');
    requireThat(marks.length === 1, 'PURGE watermark provenance is missing');
    const watermark = integer(marks[0].version, 'PURGE watermark');
    requireThat(watermark <= cutoff && state.partitions.length === 1 && state.partitions[0] === '__arrowcol_ingestion',
      'PURGE watermark is not explained by configured retention');
    const selected = new Set(removes.map(remove => safePath(remove.path)));
    let rows = 0;
    const partitionEnd = (file: DeltaActiveFile) => {
      const value = file.partition_values.__arrowcol_ingestion;
      const pattern = { minute: /^\d{4}-\d\d-\d\d-\d\d-\d\d$/, hour: /^\d{4}-\d\d-\d\d-\d\d$/, day: /^\d{4}-\d\d-\d\d$/ }[policy.granularity];
      requireThat(typeof value === 'string' && pattern.test(value), 'PURGE partition timestamp is missing');
      const iso = value.slice(0, 10) + 'T' + (policy.granularity === 'day' ? '00' : value.slice(11, 13)) +
        ':' + (policy.granularity === 'minute' ? value.slice(14, 16) : '00') + ':00Z';
      const start = Date.parse(iso);
      requireThat(Number.isSafeInteger(start) && start >= 0 && new Date(start).toISOString() === iso.replace('Z', '.000Z'),
        'PURGE partition timestamp is invalid');
      return start + width;
    };
    const ends = [...state.files.values()].map(partitionEnd), newest = Math.max(...ends);
    for (const file of state.files.values()) {
      const end = partitionEnd(file);
      requireThat(selected.has(file.path) === (end <= watermark), 'PURGE does not remove exactly whole expired partitions');
      if (selected.has(file.path)) {
        requireThat(end < newest && file.num_records !== null, 'PURGE would remove newest partition or lacks row stats');
        rows = sum([rows, file.num_records], 'PURGE rows');
      }
    }
    requireThat(selected.size === removes.length && removes.every(remove => state.files.has(String(remove.path))),
      'PURGE contains duplicate or unknown removals');
    return rows;
  }
  if (removes.length || adds.some(add => add.dataChange === false)) {
    requireThat(operation === 'OPTIMIZE' && info.engineInfo === 'arrow-deltalake-maintenance' &&
      removes.length > 0 && adds.length > 0 && [...removes, ...adds].every(action => action.dataChange === false),
    'unexplained physical maintenance');
    const old = removes.map(remove => state.files.get(safePath(remove.path)));
    requireThat(old.every(file => file && file.num_records !== null), 'OPTIMIZE input row stats are missing');
    const output = adds.map(addFile);
    requireThat(output.every(file => file.num_records !== null), 'OPTIMIZE output row stats are missing');
    if (sum(old.map(file => file!.num_records!), 'OPTIMIZE input rows') !== sum(output.map(file => file.num_records!), 'OPTIMIZE output rows'))
      throw new ReplayError('OPTIMIZE changed measured row count', 'failed');
    return 0;
  }
  requireThat(['INSERT', 'WRITE', 'STREAMING UPDATE'].includes(String(operation)) &&
    adds.every(add => add.dataChange === true), 'commit is not a proven ingestion append');
  if (operation === 'WRITE') {
    const parameters = object(info.operationParameters, 'WRITE operation parameters');
    requireThat(typeof parameters.mode === 'string' && parameters.mode.toLowerCase() === 'append', 'WRITE is not append mode');
  }
  requireThat(adds.map(addFile).every(file => file.num_records !== null), 'ingestion append row stats are missing');
  return 0;
}

function checkCreation(actions: Json[]): void {
  requireThat(actions.filter(action => action.metaData != null).length === 1 &&
    actions.filter(action => action.protocol != null).length === 1 && !actions.some(action => action.remove != null),
  'creation commit lacks unique metadata/protocol or contains removals');
  const infos = actions.filter(action => action.commitInfo != null);
  requireThat(infos.length === 1, 'creation commit operation provenance is missing or ambiguous');
  const info = object(infos[0].commitInfo, 'creation commitInfo');
  requireThat(['CREATE TABLE', 'CREATE TABLE AS SELECT', 'INSERT', 'WRITE'].includes(String(info.operation)),
    'unsupported table creation operation');
  if (info.operation === 'WRITE') {
    const parameters = object(info.operationParameters, 'creation WRITE parameters');
    requireThat(typeof parameters.mode === 'string' && parameters.mode.toLowerCase() === 'append',
      'creation WRITE is not append mode');
  }
  for (const action of actions.filter(action => action.add != null)) {
    const add = object(action.add, 'creation add');
    requireThat(add.dataChange === true && addFile(add).num_records !== null, 'creation add lacks append semantics or row stats');
  }
}

export async function verifyDeltaPreservation(azure: Azure, baseline: DeltaReplayReference, terminal: DeltaReplayReference,
  assertCurrent: () => Promise<void>, retentionPolicy?: DeltaRetentionPolicy): Promise<DeltaPreservationResult> {
  let current: DeltaReplayReference | null = null;
  try {
    baseline = parseDeltaReplayReference(baseline, azure, baseline.table_root);
    terminal = parseDeltaReplayReference(terminal, azure, baseline.table_root);
    if (baseline.observation === 'table' && (terminal.observation === 'observed_absent' ||
      baseline.table_id !== terminal.table_id || terminal.version < baseline.version))
      throw new ReplayError('terminal table ID/version does not extend baseline', 'failed');
    const captured = await capture(azure, baseline.table_root, assertCurrent,
      baseline.observation === 'table' ? baseline.version : -1);
    current = captured.reference;
    if (terminal.observation === 'observed_absent') {
      requireThat(current.observation === 'observed_absent', 'table appeared after an absent terminal reference; terminal preservation is unproven');
      return { status: 'preserved', disposition: 'absence_preserved',
        reason: 'exact planned Delta root remained observed absent; no row-delivery proof',
        current, purge_explained_rows: null };
    }
    if (current.observation === 'observed_absent')
      throw new ReplayError('actual terminal Delta table disappeared', 'failed');
    if (current.table_id !== terminal.table_id || current.version < terminal.version)
      throw new ReplayError('current table ID/version does not extend terminal reference', 'failed');
    for (const reference of [baseline, terminal]) {
      if (reference.observation === 'observed_absent') continue;
      for (const source of reference.provenance.source_objects) {
        const replayed = current.provenance.source_objects.find(entry => entry.name === source.name);
        if (replayed && !source.name.endsWith('/_last_checkpoint') && !same(source, replayed))
          throw new ReplayError('immutable Delta history object changed', 'failed');
      }
    }
    requireThat((baseline.observation === 'observed_absent' || baseline.row_count !== null) &&
      terminal.row_count !== null && current.row_count !== null,
      'reference row stats are missing');
    const state: State = baseline.observation === 'observed_absent' ? freshState() :
      { files: new Map(baseline.active_files.map(file => [file.path, file])),
      tableId: baseline.table_id, metadataHash: baseline.provenance.metadata_sha256,
      partitions: baseline.provenance.partition_columns, protocol: baseline.protocol };
    if (baseline.observation === 'table' && baseline.version === terminal.version && !stateMatches(state, terminal))
      throw new ReplayError('terminal snapshot differs at the same Delta version', 'failed');
    let purged = 0, purgedAfterTerminal = 0;
    for (let version = baseline.observation === 'observed_absent' ? 0 : baseline.version + 1; version <= current.version; version++) {
      const actions = captured.commits.get(version);
      requireThat(actions, 'post-baseline JSON history is missing');
      let removed = 0;
      if (version === 0) checkCreation(actions);
      else removed = checkCommit(state, actions, retentionPolicy);
      purged = sum([purged, removed], 'total PURGE rows');
      if (version > terminal.version) purgedAfterTerminal = sum([purgedAfterTerminal, removed], 'post-terminal PURGE rows');
      apply(state, actions);
      if (version === terminal.version && !stateMatches(state, terminal))
        throw new ReplayError('terminal reference is not reproduced by independent history replay', 'failed');
    }
    if (!stateMatches(state, current)) throw new ReplayError('fresh snapshot disagrees with interval replay', 'failed');
    if (current.row_count < terminal.row_count - purgedAfterTerminal)
      throw new ReplayError('active rows fell below terminal rows minus proven PURGE rows', 'failed');
    return { status: 'preserved', disposition: 'table_preserved', reason: null, current, purge_explained_rows: purged };
  } catch (error) {
    return { status: error instanceof ReplayError ? error.status : 'unknown',
      disposition: null,
      reason: error instanceof ReplayError ? error.message : 'Delta storage replay failed or evidence is malformed',
      current, purge_explained_rows: null };
  }
}
