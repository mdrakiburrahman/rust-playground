import fs from 'node:fs';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';

export const SOURCE_BASE = 'a6a11f01056b57881eaba29d2fb03f2992824cba';
export const BOOTSTRAP_SHA256 = '6a84d79a559ac735f4d3c490a935380b46e56b51cb8a87203b1ca341ce03a42d';
export const NATIVE = Object.freeze({
  subscription: '68fa0545-661c-4152-93f2-fc8ae1c231b8',
  tenant: '72f988bf-86f1-41af-91ab-2d7cd011db47',
  client: 'e23159c9-5be9-427c-9edf-dd08e026ec46',
  object: 'dfc4ebed-abd4-4e4c-8cce-fd442d8bdeeb',
  issuer: 'https://canadacentral.oic.prod-aks.azure.com/72f988bf-86f1-41af-91ab-2d7cd011db47/e899cc8c-6616-4e50-8647-93ef826b2e67/',
  subject: 'system:serviceaccount:arrow-collector:arrow-collector',
  audience: 'api://AzureADTokenExchange',
  namespace: 'arrow-collector',
  serviceAccount: 'arrow-collector',
  account: 'arrowcollector00000',
});
export const SEMANTICS = 'last committed stopped snapshot; not proof all admitted rows flushed';
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const HASH = /^[0-9a-f]{64}$/;
const LOG = /^_delta_log\/(\d{20})\.json$/;
const EMPTY_SHA256 = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
export class Refusal extends Error {}
export function requireThat(value: unknown, message: string): asserts value {
  if (!value) throw new Refusal(message);
}
export function object(value: unknown): Record<string, unknown> {
  requireThat(value !== null && typeof value === 'object' && !Array.isArray(value), 'Expected a mapping.');
  return value as Record<string, unknown>;
}
export function exactKeys(value: Record<string, unknown>, keys: string[]): void {
  requireThat(Object.keys(value).sort().join('\0') === [...keys].sort().join('\0'), 'Unsupported or missing fields.');
}
export const sha256 = (bytes: Uint8Array | string): string => createHash('sha256').update(bytes).digest('hex');
export function integer(value: unknown, minimum: number, maximum = Number.MAX_SAFE_INTEGER): number {
  requireThat(typeof value === 'number' && Number.isSafeInteger(value) && value >= minimum && value <= maximum,
    'Invalid bounded integer.');
  return value;
}
export function text(value: unknown, pattern: RegExp, maximum = 2048): string {
  requireThat(typeof value === 'string' && value.length <= maximum && pattern.test(value), 'Invalid bounded string.');
  return value;
}
export function relativeName(value: unknown): string {
  const name = text(value, /^[A-Za-z0-9._=-]+(?:\/[A-Za-z0-9._=-]+)*$/);
  requireThat(name.split('/').every(part => part !== '.' && part !== '..' && !part.endsWith('.')),
    'Unsafe relative path.');
  return name;
}
export interface Limits { maxFiles: number; maxBytes: number; maxRows: number }
export function limits(raw: unknown): Limits {
  const value = object(raw);
  exactKeys(value, ['maxFiles', 'maxBytes', 'maxRows']);
  return { maxFiles: integer(value.maxFiles, 2, 4096), maxBytes: integer(value.maxBytes, 1, 64 * 1024 * 1024),
    maxRows: integer(value.maxRows, 1, 1_000_000) };
}
export interface FileEntry { path: string; size: number; sha256: string }
export interface Manifest {
  schema_version: 1; run_id: string; table: string; latestVersion: number; rowCount: number;
  schemaString: string; files: FileEntry[];
  writers: Array<{ id: string; service: string; was_running: boolean; exit_code: number; finished_at: string; oom_killed: boolean }>;
  semantics: string;
}
export function manifest(raw: unknown, bound: Limits): Manifest {
  const value = object(raw);
  exactKeys(value, ['schema_version', 'run_id', 'table', 'latestVersion', 'rowCount', 'schemaString', 'files', 'writers', 'semantics']);
  requireThat(value.schema_version === 1 && value.semantics === SEMANTICS, 'Unqualified snapshot contract.');
  const run_id = text(value.run_id, UUID), table = text(value.table, /^[a-z][a-z0-9_]{0,127}$/);
  requireThat(Array.isArray(value.files) && value.files.length >= 2 && value.files.length <= bound.maxFiles,
    'Snapshot file budget exceeded or empty.');
  const files = value.files.map(rawFile => {
    const file = object(rawFile);
    exactKeys(file, ['path', 'size', 'sha256']);
    const filePath = relativeName(file.path), size = integer(file.size, filePath === '.arrow-lake-writer.lock' ? 0 : 1,
      bound.maxBytes), digest = text(file.sha256, HASH);
    requireThat(filePath !== '.arrow-lake-writer.lock' || (size === 0 && digest === EMPTY_SHA256),
      'The known writer-lock sentinel must be an unchanged empty file.');
    return { path: filePath, size, sha256: digest };
  });
  requireThat(new Set(files.map(file => file.path)).size === files.length &&
    new Set(files.map(file => file.path.toLowerCase())).size === files.length, 'Duplicate or case-alias paths.');
  requireThat(files.reduce((sum, file) => sum + file.size, 0) <= bound.maxBytes, 'Snapshot byte budget exceeded.');
  requireThat(Array.isArray(value.writers) && value.writers.length > 0 && value.writers.length <= 32,
    'Stopped-writer evidence absent or unbounded.');
  const writers = value.writers.map(rawWriter => {
    const writer = object(rawWriter);
    exactKeys(writer, ['id', 'service', 'was_running', 'exit_code', 'finished_at', 'oom_killed']);
    const exit_code = integer(writer.exit_code, 0, 255);
    requireThat(typeof writer.was_running === 'boolean' && writer.oom_killed === false &&
      (exit_code === 0 || (exit_code === 143 && writer.was_running)), 'Writer custody shows OOM, unexpected exit or pre-existing failure.');
    const finished_at = text(writer.finished_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z$/);
    requireThat(Number.isFinite(Date.parse(finished_at)) && Date.parse(finished_at) > 0, 'Invalid writer finish timestamp.');
    return { id: text(writer.id, /^[A-Za-z0-9_.:-]{1,256}$/),
      service: text(writer.service, /^[A-Za-z0-9_.-]{1,128}$/), was_running: writer.was_running,
      exit_code, finished_at, oom_killed: false };
  });
  requireThat(new Set(writers.map(writer => writer.id)).size === writers.length, 'Duplicate stopped writer.');
  return { schema_version: 1, run_id, table, latestVersion: integer(value.latestVersion, 0, bound.maxFiles - 2),
    rowCount: integer(value.rowCount, 1, bound.maxRows), schemaString: text(value.schemaString, /^.+$/s, 256 * 1024),
    files, writers, semantics: SEMANTICS };
}
export function readRegular(filename: string, maximum: number, allowEmpty = false): Buffer {
  requireThat(process.platform === 'linux' && typeof fs.constants.O_NOFOLLOW === 'number',
    'Only Linux no-follow reads are qualified.');
  const resolved = path.resolve(filename);
  requireThat(fs.realpathSync(resolved) === resolved, 'Symlinked file or ancestor refused.');
  const entry = fs.lstatSync(resolved);
  const minimum = allowEmpty ? 0 : 1;
  requireThat(entry.isFile() && entry.nlink === 1 && entry.size >= minimum && entry.size <= maximum,
    'Nonregular, hardlinked, empty or oversized file.');
  const fd = fs.openSync(resolved, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const before = fs.fstatSync(fd);
    requireThat(before.isFile() && before.nlink === 1 && before.size >= minimum && before.size <= maximum &&
      before.dev === entry.dev && before.ino === entry.ino,
      'Nonregular, hardlinked, empty or oversized file.');
    const bytes = Buffer.alloc(before.size);
    let offset = 0;
    while (offset < bytes.length) {
      const read = fs.readSync(fd, bytes, offset, bytes.length - offset, offset);
      requireThat(read > 0, 'File shrank during immutable read.');
      offset += read;
    }
    requireThat(fs.readSync(fd, Buffer.alloc(1), 0, 1, bytes.length) === 0, 'File grew during immutable read.');
    const after = fs.fstatSync(fd);
    requireThat(before.dev === after.dev && before.ino === after.ino && before.size === bytes.length &&
      before.size === after.size && before.mtimeMs === after.mtimeMs && before.ctimeMs === after.ctimeMs,
    'File changed during immutable read.');
    return bytes;
  } finally { fs.closeSync(fd); }
}
export function inventory(root: string, maximum: number): string[] {
  requireThat(fs.realpathSync(root) === path.resolve(root) && fs.lstatSync(root).isDirectory(), 'Unsafe table root.');
  const names: string[] = [];
  let entries = 0;
  const visit = (directory: string, prefix: string): void => {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      requireThat(++entries <= maximum * 4, 'Directory budget exceeded.');
      const name = relativeName(prefix + entry.name);
      if (entry.isDirectory()) visit(path.join(directory, entry.name), name + '/');
      else {
        requireThat(entry.isFile(), 'Symlink or nonregular inventory entry.');
        names.push(name);
        requireThat(names.length <= maximum, 'File budget exceeded.');
      }
    }
  };
  visit(root, '');
  return names.sort();
}
export interface Snapshot {
  manifest: Manifest; manifestSha256: string; bytes: Map<string, Buffer>;
  tableId: string; schemaSha256: string; schemaFields: string[]; active: string[]; payloads: string[]; logs: string[];
}
function supportedProtocol(value: Record<string, unknown>): string {
  if (value.minReaderVersion === 1 && value.minWriterVersion === 2) {
    exactKeys(value, ['minReaderVersion', 'minWriterVersion']);
    return '1/2';
  }
  if (value.minReaderVersion === 3 && value.minWriterVersion === 7) {
    exactKeys(value, ['minReaderVersion', 'minWriterVersion', 'readerFeatures', 'writerFeatures']);
    requireThat(Array.isArray(value.readerFeatures) && value.readerFeatures.length === 0 &&
      Array.isArray(value.writerFeatures) && value.writerFeatures.length === 1 &&
      value.writerFeatures[0] === 'invariants', 'Unsupported Delta protocol features.');
    return '3/7:invariants';
  }
  throw new Refusal('Unsupported Delta protocol/features.');
}
function validateCatalogSchemaHistory(configuration: Record<string, unknown>, schema: Record<string, unknown>,
  tableName: string): void {
  const raw = configuration['catalog.schemaHistory'];
  requireThat(typeof raw === 'string' && raw.length <= 256 * 1024, 'Invalid catalog schema history.');
  const history: unknown = JSON.parse(raw);
  requireThat(Array.isArray(history) && history.length === 1, 'Catalog schema evolution is outside this bounded probe.');
  const version = object(history[0]);
  exactKeys(version, ['table_id', 'identity', 'version', 'fields', 'fingerprint', 'canonical_ddl', 'metadata']);
  text(version.table_id, UUID);
  requireThat(version.version === 1 && Array.isArray(version.fields) && version.fields.length > 0 &&
    version.fields.length <= 256, 'Unsupported catalog schema version.');
  const identity = object(version.identity);
  exactKeys(identity, ['catalog', 'schema', 'table']);
  text(identity.catalog, /^[A-Za-z_][A-Za-z0-9_]{0,127}$/);
  text(identity.schema, /^[A-Za-z_][A-Za-z0-9_]{0,127}$/);
  requireThat(text(identity.table, /^[a-z][a-z0-9_]{0,127}$/) === tableName,
    'Catalog schema history names a different table.');
  requireThat(text(version.fingerprint, /^sha256:[0-9a-f]{64}$/) &&
    text(version.canonical_ddl, /^.+$/s, 8192), 'Invalid catalog schema identity.');
  const metadata = object(version.metadata);
  exactKeys(metadata, ['provider', 'partition_columns', 'location', 'comment', 'properties']);
  requireThat(metadata.provider === 'delta' && Array.isArray(metadata.partition_columns) &&
    metadata.partition_columns.length === 0 && typeof metadata.location === 'string' &&
    metadata.location.length > 0 && metadata.location.length <= 2048 &&
    (metadata.comment === null || typeof metadata.comment === 'string') &&
    Object.keys(object(metadata.properties)).length === 0, 'Unsupported catalog table metadata.');

  const deltaFields = object(schema).fields;
  requireThat(Array.isArray(deltaFields), 'Invalid Delta schema fields.');
  const deltaByName = new Map<string, Record<string, unknown>>();
  for (const rawField of deltaFields) {
    const field = object(rawField);
    const name = text(field.name, /^[A-Za-z_][A-Za-z0-9_]{0,127}$/);
    requireThat(!deltaByName.has(name), 'Duplicate Delta schema field.');
    deltaByName.set(name, field);
  }
  const catalogNames = new Set<string>();
  const timestampNames = new Set<string>();
  const ids = new Set<string>();
  for (let index = 0; index < version.fields.length; index++) {
    const field = object(version.fields[index]);
    exactKeys(field, ['id', 'name', 'position', 'field_number', 'nullable', 'partition', 'mapping', 'comment']);
    const id = text(field.id, UUID), name = text(field.name, /^[A-Za-z_][A-Za-z0-9_]{0,127}$/);
    requireThat(!ids.has(id) && !catalogNames.has(name) && field.position === index &&
      field.field_number === index + 1 && typeof field.nullable === 'boolean' &&
      field.partition === null && (field.comment === null || typeof field.comment === 'string'),
      'Invalid or evolved catalog field definition.');
    ids.add(id);
    catalogNames.add(name);
    const mapping = object(field.mapping);
    exactKeys(mapping, ['arrow', 'spark', 'hive', 'protobuf']);
    const arrow = object(mapping.arrow);
    const kind = text(arrow.kind, /^[a-z][a-z0-9]*$/);
    let deltaType: string;
    if (kind === 'utf8') {
      exactKeys(arrow, ['kind']);
      deltaType = 'string';
      requireThat(mapping.spark === 'STRING' && mapping.hive === 'string' && mapping.protobuf === 'string',
        'Unsupported UTF-8 catalog mapping.');
    } else if (kind === 'timestamp') {
      exactKeys(arrow, ['kind', 'unit', 'timezone']);
      deltaType = 'timestamp';
      requireThat(arrow.unit === 'microsecond' && arrow.timezone === 'UTC' &&
        mapping.spark === 'TIMESTAMP' && mapping.hive === 'timestamp' && mapping.protobuf === 'int64',
        'Unsupported timestamp catalog mapping.');
      timestampNames.add(name);
    } else if (kind === 'int16') {
      exactKeys(arrow, ['kind']);
      deltaType = 'short';
      requireThat(mapping.spark === 'SMALLINT' && mapping.hive === 'smallint' && mapping.protobuf === 'int32',
        'Unsupported int16 catalog mapping.');
    } else {
      throw new Refusal('Unsupported catalog Arrow type.');
    }
    const deltaField = deltaByName.get(name);
    requireThat(deltaField !== undefined && deltaField.type === deltaType &&
      deltaField.nullable === field.nullable && Object.keys(object(deltaField.metadata)).length === 0,
      'Catalog and Delta schema fields do not agree.');
  }
  for (const [name, field] of deltaByName) {
    if (catalogNames.has(name)) continue;
    const suffix = '__unix_nanos';
    const source = name.endsWith(suffix) ? name.slice(0, -suffix.length) : '';
    const sourceField = deltaByName.get(source);
    requireThat(sourceField !== undefined && timestampNames.has(source) && field.type === 'long' &&
      field.nullable === sourceField.nullable && Object.keys(object(field.metadata)).length === 0,
      'Unsupported derived Delta schema field.');
  }
  requireThat(deltaByName.size >= catalogNames.size, 'Invalid catalog-to-Delta schema mapping.');
}
export function replay(m: Manifest, bytes: Map<string, Buffer>): Omit<Snapshot, 'manifest' | 'manifestSha256' | 'bytes'> {
  const logs = m.files.map(file => file.path).filter(name => LOG.test(name)).sort();
  requireThat(logs.length === m.latestVersion + 1 && logs.every((name, index) =>
    name === `_delta_log/${String(index).padStart(20, '0')}.json`), 'Missing/noncontiguous JSON history.');
  requireThat(m.files.every(file => LOG.test(file.path) ||
    (!file.path.startsWith('_delta_log/') && file.path.endsWith('.parquet')) ||
    file.path === '.arrow-lake-writer.lock'), 'Unsupported checkpoint/sidecar/extra file.');
  const allAdds = new Map<string, number>(), active = new Map<string, number>();
  const fileSizes = new Map(m.files.map(file => [file.path, file.size]));
  let tableId = '', schemaString = '', schemaFields: string[] = [], protocolSeen = false, metadataSeen = false;
  let protocolIdentity: string | null = null, configurationIdentity: string | null = null;
  for (const name of logs) {
    const raw = bytes.get(name);
    requireThat(raw, 'Log bytes absent.');
    const lines = raw.toString('utf8').trimEnd().split('\n');
    requireThat(lines.length > 0 && lines.length <= 100_000, 'Unbounded/empty Delta log.');
    const actions = lines.map(line => {
      const action = object(JSON.parse(line));
      requireThat(Object.keys(action).length === 1, 'Expected one Delta action per line.');
      const kind = Object.keys(action)[0]!;
      const value = object(action[kind]);
      return { kind, value };
    });
    requireThat(actions.filter(action => action.kind === 'protocol').length <= 1 &&
      actions.filter(action => action.kind === 'metaData').length <= 1, 'Duplicate protocol/metadata in one commit.');
    // Delta actions within one commit are not ordered: resolve protocol/metadata before its file actions.
    const ordered = [...actions.filter(action => action.kind === 'protocol'),
      ...actions.filter(action => action.kind === 'metaData'),
      ...actions.filter(action => action.kind !== 'protocol' && action.kind !== 'metaData')];
    const touched = new Set<string>();
    for (const { kind, value } of ordered) {
      if (kind === 'protocol') {
      const nextProtocol = supportedProtocol(value);
      requireThat(protocolIdentity === null || protocolIdentity === nextProtocol,
        'Delta protocol changes are outside this bounded probe.');
      protocolIdentity = nextProtocol;
      protocolSeen = true;
    } else if (kind === 'metaData') {
        requireThat(Array.isArray(value.partitionColumns) && value.partitionColumns.length === 0,
          'Partitioned table is outside this bounded probe.');
        const format = object(value.format), configuration = object(value.configuration);
        exactKeys(format, ['provider', 'options']);
        requireThat(format.provider === 'parquet' && Object.keys(object(format.options)).length === 0 &&
          Object.keys(configuration).every(key =>
            ['delta.appendOnly', 'delta.checkpointInterval', 'catalog.schemaHistory'].includes(key)) &&
          (configuration['delta.appendOnly'] === undefined || ['true', 'false'].includes(String(configuration['delta.appendOnly']))) &&
          (configuration['delta.checkpointInterval'] === undefined ||
            /^\d+$/.test(String(configuration['delta.checkpointInterval']))) &&
          (configuration['catalog.schemaHistory'] === undefined ||
            (typeof configuration['catalog.schemaHistory'] === 'string' &&
              configuration['catalog.schemaHistory'].length <= 256 * 1024)),
        'Unsupported format/options/table configuration.');
        requireThat(protocolIdentity !== '3/7:invariants' || configuration['catalog.schemaHistory'] !== undefined,
          'ArrowCol DFE protocol requires catalog schema history.');
        const nextId = text(value.id, UUID), nextSchema = text(value.schemaString, /^.+$/s, 256 * 1024);
        const schema = object(JSON.parse(nextSchema));
        requireThat(schema.type === 'struct' && Array.isArray(schema.fields) && schema.fields.length > 0,
          'Invalid Delta schema.');
        const nextFields = schema.fields.map(rawField => {
          const field = object(rawField);
          requireThat(typeof field.nullable === 'boolean' &&
            typeof field.type === 'string' &&
            /^(string|long|integer|short|byte|float|double|boolean|binary|date|timestamp)$/.test(field.type) &&
            Object.keys(object(field.metadata)).length === 0, 'Unsupported nested/mapped schema field.');
          return text(field.name, /^[A-Za-z_][A-Za-z0-9_]{0,127}$/);
        });
        requireThat(new Set(nextFields).size === nextFields.length, 'Duplicate schema field.');
        if (configuration['catalog.schemaHistory'] !== undefined)
          validateCatalogSchemaHistory(configuration, schema, m.table);
        const nextConfigurationIdentity = JSON.stringify(Object.fromEntries(
          Object.keys(configuration).sort().map(key => [key, configuration[key]])));
        requireThat(!metadataSeen || (tableId === nextId && schemaString === nextSchema &&
          configurationIdentity === nextConfigurationIdentity),
        'Schema/table/configuration evolution is outside this copy probe.');
        configurationIdentity = nextConfigurationIdentity;
        tableId = nextId; schemaString = nextSchema; schemaFields = nextFields; metadataSeen = true;
      } else if (kind === 'add' || kind === 'remove') {
        requireThat(protocolSeen && metadataSeen, 'File action precedes protocol/metadata.');
        requireThat(value.deletionVector === undefined || value.deletionVector === null, 'Deletion vector unsupported.');
        const file = relativeName(value.path);
        requireThat(!file.startsWith('_delta_log/') && file.endsWith('.parquet'), 'External or unsupported file reference.');
        requireThat(!touched.has(file), 'Multiple actions for one path in a commit are outside this bounded probe.');
        touched.add(file);
        if (kind === 'add') {
          const size = integer(value.size, 1);
          requireThat(Object.keys(object(value.partitionValues)).length === 0 && fileSizes.get(file) === size,
            'Missing or size-mismatched historical Add payload.');
          requireThat(!allAdds.has(file), 'Reused Add path cannot prove immutable historical bytes.');
          allAdds.set(file, size); active.set(file, size);
        } else {
          requireThat(allAdds.has(file) && active.has(file), 'Remove without a known active Add.');
          active.delete(file);
        }
      } else {
        requireThat(kind === 'commitInfo' || kind === 'txn', 'Unsupported Delta action/dependency.');
      }
    }
    requireThat(protocolSeen && metadataSeen, 'Version lacks initial protocol/metadata.');
  }
  const payloads = [...allAdds.keys()].sort();
  const writerLockCount = m.files.filter(file => file.path === '.arrow-lake-writer.lock').length;
  requireThat(m.files.length === logs.length + payloads.length + writerLockCount,
    'Unrelated/unreferenced payload in snapshot.');
  requireThat(active.size > 0, 'Latest snapshot has no active payload.');
  requireThat(schemaString === m.schemaString, 'Manifest schemaString differs from unchanged latest Delta metadata.');
  return { tableId, schemaSha256: sha256(schemaString), schemaFields, active: [...active.keys()].sort(), payloads, logs };
}
export function readSnapshot(manifestFile: string, expectedSha: string, tableRoot: string, bound: Limits,
  expectedRun: string, expectedTable: string): Snapshot {
  text(expectedSha, HASH);
  const raw = readRegular(manifestFile, 2 * 1024 * 1024);
  requireThat(sha256(raw) === expectedSha, 'Manifest digest mismatch.');
  const m = manifest(JSON.parse(raw.toString('utf8')), bound);
  requireThat(m.run_id === expectedRun && m.table === expectedTable, 'Wrong approved snapshot selection.');
  const root = path.resolve(tableRoot);
  requireThat(!path.resolve(manifestFile).startsWith(root + path.sep), 'Manifest must remain outside replay root.');
  const wanted = m.files.map(file => file.path).sort();
  requireThat(JSON.stringify(inventory(root, bound.maxFiles)) === JSON.stringify(wanted), 'Snapshot inventory mismatch.');
  const bytes = new Map<string, Buffer>();
  for (const file of m.files) {
    const content = readRegular(path.join(root, ...file.path.split('/')), bound.maxBytes,
      file.path === '.arrow-lake-writer.lock');
    requireThat(content.length === file.size && sha256(content) === file.sha256, 'Payload/log hash mismatch.');
    bytes.set(file.path, content);
  }
  requireThat(JSON.stringify(inventory(root, bound.maxFiles)) === JSON.stringify(wanted), 'Snapshot changed during read.');
  return { manifest: m, manifestSha256: expectedSha, bytes, ...replay(m, bytes) };
}
export interface RowProof { rows: number; fields: string[] }
export type Decode = (bytes: Buffer) => RowProof;
export function proveRows(snapshot: Snapshot, decode: Decode, bound: Limits): number {
  let rows = 0;
  for (const name of snapshot.active) {
    const proof = decode(snapshot.bytes.get(name)!);
    requireThat(JSON.stringify(proof.fields) === JSON.stringify(snapshot.schemaFields), 'Decoded Parquet columns differ from Delta schema.');
    rows += integer(proof.rows, 0, bound.maxRows);
    requireThat(rows <= bound.maxRows, 'Decoded row budget exceeded.');
  }
  requireThat(rows > 0 && rows === snapshot.manifest.rowCount, 'Actual latest decoded row count is empty or mismatched.');
  return rows;
}
function claims(token: string): Record<string, unknown> {
  requireThat(token.length > 0 && token.length <= 32768 && token.split('.').length === 3, 'Invalid identity token envelope.');
  return object(JSON.parse(Buffer.from(token.split('.')[1]!, 'base64url').toString('utf8')));
}
function liveClaims(value: Record<string, unknown>, now: number): void {
  requireThat(typeof value.exp === 'number' && value.exp > now + 30 &&
    (value.nbf === undefined || (typeof value.nbf === 'number' && value.nbf <= now)), 'Identity token is expired or not yet valid.');
}
export function checkFederation(env: NodeJS.ProcessEnv, assertion: string, expectedTokenFile: string,
  now = Math.floor(Date.now() / 1000)): void {
  requireThat(path.isAbsolute(expectedTokenFile) && env.AZURE_CLIENT_ID === NATIVE.client &&
    env.AZURE_TENANT_ID === NATIVE.tenant && env.AZURE_FEDERATED_TOKEN_FILE === expectedTokenFile,
  'All three native federation inputs must match; operator CLI fallback refused.');
  const value = claims(assertion);
  const audience = typeof value.aud === 'string' ? [value.aud] : value.aud;
  requireThat(value.iss === NATIVE.issuer && value.sub === NATIVE.subject &&
    Array.isArray(audience) && audience.length === 1 && audience[0] === NATIVE.audience, 'Wrong projected token binding.');
  liveClaims(value, now);
}
export function checkAccess(token: string, now = Math.floor(Date.now() / 1000)): void {
  const value = claims(token);
  requireThat(value.tid === NATIVE.tenant && value.oid === NATIVE.object &&
    (value.appid === NATIVE.client || value.azp === NATIVE.client) &&
    ['https://storage.azure.com', 'https://storage.azure.com/'].includes(String(value.aud)) &&
    [ `https://sts.windows.net/${NATIVE.tenant}/`, `https://login.microsoftonline.com/${NATIVE.tenant}/v2.0` ].includes(String(value.iss)),
  'Exchanged token does not describe the expected native storage identity.');
  liveClaims(value, now);
}
export interface Receipt { status: number; bodyBase64: string; headers: Record<string, string> }
export interface Transport {
  list(): Promise<Array<{ name: string; etag: string }>>;
  put(name: string, bytes: Buffer, requestId: string, capture: (receipt: Receipt) => void): Promise<string>;
  get(name: string, etag: string, capture: (receipt: Receipt) => void): Promise<Buffer>;
}
export type RecordEvidence = (value: Record<string, unknown>) => void;
export interface Destination { owner: string; prefix: string; container: string }
export function destination(raw: unknown): Destination {
  const value = object(raw);
  exactKeys(value, ['owner', 'prefix', 'container']);
  const owner = text(value.owner, UUID), prefix = relativeName(value.prefix);
  requireThat(prefix.endsWith('/' + owner) && !prefix.includes('.'), 'Fresh prefix must end in the exact new owner UUID.');
  const container = text(value.container, /^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$/);
  requireThat(!container.includes('--'), 'Invalid container name.');
  return { owner, prefix, container };
}
export async function copySnapshot(snapshot: Snapshot, target: Destination, transport: Transport, decode: Decode,
  bound: Limits, record: RecordEvidence, recheckSource: () => void): Promise<void> {
  const rows = proveRows(snapshot, decode, bound);
  recheckSource();
  requireThat((await transport.list()).length === 0, 'Destination prefix collision; no adoption.');
  const ownerName = target.prefix + '/_copy-owner.json';
  const ownerBytes = Buffer.from(JSON.stringify({ kind: 'private-copy-intent-not-ready', owner: target.owner,
    manifestSha256: snapshot.manifestSha256, sourceRun: snapshot.manifest.run_id, sourceTable: snapshot.manifest.table }));
  const expected = new Map<string, { bytes: Buffer; etag: string }>();
  const create = async (name: string, bytes: Buffer): Promise<void> => {
    const requestId = randomUUID();
    record({ event: 'put-intent', name, requestId, size: bytes.length, sha256: sha256(bytes) });
    const received: { receipt?: Receipt } = {};
    const etag = await transport.put(name, bytes, requestId, receipt => {
      requireThat(!received.receipt, 'Duplicate PUT receipt.');
      received.receipt = receipt;
      record({ event: 'put-original-receipt', name, requestId, receipt });
    });
    const response = received.receipt;
    requireThat(response?.status === 201 && response.headers.etag === etag && etag.length > 0,
      'PUT lacks definitive matching 201/ETag; preserve uncertain prefix.');
    const actual = await transport.get(name, etag, receipt => record({ event: 'get-original-receipt', name, receipt }));
    requireThat(actual.length === bytes.length && sha256(actual) === sha256(bytes), 'Conditional readback hash mismatch.');
    expected.set(name, { bytes, etag });
    record({ event: 'object-verified', name, etag, sha256: sha256(actual) });
  };
  await create(ownerName, ownerBytes);
  const writerLocks = snapshot.manifest.files.filter(file => file.path === '.arrow-lake-writer.lock')
    .map(file => file.path);
  for (const name of [...snapshot.payloads, ...writerLocks, ...snapshot.logs])
    await create(target.prefix + '/table/' + name, snapshot.bytes.get(name)!);
  recheckSource();
  const verifyInventory = async (): Promise<void> => {
    const actual = await transport.list();
    requireThat(actual.length === expected.size && new Set(actual.map(file => file.name)).size === expected.size &&
      actual.every(file => expected.get(file.name)?.etag === file.etag), 'Final remote inventory/ETag mismatch.');
  };
  await verifyInventory();
  const remoteBytes = new Map<string, Buffer>();
  for (const [name, file] of expected) {
    const actual = await transport.get(name, file.etag, receipt => record({ event: 'final-get-original-receipt', name, receipt }));
    requireThat(actual.length === file.bytes.length && sha256(actual) === sha256(file.bytes), 'Final remote byte mismatch.');
    if (name !== ownerName) remoteBytes.set(name.slice((target.prefix + '/table/').length), actual);
  }
  const remote: Snapshot = { ...snapshot, bytes: remoteBytes, ...replay(snapshot.manifest, remoteBytes) };
  requireThat(proveRows(remote, decode, bound) === rows && remote.schemaSha256 === snapshot.schemaSha256 &&
    remote.tableId === snapshot.tableId, 'Remote latest snapshot proof mismatch.');
  await verifyInventory();
  recheckSource();
  record({ event: 'copy-verified-not-published', manifestSha256: snapshot.manifestSha256, owner: target.owner,
    prefix: target.prefix, container: target.container, latestVersion: snapshot.manifest.latestVersion,
    rows, tableId: snapshot.tableId, schemaSha256: snapshot.schemaSha256, sourceSemantics: SEMANTICS,
    readyMarkerCreated: false, adxAttempted: false, qapAttempted: false });
}
