import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { type Azure, type HttpReceipt, Failure, NotReady, canonical_json, publish_private, reset_lock, uuid, owned, runtime } from './bootstrap.js';
import { read_run_fic_record, parse_run_fic_record, verify_run_fic, type RunFicRecord } from './run_fic.js';

export interface OatManifest {
  schema_version: 1;
  run_id: string;
  database: string;
  tables: { external_table: string; relative_path: string }[];
}
export interface OatSnapshot {
  kind: 'delta';
  name: string;
  connectionStrings: string[];
  docString: string;
  schema: unknown;
  qapPolicy: unknown | null;
  /** Full normalized provider definition, excluding volatile telemetry, never omitting ownership-relevant properties. */
  definition: unknown;
}
export type OatObservation =
  | { kind: 'absent'; evidence: string }
  | { kind: 'regular'; evidence: string }
  | { kind: 'external'; snapshot: OatSnapshot; evidence: string };
interface OatQapOperations {
  /** Serialize cooperating OAT writers across hosts; arbitrary Kusto/admin writers are NOT fenced by Blob custody. */
  exclusive<T>(runId: string, work: () => Promise<T>): Promise<T>;
  assertHeld(runId: string): Promise<void>;
  /** Positive owning-workload stop receipt; absence from a workload listing alone is not proof. */
  assertWorkloadStopped(runId: string): Promise<void>;
  /** Inspect regular-name collisions too; malformed/unavailable provider results must throw, not return absent. */
  inspect(database: string, name: string): Promise<OatObservation>;
  /** Authenticate configured cluster/storage and existing experiment database/UAMI policy, without mutations. */
  assertEnvironment(): Promise<void>;
  /** Validate a real protocol/metaData log against the current committed Delta state; no fabricated schema. */
  validateDelta(storagePath: string, initialLog: Buffer): Promise<void>;
  /** Actual successful management receipt, not response presence/empty-array inference. */
  requireReceipt(command: 'create' | 'policy' | 'drop', name: string, response: unknown): string;
}
function object(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure(`${name} must be an object.`);
  return value as Record<string, unknown>;
}
function text(value: unknown, name: string): string {
  if (typeof value !== 'string' || !value.length) throw new Failure(`${name} must be a nonempty string.`);
  return value;
}
function exactKeys(value: Record<string, unknown>, keys: string[], name: string): void {
  if (Object.keys(value).sort().join(',') !== [...keys].sort().join(',')) throw new Failure(`${name} has missing or unexpected fields.`);
}
const digest = (value: unknown): string => createHash('sha256').update(canonical_json(value)).digest('hex');
const parse = (bytes: string | Buffer, name: string): unknown => {
  try { return JSON.parse(bytes.toString()); } catch (cause) { throw new Failure(`${name} is malformed.`, { cause }); }
};
export function managementRows(value: unknown): Record<string, unknown>[] {
  const result = object(value, 'Kusto result');
  if (result.error || result.HasErrors || (Array.isArray(result.Exceptions) ? result.Exceptions.length : result.Exceptions))
    throw new Failure('Kusto reported management errors; response is not success.');
  if (!Array.isArray(result.Tables)) throw new Failure('Kusto result has no typed tables.');
  const primary = result.Tables.filter(table => {
    const candidate = object(table, 'Kusto result table');
    return candidate.TableKind === undefined || candidate.TableKind === 'PrimaryResult';
  });
  if (primary.length !== 1) throw new Failure('Expected one Kusto primary result table.');
  const table = object(primary[0], 'Kusto primary result');
  if (!Array.isArray(table.Columns) || !Array.isArray(table.Rows)) throw new Failure('Kusto columns/rows missing.');
  const columns = table.Columns.map(column => {
    const item = object(column, 'Kusto column');
    text(item.ColumnType, 'ColumnType');
    return text(item.ColumnName, 'ColumnName');
  });
  if (new Set(columns).size !== columns.length || !columns.length) throw new Failure('Duplicate/empty Kusto columns.');
  return table.Rows.map(row => {
    if (!Array.isArray(row) || row.length !== columns.length) throw new Failure('Kusto row width mismatch.');
    return Object.fromEntries(columns.map((name, index) => [name, row[index]]));
  });
}
const qualifiedDeltaProperties = {
  AutoUpdateSchema: false,
  Compressed: false,
  CompressionType: null,
  Encoding: null,
  FileExtension: null,
  Format: 'Parquet',
  IncludeHeaders: 'None',
  NamePrefix: null,
  Shortcut: false,
};
const managedIdentityConnection = /^abfss:\/\/[^/@;]+@[a-z0-9]{3,24}\.dfs\.core\.windows\.net\/[A-Za-z0-9/_-]+;managed_identity=[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}$/i;
function deltaProperties(value: unknown): Record<string, unknown> {
  if (typeof value !== 'string') throw new Failure('Qualified Delta Properties must be the observed JSON string shape.');
  const parsed = object(jsonField(value, 'External properties'), 'External properties');
  if (canonical_json(parsed) !== canonical_json(qualifiedDeltaProperties))
    throw new Failure('Delta Properties differ from the qualified provider shape.');
  return parsed;
}
function qualifiedReceiptProperties(value: unknown): boolean {
  try { deltaProperties(value); return true; } catch (error) {
    if (error instanceof Failure) return false;
    throw error;
  }
}
export function parseQualifiedDeltaDefinition(value: unknown, expectedName: string): {
  connectionStrings: string[];
  docString: string;
  properties: Record<string, unknown>;
  definition: Record<string, unknown>;
} {
  const definition = object(value, 'External definition');
  if (definition.TableName !== expectedName || definition.TableType !== 'Delta')
    throw new Failure('External table name/type differs from the qualified Delta shape.');
  const properties = deltaProperties(definition.Properties);
  if (!Array.isArray(definition.ConnectionStrings) || definition.ConnectionStrings.length !== 1 ||
      typeof definition.ConnectionStrings[0] !== 'string' || !managedIdentityConnection.test(definition.ConnectionStrings[0]))
    throw new Failure('Delta ConnectionStrings differ from the qualified managed-identity shape.');
  return { connectionStrings: definition.ConnectionStrings as string[],
    docString: text(definition.DocString, 'External owner docString'), properties, definition };
}
export function validateExternalMutationReceipt(operation: 'create' | 'drop', name: string, owner: string,
  value: unknown): Record<string, unknown> {
  const receipt = object(value, 'External mutation receipt');
  if (operation === 'create') {
    if (receipt.TableName !== name || receipt.TableType !== 'Delta' || receipt.DocString !== owner)
      throw new Failure('Create receipt differs from the qualified Delta owner shape.');
    deltaProperties(receipt.Properties);
  } else if (receipt.TableName !== name || receipt.TableType !== null ||
      receipt.DocString !== null || receipt.Properties !== null) {
    throw new Failure('Drop receipt is not the qualified matching-name null tombstone.');
  }
  return receipt;
}
export function assertExactConnectionStrings(actual: unknown, expected: string): string[] {
  if (!Array.isArray(actual) || actual.length !== 1 || actual[0] !== expected)
    throw new Failure('External connection does not match the exact owned run URI and UAMI principal.');
  return [expected];
}
export interface KustoQueryResult {
  rows: Record<string, unknown>[];
  query_completed: true;
  query_acceleration_not_applied: boolean;
  query_acceleration_applied: false | null;
  accelerated_read_proven: false;
}
const QAP_CATALOG_UNINITIALIZED_WARNING = -2133196383;
export function parseKustoQueryResult(value: unknown): KustoQueryResult {
  const response = object(value, 'Kusto query result');
  if (response.error || response.HasErrors || (Array.isArray(response.Exceptions) ? response.Exceptions.length > 0 : Boolean(response.Exceptions)))
    throw new Failure('Kusto query response reports failure.');
  if (!Array.isArray(response.Tables) || response.Tables.length < 3) throw new Failure('Kusto query tables are missing.');
  const tables = response.Tables.map((raw, index) => {
    const table = object(raw, 'Kusto query table');
    if (table.TableName !== `Table_${index}` || !Array.isArray(table.Columns) || !Array.isArray(table.Rows))
      throw new Failure('Kusto query table shape is invalid.');
    const columns = table.Columns.map(column => {
      const item = object(column, 'Kusto query column');
      return { name: text(item.ColumnName, 'Query ColumnName'), type: text(item.ColumnType, 'Query ColumnType') };
    });
    if (!columns.length || new Set(columns.map(column => column.name)).size !== columns.length ||
        table.Rows.some(row => !Array.isArray(row) || row.length !== columns.length))
      throw new Failure('Kusto query columns or rows are malformed.');
    return { name: table.TableName, columns, rows: table.Rows as unknown[][] };
  });
  const tocCandidates = tables.filter(table => ['Ordinal', 'Kind', 'Name', 'Id', 'PrettyName'].every(name =>
    table.columns.some(column => column.name === name)));
  if (tocCandidates.length !== 1) throw new Failure('Kusto query table of contents is missing or ambiguous.');
  const toc = tocCandidates[0];
  const tocColumnTypes: Record<string, string> = {
    Ordinal: 'long', Kind: 'string', Name: 'string', Id: 'string', PrettyName: 'string',
  };
  if (toc.columns.length !== Object.keys(tocColumnTypes).length ||
      Object.entries(tocColumnTypes).some(([name, type]) =>
        toc.columns.find(column => column.name === name)?.type !== type))
    throw new Failure('Kusto query table of contents has unexpected column types or fields.');
  const indexes = Object.fromEntries(['Ordinal', 'Kind', 'Name', 'Id', 'PrettyName'].map(name =>
    [name, toc.columns.findIndex(column => column.name === name)]));
  const ordinals = new Set<number>();
  const entries = toc.rows.map(row => {
    const ordinal = row[indexes.Ordinal], kind = row[indexes.Kind], name = row[indexes.Name];
    const id = row[indexes.Id], prettyName = row[indexes.PrettyName];
    if (typeof ordinal !== 'number' || !Number.isInteger(ordinal) || ordinal < 0 || ordinal >= tables.length ||
        typeof kind !== 'string' || typeof name !== 'string' || typeof id !== 'string' || !id.length ||
        typeof prettyName !== 'string' || ordinals.has(ordinal) ||
        tables[ordinal].name !== `Table_${ordinal}`)
      throw new Failure('Kusto query table of contents is malformed.');
    ordinals.add(ordinal);
    return { ordinal, kind, name, id, prettyName };
  });
  if (entries.length !== tables.length - 1 || ordinals.has(tables.indexOf(toc)))
    throw new Failure('Kusto query table of contents does not map every result table.');
  const primaryEntries = entries.filter(entry => entry.kind === 'QueryResult' && entry.name === 'PrimaryResult');
  const statusEntries = entries.filter(entry => entry.kind === 'QueryStatus' && entry.name === 'QueryStatus');
  const propertyEntries = entries.filter(entry => entry.kind === 'QueryProperties' && entry.name === '@ExtendedProperties');
  if (primaryEntries.length !== 1 || statusEntries.length !== 1 || propertyEntries.length !== 1)
    throw new Failure('Kusto query result/status/property tables are incomplete or ambiguous.');
  const status = tables[statusEntries[0].ordinal];
  const columnIndex = (name: string) => status.columns.findIndex(column => column.name === name);
  const severityIndex = columnIndex('Severity'), severityNameIndex = columnIndex('SeverityName'), statusCodeIndex = columnIndex('StatusCode');
  if (severityIndex < 0 || severityNameIndex < 0 || statusCodeIndex < 0 || !status.rows.length)
    throw new Failure('Kusto QueryStatus is missing required fields or rows.');
  let accelerationNotApplied = false, completed = false;
  for (const row of status.rows) {
    const severity = row[severityIndex], severityName = row[severityNameIndex], statusCode = row[statusCodeIndex];
    if (typeof severity !== 'number' || !Number.isInteger(severity) ||
        typeof severityName !== 'string' || typeof statusCode !== 'number' || !Number.isInteger(statusCode))
      throw new Failure('Kusto QueryStatus contains malformed severity/status values.');
    if (severity <= 2) throw new Failure('Kusto QueryStatus reports a partial query failure.');
    if (severity === 4 && severityName === 'Info' && statusCode === 0) {
      completed = true;
      continue;
    }
    if (severity === 6 && severityName === 'Stats' && statusCode === 0) continue;
    if (severity === 3 && severityName === 'Warning' && statusCode === QAP_CATALOG_UNINITIALIZED_WARNING) {
      accelerationNotApplied = true;
      continue;
    }
    throw new Failure('Kusto QueryStatus contains an unknown or unsuccessful status.');
  }
  if (!completed) throw new Failure('Kusto QueryStatus has no successful completion record.');
  const primary = tables[primaryEntries[0].ordinal];
  return { rows: primary.rows.map(row => Object.fromEntries(primary.columns.map((column, index) => [column.name, row[index]]))),
    query_completed: true, query_acceleration_not_applied: accelerationNotApplied,
    query_acceleration_applied: accelerationNotApplied ? false : null, accelerated_read_proven: false };
}
const jsonField = (value: unknown, label: string): unknown =>
  typeof value === 'string' ? parse(value, label) : value;
function policyMatches(value: unknown, expected: unknown | null): boolean {
  if (expected === null) return value === null;
  const actual = object(value, 'QAP policy'), desired = object(expected, 'expected QAP policy');
  return Object.entries(desired).every(([key, item]) => canonical_json(actual[key]) === canonical_json(item));
}
function privateJson(filename: string): unknown {
  const stat = fs.lstatSync(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) || stat.size > 4_194_304)
    throw new Failure('QAP custody/input must be a bounded private regular file.');
  return parse(fs.readFileSync(filename), 'QAP private input');
}
function operations(azure: Azure, filename: string, stopPath?: string): OatQapOperations {
  let activeRun: string | undefined;
  let lease: { id: string; stage: 'acquire-intent' | 'held' | 'release-intent' | 'released'; run_id: string;
    acquireReceipt?: Record<string, string>; releaseReceipt?: Record<string, string>;
    originalAcquire?: HttpReceipt; originalRelease?: HttpReceipt } | undefined;
  let shared: { value: unknown; etag: string } | undefined;
  const ledgerPath = filename + '.lease';
  const markerPath = (runId: string) => `${prefixFor(azure)}/${runId}/_arrowcol-oat-run.json`;
  const sharedPath = (runId: string) => `${prefixFor(azure)}/${runId}/_arrowcol-oat-qap-custody.json`;
  const saveLease = (stage: NonNullable<typeof lease>['stage']) => {
    if (!lease) throw new Failure('Missing lease identity.');
    const next = { ...lease, stage };
    publish_private(ledgerPath, next, fs.existsSync(ledgerPath) ? lease : undefined);
    lease = next;
  };
  async function publishShared(runId: string): Promise<void> {
    if (!lease) throw new Failure('No durable proposed lease identity.');
    if (shared) {
      const [current, currentHeaders] = await azure.blob('GET', azure.c.data, sharedPath(runId), undefined, undefined, {}, false, true);
      if (current === null || currentHeaders.etag !== shared.etag ||
          canonical_json(parse(current, 'Current custody snapshot')) !== canonical_json(shared.value))
        throw new Failure('Custody version changed; root ETag alone is not custody authority.');
      if (lease.stage === 'held' || (lease.stage === 'release-intent' && !lease.releaseReceipt)) {
        const [, rootHeaders] = await azure.blob('HEAD', azure.c.data, markerPath(runId), undefined, undefined,
          { 'x-ms-lease-id': lease.id }, false, true);
        if (rootHeaders['x-ms-lease-status'] !== 'locked' || rootHeaders['x-ms-lease-state'] !== 'leased')
          throw new Failure('Root lease lost before changing the revalidated custody version.');
      }
    }
    const record = readRecord(azure, filename);
    const value = { schema_version: 1, owner: record.rootMarker.value, lease, custody: record };
    const receiptPath = filename + '.metadata-receipt';
    const previous = fs.existsSync(receiptPath) ? privateJson(receiptPath) : undefined;
    let evidence: { id: string; previous: unknown; stage: 'intent' | 'response' | 'ambiguous' | 'acknowledged';
      value_sha256: string; original: HttpReceipt | null } = {
      id: uuid(), previous: previous ?? null, stage: 'intent', value_sha256: digest(value), original: null
    };
    publish_private(receiptPath, evidence, previous);
    const capture = (original: HttpReceipt) => {
      const next = { ...evidence, original, stage: 'response' as const };
      publish_private(receiptPath, next, evidence); evidence = next;
    };
    let headers: Record<string, string>;
    try {
      [, headers] = await azure.blob('PUT', azure.c.data, sharedPath(runId), undefined, canonical_json(value),
        { 'x-ms-blob-type': 'BlockBlob', ...(shared ? { 'If-Match': shared.etag } : { 'If-None-Match': '*' }) }, false, false, capture);
    } catch (cause) {
      const next = { ...evidence, stage: 'ambiguous' as const };
      publish_private(receiptPath, next, evidence);
      throw new Failure('Shared metadata outcome is ambiguous; original captured receipt retained privately, never replay.', { cause });
    }
    if (headers[':status'] !== '201' || !headers.etag) throw new Failure('Shared custody metadata ACK unavailable; never replay.');
    if (evidence.original?.status !== 201) throw new Failure('Shared metadata original receipt unavailable; no completion claim.');
    const acknowledged = { ...evidence, stage: 'acknowledged' as const };
    publish_private(receiptPath, acknowledged, evidence);
    const [readback, readHeaders] = await azure.blob('GET', azure.c.data, sharedPath(runId), undefined, undefined, {}, false, true);
    if (readback === null || canonical_json(parse(readback, 'Shared custody ledger')) !== canonical_json(value) ||
        readHeaders.etag !== headers.etag) throw new Failure('Shared custody metadata postcondition uncertain.');
    shared = { value, etag: headers.etag };
  }
  async function assertHeld(runId: string): Promise<void> {
    if (activeRun !== runId) throw new Failure('No matching cooperating-writer custody scope.');
    const [marker, markerHeaders] = await azure.blob('GET', azure.c.data, markerPath(runId), undefined, undefined, {}, true, true);
    // Before conditional marker initialization, there is no provider mutation except create-if-absent.
    if (marker === null) {
      if (fs.existsSync(filename)) {
        const record = readRecord(azure, filename);
        if (record.intent?.operation !== 'marker') throw new Failure('Owned marker disappeared.');
      }
      return;
    }
    const record = readRecord(azure, filename);
    if (canonical_json(parse(marker, 'root marker')) !== canonical_json(record.rootMarker.value))
      throw new Failure('Cannot lease an unowned marker.');
    if (!lease) {
      const [sharedBytes, sharedHeaders] = await azure.blob('GET', azure.c.data, sharedPath(runId), undefined, undefined, {}, true, true);
      if (sharedBytes !== null) {
        const prior = object(parse(sharedBytes, 'Shared custody ledger'), 'Shared custody ledger');
        exactKeys(prior, ['schema_version', 'owner', 'lease', 'custody'], 'Shared custody ledger');
        const priorLease = object(prior.lease, 'Shared lease state');
        const acquire = object(priorLease.acquireReceipt, 'Prior acquisition receipt');
        const release = object(priorLease.releaseReceipt, 'Prior release receipt');
        const originalAcquire = object(priorLease.originalAcquire, 'Original acquisition response');
        const originalRelease = object(priorLease.originalRelease, 'Original release response');
        if (prior.schema_version !== 1 || canonical_json(prior.owner) !== canonical_json(record.rootMarker.value) ||
            priorLease.stage !== 'released' || priorLease.run_id !== runId ||
            acquire[':status'] !== '201' || acquire['x-ms-lease-id'] !== priorLease.id ||
            release[':status'] !== '200' || originalAcquire.status !== 201 || originalRelease.status !== 200 ||
            typeof originalAcquire.bodyBase64 !== 'string' || typeof originalRelease.bodyBase64 !== 'string' ||
            object(originalAcquire.headers, 'Original acquisition headers')['x-ms-lease-id'] !== priorLease.id ||
            !sharedHeaders.etag || record.intent !== null ||
            !['ready', 'cleaned'].includes(record.state) ||
            canonical_json(prior.custody) !== canonical_json(record)) throw new Failure('Prior shared custody/history unresolved; no reservation overwrite.');
        canonicalRun(text(priorLease.id, 'Prior lease ID'));
        shared = { value: prior, etag: sharedHeaders.etag };
      }
      if (fs.existsSync(ledgerPath)) {
        const prior = object(privateJson(ledgerPath), 'lease ledger');
        if (prior.stage !== 'released' || prior.run_id !== runId) throw new Failure('Unresolved prior lease intent/custody; no automatic recovery.');
        // Retain released ledger, replacing only by its exact prior private content.
        const next = { id: uuid(), stage: 'acquire-intent' as const, run_id: runId };
        publish_private(ledgerPath, next, prior);
        lease = next;
      } else {
        lease = { id: uuid(), stage: 'acquire-intent', run_id: runId };
        publish_private(ledgerPath, lease);
      }
      await publishShared(runId);
      if (!markerHeaders.etag) throw new Failure('Marker version missing before lease acquisition; intent retained.');
      const [, headers] = await azure.blob('PUT', azure.c.data, markerPath(runId), { comp: 'lease' }, Buffer.alloc(0),
        { 'If-Match': markerHeaders.etag, 'x-ms-lease-action': 'acquire', 'x-ms-lease-duration': '-1', 'x-ms-proposed-lease-id': lease.id }, false, false,
        originalAcquire => {
          if (!lease) throw new Failure('Lost proposed lease custody.');
          const next = { ...lease, originalAcquire };
          publish_private(ledgerPath, next, lease); lease = next;
        });
      if (headers[':status'] !== '201' || headers['x-ms-lease-id'] !== lease.id) throw new Failure('Missing affirmative lease acquire receipt; preserve intent.');
      // Revalidate the exact shared reservation under our newly acquired root lease before changing it.
      const [reserved, reservedHeaders] = await azure.blob('GET', azure.c.data, sharedPath(runId), undefined, undefined, {}, false, true);
      if (reserved === null || !shared || reservedHeaders.etag !== shared.etag ||
          canonical_json(parse(reserved, 'Shared reservation')) !== canonical_json(shared.value))
        throw new Failure('Shared reservation changed before custody acquired; retain uncertain custody.');
      const previousLease = lease;
      lease = { ...lease, acquireReceipt: headers };
      publish_private(ledgerPath, lease, previousLease);
      saveLease('held');
      await publishShared(runId);
    }
    if (lease.stage !== 'held') throw new Failure('Cooperating-writer lease is not affirmatively held.');
    const [, headers] = await azure.blob('HEAD', azure.c.data, markerPath(runId), undefined, undefined,
      { 'x-ms-lease-id': lease.id }, false, true);
    if (headers['x-ms-lease-status'] !== 'locked' || headers['x-ms-lease-state'] !== 'leased')
      throw new Failure('Run marker lease is not held; stop.');
  }
  return {
    async exclusive<T>(runId: string, work: () => Promise<T>): Promise<T> {
      activeRun = runId;
      const result = await work();
      if (lease) {
        saveLease('release-intent');
        await publishShared(runId);
        const [, headers] = await azure.blob('PUT', azure.c.data, markerPath(runId), { comp: 'lease' }, Buffer.alloc(0),
          { 'x-ms-lease-action': 'release', 'x-ms-lease-id': lease.id }, false, false,
          originalRelease => {
            if (!lease) throw new Failure('Lost release intent custody.');
            const next = { ...lease, originalRelease };
            publish_private(ledgerPath, next, lease); lease = next;
          });
        if (headers[':status'] !== '200') throw new Failure('Lease release receipt missing; preserve custody.');
        const previousLease = lease;
        lease = { ...lease, releaseReceipt: headers };
        publish_private(ledgerPath, lease, previousLease);
        await publishShared(runId);
        saveLease('released');
        await publishShared(runId);
      }
      activeRun = undefined;
      return result;
      // On ANY thrown/ambiguous mutation, keep the infinite lease and durable ledger; no automatic break/release.
    },
    assertHeld,
    async assertEnvironment() {
      const c = azure.c;
      const storage = object(await azure.arm('GET', c.storage_id, undefined, false, '2023-05-01'), 'Storage account');
      const cluster = object(await azure.arm('GET', c.cluster_id, undefined, false, '2024-04-13'), 'Kusto cluster');
      const identity = object(await azure.arm('GET', c.identity_id, undefined, false, '2023-01-31'), 'UAMI');
      owned(storage, c); owned(cluster, c);
      const identityTags = object(identity.tags, 'Shared UAMI tags'), storageTags = object(storage.tags, 'Storage tags');
      if (identityTags.project !== storageTags.project || identityTags.purpose !== storageTags.purpose ||
          typeof identityTags.sandboxId !== 'string' || !/^stamp-\d+$/.test(identityTags.sandboxId))
        throw new Failure('Shared stamp UAMI ownership differs.');
      if (storage.name !== c.account || object(storage.properties, 'storage properties').isHnsEnabled !== true ||
          object(cluster.properties, 'cluster properties').state !== 'Running' ||
          object(cluster.properties, 'cluster properties').uri !== c.uri) throw new Failure('Existing stamp storage/cluster readiness mismatch.');
      const database = object(await azure.arm('GET', c.cluster_id + '/databases/' + c.experimentDatabase,
        undefined, false, '2024-04-13'), 'Experiment database');
      if (database.name !== c.experimentDatabase || database.type !== 'Microsoft.Kusto/clusters/databases')
        throw new Failure('Existing experiment database identity mismatch.');
      const [, container] = await azure.blob('HEAD', c.data, '', { restype: 'container' }, undefined, {}, false, true);
      if (container[':status'] !== '200' || container['x-ms-meta-sandboxid'] !== c.sandbox) throw new Failure('Data container stamp ownership mismatch.');
      const principal = text(object(identity.properties, 'UAMI properties').principalId, 'UAMI principal');
      const policies = managementRows(await azure.kql(`.show database ['${c.experimentDatabase}'] policy managed_identity`, c.experimentDatabase, true));
      if (policies.length !== 1) throw new Failure('Experiment managed identity policy receipt missing.');
      const policy = jsonField(policies[0].Policy, 'Managed identity policy');
      if (!Array.isArray(policy) || !policy.some(item => {
        const p = object(item, 'Managed identity policy entry');
        return p.ObjectId === principal && typeof p.AllowedUsages === 'string' &&
          ['ExternalTable', 'AutomatedFlows'].every(usage => typeof p.AllowedUsages === 'string' &&
            p.AllowedUsages.split(',').map(s => s.trim()).includes(usage));
      })) throw new Failure('Required existing UAMI external-table/QAP policy is absent.');
    },
    async inspect(database, name): Promise<OatObservation> {
      const regular = managementRows(await azure.kql('.show tables', database, true));
      if (regular.some(row => text(row.TableName, 'regular TableName').toLowerCase() === name.toLowerCase()))
        return { kind: 'regular', evidence: digest(regular) };
      const external = managementRows(await azure.kql('.show external tables', database, true));
      const matching = external.filter(row => text(row.TableName, 'external TableName').toLowerCase() === name.toLowerCase());
      if (!matching.length) return { kind: 'absent', evidence: digest({ regular, external }) };
      if (matching.length !== 1 || matching[0].TableName !== name) throw new Failure('External-name identity/duplicates mismatch.');
      const definition = matching[0];
      const qualified = parseQualifiedDeltaDefinition(definition, name);
      const connectionStrings = qualified.connectionStrings;
      const schemas = managementRows(await azure.kql(`.show external table ['${name}'] schema as json`, database, true));
      if (schemas.length !== 1 || schemas[0].TableName !== name || schemas[0].DatabaseName !== database)
        throw new Failure('External schema identity mismatch.');
      const schema = object(jsonField(schemas[0].Schema, 'External schema'), 'External schema');
      if (!Array.isArray(schema.OrderedColumns) || !schema.OrderedColumns.length) throw new Failure('Actual external schema missing.');
      const policies = managementRows(await azure.kql(`.show external table ['${name}'] policy query_acceleration`, database, true));
      if (policies.length !== 1 || policies[0].EntityName !== `[${database}].[${name}]` ||
          policies[0].EntityType !== 'ExternalTable') throw new Failure('QAP policy identity/receipt missing.');
      const qapPolicy = policies[0].Policy === null ? null : jsonField(policies[0].Policy, 'QAP policy');
      return { kind: 'external', snapshot: { kind: 'delta', name, connectionStrings,
        docString: qualified.docString, schema, qapPolicy, definition: qualified.definition },
        evidence: digest({ definition, schemas, policies }) };
    },
    async validateDelta(storagePath, initialLog) {
      const actions = initialLog.toString().split(/\r?\n/).filter(line => line.trim()).map(line => object(parse(line, 'Delta action'), 'Delta action'));
      const protocol = actions.filter(action => 'protocol' in action), metadata = actions.filter(action => 'metaData' in action);
      if (protocol.length !== 1 || metadata.length !== 1) throw new Failure('Delta initialization requires one actual protocol and metadata action.');
      const p = object(protocol[0].protocol, 'Delta protocol'), m = object(metadata[0].metaData, 'Delta metadata');
      if (!Number.isInteger(p.minReaderVersion) || Number(p.minReaderVersion) < 1 ||
          !Number.isInteger(p.minWriterVersion) || Number(p.minWriterVersion) < 1) throw new Failure('Delta protocol malformed.');
      const schema = object(parse(text(m.schemaString, 'Delta schemaString'), 'Delta schema'), 'Delta schema');
      if (schema.type !== 'struct' || !Array.isArray(schema.fields) || !schema.fields.length) throw new Failure('Actual Delta schema unavailable.');
      const fieldNames = schema.fields.map(field => {
        const f = object(field, 'Delta field');
        text(f.name, 'Delta column name');
        if (typeof f.nullable !== 'boolean' || (typeof f.type !== 'string' && (!f.type || typeof f.type !== 'object')))
          throw new Failure('Delta field metadata malformed.');
        return f.name;
      });
      if (new Set(fieldNames).size !== fieldNames.length) throw new Failure('Duplicate actual Delta columns.');
      // Let create-only Kusto read the latest Delta commit; never fabricate a custom schema from v0.
      const entries = await azure.blobs(azure.c.data);
      const commits = entries.filter(entry => entry.name.startsWith(storagePath + '/_delta_log/') &&
        /^[0-9]{20}\.json$/.test(entry.name.slice((storagePath + '/_delta_log/').length)) &&
        !entry.deleted && !entry.version && !entry.snapshot);
      if (!commits.some(entry => entry.name.endsWith('/00000000000000000000.json')))
        throw new Failure('Current Delta commit inventory does not contain initialized metadata.');
    },
    requireReceipt(operation, name, value) {
      const result = managementRows(value);
      if (result.length !== 1) throw new Failure('Mutation response lacks one affirmative entity receipt.');
      if (operation === 'policy') {
        if (result[0].EntityName !== `[${azure.c.experimentDatabase}].[${name}]` || result[0].EntityType !== 'ExternalTable')
          throw new Failure('Policy mutation entity receipt mismatch.');
        object(jsonField(result[0].Policy, 'Mutation policy'), 'Mutation policy');
      } else {
        const record = readRecord(azure, filename);
        const owner = `arrowcol-oat:${record.run_id}:${record.nonce}:${record.manifest_sha256}`;
        validateExternalMutationReceipt(operation, name, owner, result[0]);
      }
      if (operation !== 'policy') {
        const record = readRecord(azure, filename);
        if (operation === 'create' && result[0].DocString !== `arrowcol-oat:${record.run_id}:${record.nonce}:${record.manifest_sha256}`)
          throw new Failure('Original mutation receipt does not belong to recorded owner.');
        const entry = record.entries.find(item => item.name === name);
        if (operation === 'drop' && !entry?.snapshot)
          throw new Failure('Drop receipt has no saved positive ownership snapshot; intent retained.');
      }
      return digest(result);
    },
    async assertWorkloadStopped(runId) {
      if (!stopPath) throw new Failure('No positive owning-workload stop input.');
      const record = readRecord(azure, filename), stop = object(privateJson(stopPath), 'Workload stop receipt');
      exactKeys(stop, ['schema_version', 'workload_record', 'run_id', 'namespace', 'namespace_uid', 'namespace_run_id',
        'namespace_owner_token', 'aks_id', 'kubectl_server', 'delete_succeeded', 'absence_confirmed', 'observed_at_unix_ms'], 'Workload stop receipt');
      const original = record.workload;
      if (stop.schema_version !== 1 || stop.run_id !== runId || stop.namespace_run_id !== runId ||
          stop.namespace !== original.namespace || stop.namespace_uid !== original.namespaceUid ||
          stop.namespace_owner_token !== original.namespaceOwner || stop.aks_id !== original.aksId ||
          stop.kubectl_server !== original.server || stop.delete_succeeded !== true || stop.absence_confirmed !== true ||
          canonical_json(stop.workload_record) !== canonical_json(original) ||
          !Number.isSafeInteger(stop.observed_at_unix_ms) || Number(stop.observed_at_unix_ms) < record.bound_at_unix_ms ||
          Number(stop.observed_at_unix_ms) > Date.now()) throw new Failure('Stop receipt is not affirmative for the originally bound workload.');
      const cluster = object(await azure.arm('GET', original.aksId, undefined, false, '2024-10-01'), 'AKS cluster');
      owned(cluster, azure.c);
      if (`https://${object(cluster.properties, 'AKS properties').fqdn}` !== original.server) throw new Failure('Original AKS server changed.');
      const result = await runtime.run_tree(['kubectl', '--server', original.server, '--insecure-skip-tls-verify=false',
        '--request-timeout', '15s', 'get', 'namespace', original.namespace, '--ignore-not-found=true', '-o', 'name'], 15);
      if (result.returncode !== 0 || result.stdout.trim() !== '') throw new Failure('Owning namespace absence not positively confirmed; do not clean bindings.');
    }
  };
}
function canonicalRun(runId: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(runId) ||
      runId === '00000000-0000-0000-0000-000000000000') throw new Failure('OAT run ID must be a nonzero canonical lowercase UUID.');
  return runId;
}
function prefixFor(azure: Azure): string {
  const prefix = azure.c.table_prefix;
  if (typeof prefix !== 'string' || (prefix && !/^[A-Za-z0-9_-]+(?:\/[A-Za-z0-9_-]+)*$/.test(prefix))) throw new Failure('Configured table_prefix is unsafe.');
  return [prefix, 'runs'].filter(Boolean).join('/');
}
export function validate_oat_manifest(azure: Azure, runId: string, value: unknown): OatManifest {
  canonicalRun(runId);
  const manifest = object(value, 'OAT manifest');
  exactKeys(manifest, ['schema_version', 'run_id', 'database', 'tables'], 'OAT manifest');
  if (manifest.schema_version !== 1 || manifest.run_id !== runId || !Array.isArray(manifest.tables) || !manifest.tables.length) throw new Failure('Manifest version, run or tables mismatch.');
  const database = text(manifest.database, 'database');
  if (database !== text(azure.c.experimentDatabase, 'experiment database'))
    throw new Failure('OAT manifest database must match the configured experiment database.');
  const names = new Set<string>(), paths = new Set<string>();
  const namePrefix = `oat_${runId.replaceAll('-', '')}_`;
  const tableRoot = `${prefixFor(azure)}/${runId}/dataflow/telemetry/sqlserver/`;
  const tables = manifest.tables.map(raw => {
    const entry = object(raw, 'OAT binding');
    exactKeys(entry, ['external_table', 'relative_path'], 'OAT binding');
    const name = text(entry.external_table, 'external_table'), relative = text(entry.relative_path, 'relative_path');
    if (!name.startsWith(namePrefix) || !/^oat_[a-f0-9]{32}_sqlserver_[A-Za-z0-9_]+$/.test(name) || name.length > 128) throw new Failure('External name must be the exact run-prefixed sqlserver table identity.');
    const logical = name.slice(namePrefix.length);
    if (relative !== tableRoot + logical) throw new Failure('Delta path must exactly match the configured prefix, run and logical table identity; traversal/aliases are forbidden.');
    if (names.has(name.toLowerCase()) || paths.has(relative)) throw new Failure('OAT manifest contains duplicate names or paths.');
    names.add(name.toLowerCase()); paths.add(relative);
    return { external_table: name, relative_path: relative };
  }).sort((a, b) => a.external_table.localeCompare(b.external_table));
  return { schema_version: 1, run_id: runId, database, tables };
}
type Entry = {
  name: string; storagePath: string; connection: string;
  state: 'planned' | 'created' | 'ready' | 'dropped';
  snapshot: OatSnapshot | null;
};
type Intent = { operation: 'marker' | 'create' | 'policy' | 'drop'; name: string; id: string };
interface RecordV1 {
  schema_version: 1; run_id: string; nonce: string; manifest: OatManifest; manifest_sha256: string;
  environment: { account: string; container: string; database: string; tablePrefix: string; sandbox: string; clusterId: string; storageId: string };
  identity: string; policy: { IsEnabled: true; Hot: string; ManagedIdentity: string };
  rootMarker: { path: string; value: unknown; verified: boolean };
  entries: Entry[]; intent: Intent | null; receipts: { operation: string; name: string; evidence: string }[];
  state: 'binding' | 'ready' | 'cleaning' | 'cleaned';
  workload: RunFicRecord;
  bound_at_unix_ms: number;
}
function environment(azure: Azure): RecordV1['environment'] {
  const c = azure.c;
  if (!/^[a-z0-9]{3,24}$/.test(c.account) || !/^[a-z0-9-]+$/.test(c.data)) throw new Failure('Configured storage account/container is invalid.');
  return { account: c.account, container: c.data, database: text(c.experimentDatabase, 'experiment database'), tablePrefix: c.table_prefix,
    sandbox: text(c.sandbox, 'sandbox'), clusterId: text(c.cluster_id, 'cluster ID'), storageId: text(c.storage_id, 'storage ID') };
}
function commit(filename: string, record: RecordV1, change: (next: RecordV1) => void): RecordV1 {
  const next = structuredClone(record);
  change(next);
  publish_private(filename, next, record);
  return next;
}
function readRecord(azure: Azure, filename: string): RecordV1 {
  const raw = object(privateJson(filename), 'OAT ownership record');
  exactKeys(raw, ['schema_version', 'run_id', 'nonce', 'manifest', 'manifest_sha256', 'environment', 'identity', 'policy', 'rootMarker', 'entries', 'intent', 'receipts', 'state', 'workload', 'bound_at_unix_ms'], 'OAT ownership record');
  const workload = parse_run_fic_record(raw.workload, azure.c);
  if (workload.phase !== 'ready' || workload.runId !== raw.run_id ||
      !Number.isSafeInteger(raw.bound_at_unix_ms) || Number(raw.bound_at_unix_ms) < 1) throw new Failure('Recorded workload identity/time invalid.');
  const runId = canonicalRun(text(raw.run_id, 'record run_id'));
  const manifest = validate_oat_manifest(azure, runId, raw.manifest);
  if (raw.schema_version !== 1 || raw.manifest_sha256 !== digest(manifest) || canonical_json(raw.environment) !== canonical_json(environment(azure))) throw new Failure('Immutable record manifest/environment mismatch.');
  canonicalRun(text(raw.nonce, 'ownership nonce'));
  const identity = text(raw.identity, 'record identity');
  if (!/^[0-9a-f-]{36}$/.test(identity)) throw new Failure('Invalid recorded UAMI principal.');
  const policy = object(raw.policy, 'record policy');
  exactKeys(policy, ['IsEnabled', 'Hot', 'ManagedIdentity'], 'record policy');
  if (policy.IsEnabled !== true || policy.ManagedIdentity !== identity || policy.Hot !== `${azure.c.hot}.00:00:00`) throw new Failure('Record policy/config mismatch.');
  const marker = object(raw.rootMarker, 'record root marker');
  if (marker.path !== `${prefixFor(azure)}/${runId}/_arrowcol-oat-run.json` || typeof marker.verified !== 'boolean' ||
      canonical_json(marker.value) !== canonical_json({ schema_version: 1, run_id: runId, nonce: raw.nonce, manifest_sha256: raw.manifest_sha256, environment: raw.environment })) throw new Failure('Root marker expectation changed.');
  if (!Array.isArray(raw.entries) || raw.entries.length !== manifest.tables.length || !Array.isArray(raw.receipts) ||
      !['binding', 'ready', 'cleaning', 'cleaned'].includes(String(raw.state))) throw new Failure('Invalid record entries/state/receipts.');
  const entries: Entry[] = raw.entries.map((item, i) => {
    const entry = object(item, 'record entry'), expected = manifest.tables[i];
    const state = entry.state;
    if (state !== 'planned' && state !== 'created' && state !== 'ready' && state !== 'dropped') throw new Failure('Invalid binding state.');
    if (entry.name !== expected.external_table || entry.storagePath !== expected.relative_path ||
        entry.connection !== connection(azure, expected.relative_path, identity) ||
        !['planned', 'created', 'ready', 'dropped'].includes(String(entry.state))) throw new Failure('Recorded binding changed.');
    if ((entry.state === 'planned' && entry.snapshot !== null) ||
        ((entry.state === 'created' || entry.state === 'ready') && entry.snapshot === null)) throw new Failure('Recorded ownership snapshot/state mismatch.');
    let snapshot: OatSnapshot | null = null;
    if (entry.snapshot !== null) {
      const saved = object(entry.snapshot, 'Saved external snapshot');
      exactKeys(saved, ['kind', 'name', 'connectionStrings', 'docString', 'schema', 'qapPolicy', 'definition'], 'Saved external snapshot');
      if (saved.kind !== 'delta' || saved.name !== entry.name ||
          canonical_json(saved.connectionStrings) !== canonical_json([entry.connection]) ||
          saved.docString !== `arrowcol-oat:${runId}:${raw.nonce}:${raw.manifest_sha256}` ||
          !saved.schema || !saved.definition) throw new Failure('Saved ownership snapshot malformed.');
      object(saved.schema, 'Saved schema'); object(saved.definition, 'Saved definition');
      snapshot = { kind: 'delta', name: expected.external_table, connectionStrings: [String(entry.connection)],
        docString: text(saved.docString, 'Saved owner'), schema: saved.schema, qapPolicy: saved.qapPolicy, definition: saved.definition };
    }
    return { name: expected.external_table, storagePath: expected.relative_path,
      connection: connection(azure, expected.relative_path, identity), state, snapshot };
  });
  const receipts = raw.receipts.map(receipt => {
    const r = object(receipt, 'Saved mutation receipt');
    exactKeys(r, ['operation', 'name', 'evidence'], 'Saved mutation receipt');
    if (!['marker', 'create', 'policy', 'drop'].includes(text(r.operation, 'receipt operation'))) throw new Failure('Unknown saved receipt operation.');
    text(r.name, 'receipt name'); text(r.evidence, 'receipt evidence');
    return { operation: text(r.operation, 'receipt operation'), name: text(r.name, 'receipt name'), evidence: text(r.evidence, 'receipt evidence') };
  });
  if (raw.state === 'ready' && raw.entries.some(entry => object(entry, 'entry').state !== 'ready'))
    throw new Failure('Ready record contains incomplete bindings.');
  if (raw.state === 'cleaned' && raw.entries.some(entry => object(entry, 'entry').state !== 'dropped'))
    throw new Failure('Cleaned record contains undropped bindings.');
  let intent: Intent | null = null;
  if (raw.intent !== null) {
    const candidate = object(raw.intent, 'record mutation intent');
    const operation = candidate.operation;
    if (operation !== 'marker' && operation !== 'create' && operation !== 'policy' && operation !== 'drop') throw new Failure('Unknown recorded mutation intent.');
    intent = { operation, name: text(candidate.name, 'intent name'), id: canonicalRun(text(candidate.id, 'intent ID')) };
  }
  const state = raw.state;
  if (state !== 'binding' && state !== 'ready' && state !== 'cleaning' && state !== 'cleaned') throw new Failure('Invalid custody state.');
  if (state === 'ready' || state === 'cleaned') {
    if (marker.verified !== true || intent !== null) throw new Failure('Completion lacks verified marker or has unresolved intent.');
    if (!receipts.filter(receipt => receipt.operation === 'marker' && receipt.name === marker.path && receipt.evidence.startsWith('{'))
      .some(receipt => {
        const response = object(parse(receipt.evidence, 'Original marker receipt'), 'Original marker receipt');
        return response.status === 201 && typeof object(response.headers, 'Marker receipt headers').etag === 'string';
      })) throw new Failure('Completion lacks original conditional root-marker creation receipt.');
    for (const entry of entries) {
      const required = state === 'ready' ? ['create', 'policy'] : entry.snapshot ? ['create', 'drop'] : [];
      if (entry.snapshot === null && receipts.some(receipt => receipt.name === entry.name && receipt.operation === 'create'))
        throw new Failure('Never-created tombstone contradicts creation history.');
      for (const operation of required) {
        const responses = receipts.filter(receipt => receipt.operation === operation && receipt.name === entry.name &&
          receipt.evidence.startsWith('{')).map(receipt => object(parse(receipt.evidence, 'Original mutation receipt'), 'Original mutation receipt'));
        if (!responses.some(response => {
          if (response.status !== 200 || typeof response.bodyBase64 !== 'string') return false;
          const entities = managementRows(parse(Buffer.from(response.bodyBase64, 'base64'), 'Original Kusto response'));
          return entities.length === 1 && (operation === 'policy'
            ? entities[0].EntityName === `[${azure.c.experimentDatabase}].[${entry.name}]` &&
              policyMatches(jsonField(entities[0].Policy, 'Original policy'), policy)
            : operation === 'create'
              ? entities[0].TableName === entry.name && entities[0].TableType === 'Delta' &&
                entities[0].DocString === `arrowcol-oat:${runId}:${raw.nonce}:${raw.manifest_sha256}` &&
                qualifiedReceiptProperties(entities[0].Properties)
              : entities[0].TableName === entry.name && entities[0].TableType === null &&
                entities[0].DocString === null && entities[0].Properties === null);
        })) throw new Failure('Completion lacks original definitive mutation/entity receipts.');
      }
    }
  }
  return { schema_version: 1, run_id: runId, nonce: text(raw.nonce, 'nonce'), manifest, manifest_sha256: digest(manifest),
    environment: environment(azure), identity, policy: { IsEnabled: true, Hot: String(policy.Hot), ManagedIdentity: identity },
    rootMarker: { path: text(marker.path, 'marker path'), value: marker.value, verified: marker.verified },
    entries, intent, receipts, state, workload, bound_at_unix_ms: Number(raw.bound_at_unix_ms) };
}
const connection = (azure: Azure, storagePath: string, identity: string): string =>
  `abfss://${azure.c.data}@${azure.c.account}.dfs.core.windows.net/${storagePath};managed_identity=${identity}`;
async function requireMarker(azure: Azure, record: RecordV1): Promise<void> {
  const [bytes] = await azure.blob('GET', azure.c.data, record.rootMarker.path, undefined, undefined, {}, true, true);
  if (bytes === null || canonical_json(parse(bytes, 'run marker')) !== canonical_json(record.rootMarker.value)) throw new Failure('Immutable run owner marker is absent or changed; no mutation allowed.');
}
function snapshot(value: OatObservation, entry: Entry, record: RecordV1, expectedPolicy: unknown | null): OatSnapshot {
  text(value.evidence, 'Provider inspection evidence');
  if (value.kind !== 'external') throw new Failure('Owned external binding is missing or replaced.');
  const found = value.snapshot;
  if (found.kind !== 'delta' || found.name !== entry.name || found.docString !== `arrowcol-oat:${record.run_id}:${record.nonce}:${record.manifest_sha256}` ||
      found.schema === null || found.schema === undefined || found.definition === null || found.definition === undefined ||
      !policyMatches(found.qapPolicy, expectedPolicy)) throw new Failure('Owned external binding/schema marker/QAP policy does not match.');
  assertExactConnectionStrings(found.connectionStrings, entry.connection);
  return found;
}
async function unchanged(guard: OatQapOperations, record: RecordV1, entry: Entry): Promise<void> {
  const found = snapshot(await guard.inspect(record.environment.database, entry.name), entry, record,
    entry.state === 'ready' || entry.state === 'dropped' ? record.policy : null);
  if (canonical_json(found) !== canonical_json(entry.snapshot)) throw new Failure('External definition or schema changed; refuse update/cleanup.');
}
async function mutate(azure: Azure, guard: OatQapOperations, filename: string, record: RecordV1,
  entryIndex: number, operation: 'create' | 'policy' | 'drop', command: string): Promise<RecordV1> {
  const entry = record.entries[entryIndex];
  await guard.assertHeld(record.run_id);
  await requireMarker(azure, record);
  record = commit(filename, record, next => { next.intent = { operation, name: entry.name, id: uuid() }; });
  // Any throw after the durable intent retains it. No retry, adoption, or absence-based reconciliation.
  let original: HttpReceipt | undefined;
  const [responseBytes] = await azure.request('POST', azure.c.uri + '/v1/rest/mgmt', 'https://kusto.kusto.windows.net',
    canonical_json({ db: record.environment.database, csl: command }),
    { 'Content-Type': 'application/json', 'x-ms-client-request-id': record.intent?.id ?? '' }, false, false,
    receipt => {
      original = receipt;
      record = commit(filename, record, next => { next.receipts.push({ operation, name: entry.name, evidence: canonical_json(receipt) }); });
    });
  if (!original || original.status !== 200 || responseBytes === null) throw new Failure('Mutation has no original HTTP200 receipt; intent retained.');
  const response = parse(responseBytes, 'Kusto mutation response');
  const evidence = guard.requireReceipt(operation, entry.name, response);
  text(evidence, 'provider mutation receipt');
  record = commit(filename, record, next => {
    next.receipts.push({ operation, name: entry.name, evidence });
  });
  await guard.assertHeld(record.run_id);
  if (operation === 'drop') {
    const observation = await guard.inspect(record.environment.database, entry.name);
    if (observation.kind !== 'absent') throw new Failure('Drop receipt exists but object is still present or replaced; intent retained.');
    record = commit(filename, record, next => { next.entries[entryIndex].state = 'dropped'; next.intent = null; });
  } else {
    const observed = snapshot(await guard.inspect(record.environment.database, entry.name), entry, record, operation === 'create' ? null : record.policy);
    if (operation === 'policy' && entry.snapshot &&
        canonical_json({ ...observed, qapPolicy: null }) !== canonical_json({ ...entry.snapshot, qapPolicy: null }))
      throw new Failure('Policy mutation changed unrelated definition/schema; intent retained.');
    record = commit(filename, record, next => {
      next.entries[entryIndex].state = operation === 'create' ? 'created' : 'ready';
      next.entries[entryIndex].snapshot = observed; next.intent = null;
    });
  }
  return record;
}
async function withRecord<T>(azure: Azure, filename: string, stopPath: string | undefined, work: (guard: OatQapOperations, record: RecordV1) => Promise<T>): Promise<T> {
  const guard = operations(azure, filename, stopPath);
  return reset_lock(filename, async () => {
    const record = readRecord(azure, filename);
    if (record.intent !== null) throw new Failure('Prior OAT mutation intent is unresolved; no automatic replay.');
    return guard.exclusive(record.run_id, async () => {
      await guard.assertEnvironment(); await guard.assertHeld(record.run_id);
      if (record.intent !== null) throw new Failure('Prior OAT mutation intent is unresolved; never replay or infer ownership from matching shape/absence.');
      await requireMarker(azure, record);
      return work(guard, record);
    });
  });
}
export async function bind_oat_qap(azure: Azure, runId: string, manifestPath: string, recordPath: string, workloadRecordPath: string): Promise<void> {
  const manifest = validate_oat_manifest(azure, runId, parse(fs.readFileSync(manifestPath), 'OAT manifest'));
  const workload = read_run_fic_record(workloadRecordPath, azure.c);
  if (workload.runId !== runId || workload.phase !== 'ready') throw new Failure('A ready exact-run workload/FIC custody record is required.');
  await verify_run_fic(azure, workload);
  const guard = operations(azure, recordPath);
  if (fs.existsSync(recordPath) && readRecord(azure, recordPath).intent !== null)
    throw new Failure('Pending mutation record cannot be rebound.');
  await reset_lock(recordPath, async () => guard.exclusive(runId, async () => {
    await guard.assertEnvironment(); await guard.assertHeld(runId);
    let record: RecordV1;
    if (fs.existsSync(recordPath)) {
      record = readRecord(azure, recordPath);
      if (canonical_json(record.workload) !== canonical_json(workload)) throw new Failure('Recorded original workload identity changed.');
      if (record.manifest_sha256 !== digest(manifest) || record.run_id !== runId) throw new Failure('Record does not own this immutable manifest/run.');
      if (record.intent !== null || record.state === 'cleaning' || record.state === 'cleaned') throw new Failure('Pending mutation/cleanup record cannot be rebound.');
      await requireMarker(azure, record);
    } else {
      const rootPath = `${prefixFor(azure)}/${runId}/_arrowcol-oat-run.json`;
      const [marker] = await azure.blob('GET', azure.c.data, rootPath, undefined, undefined, {}, true, true);
      if (marker !== null) throw new Failure('Existing run marker without this durable record is not adoption authority.');
      for (const table of manifest.tables) {
        if ((await guard.inspect(azure.c.experimentDatabase, table.external_table)).kind !== 'absent') throw new Failure('Unowned table/name collision; matching shape is not adoption authority.');
      }
      const principal: unknown = await azure.arm('GET', azure.c.identity_id, undefined, false, '2023-01-31');
      const identity = text(object(object(principal, 'identity').properties, 'identity properties').principalId, 'identity principal');
      canonicalRun(identity);
      if (!Number.isInteger(azure.c.hot) || azure.c.hot < 1) throw new Failure('Invalid configured QAP hot days.');
      for (const table of manifest.tables) {
        const [log] = await azure.blob('GET', azure.c.data, `${table.relative_path}/_delta_log/00000000000000000000.json`, undefined, undefined, {}, true, true);
        if (log === null) throw new NotReady('Real initialized Delta metadata is required before binding.');
        const actions = log.toString().split(/\r?\n/).filter(line => line.trim()).map(line => object(parse(line, 'Delta action'), 'Delta action'));
        if (!actions.some(action => action.protocol) || !actions.some(action => action.metaData)) throw new NotReady('Delta protocol and actual metadata are required.');
        await guard.validateDelta(table.relative_path, log);
      }
      const nonce = uuid(), env = environment(azure), manifestSha = digest(manifest);
      record = {
        schema_version: 1, run_id: runId, nonce, manifest, manifest_sha256: manifestSha, environment: env, identity,
        policy: { IsEnabled: true, Hot: `${azure.c.hot}.00:00:00`, ManagedIdentity: identity },
        rootMarker: { path: rootPath, value: { schema_version: 1, run_id: runId, nonce, manifest_sha256: manifestSha, environment: env }, verified: false },
        entries: manifest.tables.map(table => ({ name: table.external_table, storagePath: table.relative_path,
          connection: connection(azure, table.relative_path, identity), state: 'planned', snapshot: null })),
        intent: { operation: 'marker', name: rootPath, id: uuid() }, receipts: [], state: 'binding',
        workload, bound_at_unix_ms: Date.now()
      };
      publish_private(recordPath, record);
      await guard.assertHeld(runId);
      const [, headers] = await azure.blob('PUT', azure.c.data, rootPath, undefined, canonical_json(record.rootMarker.value),
        { 'If-None-Match': '*', 'x-ms-blob-type': 'BlockBlob' }, false, false,
        receipt => {
          record = commit(recordPath, record, next => {
            next.receipts.push({ operation: 'marker', name: rootPath, evidence: canonical_json(receipt) });
          });
        });
      if (typeof headers.etag !== 'string' || !headers.etag) throw new Failure('Root marker PUT has no affirmative ETag receipt; intent retained.');
      await requireMarker(azure, record);
      record = commit(recordPath, record, next => {
        next.rootMarker.verified = true; next.intent = null;
        next.receipts.push({ operation: 'marker', name: rootPath, evidence: headers.etag });
      });
    }
    for (let index = 0; index < record.entries.length; index++) {
      let entry = record.entries[index];
      if (entry.state === 'planned') {
        if ((await guard.inspect(record.environment.database, entry.name)).kind !== 'absent') throw new Failure('Unowned collision appeared; no create-or-alter or adoption.');
        const owner = `arrowcol-oat:${record.run_id}:${record.nonce}:${record.manifest_sha256}`;
        record = await mutate(azure, guard, recordPath, record, index, 'create',
          `.create external table ['${entry.name}'] kind=delta (h@'${entry.connection}') with (docString='${owner}', AutoUpdateSchema=false)`);
      } else await unchanged(guard, record, entry);
      entry = record.entries[index];
      if (entry.state === 'created') {
        await unchanged(guard, record, entry);
        record = await mutate(azure, guard, recordPath, record, index, 'policy',
          `.alter external table ['${entry.name}'] policy query_acceleration '${canonical_json(record.policy)}'`);
      }
    }
    record = commit(recordPath, record, next => { next.state = 'ready'; });
  }));
}
export async function verify_oat_qap(azure: Azure, recordPath: string): Promise<void> {
  await withRecord(azure, recordPath, undefined, async (guard, record) => {
    if (record.state !== 'ready' || record.entries.some(entry => entry.state !== 'ready')) throw new Failure('OAT bindings are not fully ready.');
    for (const entry of record.entries) await unchanged(guard, record, entry);
    await guard.assertHeld(record.run_id);
  });
}
export async function cleanup_oat_qap(azure: Azure, recordPath: string, workloadStopRecordPath: string): Promise<void> {
  if (!workloadStopRecordPath) throw new Failure('Positive workload stop record is required.');
  await withRecord(azure, recordPath, workloadStopRecordPath, async (guard, record) => {
    await guard.assertWorkloadStopped(record.run_id);
    if (record.state === 'cleaned') {
      for (const entry of record.entries) {
        if (entry.state !== 'dropped' || (await guard.inspect(record.environment.database, entry.name)).kind !== 'absent') throw new Failure('Cleanup tombstone disagrees with provider; do not delete a successor.');
      }
      return;
    }
    // Validate every present owned object before the first drop; never mutate unrelated tables/policies.
    for (const entry of record.entries) {
      if (entry.state === 'created' || entry.state === 'ready') await unchanged(guard, record, entry);
      else if ((await guard.inspect(record.environment.database, entry.name)).kind !== 'absent') throw new Failure('Unowned/changed planned or dropped object blocks cleanup.');
    }
    record = commit(recordPath, record, next => { next.state = 'cleaning'; });
    for (let index = 0; index < record.entries.length; index++) {
      const entry = record.entries[index];
      if (entry.state === 'created' || entry.state === 'ready') {
        await guard.assertWorkloadStopped(record.run_id); await unchanged(guard, record, entry);
        record = await mutate(azure, guard, recordPath, record, index, 'drop', `.drop external table ['${entry.name}']`);
      } else if (entry.state === 'planned') {
        record = commit(recordPath, record, next => { next.entries[index].state = 'dropped'; });
      }
    }
    commit(recordPath, record, next => { next.state = 'cleaned'; });
    // Ownership marker, durable tombstone and all Delta blobs/run roots are retained.
  });
}
