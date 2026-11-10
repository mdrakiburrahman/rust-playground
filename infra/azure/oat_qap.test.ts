import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Azure, Config, canonical_json } from './bootstrap.js';
import { managementRows, parseKustoQueryResult, parseQualifiedDeltaDefinition,
  assertExactConnectionStrings, validateExternalMutationReceipt, validate_oat_manifest } from './oat_qap.js';

const id = '22222222-2222-4222-8222-222222222222';
const run = '9dcd6847-2a56-45a3-bd3c-54af839f451e';
const name = `oat_${run.replaceAll('-', '')}_sqlserver_synthetic_table`;
const relative = `synthetic/runs/${run}/dataflow/telemetry/sqlserver/sqlserver_synthetic_table`;
const manifest = { schema_version: 1, run_id: run, database: 'experimentqap', tables: [{ external_table: name, relative_path: relative }] };

const deltaProperties = {
  AutoUpdateSchema: false, Compressed: false, CompressionType: null, Encoding: null,
  FileExtension: null, Format: 'Parquet', IncludeHeaders: 'None', NamePrefix: null, Shortcut: false,
};
const externalConnection = `abfss://synthetic@storage123.dfs.core.windows.net/${relative};managed_identity=${id}`;
function externalDefinition(patch: Record<string, unknown> = {}) {
  return { TableName: name, TableType: 'Delta', Folder: null, DocString: 'arrowcol-oat:synthetic-owner',
    Properties: JSON.stringify(deltaProperties), ConnectionStrings: [externalConnection],
    Partitions: null, PathFormat: '', ...patch };
}
function kustoTable(TableName: string, columns: { ColumnName: string; ColumnType: string }[], Rows: unknown[][]) {
  return { TableName, Columns: columns, Rows };
}
function queryResponse(statusRows: unknown[][] = [
  ['2026-11-10T00:00:00Z', 4, 'Info', 0, 'Query completed successfully'],
  ['2026-11-10T00:00:00Z', 3, 'Warning', -2133196383, 'Query acceleration was not applied; catalog is uninitialized.'],
  ['2026-11-10T00:00:00Z', 6, 'Stats', 0, null],
]) {
  const statusColumns = [
    { ColumnName: 'Timestamp', ColumnType: 'datetime' }, { ColumnName: 'Severity', ColumnType: 'int' },
    { ColumnName: 'SeverityName', ColumnType: 'string' }, { ColumnName: 'StatusCode', ColumnType: 'int' },
    { ColumnName: 'StatusDescription', ColumnType: 'string' },
  ];
  return { Tables: [
    kustoTable('Table_0', [{ ColumnName: 'Count', ColumnType: 'long' }], [[0]]),
    kustoTable('Table_1', [{ ColumnName: 'Value', ColumnType: 'string' }], [['extended']]),
    kustoTable('Table_2', statusColumns, statusRows),
    kustoTable('Table_3', [
      { ColumnName: 'Ordinal', ColumnType: 'long' }, { ColumnName: 'Kind', ColumnType: 'string' },
      { ColumnName: 'Name', ColumnType: 'string' }, { ColumnName: 'Id', ColumnType: 'string' },
      { ColumnName: 'PrettyName', ColumnType: 'string' },
    ], [[0, 'QueryResult', 'PrimaryResult', 'id0', ''], [1, 'QueryProperties', '@ExtendedProperties', 'id1', ''],
      [2, 'QueryStatus', 'QueryStatus', 'id2', '']]),
  ] };
}
test('qualified Delta provider shape parses typed connection and opaque Properties without desired-value substitution', () => {
  const parsed = parseQualifiedDeltaDefinition(externalDefinition(), name);
  assert.deepEqual(parsed.connectionStrings, [externalConnection]);
  assert.deepEqual(assertExactConnectionStrings(parsed.connectionStrings, externalConnection), [externalConnection]);
  assert.throws(() => assertExactConnectionStrings(parsed.connectionStrings,
    externalConnection.replace(id, '33333333-3333-4333-8333-333333333333')), /exact owned run URI and UAMI principal/);
  assert.throws(() => assertExactConnectionStrings([...parsed.connectionStrings, externalConnection], externalConnection),
    /exact owned run URI and UAMI principal/);
  assert.equal(parsed.docString, 'arrowcol-oat:synthetic-owner');
  assert.deepEqual(parsed.properties, deltaProperties);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ TableType: 'Parquet' }), name), /name\/type/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ TableName: name + '_other' }), name), /name\/type/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({ ConnectionStrings: externalConnection }), name), /ConnectionStrings/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    ConnectionStrings: [`${externalConnection};client_id=${id}`],
  }), name), /ConnectionStrings/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    Properties: JSON.stringify({ ...deltaProperties, Unexpected: true }),
  }), name), /Properties differ/);
  assert.throws(() => parseQualifiedDeltaDefinition(externalDefinition({
    Properties: { ...deltaProperties },
  }), name), /JSON string shape/);
});
test('management results remain separate from TOC-mapped query results and enforce typed columns', () => {
  const management = { Tables: [kustoTable('Table_0', [
    { ColumnName: 'TableName', ColumnType: 'string' }, { ColumnName: 'TableType', ColumnType: 'string' },
  ], [[name, 'Delta']])] };
  assert.deepEqual(managementRows(management), [{ TableName: name, TableType: 'Delta' }]);
  assert.throws(() => managementRows(queryResponse()), /one Kusto primary/);
  const drop = { TableName: name, TableType: null, DocString: null, Properties: null };
  const dropManagement = { Tables: [kustoTable('Table_0', [
    { ColumnName: 'TableName', ColumnType: 'string' }, { ColumnName: 'TableType', ColumnType: 'string' },
    { ColumnName: 'DocString', ColumnType: 'string' }, { ColumnName: 'Properties', ColumnType: 'string' },
  ], [[name, null, null, null]])] };
  const parsedDrop = managementRows(dropManagement)[0];
  assert.deepEqual(parsedDrop, drop);
  assert.deepEqual(validateExternalMutationReceipt('drop', name, 'arrowcol-oat:synthetic-owner', parsedDrop), drop);
  const missingType = structuredClone(management);
  delete (missingType.Tables[0].Columns[0] as { ColumnType?: string }).ColumnType;
  assert.throws(() => managementRows(missingType), /ColumnType/);
});
test('query response maps primary results by TOC and distinguishes an unaccelerated completed query', () => {
  const parsed = parseKustoQueryResult(queryResponse());
  assert.deepEqual(parsed.rows, [{ Count: 0 }]);
  assert.equal(parsed.query_completed, true);
  assert.equal(parsed.query_acceleration_not_applied, true);
  assert.equal(parsed.query_acceleration_applied, false);
  assert.equal(parsed.accelerated_read_proven, false);
});
test('query response rejects malformed TOC, unknown statuses and partial failures', () => {
  const badOrdinal = structuredClone(queryResponse()); badOrdinal.Tables[3].Rows[2][0] = 9;
  assert.throws(() => parseKustoQueryResult(badOrdinal), /table of contents is malformed/);
  const duplicatePrimary = structuredClone(queryResponse()); duplicatePrimary.Tables[3].Rows.push([...duplicatePrimary.Tables[3].Rows[2]]);
  assert.throws(() => parseKustoQueryResult(duplicatePrimary), /table of contents is malformed/);
  const missingPrimary = structuredClone(queryResponse()); missingPrimary.Tables[3].Rows.pop();
  assert.throws(() => parseKustoQueryResult(missingPrimary), /does not map every result table/);
  const failed = queryResponse([['now', 2, 'Error', -1, 'failed'], ['now', 4, 'Info', 0, 'done']]);
  assert.throws(() => parseKustoQueryResult(failed), /partial query failure/);
  const unknownCode = queryResponse([['now', 4, 'Info', 0, 'done'], ['now', 3, 'Warning', -1, 'unknown']]);
  assert.throws(() => parseKustoQueryResult(unknownCode), /unknown or unsuccessful/);
  const malformedSeverity = queryResponse([['now', '3', 'Warning', -2133196383, 'typed incorrectly'], ['now', 4, 'Info', 0, 'done']]);
  assert.throws(() => parseKustoQueryResult(malformedSeverity), /malformed severity/);
  const noCompletion = queryResponse([['now', 6, 'Stats', 0, null]]);
  assert.throws(() => parseKustoQueryResult(noCompletion), /no successful completion/);
});
test('create receipts require exact Delta metadata; drop receipts accept only matching-name null tombstones', () => {
  const owner = 'arrowcol-oat:synthetic-owner';
  const create = externalDefinition({ DocString: owner });
  assert.equal(validateExternalMutationReceipt('create', name, owner, create).TableName, name);
  assert.throws(() => validateExternalMutationReceipt('create', name, owner + 'wrong', create), /owner shape/);
  assert.throws(() => validateExternalMutationReceipt('create', name, owner,
    { ...create, Properties: JSON.stringify({ ...deltaProperties, AutoUpdateSchema: true }) }), /Properties differ/);
  const drop = { TableName: name, TableType: null, DocString: null, Properties: null };
  assert.deepEqual(validateExternalMutationReceipt('drop', name, owner, drop), drop);
  assert.throws(() => validateExternalMutationReceipt('drop', name + '_other', owner, drop), /null tombstone/);
  assert.throws(() => validateExternalMutationReceipt('drop', name, owner, { ...drop, TableType: 'Delta' }), /null tombstone/);
});
test('locked run/path/name validation still rejects traversal, wrong identity and prefix aliases', () => {
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: id }));
  azure.c.table_prefix = 'synthetic';
  assert.deepEqual(validate_oat_manifest(azure, run, manifest), manifest);
  assert.throws(() => validate_oat_manifest(azure, run, { schema_version: 1, run_id: run, tables: manifest.tables }), /missing or unexpected fields/);
  assert.throws(() => validate_oat_manifest(azure, run, { ...manifest, database: 'arrowcol' }), /must match the configured experiment database/);
  for (const patch of [
    { run_id: id }, { schema_version: '1' }, { tables: [manifest.tables[0], manifest.tables[0]] },
    { tables: [{ external_table: name, relative_path: relative + '/child' }] },
    { tables: [{ external_table: name, relative_path: relative.replace('/runs/', '/runs/../') }] },
    { tables: [{ external_table: name + '_other', relative_path: relative }] },
    { tables: [{ external_table: name, relative_path: relative.slice('synthetic/'.length) }] }
  ]) assert.throws(() => validate_oat_manifest(azure, run, { ...manifest, ...patch }));
  assert.throws(() => validate_oat_manifest(azure, run.toUpperCase(), manifest));
  azure.c.table_prefix = '';
  const withoutPrefix = { ...manifest, tables: [{ external_table: name, relative_path: relative.slice('synthetic/'.length) }] };
  assert.deepEqual(validate_oat_manifest(azure, run, withoutPrefix), withoutPrefix);
});
