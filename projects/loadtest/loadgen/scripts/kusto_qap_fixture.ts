/** Native TypeScript Delta/Parquet fixtures for kusto-qap-it.sh. No service or credentials required. */
import assert from 'node:assert/strict';
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import {
  Bool, DataType, Field, Float64, Int8, Int16, Int32, Int64, Schema, Table,
  TimestampMicrosecond, Utf8, makeData, makeVector, tableFromIPC, tableToIPC, vectorFromArray,
} from 'apache-arrow';
import { Table as WasmTable, readParquet, writeParquet } from 'parquet-wasm';

type Column = [string, string];
type Row = Record<string, string | number | bigint | boolean>;
type Action = Record<string, any>;
interface State {
  version: number; metadata: Action; adds: Map<string, Action>; removes: Map<string, Action>;
  txns: Map<string, number>;
}
const TEXT = ['', 'plain', 'quote \' and " mix', 'back\\slash', 'tab\tnew\nline', 'unicodé ✓ 漢字', 'x'.repeat(300)];
const NANOS = [-1n, 0n, 999n, 1793073575123456789n, 4102444799999999999n, -86400000000001n];
const BOUNDS: Record<string, (number | bigint | boolean)[]> = {
  TINYINT: [-128, 127, 0, -1, 1], 'TINYINT UNSIGNED': [0, 255, 1, 128, 254],
  SMALLINT: [-32768, 32767, 0, -1, 1], INT: [-2147483648, 2147483647, 0, -1, 1],
  BIGINT: [-9223372036854775808n, 9223372036854775807n, 0n, -1n, 1n],
  DOUBLE: [0, -0.5, 1e308, -2.2250738585072014e-308, 123456.789012345], BOOLEAN: [true, false],
};
const TYPES: Record<string, DataType> = {
  TINYINT: new Int8(), 'TINYINT UNSIGNED': new Int16(), SMALLINT: new Int16(), INT: new Int32(),
  BIGINT: new Int64(), DOUBLE: new Float64(), BOOLEAN: new Bool(), VARCHAR: new Utf8(),
};
const DELTA_TYPES: Record<string, string> = {
  TINYINT: 'byte', 'TINYINT UNSIGNED': 'short', SMALLINT: 'short', INT: 'integer', BIGINT: 'long',
  DOUBLE: 'double', BOOLEAN: 'boolean', VARCHAR: 'string',
};
const SESSION: Column[] = [
  ['customer_id', 'VARCHAR'], ['sample_time_utc', 'TIMESTAMP(9)'], ['sql_server_instance_name', 'VARCHAR'],
  ['machine_name', 'VARCHAR'], ['database_id', 'SMALLINT'], ['database_name', 'VARCHAR'],
  ['session_id', 'SMALLINT'], ['session_status', 'VARCHAR'], ['connection_id', 'VARCHAR'],
];
const AG: Column[] = [
  ['customer_id', 'VARCHAR'], ['sample_time_utc', 'TIMESTAMP(9)'], ['failure_condition_level', 'INT'],
  ['is_distributed', 'BOOLEAN'], ['lag_seconds', 'DOUBLE'], ['log_bytes', 'BIGINT'],
];
const WATERMARK = 'arrowcol.purge.before-ms';
const WATERMARKS = { prep: 1793070000000, post: 1793080000000 };
const timestamp = (kind: string): boolean => kind.startsWith('TIMESTAMP(9)');
const micros = (n: bigint): bigint => (n >= 0n ? n / 1000n : (n - 999n) / 1000n);
const versionName = (v: number): string => String(v).padStart(20, '0');

export function canonicalTables(sql: string): [string, Column[]][] {
  return [...sql.matchAll(/CREATE TABLE telemetry\.sqlserver\.(\w+) \((.*?)\);/gs)].map(match => [
    match[1], match[2].trim().split(',\n').map(line => {
      const column = /^\s*(\w+) (.+?) NOT NULL/u.exec(line);
      assert(column, `Invalid canonical column ${line}`);
      return [column[1], column[2]] as Column;
    }),
  ]);
}

export function canonicalRows(columns: Column[], start: number, count: number, batch: string): Row[] {
  return Array.from({ length: count }, (_, offset) => {
    const i = start + offset;
    const row: Row = { __arrowcol_ingestion: batch };
    columns.forEach(([name, kind], index) => {
      if (timestamp(kind)) {
        const nanos = NANOS[(i + index) % NANOS.length] + BigInt(i);
        row[name] = micros(nanos);
        row[`${name}__unix_nanos`] = nanos;
      } else if (kind === 'VARCHAR') {
        row[name] = TEXT[(i + index) % TEXT.length] + (i % 5 ? `#${i}` : '');
      } else {
        assert(BOUNDS[kind], `Unsupported canonical type ${kind}`);
        row[name] = BOUNDS[kind][(i + index) % BOUNDS[kind].length];
      }
    });
    return row;
  });
}

function fixtureRows(sessions: boolean, start: number, count: number, batch: string): Row[] {
  const epoch = BigInt(Date.UTC(2026, 9, 27)) * 1000n;
  return Array.from({ length: count }, (_, offset) => {
    const i = start + offset;
    const common: Row = { customer_id: `c${i % 3}`, sample_time_utc: epoch + BigInt(i) * (sessions ? 1001n : 1000000n),
      __arrowcol_ingestion: batch };
    return sessions ? { ...common, sql_server_instance_name: `inst${i % 2}`, machine_name: 'm\'1"x',
      database_id: i % 7, database_name: `db${i % 4}`, session_id: i, session_status: 'running', connection_id: `conn-${i}` } :
      { ...common, failure_condition_level: i, is_distributed: i % 2 === 0, lag_seconds: i * 0.5,
        log_bytes: BigInt(i) * 10000000000n };
  });
}

function expanded(columns: Column[], companions: boolean): Column[] {
  return columns.flatMap(([name, kind]) => timestamp(kind) && companions ?
    [[name, kind], [`${name}__unix_nanos`, 'BIGINT']] as Column[] : [[name, kind]]);
}

export function parquetRows(columns: Column[], rows: Row[]): Uint8Array {
  const fields: Field[] = [];
  const vectors: Record<string, any> = {};
  for (const [name, kind] of columns) {
    const type = timestamp(kind) ? new TimestampMicrosecond('UTC') : TYPES[kind];
    assert(type, `Unsupported Arrow type ${kind}`);
    fields.push(new Field(name, type, false));
    vectors[name] = timestamp(kind) ? makeVector(makeData({
      type: type as TimestampMicrosecond, data: BigInt64Array.from(rows, row => BigInt(row[name])), length: rows.length, nullCount: 0,
    })) : vectorFromArray(rows.map(row => row[name]) as any[], type);
  }
  const table = new Table(new Schema(fields), vectors);
  return writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream')));
}

function readRows(file: string): Row[] {
  const wasm = readParquet(readFileSync(file));
  const table = tableFromIPC(wasm.intoIPCStream());
  const rows: Row[] = [];
  for (let i = 0; i < table.numRows; i++) {
    const row: Row = {};
    for (const field of table.schema.fields) {
      const vector = table.getChild(field.name)!;
      // Arrow JS timestamp accessors expose milliseconds as floating point; preserve raw micros.
      row[field.name] = DataType.isTimestamp(field.type) ?
        (vector.data[0].values as BigInt64Array)[i] : vector.get(i);
    }
    rows.push(row);
  }
  return rows;
}

function checkpointRows(file: string): Action[] {
  const wasm = readParquet(readFileSync(file));
  return tableFromIPC(wasm.intoIPCStream()).toArray().map(row =>
    JSON.parse(JSON.stringify(row, (_, value) => typeof value === 'bigint' ? Number(value) : value)));
}

export function readState(table: string): State {
  const log = path.join(table, '_delta_log');
  const state: State = { version: -1, metadata: {}, adds: new Map(), removes: new Map(), txns: new Map() };
  if (!existsSync(log)) return state;
  const apply = (action: Action): void => {
    if (action.metaData) state.metadata = action.metaData;
    if (action.add) { state.adds.set(action.add.path, action.add); state.removes.delete(action.add.path); }
    if (action.remove) { state.adds.delete(action.remove.path); state.removes.set(action.remove.path, action.remove); }
    if (action.txn) state.txns.set(action.txn.appId, Number(action.txn.version));
  };
  const hint = path.join(log, '_last_checkpoint');
  if (existsSync(hint)) {
    state.version = JSON.parse(readFileSync(hint, 'utf8')).version;
    for (const row of checkpointRows(path.join(log, `${versionName(state.version)}.checkpoint.parquet`))) apply(row);
  }
  for (const name of readdirSync(log).filter(name => /^\d{20}\.json$/u.test(name)).sort()) {
    const version = Number(name.slice(0, 20));
    if (version <= state.version) continue;
    for (const line of readFileSync(path.join(log, name), 'utf8').trim().split('\n')) apply(JSON.parse(line));
    state.version = version;
  }
  return state;
}

function commit(table: string, actions: Action[]): void {
  const state = readState(table);
  mkdirSync(path.join(table, '_delta_log'), { recursive: true });
  writeFileSync(path.join(table, '_delta_log', `${versionName(state.version + 1)}.json`),
    [...actions, { commitInfo: { timestamp: Date.now(), operation: 'WRITE', engineInfo: 'arrowcol-typescript-fixture' } }]
      .map(value => JSON.stringify(value)).join('\n') + '\n', { flag: 'wx' });
}

function addFile(table: string, columns: Column[], rows: Row[], batch: string, dataChange: boolean): Action {
  const directory = `__arrowcol_ingestion=${batch}`;
  const relative = `${directory}/part-${randomUUID()}.parquet`;
  mkdirSync(path.join(table, directory), { recursive: true });
  writeFileSync(path.join(table, relative), parquetRows(columns, rows));
  return { add: { path: relative, partitionValues: { __arrowcol_ingestion: batch },
    size: statSync(path.join(table, relative)).size, modificationTime: Date.now(), dataChange } };
}

function append(table: string, columns: Column[], rows: Row[], batch: string, txns: Action[] = [], nullable = true): void {
  const state = readState(table);
  const actions: Action[] = [];
  if (state.version < 0) {
    actions.push({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }, { metaData: {
      id: randomUUID(), format: { provider: 'parquet', options: {} }, partitionColumns: ['__arrowcol_ingestion'],
      configuration: {}, createdTime: Date.now(), schemaString: JSON.stringify({ type: 'struct', fields:
        [...columns, ['__arrowcol_ingestion', 'VARCHAR']].map(([name, kind]) => ({
          name, type: timestamp(kind) ? 'timestamp' : DELTA_TYPES[kind], nullable, metadata: {},
        })) }),
    } });
  }
  actions.push(addFile(table, columns, rows, batch, true), ...txns);
  commit(table, actions);
}

function removeFile(add: Action, dataChange: boolean): Action {
  return { remove: { path: add.path, deletionTimestamp: Date.now(), dataChange, extendedFileMetadata: true,
    partitionValues: add.partitionValues, size: add.size } };
}

function rewrite(table: string, columns: Column[], keep: (row: Row, batch: string) => boolean, optimize = false, txns: Action[] = []): void {
  const state = readState(table);
  const actions: Action[] = [];
  const partitions = new Map<string, Row[]>();
  for (const add of state.adds.values()) {
    const batch = add.partitionValues.__arrowcol_ingestion;
    const rows = readRows(path.join(table, decodeURIComponent(add.path)));
    const kept = rows.filter(row => keep(row, batch));
    if (!optimize && kept.length === rows.length) continue;
    actions.push(removeFile(add, !optimize));
    partitions.set(batch, [...(partitions.get(batch) ?? []), ...kept]);
  }
  for (const [batch, rows] of partitions) if (rows.length) actions.push(addFile(table, columns, rows, batch, !optimize));
  if (actions.length || txns.length) commit(table, [...actions, ...txns]);
}

function truth(step: string, table: string): void {
  const state = readState(table);
  let rows = 0;
  for (const add of state.adds.values()) rows += readRows(path.join(table, decodeURIComponent(add.path))).length;
  console.log('TRUTH', step, path.basename(table), 'v', state.version, 'files', state.adds.size, 'rows', rows);
}

function pruneAssert(table: string, cycle: number): void {
  const state = readState(table);
  const log = path.join(table, '_delta_log');
  const hint = JSON.parse(readFileSync(path.join(log, '_last_checkpoint'), 'utf8')).version;
  const floor = JSON.parse(readFileSync(path.join(table, '_arrowcol_maintenance/log-floor.json'), 'utf8'));
  const progress = JSON.parse(readFileSync(path.join(table, '_arrowcol_maintenance/vacuum-lite.json'), 'utf8'));
  assert.equal(floor.checkpoint_version, hint);
  assert.equal(hint, state.version);
  assert.equal(progress.table_id, floor.table_id);
  assert(progress.version >= hint);
  const entries = readdirSync(log).sort();
  assert.deepEqual(entries.filter(name => /^\d{20}\.json$/u.test(name)), [`${versionName(hint)}.json`]);
  assert.deepEqual(entries.filter(name => name.includes('.checkpoint.')), [`${versionName(hint)}.checkpoint.parquet`]);
  const checkpoint = checkpointRows(path.join(log, `${versionName(hint)}.checkpoint.parquet`));
  assert.deepEqual(checkpoint.filter(row => row.metaData).map(row => row.metaData.id), [floor.table_id]);
  const expected: Record<string, number> = { 'arrowcol.publication.prep': 1 };
  if (cycle === 2) expected['arrowcol.publication.post'] = 1;
  if (path.basename(table) === 'sqlserver_active_sessions') expected[WATERMARK] = WATERMARKS[cycle === 1 ? 'prep' : 'post'];
  assert.deepEqual(Object.fromEntries(checkpoint.filter(row => row.txn).map(row => [row.txn.appId, row.txn.version])), expected);
  const adds = checkpoint.filter(row => row.add).map(row => row.add.path);
  const removes = checkpoint.filter(row => row.remove).map(row => row.remove.path);
  assert(removes.length, 'checkpoint dropped every tombstone');
  assert.deepEqual([...adds].sort(), [...state.adds.keys()].sort());
  assert(adds.every(file => statSync(path.join(table, decodeURIComponent(file))).isFile()));
  assert(removes.every(file => !existsSync(path.join(table, decodeURIComponent(file)))), 'tombstone not vacuumed');
  console.log('PRUNED', cycle, path.basename(table), 'floor', hint, 'txns', Object.keys(expected).length,
    'adds', adds.length, 'tombstones', removes.length);
}

export function runFixture(root: string, step: string, sql?: string): void {
  const base = path.join(root, 'telemetry/sqlserver');
  const sessions = path.join(base, 'sqlserver_active_sessions');
  const ag = path.join(base, 'sqlserver_availability_group_states');
  const tables: [string, Column[], boolean][] = [[sessions, SESSION, true], [ag, AG, false]];
  let paths = tables.map(([table]) => table);
  if (step === 'schemas') {
    assert(sql, 'schemas requires canonical SQL');
    const schemas = canonicalTables(readFileSync(sql, 'utf8'));
    assert.equal(schemas.length, 13);
    paths = [];
    for (const [name, columns] of schemas) {
      const table = path.join(base, name);
      const fields = expanded(columns, true);
      for (const [start, batch] of [[0, 'b1'], [30, 'b2']] as const) {
        append(table, fields, canonicalRows(columns, start, 30, batch), batch, [], false);
      }
      paths.push(table);
    }
  } else if (step === 'init') {
    for (const [table, columns, isSessions] of tables) {
      append(table, columns, fixtureRows(isSessions, 0, 50, 'b1'), 'b1');
      append(table, columns, fixtureRows(isSessions, 50, 50, 'b2'), 'b2');
    }
  } else if (step === 'mutate') {
    rewrite(sessions, SESSION, (_, batch) => batch !== 'b1');
    append(sessions, SESSION, fixtureRows(true, 100, 25, 'b3'), 'b3');
    rewrite(ag, AG, row => Number(row.failure_condition_level) >= 10);
    rewrite(ag, AG, () => true, true);
  } else if (step === 'vacuum') {
    for (const [table] of tables) for (const file of readState(table).removes.keys()) {
      rmSync(path.join(table, decodeURIComponent(file)), { force: true });
    }
  } else if (step === 'prune-prep' || step === 'prune-post') {
    const stage = step === 'prune-prep' ? 'prep' : 'post';
    const [batch, purged, start] = stage === 'prep' ? ['b4', 'b2', 125] as const : ['b5', 'b3', 150] as const;
    for (const [table, columns, isSessions] of tables) {
      append(table, columns, fixtureRows(isSessions, start, 25, batch), batch,
        [{ txn: { appId: `arrowcol.publication.${stage}`, version: 1 } }]);
    }
    rewrite(sessions, SESSION, (_, value) => value !== purged, false,
      [{ txn: { appId: WATERMARK, version: WATERMARKS[stage] } }]);
    rewrite(ag, AG, row => Number(row.failure_condition_level) < start + 20);
    rewrite(ag, AG, () => true, true);
  } else if (/^prune-assert-[12]$/u.test(step)) {
    for (const [table] of tables) pruneAssert(table, Number(step.at(-1)));
  } else throw new Error(`Unknown fixture step ${step}`);
  for (const table of paths) truth(step, table);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  assert(process.argv[2] && process.argv[3], 'usage: kusto_qap_fixture.ts ROOT STEP [SQLSERVER_SQL]');
  runFixture(process.argv[2], process.argv[3], process.argv[4]);
}
