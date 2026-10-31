import assert from 'node:assert/strict';
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { tableFromIPC } from 'apache-arrow';
import { readParquet } from 'parquet-wasm';
import { canonicalRows, canonicalTables, readState, runFixture } from '../loadgen/scripts/kusto_qap_fixture.js';

test('native Delta fixtures retain mutations, partition purge, transactions and vacuum tombstones', t => {
  const root = mkdtempSync(path.join(process.cwd(), '.kusto-fixture-test-'));
  t.after(() => rmSync(root, { force: true, recursive: true }));
  const table = (name: string): string => path.join(root, 'telemetry/sqlserver', name);
  const sessions = table('sqlserver_active_sessions');
  const ag = table('sqlserver_availability_group_states');
  const count = (directory: string): number => {
    let rows = 0;
    for (const add of readState(directory).adds.values()) {
      rows += tableFromIPC(readParquet(readFileSync(path.join(directory, add.path))).intoIPCStream()).numRows;
    }
    return rows;
  };
  runFixture(root, 'init');
  assert(JSON.parse(readState(sessions).metadata.schemaString).fields.every((field: { nullable: boolean }) => field.nullable));
  assert.equal(count(sessions), 100);
  assert.equal(count(ag), 100);
  runFixture(root, 'mutate');
  assert.equal(count(sessions), 75);
  assert.equal(count(ag), 90);
  assert([...readState(sessions).adds.values()].every(add => add.partitionValues.__arrowcol_ingestion !== 'b1'));
  assert([...readState(ag).removes.values()].some(remove => remove.dataChange === false));
  runFixture(root, 'prune-prep');
  assert.equal(count(sessions), 50);
  assert.equal(count(ag), 110);
  assert.deepEqual(Object.fromEntries(readState(sessions).txns), {
    'arrowcol.publication.prep': 1, 'arrowcol.purge.before-ms': 1793070000000,
  });
  runFixture(root, 'prune-post');
  assert.equal(count(sessions), 50);
  assert.equal(count(ag), 130);
  assert.equal(readState(sessions).txns.get('arrowcol.purge.before-ms'), 1793080000000);
  runFixture(root, 'vacuum');
  for (const directory of [sessions, ag]) {
    for (const file of readState(directory).removes.keys()) assert(!existsSync(path.join(directory, file)));
    for (const file of readState(directory).adds.keys()) assert(statSync(path.join(directory, file)).isFile());
  }
});

test('all canonical schemas preserve signed boundaries, normalized unsigneds and exact timestamp micros/nanos', t => {
  const root = mkdtempSync(path.join(process.cwd(), '.kusto-schema-test-'));
  t.after(() => rmSync(root, { force: true, recursive: true }));
  const sql = path.join(process.cwd(), 'projects/loadtest/schemas/sqlserver.sql');
  const tables = canonicalTables(readFileSync(sql, 'utf8'));
  assert.equal(tables.length, 13);
  runFixture(root, 'schemas', sql);
  for (const [name, columns] of tables) {
    const directory = path.join(root, 'telemetry/sqlserver', name);
    const state = readState(directory);
    assert(JSON.parse(state.metadata.schemaString).fields.every((field: { nullable: boolean }) => !field.nullable));
    assert.equal(state.adds.size, 2);
    const first = [...state.adds.values()][0];
    const table = tableFromIPC(readParquet(readFileSync(path.join(directory, first.path))).intoIPCStream());
    assert.equal(table.numRows, 30);
    const expected = canonicalRows(columns, 0, 30, 'b1');
    for (const [column, kind] of columns) {
      if (kind.startsWith('TIMESTAMP(9)')) {
        const nanos = table.getChild(`${column}__unix_nanos`)!;
        const micros = table.getChild(column)!.data[0].values as BigInt64Array;
        assert(Array.from(nanos).some(value => value! < 0n));
        assert(Array.from(nanos).some(value => value! % 1000n !== 0n));
        for (let i = 0; i < table.numRows; i++) {
          assert.equal(nanos.get(i), expected[i][`${column}__unix_nanos`]);
          assert.equal(micros[i], expected[i][column]);
        }
      } else {
        const vector = table.getChild(column)!;
        for (let i = 0; i < table.numRows; i++) assert.equal(vector.get(i), expected[i][column]);
      }
    }
  }
});
