import assert from 'node:assert/strict';
import test from 'node:test';
import { kql, queryRows } from './kusto.js';

test('queryRows returns the primary result and throws on error frames', () => {
  const primary = { FrameType: 'DataTable', TableKind: 'PrimaryResult', Columns: [{ ColumnName: 'client' }, { ColumnName: 'rows' }], Rows: [['a', 1]] };
  assert.deepEqual(queryRows(JSON.stringify([primary])), [{ client: 'a', rows: 1 }]);
  assert.throws(() => queryRows(JSON.stringify([primary, { FrameType: 'DataSetCompletion', HasErrors: true, OneApiErrors: [{}] }])), /failed/);
});

test('generated KQL rejects unsafe names and locations', () => {
  assert.equal(kql.externalTable('otap_logs', '/delta/otap/logs'), ".create-or-alter external table ['otap_logs'] kind=delta (h@'/delta/otap/logs')");
  assert.throws(() => kql.externalTable("x']", '/delta'), /identifier/);
  assert.throws(() => kql.externalTable('x', "/delta')"), /location/);
  assert.equal(kql.dmvClients('dmv_t'), "external_table('dmv_t') | summarize rows=count() by client=customer_id");
  assert.throws(() => kql.otapClients("x')"), /identifier/);
});