import assert from 'node:assert/strict';
import { test } from 'node:test';

import {
  expectedTableRows, kustoExplorerClusterUri, kustoExplorerStartArguments, kustoExplorerUrl, tableRowMismatches, tableSummaryQuery,
} from '../src/ui-gate.js';

const validation = {
  passed: true,
  run_id: 'run-1',
  tables: {
    "['direct_sqlserver_cpu_utilization']": { expected_rows: 24576, actual: { Rows: 24576, Invalid: 0 } },
    "['dataflow_sqlserver_cpu_utilization']": { expected_rows: 23552, actual: { Rows: 23552, Invalid: 0 } },
  },
};

test('kusto-explorer starts alone, waits for its health check and is addressed through Compose DNS', () => {
  assert.deepEqual(kustoExplorerStartArguments, [
    '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', '--wait', '--wait-timeout', '120', 'kusto-explorer',
  ]);
  assert.equal(kustoExplorerUrl, 'http://kusto-explorer:8080/');
  assert.equal(kustoExplorerClusterUri, 'http://kusto:8080');
});

test('native QAP UI queries enumerate external tables rather than a native-table wildcard', () => {
  const query = tableSummaryQuery(['direct_x', 'dataflow_y']);
  assert(query.includes("external_table('direct_x')"));
  assert(query.includes("external_table('dataflow_y')"));
  assert(query.includes('| summarize Rows=count()'));
  assert(!query.includes('direct_*'));
  assert.throws(() => tableSummaryQuery([]), /safe validated/u);
  assert.throws(() => tableSummaryQuery(["x'); drop"]), /safe validated/u);
});

test('expected table rows come from a passing Kusto validation with bracket-quoted names', () => {
  assert.deepEqual([...expectedTableRows(validation)], [
    ['direct_sqlserver_cpu_utilization', 24576],
    ['dataflow_sqlserver_cpu_utilization', 23552],
  ]);
  assert.throws(() => expectedTableRows({ ...validation, passed: false }), /did not pass/u);
  assert.throws(() => expectedTableRows({ passed: true, tables: {} }), /lists no tables/u);
  assert.throws(() => expectedTableRows({ passed: true, tables: { "['x; drop']": { expected_rows: 1 } } }), /invalid entry/u);
  assert.throws(() => expectedTableRows({ passed: true, tables: { "['t']": { expected_rows: -1 } } }), /invalid entry/u);
  assert.throws(() => expectedTableRows(null), /not an object/u);
});

test('UI exports must match every validated table exactly', () => {
  const expected = expectedTableRows(validation);
  assert.deepEqual(tableRowMismatches(expected, [
    { TableName: 'dataflow_sqlserver_cpu_utilization', Rows: 23552 },
    { TableName: 'direct_sqlserver_cpu_utilization', Rows: 24576 },
  ]), []);
  assert.deepEqual(tableRowMismatches(expected, [
    { TableName: 'direct_sqlserver_cpu_utilization', Rows: 24575 },
    { TableName: 'direct_unexpected', Rows: 1 },
  ]), [
    'direct_sqlserver_cpu_utilization: the UI shows 24575 rows, expected 24576',
    'dataflow_sqlserver_cpu_utilization: missing from the UI results',
    'direct_unexpected: returned by the UI but absent from kusto-validation.json',
  ]);
  assert.deepEqual(tableRowMismatches(expected, [{ TableName: 'direct_sqlserver_cpu_utilization', Rows: '24576' }]).slice(0, 1),
    ['direct_sqlserver_cpu_utilization: the UI shows "24576" rows, expected 24576']);
  assert.deepEqual(tableRowMismatches(expected, { rows: [] }), ['the JSON export is not an array of rows']);
  assert.deepEqual(tableRowMismatches(expected, [{ Rows: 1 }]), ['the JSON export has a row without a TableName']);
});
