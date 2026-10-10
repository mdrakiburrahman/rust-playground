import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mapHostPath } from '../src/main.js';
import { parseInspection, validateSignals, type Inspection } from '../src/validate.js';

function fixture(): Inspection {
  return {
    files: 3,
    schemas: {},
    tables: {
      logs: [{ id: 1, body: { str: 'greeting generated' }, trace_id: '1'.repeat(32), span_id: '2'.repeat(16) }],
      log_attrs: [{ parent_id: 1, key: 'run.id', str: 'current' }],
      spans: [{ id: 2, name: 'greeting', trace_id: '1'.repeat(32), span_id: '2'.repeat(16) }],
      span_attrs: [{ parent_id: 2, key: 'run.id', str: 'current' }],
      univariate_metrics: [{ id: 3, name: 'hello_world.greetings' }],
      number_data_points: [{ id: 4, parent_id: 3, int_value: 1 }],
      number_dp_attrs: [{ parent_id: 4, key: 'run.id', str: 'current' }],
    },
  };
}

test('accepts all three signals with joined attributes and correlated IDs', () => {
  validateSignals(fixture(), 'current', 1);
});

test('rejects stale data from another run', () => {
  assert.throws(() => validateSignals(fixture(), 'previous', 1), /logs/u);
});

test('rejects a missing signal', () => {
  const value = fixture();
  delete value.tables.spans;
  assert.throws(() => validateSignals(value, 'current', 1), /spans/u);
});

test('rejects wrong log body and uncorrelated trace IDs', () => {
  const value = fixture();
  value.tables.logs[0].body = { str: 'unrelated' };
  assert.throws(() => validateSignals(value, 'current', 1), /body/u);
  value.tables.logs[0].body = { str: 'greeting generated' };
  value.tables.logs[0].trace_id = '3'.repeat(32);
  assert.throws(() => validateSignals(value, 'current', 1), /correlated/u);
});

test('rejects a counter joined to the wrong metric', () => {
  const value = fixture();
  value.tables.number_data_points[0].parent_id = 99;
  assert.throws(() => validateSignals(value, 'current', 1), /datapoints/u);
});

test('rejects corrupt trace IDs and a counter that never reaches the expected value', () => {
  const value = fixture();
  value.tables.spans[0].span_id = '0'.repeat(16);
  value.tables.logs[0].span_id = '0'.repeat(16);
  assert.throws(() => validateSignals(value, 'current', 1), /Invalid span ID/u);
  value.tables.spans[0].span_id = '2'.repeat(16);
  value.tables.logs[0].span_id = '2'.repeat(16);
  value.tables.number_data_points[0].int_value = 0;
  assert.throws(() => validateSignals(value, 'current', 1), /counter datapoint/u);
});

test('rejects invalid and empty inspection output', () => {
  assert.throws(() => parseInspection({}), /Invalid/u);
  assert.throws(() => parseInspection({ files: 1, schemas: {}, tables: { logs: [null] } }), /Invalid/u);
  const value = fixture();
  value.files = 0;
  assert.throws(() => validateSignals(value, 'current', 1), /No Parquet/u);
});

test('maps Docker-outside-Docker paths using the most specific bind', () => {
  assert.equal(mapHostPath('/workspaces/repo/onelake', [
    { Type: 'bind', Source: '/host', Destination: '/workspaces' },
    { Type: 'bind', Source: '/host/project', Destination: '/workspaces/repo' },
  ]), '/host/project/onelake');
  assert.throws(() => mapHostPath('/unmounted', []), /No host bind/u);
});
