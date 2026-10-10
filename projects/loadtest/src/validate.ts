import assert from 'node:assert/strict';

export type Row = Record<string, unknown>;
export interface Inspection {
  files: number;
  schemas: Record<string, unknown>;
  tables: Record<string, Row[]>;
}

function object(value: unknown): value is Row {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function parseInspection(value: unknown): Inspection {
  if (!object(value) || typeof value.files !== 'number' || !object(value.tables) || !object(value.schemas)) {
    throw new Error('Invalid Parquet inspection result');
  }
  const tables: Record<string, Row[]> = {};
  for (const [table, rows] of Object.entries(value.tables)) {
    if (!Array.isArray(rows) || !rows.every(object)) {
      throw new Error(`Invalid rows for table ${table}`);
    }
    tables[table] = rows;
  }
  return { files: value.files, schemas: value.schemas, tables };
}

function markedRows(inspection: Inspection, table: string, attributeTable: string, runId: string): Row[] {
  const parentIds = new Set(
    (inspection.tables[attributeTable] ?? [])
      .filter((row) => row.key === 'run.id' && row.str === runId)
      .map((row) => row.parent_id),
  );
  return (inspection.tables[table] ?? []).filter((row) => parentIds.has(row.id));
}

export function validateSignals(inspection: Inspection, runId: string, count: number): void {
  assert(inspection.files > 0, 'No Parquet files');
  const logs = markedRows(inspection, 'logs', 'log_attrs', runId);
  const spans = markedRows(inspection, 'spans', 'span_attrs', runId);
  assert.equal(logs.length, count, `Expected ${count} logs with run.id=${runId}`);
  assert.equal(spans.length, count, `Expected ${count} spans with run.id=${runId}`);
  for (const log of logs) {
    assert(object(log.body) && log.body.str === 'greeting generated', 'Wrong log body');
    assert(spans.some((span) => span.trace_id === log.trace_id && span.span_id === log.span_id), 'Log is not correlated with a greeting span');
  }
  for (const span of spans) {
    assert.equal(span.name, 'greeting', 'Wrong span name');
    assert(typeof span.trace_id === 'string' && /^[a-f0-9]{32}$/u.test(span.trace_id) && !/^0+$/u.test(span.trace_id), 'Invalid trace ID');
    assert(typeof span.span_id === 'string' && /^[a-f0-9]{16}$/u.test(span.span_id) && !/^0+$/u.test(span.span_id), 'Invalid span ID');
  }
  const metricIds = new Set((inspection.tables.univariate_metrics ?? [])
    .filter((row) => row.name === 'hello_world.greetings').map((row) => row.id));
  const datapoints = markedRows(inspection, 'number_data_points', 'number_dp_attrs', runId)
    .filter((row) => metricIds.has(row.parent_id));
  assert(datapoints.length > 0, 'Missing greeting metric datapoints');
  assert(datapoints.every((row) => typeof row.int_value === 'number' && row.int_value > 0), 'Invalid counter datapoint');
  assert(datapoints.some((row) => row.int_value === count), `Counter never reached ${count}`);
}
