import assert from 'node:assert/strict';

export type Row = Record<string, unknown>;
export interface Inspection {
  files: number;
  schemas: Record<string, unknown>;
  tables: Record<string, Row[]>;
  versions: Record<string, number>;
  active_files: Record<string, number>;
  file_namespaces: Record<string, Record<string, string[]>>;
}

function object(value: unknown): value is Row {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function parseInspection(value: unknown): Inspection {
  if (!object(value) || typeof value.files !== 'number' || !object(value.tables) || !object(value.schemas) ||
      !object(value.versions) || !object(value.active_files) || !object(value.file_namespaces)) {
    throw new Error('Invalid Delta inspection result');
  }
  const tables: Record<string, Row[]> = {};
  const versions: Record<string, number> = {};
  const activeFiles: Record<string, number> = {};
  const fileNamespaces: Record<string, Record<string, string[]>> = {};
  for (const [table, rows] of Object.entries(value.tables)) {
    if (!Array.isArray(rows) || !rows.every(object)) {
      throw new Error(`Invalid rows for table ${table}`);
    }
    tables[table] = rows;
    const version = value.versions[table];
    const files = value.active_files[table];
    if (typeof version !== 'number' || !Number.isSafeInteger(version) || version < 0 ||
        typeof files !== 'number' || !Number.isSafeInteger(files) || files < 0) {
      throw new Error(`Invalid Delta version/file count for ${table}`);
    }
    versions[table] = version;
    activeFiles[table] = files;
    const namespaces = value.file_namespaces[table];
    if (!object(namespaces)) throw new Error(`Missing file namespaces for ${table}`);
    fileNamespaces[table] = {};
    for (const [file, values] of Object.entries(namespaces)) {
      if (!Array.isArray(values) || !values.every((namespace) => typeof namespace === 'string' && namespace.length > 0)) {
        throw new Error(`Invalid file namespaces for ${table}/${file}`);
      }
      fileNamespaces[table][file] = values;
    }
  }
  return { files: value.files, schemas: value.schemas, tables, versions, active_files: activeFiles, file_namespaces: fileNamespaces };
}

export function commitSpansCores(inspection: Inspection, table: string, files: string[]): boolean {
  if (files.length < 2) return false;
  const namespaces = files.flatMap((file) => inspection.file_namespaces[table]?.[file] ?? []);
  return new Set(namespaces).size > 1;
}

function joinKey(row: Row, id: unknown): string {
  assert(typeof row._otel_join_namespace === 'string' && row._otel_join_namespace.length > 0, 'Missing join namespace');
  assert(typeof id === 'number' && Number.isSafeInteger(id), 'Invalid join ID');
  return `${row._otel_join_namespace}:${id}`;
}

function markedRows(inspection: Inspection, table: string, attributeTable: string, runId: string): Row[] {
  const parentIds = new Set(
    (inspection.tables[attributeTable] ?? [])
      .filter((row) => row.key === 'run.id' && row.str === runId)
      .map((row) => joinKey(row, row.parent_id)),
  );
  return (inspection.tables[table] ?? []).filter((row) => parentIds.has(joinKey(row, row.id)));
}

function validateTimestamps(inspection: Inspection, table: string, rows: Row[]): void {
  const schema = inspection.schemas[table];
  assert(object(schema) && Array.isArray(schema.fields), `Missing Delta schema for ${table}`);
  const fields = schema.fields.filter(object);
  for (const field of fields.filter((field) => field.type === 'timestamp')) {
    assert(typeof field.name === 'string', 'Invalid timestamp field name');
    const companion = `${field.name}__unix_nanos`;
    assert(fields.some((candidate) => candidate.name === companion && candidate.type === 'long'),
      `Missing exact nanosecond schema for ${table}.${field.name}`);
    for (const row of rows) {
      if (row[field.name] === null) {
        assert.equal(row[companion], null, 'Timestamp null mask changed');
        continue;
      }
      const value = row[field.name];
      const nanos = row[companion];
      assert(typeof value === 'string', `Missing native timestamp ${table}.${field.name}`);
      assert(typeof nanos === 'string' && /^-?\d+$/u.test(nanos) ||
        typeof nanos === 'number' && Number.isSafeInteger(nanos), 'Nanoseconds lost integer precision');
      const match = /^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))? \+00:00$/u.exec(value);
      assert(match, `Invalid UTC microsecond timestamp ${value}`);
      const micros = BigInt(Date.parse(`${match[1]}T${match[2]}Z`)) * 1000n +
        BigInt((match[3] ?? '').padEnd(6, '0'));
      const exact = BigInt(nanos);
      const truncated = exact >= 0n ? exact / 1000n : (exact - 999n) / 1000n;
      assert.equal(micros, truncated, 'Native timestamp disagrees with exact nanoseconds');
    }
  }
  assert(fields.some((field) => field.type === 'timestamp'), `No native timestamps in ${table}`);
}

function validateMetadataJoins(inspection: Inspection, rows: Row[]): void {
  for (const row of rows) {
    for (const [field, table] of [['resource', 'resource_attrs'], ['scope', 'scope_attrs']]) {
      const value = row[field];
      assert(object(value), `Missing ${field}`);
      if (field === 'resource') {
        const id = joinKey(row, value.id);
        const attrs = (inspection.tables[table] ?? []).filter((attr) => joinKey(attr, attr.parent_id) === id);
        assert(attrs.some((attr) => attr.key === 'service.name' && attr.str === 'hello-world'), 'Resource attributes are not joined');
      } else {
        assert(typeof value.name === 'string' && value.name.length > 0, 'Missing instrumentation scope');
      }
    }
  }
}

export function validateSignals(inspection: Inspection, runId: string, count: number): void {
  assert(inspection.files > 0, 'No committed Delta files');
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
    .filter((row) => row.name === 'hello_world.greetings').map((row) => joinKey(row, row.id)));
  const datapoints = markedRows(inspection, 'number_data_points', 'number_dp_attrs', runId)
    .filter((row) => metricIds.has(joinKey(row, row.parent_id)));
  assert(datapoints.length > 0, 'Missing greeting metric datapoints');
  assert(datapoints.every((row) => typeof row.int_value === 'number' && row.int_value > 0), 'Invalid counter datapoint');
  assert(datapoints.some((row) => row.int_value === count), `Counter never reached ${count}`);
  validateTimestamps(inspection, 'logs', logs);
  validateTimestamps(inspection, 'spans', spans);
  validateTimestamps(inspection, 'number_data_points', datapoints);
  validateMetadataJoins(inspection, [...logs, ...spans]);
}

export function signalsReady(inspection: Inspection, runId: string, count: number): boolean {
  const metricIds = new Set((inspection.tables.univariate_metrics ?? [])
    .filter((row) => row.name === 'hello_world.greetings').map((row) => joinKey(row, row.id)));
  return markedRows(inspection, 'logs', 'log_attrs', runId).length >= count &&
    markedRows(inspection, 'spans', 'span_attrs', runId).length >= count &&
    markedRows(inspection, 'number_data_points', 'number_dp_attrs', runId)
      .some((row) => row.int_value === count && metricIds.has(joinKey(row, row.parent_id)));
}
