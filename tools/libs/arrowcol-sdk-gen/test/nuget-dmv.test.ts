import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { datasetDdl, dmvCatalog, dmvDefinitions, type DmvCatalogSource } from '../src/nuget-dmv.js';

const fixture = readFileSync('projects/loadtest/schemas/sqlserver.sql', 'utf8');
const source = JSON.parse(readFileSync('tools/libs/arrowcol-sdk-gen/catalog/dmv-catalog.json', 'utf8')) as DmvCatalogSource;

test('derive a distinct, nullable DMV contract preserving field order and exact numeric representation', () => {
  const definitions = dmvDefinitions(fixture);
  assert.equal(definitions.size, 32);
  assert.equal(definitions.get('sqlserver_cpu_utilization'), `CREATE TABLE telemetry.sqlserver_dmv_v2.sqlserver_cpu_utilization (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP,
    process_sample_time_utc TIMESTAMP,
    sql_server_instance_name STRING,
    machine_name STRING,
    avg_cpu_percent DECIMAL(38,12),
    sql_process_cpu_percent INT,
    idle_cpu_percent INT
) USING DELTA;
`);
  for (const kind of ['common', 'detailed']) {
    assert.match(definitions.get(`sqlserver_performance_counters_${kind}`) ?? '', /^\s+cntr_value DOUBLE,$/m);
  }
  assert.match(definitions.get('sqlserver_missing_indexes') ?? '', /^\s+avg_total_user_cost DOUBLE,$/m);
});

test('reject unexpected fixture shapes rather than inventing a contract', () => {
  assert.throws(() => dmvDefinitions('CREATE TABLE telemetry.sqlserver.sqlserver_example (name STRING NOT NULL) USING DELTA;'), /missing customer_id/);
});

test('catalog keeps the legacy tables and adds PaaS families and one bounded table per public view', () => {
  const datasets = dmvCatalog(fixture, source);
  const count = (family: string) => datasets.filter(dataset => dataset.family === family).length;
  assert.deepEqual([datasets.length, count('sqlserver_dmv_v2'), count('sqldb_single_database'), count('sqldb_elastic_pool'),
    count('sqlmi'), count('dmv')], [139, 32, 9, 7, 9, 82]);
  assert.equal(new Set(datasets.map(dataset => dataset.table)).size, datasets.length);
  const legacy = dmvDefinitions(fixture);
  for (const dataset of datasets.filter(dataset => dataset.family === 'sqlserver_dmv_v2')) {
    assert.equal(datasetDdl(dataset), legacy.get(dataset.dataset));
    assert.equal(dataset.source ?? dataset.query, undefined);
  }
  const pool = datasets.find(dataset => dataset.family === 'sqldb_elastic_pool' && dataset.dataset === 'wait_stats');
  assert.deepEqual(pool?.columns.slice(-4).map(column => column.name), source.identity.map(text => text.split(' ')[0]));
  assert.deepEqual(pool?.targets, ['sqldb_elastic_pool']);
  const entities = datasets.find(dataset => dataset.dataset === 'dm_db_uncontained_entities');
  assert.equal(entities?.source, 'sys.dm_db_uncontained_entities');
  assert.deepEqual(entities?.columns.slice(0, 2), [
    { name: 'customer_id', type: 'STRING', nullable: false }, { name: 'sample_time_utc', type: 'TIMESTAMP', nullable: true },
  ]);
  assert.deepEqual(entities?.columns.find(column => column.name === 'entity_class'),
    { name: 'entity_class', type: 'INT', nullable: true, source: 'class' });
  for (const dataset of datasets.filter(dataset => dataset.family === 'dmv')) {
    assert.ok(dataset.query || dataset.columns.length > 6, `${dataset.dataset} projects no view columns`);
    assert.ok(!dataset.query || dataset.top === undefined, `${dataset.dataset} mixes a verbatim query with TOP`);
  }
});

test('reject catalog entries that would silently change what is collected', () => {
  const view = { view: 'dm_os_wait_stats', interval: '00:05:00', provenance: ['sql-docs'], targets: ['sqlserver'] };
  const reject = (entry: object, message: RegExp) =>
    assert.throws(() => dmvCatalog(fixture, { ...source, families: {}, views: [{ ...view, columns: ['wait_type STRING'], ...entry }] }), message);
  reject({ columns: ['wait_type NVARCHAR'] }, /Unsupported column/);
  reject({ columns: ['wait_type STRING', 'wait_type BIGINT'] }, /Duplicate column/);
  reject({ query: 'SELECT SYSUTCDATETIME() AS sample_time_utc, wait_type FROM sys.dm_os_wait_stats', top: 10 }, /carries its own bounds/);
  reject({ watermark: 'missing_column' }, /ordering column/);
  reject({ aliases: { other: 'x' } }, /alias other has no column/);
});