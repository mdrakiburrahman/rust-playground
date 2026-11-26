import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import { dmvDefinitions } from '../src/nuget-dmv.js';

test('derive a distinct, nullable DMV contract preserving field order and exact numeric representation', () => {
  const source = readFileSync('projects/loadtest/schemas/sqlserver.sql', 'utf8');
  const definitions = dmvDefinitions(source);
  assert.equal(definitions.size, 32);
  assert.equal(definitions.get('sqlserver_cpu_utilization'), `CREATE TABLE telemetry.sqlserver_dmv.sqlserver_cpu_utilization (
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
});

test('reject unexpected fixture shapes rather than inventing a contract', () => {
  assert.throws(() => dmvDefinitions('CREATE TABLE telemetry.sqlserver.sqlserver_example (name STRING NOT NULL) USING DELTA;'), /missing customer_id/);
});
