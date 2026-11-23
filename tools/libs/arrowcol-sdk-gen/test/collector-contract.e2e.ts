import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { once } from 'node:events';
import { closeSync, mkdirSync, openSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:net';
import { join, resolve, sep } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { commandRunner, contractRoot, digest, loadConfig, readRun, registryEnvironment, writeJson } from '../src/contract.js';

// This proof consumes a fresh successful UI run; it never creates or replays DDL.
const workspace = process.cwd();
const id = process.env.CONTRACT_COLLECTOR_RUN;
assert.ok(id, 'Set CONTRACT_COLLECTOR_RUN to a fresh successful UI run UUID');
const run = readRun(workspace, id);
assert.equal(run.status, 'succeeded');
assert.equal(run.request.codegen, 'protoc');
assert.equal(run.registry.mode, 'owned', 'This local proof must not write through an external authority');
assert.deepEqual(loadConfig(workspace).registries.find((item) => item.id === run.registry.id), run.registry);
assert.ok(run.pin && run.artifact);
assert.equal(digest(readFileSync(resolve(run.artifact.path))), run.artifact.sha256);
const execute = commandRunner(workspace);
const warehouse = resolve(contractRoot, 'registries', digest(run.registry.authority), 'warehouse');
const tableRoot = join(warehouse, ...run.pin.table.split('.'));
const deltaLog = join(tableRoot, '_delta_log');
function committedFiles(): string[] {
  const files = new Set<string>();
  for (const name of readdirSync(deltaLog).filter((entry) => /^\d{20}\.json$/.test(entry)).sort()) {
    for (const line of readFileSync(join(deltaLog, name), 'utf8').trim().split('\n')) {
      const action = JSON.parse(line);
      if (action.remove) files.delete(action.remove.path);
      if (action.add) files.add(action.add.path);
    }
  }
  return [...files].map((path) => {
    const absolute = resolve(tableRoot, path);
    assert.ok(absolute.startsWith(`${tableRoot}${sep}`), 'Delta Add must stay inside the local table');
    return absolute;
  });
}
assert.equal(committedFiles().length, 0, 'Use a fresh table: the proof writes three rows and never deletes data');
const evidence = resolve(contractRoot, 'evidence', `collector-${id}-${randomUUID()}`);
mkdirSync(join(evidence, 'sender/src'), { recursive: true });
mkdirSync(join(evidence, 'release'));
await execute('tar', ['-xzf', resolve(run.artifact.path), '-C', join(evidence, 'release'), 'sdk']);
const sdk = join(evidence, 'release/sdk');
const manifest = JSON.parse(readFileSync(join(sdk, 'sdk-manifest.json'), 'utf8'));
const model = readFileSync(join(sdk, 'rust/src/lib.rs'), 'utf8').match(/pub struct ([A-Za-z0-9_]+Row) \{/);
assert.ok(model, 'Expected generated typed row');
async function unusedPort(): Promise<number> {
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  await new Promise<void>((done, reject) => server.close((error) => error ? reject(error) : done()));
  return address.port;
}
const flightPort = await unusedPort();
const adminPort = await unusedPort();
const config = {
  version: 'otel_dataflow/v1', engine: {},
  policies: { resources: { core_allocation: { type: 'core_count', count: 1 } } },
  groups: { arrowcol: { pipelines: { ingest: { nodes: {
    flight: { type: 'receiver:arrow_flight', config: {
      listening_addr: `127.0.0.1:${flightPort}`, admin_listening_addr: `127.0.0.1:${adminPort}`,
      catalog: { type: 'remote', endpoint: run.registry.endpoint, maximum_cache_age: '10s' },
      routing: { type: 'standalone' },
      registration: { instance_key: `contract-${id}`, flight_endpoint: `127.0.0.1:${flightPort}` },
    } },
    delta: { type: 'exporter:arrow_deltalake', config: {
      base_path: warehouse, routing: { type: 'standalone' },
      commit: { batch_window: '100ms' }, batching: { max_age: '1s' },
    } },
  }, connections: [{ from: 'flight', to: 'delta' }] } } } },
};
const configPath = join(evidence, 'collector.json');
writeJson(configPath, config);
writeFileSync(join(evidence, 'sender/Cargo.toml'), `[package]
name = "contract-collector-proof"
version = "0.1.0"
edition = "2024"
[workspace]
[dependencies]
sdk = { package = ${JSON.stringify(manifest.packages.rust.name)}, path = ${JSON.stringify(join(sdk, 'rust'))} }
arrow-array = "=58.4.0"
arrow-flight = { version = "=58.4.0", default-features = false }
parquet = { version = "=58.4.0", features = ["arrow"] }
tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread"] }
tonic = { version = "=0.14.6", features = ["transport"] }
serde_json = "=1.0.154"
`);
writeFileSync(join(evidence, 'sender/src/main.rs'), `use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_flight::flight_service_client::FlightServiceClient;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args[1] == "send" {
        let rows: Vec<_> = (0..3).map(|id| sdk::${model[1]} {
            event_id: 7000 + id, observed_at: 1700000000000000 + id,
            payload: if id == 1 { None } else { Some(format!("fabric-contract-{id}")) },
        }).collect();
        let batch = sdk::record_batch(&rows)?;
        let channel = tonic::transport::Channel::from_shared(args[2].clone())?.connect().await?;
        let mut client = FlightServiceClient::new(channel);
        let mut response = sdk::do_put(&mut client, [batch], sdk::ImplementationPool::Dataflow).await?.into_inner();
        while response.message().await?.is_some() {}
        println!("Flight stream admitted; Delta commit is verified separately");
    } else {
        assert_eq!(args[1], "verify");
        let files: Vec<String> = serde_json::from_slice(&std::fs::read(&args[2])?)?;
        let mut rows = Vec::new();
        for file in files {
            let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(file)?)?.build()?;
            for batch in reader {
                let batch = batch?;
                let ids = batch.column_by_name("event_id").ok_or("missing event_id")?.as_any()
                    .downcast_ref::<Int64Array>().ok_or("wrong event_id type")?;
                let times = batch.column_by_name("observed_at").ok_or("missing observed_at")?.as_any()
                    .downcast_ref::<TimestampMicrosecondArray>().ok_or("wrong timestamp type")?;
                let payloads = batch.column_by_name("payload").ok_or("missing payload")?.as_any()
                    .downcast_ref::<StringArray>().ok_or("wrong payload type")?;
                for index in 0..batch.num_rows() {
                    rows.push((ids.value(index), times.value(index),
                        (!payloads.is_null(index)).then(|| payloads.value(index).to_owned())));
                }
            }
        }
        rows.sort();
        let expected: Vec<_> = (0..3).map(|id| (7000 + id, 1700000000000000 + id,
            if id == 1 { None } else { Some(format!("fabric-contract-{id}")) })).collect();
        assert_eq!(rows, expected, "committed Delta rows must exactly match typed SDK input");
        println!("{}", serde_json::to_string(&rows)?);
    }
    Ok(())
}
`);
let collector: ChildProcess | undefined;
try {
  const log = openSync(join(evidence, 'collector.log'), 'wx');
  try {
    collector = spawn(resolve('target/debug/arrowcol-dataflow-rust'), ['--num-cores', '1', '--config', configPath], {
      cwd: workspace, shell: false, stdio: ['ignore', log, log],
      env: { ...process.env, ...registryEnvironment(workspace, run.registry) },
    });
  } finally { closeSync(log); }
  let spawnError: Error | undefined;
  collector.on('error', (error) => { spawnError = error; });
  let ready = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    if (spawnError) throw spawnError;
    if (collector.exitCode !== null) throw new Error(readFileSync(join(evidence, 'collector.log'), 'utf8'));
    try {
      const response = await fetch(`http://127.0.0.1:${adminPort}/readyz`, { signal: AbortSignal.timeout(1000) });
      if (response.ok) {
        writeJson(join(evidence, 'ready.json'), await response.json());
        ready = true;
        break;
      }
    } catch (error) {
      if (!(error instanceof TypeError && error.cause instanceof Error && 'code' in error.cause &&
          error.cause.code === 'ECONNREFUSED')) throw error;
    }
    await delay(250);
  }
  assert.ok(ready, `Collector not ready; inspect ${evidence}/collector.log`);
  const cargo = ['run', '--quiet', '--manifest-path', join(evidence, 'sender/Cargo.toml')];
  const env = { CARGO_TARGET_DIR: resolve('target/arrowcol-sdk-gen-validation-cargo-target') };
  await execute('cargo', [...cargo, '--', 'send', `http://127.0.0.1:${flightPort}`], join(evidence, 'sender.log'), env);
  let files: string[] = [];
  for (let attempt = 0; attempt < 120; attempt++) {
    files = committedFiles();
    if (files.length) break;
    await delay(250);
  }
  assert.ok(files.length, `No committed Delta Add after Flight admission; inspect ${evidence}/collector.log`);
  writeJson(join(evidence, 'committed-files.json'), files);
  const rows = await execute('cargo', [...cargo, '--', 'verify', join(evidence, 'committed-files.json')],
    join(evidence, 'sender.log'), env);
  const report = { result: 'passed', run: id, artifactSha256: run.artifact.sha256,
    table: run.pin, registry: run.registry.id, rows: JSON.parse(rows), files,
    topology: 'Generated SDK -> direct endpoint Arrowcol -> committed Delta; remote catalogue configured on collector' };
  writeJson(join(evidence, 'result.json'), report);
  console.log(JSON.stringify({ ...report, evidence }, null, 2));
} finally {
  if (collector?.pid && collector.exitCode === null && collector.signalCode === null) {
    collector.kill('SIGTERM');
    for (let attempt = 0; attempt < 120 && collector.exitCode === null && collector.signalCode === null; attempt++) await delay(250);
    if (collector.exitCode === null && collector.signalCode === null) {
      collector.kill('SIGKILL');
      await once(collector, 'exit');
      throw new Error('Collector failed bounded graceful shutdown');
    }
    assert.ok(collector.exitCode === 0 || collector.signalCode === 'SIGTERM', 'Collector shutdown failed');
  }
}
