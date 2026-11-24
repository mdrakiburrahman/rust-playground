import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import {
  closeSync, copyFileSync, existsSync, lstatSync, openSync, readFileSync, readdirSync, unlinkSync, writeFileSync,
} from 'node:fs';
import { join, resolve, sep } from 'node:path';
import { createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import {
  commandRunner, contractRoot, digest, ensureDirectory, errorMessage, isCode,
  loadConfig, readRun, registryEnvironment, writeJson,
} from './contract.js';
import { kustoDemo } from './stream-demo-kusto.js';

const { values } = parseArgs({ options: {
  action: { type: 'string' }, 'run-id': { type: 'string' },
  language: { type: 'string', default: 'rust' }, node: { type: 'boolean', default: false },
  'duration-seconds': { type: 'string', default: '300' }, 'interval-ms': { type: 'string', default: '1000' },
  'port-base': { type: 'string', default: '18830' },
  'kusto-port': { type: 'string', default: '18090' },
}, strict: true });

async function main(): Promise<void> {
  assert.ok(values['run-id'], 'Provide --run-id from a successful build-contract run');
  assert.ok(['prepare', 'build', 'serve', 'emit', 'inspect', 'kusto-up', 'kusto-query', 'kusto-stop'].includes(values.action ?? ''),
    'Use --action prepare|build|serve|emit|inspect|kusto-up|kusto-query|kusto-stop');
  const workspace = process.cwd();
  const run = readRun(workspace, values['run-id']);
  assert.equal(run.status, 'succeeded');
  assert.ok(run.pin && run.artifact);
  assert.equal(run.registry.mode, 'owned', 'The localhost demo never writes to external registries');
  assert.deepEqual(loadConfig(workspace).registries.find((registry) => registry.id === run.registry.id), run.registry);
  assert.equal(digest(readFileSync(resolve(run.artifact.path))), run.artifact.sha256, 'Artifact digest changed');
  assert.equal(run.pin.table.split('.').at(-1), 'events', 'Use the sample schema with table name events');
  const root = resolve(contractRoot, 'demos', run.id);
  ensureDirectory(workspace, root);
  const execute = commandRunner(workspace);
  const sdk = join(root, 'sdk');
  const proof = { run: run.id, artifactSha256: run.artifact.sha256, pin: run.pin };
  const prepared = join(root, 'prepared.json');
  const sample = resolve('examples/arrowcol-stream');
  const cargoTarget = resolve('target/arrowcol-sdk-gen-validation-cargo-target');
  const cargoManifest = join(root, 'apps/rust/Cargo.toml');
  const csProject = join(root, 'apps/csharp/Emitter.csproj');
  const cppProject = join(root, 'apps/cpp');
  const cppBuild = join(cppProject, 'build');
  const xml = (value: string): string => value.replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll('<', '&lt;');
  const cmake = (value: string): string => JSON.stringify(value.replaceAll('\\', '/'));

  if (values.action === 'prepare') {
    if (existsSync(prepared)) {
      assert.deepEqual(JSON.parse(readFileSync(prepared, 'utf8')), proof);
      console.log(`Already prepared: ${root}`);
      return;
    }
    assert.ok(!existsSync(sdk), `Partial preparation exists at ${root}; preserve it and use a new build run`);
    await execute('tar', ['-xzf', resolve(run.artifact.path), '-C', root, 'sdk']);
    assert.ok(existsSync(join(sdk, 'native/Cargo.toml')), 'Rebuild this contract with the new typed stream generator');
    const manifest = JSON.parse(readFileSync(join(sdk, 'sdk-manifest.json'), 'utf8'));
    const csEvent = readFileSync(join(sdk, 'csharp/Contract.cs'), 'utf8').match(/IArrowcolContract<([^>]+)>/)?.[1];
    assert.ok(csEvent, 'Missing generated protobuf event alias');
    for (const language of ['rust', 'csharp', 'cpp']) ensureDirectory(workspace, join(root, 'apps', language));
    writeFileSync(cargoManifest, `[package]
name = "arrowcol-stream-demo"
version = "0.1.0"
edition = "2024"
[workspace]
[[bin]]
name = "emitter"
path = ${JSON.stringify(join(sample, 'rust/main.rs'))}
[[bin]]
name = "inspect"
path = ${JSON.stringify(join(sample, 'rust/inspect.rs'))}
[dependencies]
sdk = { package = ${JSON.stringify(manifest.packages.rust.name)}, path = ${JSON.stringify(join(sdk, 'rust'))} }
tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread", "signal"] }
arrow-array = "=58.4.0"
parquet = { version = "=58.4.0", features = ["arrow"] }
serde_json = "=1.0.154"
`);
    writeFileSync(csProject, `<Project Sdk="Microsoft.NET.Sdk">
<PropertyGroup><OutputType>Exe</OutputType><TargetFramework>net8.0</TargetFramework><Nullable>enable</Nullable></PropertyGroup>
<ItemGroup>
<ProjectReference Include="${xml(join(sdk, 'csharp/Contract.csproj'))}" />
<Compile Include="${xml(join(sample, 'csharp/Program.cs'))}" Link="Program.cs" />
</ItemGroup></Project>
`);
    writeFileSync(join(root, 'apps/csharp/Schema.cs'),
      `global using Event = ${csEvent};\nglobal using EventsStream = Arrowcol.Generated.EventsStream;\n`);
    writeFileSync(join(cppProject, 'CMakeLists.txt'), `cmake_minimum_required(VERSION 3.20)
project(arrowcol_emitter LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_subdirectory(${cmake(join(sdk, 'cpp'))} contract)
add_executable(emitter ${cmake(join(sample, 'cpp/main.cpp'))})
target_link_libraries(emitter PRIVATE arrowcol_contract)
if(WIN32)
  add_custom_command(TARGET emitter POST_BUILD COMMAND \${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:contract_native> $<TARGET_FILE_DIR:emitter>)
endif()
`);
    writeJson(prepared, proof);
    console.log(JSON.stringify({ ...proof, root, editableSamples: sample }, null, 2));
    return;
  }
  assert.deepEqual(JSON.parse(readFileSync(prepared, 'utf8')), proof, 'Run prepare first');
  const warehouse = resolve(contractRoot, 'registries', digest(run.registry.authority), 'warehouse');
  const tableRoot = join(warehouse, ...run.pin.table.split('.'));
  if (values.action?.startsWith('kusto-')) {
    await kustoDemo(values.action, workspace, root, run.id, warehouse, run.pin.table, Number(values['kusto-port']), execute);
    return;
  }

  if (values.action === 'inspect') {
    const active = new Set<string>();
    for (const file of readdirSync(join(tableRoot, '_delta_log')).filter((name) => /^\d{20}\.json$/.test(name)).sort()) {
      for (const line of readFileSync(join(tableRoot, '_delta_log', file), 'utf8').trim().split('\n')) {
        const action = JSON.parse(line);
        if (action.remove) active.delete(action.remove.path);
        if (action.add) active.add(action.add.path);
      }
    }
    const files = [...active].map((path) => {
      const absolute = resolve(tableRoot, path);
      assert.ok(absolute.startsWith(`${tableRoot}${sep}`), 'Delta Add must stay inside the local table');
      assert.ok(lstatSync(absolute).isFile() && !lstatSync(absolute).isSymbolicLink(), 'Expected a regular Parquet file');
      return absolute;
    });
    const inventory = join(root, 'committed-files.json');
    writeJson(inventory, files);
    const rows = JSON.parse(await execute('cargo', ['run', '--quiet', '--manifest-path', cargoManifest,
      '--bin', 'inspect', '--', inventory], undefined, { CARGO_TARGET_DIR: cargoTarget }));
    writeJson(join(root, 'committed-rows.json'), rows);
    console.log(JSON.stringify({ ...proof, tableRoot, files, rows }, null, 2));
    return;
  }
  if (values.action === 'emit' || values.action === 'build') {
    assert.ok(['rust', 'csharp', 'cpp'].includes(values.language ?? ''), 'Use --language rust|csharp|cpp');
    const seconds = Number(values['duration-seconds']), interval = Number(values['interval-ms']);
    assert.ok(Number.isInteger(seconds) && seconds >= 1 && seconds <= 86400, 'Duration must be 1..86400 seconds');
    assert.ok(Number.isInteger(interval) && interval >= 1 && interval <= 60000, 'Interval must be 1..60000 ms');
    let args: string[] = [];
    if (values.action === 'emit') {
      const endpoints = JSON.parse(readFileSync(join(root, 'endpoints.json'), 'utf8'));
      assert.equal(readFileSync(join(root, 'serve.lock'), 'utf8'), String(endpoints.pid), 'The demo server is not running');
      process.kill(endpoints.pid, 0);
      const endpoint = values.node ? endpoints.node : endpoints.regional;
      assert.ok(typeof endpoint === 'string', 'Start serve with --node before emitting through the on-node hop');
      args = ['--endpoint', endpoint, '--duration-seconds', String(seconds), '--interval-ms', String(interval)];
    }
    if (values.language === 'rust') {
      console.log(await execute('cargo', [values.action === 'build' ? 'build' : 'run', '--quiet', '--manifest-path', cargoManifest,
        '--bin', 'emitter', ...(values.action === 'build' ? [] : ['--', ...args])],
        undefined, { CARGO_TARGET_DIR: cargoTarget }));
      return;
    }
    const nativeManifest = join(sdk, 'native/Cargo.toml');
    const name = readFileSync(nativeManifest, 'utf8').match(/^name = "([a-z0-9_]+)"$/m)?.[1];
    assert.ok(name, 'Missing native adapter library');
    const nativeTarget = resolve('target/arrowcol-sdk-native-target');
    await execute('cargo', ['build', '--quiet', '--release', '--manifest-path', nativeManifest, '--target-dir', nativeTarget]);
    ensureDirectory(workspace, join(sdk, 'native/target/release'));
    const libraries = process.platform === 'win32' ? [`${name}.dll`, `${name}.dll.lib`]
      : [process.platform === 'darwin' ? `lib${name}.dylib` : `lib${name}.so`];
    for (const file of libraries) copyFileSync(join(nativeTarget, 'release', file), join(sdk, 'native/target/release', file));
    if (values.language === 'csharp') {
      console.log(await execute('dotnet', values.action === 'build'
        ? ['build', csProject, '--configuration', 'Release', '--nologo']
        : ['run', '--project', csProject, '--configuration', 'Release', '--', ...args]));
    } else {
      await execute('cmake', ['-S', cppProject, '-B', cppBuild]);
      await execute('cmake', ['--build', cppBuild, '--config', 'Release', '--parallel', '2']);
      if (values.action === 'emit') {
        console.log(await execute(join(cppBuild, process.platform === 'win32' ? 'Release/emitter.exe' : 'emitter'), args));
      } else console.log(`C++ sample built: ${cppBuild}`);
    }
    return;
  }

  const port = Number(values['port-base']);
  assert.ok(Number.isInteger(port) && port > 1024 && port <= 65532, 'Port base must be 1025..65532');
  for (let offset = 0; offset < (values.node ? 4 : 2); offset++) {
    const probe = createServer();
    await new Promise<void>((done, reject) => {
      probe.once('error', reject);
      probe.listen(port + offset, '127.0.0.1', done);
    });
    await new Promise<void>((done, reject) => probe.close((error) => error ? reject(error) : done()));
  }
  const lock = join(root, 'serve.lock');
  writeFileSync(lock, String(process.pid), { flag: 'wx' });
  const children: ChildProcess[] = [];
  let stopping = false;
  let stop: () => void = () => { throw new Error('Shutdown handler not initialized'); };
  const stopped = new Promise<void>((done) => { stop = done; });
  process.once('SIGINT', stop); process.once('SIGTERM', stop);
  async function start(role: 'regional' | 'node', flightPort: number): Promise<void> {
    const instance = `stream-demo-${role}-${run.id}`;
    const sink = role === 'regional'
      ? { type: 'exporter:arrow_deltalake', config: {
        base_path: warehouse, routing: { type: 'standalone' },
        commit: { batch_window: '100ms' }, batching: { max_age: '1s' },
      } }
      : { type: 'exporter:arrow_flight', config: {
        endpoint: `http://127.0.0.1:${port}`, registration: 'shared_catalog', timeout: '30s',
      } };
    const config = {
      version: 'otel_dataflow/v1', engine: {},
      policies: { resources: { core_allocation: { type: 'core_count', count: 1 } } },
      groups: { arrowcol: { pipelines: { ingest: { nodes: {
        flight: { type: 'receiver:arrow_flight', config: {
          listening_addr: `127.0.0.1:${flightPort}`, admin_listening_addr: `127.0.0.1:${flightPort + 1}`,
          catalog: { type: 'remote', endpoint: run.registry.endpoint, maximum_cache_age: '10s' },
          routing: { type: 'standalone' },
          registration: { instance_key: instance, flight_endpoint: `127.0.0.1:${flightPort}` },
        } }, sink,
      }, connections: [{ from: 'flight', to: 'sink' }] } } } },
    };
    const path = join(root, `${role}.json`), log = join(root, `${role}.log`);
    writeJson(path, config);
    const fd = openSync(log, 'a');
    let child: ChildProcess;
    try {
      child = spawn(resolve(`target/debug/arrowcol-dataflow-rust${process.platform === 'win32' ? '.exe' : ''}`),
        ['--num-cores', '1', '--config', path], {
          shell: false, stdio: ['ignore', fd, fd], env: { ...process.env, ...registryEnvironment(workspace, run.registry) },
        });
      children.push(child);
    } finally { closeSync(fd); }
    let spawnError: Error | undefined;
    child.on('error', (error) => { spawnError = error; stop(); });
    child.on('exit', () => { if (!stopping) stop(); });
    for (let attempt = 0; attempt < 120; attempt++) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`${role} exited; inspect ${log}`);
      try {
        const response = await fetch(`http://127.0.0.1:${flightPort + 1}/readyz`, { signal: AbortSignal.timeout(1000) });
        if (response.ok) return;
      } catch (error) {
        if (!(error instanceof TypeError && error.cause instanceof Error && isCode(error.cause, 'ECONNREFUSED'))) throw error;
      }
      await delay(250);
    }
    throw new Error(`${role} not ready; inspect ${log}`);
  }
  try {
    await start('regional', port);
    if (values.node) await start('node', port + 2);
    const endpoints = { pid: process.pid, regional: `http://127.0.0.1:${port}`,
      ...(values.node ? { node: `http://127.0.0.1:${port + 2}` } : {}), warehouse, tableRoot };
    writeJson(join(root, 'endpoints.json'), endpoints);
    console.log(JSON.stringify({ ...endpoints, message: 'Ready. Ctrl+C stops only these collectors; all data is retained.' }, null, 2));
    await stopped;
    for (const child of children) {
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Collector ${child.pid} exited unexpectedly; inspect ${root}`);
    }
  } finally {
    stopping = true;
    process.removeListener('SIGINT', stop); process.removeListener('SIGTERM', stop);
    const shutdownErrors: string[] = [];
    try {
      for (const child of children.reverse()) {
        if (child.pid && child.exitCode === null && child.signalCode === null) {
          child.kill('SIGTERM');
          for (let i = 0; i < 120 && child.exitCode === null && child.signalCode === null; i++) await delay(250);
          if (child.exitCode === null && child.signalCode === null) {
            child.kill('SIGKILL'); await once(child, 'exit');
            shutdownErrors.push(`Collector ${child.pid} did not stop gracefully; inspect ${root}`);
          }
        }
      }
    } finally { unlinkSync(lock); }
    assert.equal(shutdownErrors.length, 0, shutdownErrors.join('\n'));
  }
}
main().catch((error: unknown) => { console.error(`stream-demo: ${errorMessage(error)}`); process.exitCode = 1; });
