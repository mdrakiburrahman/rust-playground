import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

import { commitSpansCores, parseInspection, signalsReady, validateSignals, type Inspection } from './validate.js';

const workspace = realpathSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..'));
const composeFile = path.join(workspace, 'projects/loadtest/compose.yaml');

function run(command: string, args: string[], env: NodeJS.ProcessEnv = process.env, timeout = 300_000): string {
  const result = spawnSync(command, args, { cwd: workspace, env, encoding: 'utf8', timeout, maxBuffer: 64 * 1024 * 1024 });
  if (result.error || result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} failed: ${result.error?.message ?? result.status}\n${result.stdout}\n${result.stderr}`);
  }
  return result.stdout;
}

export function mapHostPath(localPath: string, mounts: unknown): string {
  if (!Array.isArray(mounts)) {
    throw new Error('Docker did not return container mounts');
  }
  const bindings = mounts.filter((mount): mount is { Type: string; Source: string; Destination: string } =>
    typeof mount === 'object' && mount !== null &&
    mount.Type === 'bind' && typeof mount.Source === 'string' && typeof mount.Destination === 'string')
    .filter((mount) => localPath === mount.Destination || localPath.startsWith(`${mount.Destination}/`))
    .sort((left, right) => right.Destination.length - left.Destination.length);
  const binding = bindings[0];
  if (!binding) {
    throw new Error(`No host bind mount covers ${localPath}; set LOADTEST_HOST_WORKSPACE explicitly`);
  }
  return path.join(binding.Source, path.relative(binding.Destination, localPath));
}

function hostWorkspace(): string {
  if (process.env.LOADTEST_HOST_WORKSPACE) {
    return path.resolve(process.env.LOADTEST_HOST_WORKSPACE);
  }
  if (!existsSync('/.dockerenv') && !process.env.REMOTE_CONTAINERS_IPC) {
    return workspace;
  }
  const mounts: unknown = JSON.parse(run('docker', ['inspect', '--format', '{{json .Mounts}}', hostname()]));
  return mapHostPath(workspace, mounts);
}

function environment(directory: string): NodeJS.ProcessEnv {
  if (process.getuid?.() === 0) {
    throw new Error('Run loadtest as a non-root user so output stays locally readable');
  }
  return {
    ...process.env,
    LOADTEST_UID: String(process.getuid?.() ?? 10001),
    LOADTEST_GID: String(process.getgid?.() ?? 10001),
    ONELAKE_HOST_PATH: path.join(hostWorkspace(), path.relative(workspace, directory)),
  };
}

function compose(project: string, env: NodeJS.ProcessEnv, args: string[], timeout = 300_000): string {
  return run('docker', ['compose', '--project-name', project, '--file', composeFile, ...args], env, timeout);
}

function inspect(directory: string): Inspection {
  const value: unknown = JSON.parse(run(path.join(workspace, 'target/debug/delta-inspect'), [directory]));
  return parseInspection(value);
}

function report(inspection: Inspection): void {
  console.log(`${inspection.files} committed Delta data files`);
  for (const [table, rows] of Object.entries(inspection.tables)) {
    console.log(`\n${table}: ${rows.length} rows, version ${inspection.versions[table]}`);
    console.log(`Schema: ${JSON.stringify(inspection.schemas[table])}`);
    console.log(JSON.stringify(rows.slice(0, 2), null, 2));
  }
}

function tablesInitialized(directory: string): boolean {
  const tables = readdirSync(directory, { withFileTypes: true }).filter((entry) => entry.isDirectory());
  return tables.length > 0 && tables.every((table) =>
    existsSync(path.join(directory, table.name, '_delta_log/00000000000000000000.json')));
}

async function waitForSignals(directory: string, runIds: string[], container: string): Promise<Inspection> {
  const deadline = Date.now() + 90_000;
  while (Date.now() < deadline) {
    if (run('docker', ['inspect', '--format', '{{.State.Running}}', container]).trim() !== 'true') {
      throw new Error('Collector exited before committing the expected signals');
    }
    if (tablesInitialized(directory)) {
      const result = inspect(directory);
      if (runIds.every((runId) => signalsReady(result, runId, 3))) {
        for (const runId of runIds) validateSignals(result, runId, 3);
        return result;
      }
    }
    await delay(1000);
  }
  throw new Error(`Timed out waiting for committed Delta signals for ${runIds.join(', ')}`);
}

function crossCoreCommit(directory: string, inspection: Inspection): boolean {
  const log = path.join(directory, 'logs/_delta_log');
  for (const file of readdirSync(log).filter((name) => /^\d{20}\.json$/u.test(name))) {
    const paths = readFileSync(path.join(log, file), 'utf8').trim().split('\n').flatMap((line) => {
      const action: unknown = JSON.parse(line);
      assert(typeof action === 'object' && action !== null && !Array.isArray(action), 'Invalid Delta action');
      if (!('add' in action)) return [];
      const add = action.add;
      assert(typeof add === 'object' && add !== null && 'path' in add && typeof add.path === 'string', 'Invalid Delta add action');
      return [add.path];
    });
    if (commitSpansCores(inspection, 'logs', paths)) return true;
  }
  return false;
}

function emit(project: string, env: NodeJS.ProcessEnv, runId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['compose', '--project-name', project, '--file', composeFile,
      'run', '--rm', '--no-deps', 'hello-world', '--telemetry', '--count', '3', '--interval-ms', '100', '--run-id', runId],
    { cwd: workspace, env, stdio: ['ignore', 'ignore', 'pipe'] });
    let stderr = '';
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => { stderr = `${stderr}${chunk}`.slice(-32_768); });
    const timer = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error(`Emitter timed out for ${runId}`));
    }, 60_000);
    child.once('error', (error) => { clearTimeout(timer); reject(error); });
    child.once('exit', (code) => {
      clearTimeout(timer);
      if (code === 0) resolve();
      else reject(new Error(`Emitter ${runId} failed (${code}): ${stderr}`));
    });
  });
}

function stopCollector(project: string, env: NodeJS.ProcessEnv): void {
  compose(project, env, ['stop', 'otelcol-rust']);
  const container = compose(project, env, ['ps', '--all', '--quiet', 'otelcol-rust']).trim();
  const exitCode = run('docker', ['inspect', '--format', '{{.State.ExitCode}}', container]).trim();
  if (exitCode !== '0') {
    throw new Error(`Collector shutdown failed with exit code ${exitCode}`);
  }
}

async function e2e(): Promise<void> {
  const runId = `e2e-${randomUUID()}`;
  const directory = path.join(workspace, 'onelake/e2e', runId);
  mkdirSync(directory, { recursive: true });
  const env = { ...environment(directory), LOADTEST_CORES: '2' };
  const project = `rust-playground-${runId}`;
  console.log(`E2E output: ${path.relative(workspace, directory)}`);
  let failure: unknown;
  try {
    compose(project, env, ['config', '--quiet']);
    compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90', 'otelcol-rust']);
    const container = compose(project, env, ['ps', '--all', '--quiet', 'otelcol-rust']).trim();
    const runIds: string[] = [];
    let committed: Inspection | undefined;
    for (let round = 0; round < 3; round++) {
      const emitters = Array.from({ length: 4 }, (_, index) => `${runId}-${round}-${index}`);
      runIds.push(...emitters);
      await Promise.all(emitters.map((id) => emit(project, env, id)));
      committed = await waitForSignals(directory, runIds, container);
      if (crossCoreCommit(directory, committed)) break;
    }
    assert(committed && crossCoreCommit(directory, committed), 'No Delta commit batched files from different collector cores');
    const shutdownId = `${runId}-shutdown`;
    await emit(project, env, shutdownId);
    assert(!signalsReady(inspect(directory), shutdownId, 3), 'Shutdown test had no unpublished telemetry');
    stopCollector(project, env);
    const first = inspect(directory);
    runIds.push(shutdownId);
    for (const id of runIds) validateSignals(first, id, 3);
    const restartId = `${runId}-restart`;
    compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90', 'otelcol-rust']);
    await emit(project, env, restartId);
    stopCollector(project, env);
    const result = inspect(directory);
    writeFileSync(path.join(directory, 'inspection.json'), JSON.stringify(result, null, 2));
    for (const id of runIds) validateSignals(result, id, 3);
    validateSignals(result, restartId, 3);
    for (const table of ['logs', 'spans', 'number_data_points']) {
      if (result.versions[table] <= first.versions[table]) {
        throw new Error(`Delta version did not advance after restart for ${table}`);
      }
    }
    console.log(`PASS: multi-core batched commits, forced shutdown, restart, and three signals from ${result.files} committed Delta files`);
  } catch (error) {
    failure = error;
  } finally {
    try {
      writeFileSync(path.join(directory, 'compose.log'), compose(project, env, ['logs', '--no-color']));
    } catch (error) {
      console.error(`Unable to capture Compose logs: ${String(error)}`);
      failure ??= error;
    }
    try {
      compose(project, env, ['down', '--remove-orphans']);
    } catch (error) {
      console.error(`Compose cleanup failed: ${String(error)}`);
      failure ??= error;
    }
  }
  if (failure) {
    throw failure;
  }
}

async function main(): Promise<void> {
  const action = process.argv[2];
  if (action === 'e2e') {
    await e2e();
    return;
  }
  const directory = path.join(workspace, 'onelake/demo');
  mkdirSync(directory, { recursive: true });
  if (action === 'inspect') {
    report(inspect(process.env.ONELAKE_INSPECT_PATH ? path.resolve(process.env.ONELAKE_INSPECT_PATH) : directory));
    return;
  }
  const env = environment(directory);
  const project = 'rust-playground-loadtest';
  switch (action) {
    case 'up':
      console.log(compose(project, env, ['up', '--detach', '--build', '--wait', '--wait-timeout', '90'], 1_800_000));
      break;
    case 'down':
      console.log(compose(project, env, ['down', '--remove-orphans']));
      break;
    case 'logs':
      console.log(compose(project, env, ['logs', '--no-color', '--tail', '100']));
      break;
    default:
      throw new Error('Usage: loadtest <up|down|logs|inspect|e2e>');
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
