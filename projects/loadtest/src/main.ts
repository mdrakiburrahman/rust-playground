import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';

import { parseInspection, validateSignals, type Inspection } from './validate.js';

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
  const value: unknown = JSON.parse(run(path.join(workspace, 'target/debug/parquet-inspect'), [directory]));
  return parseInspection(value);
}

function report(inspection: Inspection): void {
  console.log(`${inspection.files} Parquet files`);
  for (const [table, rows] of Object.entries(inspection.tables)) {
    console.log(`\n${table}: ${rows.length} rows`);
    console.log(`Schema: ${JSON.stringify(inspection.schemas[table])}`);
    console.log(JSON.stringify(rows.slice(0, 2), null, 2));
  }
}

function parquetCount(directory: string): number {
  return readdirSync(directory, { withFileTypes: true }).reduce((count, entry) =>
    count + (entry.isDirectory() ? parquetCount(path.join(directory, entry.name)) : Number(entry.name.endsWith('.parquet'))), 0);
}

async function e2e(): Promise<void> {
  const runId = `e2e-${randomUUID()}`;
  const directory = path.join(workspace, 'onelake/e2e', runId);
  mkdirSync(directory, { recursive: true });
  const env = environment(directory);
  const project = `rust-playground-${runId}`;
  console.log(`E2E output: ${path.relative(workspace, directory)}`);
  let failure: unknown;
  try {
    compose(project, env, ['config', '--quiet']);
    compose(project, env, ['up', '--detach', '--no-build', '--wait', '--wait-timeout', '90', 'otelcol-rust']);
    const output = compose(project, env, ['run', '--rm', '--no-deps', 'hello-world', '--telemetry', '--count', '3', '--interval-ms', '100', '--run-id', runId]);
    console.log(output.trim());
    const deadline = Date.now() + 30_000;
    while (parquetCount(directory) < 3 && Date.now() < deadline) {
      await delay(250);
    }
    // SIGTERM drains ingress and closes Parquet writers. Never validate a
    // partially open file or let previous runs satisfy this run's assertions.
    compose(project, env, ['stop', 'otelcol-rust']);
    const container = compose(project, env, ['ps', '--all', '--quiet', 'otelcol-rust']).trim();
    const exitCode = run('docker', ['inspect', '--format', '{{.State.ExitCode}}', container]).trim();
    if (exitCode !== '0') {
      throw new Error(`Collector shutdown failed with exit code ${exitCode}`);
    }
    const result = inspect(directory);
    writeFileSync(path.join(directory, 'inspection.json'), JSON.stringify(result, null, 2));
    validateSignals(result, runId, 3);
    console.log(`PASS: logs, metric datapoints, and correlated traces read from ${result.files} Parquet files`);
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
