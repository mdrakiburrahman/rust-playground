import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import {
  chmodSync, closeSync, copyFileSync, existsSync, mkdirSync, openSync, readFileSync, realpathSync, writeFileSync,
} from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  benchmarkUsage,
  BENCHMARK_PROJECT,
  parseBenchmarkOptions,
  type BenchmarkOptions,
} from './cli.js';
import { startResourceSampler } from './resource-sampler.js';
import { beginAttemptEvidence, sealAttemptEvidence, stoppedAttemptMutators, type StoppedMutator } from './attempt-evidence.js';
import { writeProvenance } from './provenance.js';
import { runQuiverCrash, QUIVER_RECOVERY_FILE } from './quiver-crash.js';
import { killQuiverPublishers, recordQuiverSuccessors, type KilledPublishers } from './quiver-process-exits.js';
import { OwnedProcessError, runOwnedProcess } from './owned-process.js';
import { admitPublisherRelease, sqlIdentity, type SqlIdentity } from './publisher-release.js';
import { startQapObserver } from './qap-observer.js';
import { readStressPlan, runStressDriver, stressInputBudget } from './stress-driver.js';
import { LocalStressRuntime } from './stress-runtime.js';
import {
  benchmarkComposeFiles, benchmarkQuiver, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs, dataflowConfigFile,
} from './runtime-config.js';
import { acquireBenchmarkController, discardUnsealedAttempts, requirePreviousAttemptEvidence, resetBenchmark, stopBenchmark, stopReceipt, type BenchmarkController } from './lifecycle.js';
import { azureTargets, modeEnvironment, stageAzureCliConfig, type AzureTargets } from './mode.js';
import { kustoComposeStartArguments, kustoStatusSummary, readKustoStatus, requireHealthyKusto, safeKustoError, startKusto } from './kusto.js';
import { kustoExplorerStartArguments, runGrafanaGate, runUiGate } from './ui-gate.js';

const workspace = realpathSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..'));
const composeFile = path.join(workspace, 'projects/loadtest/compose.yaml');

const startedAt = Date.now();

function step(message: string): void {
  const elapsed = Math.round((Date.now() - startedAt) / 1000);
  const stamp = `${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}`;
  console.log(`\n[loadtest +${stamp}] ${message}`);
}

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

function hostPath(localPath: string): string {
  const absolute = path.resolve(localPath);
  return path.join(hostWorkspace(), path.relative(workspace, absolute));
}

function environment(directory: string): NodeJS.ProcessEnv {
  if (process.getuid?.() === 0) {
    throw new Error('Run loadtest as a non-root user so output stays locally readable');
  }
  return {
    ...process.env,
    LOADTEST_UID: String(process.getuid?.() ?? 10001),
    LOADTEST_GID: String(process.getgid?.() ?? 10001),
    LOADTEST_WORKSPACE: workspace,
    ONELAKE_HOST_PATH: path.join(hostWorkspace(), path.relative(workspace, directory)),
    LOADTEST_SCRATCH_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp')),
    LOADTEST_BIN_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp/bin')),
  };
}

function compose(project: string, env: NodeJS.ProcessEnv, args: string[], timeout = 300_000): string {
  const files = benchmarkComposeFiles(composeFile, env);
  return run('docker', ['compose', '--project-name', project, ...files, ...args], env, timeout);
}

function composeAsync(project: string, env: NodeJS.ProcessEnv, args: string[], timeout = 600_000): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn('docker', ['compose', '--project-name', project, ...benchmarkComposeFiles(composeFile, env), ...args], {
      cwd: workspace, env, timeout, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8').on('data', (chunk: string) => { stdout += chunk; });
    child.stderr.setEncoding('utf8').on('data', (chunk: string) => { stderr += chunk; });
    child.once('error', reject);
    child.once('close', (code, signal) => {
      if (code === 0 && signal === null) resolve(stdout);
      else reject(new Error(`docker compose ${args.join(' ')} failed: ${signal ?? code}\n${stderr}`));
    });
  });
}

function composeToFile(project: string, env: NodeJS.ProcessEnv, args: string[], file: string): void {
  const fd = openSync(file, 'w');
  try {
    const result = spawnSync('docker', ['compose', '--project-name', project, ...benchmarkComposeFiles(composeFile, env), ...args], {
      cwd: workspace, env, stdio: ['ignore', fd, 'inherit'], timeout: 600_000,
    });
    if (result.error || result.status !== 0) throw new Error(`docker compose ${args.join(' ')} failed: ${result.error?.message ?? result.status}`);
  } finally {
    closeSync(fd);
  }
}

function composeInteractive(project: string, env: NodeJS.ProcessEnv, args: string[], timeout?: number): void {
  const files = benchmarkComposeFiles(composeFile, env);
  const result = spawnSync('docker', ['compose', '--project-name', project, ...files, ...args], {
    cwd: workspace,
    env,
    stdio: 'inherit',
    timeout,
  });
  if (result.error || result.status !== 0) {
    throw new Error(`docker compose ${args.join(' ')} failed: ${result.error?.message ?? result.status}`);
  }
}

async function runInteractive(
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv,
  controller: BenchmarkController,
  timeout?: number,
): Promise<void> {
  const child = spawn(command, args, {
    cwd: workspace,
    env,
    stdio: 'inherit',
    timeout,
  });
  controller.setChild(child);
  try {
    await new Promise<void>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => {
        if (code === 0 && signal === null) resolve();
        else reject(new Error(`${command} ${args.join(' ')} failed: ${signal ?? code}`));
      });
    });
  } finally {
    controller.setChild(undefined);
  }
}

function writeBenchmarkSecrets(): { edge: string; internal: string } {
  const root = path.join(workspace, 'projects/loadtest/.temp/secrets/latest');
  mkdirSync(root, { recursive: true });
  const edge = path.join(root, 'edge-hmac.txt');
  const internal = path.join(root, 'internal-hmac.txt');
  writeFileSync(edge, `${randomBytes(48).toString('base64')}\n`, { mode: 0o600 });
  writeFileSync(internal, `${randomBytes(48).toString('base64')}\n`, { mode: 0o600 });
  return { edge, internal };
}

function cargoTargetDirectory(): string {
  const target = process.env.CARGO_TARGET_DIR
    ? path.resolve(workspace, process.env.CARGO_TARGET_DIR)
    : path.join(workspace, 'target');
  return target;
}

function stageHelperBinaries(): string {
  const source = path.join(cargoTargetDirectory(), 'debug');
  const destination = path.join(workspace, 'projects/loadtest/.temp/bin/latest');
  mkdirSync(destination, { recursive: true });
  for (const binary of ['arrowcol-loadgen', 'arrowcol-sdk-gen']) {
    const input = path.join(source, binary);
    if (!existsSync(input)) throw new Error(`Required helper artifact is missing: ${input}`);
    const output = path.join(destination, binary);
    copyFileSync(input, output);
    chmodSync(output, 0o755);
  }
  return destination;
}

function writeDirectConfig(directory: string, service: string, network: BenchmarkNetwork, quiver: boolean, azure?: AzureTargets, privateEnv?: NodeJS.ProcessEnv): void {
  const state = path.join(directory, `direct-state-${service.split('-')[1]}`);
  mkdirSync(state, { recursive: true });
  const generationFile = path.join(state, 'generation');
  const previous = existsSync(generationFile)
    ? Number(readFileSync(generationFile, 'utf8').trim())
    : 0;
  if (!Number.isSafeInteger(previous) || previous < 0 || previous >= 2_147_483_646) {
    throw new Error(`Invalid persisted direct collector generation for ${service}`);
  }
  const generation = previous + 1;
  const address = network.direct;
  const template = readFileSync(path.join(workspace, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8');
  const runtime = buildBenchmarkRuntimeConfigs(template,
    readFileSync(path.join(workspace, dataflowConfigFile(quiver)), 'utf8'),
    { instanceKey: `loadtest-${service}`, advertisedEndpoint: `${address}:50051`, generation },
    { quiver, mode: azure ? 'azure' : 'local', azure });
  writeFileSync(path.join(state, 'direct.yaml'), runtime.directYaml);
  const dataflowState = path.join(directory, 'dataflow-state-1');
  mkdirSync(dataflowState, { recursive: true });
  writeFileSync(path.join(dataflowState, 'dataflow.yaml'), runtime.dataflowYaml);
  writeFileSync(path.join(directory, 'router-state/compose-runtime.json'),
    `${buildBenchmarkComposeOverride(hostPath(directory), { quiver, ...(azure ? {
      mode: 'azure', azure, azureConfigHostDirectory: hostPath(path.join(directory, 'azure-cli')), azureEnvironment: privateEnv,
    } : {}) })}\n`);
  writeFileSync(generationFile, `${generation}\n`);
}

function writeRuntimeConfigs(directory: string, network: BenchmarkNetwork, quiver: boolean, azure?: AzureTargets, privateEnv?: NodeJS.ProcessEnv): void {
  writeDirectConfig(directory, 'direct-1', network, quiver, azure, privateEnv);
  writeRouterConfig(directory, 500);
}

function writeRouterConfig(directory: string, refreshIntervalMs: number): void {
  const config = readFileSync(path.join(workspace, 'projects/loadtest/config/router.yaml'), 'utf8')
    .replace(/refresh_interval_ms: [0-9]+/u, `refresh_interval_ms: ${refreshIntervalMs}`);
  writeFileSync(
    path.join(directory, 'router-state/config.yaml'),
    config,
  );
}

function benchmarkEnvironment(
  directory: string,
  options: BenchmarkOptions,
  network: BenchmarkNetwork,
  binaryDirectory: string,
  privateEnv: NodeJS.ProcessEnv,
  azure?: AzureTargets,
): NodeJS.ProcessEnv {
  const secrets = writeBenchmarkSecrets();
  const infra = path.join(workspace, 'projects/arrowcol-infra/secrets');
  return {
    ...environment(directory),
    ...privateEnv,
    ARROWCOL_MODE: options.mode,
    ARROWCOL_PARTITION_GRANULARITY: 'minute',
    LOADTEST_DIRECT_DELTA_ROOT: azure?.directRoot ?? '/delta/direct',
    LOADTEST_DATAFLOW_DELTA_ROOT: azure?.dataflowRoot ?? '/delta/dataflow',
    ...(azure ? {
      ARROWCOL_AZURE_TABLE_ROOT: azure.root,
      LOADTEST_DIRECT_ROOT: azure.directRoot,
      LOADTEST_DATAFLOW_ROOT: azure.dataflowRoot,
      LOADTEST_MAINTENANCE_ROOT: `${azure.directRoot}/telemetry/sqlserver/`,
      LOADTEST_TENANT_SOURCE: `${azure.directRoot}/telemetry/sqlserver/sqlserver_cpu_utilization`,
      LOADTEST_TENANT_TARGET: `${azure.tenantRoot}/sqlserver_cpu_utilization`,
      LOADTEST_TENANT_CHECKPOINT: `${azure.checkpointRoot}/tenant-cpu`,
      AZURE_USE_AZURE_CLI: 'true',
    } : {}),
    LOADTEST_COMPOSE_OVERRIDE_FILE: path.join(directory, 'router-state/compose-runtime.json'),
    LOADTEST_WARMUP: `${options.warmupSeconds}s`,
    ...(options.measureSeconds === undefined ? {} : { LOADTEST_MEASURE: `${options.measureSeconds}s` }),
    LOADTEST_CAPTURE_SETTLE: `${options.captureSettleSeconds}s`,
    LOADTEST_DRAIN_TIMEOUT: `${options.drainTimeoutSeconds}s`,
    LOADTEST_REQUEST_TIMEOUT: `${options.requestTimeoutSeconds}s`,
    LOADTEST_CONTINUOUS: String(options.measureSeconds === undefined),
    LOADTEST_KEEP_STACK: 'true',
    LOADTEST_LANES: String(options.concurrency),
    LOADTEST_BATCH_ROWS: String(options.batchRows),
    LOADTEST_SEED: String(options.seed),
    LOADTEST_SCENARIOS: 'paired',
    LOADTEST_QUIVER: String(options.quiver),
    // The crash demo keeps retrying through the collector outage instead of exhausting attempts.
    ...(options.crashAfterSeconds === undefined ? {} : { LOADTEST_RETRY_WINDOW: '5m' }),
    LOADTEST_EDGE_HMAC_FILE: hostPath(secrets.edge),
    LOADTEST_INTERNAL_HMAC_FILE: hostPath(secrets.internal),
    LOADTEST_SCRATCH_HOST_PATH: hostPath(path.join(workspace, 'projects/loadtest/.temp')),
    LOADTEST_BIN_HOST_PATH: hostPath(binaryDirectory),
    LOADTEST_SUBNET: network.subnet,
    LOADTEST_SQL_IP: network.sql,
    LOADTEST_HIVE_IP: network.hive,
    LOADTEST_ROUTER_IP: network.router,
    LOADTEST_DIRECT_1_IP: network.direct,
    LOADTEST_DATAFLOW_1_IP: network.dataflow,
    LOADTEST_KUSTO_IP: network.kusto,
    LOADTEST_KUSTO_EXPLORER_IP: network.kustoExplorer,
    LOADTEST_KUSTO_BRIDGE_IP: network.kustoBridge,
    LOADTEST_PLAYWRIGHT_IP: network.playwright,
    LOADTEST_LGTM_IP: network.sql.replace(/\.2$/u, '.40'),
    LOADTEST_CADVISOR_IP: network.sql.replace(/\.2$/u, '.105'),
    LOADTEST_PORTAINER_IP: network.sql.replace(/\.2$/u, '.106'),
    SQLSERVER_SA_PASSWORD_FILE: hostPath(path.join(infra, 'sqlserver-sa-password.txt')),
    ARROWCOL_APP_PASSWORD_FILE: hostPath(path.join(infra, 'arrowcol-app-password.txt')),
    ARROWCOL_MIGRATOR_PASSWORD_FILE: hostPath(path.join(infra, 'arrowcol-migrator-password.txt')),
    HIVE_METASTORE_PASSWORD_FILE: hostPath(path.join(infra, 'hive-metastore-password.txt')),
  };
}

interface BenchmarkContext {
  directory: string;
  env: NodeJS.ProcessEnv;
  network: BenchmarkNetwork;
  networkName?: string;
  project: string;
  options: BenchmarkOptions;
  workspaceContainer?: string;
}

export interface BenchmarkNetwork {
  subnet: string;
  sql: string;
  hive: string;
  router: string;
  direct: string;
  dataflow: string;
  kusto: string;
  kustoExplorer: string;
  kustoBridge: string;
  playwright: string;
}

export const benchmarkCoreServices = [
  'sqlserver', 'database-bootstrap', 'arrowcol-migrate', 'hive-schema', 'hive-metastore',
  'lgtm', 'cadvisor', 'portainer', 'direct-1', 'dataflow-1', 'router',
];

function cidrRange(value: string): [number, number] | undefined {
  const match = /^(\d+)\.(\d+)\.(\d+)\.(\d+)\/(\d+)$/u.exec(value);
  if (!match) return undefined;
  const octets = match.slice(1, 5).map(Number);
  const prefix = Number(match[5]);
  if (octets.some((octet) => !Number.isInteger(octet) || octet < 0 || octet > 255) ||
      !Number.isInteger(prefix) || prefix < 0 || prefix > 32) return undefined;
  const address = (((octets[0] * 256 + octets[1]) * 256 + octets[2]) * 256 + octets[3]) >>> 0;
  const mask = prefix === 0 ? 0 : (0xffff_ffff << (32 - prefix)) >>> 0;
  const start = (address & mask) >>> 0;
  const size = 2 ** (32 - prefix);
  return [start, start + size - 1];
}

function selectBenchmarkNetwork(): BenchmarkNetwork {
  const ids = run('docker', ['network', 'ls', '--quiet']).trim().split('\n').filter(Boolean);
  const used = ids.length === 0
    ? []
    : (JSON.parse(run('docker', ['network', 'inspect', ...ids])) as Array<{
      IPAM?: { Config?: Array<{ Subnet?: string }> };
    }>).flatMap((network) => network.IPAM?.Config ?? [])
      .flatMap((config) => config.Subnet ? [cidrRange(config.Subnet)] : [])
      .filter((range): range is [number, number] => range !== undefined);
  for (let index = 0; index < 256; index++) {
    const base = `10.89.${index}`;
    const subnet = `${base}.0/24`;
    const range = cidrRange(subnet)!;
    if (used.some(([start, end]) => range[0] <= end && start <= range[1])) continue;
    return {
      subnet, sql: `${base}.2`, hive: `${base}.3`, router: `${base}.10`,
      direct: `${base}.21`, dataflow: `${base}.31`,
      kusto: `${base}.50`, kustoExplorer: `${base}.51`, playwright: `${base}.52`, kustoBridge: `${base}.53`,
    };
  }
  throw new Error('No available benchmark subnet in 10.89.0.0/16');
}

function connectWorkspaceContainer(
  project: string,
  env: NodeJS.ProcessEnv,
): { container?: string; network?: string } {
  const container = hostname();
  const inspected = spawnSync('docker', ['inspect', container], { cwd: workspace, stdio: 'ignore' });
  if (inspected.status !== 0) return {};
  const sqlContainer = compose(project, env, ['--profile', 'benchmark', 'ps', '--quiet', 'sqlserver']).trim();
  if (!sqlContainer) throw new Error('SQL Server container is unavailable for workspace network attachment');
  const network = run('docker', [
    'inspect', '--format', '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{"\\n"}}{{end}}', sqlContainer,
  ]).trim().split('\n').find(Boolean);
  if (!network) throw new Error('Benchmark Compose network was not found');
  const connected = spawnSync('docker', ['network', 'connect', network, container], {
    cwd: workspace,
    stdio: 'ignore',
  });
  if (connected.status !== 0) {
    const networks = run('docker', [
      'inspect', '--format', '{{json .NetworkSettings.Networks}}', container,
    ]);
    if (!Object.hasOwn(JSON.parse(networks) as object, network)) {
      throw new Error(`Could not attach workspace container to ${network}`);
    }
  }
  return { container, network };
}

export function localLoadgenEnvironment(
  context: Pick<BenchmarkContext, 'directory' | 'env' | 'network' | 'options'>,
  results: string,
): NodeJS.ProcessEnv {
  const secrets = path.join(workspace, 'projects/loadtest/.temp/secrets/latest');
  const infra = path.join(workspace, 'projects/arrowcol-infra/secrets');
  return {
    ...context.env,
    ...(context.options.mode === 'azure' ? { AZURE_CONFIG_DIR: path.join(context.directory, 'azure-cli/loadgen') } : {}),
    LOADTEST_RESULTS: results,
    LOADTEST_DIRECT_ROOT: context.options.mode === 'azure' ? context.env.LOADTEST_DIRECT_ROOT : path.join(context.directory, 'direct'),
    LOADTEST_DATAFLOW_ROOT: context.options.mode === 'azure' ? context.env.LOADTEST_DATAFLOW_ROOT : path.join(context.directory, 'dataflow'),
    LOADTEST_ROUTER_ENDPOINT: `http://${context.network.router}:50050`,
    LOADTEST_CATALOG_ENDPOINT: `http://${context.network.direct}:50051`,
    LOADTEST_RECEIPT_ENDPOINT: `http://${context.network.router}:50050`,
    LOADTEST_EDGE_SECRET_FILE: path.join(secrets, 'edge-hmac.txt'),
    LOADTEST_INTERNAL_SECRET_FILE: path.join(secrets, 'internal-hmac.txt'),
    LOADTEST_SCRATCH_ROOT: 'projects/loadtest/.temp/loadgen/latest',
    LOADTEST_DIRECT_ADMIN_ENDPOINTS: `http://${context.network.direct}:8081`,
    LOADTEST_DATAFLOW_ADMIN_ENDPOINTS: `http://${context.network.dataflow}:32011`,
    LOADTEST_KUSTO_ENDPOINT: context.options.mode === 'azure' ? context.env.AZURE_KUSTO_URI : `http://${context.network.kusto}:8080`,
    LOADTEST_KUSTO_BRIDGE_ENDPOINT: context.options.mode === 'azure' ? undefined : context.env.LOADTEST_KUSTO_BRIDGE_ENDPOINT,
    ARROWCOL_SQL_HOST: context.network.sql,
    ARROWCOL_SQL_PASSWORD_FILE: path.join(infra, 'arrowcol-app-password.txt'),
    ARROWCOL_SDK_GEN: path.join(cargoTargetDirectory(), 'debug/arrowcol-sdk-gen'),
  };
}

/** Hard backstop for one non-load loadgen command; the runner kills the command after it. */
export const LOADGEN_PHASE_TIMEOUT_MS = 600_000;
/** Part of the backstop left for validate's fail-closed report; unpreemptible reader work can consume it. */
export const VALIDATE_RESERVE_MS = 30_000;

/**
 * Timeout and private environment for one loadgen command. Validate alone receives an absolute
 * deadline inside the unchanged backstop; its inner waits stop cooperatively at that deadline and
 * write a failed report, while the backstop remains the only hard bound. An inherited deadline
 * never bounds run or drain.
 */
export function loadgenSpawnPlan(
  command: 'run' | 'drain' | 'validate' | 'publisher-release' | 'final-compaction',
  options: Pick<BenchmarkContext['options'], 'measureSeconds' | 'warmupSeconds'>,
  environment: NodeJS.ProcessEnv,
  nowMs: number,
): { timeout: number | undefined; env: NodeJS.ProcessEnv } {
  const env: NodeJS.ProcessEnv = { ...environment, LOADTEST_COLLECTORS_DRAINED: String(command === 'validate') };
  delete env.LOADTEST_VALIDATE_DEADLINE_UNIX_MS;
  if (command === 'validate') env.LOADTEST_VALIDATE_DEADLINE_UNIX_MS = String(nowMs + LOADGEN_PHASE_TIMEOUT_MS - VALIDATE_RESERVE_MS);
  const timeout = command !== 'run'
    ? LOADGEN_PHASE_TIMEOUT_MS
    : options.measureSeconds === undefined ? undefined : (options.warmupSeconds + options.measureSeconds + 600) * 1000;
  return { timeout, env };
}

async function runLocalLoadgen(
  context: Pick<BenchmarkContext, 'directory' | 'env' | 'network' | 'options'>,
  command: 'run' | 'drain' | 'validate' | 'publisher-release' | 'final-compaction',
  results: string,
  controller: BenchmarkController,
): Promise<void> {
  const plan = loadgenSpawnPlan(command, context.options, localLoadgenEnvironment(context, results), Date.now());
  await runInteractive(
    path.join(cargoTargetDirectory(), 'debug/arrowcol-loadgen'),
    [command, ...(command === 'final-compaction' && context.options.finalCompaction === false ? ['--skip-final-compaction'] : [])],
    plan.env,
    controller,
    plan.timeout,
  );
}

export function portainerAccess(port: string, passwordFile: string): string {
  // Portainer trims exactly one trailing newline from --admin-password-file.
  const password = readFileSync(passwordFile, 'utf8').replace(/\n$/u, '');
  return `Portainer: https://localhost:${port} (port ${port}, username admin, password ${password}; self-signed TLS)`;
}

export function sqlserverAccess(port: string, passwordFile: string): string {
  const password = readFileSync(passwordFile, 'utf8').replace(/\n+$/u, '');
  assert(password.length > 0 && !/[\r\n]/u.test(password), 'SQL Server password secret must be non-empty and single-line');
  return `SQL Server: localhost,${port} (host port ${port} -> container port 1433, username sa, password ${password})`;
}

export function quiverLocations(directory: string): string {
  const root = path.resolve(directory);
  return [
    'Quiver write-ahead logs and segments on disk (workspace paths)',
    '-------------------------------------------------------------',
    `  Direct:   ${path.join(root, benchmarkQuiver.directHostDirectory)}`,
    `  Dataflow: ${path.join(root, benchmarkQuiver.dataflowHostDirectory)}`,
  ].join('\n');
}

export function deltaTableLocations(directory: string): string {
  const root = path.resolve(directory);
  return [
    'Delta tables on disk (workspace paths)',
    '-------------------------------------',
    `  Direct:   ${path.join(root, 'direct')}`,
    `  Dataflow: ${path.join(root, 'dataflow')}`,
  ].join('\n');
}

async function reconcileAzure(context: BenchmarkContext, occupancyId: string): Promise<void> {
  const manifest = path.join(context.directory, 'router-state/azure-tables.json');
  run(path.join(cargoTargetDirectory(), 'debug/arrowcol-loadgen'), ['azure-manifest', '--manifest', manifest], context.env);
  // The helper waits only for absent logs; auth, ownership and malformed logs fail immediately.
  const stderrFile = path.join(context.directory, 'results/azure-reconcile.stderr.log');
  try {
    await runOwnedProcess('python3', [
      'infra/azure/bootstrap.py', 'reconcile', '--tables', manifest,
      '--occupancy-id', occupancyId, '--wait-seconds', '120',
    ], { cwd: workspace, env: { ...context.env, AZURE_CONFIG_DIR: path.join(context.directory, 'azure-cli/infra') },
      // Budget: up to 120s waiting for Delta logs, then ~3 sequential management commands per table
      // against a cluster that is serving the live load.
      timeoutMs: 600_000, stderrFile });
  } catch (error) {
    // bootstrap.py redacts private values in its own error lines.
    const detail = existsSync(stderrFile) ? readFileSync(stderrFile, 'utf8').trim().split('\n').slice(-5).join('\n') : '';
    if (detail) console.error(`[loadtest] Azure reconcile stderr (${stderrFile}):\n${detail}`);
    if (error instanceof OwnedProcessError) {
      console.error(`[loadtest] Azure reconcile outcome: ${JSON.stringify(error.outcome)} supervisor_timed_out=${error.supervisor_timed_out}`);
    }
    throw error;
  }
}

interface CloudRun {
  environment: NodeJS.ProcessEnv;
  occupancyId: string;
  reconciliationFailed: boolean;
  qapObservationFailed: boolean;
}

async function benchmark(
  options: BenchmarkOptions, controller: BenchmarkController, cloud: CloudRun,
): Promise<BenchmarkContext> {
  const stressPlan = options.stressConfiguration;
  const privateEnv: NodeJS.ProcessEnv = stressPlan
    ? { ...cloud.environment, ARROWCOL_LAYOUT_ROW_GROUP_ROWS: String(stressPlan.fixture.rowGroupRows),
      ARROWCOL_LAYOUT_PAGE_ROWS: String(stressPlan.fixture.pageRows) }
    : cloud.environment;
  const azure = options.mode === 'azure'
    ? azureTargets(privateEnv, `run-${Date.now()}-${randomBytes(4).toString('hex')}`)
    : undefined;
  step(`Load test: warmup ${options.warmupSeconds}s, concurrency ${options.concurrency}, batch rows ${options.batchRows}` +
    (options.quiver ? ', native Quiver disk persistence' : '') +
    (options.crashAfterSeconds === undefined ? '' : `, kill -9 both collectors ${options.crashAfterSeconds}s into load`));
  step('Starting fresh: removing previous loadtest containers, volumes, and generated data');
  const project = BENCHMARK_PROJECT;
  const directory = await resetBenchmark(workspace, args => run('docker', args));
  beginAttemptEvidence(workspace, directory);
  controller.checkRunning();
  step('Bootstrapping secrets and preparing benchmark');
  for (const child of [
    'direct', 'dataflow', 'tenant', 'tenant-checkpoints', 'otel', 'results', 'router-state',
    'direct-state-1',
    ...(options.quiver ? [benchmarkQuiver.directHostDirectory, benchmarkQuiver.dataflowHostDirectory] : []),
  ]) {
    mkdirSync(path.join(directory, child), { recursive: true });
  }
  if (stressPlan) writeFileSync(path.join(directory, 'results/stress-input-budget.json'),
    `${JSON.stringify(stressInputBudget(stressPlan, options), null, 2)}\n`, { flag: 'wx' });
  let network: BenchmarkNetwork;
  let env: NodeJS.ProcessEnv;
  try {
    run(path.join(workspace, 'projects/arrowcol-infra/scripts/bootstrap-secrets.sh'), []);
    mkdirSync(path.join(workspace, 'projects/loadtest/.temp'), { recursive: true });
    network = selectBenchmarkNetwork();
    if (azure) stageAzureCliConfig(directory, privateEnv);
    writeRuntimeConfigs(directory, network, options.quiver, azure, privateEnv);
    writeFileSync(path.join(directory, 'router-state/network.json'), `${JSON.stringify(network, null, 2)}\n`);
    env = benchmarkEnvironment(directory, options, network, stageHelperBinaries(), privateEnv, azure);
  } catch (error) {
    writeFileSync(path.join(directory, 'results/runner-failure.json'), `${JSON.stringify({
      timestamp: new Date().toISOString(), phase: 'preparation', kind: error instanceof Error ? error.name : 'Error',
    })}\n`);
    throw error;
  }
  const services = benchmarkCoreServices;
  let workspaceConnection: { container?: string; network?: string } = {};
  let failure: unknown;
  let phase = 'compose-validation';
  let stopSampler: (() => Promise<void>) | undefined;
  let stopQapObserver: (() => Promise<void>) | undefined;
  let stoppedMutators: StoppedMutator[] | undefined;
  let stackSql: SqlIdentity | undefined;
  const resultsDirectory = path.relative(workspace, path.join(directory, 'results'));
  try {
    step('Validating Compose configuration');
    writeProvenance(workspace, directory, project, options, env, path.join(cargoTargetDirectory(), 'debug'));
    compose(project, env, ['--profile', 'benchmark', 'config', '--quiet']);
    phase = 'image-build';
    step('Building sqlserver and hive-metastore images (can take several minutes on first run)');
    composeInteractive(project, env, [
      '--profile', 'benchmark', 'build', 'sqlserver', 'hive-metastore',
    ], 3_600_000);
    controller.checkRunning();
    phase = 'stack-start';
    step(`Starting ${services.length} services and waiting for health (up to 10 minutes)`);
    composeInteractive(project, env, [
      '--profile', 'benchmark', 'up', '--detach', '--no-build', '--wait', '--wait-timeout', '600', ...services,
    ], 3_600_000);
    controller.checkRunning();
    workspaceConnection = connectWorkspaceContainer(project, env);
    if (!azure && options.validate) stackSql = sqlIdentity(workspace, args => run('docker', args));
    if (!azure) {
    phase = 'kusto-startup';
    step('Starting optional Kusto inspection target separately from the collectors');
    const kustoStartup = await startKusto({
      endpoint: `http://${network.kusto}:8080`,
      results: path.join(directory, 'results'),
      start: () => { compose(project, env, kustoComposeStartArguments); },
    });
    step(`Kusto: http://localhost:${env.KUSTO_PORT ?? '8080'} (HTTP, no authentication; database loadtest)`);
    if (kustoStartup.state === 'degraded') {
      step(`Kusto startup DEGRADED: ${kustoStartup.error}. Load generation will continue; see results/kusto-startup.json`);
    }
    if (kustoStartup.state !== 'degraded' && privateEnv.LOADTEST_KUSTO_BRIDGE_ENDPOINT) {
      compose(project, env, ['--profile', 'benchmark', 'up', '--detach', '--no-build', '--wait', '--wait-timeout', '300', 'kusto-bridge'], 360_000);
    }
    controller.checkRunning();
    phase = 'kusto-explorer-startup';
    step('Starting kusto-explorer (connects to Kusto over the Compose network)');
    try {
      compose(project, env, kustoExplorerStartArguments, 300_000);
      step(`kusto-explorer: http://localhost:${env.KUSTO_EXPLORER_PORT ?? '8090'} (browser UI for the Kusto emulator)`);
    } catch (error) {
      if (options.validate) throw error;
      step(`kusto-explorer startup DEGRADED: ${safeKustoError(error)}. Load generation will continue`);
    }
    }
    controller.checkRunning();
    phase = 'startup-provenance';
    writeProvenance(workspace, directory, project, options, env, path.join(cargoTargetDirectory(), 'debug'), true);
    phase = 'sampler-start';
    stopSampler = await startResourceSampler({
      project, results: path.join(directory, 'results'),
      directAdmin: `http://${network.direct}:8081`,
      dataflowAdmin: `http://${network.dataflow}:32011`,
    });
    if (options.validate) {
      const endpoint = azure ? env.AZURE_KUSTO_URI : `http://${network.kusto}:8080`;
      if (!endpoint) throw new Error('Azure QAP observation requires the captured Kusto endpoint');
      stopQapObserver = startQapObserver({
        binary: path.join(cargoTargetDirectory(), 'debug/arrowcol-loadgen'), cwd: workspace,
        env: { ...env, ...(azure ? { AZURE_CONFIG_DIR: path.join(directory, 'azure-cli/qap-observer') } : {}) },
        results: path.join(directory, 'results'),
        endpoint, otlpEndpoint: `http://${env.LOADTEST_LGTM_IP}:4318`,
      });
    }
    step(
      `Grafana: http://localhost:${process.env.GRAFANA_PORT ?? 3000} (admin/admin); ` +
      `logs: docker logs -f ${project}-dataflow-1-1`,
    );
    step(portainerAccess(
      process.env.PORTAINER_PORT ?? '9444',
      path.join(workspace, 'projects/arrowcol-infra/secrets/sqlserver-sa-password.txt'),
    ));
    step(sqlserverAccess(
      process.env.SQLSERVER_PORT ?? '14333',
      path.join(workspace, 'projects/arrowcol-infra/secrets/sqlserver-sa-password.txt'),
    ));
    step(azure ? 'Delta storage: isolated ADLS run roots (locations retained only in private runtime configuration)' : deltaTableLocations(directory));
    if (options.quiver) step(quiverLocations(directory));
    const context = { directory, env, network, options };
    phase = 'load-generation';
    step('Running load generator (Ctrl+C to stop; stack is kept)');
    const loadEnded = new AbortController();
    const crashEndpoints = { direct: `http://${network.direct}:8081`, dataflow: `http://${network.dataflow}:32011` };
    let killedPublishers: KilledPublishers | undefined;
    const crash = options.crashAfterSeconds === undefined ? undefined : runQuiverCrash({
      crashAfterSeconds: options.crashAfterSeconds,
      directAdmin: `http://${network.direct}:8081`,
      dataflowAdmin: `http://${network.dataflow}:32011`,
      results: path.join(directory, 'results'),
      kill: async () => {
        killedPublishers = await killQuiverPublishers({
          workspace, results: path.join(directory, 'results'), endpoints: crashEndpoints,
          docker: args => run('docker', args, env),
          kill: async () => {
            await composeAsync(project, env, ['--profile', 'benchmark', 'kill', '--signal', 'SIGKILL', 'direct-1', 'dataflow-1']);
          },
        });
      },
      restart: async () => {
        // A restarted Direct process is a new receipt-owner generation that adopts its predecessor's receipts.
        writeDirectConfig(directory, 'direct-1', network, true, azure, privateEnv);
        await composeAsync(project, env, [
          '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', '--wait', '--wait-timeout', '300',
          'direct-1', 'dataflow-1',
        ], 360_000);
        assert(killedPublishers, 'Missing exact killed collector identities');
        await recordQuiverSuccessors({
          workspace, results: path.join(directory, 'results'), endpoints: crashEndpoints,
          docker: args => run('docker', args, env), killed: killedPublishers,
        });
      },
      log: step,
      loadEnded: loadEnded.signal,
      checkRunning: () => controller.checkRunning(),
    });
    const load = runLocalLoadgen(context, 'run', path.join(directory, 'results'), controller)
      .finally(() => loadEnded.abort());
    const stressRuntime = stressPlan ? new LocalStressRuntime({
      workspace, directory, project, env, composeFiles: benchmarkComposeFiles(composeFile, env),
      reader: path.join(cargoTargetDirectory(), 'debug/delta-inspect'),
      directAdmin: `http://${network.direct}:8081`, dataflowAdmin: `http://${network.dataflow}:32011`,
      quiver: options.quiver, loadEnded: loadEnded.signal, checkRunning: () => controller.checkRunning(),
      fixture: stressPlan.fixture, windowSeconds: stressPlan.durationSeconds,
      sampleTimeoutSeconds: stressPlan.sampleTimeoutSeconds, measureSeconds: options.measureSeconds,
      privateScratch: path.join(workspace, 'projects/loadtest/.temp/loadgen/latest/diagnostics'),
      physicalRoots: Object.fromEntries([
        'direct', 'dataflow', 'tenant', 'tenant-checkpoints', 'direct-state-1', 'dataflow-state-1', 'router-state',
      ].map(name => [name, path.join(directory, name)]).concat([
        ['loadgen-staging', path.join(workspace, 'projects/loadtest/.temp/loadgen/latest')],
      ])),
    }) : undefined;
    const stress = stressPlan && stressRuntime ? runStressDriver({
      plan: stressPlan, results: path.join(directory, 'results'), loadEnded: loadEnded.signal,
      checkRunning: () => controller.checkRunning(),
      waitForInput: deadline => stressRuntime.waitForInput(deadline),
      operate: (wave, deadline) => stressRuntime.operate(wave, deadline),
      sample: (wave, runId, deadline) => stressRuntime.sample(wave, runId, deadline),
    }) : undefined;
    const reconciliation = azure ? reconcileAzure({ ...context, project }, cloud.occupancyId).catch(error => {
      cloud.reconciliationFailed = true;
      throw error;
    }) : undefined;
    const [loadOutcome, crashOutcome, reconcileOutcome, stressOutcome] = await Promise.allSettled([load, crash, reconciliation, stress]);
    if (reconcileOutcome.status === 'rejected') {
      phase = 'azure-external-reconcile';
      if (loadOutcome.status === 'rejected') {
        throw new AggregateError([reconcileOutcome.reason, loadOutcome.reason], 'Azure reconciliation and load generation failed');
      }
      throw reconcileOutcome.reason;
    }
    if (loadOutcome.status === 'rejected') throw loadOutcome.reason;
    if (crashOutcome.status === 'rejected') {
      phase = 'quiver-recovery';
      throw crashOutcome.reason;
    }
    if (crash) step(`Quiver crash recovery evidence: ${resultsDirectory}/${QUIVER_RECOVERY_FILE}`);
    if (options.validate) {
      phase = 'publication-drain';
      step('Draining publication while collector admin endpoints remain available');
      await runLocalLoadgen(context, 'drain', path.join(directory, 'results'), controller);
      phase = 'live-grafana-validation';
      await runGrafanaGate({
        workspace, results: path.join(directory, 'results'), browserEndpoint: `ws://${network.playwright}:3000/`,
        metricScope: 'source-sdk', bridgeEnabled: Boolean(env.LOADTEST_KUSTO_BRIDGE_ENDPOINT),
        compose: (args, timeout) => compose(project, env, args, timeout), log: step,
      });
      phase = 'mutator-drain';
      step('Cleanly stopping all source, maintenance and tenant mutators before immutable snapshot capture');
      if (!azure) {
        assert(stackSql, 'Missing stack-start SQL generation');
        stoppedMutators = await admitPublisherRelease({
          workspace, directory, sql: stackSql, docker: args => run('docker', args),
          sdk: path.join(cargoTargetDirectory(), 'debug/arrowcol-sdk-gen'),
          stop: () => stopBenchmark(workspace, args => run('docker', args, process.env, 360_000), {
            requireCleanMutators: true, mutatorsOnly: true,
          }),
          release: async () => {
            phase = 'publisher-release';
            await runLocalLoadgen(context, 'publisher-release', path.join(directory, 'results'), controller);
          },
          compact: async () => {
            phase = 'final-compaction';
            await runLocalLoadgen(context, 'final-compaction', path.join(directory, 'results'), controller);
          },
        });
      } else {
      const cleanServices = await stopBenchmark(workspace, args => run('docker', args, process.env, 360_000), {
        requireCleanMutators: true, mutatorsOnly: true,
      });
      stoppedMutators = stoppedAttemptMutators(workspace, args => run('docker', args), cleanServices);
      const metadata: unknown = JSON.parse(readFileSync(path.join(directory, 'results/run-metadata.json'), 'utf8'));
      assert(metadata && typeof metadata === 'object' && 'run_id' in metadata && typeof metadata.run_id === 'string',
        'Cannot bind stopped-mutator evidence without the run identity');
      writeFileSync(path.join(directory, 'results/mutator-drain.json'), `${JSON.stringify({
        run_id: metadata.run_id, clean: true, services: cleanServices, finished_at: new Date().toISOString(),
      }, null, 2)}\n`);
      }
      phase = 'output-validation';
      step('Validating Delta and native Kusto output');
      controller.checkRunning();
      await runLocalLoadgen(context, 'validate', path.join(directory, 'results'), controller);
      if (!azure) {
      requireHealthyKusto(readKustoStatus(path.join(directory, 'results')));
      phase = 'ui-validation';
      step('Validating that the loadtest data is queryable through kusto-explorer (Playwright)');
      controller.checkRunning();
      const gate = await runUiGate({
        workspace,
        results: path.join(directory, 'results'),
        browserEndpoint: `ws://${network.playwright}:3000/`,
        compose: (args, timeout) => compose(project, env, args, timeout),
        log: step,
      });
      step(`kusto-explorer UI gate passed: ${gate.tables} tables and ${gate.rows} rows matched through the UI`);
      }
    }
    if (stressOutcome.status === 'rejected') {
      phase = 'fixed-window-stress';
      throw stressOutcome.reason;
    }
  } catch (error) {
    failure = error;
  } finally {
    try {
      await stopQapObserver?.();
    } catch (error) {
      cloud.qapObservationFailed = true;
      failure = failure ? new AggregateError([failure, error], 'Benchmark and QAP observation failed') : error;
    }
    try {
      await stopSampler?.();
    } catch (error) {
      failure ??= error;
    }
    step('Capturing Compose logs and diagnostics');
    try {
      composeToFile(project, env, ['--profile', 'benchmark', 'logs', '--no-color', '--timestamps'],
        path.join(directory, 'results/compose.log'));
      writeFileSync(path.join(directory, 'results/compose-ps.json'),
        compose(project, env, ['--profile', 'benchmark', 'ps', '--all', '--format', 'json']));
    } catch (error) {
      console.error(`Unable to capture benchmark Compose diagnostics: ${String(error)}`);
      failure ??= error;
    }
  }
  if (failure) {
    writeFileSync(path.join(directory, 'results/runner-failure.json'), `${JSON.stringify({
      timestamp: new Date().toISOString(), phase, kind: failure instanceof Error ? failure.name : 'Error',
    })}\n`);
    step(`FAILED. Diagnostics: ${resultsDirectory}/compose.log`);
    throw failure;
  }
  if (options.validate && !azure) {
    if (!stoppedMutators) throw new Error('Attempt evidence requires positively verified stopped mutators');
    try {
      await sealAttemptEvidence(workspace, directory, path.join(cargoTargetDirectory(), 'debug'),
        (args, timeout) => run('docker', args, process.env, timeout), stoppedMutators, env.ONELAKE_HOST_PATH,
        undefined, () => controller.checkRunning());
    } catch (error) {
      writeFileSync(path.join(directory, 'results/runner-failure.json'), `${JSON.stringify({
        timestamp: new Date().toISOString(), phase: 'attempt-evidence', kind: 'Error',
      })}\n`);
      throw error;
    }
    step('Validated attempt evidence sealed privately; owned router stopped after qualification');
  } else {
    step('Attempt remains unsealed; a subsequent reset is blocked to preserve its original data');
  }
  step(`Load generation finished. ${azure ? 'Azure mode; QAP evidence is a separate required live gate' : kustoStatusSummary(readKustoStatus(path.join(directory, 'results')))}. ` +
    `Results: ${resultsDirectory}. Stack retained for inspection; ` +
    (options.validate && !azure ? 'the next start verifies the private seal before reset.' : 'reset is blocked for this unsealed attempt.'));
  return {
    directory,
    env,
    network,
    networkName: workspaceConnection.network,
    project,
    options,
    workspaceContainer: workspaceConnection.container,
  };
}

async function main(): Promise<void> {
  const userArguments = process.argv.slice(2);
  if (userArguments.some((argument) => argument === '--help' || argument === '-h')) {
    console.log(`${benchmarkUsage}\n--stop (stop this workspace's owned services and generator; preserve data)` +
      `\n--discard-unsealed (stop, then move failed/interrupted/Azure attempts to .temp/loadtest-discarded so the next start can reset)`);
    return;
  }
  if (userArguments.length === 1 && userArguments[0] === '--discard-unsealed') {
    const controller = await acquireBenchmarkController(workspace);
    try {
      await stopBenchmark(workspace, args => run('docker', args));
      const moved = discardUnsealedAttempts(workspace);
      console.log(JSON.stringify({ discarded: moved.length, quarantine: moved, dataDeleted: false }));
    } finally {
      controller.close();
    }
    return;
  }
  if (userArguments.length === 1 && userArguments[0] === '--stop') {
    const receipt = stopReceipt(process.env.ARROWCOL_RESET_GENERATION);
    const controller = await acquireBenchmarkController(workspace);
    try {
      await stopBenchmark(workspace, args => run('docker', args));
      console.log(JSON.stringify(receipt));
    } finally {
      controller.close();
    }
    return;
  }
  const options = parseBenchmarkOptions(userArguments);
  if (options.stressPlan) {
    if (options.mode !== 'local') {
      throw new Error('Azure stress requires a complete ADLS physical inventory adapter; local functional evidence cannot qualify Azure');
    }
    const plan = readStressPlan(options.stressPlan);
    options.stressConfiguration = plan;
    if (options.batchRows !== plan.fixture.batchRows) throw new Error('--batch-rows must equal the predeclared stress fixture batchRows');
    options.measureSeconds = stressInputBudget(plan, options).measureSeconds;
  }
  const privateEnv = modeEnvironment(workspace, options.mode, process.env);
  requirePreviousAttemptEvidence(workspace);
  const controller = await acquireBenchmarkController(workspace);
  const occupancyId = randomUUID();
  const cloud: CloudRun = { environment: privateEnv, occupancyId, reconciliationFailed: false, qapObservationFailed: false };
  let occupied = false;
  let failure: unknown;
  try {
    if (options.mode === 'azure') {
      azureTargets(privateEnv, 'preflight');
      run('python3', ['infra/azure/bootstrap.py', 'occupy', '--occupancy-id', occupancyId], privateEnv);
      occupied = true;
      cloud.environment = modeEnvironment(workspace, options.mode, process.env);
    }
    // Occupy exports the exact ready storage generation; never substitute a run UUID.
    await benchmark(options, controller, cloud);
  } catch (error) {
    failure = error;
  } finally {
    try {
      if (occupied) {
        await stopBenchmark(workspace, args => run('docker', args, process.env, 360_000), { requireCleanMutators: true });
        if (cloud.reconciliationFailed) throw new Error('Azure reconciliation failed; retain occupancy pending authoritative reconciliation');
        if (cloud.qapObservationFailed) throw new Error('QAP helper failed; retain occupancy pending helper/evidence inspection');
        run('python3', ['infra/azure/bootstrap.py', 'release', '--occupancy-id', occupancyId], cloud.environment);
      }
    } catch (error) {
      failure = failure ? new AggregateError([failure, error], 'Benchmark and cloud drain/release failed; occupancy is not force-cleared') : error;
    } finally {
      controller.close();
    }
  }
  if (failure) throw failure;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
