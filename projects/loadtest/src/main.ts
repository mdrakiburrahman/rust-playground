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
import { writeProvenance } from './provenance.js';
import { runQuiverCrash, QUIVER_RECOVERY_FILE } from './quiver-crash.js';
import {
  benchmarkComposeFiles, benchmarkQuiver, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs, dataflowConfigFile,
} from './runtime-config.js';
import { acquireBenchmarkController, resetBenchmark, stopBenchmark, type BenchmarkController } from './lifecycle.js';
import { azureTargets, modeEnvironment, type AzureTargets } from './mode.js';
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

function writeDirectConfig(directory: string, service: string, network: BenchmarkNetwork, quiver: boolean, azure?: AzureTargets): void {
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
    `${buildBenchmarkComposeOverride(hostPath(directory), { quiver })}\n`);
  writeFileSync(generationFile, `${generation}\n`);
}

function writeRuntimeConfigs(directory: string, network: BenchmarkNetwork, quiver: boolean, azure?: AzureTargets): void {
  writeDirectConfig(directory, 'direct-1', network, quiver, azure);
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
    LOADTEST_KUSTO_ENDPOINT: context.options.mode === 'azure' ? undefined : `http://${context.network.kusto}:8080`,
    LOADTEST_KUSTO_BRIDGE_ENDPOINT: context.options.mode === 'azure' ? undefined : `http://${context.network.kustoBridge}:9464`,
    ARROWCOL_SQL_HOST: context.network.sql,
    ARROWCOL_SQL_PASSWORD_FILE: path.join(infra, 'arrowcol-app-password.txt'),
    ARROWCOL_SDK_GEN: path.join(cargoTargetDirectory(), 'debug/arrowcol-sdk-gen'),
  };
}

async function runLocalLoadgen(
  context: Pick<BenchmarkContext, 'directory' | 'env' | 'network' | 'options'>,
  command: 'run' | 'validate',
  results: string,
  controller: BenchmarkController,
): Promise<void> {
  await runInteractive(
    path.join(cargoTargetDirectory(), 'debug/arrowcol-loadgen'),
    [command],
    localLoadgenEnvironment(context, results),
    controller,
    command === 'validate' ? 3_600_000 : context.options.measureSeconds === undefined ? undefined : (context.options.warmupSeconds + context.options.measureSeconds + 600) * 1000,
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

async function benchmark(options: BenchmarkOptions, controller: BenchmarkController): Promise<BenchmarkContext> {
  const privateEnv = modeEnvironment(workspace, options.mode, process.env);
  const azure = options.mode === 'azure'
    ? azureTargets(privateEnv, `run-${Date.now()}-${randomBytes(4).toString('hex')}`)
    : undefined;
  step(`Load test: warmup ${options.warmupSeconds}s, concurrency ${options.concurrency}, batch rows ${options.batchRows}` +
    (options.quiver ? ', native Quiver disk persistence' : '') +
    (options.crashAfterSeconds === undefined ? '' : `, kill -9 both collectors ${options.crashAfterSeconds}s into load`));
  step('Starting fresh: removing previous loadtest containers, volumes, and generated data');
  const project = BENCHMARK_PROJECT;
  const directory = await resetBenchmark(workspace, args => run('docker', args));
  controller.checkRunning();
  step('Bootstrapping secrets and preparing benchmark');
  for (const child of [
    'direct', 'dataflow', 'otel', 'results', 'router-state',
    'direct-state-1',
    ...(options.quiver ? [benchmarkQuiver.directHostDirectory, benchmarkQuiver.dataflowHostDirectory] : []),
  ]) {
    mkdirSync(path.join(directory, child), { recursive: true });
  }
  let network: BenchmarkNetwork;
  let env: NodeJS.ProcessEnv;
  try {
    run(path.join(workspace, 'projects/arrowcol-infra/scripts/bootstrap-secrets.sh'), []);
    mkdirSync(path.join(workspace, 'projects/loadtest/.temp'), { recursive: true });
    network = selectBenchmarkNetwork();
    writeRuntimeConfigs(directory, network, options.quiver, azure);
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
  const resultsDirectory = path.relative(workspace, path.join(directory, 'results'));
  try {
    step('Validating Compose configuration');
    writeProvenance(workspace, directory, project, options, env);
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
    if (kustoStartup.state !== 'degraded') {
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
    writeProvenance(workspace, directory, project, options, env, true);
    phase = 'sampler-start';
    stopSampler = await startResourceSampler({
      project, results: path.join(directory, 'results'),
      directAdmin: `http://${network.direct}:8081`,
      dataflowAdmin: `http://${network.dataflow}:32011`,
    });
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
    const crash = options.crashAfterSeconds === undefined ? undefined : runQuiverCrash({
      crashAfterSeconds: options.crashAfterSeconds,
      directAdmin: `http://${network.direct}:8081`,
      dataflowAdmin: `http://${network.dataflow}:32011`,
      results: path.join(directory, 'results'),
      kill: async () => {
        await composeAsync(project, env, ['--profile', 'benchmark', 'kill', '--signal', 'SIGKILL', 'direct-1', 'dataflow-1']);
      },
      restart: async () => {
        // A restarted Direct process is a new receipt-owner generation that adopts its predecessor's receipts.
        writeDirectConfig(directory, 'direct-1', network, true, azure);
        await composeAsync(project, env, [
          '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', '--wait', '--wait-timeout', '300',
          'direct-1', 'dataflow-1',
        ], 360_000);
      },
      log: step,
      loadEnded: loadEnded.signal,
      checkRunning: () => controller.checkRunning(),
    });
    const load = runLocalLoadgen(context, 'run', path.join(directory, 'results'), controller)
      .finally(() => loadEnded.abort());
    const [loadOutcome, crashOutcome] = await Promise.allSettled([load, crash]);
    if (loadOutcome.status === 'rejected') throw loadOutcome.reason;
    if (crashOutcome.status === 'rejected') {
      phase = 'quiver-recovery';
      throw crashOutcome.reason;
    }
    if (crash) step(`Quiver crash recovery evidence: ${resultsDirectory}/${QUIVER_RECOVERY_FILE}`);
    if (options.validate) {
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
      phase = 'grafana-validation';
      await runGrafanaGate({
        workspace, results: path.join(directory, 'results'), browserEndpoint: `ws://${network.playwright}:3000/`,
        compose: (args, timeout) => compose(project, env, args, timeout), log: step,
      });
      }
    }
  } catch (error) {
    failure = error;
  } finally {
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
  step(`Load generation finished. ${azure ? 'Azure mode; QAP evidence is a separate required live gate' : kustoStatusSummary(readKustoStatus(path.join(directory, 'results')))}. ` +
    `Results: ${resultsDirectory}. Stack kept for inspection; the next start resets it.`);
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
    console.log(`${benchmarkUsage}\n--stop (stop this workspace's owned services and generator; preserve data)`);
    return;
  }
  if (userArguments.length === 1 && userArguments[0] === '--stop') {
    const controller = await acquireBenchmarkController(workspace);
    try {
      await stopBenchmark(workspace, args => run('docker', args));
      console.log(JSON.stringify({ stopped: true, scope: 'workspace-owned-loadtest', dataPreserved: true }));
    } finally {
      controller.close();
    }
    return;
  }
  const options = parseBenchmarkOptions(userArguments);
  const privateEnv = modeEnvironment(workspace, options.mode, process.env);
  const controller = await acquireBenchmarkController(workspace);
  const occupancyId = randomUUID();
  let occupied = false;
  try {
    if (options.mode === 'azure') {
      azureTargets(privateEnv, 'preflight');
      run('python3', ['infra/azure/bootstrap.py', 'occupy', '--occupancy-id', occupancyId], privateEnv);
      occupied = true;
    }
    await benchmark(options, controller);
  } finally {
    try {
      if (occupied) {
        await stopBenchmark(workspace, args => run('docker', args));
        run('python3', ['infra/azure/bootstrap.py', 'release', '--occupancy-id', occupancyId], privateEnv);
      }
    } finally {
      controller.close();
    }
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
