import { execFileSync, spawn, type ChildProcess } from 'node:child_process';
import { existsSync } from 'node:fs';
import { lstat, mkdir, readFile, realpath, readdir, rm, stat, unlink, writeFile } from 'node:fs/promises';
import { createServer as createHttpServer } from 'node:http';
import { createServer as createTcpServer, isIP } from 'node:net';
import { extname, isAbsolute, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { formatPreflightReport, loadAksEnvironment, runAksPreflight } from './aks-preflight.js';
import { run_tree } from '../../infra/azure/bootstrap.js';
import { record, reportDataFlowRun, type NetworkObservation } from './dataflow-verdict.js';

export interface OatRunStatus {
  id: string;
  state: 'queued' | 'running' | 'succeeded' | 'failed' | 'cancelled';
  progress: string;
  result_count: number;
  error: string | null;
}

type FetchLike = typeof fetch;
type ProgressHandler = (status: OatRunStatus) => void;

export interface LocalViewer {
  url: string;
  close(): Promise<void>;
}

interface LocalViewerConfig {
  resultsPath: string;
  siteRoot: string;
}

const workspaceRoot = resolve(fileURLToPath(new URL('../..', import.meta.url)));

export class OatCliError extends Error {
  readonly exitCode = 2;
}

export function cliExitCode(error: unknown): number {
  return error instanceof OatCliError ? error.exitCode : 1;
}

function validateArguments(args: string[]): void {
  const valuedOptions = new Set(['--config', '--mode', '--server', '--poll-ms', '--cleanup-local']);
  const flags = new Set(['--viewer-only', '--preflight-only']);
  let positionalConfig = false;
  for (let index = 0; index < args.length; index++) {
    const argument = args[index]!;
    if (argument === '-h' || argument === '--help') continue;
    if (valuedOptions.has(argument)) {
      const value = args[index + 1];
      if (!value || value.startsWith('-')) throw new OatCliError(`Expected a value after ${argument}`);
      index++;
      continue;
    }
    if (flags.has(argument)) continue;
    if (argument.startsWith('-')) throw new OatCliError(`Unknown option: ${argument}`);
    if (positionalConfig) throw new OatCliError(`Unexpected argument: ${argument}`);
    positionalConfig = true;
  }
}

const cliUsage = `Usage: arrowcol-oat [options] [config]

Options:
  --mode <local|aks>   Select the execution mode
  --config <path>      Use a configuration file
  --server <URL>       Use an existing OAT API server
  --poll-ms <number>   Set the status polling interval
  --viewer-only        Serve existing local results without running a benchmark
  --preflight-only     Run the read-only AKS preflight
  --cleanup-local <id> Remove one abandoned local Compose cell by its run UUID
  -h, --help           Show this help`;

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function resolveOatConfigPath(mode: 'local' | 'aks', configPath?: string, root = workspaceRoot): string {
  if (configPath) return configPath;
  if (mode === 'aks') {
    const generated = resolve(root, 'out/arrowcol-oat/oat-config.aks.json');
    if (existsSync(generated)) return generated;
  }
  const name = mode === 'local' ? 'oat-config.quick.json' : 'oat-config.aks.quick.json';
  return resolve(root, 'docs/experiments/arrowcol-oat', name);
}

export type DockerCommand = (args: string[]) => string;

function runDocker(args: string[]): string {
  return execFileSync('docker', args, { encoding: 'utf8', timeout: 30_000 });
}

function localProjectPrefix(config: Record<string, unknown>): string {
  const platform = isRecord(config.platform) ? config.platform : {};
  const base = platform.compose_project;
  if (typeof base !== 'string' || !base.trim()) throw new Error('Local Compose project is missing from config');
  return `${base.slice(0, 39).replace(/[-_]+$/g, '')}-`;
}

export async function cleanupLocalComposeProjects(
  config: Record<string, unknown>,
  runId: string,
  docker: DockerCommand = runDocker,
): Promise<string[]> {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(runId)) {
    throw new Error('--cleanup-local requires a canonical OAT cell run UUID');
  }
  const prefix = localProjectPrefix(config);
  const targetProject = prefix + runId.replace(/-/g, '').slice(0, 8).toLowerCase();
  const projectPattern = /^[a-z0-9][a-z0-9_-]*-[a-f0-9]{8}$/i;
  const projects = new Set<string>();
  const resources: Array<{ list: string[]; inspect: (id: string) => string[] }> = [
    {
      list: ['ps', '-aq', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['inspect', '--format', '{{ index .Config.Labels "com.docker.compose.project" }}', id],
    },
    {
      list: ['network', 'ls', '-q', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['network', 'inspect', '--format', '{{ index .Labels "com.docker.compose.project" }}', id],
    },
    {
      list: ['volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project'],
      inspect: (id) => ['volume', 'inspect', '--format', '{{ index .Labels "com.docker.compose.project" }}', id],
    },
  ];
  for (const resource of resources) {
    for (const id of docker(resource.list).split(/\r?\n/).filter(Boolean)) {
      const project = docker(resource.inspect(id)).trim();
      if (project === targetProject && projectPattern.test(project)) projects.add(project);
    }
  }
  projects.add(targetProject);
  for (const project of projects) {
    const containers = docker(['ps', '-aq', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (containers.length) docker(['rm', '-f', ...containers]);
    const networks = docker(['network', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (networks.length) docker(['network', 'rm', ...networks]);
    const volumes = docker(['volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${project}`]).split(/\r?\n/).filter(Boolean);
    if (volumes.length) docker(['volume', 'rm', ...volumes]);
  }
  const platform = isRecord(config.platform) ? config.platform : {};
  if (typeof platform.compose_state_root === 'string') {
    const stateRoot = resolve(platform.compose_state_root);
    for (const project of projects) {
      const matches = (await readdir(stateRoot, { withFileTypes: true }).catch((error: unknown) => {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
        throw error;
      })).filter((entry) => entry.isDirectory() && entry.name.toLowerCase() === runId.toLowerCase());
      if (matches.length > 1) throw new Error(`Multiple local state directories match Compose project ${project}`);
      if (matches.length === 1) {
        const stateDir = resolve(stateRoot, matches[0]!.name);
        if ((await lstat(stateDir)).isSymbolicLink()) throw new Error(`Refusing symlinked local OAT state directory ${stateDir}`);
        await rm(stateDir, { recursive: true });
      }
    }
  }
  return [...projects];
}
interface LocalRunMarker {
  version: 1;
  runId: string;
  ownerPid: number;
  ownerStartTicks: string;
  serverPid: number;
  serverStartTicks: string;
  serverUrl: string;
  serverExecutable: string;
}

interface LocalRunMarkerContext {
  path: string;
  marker: Omit<LocalRunMarker, 'runId'>;
}

function localStateRoot(config: Record<string, unknown>): string {
  const platform = isRecord(config.platform) ? config.platform : {};
  if (typeof platform.compose_state_root !== 'string' || !platform.compose_state_root) {
    throw new Error('Local Compose state root is missing from config');
  }
  return resolve(platform.compose_state_root);
}

async function processStartTicks(pid: number): Promise<string | undefined> {
  if (process.platform !== 'linux') return undefined;
  let statText: string;
  try {
    statText = await readFile(`/proc/${pid}/stat`, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return undefined;
    throw error;
  }
  const fields = statText.slice(statText.lastIndexOf(')') + 2).trim().split(/\s+/);
  if (fields[0] === 'Z' || fields.length <= 19) return undefined;
  return fields[19];
}

async function prepareLocalRunMarker(config: Record<string, unknown>, server: {
  child: ChildProcess;
  url: string;
  executable: string;
}): Promise<LocalRunMarkerContext> {
  const serverPid = server.child.pid;
  if (!serverPid) throw new Error('Could not identify the owned OAT server process');
  const ownerStartTicks = await processStartTicks(process.pid);
  const serverStartTicks = await processStartTicks(serverPid);
  if (!ownerStartTicks || !serverStartTicks) {
    throw new Error('Cannot safely record local OAT process ownership on this platform');
  }
  const root = localStateRoot(config);
  await mkdir(root, { recursive: true });
  const path = resolve(root, `.arrowcol-oat-owner-${serverPid}.json`);
  return {
    path,
    marker: {
      version: 1,
      ownerPid: process.pid,
      ownerStartTicks,
      serverPid,
      serverStartTicks,
      serverUrl: server.url,
      serverExecutable: await realpath(server.executable),
    },
  };
}

async function persistLocalRunMarker(context: LocalRunMarkerContext, status: OatRunStatus): Promise<void> {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(status.id)) {
    throw new Error('OAT server returned a noncanonical run ID; local recovery marker was not written');
  }
  const marker: LocalRunMarker = { ...context.marker, runId: status.id };
  await writeFile(context.path, JSON.stringify(marker), { flag: 'wx', mode: 0o600 });
}

function parseLocalRunMarker(text: string, markerPath: string): LocalRunMarker {
  const value: unknown = JSON.parse(text);
  if (!isRecord(value) || value.version !== 1 ||
      typeof value.runId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.runId) ||
      typeof value.ownerPid !== 'number' || !Number.isSafeInteger(value.ownerPid) ||
      typeof value.ownerStartTicks !== 'string' ||
      typeof value.serverPid !== 'number' || !Number.isSafeInteger(value.serverPid) ||
      typeof value.serverStartTicks !== 'string' ||
      typeof value.serverUrl !== 'string' || typeof value.serverExecutable !== 'string') {
    throw new Error(`Invalid local OAT ownership marker: ${markerPath}`);
  }
  const server = new URL(value.serverUrl);
  if (server.protocol !== 'http:' || server.hostname !== '127.0.0.1' || !server.port) {
    throw new Error(`Unsafe local OAT ownership marker URL: ${markerPath}`);
  }
  return {
    version: 1,
    runId: value.runId,
    ownerPid: value.ownerPid,
    ownerStartTicks: value.ownerStartTicks,
    serverPid: value.serverPid,
    serverStartTicks: value.serverStartTicks,
    serverUrl: value.serverUrl,
    serverExecutable: value.serverExecutable,
  };
}

async function cancelMarkedRun(marker: LocalRunMarker): Promise<void> {
  const statusUrl = new URL(`/runs/${encodeURIComponent(marker.runId)}`, marker.serverUrl);
  let response = await fetch(statusUrl, { signal: AbortSignal.timeout(10_000) });
  let status = await readStatus(response, 'inspect interrupted local OAT run');
  if (status.state === 'queued' || status.state === 'running') {
    response = await fetch(new URL(`/runs/${encodeURIComponent(marker.runId)}/cancel`, marker.serverUrl), {
      method: 'POST',
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok && response.status !== 409) await readStatus(response, 'cancel interrupted local OAT run');
    const deadline = Date.now() + 10 * 60 * 1000;
    while (status.state === 'queued' || status.state === 'running') {
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for local OAT cleanup for run ${marker.runId}`);
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 250));
      response = await fetch(statusUrl, { signal: AbortSignal.timeout(10_000) });
      status = await readStatus(response, 'wait for interrupted local OAT cleanup');
    }
  }
}

async function stopMarkedServer(marker: LocalRunMarker): Promise<void> {
  if (await processStartTicks(marker.serverPid) !== marker.serverStartTicks) return;
  const executable = await realpath(`/proc/${marker.serverPid}/exe`);
  if (executable !== marker.serverExecutable) throw new Error('Refusing to stop a process that does not match the recorded OAT server');
  process.kill(marker.serverPid, 'SIGTERM');
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline && await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));
  }
  if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
    process.kill(marker.serverPid, 'SIGKILL');
    const killDeadline = Date.now() + 1_000;
    while (Date.now() < killDeadline && await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      await new Promise((resolvePromise) => setTimeout(resolvePromise, 50));
    }
    if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      throw new Error(`Could not stop interrupted OAT server process ${marker.serverPid}`);
    }
  }
}

async function removeStaleLocalMarkers(config: Record<string, unknown>): Promise<void> {
  const root = localStateRoot(config);
  const files = await readdir(root).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  const remainingCells = files.filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry));
  if (remainingCells.length) return;
  for (const name of files.filter((entry) => entry.startsWith('.arrowcol-oat-owner-') && entry.endsWith('.json'))) {
    const markerPath = resolve(root, name);
    const marker = parseLocalRunMarker(await readFile(markerPath, 'utf8'), markerPath);
    if (await processStartTicks(marker.ownerPid) === marker.ownerStartTicks ||
        await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      throw new Error(`Refusing to remove ownership marker for active local run ${marker.runId}`);
    }
    await unlink(markerPath);
  }
}
async function recoverInterruptedLocalRuns(config: Record<string, unknown>, allowMissingServer = false): Promise<void> {
  const root = localStateRoot(config);
  const files = await readdir(root).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return [];
    throw error;
  });
  for (const name of files.filter((entry) => entry.startsWith('.arrowcol-oat-owner-') && entry.endsWith('.json'))) {
    const markerPath = resolve(root, name);
    const marker = parseLocalRunMarker(await readFile(markerPath, 'utf8'), markerPath);
    if (await processStartTicks(marker.ownerPid) === marker.ownerStartTicks) {
      throw new Error(`Local OAT run ${marker.runId} is still owned by process ${marker.ownerPid}`);
    }
    if (await processStartTicks(marker.serverPid) === marker.serverStartTicks) {
      const executable = await realpath(`/proc/${marker.serverPid}/exe`);
      if (executable !== marker.serverExecutable) throw new Error('OAT server ownership marker does not match the live process');
      await cancelMarkedRun(marker);
      await stopMarkedServer(marker);
      await unlink(markerPath);
    } else if (!allowMissingServer) {
      const cellRuns = files.filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry));
      if (cellRuns.length) {
        throw new Error(`Interrupted OAT server is gone; clean each local cell with --cleanup-local <cell-run-uuid>. Remaining cells: ${cellRuns.join(', ')}`);
      }
      await unlink(markerPath);
    }
  }
}
function getDockerBridgeGateway(): string {
  try {
    const gateway = execFileSync('docker', [
      'network', 'inspect', 'bridge', '--format', '{{(index .IPAM.Config 0).Gateway}}',
    ], { encoding: 'utf8', timeout: 10_000 }).trim();
    if (!gateway || isIP(gateway) === 0) throw new Error('Docker bridge returned no valid gateway address');
    return gateway;
  } catch (error) {
    throw new Error(`Could not detect Docker bridge gateway: ${error instanceof Error ? error.message : String(error)}`);
  }
}

export async function readOatConfigForMode(
  configPath: string,
  mode: 'local' | 'aks',
  bridgeGateway: () => string = getDockerBridgeGateway,
): Promise<Record<string, unknown>> {
  const parsed: unknown = JSON.parse(await readFile(configPath, 'utf8'));
  if (!isRecord(parsed)) throw new Error('OAT config must be a JSON object');
  const platform: Record<string, unknown> = isRecord(parsed.platform) ? { ...parsed.platform, kind: mode } : { kind: mode };
  if (mode === 'local' && isRecord(platform.local)) {
    const local = { ...platform.local };
    if (local.bind_host == null || local.endpoint_host == null) {
      const gateway = bridgeGateway();
      if (local.bind_host == null) local.bind_host = gateway;
      if (local.endpoint_host == null) local.endpoint_host = gateway;
    }
    platform.local = local;
  }
  return { ...parsed, platform };
}

export async function launchOat(
  configPath: string,
  serverUrl = 'http://127.0.0.1:8088',
  pollIntervalMs = 1000,
  fetcher: FetchLike = fetch,
  onProgress: ProgressHandler = (status) => console.log(`${status.state}: ${status.progress}`),
  mode?: 'local' | 'aks',
  bridgeGateway: () => string = getDockerBridgeGateway,
  signal?: AbortSignal,
  onRunStarted?: (status: OatRunStatus) => Promise<void>,
  onRunFinished?: (status: OatRunStatus) => void,
): Promise<OatRunStatus> {
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1) {
    throw new Error('poll interval must be a positive integer');
  }
  const server = new URL(serverUrl);
  if (!['http:', 'https:'].includes(server.protocol)) {
    throw new Error('server URL must use HTTP or HTTPS');
  }
  const config = mode
    ? await readOatConfigForMode(configPath, mode, bridgeGateway)
    : JSON.parse(await readFile(configPath, 'utf8'));
  if (!isRecord(config)) throw new Error('OAT config must be a JSON object');

  const startResponse = await fetcher(new URL('/runs', server), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(config),
    signal: AbortSignal.timeout(30_000),
  });
  let status = await readStatus(startResponse, 'start OAT run');
  await onRunStarted?.(status);
  onProgress(status);
  let cancellationRequest: Promise<void> = Promise.resolve();
  const requestCancellation = (): void => {
    cancellationRequest = (async () => {
      const response = await fetcher(new URL(`/runs/${encodeURIComponent(status.id)}/cancel`, server), {
        method: 'POST',
        signal: AbortSignal.timeout(30_000),
      });
      if (!response.ok && response.status !== 409) {
        await readStatus(response, 'cancel local OAT run');
      }
    })();
  };
  if (signal?.aborted) requestCancellation();
  else signal?.addEventListener('abort', requestCancellation, { once: true });
  try {
    while (status.state === 'queued' || status.state === 'running') {
      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
      if (signal?.aborted) await cancellationRequest;
      const response = await fetcher(new URL(`/runs/${encodeURIComponent(status.id)}`, server), {
        signal: AbortSignal.timeout(30_000),
      });
      status = await readStatus(response, 'poll OAT run');
      onProgress(status);
    }
  } finally {
    signal?.removeEventListener('abort', requestCancellation);
  }
  onRunFinished?.(status);
  if (status.state === 'failed' || status.state === 'cancelled') {
    throw new Error(status.error ?? `OAT run ${status.id} ${status.state}`);
  }
  return status;
}

async function readStatus(response: Response, action: string): Promise<OatRunStatus> {
  const body: unknown = await response.json().catch(() => undefined);
  if (!response.ok) {
    const error = body !== null && typeof body === 'object' && 'error' in body && typeof body.error === 'string'
      ? body.error
      : `${response.status} ${response.statusText}`;
    throw new Error(`Could not ${action}: ${error}`);
  }
  if (
    body === null || typeof body !== 'object' ||
    !('id' in body) || typeof body.id !== 'string' ||
    !('state' in body) || !['queued', 'running', 'succeeded', 'failed', 'cancelled'].includes(String(body.state)) ||
    !('progress' in body) || typeof body.progress !== 'string' ||
    !('result_count' in body) || typeof body.result_count !== 'number'
  ) {
    throw new Error(`Server returned an invalid status while trying to ${action}`);
  }
  return body as OatRunStatus;
}

export async function main(args: string[], runner: typeof run_tree = run_tree): Promise<void> {
  if (args.includes('-h') || args.includes('--help')) {
    console.log(cliUsage);
    return;
  }
  validateArguments(args);
  const option = (name: string): string | undefined => {
    const index = args.indexOf(name);
    if (index < 0) return undefined;
    const value = args[index + 1];
    if (!value || value.startsWith('-')) throw new OatCliError(`Expected a value after ${name}`);
    return value;
  };
  const valuedOptions = new Set(['--config', '--mode', '--server', '--poll-ms']);
  const positionalConfig = args.find((value, index) =>
    !value.startsWith('--') && !valuedOptions.has(args[index - 1] ?? ''),
  );
  const requestedMode = option('--mode');
  if (requestedMode !== undefined && requestedMode !== 'local' && requestedMode !== 'aks') {
    throw new OatCliError('--mode must be local or aks');
  }
  const requestedConfig = option('--config') ?? positionalConfig;
  const requestedServerUrl = option('--server');
  const cleanupRunId = option('--cleanup-local');
  const viewerOnly = args.includes('--viewer-only');
  const preflightOnly = args.includes('--preflight-only');
  if (cleanupRunId !== undefined && (requestedMode === 'aks' || requestedServerUrl || args.includes('--poll-ms') || args.includes('--viewer-only') || args.includes('--preflight-only'))) {
    throw new OatCliError('--cleanup-local requires local mode and cannot be combined with server, poll, viewer, or preflight options');
  }
  if (requestedMode === 'aks') Object.assign(process.env, await loadAksEnvironment(workspaceRoot));
  if (preflightOnly) {
    if (requestedMode !== 'aks' || viewerOnly || requestedServerUrl || args.includes('--poll-ms')) {
      throw new OatCliError('--preflight-only requires --mode aks and cannot be combined with viewer/server/poll options');
    }
    const configPath = resolveOatConfigPath('aks', requestedConfig);
    const report = await runAksPreflight(configPath);
    console.log(formatPreflightReport(report));
    if (!report.ready) process.exitCode = 2;
    return;
  }
  const mode: 'local' | 'aks' = requestedMode === 'aks' ? 'aks' : 'local';
  const configPath = resolveOatConfigPath(mode, requestedConfig);
  if (viewerOnly) {
    if (mode !== 'local' || requestedServerUrl || args.includes('--poll-ms')) {
      throw new OatCliError('--viewer-only is local-only and supports only --mode local and --config');
    }
    await keepLocalViewerOpen(await startLocalViewer(configPath));
    return;
  }
  const pollValue = Number(option('--poll-ms') ?? 1000);
  if (!Number.isSafeInteger(pollValue) || pollValue < 1) {
    throw new OatCliError('Expected values after --server and --poll-ms');
  }
  let network: NetworkObservation | undefined;
  let warnings: string[] = [];
  if (mode === 'aks') {
    const report = await runAksPreflight(configPath);
    console.log(formatPreflightReport(report));
    if (!report.ready) throw new Error('AKS preflight is NOT READY; refusing to start the benchmark.');
    if ('network' in report && record(report.network) &&
        typeof report.network.provider === 'string' && typeof report.network.enforcing === 'boolean') {
      network = { provider: report.network.provider, enforcing: report.network.enforcing };
    }
    if ('warnings' in report && Array.isArray(report.warnings)) {
      warnings = report.warnings.filter((warning): warning is string => typeof warning === 'string');
    }
  }
  const config = await readOatConfigForMode(configPath, mode);
  if (cleanupRunId !== undefined) {
    if (mode !== 'local') throw new OatCliError('--cleanup-local is local-only');
    await recoverInterruptedLocalRuns(config, true);
    const projects = await cleanupLocalComposeProjects(config, cleanupRunId);
    await removeStaleLocalMarkers(config);
    console.log(projects.length ? `Cleaned local Compose project(s): ${projects.join(', ')}` : `No resources found for local run ${cleanupRunId}`);
    return;
  }
  if (mode === 'aks') {
    const subscriptionId = process.env.AZURE_SUBSCRIPTION_ID;
    if (!subscriptionId || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(subscriptionId)) {
      throw new Error('Validated selected Azure subscription is missing or invalid; rerun AKS setup.');
    }
    config.subscriptionId = subscriptionId;
  }
  if (requestedServerUrl) {
    const interrupt = mode === 'local' ? registerLocalInterrupts() : undefined;
    try {
      try {
        const code = await reportDataFlowRun(
          (progress) => launchOat(configPath, requestedServerUrl, pollValue, fetch, progress, mode, getDockerBridgeGateway, interrupt?.controller.signal),
          config, mode, undefined, runner, network, warnings,
        );
        if (interrupt?.signal) {
          process.exitCode = interrupt.exitCode;
          return;
        }
        if (code) process.exitCode = code;
      } catch (error) {
        if (!interrupt?.signal) throw error;
        process.exitCode = interrupt.exitCode;
        return;
      }
    } finally {
      interrupt?.dispose();
    }
    await maybeServeLocalViewer(configPath, mode);
    return;
  }

  if (mode === 'local') await recoverInterruptedLocalRuns(config);
  const server = await startLocalServer(runner);
  let markerContext: LocalRunMarkerContext | undefined;
  try {
    if (mode === 'local') markerContext = await prepareLocalRunMarker(config, server);
  } catch (error) {
    await stopLocalServer(server.child);
    throw error;
  }
  let markerWritten = false;
  let runCleanupComplete = false;
  const interrupt = mode === 'local' ? registerLocalInterrupts() : undefined;
  try {
    try {
      const localMarker = markerContext;
      const onRunStarted = localMarker ? async (status: OatRunStatus): Promise<void> => {
        await persistLocalRunMarker(localMarker, status);
        markerWritten = true;
      } : undefined;
      const onRunFinished = (status: OatRunStatus): void => {
        runCleanupComplete = status.state === 'succeeded' || status.state === 'cancelled';
      };
      const code = await reportDataFlowRun(
        (progress) => launchOat(configPath, server.url, pollValue, fetch, progress, mode, getDockerBridgeGateway, interrupt?.controller.signal, onRunStarted, onRunFinished),
        config, mode, server.gitSha, runner, network, warnings,
      );
      if (interrupt?.signal) {
        process.exitCode = interrupt.exitCode;
        return;
      }
      if (code) process.exitCode = code;
    } catch (error) {
      if (!interrupt?.signal) throw error;
      process.exitCode = interrupt.exitCode;
      return;
    }
  } finally {
    interrupt?.dispose();
    await stopLocalServer(server.child);
    if (markerWritten && runCleanupComplete && markerContext) await unlink(markerContext.path);
  }
  await maybeServeLocalViewer(configPath, mode);
}

export async function startLocalViewer(configPath: string): Promise<LocalViewer> {
  const config = await readLocalViewerConfig(configPath);
  if (!config) {
    throw new Error('--viewer-only requires platform.kind=local and website_url to be a file:// URL');
  }
  try {
    const resultFile = await stat(config.resultsPath);
    if (!resultFile.isFile()) throw new Error(`Local OAT results URL is not a file: ${config.resultsPath}`);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      throw new Error(`Local OAT results file does not exist: ${config.resultsPath}`);
    }
    throw error;
  }
  return createLocalViewer(config);
}

async function maybeServeLocalViewer(configPath: string, mode?: 'local' | 'aks'): Promise<void> {
  const config = await readLocalViewerConfig(configPath, mode);
  if (!config) return;
  await keepLocalViewerOpen(await createLocalViewer(config));
}

async function readLocalViewerConfig(
  configPath: string,
  mode?: 'local' | 'aks',
): Promise<LocalViewerConfig | undefined> {
  const parsed: unknown = JSON.parse(await readFile(configPath, 'utf8'));
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return;
  const config = parsed as { platform?: { kind?: string }; website_url?: string };
  const platform = mode ?? config.platform?.kind;
  const websiteUrl = config.website_url;
  if (platform !== 'local' || !websiteUrl?.startsWith('file://')) return undefined;
  const resultsPath = fileURLToPath(new URL(websiteUrl));
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const siteRoot = await realpath(resolve(workspace, 'docs/experiments/arrowcol-oat'));
  return { resultsPath, siteRoot };
}

async function createLocalViewer(config: LocalViewerConfig): Promise<LocalViewer> {
  const { resultsPath, siteRoot } = config;
  const server = createHttpServer(async (request, response) => {
    try {
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        response.writeHead(405, { allow: 'GET, HEAD' }).end();
        return;
      }
      const requestUrl = new URL(request.url ?? '/', 'http://127.0.0.1');
      if (requestUrl.pathname === '/results.parquet') {
        await sendViewerFile(request, response, await readFile(resultsPath), 'application/vnd.apache.parquet');
        return;
      }
      let pathname: string;
      try {
        pathname = decodeURIComponent(requestUrl.pathname === '/' ? '/index.html' : requestUrl.pathname);
      } catch {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const file = resolve(siteRoot, `.${pathname}`);
      if (!isPathWithin(siteRoot, file)) {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const actualFile = await realpath(file);
      if (!isPathWithin(siteRoot, actualFile)) {
        response.writeHead(403).end('Forbidden');
        return;
      }
      const contentTypes: Record<string, string> = {
        '.css': 'text/css; charset=utf-8',
        '.html': 'text/html; charset=utf-8',
        '.js': 'text/javascript; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.png': 'image/png',
        '.svg': 'image/svg+xml',
      };
      await sendViewerFile(
        request,
        response,
        await readFile(actualFile),
        contentTypes[extname(actualFile).toLowerCase()] ?? 'application/octet-stream',
      );
    } catch (error) {
      const code = error instanceof Error && 'code' in error && error.code === 'ENOENT' ? 404 : 500;
      response.writeHead(code).end(code === 404 ? 'Not found' : 'Could not serve local OAT viewer');
      if (code === 500) console.error(error);
    }
  });
  await new Promise<void>((resolvePromise, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolvePromise);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Could not allocate a local viewer port');
  return {
    url: `http://127.0.0.1:${address.port}/`,
    close: () => new Promise<void>((resolvePromise, reject) => {
      server.close((error) => error ? reject(error) : resolvePromise());
    }),
  };
}

function registerLocalInterrupts(): {
  controller: AbortController;
  readonly signal: NodeJS.Signals | undefined;
  readonly exitCode: number;
  dispose(): void;
} {
  const controller = new AbortController();
  let received: NodeJS.Signals | undefined;
  let count = 0;
  const exitCodes: Partial<Record<NodeJS.Signals, number>> = { SIGINT: 130, SIGTERM: 143, SIGHUP: 129 };
  const handlers = new Map<NodeJS.Signals, () => void>();
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
    const handler = (): void => {
      count++;
      if (count > 1) process.exit(128 + (exitCodes[signal] ?? 1));
      received = signal;
      controller.abort();
    };
    handlers.set(signal, handler);
    process.on(signal, handler);
  }
  return {
    controller,
    get signal() { return received; },
    get exitCode() { return received ? exitCodes[received] ?? 1 : 0; },
    dispose() {
      for (const [signal, handler] of handlers) process.off(signal, handler);
    },
  };
}
async function keepLocalViewerOpen(viewer: LocalViewer): Promise<void> {
  console.log(`Local results viewer: ${viewer.url}`);
  console.log('Press Ctrl+C to stop the viewer.');
  try {
    await new Promise<void>((resolvePromise) => {
      const stop = (): void => {
        process.off('SIGINT', stop);
        process.off('SIGTERM', stop);
        process.off('SIGHUP', stop);
        resolvePromise();
      };
      process.once('SIGINT', stop);
      process.once('SIGTERM', stop);
      process.once('SIGHUP', stop);
    });
  } finally {
    await viewer.close();
  }
}

function isPathWithin(root: string, target: string): boolean {
  const path = relative(root, target);
  return path !== '..' && !path.startsWith(`..${sep}`) && !isAbsolute(path);
}

export async function sendViewerFile(
  request: import('node:http').IncomingMessage,
  response: import('node:http').ServerResponse,
  data: Buffer,
  contentType: string,
): Promise<void> {
  const range = request.headers.range;
  const headers = {
    'accept-ranges': 'bytes',
    'cache-control': 'no-store',
    'content-type': contentType,
  };
  if (!range) {
    response.writeHead(200, { ...headers, 'content-length': data.length });
    response.end(request.method === 'HEAD' ? undefined : data);
    return;
  }
  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match || (!match[1] && !match[2])) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const suffixLength = match[1] ? undefined : Number(match[2]);
  const start = match[1] ? Number(match[1]) : Math.max(0, data.length - (suffixLength ?? 0));
  const end = match[2] && match[1] ? Math.min(Number(match[2]), data.length - 1) : data.length - 1;
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || end >= data.length) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const body = data.subarray(start, end + 1);
  response.writeHead(206, {
    ...headers,
    'content-length': body.length,
    'content-range': `bytes ${start}-${end}/${data.length}`,
  });
  response.end(request.method === 'HEAD' ? undefined : body);
}

export async function buildOwnedServer(
  workspace: string,
  runner: typeof run_tree = run_tree,
): Promise<{ executable: string; gitSha: string }> {
  const root = await realpath(workspace);
  const run = async (command: string[]): Promise<string> => {
    const result = await runner(command, command[0] === 'cargo' ? 600 : 15, root);
    if (result.returncode) throw new Error(`OAT ${command[0]} failed: ${result.stderr.trim()}`);
    if (command[0] === 'cargo' && result.stderr) process.stderr.write(result.stderr);
    return result.stdout;
  };
  const source = async () => ({
    sha: (await run(['git', 'rev-parse', '--verify', 'HEAD'])).trim(),
    status: await run(['git', 'status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none']),
  });
  const before = await source();
  if (!/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(before.sha)) {
    throw new Error('Git did not return a full OAT source commit');
  }
  const output = await run([
    'cargo', 'build', '--locked', '--package', 'arrowcol-oat-rust',
    '--bin', 'arrowcol-oat-rust', '--message-format=json-render-diagnostics',
  ]);
  const after = await source();
  if (before.sha !== after.sha || before.status !== after.status) {
    throw new Error('OAT source changed during the build; refusing to start an ambiguously attributed server');
  }
  const executables: string[] = [];
  for (const line of output.split('\n').filter((line) => line.trim())) {
    const message: unknown = JSON.parse(line);
    if (!message || typeof message !== 'object' ||
        !('reason' in message) || message.reason !== 'compiler-artifact' ||
        !('manifest_path' in message) ||
        message.manifest_path !== resolve(root, 'projects/arrowcol-oat-rust/Cargo.toml') ||
        !('target' in message) || !message.target || typeof message.target !== 'object' ||
        !('name' in message.target) || message.target.name !== 'arrowcol-oat-rust' ||
        !('kind' in message.target) || !Array.isArray(message.target.kind) || !message.target.kind.includes('bin') ||
        !('executable' in message) || typeof message.executable !== 'string' || !isAbsolute(message.executable)) continue;
    executables.push(message.executable);
  }
  if (executables.length !== 1) throw new Error('Cargo did not identify exactly one built OAT server executable');
  const gitSha = before.status.trim() ? 'unknown' : before.sha;
  if (gitSha === 'unknown') console.warn('OAT source has uncommitted changes; result git_sha will remain unknown.');
  return { executable: await realpath(executables[0]!), gitSha };
}

export async function startLocalServer(runner: typeof run_tree = run_tree): Promise<{ child: ChildProcess; url: string; gitSha: string; executable: string }> {
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const build = await buildOwnedServer(workspace, runner);
  const port = await availablePort();
  const url = `http://127.0.0.1:${port}`;
  const child = spawn(build.executable, ['--listen', `127.0.0.1:${port}`], {
    cwd: workspace, stdio: 'inherit', env: { ...process.env, GIT_SHA: build.gitSha },
  });
  let exit: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  let spawnError: Error | undefined;
  child.once('exit', (code, signal) => { exit = { code, signal }; });
  child.once('error', (error) => { spawnError = error; });
  const deadline = Date.now() + 10 * 60 * 1000;
  while (Date.now() < deadline) {
    if (spawnError) throw new Error(`Could not start OAT server: ${spawnError.message}`);
    if (exit) throw new Error(`OAT server exited before becoming ready (${exit.signal ?? exit.code})`);
    try {
      const response = await fetch(new URL('/healthz', url), { signal: AbortSignal.timeout(2000) });
      if (response.ok) return { child, url, gitSha: build.gitSha, executable: build.executable };
    } catch {
      // The server may still be compiling or starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  await stopLocalServer(child);
  throw new Error('OAT server did not become ready within ten minutes');
}

async function availablePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === 'string') {
    server.close();
    throw new Error('Could not allocate a local OAT server port');
  }
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

export async function stopLocalServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
  child.kill('SIGTERM');
  let timeout: NodeJS.Timeout | undefined;
  const stopped = await Promise.race([
    exited.then(() => true),
    new Promise<boolean>((resolve) => { timeout = setTimeout(() => resolve(false), 5000); }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (!stopped) throw new Error('OAT server did not stop after SIGTERM; check its process before retrying');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = cliExitCode(error);
  });
}
