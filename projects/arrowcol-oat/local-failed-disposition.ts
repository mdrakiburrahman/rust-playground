import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { link, lstat, mkdir, open, readFile, readdir, rename, rm, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { guardContainerMemoryMatches, guardContainerMemoryMetric, parseLocalCollectorCpuQuota } from './campaign-results.js';

export const localDispositionSuffix = '.failed-local-disposition';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const sha256 = (bytes: Uint8Array): string => createHash('sha256').update(bytes).digest('hex');
type Json = Record<string, unknown>;

export type PreloadProofPins = {
  invocation: string;
  run: string;
  cell: string;
  table: string;
  collectorImage: string;
  collectorRevision: string;
  receiptSha256: string;
  markerSha256: string;
  resultSha256: string;
  logSha256: string;
};

const reviewedPreloadPins: PreloadProofPins = Object.freeze({
  invocation: '89fdefa5-855d-46ea-93ea-481fd77cbd2e',
  run: '8d46570b-791e-43c6-a25a-4832c5cbee81',
  cell: '241b7533-1025-441c-b8b2-ae9bbc547c1e',
  table: 'sqlserver_availability_replica_states',
  collectorImage: 'sha256:c6ed6724f3323ca120fe50f93fccf505c48eb593cd3b7c2e5c8ce85b92a8a93e',
  collectorRevision: '3630d3e4301bc3ba8f9f69a308d077cebee6d12a',
  receiptSha256: '9c288590a651a8fcdbe1ba9fa242ed235b565923416392e5bbbb6c9701d359a8',
  markerSha256: '9dafe9bf79d8dd0f3c7398a2abcbe851a0a931fec4f319d8bfe5fa0facf9d12e',
  resultSha256: 'b6dc74b976de44d22f4ee328f4add64cdf36060e05bb7b268191cac3eeea7ee1',
  logSha256: '04fd38d33e9ac82af23be0d5fcefb078128a0bc939329e50f7bb3a19a7663503',
});

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function object(value: unknown, label: string): Json {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new Error(`Invalid ${label}`);
  return value as Json;
}

function string(value: unknown, label: string): string {
  if (typeof value !== 'string' || value.length === 0) throw new Error(`Invalid ${label}`);
  return value;
}

async function boundedFile(path: string, maximum = 16 * 1024 * 1024): Promise<Buffer> {
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const info = await file.stat();
    if (!info.isFile() || info.size === 0 || info.size > maximum) throw new Error(`Invalid evidence file: ${path}`);
    return await file.readFile();
  } finally {
    await file.close();
  }
}

async function startTicks(pid: number): Promise<string | undefined> {
  try {
    const stat = await readFile(`/proc/${pid}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/);
    if (fields[0] === 'Z') return undefined;
    if (fields.length <= 19) throw new Error('Invalid process identity');
    return fields[19];
  } catch (error) {
    if (error instanceof Error && 'code' in error && (error.code === 'ENOENT' || error.code === 'ESRCH')) return undefined;
    throw error;
  }
}

type RecoveryEnvironment = {
  docker: (args: string[]) => string;
  processStartTicks: (pid: number) => Promise<string | undefined>;
  bootId: () => Promise<string>;
  listeningSockets?: (port: number) => string;
};

const environment: RecoveryEnvironment = {
  docker: args => execFileSync('docker', args, {
    encoding: 'utf8', timeout: 10_000,
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: '/nonexistent', DOCKER_HOST: 'unix:///var/run/docker.sock' },
  }),
  processStartTicks: startTicks,
  bootId: async () => (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(),
  listeningSockets: port => execFileSync('ss', ['-H', '-ltnp', `sport = :${port}`], {
    encoding: 'utf8', timeout: 10_000,
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: '/nonexistent', DOCKER_HOST: 'unix:///var/run/docker.sock' },
  }),
};

function readListeningSockets(env: RecoveryEnvironment, port: number): string {
  const check = env.listeningSockets ?? environment.listeningSockets;
  if (!check) throw new Error('Local listener check is unavailable');
  return check(port);
}

function projectName(base: unknown, cell: string, length: number, compactLength: number): string {
  if (typeof base !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(base)) throw new Error('Invalid Compose project');
  return `${base.slice(0, length).replace(/[-_]+$/g, '')}-${cell.replace(/-/g, '').slice(0, compactLength).toLowerCase()}`;
}

async function inspect(
  receiptPath: string, markerPath: string, env: RecoveryEnvironment, preload?: PreloadProofPins,
  classifyQuarantine = false,
): Promise<Json> {
  const receiptBytes = await boundedFile(receiptPath, 1024 * 1024);
  const markerBytes = await boundedFile(markerPath, 1024 * 1024);
  if (preload && (sha256(receiptBytes) !== preload.receiptSha256 || sha256(markerBytes) !== preload.markerSha256)) {
    throw new Error('Pre-load receipt or marker hash mismatch');
  }
  const receipt = object(JSON.parse(receiptBytes.toString('utf8')), 'receipt');
  const marker = object(JSON.parse(markerBytes.toString('utf8')), 'ownership marker');
  const invocation = string(receipt.invocation_id, 'invocation');
  const run = string(receipt.run_id, 'run');
  if (!uuid.test(invocation) || basename(receiptPath) !== `${invocation}.json` ||
      !uuid.test(run) || receipt.version !== 1 ||
      receipt.creation_state !== 'accepted' || receipt.run_state !== 'failed' ||
      receipt.cleanup_state !== 'unsettled' || receipt.config_binding_error !== null ||
      marker.version !== 1 || marker.runId !== run ||
      marker.submitted_config_body !== receipt.submitted_config_body ||
      marker.submitted_config_sha256 !== receipt.submitted_config_sha256 ||
      marker.source_config_sha256 !== receipt.source_config_sha256 ||
      marker.serverPid !== receipt.server_pid || marker.serverStartTicks !== receipt.server_start_ticks ||
      marker.serverExecutable !== receipt.server_executable || marker.serverUrl !== receipt.server_url) {
    throw new Error('Failed receipt and ownership marker do not match');
  }
  if (preload && (invocation !== preload.invocation || run !== preload.run)) {
    throw new Error('Pre-load invocation or run does not match the pinned identity');
  }
  if (typeof receipt.source_config_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(receipt.source_config_sha256)) {
    throw new Error('Source configuration hash is invalid');
  }
  const serverUrl = new URL(string(marker.serverUrl, 'server URL'));
  if (serverUrl.protocol !== 'http:' || serverUrl.hostname !== '127.0.0.1' || !serverUrl.port ||
      serverUrl.username || serverUrl.password || typeof marker.serverExecutable !== 'string' ||
      !isAbsolute(marker.serverExecutable)) throw new Error('Unsafe owned server identity');
  const listenerOutput = readListeningSockets(env, Number(serverUrl.port));
  if (listenerOutput.trim()) throw new Error('Local OAT server port is still listening');
  const body = string(receipt.submitted_config_body, 'submitted configuration');
  if (sha256(Buffer.from(body)) !== receipt.submitted_config_sha256) throw new Error('Submitted configuration hash mismatch');
  const observation = object(receipt.terminal_observation, 'terminal observation');
  const status = object(observation.status, 'terminal status');
  if (status.id !== run || status.state !== 'failed' || status.config_sha256 !== receipt.submitted_config_sha256 ||
      typeof status.error !== 'string' || !status.error ||
      typeof receipt.started_at_unix_ms !== 'number' || !Number.isSafeInteger(receipt.started_at_unix_ms) ||
      typeof status.started_at_unix_ms !== 'number' || !Number.isSafeInteger(status.started_at_unix_ms) ||
      status.started_at_unix_ms < receipt.started_at_unix_ms ||
      typeof status.finished_at_unix_ms !== 'number' || !Number.isSafeInteger(status.finished_at_unix_ms) || status.finished_at_unix_ms <= 0 ||
      status.finished_at_unix_ms < status.started_at_unix_ms ||
      typeof observation.observed_at_unix_ms !== 'number' || !Number.isSafeInteger(observation.observed_at_unix_ms) ||
      observation.observed_at_unix_ms < status.finished_at_unix_ms) {
    throw new Error('Failed terminal observation is not bound to the receipt');
  }
  for (const [pid, ticks] of [[marker.ownerPid, marker.ownerStartTicks], [marker.serverPid, marker.serverStartTicks]]) {
    if (typeof pid !== 'number' || !Number.isSafeInteger(pid) || pid <= 0 ||
        typeof ticks !== 'string' || !/^\d+$/.test(ticks) || await env.processStartTicks(pid) === ticks) {
      throw new Error('Owned launcher or server is still active or has invalid identity');
    }
  }
  const config = object(JSON.parse(body), 'configuration');
  const platform = object(config.platform, 'platform');
  if (!guardContainerMemoryMetric(config)) throw new Error('Invalid overload_guard.container_memory_metric');
  if (platform.kind !== 'local' || !Array.isArray(config.tables) || config.tables.length !== 1 ||
      !Array.isArray(config.replica_counts) || config.replica_counts.length !== 1 ||
      !Array.isArray(config.dfe_worker_concurrency) || config.dfe_worker_concurrency.length !== 1) {
    throw new Error('Local disposition requires exactly one configured cell');
  }
  const localConfig = isRecord(platform.local) ? platform.local : undefined;
  const quota = parseLocalCollectorCpuQuota(localConfig?.collector_cpu_quota);
  if (quota.requested_quota !== null) {
    let hostCpus: number;
    try {
      const output = env.docker(['info', '--format', '{{.NCPU}}']).trim();
      hostCpus = /^\d+$/.test(output) ? Number(output) : NaN;
    } catch (error) {
      throw new Error('platform.local.collector_cpu_quota cannot read Docker host logical CPUs', { cause: error });
    }
    parseLocalCollectorCpuQuota(quota.requested_quota, hostCpus);
  }
  if (preload && ((config.clients_per_table_sweep !== undefined && config.clients_per_table_sweep !== null) ||
      config.replica_counts[0] !== 1 || config.dfe_worker_concurrency[0] !== 32 ||
      config.tables[0] !== preload.table)) {
    throw new Error('Pre-load configuration is not the pinned single-cell profile');
  }
  if (preload) {
    const local = object(platform.local, 'local platform');
    const image = object(local.collector_image_override, 'collector image');
    if (image.image_id !== preload.collectorImage || image.source_revision !== preload.collectorRevision) {
      throw new Error('Pre-load collector image does not match the pinned configuration');
    }
  }
  const resultUrl = new URL(string(config.result_url, 'result URL'));
  if (resultUrl.protocol !== 'file:' || resultUrl.search || resultUrl.hash || resultUrl.hostname) throw new Error('Unsafe local result URL');
  const resultPath = fileURLToPath(resultUrl);
  const runRoot = dirname(resultPath);
  const stateRoot = string(platform.compose_state_root, 'state root');
  if (!isAbsolute(markerPath) || dirname(resolve(markerPath)) !== resolve(stateRoot) || resolve(stateRoot) !== join(runRoot, 'state')) {
    throw new Error('Recovery ownership marker is outside the Compose state root');
  }
  const logPath = join(runRoot, 'run.log');
  const resultBytes = await boundedFile(resultPath);
  const logBytes = await boundedFile(logPath);
  if (preload && (sha256(resultBytes) !== preload.resultSha256 || sha256(logBytes) !== preload.logSha256)) {
    throw new Error('Pre-load result or log hash mismatch');
  }
  const [{ tableFromIPC }, { readParquet }] = await Promise.all([import('apache-arrow'), import('parquet-wasm')]);
  const table = tableFromIPC(readParquet(resultBytes).intoIPCStream());
  if (table.numRows !== 1) throw new Error('Expected exactly one failed cell result');
  const row = table.toArray()[0];
  const cell = string(row.run_id, 'cell');
  const evidence = object(JSON.parse(string(row.evidence_json, 'cell evidence')), 'cell evidence');
  if (preload) {
    const tunables = object(JSON.parse(string(row.tunables_json, 'cell tunables')), 'cell tunables');
    if (!uuid.test(cell) || cell !== preload.cell || row.mode !== 'local' || row.status !== 'failed' ||
        row.table !== config.tables[0] || tunables.orchestration_run_id !== run ||
        Number(row.replicas) !== 1 || Number(row.dfe_worker_concurrency) !== 32 ||
        tunables.dfe_worker_concurrency !== 32 ||
        evidence.overload_guard !== null || evidence.overload_stop !== null ||
        evidence.preload_delta !== null || evidence.durable_rows_per_second !== null ||
        evidence.client_schedule !== null || evidence.client_phase_reports !== null ||
        evidence.durable_commits !== null || evidence.durable_bytes !== null ||
        object(evidence.rows, 'cell rows').delta_committed !== null) {
      throw new Error('Pre-load failed cell or startup error does not match');
    }
  } else {
    const rows = isRecord(evidence.rows) ? evidence.rows : undefined;
    const preMeasurementFailure = evidence.overload_guard === null && evidence.overload_stop === null &&
      evidence.preload_delta === null && evidence.durable_rows_per_second === null &&
      evidence.client_schedule === null && evidence.client_phase_reports === null &&
      evidence.durable_commits === null && evidence.durable_bytes === null && rows?.delta_committed === null;
    const guard = preMeasurementFailure ? undefined : object(evidence.overload_guard, 'overload evidence');
    if (guard && !guardContainerMemoryMatches(config, guard)) {
      throw new Error('Invalid overload_guard.container_memory_metric guard evidence');
    }
    const failedCell = row.status === 'failed' ||
      (row.status === 'pinning_violation' && row.pinning_ok === false && status.error.includes('pinning_violation'));
    if (!rows || !uuid.test(cell) || row.mode !== 'local' || !failedCell || row.table !== config.tables[0] ||
        (!preMeasurementFailure && guard?.run_id !== cell)) throw new Error('Failed cell identity mismatch');
  }
  const project = projectName(platform.compose_project, cell, 15, 32);
  const legacy = projectName(platform.compose_project, cell, 39, 8);
  const log = logBytes.toString('utf8');
  const startupFailure = `dependency failed to start: container ${project}-dataflow-1-1 exited (127)`;
  if (!log.includes(run) || !log.includes(project) || !log.includes(status.error) ||
      (preload && (!status.error.includes('prepare isolated OAT matrix-cell platform') ||
        !status.error.includes('start remaining local OAT stack failed') ||
        !status.error.includes(startupFailure) || !log.includes(startupFailure) ||
        !log.includes(`[oat] Preserving OAT server for unresolved run receipt ${receiptPath}`)))) {
    throw new Error('Run log does not bind the failure and cell');
  }
  const stateInfo = await lstat(stateRoot);
  if (!stateInfo.isDirectory() || stateInfo.isSymbolicLink()) throw new Error('Invalid local state root');
  const entries = await readdir(stateRoot);
  const markerName = basename(resolve(markerPath));
  if (entries.some(name => uuid.test(name)) || entries.some(name => /^\.arrowcol-oat-owner-.*\.json$/.test(name) && name !== markerName) ||
      (preload && entries.some(name => name !== markerName))) {
    throw new Error('Local state contains an unresolved cell or owner');
  }
  for (const name of [project, legacy]) {
    for (const args of [
      ['ps', '-aq', '--filter', `label=com.docker.compose.project=${name}`],
      ['network', 'ls', '-q', '--filter', `label=com.docker.compose.project=${name}`],
      ['volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${name}`],
    ]) {
      if (env.docker(args).trim()) throw new Error(`Local Compose resources remain for ${name}`);
    }
  }
  if (env.docker(['ps', '-aq', '--filter', `name=^/oat-client-${cell.replace(/-/g, '').slice(0, 12)}-`]).trim()) {
    throw new Error('Local client containers remain');
  }
  const bootId = await env.bootId();
  if (!uuid.test(bootId)) throw new Error('Host boot identity unavailable');
  return {
    version: 1, kind: preload ? 'verified-preload-failed-cleanup-read-only' : 'verified-local-failed-cleanup',
    ...(preload ? {
      disposition: 'not-recorded', qualification: 'not-evaluated',
      failure: 'collector dependency exited 127 during stack startup',
      underlying_error: 'not-established-from-retained-artifacts',
    } : {}),
    invocation_id: invocation, run_id: run,
    cell_id: cell, project, legacy_project: legacy, boot_id: bootId, receipt_sha256: sha256(receiptBytes),
    marker_path: markerPath, marker_sha256: sha256(markerBytes), result_path: resultPath,
    result_sha256: sha256(resultBytes), log_path: logPath, log_sha256: sha256(logBytes),
    state_root: stateRoot, owner_pid: marker.ownerPid, owner_start_ticks: marker.ownerStartTicks,
    collector_cpu_quota: quota,
    server_pid: marker.serverPid, server_start_ticks: marker.serverStartTicks,
    measurement_produced: evidence.overload_guard !== null || evidence.durable_commits !== null || evidence.client_schedule !== null,
    ...(classifyQuarantine ? { quarantine_disposition: classifyLocalQuarantine(row, evidence, status.error) } : {}),
    port: Number(serverUrl.port), listener_output: listenerOutput,
    cleanup: { state_empty: true, compose_resources: 0, legacy_resources: 0, client_containers: 0 },
  };
}

export async function inspectPinnedPreloadFailure(
  receiptPath: string, markerPath: string, pins: PreloadProofPins, env: RecoveryEnvironment = environment,
): Promise<Json> {
  const first = await inspect(receiptPath, markerPath, env, pins);
  const second = await inspect(receiptPath, markerPath, env, pins);
  if (JSON.stringify(first) !== JSON.stringify(second)) throw new Error('Pre-load evidence changed during inspection');
  return first;
}

export async function inspectReviewedPreloadFailure(
  receiptPath: string, markerPath: string,
): Promise<Json> {
  return inspectPinnedPreloadFailure(receiptPath, markerPath, reviewedPreloadPins);
}

async function dispositionProof(receiptPath: string, markerPath: string, env: RecoveryEnvironment): Promise<Json> {
  if (basename(receiptPath) !== `${reviewedPreloadPins.invocation}.json`) return inspect(receiptPath, markerPath, env);
  const proof = await inspectPinnedPreloadFailure(receiptPath, markerPath, reviewedPreloadPins, env);
  return { ...proof, kind: 'verified-preload-failed-cleanup', disposition: 'recorded' };
}

async function assertNoConflictingAksDisposition(receiptPath: string): Promise<void> {
  try {
    await lstat(`${receiptPath}.failed-aks-disposition`);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return;
    throw error;
  }
  throw new Error('Conflicting AKS disposition exists');
}

export async function assertLocalFailedDisposition(
  receiptPath: string, dispositionPath: string, env: RecoveryEnvironment = environment,
): Promise<void> {
  if (basename(receiptPath) === `${reviewedPreloadPins.invocation}.json`) await assertNoConflictingAksDisposition(receiptPath);
  const saved = object(JSON.parse((await boundedFile(dispositionPath, 1024 * 1024)).toString('utf8')), 'local disposition');
  const markerPath = string(saved.marker_path, 'marker path');
  const { recorded_at_unix_ms: recordedAt, ...recordedProof } = saved;
  if (typeof recordedAt !== 'number' || !Number.isSafeInteger(recordedAt) || recordedAt <= 0 || recordedAt > Date.now()) {
    throw new Error('Local disposition timestamp is invalid');
  }
  const expected = await dispositionProof(receiptPath, markerPath, env);
  if (!Object.hasOwn(recordedProof, 'collector_cpu_quota')) delete expected.collector_cpu_quota;
  if (JSON.stringify(recordedProof) !== JSON.stringify(expected)) throw new Error('Local failure disposition or evidence changed');
}

export async function recordLocalFailedDisposition(
  root: string, invocation: string, markerPath: string, env: RecoveryEnvironment = environment,
): Promise<void> {
  if (!uuid.test(invocation)) throw new Error('Invalid invocation ID');
  const receiptPath = resolve(root, `${invocation}.json`);
  const dispositionPath = receiptPath + localDispositionSuffix;
  if (invocation === reviewedPreloadPins.invocation) await assertNoConflictingAksDisposition(receiptPath);
  const proof = await dispositionProof(receiptPath, markerPath, env);
  if (proof.invocation_id !== invocation) throw new Error('Invocation does not match receipt');
  const temporary = `${dispositionPath}.${randomUUID()}.tmp`;
  let created = false;
  try {
    const file = await open(temporary, 'wx', 0o600);
    created = true;
    try {
      await file.writeFile(`${JSON.stringify({ ...proof, recorded_at_unix_ms: Date.now() })}\n`);
      await file.sync();
    } finally {
      await file.close();
    }
    const verified = await dispositionProof(receiptPath, markerPath, env);
    if (JSON.stringify(verified) !== JSON.stringify(proof)) throw new Error('Recovery evidence changed while recording');
    if (invocation === reviewedPreloadPins.invocation) await assertNoConflictingAksDisposition(receiptPath);
    await link(temporary, dispositionPath);
    const directory = await open(dirname(receiptPath), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally {
    if (created) await unlink(temporary);
  }
}

const operatorAuthorization = 'coordinator f0767c48 (ArrowCollector tunability Project)';
export const localQuarantineDispositions = [
  'failed_before_measurement_quarantined_not_settled',
  'failed_incomplete_delivery_quarantined_not_settled',
  'failed_teardown_drain_quarantined_not_settled',
] as const;

function classifyLocalQuarantine(row: Json, evidence: Json, error: string): string | null {
  const measured = evidence.overload_guard !== null || evidence.durable_commits !== null || evidence.client_schedule !== null;
  if (!measured) return localQuarantineDispositions[0];
  if (row.status !== 'failed') return null;
  const failure = typeof row.failure === 'string' ? row.failure : '';
  if (error.includes('dataflow drain incomplete') || failure.includes('dataflow drain incomplete')) {
    return localQuarantineDispositions[2];
  }
  const durable = isRecord(evidence.durable_commits) ? evidence.durable_commits : undefined;
  const reconciliation = isRecord(evidence.reconciliation) ? evidence.reconciliation : undefined;
  const admissions = isRecord(durable?.collector_teardown_admissions) ? durable.collector_teardown_admissions : undefined;
  if (evidence.complete_delivery_established === false || durable?.complete_delivery_established === false ||
      reconciliation?.status === 'unsettled' || reconciliation?.status === 'outside_bound' ||
      admissions?.status === 'ambiguous') return localQuarantineDispositions[1];
  return null;
}

async function writeDurable(path: string, contents: Uint8Array): Promise<void> {
  const file = await open(path, 'wx', 0o600);
  try {
    await file.writeFile(contents);
    await file.sync();
  } finally {
    await file.close();
  }
}

async function syncDirectory(path: string): Promise<void> {
  const directory = await open(path, 'r');
  try { await directory.sync(); } finally { await directory.close(); }
}

function utcNow(): string {
  return new Date().toISOString();
}

export async function reconcileAndQuarantineLocalFailedReceipt(
  root: string, invocation: string, markerPath: string, env: RecoveryEnvironment = environment,
): Promise<Json> {
  if (!uuid.test(invocation)) throw new Error('Invalid invocation ID');
  const receiptPath = resolve(root, `${invocation}.json`);
  const proof = await inspect(receiptPath, markerPath, env, undefined, true);
  if (proof.invocation_id !== invocation || typeof proof.quarantine_disposition !== 'string') {
    throw new Error('Operator quarantine requires a supported failed-run disposition');
  }
  const receiptBytes = await boundedFile(receiptPath, 1024 * 1024);
  const markerBytes = await boundedFile(markerPath, 1024 * 1024);
  const receipt = object(JSON.parse(receiptBytes.toString('utf8')), 'receipt');
  const serverUrl = new URL(string(receipt.server_url, 'server URL'));
  const resultPath = string(proof.result_path, 'result path');
  const logPath = string(proof.log_path, 'run log path');
  const project = string(proof.project, 'Compose project');
  const legacy = string(proof.legacy_project, 'legacy Compose project');
  const cell = string(proof.cell_id, 'cell ID');

  const checks: Json = {};
  const ownerObserved = await env.processStartTicks(Number(proof.owner_pid));
  const serverObserved = await env.processStartTicks(Number(proof.server_pid));
  if (ownerObserved === proof.owner_start_ticks || serverObserved === proof.server_start_ticks) {
    throw new Error('Owned launcher or server process is still active');
  }
  checks.server_pid = {
    pid: proof.server_pid, start_ticks: proof.server_start_ticks, checked_at_utc: utcNow(),
    output: serverObserved === undefined ? 'recorded process identity is absent' : `PID reused with start ticks ${serverObserved}`,
    absent: true,
  };
  checks.owner_pid = {
    pid: proof.owner_pid, start_ticks: proof.owner_start_ticks, checked_at_utc: utcNow(),
    output: ownerObserved === undefined ? 'recorded process identity is absent' : `PID reused with start ticks ${ownerObserved}`,
    absent: true,
  };
  const listenerOutput = readListeningSockets(env, Number(serverUrl.port));
  if (listenerOutput.trim()) throw new Error('Local OAT server port is still listening');
  checks.port = { host: serverUrl.hostname, port: Number(serverUrl.port), checked_at_utc: utcNow(), output: listenerOutput, listening: false };
  const dockerChecks: Json[] = [];
  for (const composeProject of [project, legacy]) {
    for (const [kind, args] of [
      ['containers', ['ps', '-aq', '--filter', `label=com.docker.compose.project=${composeProject}`]],
      ['networks', ['network', 'ls', '-q', '--filter', `label=com.docker.compose.project=${composeProject}`]],
      ['volumes', ['volume', 'ls', '-q', '--filter', `label=com.docker.compose.project=${composeProject}`]],
    ] as const) {
      const output = env.docker([...args]);
      if (output.trim()) throw new Error(`Local Compose ${kind} remain for ${composeProject}`);
      dockerChecks.push({ kind, project: composeProject, checked_at_utc: utcNow(), args, output });
    }
  }
  const clientArgs = ['ps', '-aq', '--filter', `name=^/oat-client-${cell.replace(/-/g, '').slice(0, 12)}-`];
  const clientOutput = env.docker(clientArgs);
  if (clientOutput.trim()) throw new Error('Local client containers remain');
  dockerChecks.push({ kind: 'client_containers', checked_at_utc: utcNow(), args: clientArgs, output: clientOutput });
  checks.docker = dockerChecks;

  const quarantineRoot = resolve(dirname(root), 'operator-quarantine');
  const quarantineDir = join(quarantineRoot, invocation);
  const stagingDir = join(dirname(quarantineRoot), `.operator-quarantine-${invocation}-${randomUUID()}.tmp`);
  let receiptMoved = false;
  let markerMoved = false;
  let committed = false;
  await mkdir(quarantineRoot, { recursive: true, mode: 0o700 });
  try {
    try {
      await lstat(quarantineDir);
      throw new Error('Operator quarantine already exists for this invocation');
    } catch (error) {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    }
    await mkdir(stagingDir, { mode: 0o700 });
    const evidenceDir = join(stagingDir, 'evidence');
    await mkdir(evidenceDir, { mode: 0o700 });
    const archivedEvidence: Array<{ name: string; path: string; sha256: string }> = [];
    const evidenceSources: Array<{ name: string; path: string; archiveName: string }> = [
      { name: 'result', path: resultPath, archiveName: `result-${basename(resultPath)}` },
      { name: 'run_log', path: logPath, archiveName: 'run.log' },
    ];
    const missingEvidence: string[] = [];
    for (const [name, value] of [
      ['server_stdout', receipt.server_stdout_path],
      ['server_stderr', receipt.server_stderr_path],
      ['cleanup_output', receipt.cleanup_output_path],
    ] as const) {
      if (typeof value !== 'string' || value.length === 0) {
        missingEvidence.push(`${name}_not_recorded`);
        continue;
      }
      const source = resolve(value);
      if (dirname(source) !== dirname(resultPath)) throw new Error(`${name} evidence is outside the run root`);
      evidenceSources.push({ name, path: source, archiveName: `${name}-${basename(source)}` });
    }
    for (const source of evidenceSources) {
      try {
        const contents = await boundedFile(source.path, 64 * 1024 * 1024);
        const archivePath = join(evidenceDir, source.archiveName);
        await writeDurable(archivePath, contents);
        archivedEvidence.push({ name: source.name, path: `evidence/${source.archiveName}`, sha256: sha256(contents) });
      } catch (error) {
        if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
          missingEvidence.push(source.name);
        } else {
          throw error;
        }
      }
    }
    if (!archivedEvidence.some(item => item.name === 'result') || !archivedEvidence.some(item => item.name === 'run_log')) {
      throw new Error('Required result or run-log evidence is unavailable');
    }
    archivedEvidence.push({ name: 'owner_marker', path: 'evidence/owner-marker.json', sha256: sha256(markerBytes) });
    const record: Json = {
      version: 1,
      authorized_by: operatorAuthorization,
      authorized_at_utc: utcNow(),
      reason: 'Failed run retained as evidence only; the recorded server and launcher are gone, the port is closed, and no run-owned Docker resources remain.',
      invocation_id: invocation,
      run_id: proof.run_id,
      cell_id: cell,
      receipt_sha256: sha256(receiptBytes),
      archived_evidence_files: archivedEvidence,
      missing_evidence: missingEvidence,
      custody_checks: checks,
      disposition: proof.quarantine_disposition,
    };
    await syncDirectory(evidenceDir);
    const finalProof = await inspect(receiptPath, markerPath, env, undefined, true);
    if (JSON.stringify(finalProof) !== JSON.stringify(proof) ||
        sha256(await boundedFile(receiptPath, 1024 * 1024)) !== sha256(receiptBytes) ||
        sha256(await boundedFile(markerPath, 1024 * 1024)) !== sha256(markerBytes)) {
      throw new Error('Recovery evidence changed during operator reconciliation');
    }
    await rename(receiptPath, join(stagingDir, `${invocation}.json`));
    receiptMoved = true;
    await rename(markerPath, join(evidenceDir, 'owner-marker.json'));
    markerMoved = true;
    await syncDirectory(dirname(receiptPath));
    await syncDirectory(dirname(markerPath));
    await writeDurable(join(stagingDir, 'operator-reconciliation.json'), Buffer.from(`${JSON.stringify(record, null, 2)}\n`));
    await syncDirectory(evidenceDir);
    await syncDirectory(stagingDir);
    await rename(stagingDir, quarantineDir);
    committed = true;
    await syncDirectory(quarantineRoot);
    return record;
  } catch (error) {
    if (committed) throw error;
    const rollbackErrors: unknown[] = [];
    if (markerMoved) {
      try { await rename(join(stagingDir, 'evidence', 'owner-marker.json'), markerPath); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
    }
    if (receiptMoved) {
      try { await rename(join(stagingDir, `${invocation}.json`), receiptPath); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
    }
    if (markerMoved || receiptMoved) {
      try { await syncDirectory(dirname(receiptPath)); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
      try { await syncDirectory(dirname(markerPath)); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
    }
    try { await rm(stagingDir, { recursive: true, force: true }); } catch (rollbackError) { rollbackErrors.push(rollbackError); }
    if (rollbackErrors.length) throw new AggregateError([error, ...rollbackErrors], 'Operator quarantine failed and rollback was incomplete');
    throw error;
  }
}
