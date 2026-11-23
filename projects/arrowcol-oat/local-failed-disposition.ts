import { execFileSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { link, lstat, open, readFile, readdir, unlink } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

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
};

const environment: RecoveryEnvironment = {
  docker: args => execFileSync('docker', args, {
    encoding: 'utf8', timeout: 10_000,
    env: { PATH: process.env.PATH ?? '/usr/bin:/bin', HOME: '/nonexistent', DOCKER_HOST: 'unix:///var/run/docker.sock' },
  }),
  processStartTicks: startTicks,
  bootId: async () => (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(),
};

function projectName(base: unknown, cell: string, length: number, compactLength: number): string {
  if (typeof base !== 'string' || !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(base)) throw new Error('Invalid Compose project');
  return `${base.slice(0, length).replace(/[-_]+$/g, '')}-${cell.replace(/-/g, '').slice(0, compactLength).toLowerCase()}`;
}

async function inspect(
  receiptPath: string, markerPath: string, env: RecoveryEnvironment, preload?: PreloadProofPins,
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
  if (platform.kind !== 'local' || !Array.isArray(config.tables) || config.tables.length !== 1 ||
      !Array.isArray(config.replica_counts) || config.replica_counts.length !== 1 ||
      !Array.isArray(config.dfe_worker_concurrency) || config.dfe_worker_concurrency.length !== 1) {
    throw new Error('Local disposition requires exactly one configured cell');
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
  if (!isAbsolute(markerPath) || dirname(resolve(markerPath)) !== runRoot || resolve(stateRoot) !== join(runRoot, 'state')) {
    throw new Error('Recovery evidence is outside the owned run');
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
    const guard = object(evidence.overload_guard, 'overload evidence');
    const failedCell = row.status === 'failed' ||
      (row.status === 'pinning_violation' && row.pinning_ok === false && status.error.includes('pinning_violation'));
    if (!uuid.test(cell) || row.mode !== 'local' || !failedCell ||
        row.table !== config.tables[0] || guard.run_id !== cell) throw new Error('Failed cell identity mismatch');
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
  if (preload ? entries.length !== 0 : entries.some(name => uuid.test(name) || /^\.arrowcol-oat-owner-.*\.json$/.test(name))) {
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
    cell_id: cell, project, boot_id: bootId, receipt_sha256: sha256(receiptBytes),
    marker_path: markerPath, marker_sha256: sha256(markerBytes), result_path: resultPath,
    result_sha256: sha256(resultBytes), log_path: logPath, log_sha256: sha256(logBytes),
    state_root: stateRoot, owner_pid: marker.ownerPid, owner_start_ticks: marker.ownerStartTicks,
    server_pid: marker.serverPid, server_start_ticks: marker.serverStartTicks,
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

export async function assertLocalFailedDisposition(
  receiptPath: string, dispositionPath: string, env: RecoveryEnvironment = environment,
): Promise<void> {
  const saved = object(JSON.parse((await boundedFile(dispositionPath, 1024 * 1024)).toString('utf8')), 'local disposition');
  const markerPath = string(saved.marker_path, 'marker path');
  const { recorded_at_unix_ms: recordedAt, ...recordedProof } = saved;
  if (typeof recordedAt !== 'number' || !Number.isSafeInteger(recordedAt) || recordedAt <= 0 || recordedAt > Date.now()) {
    throw new Error('Local disposition timestamp is invalid');
  }
  const expected = await inspect(receiptPath, markerPath, env);
  if (JSON.stringify(recordedProof) !== JSON.stringify(expected)) throw new Error('Local failure disposition or evidence changed');
}

export async function recordLocalFailedDisposition(
  root: string, invocation: string, markerPath: string, env: RecoveryEnvironment = environment,
): Promise<void> {
  if (!uuid.test(invocation)) throw new Error('Invalid invocation ID');
  const receiptPath = resolve(root, `${invocation}.json`);
  const dispositionPath = receiptPath + localDispositionSuffix;
  const proof = await inspect(receiptPath, markerPath, env);
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
    const verified = await inspect(receiptPath, markerPath, env);
    if (JSON.stringify(verified) !== JSON.stringify(proof)) throw new Error('Recovery evidence changed while recording');
    await link(temporary, dispositionPath);
    const directory = await open(dirname(receiptPath), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally {
    if (created) await unlink(temporary);
  }
}
