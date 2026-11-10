import { chmod, mkdtemp, open, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { tableFromIPC } from 'apache-arrow';
import { readParquet } from 'parquet-wasm';
import { run_tree } from '../../infra/azure/bootstrap.js';
import type { OatRunStatus } from './launch.js';

export const MAX_RESULT_BYTES = 16 * 1024 * 1024;
export const RESULT_TIMEOUT_MS = 30_000;
export const CLEANUP_TIMEOUT_MS = 150_000;
const UUID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const SHA = /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/;
type RecordValue = Record<string, unknown>;

export interface NetworkObservation {
  provider: string;
  enforcing: boolean;
}

export interface Verdict {
  pass: boolean;
  reasons: string[];
  rows: RecordValue[];
  lines: string[];
}

export function record(value: unknown): value is RecordValue {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function integer(value: unknown): bigint | undefined {
  if (typeof value === 'bigint' && value >= 0n) return value;
  if (typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) return BigInt(value);
  return undefined;
}

function tunables(row: RecordValue): RecordValue | undefined {
  try {
    const parsed: unknown = JSON.parse(String(row.tunables_json));
    return record(parsed) ? parsed : undefined;
  } catch {
    return undefined;
  }
}

export function evaluateDataFlow(
  status: OatRunStatus,
  allRows: RecordValue[],
  expectedSha: string | undefined,
  mode: 'local' | 'aks',
  config: RecordValue,
): Verdict {
  const reasons: string[] = [];
  const rows = allRows.filter((row) => tunables(row)?.orchestration_run_id === status.id);
  if (status.state !== 'succeeded') reasons.push(`run state is ${status.state}: ${status.error ?? status.progress}`);
  if (status.error) reasons.push(`runner error: ${status.error}`);
  if (!Number.isSafeInteger(status.result_count) || status.result_count <= 0) reasons.push('runner published zero or invalid result rows');
  if (!rows.length) reasons.push('no Parquet rows match this orchestration run');
  if (rows.length !== status.result_count) reasons.push('matching Parquet result count differs from runner result_count');
  if (!expectedSha || !SHA.test(expectedSha)) reasons.push('expected server revision unavailable; cannot verify provenance');
  if (mode === 'aks' && (typeof config.qap_database !== 'string' || !config.qap_database.trim())) reasons.push('QAP database is missing from run configuration');
  const tables = Array.isArray(config.tables) && config.tables.every((table) => typeof table === 'string') ? config.tables : [];
  const replicas = Array.isArray(config.replica_counts) ? config.replica_counts : [];
  const concurrency = Array.isArray(config.dfe_worker_concurrency) ? config.dfe_worker_concurrency : [];
  if (!tables.length || !replicas.length || !concurrency.length) reasons.push('expected experiment matrix is missing');
  const expected = new Set(replicas.flatMap((replica) =>
    concurrency.flatMap((worker) => tables.map((table) => `${replica}:${worker}:${table}`))));
  const seen = new Set<string>();
  const cells = new Map<string, string>();
  const cellIds = new Map<string, string>();
  for (const row of rows) {
    const label = String(row.table);
    if (row.schema_version !== 2) reasons.push(`${label}: unsupported result schema`);
    if (row.mode !== mode) reasons.push(`${label}: result mode mismatch`);
    if (mode === 'aks' && row.stamp !== config.stamp) reasons.push(`${label}: result stamp mismatch`);
    if (typeof row.run_id !== 'string' || !UUID.test(row.run_id)) reasons.push(`${label}: invalid cell run_id`);
    if (row.git_sha !== expectedSha) reasons.push(`${label}: git_sha differs from expected server revision`);
    if (row.status !== 'succeeded') reasons.push(`${label}: result status is ${String(row.status)}`);
    if (row.pinning_ok !== true || typeof row.table_owner !== 'string' || !row.table_owner.trim()) reasons.push(`${label}: pinning mismatch or missing owner`);
    if ((integer(row.rows) ?? 0n) <= 0n) reasons.push(`${label}: zero or invalid measured rows`);
    if ((integer(row.duration_ms) ?? 0n) <= 0n) reasons.push(`${label}: zero or invalid duration`);
    if (typeof row.rows_per_second !== 'number' || !Number.isFinite(row.rows_per_second) || row.rows_per_second <= 0) reasons.push(`${label}: invalid rows per second`);
    const key = `${String(row.replicas)}:${String(row.dfe_worker_concurrency)}:${label}`;
    if (!expected.has(key) || seen.has(key)) reasons.push(`${label}: unexpected or duplicate matrix result`);
    seen.add(key);
    const cell = `${String(row.replicas)}:${String(row.dfe_worker_concurrency)}`;
    if (cells.has(cell) && cells.get(cell) !== row.run_id) reasons.push(`${label}: inconsistent matrix cell run_id`);
    cells.set(cell, String(row.run_id));
    if (cellIds.has(String(row.run_id)) && cellIds.get(String(row.run_id)) !== cell) reasons.push(`${label}: reused matrix cell run_id`);
    cellIds.set(String(row.run_id), cell);
    if (integer(row.tables) !== BigInt(tables.length)) reasons.push(`${label}: table count mismatch`);
    if (mode === 'aks' && (
      integer(row.qap_observation_complete) !== 1n ||
      integer(row.qap_observed_tables) !== BigInt(tables.length) ||
      row.qap_completion_percent !== 100 ||
      integer(row.qap_pending_artifacts) !== 0n ||
      integer(row.qap_unhealthy_catalogs) !== 0n
    )) reasons.push(`${label}: QAP observation incomplete or unhealthy`);
  }
  if (expected.size !== seen.size) reasons.push('Parquet does not cover the expected experiment matrix');
  const lines = [
    `run: ${status.id}; matching result rows: ${rows.length}`,
    `git_sha: ${expectedSha ?? 'unavailable'}`,
  ];
  if (mode === 'aks') lines.push(`QAP database: ${String(config.qap_database)}; observation only, NOT ADX row-count proof`);
  return { pass: !reasons.length, reasons: [...new Set(reasons)], rows, lines };
}

export function formatDataFlow(verdict: Verdict, network?: NetworkObservation, warnings: string[] = []): string {
  const lines = [`DATA FLOW: ${verdict.pass ? 'PASS' : 'FAIL'}`, ...verdict.lines];
  if (network) lines.push(network.enforcing
    ? `network: ${network.provider} (enforced)`
    : "network: NOT ENFORCED (accepted risk: another workload inside the cluster could reach the dataflow engine's passwordless port; no public ingress)");
  for (const warning of warnings) lines.push(`WARNING: ${warning}`);
  for (const reason of verdict.reasons) lines.push(`FAIL: ${reason}`);
  return lines.join('\n');
}

export function dataFlowExitCode(verdict: Verdict): number {
  return verdict.pass ? 0 : 1;
}

export function decodeResultRows(bytes: Uint8Array): RecordValue[] {
  if (!bytes.length || bytes.length > MAX_RESULT_BYTES) throw new Error('result Parquet is empty or exceeds the size cap');
  const table = tableFromIPC(readParquet(bytes).intoIPCStream());
  if (!table.numRows) throw new Error('result Parquet contains zero rows');
  return table.toArray().map((row) => Object.fromEntries(table.schema.fields.map((field) => [field.name, row[field.name]])));
}

async function boundedFile(path: string): Promise<Uint8Array> {
  const file = await open(path, 'r');
  try {
    const metadata = await file.stat();
    if (!metadata.isFile() || metadata.size <= 0 || metadata.size > MAX_RESULT_BYTES) throw new Error('result file is empty, not regular, or exceeds size cap');
    const bytes = Buffer.alloc(metadata.size + 1);
    let size = 0;
    while (size < bytes.length) {
      const read = await file.read(bytes, size, bytes.length - size, null);
      if (!read.bytesRead) break;
      size += read.bytesRead;
    }
    if (size > MAX_RESULT_BYTES || size !== metadata.size) throw new Error('result file changed size during read');
    return bytes.subarray(0, size);
  } finally {
    await file.close();
  }
}

export async function fetchResultBytes(url: URL, fetcher: typeof fetch = fetch): Promise<Uint8Array> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), RESULT_TIMEOUT_MS);
  try {
    const response = await fetcher(url, { signal: controller.signal, redirect: 'error', cache: 'no-store' });
    if (!response.ok || !response.body) throw new Error(`result mirror returned HTTP ${response.status}`);
    const length = response.headers.get('content-length');
    if (length && (!/^\d+$/.test(length) || Number(length) > MAX_RESULT_BYTES)) throw new Error('result mirror exceeds size cap');
    const reader = response.body.getReader();
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        size += next.value.length;
        if (size > MAX_RESULT_BYTES) throw new Error('result mirror exceeds size cap');
        chunks.push(next.value);
      }
    } finally {
      await reader.cancel();
    }
    if (!size) throw new Error('result mirror is empty');
    return Buffer.concat(chunks, size);
  } finally {
    clearTimeout(timer);
  }
}

async function command(runner: typeof run_tree, args: string[], timeout = 30): Promise<string> {
  const result = await runner(args, timeout);
  if (result.returncode) throw new Error(`data-flow evidence command failed: ${args.slice(0, 3).join(' ')}`);
  return result.stdout;
}

function stampAccount(config: RecordValue): string {
  if (typeof config.stamp !== 'string' || !/^\d{5}$/.test(config.stamp)) throw new Error('invalid result stamp');
  return `arrowcollector${config.stamp}`;
}

export async function readResultBytes(
  config: RecordValue,
  mode: 'local' | 'aks',
  runner: typeof run_tree = run_tree,
  fetcher: typeof fetch = fetch,
  log: (message: string) => void = console.log,
): Promise<Uint8Array> {
  if (mode === 'local') {
    const url = new URL(String(config.result_url));
    if (url.protocol !== 'file:') throw new Error('local data-flow verdict requires a file:// result_url');
    log('result source: local authoritative Parquet');
    return boundedFile(fileURLToPath(url));
  }
  const account = stampAccount(config);
  const authoritative = new URL(String(config.result_url || `abfss://experiments@${account}.dfs.core.windows.net/arrowcol-oat/results.parquet`));
  const mirror = new URL(String(config.website_url || `abfss://$web@${account}.dfs.core.windows.net/results.parquet`));
  if (authoritative.protocol !== 'abfss:' || authoritative.username !== 'experiments' ||
      mirror.protocol !== 'abfss:' || mirror.username !== '$web' ||
      [authoritative, mirror].some((url) => url.hostname !== `${account}.dfs.core.windows.net` || url.password || url.search || url.hash)) {
    throw new Error('result URLs do not match the selected stamp');
  }
  try {
    const endpoint = (await command(runner, ['az', 'storage', 'account', 'show', '-g', 'arrow-collector-rg',
      '-n', account, '--query', 'primaryEndpoints.web', '-o', 'tsv'])).trim();
    const url = new URL(endpoint);
    if (url.protocol !== 'https:' || !url.hostname.startsWith(`${account}.`) ||
        !url.hostname.endsWith('.web.core.windows.net') || url.username || url.password || url.search || url.hash) {
      throw new Error('invalid static website endpoint');
    }
    url.pathname = mirror.pathname;
    log('result source: public sanitized $web Parquet mirror');
    return await fetchResultBytes(url, fetcher);
  } catch (error) {
    log(`WARNING: sanitized mirror unavailable (${error instanceof Error ? error.message : String(error)}); using authenticated experiments Blob download`);
  }
  const directory = await mkdtemp(join(tmpdir(), 'oat-result-'));
  await chmod(directory, 0o700);
  const path = join(directory, 'results.parquet');
  try {
    const metadata: unknown = JSON.parse(await command(runner, ['az', 'storage', 'blob', 'show', '--account-name', account,
      '--container-name', 'experiments', '--name', decodeURIComponent(authoritative.pathname.slice(1)),
      '--auth-mode', 'login', '--query', '{size:properties.contentLength,etag:properties.etag}', '-o', 'json']));
    if (!record(metadata) || typeof metadata.size !== 'number' || !Number.isSafeInteger(metadata.size) ||
        metadata.size <= 0 || metadata.size > MAX_RESULT_BYTES || typeof metadata.etag !== 'string' || !metadata.etag) {
      throw new Error('authoritative result exceeds size cap, is empty, or lacks a version');
    }
    const file = await open(path, 'wx', 0o600);
    await file.close();
    await command(runner, ['az', 'storage', 'blob', 'download', '--account-name', account,
      '--container-name', 'experiments', '--name', decodeURIComponent(authoritative.pathname.slice(1)),
      '--file', path, '--auth-mode', 'login', '--if-match', metadata.etag,
      '--overwrite', 'true', '--no-progress', '-o', 'none'], RESULT_TIMEOUT_MS / 1000);
    return await boundedFile(path);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

export async function verifyRunCleanup(
  rows: RecordValue[], config: RecordValue, runner: typeof run_tree = run_tree,
  clock: () => number = Date.now, sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
): Promise<void> {
  stampAccount(config);
  const context = `arrow-collector-${String(config.stamp)}`;
  if (config.kubectl_context !== context) throw new Error('cleanup context differs from selected stamp');
  const prefix = String(config.namespace ?? '').slice(0, 30).replace(/-+$/, '');
  if (!/^[a-z0-9][a-z0-9-]*$/.test(prefix)) throw new Error('invalid run namespace prefix');
  const ids = [...new Set(rows.map((row) => String(row.run_id)))];
  if (!ids.length || ids.some((id) => !UUID.test(id))) throw new Error('cleanup has no verified cell run IDs');
  const deadline = clock() + CLEANUP_TIMEOUT_MS;
  const boundedCommand = (args: string[]) => {
    const remaining = deadline - clock();
    if (remaining <= 0) throw new Error('cleanup timed out: per-run namespace or federated credential remains');
    return command(runner, args, Math.min(30, remaining / 1000));
  };
  for (;;) {
    let leaked = false;
    for (const id of ids) {
      const output: unknown = JSON.parse(await boundedCommand(['kubectl', '--context', context,
        'get', 'namespaces', '-l', `arrowcol-oat/run-id=${id}`, '-o', 'json']));
      if (!record(output) || !Array.isArray(output.items)) throw new Error('invalid namespace cleanup response');
      leaked ||= output.items.length > 0;
    }
    const credentials: unknown = JSON.parse(await boundedCommand(['az', 'identity', 'federated-credential',
      'list', '-g', 'arrow-collector-rg', '--identity-name', 'arrow-collector', '-o', 'json']));
    if (!Array.isArray(credentials) || credentials.some((item) => !record(item) || typeof item.subject !== 'string' || typeof item.name !== 'string')) {
      throw new Error('invalid federated credential cleanup response');
    }
    leaked ||= credentials.some((item: RecordValue) => ids.some((id) =>
      item.name === `arrowcol-run-${id}` || String(item.subject).startsWith(`system:serviceaccount:${prefix}-${id.replaceAll('-', '')}:`)));
    if (!leaked) return;
    if (clock() >= deadline) throw new Error('cleanup timed out: per-run namespace or federated credential remains');
    await sleep(Math.min(5000, deadline - clock()));
  }
}

export async function collectDataFlow(
  status: OatRunStatus, config: RecordValue, mode: 'local' | 'aks', expectedSha: string | undefined,
  runner: typeof run_tree = run_tree,
): Promise<Verdict> {
  if (status.state !== 'succeeded') return evaluateDataFlow(status, [], expectedSha, mode, config);
  let verdict: Verdict;
  try {
    const bytes = await readResultBytes(config, mode, runner);
    verdict = evaluateDataFlow(status, decodeResultRows(bytes), expectedSha, mode, config);
  } catch (error) {
    const verdict = evaluateDataFlow(status, [], expectedSha, mode, config);
    verdict.reasons.push(error instanceof Error ? error.message : 'unreadable result evidence');
    return verdict;
  }
  if (mode === 'aks' && verdict.pass) {
    try {
      await verifyRunCleanup(verdict.rows, config, runner);
      verdict.lines.push('cleanup: zero run-scoped namespaces or federated credentials');
    } catch (error) {
      verdict.pass = false;
      verdict.reasons.push(error instanceof Error ? error.message : 'unreadable cleanup evidence');
    }
  }
  return verdict;
}

export async function reportDataFlowRun(
  execute: (onProgress: (status: OatRunStatus) => void) => Promise<OatRunStatus>,
  config: RecordValue, mode: 'local' | 'aks', expectedSha: string | undefined,
  runner: typeof run_tree = run_tree, network?: NetworkObservation, warnings: string[] = [],
  collect: typeof collectDataFlow = collectDataFlow,
): Promise<number> {
  let latest: OatRunStatus | undefined;
  let verdict: Verdict;
  try {
    const status = await execute((progress) => {
      latest = progress;
      console.log(`${progress.state}: ${progress.progress}`);
    });
    verdict = await collect(status, config, mode, expectedSha, runner);
  } catch (error) {
    verdict = {
      pass: false, rows: [], lines: latest ? [`run: ${latest.id}; state: ${latest.state}`] : [],
      reasons: [error instanceof Error ? error.message : String(error)],
    };
  }
  console.log(formatDataFlow(verdict, network, warnings));
  return dataFlowExitCode(verdict);
}
