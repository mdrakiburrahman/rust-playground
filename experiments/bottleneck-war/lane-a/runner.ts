import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { createHash, randomBytes } from 'node:crypto';
import { spawn, execFileSync, type ChildProcess } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { createServer } from 'node:net';
import { snapshot, difference, proc, procDifference, clientMetrics } from './measure.js';

type Cell = { id: string; grid: string; x: string; y: string; env: Record<string, string> };
type Plan = { duration: number; warmup: number; common: Record<string, string>; cells: Cell[] };
type Managed = { child: ChildProcess; done: Promise<number>; code: number | null; error?: string; log: string };
const repo = process.cwd();
const here = path.dirname(path.resolve(process.argv[1]));
const output = path.resolve(process.argv[3] ?? path.join(repo, 'out/bwa'));
const bin = path.resolve(process.env.BWA_BIN ?? path.join(repo, 'target/release'));
const hz = Number(execFileSync('getconf', ['CLK_TCK'], { encoding: 'utf8' }).trim());
const results = path.join(output, 'results.jsonl');
const children: Managed[] = [];

async function assertPortFree(port: number): Promise<void> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => server.close((error) => error ? reject(error) : resolve()));
  });
}

function environment(value: unknown): Record<string, string> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected env object');
  const result: Record<string, string> = {};
  for (const [key, val] of Object.entries(value)) {
    if (!/^(BWA_|ARROWCOL_COLLECTOR_|ARROWCOL_LAYOUT_|ARROWCOL_PARTITION_|GLIBC_TUNABLES$)/.test(key) ||
        ['BWA_ROOT', 'BWA_BIN'].includes(key) || typeof val !== 'string' || /[\r\n\0]/.test(val)) {
      throw new Error(`Invalid environment override: ${key}`);
    }
    result[key] = val;
  }
  return result;
}

function parsePlan(file: string): Plan {
  const data = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (!Number.isInteger(data.duration) || data.duration < 5 || data.duration > 1800 ||
      !Number.isInteger(data.warmup) || data.warmup < 0 || data.warmup > 300 || !Array.isArray(data.cells)) {
    throw new Error('Plan requires duration 5..1800, warmup 0..300, cells array');
  }
  const ids = new Set<string>();
  const cells: Cell[] = data.cells.map((cell: Cell) => {
    if (!cell || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}$/.test(cell.id) ||
        ids.has(cell.id) || !['grid', 'x', 'y'].every((key) => typeof cell[key as keyof Cell] === 'string')) {
      throw new Error('Invalid or duplicate cell identity');
    }
    ids.add(cell.id);
    return { ...cell, env: environment(cell.env) };
  });
  return { duration: data.duration, warmup: data.warmup, common: environment(data.common), cells };
}

function start(binary: string, args: string[], env: NodeJS.ProcessEnv, log: string, cpus?: string): Managed {
  const fd = fs.openSync(log, 'w');
  const child = spawn(cpus ? 'taskset' : binary, cpus ? ['-c', cpus, binary, ...args] : args,
    { env, stdio: ['ignore', fd, fd] });
  fs.closeSync(fd);
  const managed: Managed = { child, done: Promise.resolve(-1), code: null, log };
  managed.done = new Promise((resolve) => {
    child.once('error', (error) => { managed.error = error.message; });
    child.once('close', (code) => { managed.code = code ?? -1; resolve(managed.code); });
  });
  children.push(managed);
  return managed;
}

function alive(child: Managed): void {
  if (!child.child.pid || child.code !== null || child.error) {
    throw new Error(`Component exited (${child.code}): ${child.error ?? fs.readFileSync(child.log, 'utf8').slice(-700)}`);
  }
}

async function stop(child: Managed): Promise<void> {
  if (child.code !== null) return;
  child.child.kill('SIGTERM');
  for (let i = 0; i < 120 && child.code === null; i++) await sleep(250);
  if (child.code === null) child.child.kill('SIGKILL');
  await child.done;
}

async function waitReady(child: Managed, probe: () => Promise<boolean>): Promise<void> {
  for (let i = 0; i < 240; i++) {
    alive(child);
    if (await probe()) return;
    await sleep(250);
  }
  throw new Error(`Readiness timeout: ${child.log}`);
}

async function http(url: string): Promise<string> {
  const response = await fetch(url, { signal: AbortSignal.timeout(2000) });
  if (!response.ok) throw new Error(`HTTP ${response.status}: ${url}`);
  return response.text();
}

async function sleepUntil(ms: number): Promise<void> {
  if (ms > Date.now()) await sleep(ms - Date.now());
}

function positive(env: NodeJS.ProcessEnv, name: string, fallback: number, max = 256): number {
  const value = Number(env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0 || value > max) throw new Error(`Invalid ${name}`);
  return value;
}

async function runCell(cell: Cell, plan: Plan, identity: object): Promise<boolean> {
  const run = fs.mkdtempSync(path.join(output, `${cell.id}-`));
  const root = path.join(run, 'delta');
  fs.mkdirSync(root);
  const key = path.join(run, 'hmac');
  fs.writeFileSync(key, randomBytes(32).toString('hex'), { mode: 0o600 });
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    ARROWCOL_PARTITION_GRANULARITY: 'hour',
    ARROWCOL_LAYOUT_ROW_GROUP_ROWS: '262144',
    ARROWCOL_LAYOUT_MAX_INPUT_BYTES: '1073741824',
    ARROWCOL_LAYOUT_MAX_ROWS: '2000000',
    ...plan.common, ...cell.env, BWA_ROOT: root,
    ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE: key,
  };
  const knobs = { ...plan.common, ...cell.env };
  const isolation = env.BWA_ISOLATION ?? 'l5';
  const diagnostic = isolation !== 'l5';
  if (env.BWA_ISOLATION) {
    env.ARROWCOL_COLLECTOR_ISOLATION_LEVEL = isolation;
    env.ARROWCOL_CLIENT_ISOLATION_LEVEL = isolation;
  }
  const metadata = { cell: cell.id, grid: cell.grid, x: cell.x, y: cell.y,
    duration: plan.duration, warmup: plan.warmup, knobs, run, ...identity,
    allocator_env: { GLIBC_TUNABLES: env.GLIBC_TUNABLES ?? null, LD_PRELOAD: env.LD_PRELOAD ?? null },
    storage: 'local_filesystem', isolation_level: isolation, kustainer_rows: null,
    kustainer_comment: 'Not started: collector-only local filesystem experiment, no Kustainer claim',
  };
  let result: object;
  let valid = false;
  const offset = children.length;
  const observationErrors: { endpoint: string; message: string; at: number }[] = [];
  async function observe(endpoint: string): Promise<string | null> {
    try { return await http(`http://127.0.0.1:32011/${endpoint}`); }
    catch (error) {
      observationErrors.push({ endpoint, message: error instanceof Error ? error.message : String(error), at: Date.now() });
      return null;
    }
  }
  try {
    if (!['l0', 'l1', 'l5'].includes(isolation)) throw new Error('BWA_ISOLATION must be l0, l1 or l5');
    for (const port of [8817, 32010, 32011]) await assertPortFree(port);
    const clients = positive(env, 'BWA_NCLIENTS', 1, 16);
    const cores = positive(env, 'BWA_CORES', 24, 128);
    const ccpus = env.BWA_CCPUS ?? '0-23', kcpus = env.BWA_KCPUS ?? '24-31';
    const catalog = start(path.join(bin, 'arrowcol-catalog-rust'),
      ['--listen', '127.0.0.1:8817', '--warehouse-root', root, '--state-dir', path.join(run, 'catalog-state'), '--ingestion-layout'],
      env, path.join(run, 'catalog.log'), kcpus);
    await waitReady(catalog, async () => {
      const p = spawn(path.join(bin, 'arrowcol-catalog-rust'), ['--listen', '127.0.0.1:8817', '--healthcheck'], { env, stdio: 'ignore' });
      return new Promise<boolean>((resolve, reject) => { p.once('error', reject); p.once('close', (code) => resolve(code === 0)); });
    });
    const collector = start(path.join(bin, 'arrowcol-dataflow-rust'),
      ['--config', path.join(here, 'base.yaml'), '--num-cores', String(cores)], env, path.join(run, 'collector.log'), ccpus);
    await waitReady(collector, async () => {
      try { await http('http://127.0.0.1:32011/readyz'); return true; } catch { return false; }
    });
    const table = env.BWA_TABLE ?? 'sqlserver_availability_replica_states';
    const isolationArgs = env.BWA_ISOLATION ? ['--isolation-level', isolation] : [];
    const ddl = start(path.join(bin, 'arrowcol-dmv'),
      ['--endpoint', 'http://127.0.0.1:32010', '--client-id', 'bwa-ddl', '--table', table, '--rows', '8', ...isolationArgs],
      env, path.join(run, 'ddl.log'), kcpus);
    if (await ddl.done !== 0) throw new Error(`DDL failed: ${fs.readFileSync(ddl.log, 'utf8').slice(-700)}`);
    const maintenance = env.BWA_CHECKPOINT_EVERY_COMMITS ? start(path.join(bin, 'delta-maintenance'), [
      '--catalog-endpoint', 'http://127.0.0.1:8817', '--operation', 'checkpoint', '--apply',
      '--interval-secs', '1', '--max-idle-backoff-secs', '1',
      '--checkpoint-every-commits', String(positive(env, 'BWA_CHECKPOINT_EVERY_COMMITS', 5, 100000)),
      '--no-metrics-listener',
    ], env, path.join(run, 'maintenance.log'), kcpus) : null;
    const startMs = Date.now() + 8000, ws = startMs + plan.warmup * 1000, we = ws + plan.duration * 1000;
    const load: Managed[] = [];
    for (let i = 0; i < clients; i++) {
      load.push(start(path.join(bin, 'arrowcol-dmv'), [
        '--endpoint', 'http://127.0.0.1:32010', '--client-id', `bwa-${i}`, '--table', table, '--rows', '8192',
        '--workers', String(positive(env, 'BWA_CLIENT_WORKERS', 8)),
        '--streams-per-worker', String(positive(env, 'BWA_CLIENT_SPW', 2)),
        '--runtime-threads', String(positive(env, 'BWA_CLIENT_THREADS', 8)),
        '--interval-us', '0', '--batch-rows', env.BWA_CLIENT_BATCH_ROWS ?? '65536',
        '--corpus-rows', env.BWA_CLIENT_CORPUS_ROWS ?? '65536',
        '--string-width', env.BWA_CLIENT_STRING_WIDTH ?? '0',
        '--max-flight-data-bytes', '67108864', '--max-message-bytes', '134217728',
        '--max-memory-bytes', '1073741824', '--pod-ordinal', String(i),
        '--warmup-secs', String(plan.warmup), '--duration-secs', String(plan.duration), '--run-start-unix-ms', String(startMs),
        ...isolationArgs,
      ], env, path.join(run, `client-${i}.log`), kcpus));
    }
    await sleepUntil(ws);
    alive(collector); alive(catalog); load.forEach(alive);
    if (maintenance) alive(maintenance);
    const before = snapshot(root), cpuBefore = proc(collector.child.pid!);
    const maintenanceBefore = maintenance ? proc(maintenance.child.pid!) : null;
    const metricsStart = await observe('metrics');
    if (metricsStart !== null) fs.writeFileSync(path.join(run, 'metrics-start.txt'), metricsStart);
    const pressure = [];
    for (let next = ws + 2500; next < we; next += 5000) {
      await sleepUntil(next);
      alive(collector); alive(catalog);
      if (maintenance) alive(maintenance);
      const sample = await observe('v1/pressure');
      if (sample !== null) pressure.push({ at: Date.now(), value: JSON.parse(sample) });
    }
    await sleepUntil(we);
    alive(collector); alive(catalog);
    if (maintenance) alive(maintenance);
    const after = snapshot(root), cpuAfter = proc(collector.child.pid!);
    const maintenanceAfter = maintenance ? proc(maintenance.child.pid!) : null;
    const metricsEnd = await observe('metrics');
    if (metricsEnd !== null) fs.writeFileSync(path.join(run, 'metrics-end.txt'), metricsEnd);
    fs.writeFileSync(path.join(run, 'pressure.json'), JSON.stringify(pressure));
    fs.writeFileSync(path.join(run, 'commit-boundaries.json'), JSON.stringify({
      start: [...before.commits], end: [...after.commits], requested: { start: ws, end: we },
      observed: { start: before.observedAt, end: after.observedAt },
    }));
    for (const client of load) {
      const deadline = Date.now() + 45000;
      while (client.code === null && Date.now() < deadline) await sleep(100);
      if (client.code === null) throw new Error('Client failed to finish within 45 seconds of window');
      if (client.code !== 0) throw new Error(`Client exited ${client.code}: ${fs.readFileSync(client.log, 'utf8').slice(-700)}`);
    }
    const stats = difference(before, after);
    const client = load.map((c) => clientMetrics(fs.readFileSync(c.log, 'utf8')));
    if (client.some((c) => c.window_start_ms !== ws || c.window_end_ms !== we)) {
      throw new Error('Client and collector measurement windows do not match');
    }
    if (diagnostic && (stats.rows !== 0 || client.some((c) =>
      !c.completion || !(c.completion.diagnostic_discard_streams > 0) ||
      c.completion.best_effort_streams !== 0 || c.completion.durable_streams !== 0))) {
      throw new Error('Diagnostic sink emitted commits or incorrect receipt accounting');
    }
    const admitted = client.reduce((sum, c) => sum + c.completed_rows, 0);
    const clientErrors = client.reduce((sum, c) => sum + c.error_logs + c.given_up_rows + c.ambiguous_rows, 0);
    const startup = fs.readFileSync(collector.log, 'utf8').split('\n')
      .filter((line) => line.startsWith('arrowcol_effective_config '))
      .map((line) => JSON.parse(line.slice('arrowcol_effective_config '.length)));
    const checkpointMissing = maintenance !== null && stats.checkpoints === 0;
    valid = clientErrors === 0 && !checkpointMissing;
    result = {
      ...metadata, status: clientErrors ? 'invalid_client_errors' : checkpointMissing ? 'checkpoint_not_exercised' : 'ok',
      committed_rps: !valid || diagnostic ? null : stats.committed_rps,
      diagnostic_rps: diagnostic && !clientErrors ? admitted / plan.duration : null,
      comment: clientErrors ? 'Client errors or ambiguous outcomes: see raw stats' :
        checkpointMissing ? 'Requested checkpoint service produced no checkpoint files; raw Delta rate is not a checkpoint treatment' :
        diagnostic ? 'Diagnostic discard path: not admission, durability or production throughput' :
          observationErrors.length ? 'Some admin telemetry unavailable; Delta boundary evidence and process counters retained' : null,
      admitted_rps: diagnostic ? null : admitted / plan.duration,
      first_attempt_offered_rps: client.every((c) => c.offered_rows !== null)
        ? client.reduce((sum, c) => sum + c.offered_rows!, 0) / plan.duration : null,
      retry_offered_rps: client.every((c) => c.retry_offered_rows !== null)
        ? client.reduce((sum, c) => sum + c.retry_offered_rows!, 0) / plan.duration : null,
      ipc_bytes_per_row: admitted ? client.reduce((sum, c) => sum + c.completed_ipc_bytes, 0) / admitted : null,
      bytes_per_row: stats.bytes_per_row, stats, client,
      effective_startup: startup,
      collector: procDifference(cpuBefore, cpuAfter, stats.seconds, hz),
      maintenance: maintenanceBefore && maintenanceAfter ? procDifference(maintenanceBefore, maintenanceAfter, stats.seconds, hz) : null,
      pressure: pressure.map((sample) => ({ at: sample.at, rich: sample.value.rich })),
      offered_capacity_rps: diagnostic && !clientErrors ? admitted / plan.duration : null,
      offered_capacity_comment: diagnostic ? 'Validated diagnostic sink receipt rate; same configured clients, no Delta work' :
        'Closed-loop ACK rate is not an independent offered-capacity measurement',
      boundary_scan_lag_ms: { start: before.observedAt - ws, end: after.observedAt - we },
      observation_errors: observationErrors,
    };
  } catch (error) {
    result = { ...metadata, status: 'failed', committed_rps: null,
      comment: error instanceof Error ? error.message : String(error) };
  } finally {
    for (const child of children.slice(offset).reverse()) await stop(child);
    fs.rmSync(key, { force: true });
    fs.writeFileSync(path.join(run, 'observation-errors.json'), JSON.stringify(observationErrors));
  }
  fs.appendFileSync(results, JSON.stringify(result) + '\n');
  fs.rmSync(root, { recursive: true });
  console.log(JSON.stringify(result, (key, value) => ['client', 'pressure', 'knobs', 'effective_startup'].includes(key) ? undefined : value));
  return valid;
}

async function main(): Promise<void> {
  const plan = parsePlan(process.argv[2]);
  fs.mkdirSync(output, { recursive: true });
  const lock = path.join(output, 'runner.lock');
  const fd = fs.openSync(lock, 'wx');
  fs.writeFileSync(fd, `${process.pid}\n`); fs.closeSync(fd);
  try {
    const identity = {
      sha: execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(),
      dirty: execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim(),
      host: os.hostname(),
      harness_sha256: createHash('sha256').update(fs.readFileSync(process.argv[1])).digest('hex'),
      config_sha256: createHash('sha256').update(fs.readFileSync(path.join(here, 'base.yaml'))).digest('hex'),
      binaries: Object.fromEntries(['arrowcol-dataflow-rust', 'arrowcol-dmv', 'arrowcol-catalog-rust'].map((name) =>
        [name, createHash('sha256').update(fs.readFileSync(path.join(bin, name))).digest('hex')])),
      maintenance_sha256: fs.existsSync(path.join(bin, 'delta-maintenance'))
        ? createHash('sha256').update(fs.readFileSync(path.join(bin, 'delta-maintenance'))).digest('hex') : null,
    };
    const done = new Set(fs.existsSync(results) ? fs.readFileSync(results, 'utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line).cell) : []);
    for (const cell of plan.cells) {
      if (done.has(cell.id)) continue;
      console.log(`CELL_START ${cell.id} ${new Date().toISOString()}`);
      if (!await runCell(cell, plan, identity)) process.exitCode = 1;
    }
  } finally {
    for (const child of children) await stop(child);
    fs.unlinkSync(lock);
  }
}

main().catch((error: unknown) => { console.error(error); process.exitCode = 1; });
