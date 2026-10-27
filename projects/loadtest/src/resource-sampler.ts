import { execFile, spawn } from 'node:child_process';
import { appendFileSync, closeSync, existsSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { BENCHMARK_PROJECT } from './cli.js';

const execute = promisify(execFile);
export const SAMPLE_INTERVAL_MS = 10_000;
const TIMEOUT_MS = 2500;

export interface SamplerOptions {
  project: string;
  results: string;
  directAdmin: string;
  dataflowAdmin: string;
  ownerPid?: number;
}

function safeFailure(error: unknown): { kind: string; code?: string | number } {
  const value = error as NodeJS.ErrnoException;
  return {
    kind: value.name ?? 'Error',
    code: typeof value.code === 'number' || (typeof value.code === 'string' && /^[A-Z0-9_]+$/u.test(value.code))
      ? value.code : undefined,
  };
}

async function capture<T>(operation: () => Promise<T>): Promise<{ ok: true; value: T } | { ok: false; error: ReturnType<typeof safeFailure> }> {
  try {
    return { ok: true, value: await operation() };
  } catch (error) {
    return { ok: false, error: safeFailure(error) };
  }
}

async function docker(args: string[]): Promise<string> {
  const { stdout } = await execute('docker', args, { timeout: TIMEOUT_MS, maxBuffer: 4 * 1024 * 1024 });
  return stdout;
}

export function parseStatus(text: string): { pid: number; rssBytes: number; rssAnonBytes: number; threads: number } {
  const field = (name: string): number => {
    const match = new RegExp(`^${name}:\\s+(\\d+)(?:\\s+kB)?$`, 'mu').exec(text);
    if (!match) throw new Error(`Missing ${name}`);
    return Number(match[1]);
  };
  return { pid: field('Pid'), rssBytes: field('VmRSS') * 1024, rssAnonBytes: field('RssAnon') * 1024, threads: field('Threads') };
}

export function parseCounters(text: string): Record<string, number> {
  const entries = text.trim().split('\n').map((line) => /^([a-z_]+)\s+(\d+)$/u.exec(line));
  if (!entries.length || entries.some((entry) => !entry)) throw new Error('Invalid cgroup counters');
  return Object.fromEntries(entries.map((entry) => [entry![1], Number(entry![2])]));
}

export function parseMetrics(text: string): Record<string, number> {
  const result: Record<string, number> = {};
  for (const line of text.split('\n')) {
    const match = /^(arrowcol_[a-zA-Z0-9_]+)(?:\{[^}]*\})?\s+([-+0-9.eE]+)(?:\s+\d+)?$/u.exec(line);
    if (!match) continue;
    const value = Number(match[2]);
    if (Number.isFinite(value)) result[match[1]] = (result[match[1]] ?? 0) + value;
  }
  if (Object.keys(result).length === 0) throw new Error('No Arrowcol metrics');
  return result;
}

// Static commands only; never read container environment or print Docker error output.
export const resourceScript = [
  'pid=1',
  'case "$(cat /proc/1/comm)" in docker-init|tini) set -- $(cat /proc/1/task/1/children); ' +
    'if [ "$#" -ne 1 ]; then printf "__READ_ERROR__\\n"; exit 1; fi; pid="$1";; esac',
  'printf "\\n__status__\\n"; cat "/proc/$pid/status" 2>/dev/null || printf "__READ_ERROR__\\n"',
  ...[
  ['current', '/sys/fs/cgroup/memory.current'],
  ['stat', '/sys/fs/cgroup/memory.stat'],
  ['cpu', '/sys/fs/cgroup/cpu.stat'],
  ].map(([name, file]) =>
    `printf '\\n__${name}__\\n'; cat ${file} 2>/dev/null || printf '__READ_ERROR__\\n'`),
].join('; ');

export function parseResources(text: string): Record<string, unknown> {
  const sections = (name: string): string => {
    const match = new RegExp(`__${name}__\\n([\\s\\S]*?)(?=\\n__[a-z]+__\\n|$)`, 'u').exec(text);
    if (!match || match[1].includes('__READ_ERROR__')) throw new Error(`Unavailable ${name}`);
    return match[1].trim();
  };
  const parsed: Record<string, unknown> = {};
  const read = (name: string, operation: () => unknown): void => {
    try {
      parsed[name] = { ok: true, value: operation() };
    } catch (error) {
      parsed[name] = { ok: false, error: safeFailure(error) };
    }
  };
  read('process', () => parseStatus(sections('status')));
  read('memory', () => {
    const current = sections('current');
    if (!/^\d+$/u.test(current)) throw new Error('Invalid memory.current');
    const stat = parseCounters(sections('stat'));
    if (stat.inactive_file === undefined) throw new Error('Missing inactive_file');
    const currentBytes = Number(current);
    return { currentBytes, workingSetBytes: Math.max(0, currentBytes - stat.inactive_file), stat };
  });
  read('cpu', () => parseCounters(sections('cpu')));
  return parsed;
}

async function adminSample(endpoint: string): Promise<Record<string, unknown>> {
  const get = async (suffix: string): Promise<Response> =>
    fetch(`${endpoint}/${suffix}`, { signal: AbortSignal.timeout(TIMEOUT_MS), redirect: 'error' });
  const [metrics, readiness] = await Promise.all([
    capture(async () => {
      const response = await get('metrics');
      if (!response.ok) throw Object.assign(new Error('Metrics HTTP failure'), { code: `HTTP_${response.status}` });
      return parseMetrics(await response.text());
    }),
    capture(async () => {
      const response = await get('readyz');
      await response.body?.cancel();
      return { status: response.status, ready: response.status === 200 };
    }),
  ]);
  return { metrics, readiness };
}

async function collectorSample(project: string, service: string, endpoint: string): Promise<Record<string, unknown>> {
  const timestamp = new Date().toISOString();
  const identity = await capture(async () => {
    const ids = (await docker([
      'ps', '--all', '--quiet', '--filter', `label=com.docker.compose.project=${project}`,
      '--filter', `label=com.docker.compose.service=${service}`,
    ])).trim().split('\n').filter(Boolean);
    if (ids.length !== 1) throw Object.assign(new Error('Collector identity unavailable'), { code: 'CONTAINER_COUNT' });
    return ids[0];
  });
  if (!identity.ok) return { timestamp, service, identity, admin: await adminSample(endpoint) };
  const [container, resources, admin] = await Promise.all([
    capture(async () => JSON.parse(await docker([
      'inspect', '--format',
      '{"id":{{json .Id}},"imageId":{{json .Image}},"hostPid":{{json .State.Pid}},"running":{{json .State.Running}},"restartCount":{{json .RestartCount}}}',
      identity.value,
    ])) as unknown),
    capture(async () => parseResources(await docker(['exec', identity.value, 'sh', '-c', resourceScript]))),
    adminSample(endpoint),
  ]);
  return { timestamp, service, container, resources, admin };
}

export async function runResourceSampler(options: SamplerOptions): Promise<void> {
  const results = path.resolve(options.results);
  mkdirSync(results, { recursive: true });
  const statusFile = path.join(results, 'sampler-status.json');
  const samples = path.join(results, 'resource-samples.jsonl');
  const lockFile = path.join(results, 'sampler.lock');
  const lock = openSync(lockFile, 'wx', 0o600);
  try {
    writeFileSync(lock, `${JSON.stringify({ pid: process.pid, timestamp: new Date().toISOString() })}\n`);
  } finally {
    closeSync(lock);
  }
  const controller = new AbortController();
  const stop = (): void => controller.abort();
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  const ownerAlive = (): boolean => {
    if (!options.ownerPid) return true;
    try {
      process.kill(options.ownerPid, 0);
      return process.ppid === options.ownerPid;
    } catch {
      return false;
    }
  };
  let sequence = 0;
  const status = (state: string): void => {
    writeFileSync(`${statusFile}.next`, `${JSON.stringify({
      timestamp: new Date().toISOString(), pid: process.pid, state, sequence, intervalMs: SAMPLE_INTERVAL_MS,
    })}\n`);
    renameSync(`${statusFile}.next`, statusFile);
  };
  try {
    while (!controller.signal.aborted && ownerAlive()) {
      const timestamp = new Date().toISOString();
      const start = Date.now();
      const collectors = await Promise.all([
        collectorSample(options.project, 'direct-1', options.directAdmin),
        collectorSample(options.project, 'dataflow-1', options.dataflowAdmin),
      ]);
      appendFileSync(samples, `${JSON.stringify({ timestamp, sequence: sequence++, collectors })}\n`);
      status('running');
      await delay(Math.max(0, SAMPLE_INTERVAL_MS - (Date.now() - start)), undefined, { signal: controller.signal })
        .catch((error: unknown) => { if (!controller.signal.aborted) throw error; });
    }
    status('stopped');
  } catch (error) {
    appendFileSync(samples, `${JSON.stringify({ timestamp: new Date().toISOString(), samplerFailure: safeFailure(error) })}\n`);
    status('failed');
    throw error;
  } finally {
    process.removeListener('SIGTERM', stop);
    process.removeListener('SIGINT', stop);
    unlinkSync(lockFile);
  }
}

export async function startResourceSampler(options: SamplerOptions, env: NodeJS.ProcessEnv = process.env): Promise<() => Promise<void>> {
  const fd = openSync(path.join(options.results, 'sampler.log'), 'a', 0o600);
  const child = spawn(process.execPath, [
    ...process.execArgv, fileURLToPath(import.meta.url),
    '--project', options.project, '--results', options.results,
    '--direct-admin', options.directAdmin, '--dataflow-admin', options.dataflowAdmin,
    '--owner-pid', String(process.pid),
  ], { env, stdio: ['ignore', fd, fd] });
  closeSync(fd);
  let launchError: unknown;
  child.on('error', (error) => { launchError = error; });
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
  const stop = async (): Promise<void> => {
    if (child.exitCode === null && child.signalCode === null && !launchError) {
      child.kill('SIGTERM');
      const timeout = new AbortController();
      const timely = await Promise.race([
        exited.then(() => true),
        delay(10_000, undefined, { signal: timeout.signal }).then(() => false).catch(() => true),
      ]);
      timeout.abort();
      if (!timely) {
        child.kill('SIGKILL');
        await exited;
        throw new Error('Resource sampler required SIGKILL; inspect sampler status');
      }
    }
    const statusFile = path.join(options.results, 'sampler-status.json');
    if (launchError || !existsSync(statusFile) || JSON.parse(readFileSync(statusFile, 'utf8')).state !== 'stopped') {
      throw new Error('Resource sampler failed; inspect sampler-status.json and sampler.log');
    }
  };
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline && !launchError && child.exitCode === null) {
    const statusFile = path.join(options.results, 'sampler-status.json');
    if (existsSync(statusFile)) {
      const status = JSON.parse(readFileSync(statusFile, 'utf8'));
      if (status.state === 'running' && status.pid === child.pid) return stop;
    }
    await delay(100);
  }
  await stop();
  throw new Error('Resource sampler did not become responsive before load generation');
}

function parseSamplerOptions(args: string[]): SamplerOptions {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 2) {
    if (!['--project', '--results', '--direct-admin', '--dataflow-admin', '--owner-pid'].includes(args[index]) || !args[index + 1]) {
      throw new Error('Invalid sampler arguments');
    }
    values.set(args[index], args[index + 1]);
  }
  const project = values.get('--project') ?? '';
  if (project !== BENCHMARK_PROJECT) {
    throw new Error('Invalid sampler project');
  }
  const endpoint = (name: string): string => {
    const value = values.get(name) ?? '';
    const url = new URL(value);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/') {
      throw new Error('Invalid sampler admin endpoint');
    }
    return url.origin;
  };
  const results = values.get('--results');
  if (!results) throw new Error('Sampler requires --results');
  const owner = values.get('--owner-pid');
  if (owner !== undefined && (!/^[1-9]\d*$/u.test(owner) || !Number.isSafeInteger(Number(owner)))) {
    throw new Error('Invalid sampler owner PID');
  }
  return {
    project, results, directAdmin: endpoint('--direct-admin'), dataflowAdmin: endpoint('--dataflow-admin'),
    ownerPid: owner === undefined ? undefined : Number(owner),
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  Promise.resolve().then(() => runResourceSampler(parseSamplerOptions(process.argv.slice(2))))
    .catch((error: unknown) => {
      console.error(JSON.stringify({ samplerFailure: safeFailure(error) }));
      process.exitCode = 1;
    });
}
