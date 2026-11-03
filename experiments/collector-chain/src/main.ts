import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import {
  runExperiment, runProcess, type StageContext, type StageResult,
} from '../../../tools/libs/experiment-kit/src/experiment.js';
import {
  HELP, MIDDLES, batching, clientCoverage, mapHostPath, parseOptions, pendingFlush,
  type Expectation, type Options, type Row,
} from './checks.js';
import { deltaTables, readLog, type TableLog } from './delta.js';
import { Kusto, kql, type Row as KustoRow } from './kusto.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const EXPERIMENT = path.resolve(HERE, '..');
const WORKSPACE = path.resolve(EXPERIMENT, '../..');
const CLIENT = path.join(WORKSPACE, 'target/release/collector-chain');
const OTAP_TABLES = ['logs', 'spans', 'univariate_metrics', 'resource_attrs'];
const started = Date.now();

function say(message: string): void {
  const elapsed = Math.round((Date.now() - started) / 1000);
  console.log(`[collector-chain +${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}] ${message}`);
}

async function run(command: string, args: string[], signal: AbortSignal, env?: NodeJS.ProcessEnv): Promise<string> {
  const result = await runProcess(command, args, { signal, cwd: EXPERIMENT, ...(env ? { env } : {}) });
  if (result.exitCode !== 0)
    throw new Error(`${command} ${args.join(' ')} exited ${result.exitCode ?? result.signal}:\n${result.stderr || result.stdout}`);
  return result.stdout;
}

function inContainer(): boolean {
  return fs.existsSync('/.dockerenv') || Boolean(process.env.REMOTE_CONTAINERS_IPC);
}

interface Run {
  readonly options: Options;
  readonly project: string;
  readonly delta: string;
  env?: NodeJS.ProcessEnv;
  network?: string;
  ips: Record<string, string>;
  kusto?: Kusto;
  clientIds: string[];
  dmvTables: number;
}

function compose(state: Run, args: string[], signal: AbortSignal): Promise<string> {
  return run('docker', ['compose', '--file', 'compose.yaml', '--project-name', state.project, ...args], signal, state.env);
}

async function ip(state: Run, service: string, signal: AbortSignal): Promise<string> {
  const container = (await compose(state, ['ps', '--quiet', service], signal)).trim();
  const address = (await run('docker', ['inspect', '--format',
    `{{(index .NetworkSettings.Networks "${state.project}_default").IPAddress}}`, container], signal)).trim();
  if (!/^\d+\.\d+\.\d+\.\d+$/.test(address)) throw new Error(`${service} has no address on ${state.project}_default`);
  return address;
}

function expectation(state: Run): Expectation {
  const { clients, rows, signals } = state.options;
  return { clients, rows, signals, dmvTables: state.dmvTables };
}

function logs(root: string): Map<string, TableLog> {
  return new Map(deltaTables(root).map(table => [table, readLog(path.join(root, table))]));
}

async function stack(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  if (process.getuid?.() === 0) throw new Error('Run as a non-root user so the Delta output stays readable');
  for (const chain of ['arrowcol', 'otap']) fs.mkdirSync(path.join(state.delta, chain), { recursive: true });
  const hostPath = async (local: string) => inContainer()
    ? mapHostPath(local, JSON.parse(await run('docker', ['inspect', '--format', '{{json .Mounts}}', hostname()], ctx.signal)))
    : local;
  state.env = {
    ...process.env,
    CHAIN_PROJECT: state.project,
    CHAIN_UID: String(process.getuid?.() ?? 1000),
    CHAIN_GID: String(process.getgid?.() ?? 1000),
    CHAIN_CONFIG_HOST_PATH: await hostPath(path.join(EXPERIMENT, 'config')),
    CHAIN_DELTA_HOST_PATH: await hostPath(state.delta),
  };
  say(`Starting ${state.project}: ${MIDDLES} middles and a gateway per chain, plus Kustainer.`);
  await compose(state, ['up', '--detach', '--wait', '--wait-timeout', '480'], ctx.signal);
  if (inContainer()) {
    state.network = `${state.project}_default`;
    await run('docker', ['network', 'connect', state.network, hostname()], ctx.signal);
  }
  for (const service of ['kusto', ...Array.from({ length: MIDDLES }, (_, i) => [`arrowcol-middle-${i + 1}`, `otapcol-middle-${i + 1}`]).flat()])
    state.ips[service] = await ip(state, service, ctx.signal);
  state.kusto = new Kusto(`http://${state.ips.kusto}:8080`);
  for (;;) {
    try { await state.kusto.command('.show databases', ctx.signal); break; }
    catch (error) { if (ctx.signal.aborted) throw error; await sleep(2000, undefined, { signal: ctx.signal }); }
  }
  return { status: 'passed', summary: `${2 * (MIDDLES + 1)} collectors and Kustainer are healthy` };
}

async function clients(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  const { clients, rows, signals } = state.options;
  say(`Running ${clients} clients concurrently; each sends ${rows} rows per DMV table and ${signals} logs, spans and counter adds.`);
  const results = await Promise.all(Array.from({ length: clients }, async (_, index) => {
    const middle = index % MIDDLES + 1;
    const output = await run(CLIENT, [
      '--arrow-endpoint', `http://${state.ips[`arrowcol-middle-${middle}`]}:32310`,
      '--otlp-endpoint', `http://${state.ips[`otapcol-middle-${middle}`]}:4318`,
      '--rows', String(rows), '--signals', String(signals),
    ], ctx.signal);
    const sent = JSON.parse(output.trim()) as { client_id: string; dmv_tables: number };
    ctx.record('client', { middle, ...sent });
    return { middle, ...sent };
  }));
  state.clientIds = results.map(result => result.client_id);
  state.dmvTables = Math.min(...results.map(result => result.dmv_tables));
  return {
    status: 'passed',
    summary: `${clients} clients sent DMV Arrow and OTLP to ${Math.min(clients, MIDDLES)} middles per chain`,
    rows: results.map(result => ({
      check: 'client sent', subject: result.client_id, expected: `middle-${result.middle} of each chain`,
      observed: `${result.dmv_tables} DMV tables x ${rows} rows; ${signals} logs, spans and counter adds`, ok: true,
    })),
  };
}

async function flush(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  say('Waiting for both gateways to batch and commit to Delta Lake.');
  for (;;) {
    const pending = pendingFlush(logs(path.join(state.delta, 'arrowcol')), logs(path.join(state.delta, 'otap')), expectation(state));
    if (pending.length === 0) return { status: 'passed', summary: 'every client row is committed to the gateway Delta tables' };
    if (Date.now() + 2000 >= ctx.deadline) return { status: 'failed', summary: `still pending: ${pending.slice(0, 5).join('; ')}` };
    await sleep(2000, undefined, { signal: ctx.signal });
  }
}

async function kusto(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  const kusto = state.kusto!;
  const execute = async (csl: string, command: boolean) => {
    ctx.record('kql', { csl });
    return command ? kusto.command(csl, ctx.signal) : kusto.query(csl, ctx.signal);
  };
  say('Creating Kustainer delta external tables over the gateway output and asserting every client ID.');
  const dmv = new Map<string, KustoRow[]>();
  for (const table of deltaTables(path.join(state.delta, 'arrowcol'))) {
    const name = `dmv_${path.basename(table)}`;
    await execute(kql.externalTable(name, `/delta/arrowcol/${table}`), true);
    dmv.set(name, await execute(kql.dmvClients(name), false));
  }
  for (const table of OTAP_TABLES) await execute(kql.externalTable(`otap_${table}`, `/delta/otap/${table}`), true);
  const otap = await execute(kql.otapClients('otap_'), false);
  const rows = clientCoverage(state.clientIds, dmv, otap, expectation(state));
  const missing = rows.filter(row => !row.ok).length;
  return {
    status: missing === 0 ? 'passed' : 'failed',
    summary: missing === 0 ? `all ${state.clientIds.length} client IDs found in every DMV table and OTAP signal`
      : `${missing} client checks failed`,
    rows,
  };
}

async function batched(state: Run): Promise<StageResult<Row>> {
  const { clients } = state.options;
  const rows = [
    ...batching('ArrowCol', logs(path.join(state.delta, 'arrowcol')), clients),
    ...batching('OTAP', new Map([...logs(path.join(state.delta, 'otap'))].filter(([table]) => OTAP_TABLES.includes(table))), clients),
  ];
  if (clients < 2) return { status: 'inconclusive', summary: 'batching across clients needs --clients 2 or more', rows };
  const unbatched = rows.filter(row => !row.ok).length;
  return {
    status: unbatched === 0 ? 'passed' : 'failed',
    summary: unbatched === 0 ? `every gateway table holds ${clients} clients in fewer than ${clients} Delta commits`
      : `${unbatched} gateway tables committed once per client or more`,
    rows,
  };
}

async function teardown(state: Run, signal: AbortSignal): Promise<void> {
  if (!state.env) return;
  if (state.options.keep) {
    say(`Left ${state.project} running (Kusto at http://${state.ips.kusto ?? '?'}:8080). Remove it with:\n` +
      `  ${state.network ? `docker network disconnect ${state.network} $(hostname) && ` : ''}` +
      `docker compose -p ${state.project} down -v`);
    return;
  }
  if (state.network) await run('docker', ['network', 'disconnect', '--force', state.network, hostname()], signal).catch(() => undefined);
  await compose(state, ['down', '--volumes', '--remove-orphans', '--timeout', '10'], signal);
}

export async function main(argv: readonly string[]): Promise<number> {
  const options = parseOptions(argv);
  if (options.help) { console.log(HELP); return 0; }
  const runId = `${new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).toLowerCase()}-${randomBytes(2).toString('hex')}`;
  const out = path.resolve(options.out ?? path.join(WORKSPACE, '.temp/experiments/collector-chain', runId));
  const state: Run = {
    options, project: `collector-chain-${runId}`, delta: path.join(out, 'delta'), ips: {}, clientIds: [], dmvTables: 0,
  };
  const interrupt = new AbortController();
  const stop = () => interrupt.abort(new Error('interrupted'));
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    const outcome = await runExperiment<Row>({
      id: 'collector-chain',
      title: 'Collector chaining: clients to middle collectors to one batching gateway per chain',
      columns: [
        { header: 'Check', value: row => row.check },
        { header: 'Subject', value: row => row.subject },
        { header: 'Expected', value: row => row.expected },
        { header: 'Observed', value: row => row.observed },
        { header: 'Result', value: row => (row.ok ? 'ok' : 'FAIL') },
      ],
      stages: [
        { name: 'stack', timeoutMs: 10 * 60_000, run: ctx => stack(state, ctx) },
        { name: 'clients', timeoutMs: 5 * 60_000, run: ctx => clients(state, ctx) },
        { name: 'flush', timeoutMs: 3 * 60_000, run: ctx => flush(state, ctx) },
        { name: 'kusto', timeoutMs: 5 * 60_000, run: ctx => kusto(state, ctx) },
        { name: 'batching', timeoutMs: 60_000, run: () => batched(state) },
      ],
      cleanup: ctx => teardown(state, ctx.signal),
    }, { outDir: out, timeoutMs: 30 * 60_000, signal: interrupt.signal, cleanupTimeoutMs: 2 * 60_000, output: markdown => console.log(markdown) });
    say(`Report and Delta output: ${path.relative(process.cwd(), out)}`);
    return outcome.report.status === 'passed' ? 0 : 1;
  } finally {
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    return 2;
  });