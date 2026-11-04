import { spawn } from 'node:child_process';
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
import { PROJECT, clientPids, networkAddresses, parseContainers, portConflicts, type Container } from './stacks.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const EXPERIMENT = path.resolve(HERE, '..');
const WORKSPACE = path.resolve(EXPERIMENT, '../..');
const CLIENT = path.join(WORKSPACE, 'target/release/collector-chain');
const OTAP_TABLES = ['logs', 'spans', 'univariate_metrics', 'resource_attrs'];
/** kusto-explorer's ID for the `Collector chain` connection in `compose.keep.yaml`. */
const UI_CONNECTION = 'collector-chain';
const started = Date.now();

function say(message: string): void {
  const elapsed = Math.round((Date.now() - started) / 1000);
  console.log(`[collector-chain +${String(Math.floor(elapsed / 60)).padStart(2, '0')}:${String(elapsed % 60).padStart(2, '0')}] ${message}`);
}

async function run(command: string, args: string[], signal: AbortSignal, env?: NodeJS.ProcessEnv): Promise<string> {
  const result = await runProcess(command, args, { signal, cwd: EXPERIMENT, ...(env ? { env } : {}) });
  if (result.exitCode !== 0) {
    const detail = (result.stderr || result.stdout).trimEnd().split('\n').slice(-20).join('\n');
    throw new Error(`${command} ${args.join(' ')} exited ${result.exitCode ?? result.signal}:\n${detail}`);
  }
  return result.stdout;
}

function inContainer(): boolean {
  return fs.existsSync('/.dockerenv') || Boolean(process.env.REMOTE_CONTAINERS_IPC);
}

/** Runs `command` where every process of this container is visible, even when this harness runs in a PID namespace of its own. */
function fromRoot(command: string, args: string[], signal: AbortSignal): Promise<string> {
  return inContainer() ? run('docker', ['exec', hostname(), command, ...args], signal) : run(command, args, signal);
}

async function containers(signal: AbortSignal): Promise<Container[]> {
  return parseContainers(await run('docker', [
    'ps', '--all', '--format', '{{.Names}}\t{{.Label "com.docker.compose.project"}}\t{{.Ports}}',
  ], signal));
}

/** Stops the clients that send to `project`'s containers. A pid file only works in the PID namespace that wrote it, so match by address. */
async function stopClients(project: string, signal: AbortSignal): Promise<void> {
  const network = `${project}_default`;
  if ((await run('docker', ['network', 'ls', '--filter', `name=^${network}$`, '--format', '{{.Name}}'], signal)).trim() === '') return;
  const addresses = networkAddresses(await run('docker', [
    'network', 'inspect', network, '--format', '{{range .Containers}}{{.IPv4Address}}{{"\\n"}}{{end}}',
  ], signal));
  for (let attempt = 0; ; attempt++) {
    const pids = clientPids(await fromRoot('ps', ['-ww', '-eo', 'pid=,args='], signal), path.basename(CLIENT), addresses);
    if (pids.length === 0) return;
    if (attempt === 5) throw new Error(`Could not stop the clients of ${project}: pids ${pids.join(', ')}`);
    await fromRoot('kill', [attempt < 3 ? '-TERM' : '-KILL', ...pids.map(String)], signal).catch(() => undefined);
    await sleep(1000, undefined, { signal });
  }
}

async function release(project: string, signal: AbortSignal): Promise<void> {
  await stopClients(project, signal);
  if (inContainer()) await run('docker', ['network', 'disconnect', '--force', `${project}_default`, hostname()], signal).catch(() => undefined);
  await run('docker', ['compose', '--project-name', project, 'down', '--volumes', '--remove-orphans', '--timeout', '10'], signal);
}

/** `--keep` publishes fixed host ports, so an earlier run's stack has to go first. A container that is not ours is never touched. */
async function replaceEarlier({ kustoPort, uiPort }: Options, signal: AbortSignal): Promise<void> {
  const { earlier, foreign } = portConflicts(await containers(signal), [kustoPort, uiPort]);
  if (foreign.length > 0)
    throw new Error(`${foreign.map(({ name, port }) => `Host port ${port} is published by ${name}`).join('; ')}. ` +
      'Stop it, or pick other ports with --kusto-port and --ui-port.');
  for (const [project, ports] of earlier) {
    say(`Replacing ${project}, which publishes port ${ports.join(' and ')}.`);
    await release(project, signal);
  }
}

interface Run {
  readonly options: Options;
  readonly project: string;
  readonly out: string;
  readonly delta: string;
  readonly files: string[];
  readonly pids: string;
  env?: NodeJS.ProcessEnv;
  network?: string;
  /** Set once `stack` has passed: only then does `--keep` leave the stack behind. */
  up: boolean;
  ips: Record<string, string>;
  kusto?: Kusto;
  clientIds: string[];
  dmvTables: number;
  externalTables: string[];
}

function compose(state: Run, args: string[], signal: AbortSignal): Promise<string> {
  const files = state.files.flatMap(file => ['--file', file]);
  return run('docker', ['compose', ...files, '--project-name', state.project, ...args], signal, state.env);
}

async function hostPath(local: string, signal: AbortSignal): Promise<string> {
  return inContainer()
    ? mapHostPath(local, JSON.parse(await run('docker', ['inspect', '--format', '{{json .Mounts}}', hostname()], signal)))
    : local;
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
  if (state.options.keep) await replaceEarlier(state.options, ctx.signal);
  for (const chain of ['arrowcol', 'otap']) fs.mkdirSync(path.join(state.delta, chain), { recursive: true });
  state.env = {
    ...process.env,
    CHAIN_PROJECT: state.project,
    CHAIN_UID: String(process.getuid?.() ?? 1000),
    CHAIN_GID: String(process.getgid?.() ?? 1000),
    CHAIN_CONFIG_HOST_PATH: await hostPath(path.join(EXPERIMENT, 'config'), ctx.signal),
    CHAIN_DELTA_HOST_PATH: await hostPath(state.delta, ctx.signal),
    CHAIN_KUSTO_PORT: String(state.options.kustoPort),
    CHAIN_UI_PORT: String(state.options.uiPort),
  };
  if (state.options.keep) state.files.push(path.join(EXPERIMENT, 'compose.keep.yaml'));
  say(`Starting ${state.project}: ${MIDDLES} middles and a gateway per chain, plus Kustainer${state.options.keep ? ' and kusto-explorer' : ''}.`);
  await compose(state, ['up', '--detach', '--wait', '--wait-timeout', '480'], ctx.signal);
  if (inContainer()) {
    state.network = `${state.project}_default`;
    await run('docker', ['network', 'connect', state.network, hostname()], ctx.signal);
  }
  for (const service of [
    'kusto', ...(state.options.keep ? ['kusto-explorer'] : []),
    ...Array.from({ length: MIDDLES }, (_, i) => [`arrowcol-middle-${i + 1}`, `otapcol-middle-${i + 1}`]).flat(),
  ]) state.ips[service] = await ip(state, service, ctx.signal);
  state.kusto = new Kusto(`http://${state.ips.kusto}:8080`);
  for (;;) {
    try { await state.kusto.command('.show databases', ctx.signal); break; }
    catch (error) { if (ctx.signal.aborted) throw error; await sleep(2000, undefined, { signal: ctx.signal }); }
  }
  state.up = true;
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
  state.externalTables = [...dmv.keys(), ...OTAP_TABLES.map(table => `otap_${table}`)];
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

/** The browser UI that `--keep` leaves behind answers through Kusto and lists every external table the kusto stage created. */
async function explorer(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  const base = `http://${state.ips['kusto-explorer']}:8080`;
  say(`Checking kusto-explorer at ${base}: Kusto readiness and the external tables in its schema tree.`);
  const get = async (route: string) => {
    const response = await fetch(`${base}${route}`, { signal: ctx.signal });
    return { status: response.status, type: response.headers.get('content-type') ?? '', body: await response.text() };
  };
  let ready = await get('/readyz');
  while (ready.status !== 200 && Date.now() + 2000 < ctx.deadline) {
    await sleep(2000, undefined, { signal: ctx.signal });
    ready = await get('/readyz');
  }
  const page = await get('/');
  const schema = await get(`/api/connections/${UI_CONNECTION}/databases/NetDefaultDB/schema`);
  const listed = new Set(schema.status === 200
    ? (JSON.parse(schema.body) as { externalTables: { name: string }[] }).externalTables.map(table => table.name) : []);
  const missing = state.externalTables.filter(table => !listed.has(table));
  const rows: Row[] = [
    {
      check: 'kusto-explorer ready', subject: 'GET / and /readyz', expected: 'HTML page; 200 once Kusto answers',
      observed: `${page.status} ${page.type.split(';')[0]}; readyz ${ready.status}`,
      ok: page.status === 200 && page.type.startsWith('text/html') && ready.status === 200,
    },
    {
      check: 'kusto-explorer schema', subject: 'NetDefaultDB', expected: `${state.externalTables.length} external tables listed`,
      observed: schema.status !== 200 ? `HTTP ${schema.status}` : missing.length === 0 ? `${listed.size} listed` : `missing ${missing.join(', ')}`,
      ok: schema.status === 200 && missing.length === 0,
    },
  ];
  const failed = rows.filter(row => !row.ok).length;
  return {
    status: failed === 0 ? 'passed' : 'failed',
    summary: failed === 0 ? `kusto-explorer reaches Kusto and lists all ${state.externalTables.length} external tables`
      : `${failed} kusto-explorer checks failed`,
    rows,
  };
}

/** Restarts the verified clients, same IDs, as detached processes that keep sending after this process exits. */
async function stream(state: Run, ctx: StageContext): Promise<StageResult<Row>> {
  const { rows, signals, interval } = state.options;
  say(`Restarting the ${state.clientIds.length} clients with the same IDs; each sends a round every ${interval}s.`);
  const pids = state.clientIds.map((clientId, index) => {
    const middle = index % MIDDLES + 1;
    const log = fs.openSync(path.join(state.out, `client-${index + 1}.log`), 'a');
    try {
      const child = spawn(CLIENT, [
        '--arrow-endpoint', `http://${state.ips[`arrowcol-middle-${middle}`]}:32310`,
        '--otlp-endpoint', `http://${state.ips[`otapcol-middle-${middle}`]}:4318`,
        '--rows', String(rows), '--signals', String(signals), '--client-id', clientId, '--interval', String(interval),
      ], { detached: true, stdio: ['ignore', log, log] });
      child.unref();
      if (child.pid === undefined) throw new Error(`could not start client ${clientId}`);
      return child.pid;
    } finally {
      fs.closeSync(log);
    }
  });
  fs.writeFileSync(state.pids, `${pids.join('\n')}\n`);
  const next = { ...expectation(state), rows: 2 * rows, signals: 2 * signals };
  for (;;) {
    const pending = pendingFlush(logs(path.join(state.delta, 'arrowcol')), logs(path.join(state.delta, 'otap')), next);
    if (pending.length === 0)
      return { status: 'passed', summary: `every client sent a second round through both chains and keeps sending every ${interval}s` };
    if (Date.now() + 2000 >= ctx.deadline) return { status: 'failed', summary: `no second round yet: ${pending.slice(0, 5).join('; ')}` };
    await sleep(2000, undefined, { signal: ctx.signal });
  }
}

async function teardown(state: Run, signal: AbortSignal): Promise<void> {
  if (!state.env) return;
  if (state.options.keep && state.up) {
    const { interval, kustoPort, uiPort } = state.options;
    const streaming = fs.existsSync(state.pids);
    say([
      `Left ${state.project} running.`,
      `  Browser:   http://localhost:${uiPort} on the Docker host, http://${state.ips['kusto-explorer'] ?? '?'}:8080 from this devcontainer`,
      `  Kusto:     http://localhost:${kustoPort} on the Docker host, http://${state.ips.kusto ?? '?'}:8080 from this devcontainer`,
      '             database NetDefaultDB, external tables dmv_* and otap_*',
      streaming
        ? `  Clients:   ${state.clientIds.join(', ')} send a round every ${interval}s; logs in ${path.join(state.out, 'client-*.log')}`
        : `  Clients:   ${state.clientIds.join(', ')} sent one round; rerun with --stream to keep data flowing`,
      '  Tear down: npx nx run collector-chain-experiment:down',
    ].join('\n'));
    return;
  }
  if (state.network) await run('docker', ['network', 'disconnect', '--force', state.network, hostname()], signal).catch(() => undefined);
  await compose(state, ['down', '--volumes', '--remove-orphans', '--timeout', '10'], signal);
}

async function down(signal: AbortSignal): Promise<number> {
  const projects = new Set((await containers(signal)).map(({ project }) => project).filter(project => PROJECT.test(project)));
  if (projects.size === 0) say('No collector-chain stacks are running.');
  for (const project of projects) {
    say(`Removing ${project}: its clients, containers and network.`);
    await release(project, signal);
  }
  return 0;
}

export async function main(argv: readonly string[]): Promise<number> {
  const options = parseOptions(argv);
  if (options.help) { console.log(HELP); return 0; }
  const runId = `${new Date().toISOString().replace(/[-:]/g, '').slice(0, 15).toLowerCase()}-${randomBytes(2).toString('hex')}`;
  const out = path.resolve(options.out ?? path.join(WORKSPACE, '.temp/experiments/collector-chain', runId));
  const state: Run = {
    options, project: `collector-chain-${runId}`, out, delta: path.join(out, 'delta'), files: [path.join(EXPERIMENT, 'compose.yaml')],
    pids: path.join(out, 'clients.pid'), up: false, ips: {}, clientIds: [], dmvTables: 0, externalTables: [],
  };
  const interrupt = new AbortController();
  const stop = () => interrupt.abort(new Error('interrupted'));
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  try {
    if (options.down) return await down(interrupt.signal);
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
        ...(options.keep ? [{ name: 'explorer', timeoutMs: 2 * 60_000, run: (ctx: StageContext) => explorer(state, ctx) }] : []),
        ...(options.stream ? [{ name: 'stream', timeoutMs: 3 * 60_000 + options.interval * 1000, run: (ctx: StageContext) => stream(state, ctx) }] : []),
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
