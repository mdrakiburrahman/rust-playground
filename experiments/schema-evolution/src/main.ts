import { spawn, type ChildProcess } from 'node:child_process';
import { createHash, randomBytes, randomUUID } from 'node:crypto';
import fs from 'node:fs';
import { hostname } from 'node:os';
import net from 'node:net';
import path from 'node:path';
import { createInterface } from 'node:readline';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Azure, Config, identity_principal, load_env, occupancy } from '../../../infra/azure/bootstrap.ts';
import { PublisherControl } from '../../../infra/azure/publisher_control.ts';
import { runExperiment, runProcess, type StageContext } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { deletePrefix } from '../../azure-qap/src/fixtures.ts';
import { secretsFrom } from '../../azure-qap/src/main.ts';
import { Kusto, object, ready, tables, type KustoClient } from '../../azure-qap/src/kusto.ts';
import { buildCatalogProof, buildClient } from './build-clients.ts';
import { nullableBar, physicalCounts, snapshot, type Snapshot } from './delta.ts';
import { exactRowsQuery, observeHydration, requestedNullQuery, schemaQuery, verifyExactRows } from './hydration.ts';

const WORKSPACE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const IMAGE = 'mcr.microsoft.com/azuredataexplorer/kustainer-linux@sha256:51e2250da2faf292de903f871a073eb37d3f2641cf63381b183879d21ebb1013';
const TABLE = 'schema_evolution/public/foo';
const ROWS = 8;

interface Process {
  child: ChildProcess;
  lines: string[];
  closed: boolean;
  error?: Error;
  exit: Promise<void>;
}

function launch(binary: string, args: string[], env: NodeJS.ProcessEnv, log: string,
  ctx: StageContext, role: 'catalog' | 'dfe' | 'v1' | 'v2'): Process {
  const child = spawn(binary, args, { cwd: WORKSPACE, env, stdio: ['pipe', 'pipe', 'pipe'] });
  if (!child.pid) throw new Error(`Owned ${role} process did not acquire a PID.`);
  const identity = { role, pid: child.pid, pidNamespace: fs.readlinkSync('/proc/self/ns/pid'),
    bootId: fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim() };
  ctx.record('owned-runtime-start', identity);
  const output = fs.createWriteStream(log);
  child.stdout!.pipe(output, { end: false });
  child.stderr!.pipe(output, { end: false });
  const process: Process = { child, lines: [], closed: false, exit: Promise.resolve() };
  createInterface({ input: child.stdout! }).on('line', line => process.lines.push(line));
  child.on('error', error => { process.error = error; });
  process.exit = new Promise(resolve => child.on('close', () => {
    process.closed = true;
    ctx.record('owned-runtime-close', { ...identity, exitCode: child.exitCode, signalCode: child.signalCode });
    output.end();
    resolve();
  }));
  return process;
}

async function port(): Promise<number> {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Cannot reserve a loopback endpoint.');
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

async function command(binary: string, args: string[], ctx: Pick<StageContext, 'signal'>,
  env: NodeJS.ProcessEnv = process.env): Promise<string> {
  const result = await runProcess(binary, args, { cwd: WORKSPACE, signal: ctx.signal, env });
  if (result.exitCode !== 0) throw new Error(`${path.basename(binary)} failed: ${result.stderr || result.stdout}`);
  return result.stdout.trim();
}

async function until(ctx: StageContext, label: string, check: () => Promise<boolean>): Promise<void> {
  const started = Date.now();
  while (Date.now() < ctx.deadline) {
    ctx.signal.throwIfAborted();
    const complete = await check();
    ctx.record(label, { elapsedMs: Date.now() - started, complete });
    if (complete && Date.now() < ctx.deadline) return;
    await sleep(Math.min(1000, Math.max(1, ctx.deadline - Date.now())), undefined, { signal: ctx.signal });
  }
  throw new Error(`${label} deadline exceeded.`);
}

async function event(process: Process, ctx: StageContext): Promise<Record<string, unknown>> {
  let value: Record<string, unknown> | undefined;
  await until(ctx, 'client-event', async () => {
    if (process.error) throw process.error;
    if (process.lines.length) { value = object(JSON.parse(process.lines.shift()!)); return true; }
    if (process.closed) throw new Error(`Client exited before its receipt: ${process.child.exitCode ?? process.child.signalCode}`);
    return false;
  });
  return value!;
}

async function stop(process: Process | undefined, signal: AbortSignal, graceful: NodeJS.Signals): Promise<void> {
  if (!process || process.closed) return;
  process.child.kill(graceful);
  const timeout = new AbortController();
  try {
    await Promise.race([
      process.exit,
      sleep(30_000, undefined, { signal: AbortSignal.any([signal, timeout.signal]) })
        .then(() => { throw new Error('Owned process did not shut down within 30 seconds.'); }),
    ]);
  } finally { timeout.abort(); }
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const { values: args } = parseArgs({ args: argv, options: {
    target: { type: 'string', default: 'local' }, mode: { type: 'string' }, 'env-file': { type: 'string' },
    'ready-env': { type: 'string' }, out: { type: 'string' },
  } });
  if (!['local', 'azure'].includes(args.target!)) throw new Error('--target must be local or azure.');
  if (args.target === 'azure' && (!args['env-file'] || !args['ready-env']))
    throw new Error('Azure requires explicit separate --env-file and --ready-env; shared .env.azure is never inferred.');
  if (args.target === 'azure' ? args.mode !== 'schema-evolution' : args.mode !== undefined)
    throw new Error('Azure requires explicit --mode schema-evolution; local does not accept an Azure mode.');
  const runId = `evo${Date.now()}_${randomBytes(4).toString('hex')}`, owner = randomUUID();
  const out = path.resolve(WORKSPACE, args.out ?? `.temp/experiments/schema-evolution/${runId}`);
  const confined = path.relative(path.join(WORKSPACE, '.temp/experiments/schema-evolution'), out);
  if (confined.startsWith('..') || path.isAbsolute(confined))
    throw new Error('Evidence output must be inside .temp/experiments/schema-evolution.');
  fs.mkdirSync(out, { recursive: true });
  const local = args.target === 'local';
  const baseValues = local ? {} : load_env(path.resolve(WORKSPACE, args['env-file']!));
  const baseConfig = local ? undefined : new Config(baseValues, 'schema-evolution');
  const values = local ? {} : { ...baseValues, ...load_env(path.resolve(WORKSPACE, args['ready-env']!)) };
  const config = local ? undefined : new Config(values, 'schema-evolution');
  if (config && config.sandbox !== baseConfig!.sandbox)
    throw new Error('Ready environment changed the explicitly selected isolated sandbox identity.');
  if (config && (config.group !== 'rust-playground-schema-evo' || !values.ARROWCOL_RESET_GENERATION))
    throw new Error('Azure requires the separately named schema-evo stamp and its genuinely exported ready generation.');
  const azure = config ? new Azure(config) : undefined;
  const root = local ? path.join(out, 'warehouse') :
    `abfss://${config!.experimentContainer}@${config!.account}.dfs.core.windows.net/runs/${runId}`;
  const mirror = local ? path.join(root, TABLE) : path.join(out, 'delta');
  const table = local ? 'foo' : `foo_${runId}`;
  const [catalogPort, flightPort, adminPort] = await Promise.all([port(), port(), port()]);
  if (new Set([catalogPort, flightPort, adminPort]).size !== 3) throw new Error('Loopback port allocation collided.');
  const endpoint = `http://127.0.0.1:${catalogPort}`, flight = `http://127.0.0.1:${flightPort}`;
  const env: NodeJS.ProcessEnv = { ...process.env };
  for (const key of Object.keys(env)) if (/^(AZURE_|ARROWCOL_)/.test(key)) delete env[key];
  Object.assign(env, values, config?.generated(), { ARROWCOL_DELTA_ROOT: root, ARROWCOL_CATALOG_ENDPOINT: endpoint,
    ARROWCOL_FLIGHT_LISTEN: `127.0.0.1:${flightPort}`, ARROWCOL_ADMIN_LISTEN: `127.0.0.1:${adminPort}`,
    ARROWCOL_INSTANCE_KEY: runId });
  fs.mkdirSync(local ? root : mirror, { recursive: true });
  let catalog: Process | undefined, dfe: Process | undefined, old: Process | undefined, newClient: Process | undefined;
  let oldReceipt: Record<string, unknown> | undefined, v1Hash = '', oldHash = '';
  let proof = '', source = '', principal = '', kusto: Kusto | undefined;
  let baseline: Record<string, unknown> | undefined, occupied = false, externalCreated = false, containerStarted = false;
  let requestSignal = new AbortController().signal;
  let warningContext: Pick<StageContext, 'record'> | undefined;
  const recordWarning = (warning: Record<string, unknown>) => {
    if (!warningContext) throw new Error('Kusto warning has no active evidence context.');
    warningContext.record('kusto-warning', warning);
  };
  const container = `w6-${runId}`;
  const sdk = (version: number) => path.join(out, `sdk-v${version}`);
  const binary = (name: string) => path.join(WORKSPACE, 'target/debug', name);
  const digest = (filename: string) => createHash('sha256').update(fs.readFileSync(filename)).digest('hex');
  const generate = async (version: number, ctx: StageContext) => {
    await command(binary('arrowcol-sdk-gen'), ['generate', '--catalog-endpoint', endpoint,
      '--ddl', `experiments/schema-evolution/fixtures/${version === 1 ? 'create' : 'alter'}.sql`,
      '--table', 'schema_evolution.public.foo', '--out', path.relative(WORKSPACE, sdk(version)),
      '--export-artifacts', path.relative(WORKSPACE, path.join(out, `catalog-v${version}`))], ctx, env);
  };
  const fetchSnapshot = async (ctx: StageContext): Promise<Snapshot> => {
    if (azure) {
      const prefix = `runs/${runId}/${TABLE}/`;
      fs.mkdirSync(path.join(mirror, '_delta_log'), { recursive: true });
      for (let version = 0; ; version++) {
        ctx.signal.throwIfAborted();
        if (version >= 100) throw new Error('Fixture exceeded the bounded 100-commit oracle.');
        const name = `_delta_log/${String(version).padStart(20, '0')}.json`;
        const [data] = await azure.blob('GET', config!.experimentContainer, prefix + name, undefined, undefined, {}, true);
        if (!data) break;
        fs.writeFileSync(path.join(mirror, name), data);
      }
      const state = snapshot(mirror);
      for (const filename of state.files) {
        ctx.signal.throwIfAborted();
        const [data] = await azure.blob('GET', config!.experimentContainer,
          prefix + path.relative(mirror, filename).split(path.sep).join('/'));
        if (!data) throw new Error('Active Delta file is absent.');
        fs.mkdirSync(path.dirname(filename), { recursive: true });
        fs.writeFileSync(filename, data);
      }
    }
    return snapshot(mirror);
  };
  const lockPath = path.join(WORKSPACE, '.temp/experiments/schema-evolution/runner.lock');
  const lock = fs.openSync(lockPath, 'wx');
  fs.writeSync(lock, `${process.pid} ${runId}\n`);
  const interrupted = new AbortController();
  const interrupt = () => interrupted.abort(new Error('Schema evolution interrupted; owned cleanup is running.'));
  process.on('SIGINT', interrupt);
  process.on('SIGTERM', interrupt);
  try {
  const outcome = await runExperiment<{ claim: string; evidence: string }>({
    id: `schema-evolution-${args.target}`, title: `Real schema evolution to ${local ? 'Kustainer' : 'Azure Kusto'}`,
    columns: [{ header: 'Claim', value: row => row.claim }, { header: 'Evidence', value: row => row.evidence }],
    stages: [
      { name: 'runtime', timeoutMs: 5 * 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        warningContext = ctx;
        ctx.record('provenance', { sha: await command('git', ['rev-parse', 'HEAD'], ctx),
          harnessHash: digest(path.join(WORKSPACE, 'experiments/schema-evolution/src/main.ts')),
          runbookHash: digest(path.join(WORKSPACE, 'experiments/schema-evolution/README.md')),
          target: args.target, group: config?.group ?? null, stamp: config?.sandbox ?? null, runId });
        if (azure) {
          await occupancy(azure, 'occupy', owner, undefined, values.ARROWCOL_RESET_GENERATION);
          occupied = true;
          principal = await identity_principal(azure);
          kusto = new Kusto(azure, config!.experimentDatabase, runId, undefined, 3, recordWarning);
          source = `${root}/${TABLE};managed_identity=${principal}`;
        } else {
          await command('docker', ['run', '-d', '--name', container, '--memory', '6g', '-e', 'ACCEPT_EULA=Y',
            '--mount', `type=bind,source=${root},target=/delta,readonly`, IMAGE], ctx);
          containerStarted = true;
          if (fs.existsSync('/.dockerenv')) {
            const networks = object(JSON.parse(await command('docker',
              ['inspect', '--format', '{{json .NetworkSettings.Networks}}', hostname()], ctx)));
            if (!Object.hasOwn(networks, 'bridge')) await command('docker', ['network', 'connect', 'bridge', hostname()], ctx);
          }
          const ip = await command('docker', ['inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', container], ctx);
          if (!/^\d+\.\d+\.\d+\.\d+$/.test(ip)) throw new Error('Kustainer has no bridge IP.');
          const transport: KustoClient = { c: { uri: `http://${ip}:8080` }, request: async (method, url, _resource, body, headers) => {
            const response = await fetch(url, { method, headers, body: body ? body.toString() : undefined,
              signal: AbortSignal.any([requestSignal, AbortSignal.timeout(60_000)]) });
            const data = Buffer.from(await response.arrayBuffer());
            if (!response.ok) throw new Error(`Kustainer HTTP ${response.status}: ${data.toString().slice(0, 1000)}`);
            try { tables(JSON.parse(data.toString())); }
            catch (error) {
              fs.writeFileSync(path.join(out, 'kustainer-error-response.json'), data);
              throw error;
            }
            return [data, Object.fromEntries(response.headers)];
          } };
          kusto = new Kusto(transport, 'NetDefaultDB', runId, undefined, 3, recordWarning);
          source = `/delta/${TABLE}`;
          await until(ctx, 'kustainer-ready', async () => {
            try { await kusto!.management('.show databases', ctx.signal); return true; }
            catch (error) {
              ctx.signal.throwIfAborted();
              if (!(error instanceof TypeError && error.message === 'fetch failed') &&
                  !(error instanceof Error && /^Kustainer HTTP 503:/.test(error.message))) throw error;
              ctx.record('startup-error', { message: String(error) });
              return false;
            }
          });
        }
        catalog = launch(binary('arrowcol-catalog-rust'), ['--listen', `127.0.0.1:${catalogPort}`,
          '--warehouse-root', root, '--state-dir', path.join(out, 'catalog-state')], env, path.join(out, 'catalog.log'), ctx, 'catalog');
        await until(ctx, 'catalog-ready', async () => {
          if (catalog!.closed || catalog!.error) throw new Error('Catalog failed; inspect catalog.log.');
          const result = await runProcess(binary('arrowcol-catalog-rust'), ['--listen', `127.0.0.1:${catalogPort}`, '--healthcheck'],
            { cwd: WORKSPACE, env, signal: ctx.signal });
          return result.exitCode === 0;
        });
        return { status: 'passed', summary: 'Actual catalog Flight SQL and CES endpoint is healthy.' };
      } },
      { name: 'create-and-old-client', timeoutMs: 20 * 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        proof = await buildCatalogProof(ctx.signal);
        await generate(1, ctx);
        baseline = object(JSON.parse(await command(proof, [endpoint], ctx, env)));
        const metadata = object(baseline.snapshot);
        if (metadata.provider !== 'delta' || !Array.isArray(metadata.partitions) || !metadata.partitions.includes('tenant') ||
            ![metadata.arrowSchemaBytes, metadata.manifestBytes, metadata.protobufDescriptorBytes]
              .every(value => typeof value === 'number' && value > 0))
          throw new Error('Live catalog CREATE did not export real Arrow, SDK and protobuf metadata.');
        ctx.record('catalog-create', baseline);
        v1Hash = digest(path.join(sdk(1), 'sdk-manifest.json'));
        const oldBinary = await buildClient(sdk(1), 1, ctx.signal);
        oldHash = digest(oldBinary);
        ctx.record('immutable-v1', { sdkManifestSha256: v1Hash, binarySha256: oldHash });
        dfe = launch(binary('arrowcol-dataflow-rust'), ['--num-cores', '1', '--config', 'experiments/schema-evolution/config.yaml'],
          env, path.join(out, 'dfe.log'), ctx, 'dfe');
        await until(ctx, 'dfe-ready', async () => {
          if (dfe!.closed || dfe!.error) throw new Error('DFE failed; inspect dfe.log.');
          try {
            const response = await fetch(`http://127.0.0.1:${adminPort}/readyz`,
              { signal: AbortSignal.any([ctx.signal, AbortSignal.timeout(2000)]) });
            if (!response.ok && response.status !== 503) throw new Error(`DFE readiness HTTP ${response.status}`);
            return response.ok;
          } catch (error) {
            ctx.signal.throwIfAborted();
            if (!(error instanceof TypeError && error.message === 'fetch failed') &&
                !(error instanceof DOMException && error.name === 'TimeoutError')) throw error;
            ctx.record('dfe-startup', { message: String(error) });
            return false;
          }
        });
        old = launch(oldBinary, [flight, String(ROWS), 'write'], env, path.join(out, 'old.log'), ctx, 'v1');
        oldReceipt = await event(old, ctx);
        if (oldReceipt.event !== 'accepted' || oldReceipt.phase !== 'before' || oldReceipt.version !== 1 ||
            oldReceipt.pid !== old.child.pid || oldReceipt.rows !== ROWS)
          throw new Error('Old client did not actually write v1 before ALTER.');
        ctx.record('old-before', oldReceipt);
        await until({ ...ctx, deadline: Math.min(ctx.deadline, Date.now() + 120_000) }, 'old-before-published', async () => {
          const state = await fetchSnapshot(ctx);
          ctx.record('delta-before-observed', { version: state.version, rows: state.rows, activeFiles: state.files.length });
          if (state.rows > ROWS) throw new Error('Unexpected duplicate or foreign rows before ALTER.');
          return state.rows === ROWS;
        });
        if (nullableBar((await fetchSnapshot(ctx)).schema)) throw new Error('bar exists before ALTER.');
        return { status: 'passed', summary: 'Genuinely generated v1 client is still alive after publishing its first cohort.' };
      } },
      { name: 'enable-native-refresh-before-alter', timeoutMs: 15 * 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        warningContext = ctx;
        const create = `.create external table ['${table}'] kind=delta (h@'${source}')`;
        await kusto!.management(create, ctx.signal);
        externalCreated = true;
        const policy = `.alter external table ['${table}'] policy query_acceleration '${JSON.stringify({
          IsEnabled: true, Hot: '7.00:00:00', ...(principal ? { ManagedIdentity: principal } : {}),
        })}'`;
        await kusto!.management(policy, ctx.signal);
        const auto = `.alter external table ['${table}'] kind=delta (h@'${source}') with (AutoUpdateSchema=true)`;
        await kusto!.management(auto, ctx.signal);
        ctx.record('native-prerequisites', { create, policy, auto });
        const before = await fetchSnapshot(ctx);
        await until(ctx, 'initial-qap-ready', async () => {
          const sample = await kusto!.sample(table, ctx.signal);
          ctx.record('initial-qap', sample);
          return ready(sample, { version: before.version, rows: ROWS });
        });
        const columns = await kusto!.query(schemaQuery(table), ctx.signal);
        if (columns.some(row => row[0] === 'bar')) throw new Error('Kusto exposes bar before ALTER.');
        return { status: 'passed', summary: 'Inferred v1 external table, QAP, then native AutoUpdateSchema enabled before ALTER.' };
      } },
      { name: 'alter-and-continuing-clients', timeoutMs: 20 * 60_000, run: async ctx => {
        if (!old || old.closed) throw new Error('Old client died before ALTER.');
        await generate(2, ctx);
        const position = object(baseline!.position);
        const after = object(JSON.parse(await command(proof, [endpoint, String(position.epoch), String(position.eventId)], ctx, env)));
        ctx.record('catalog-alter', after);
        if (!Array.isArray(after.events) || !after.events.some(raw => {
          const event = object(raw);
          return event.type === 2 && event.version === 2;
        })) throw new Error('Committed ALTER did not emit a genuine version-2 CES event.');
        const current = await buildClient(sdk(2), 2, ctx.signal);
        ctx.record('distinct-v2', { sdkManifestSha256: digest(path.join(sdk(2), 'sdk-manifest.json')),
          binarySha256: digest(current) });
        if (digest(path.join(sdk(1), 'sdk-manifest.json')) !== v1Hash ||
            digest(path.join(WORKSPACE, '.temp/experiments/schema-evolution/clients/target/debug/schema-evolution-client-v1')) !== oldHash)
          throw new Error('Immutable v1 SDK or running binary was overwritten.');
        old.child.stdin!.end('after\n');
        const continued = await event(old, ctx);
        if (continued.event !== 'accepted' || continued.phase !== 'after' || continued.pid !== oldReceipt!.pid ||
            continued.version !== 1 || continued.fingerprint !== oldReceipt!.fingerprint || continued.rows !== ROWS)
          throw new Error('Old-client continuation was restarted or upgraded.');
        await until(ctx, 'old-client-completed', async () => old!.closed);
        if (old.child.exitCode !== 0) throw new Error('Old client failed after ALTER.');
        ctx.record('old-after-same-process', continued);
        newClient = launch(current, [flight, String(ROWS), 'write'], env, path.join(out, 'new.log'), ctx, 'v2');
        const writingClient = newClient;
        const newReceipt = await event(writingClient, ctx);
        await until(ctx, 'new-client-completed', async () => writingClient.closed);
        if (writingClient.child.exitCode !== 0 || newReceipt.pid !== writingClient.child.pid)
          throw new Error('New client did not close with its captured process identity.');
        ctx.record('new-after', newReceipt);
        if (newReceipt.event !== 'accepted' || newReceipt.client !== 'new' || newReceipt.version !== 2 || newReceipt.rows !== ROWS)
          throw new Error('New client did not actually write the independently generated v2 SDK.');
        const rejected = object(JSON.parse(await command(current, [flight, String(ROWS), 'wrong-schema'], ctx, env)));
        if (rejected.event !== 'rejected' || rejected.code !== 'InvalidArgument') throw new Error('DFE did not reject the wrong schema.');
        ctx.record('wrong-schema', rejected);
        return { status: 'passed', summary: 'CES v2, old PID/fingerprint unchanged, distinct v2 writer and server-side wrong-schema rejection.' };
      } },
      { name: 'delta-null-and-sentinel-proof', timeoutMs: 3 * 60_000, run: async ctx => {
        let state: Snapshot | undefined;
        await until(ctx, 'all-cohorts-published', async () => {
          state = await fetchSnapshot(ctx);
          ctx.record('delta-after-observed', { version: state.version, rows: state.rows, activeFiles: state.files.length });
          if (state.rows > ROWS * 3) throw new Error('Unexpected duplicate or foreign rows after ALTER.');
          return state.rows === ROWS * 3;
        });
        if (!nullableBar(state!.schema)) throw new Error('Current Delta schema has no nullable bar.');
        const result = JSON.parse(await command(
          path.join(path.dirname(proof), 'schema-evolution-inspect-parquet'), state!.files, ctx, env));
        physicalCounts(result, ROWS);
        ctx.record('physical-delta', { version: state!.version, schema: state!.schema, result });
        return { status: 'passed', summary: 'Active Delta files prove true old-row nulls and exact nonempty new-client sentinels.' };
      } },
      { name: 'native-kusto-schema-and-data-hydration', timeoutMs: 20 * 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        warningContext = ctx;
        const target = await fetchSnapshot(ctx);
        await until(ctx, 'evolved-qap-ready', async () => {
          const sample = await kusto!.sample(table, ctx.signal);
          ctx.record('evolved-qap', sample);
          return ready(sample, { version: target.version, rows: ROWS * 3 });
        });
        const counts = await observeHydration(kusto!, table, { deadline: ctx.deadline, signal: ctx.signal,
          rowsPerPhase: ROWS, record: observation => ctx.record('native-hydration', observation) });
        const exactQuery = exactRowsQuery(table), exactRows = await kusto!.query(exactQuery, ctx.signal);
        verifyExactRows(exactRows, ROWS);
        ctx.record('native-exact-rows', { csl: exactQuery, rows: exactRows });
        const requested = requestedNullQuery(table), requestedRows = await kusto!.query(requested, ctx.signal);
        ctx.record('requested-isnull-query', { csl: requested, rows: requestedRows,
          caveat: 'Kusto STRING nulls normalize to empty strings. Physical Delta nulls are proved separately.' });
        return { status: 'passed', summary: 'Native AutoUpdateSchema hydrated actual cohort data without post-ALTER reconciliation.',
          rows: [{ claim: 'Native hydration', evidence: JSON.stringify(counts) }] };
      } },
    ],
    cleanup: async ctx => {
      requestSignal = ctx.signal;
      warningContext = ctx;
      const errors: unknown[] = [];
      const attempt = async (label: string, work: () => Promise<unknown>) => {
        try { await work(); }
        catch (error) { errors.push(error); ctx.record('cleanup-failure', { label, message: String(error) }); }
      };
      await attempt('old-client', () => stop(old, ctx.signal, 'SIGTERM'));
      await attempt('new-client', () => stop(newClient, ctx.signal, 'SIGTERM'));
      await attempt('dfe', async () => {
        await stop(dfe, ctx.signal, 'SIGTERM');
        if (dfe && dfe.child.exitCode !== 0) throw new Error('DFE did not shut down cleanly; preserve Azure data.');
      });
      await attempt('catalog', () => stop(catalog, ctx.signal, 'SIGINT'));
      if (externalCreated) await attempt('external-table', () => kusto!.drop(table, ctx.signal));
      if (containerStarted) await attempt('kustainer', () => command('docker', ['rm', '--force', container], ctx));
      if (azure && !errors.length) await attempt('cloud-owner', async () => {
        const [metadata, registry] = await new PublisherControl(azure).read();
        ctx.record('cloud-drain-observed', {
          generationMatches: metadata.resetgeneration === values.ARROWCOL_RESET_GENERATION,
          resetState: metadata.resetstate,
          occupancy: metadata.occupancy,
          occupancyOwnerMatches: metadata.occupancyid === owner,
          publishers: Object.entries(registry).map(([id, entry]) => ({ id, state: entry.state })),
        });
        if (metadata.resetgeneration !== values.ARROWCOL_RESET_GENERATION || Object.keys(registry).length)
          throw new Error('Cloud publishers or changed authority remain; retain run data and occupancy.');
        await deletePrefix(azure, config!.experimentContainer, `runs/${runId}`);
        if (occupied) await occupancy(azure, 'release', owner, undefined, values.ARROWCOL_RESET_GENERATION);
      });
      if (errors.length) throw new AggregateError(errors, 'Schema evolution cleanup incomplete; inspect recorded failures.');
    },
  }, { outDir: out, timeoutMs: 75 * 60_000, cleanupTimeoutMs: 3 * 60_000,
    secrets: secretsFrom(values), signal: interrupted.signal });
  return outcome.report.status === 'passed' ? 0 : 1;
  } finally {
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
    fs.closeSync(lock);
    fs.unlinkSync(lockPath);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main();
