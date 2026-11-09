import fs from 'node:fs';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { hostname } from 'node:os';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import { runExperiment, runProcess } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { Kusto, type KustoClient } from '../../azure-qap/src/kusto.ts';

const WORKSPACE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const IMAGE = 'mcr.microsoft.com/azuredataexplorer/kustainer-linux@sha256:51e2250da2faf292de903f871a073eb37d3f2641cf63381b183879d21ebb1013';

async function docker(args: string[], signal: AbortSignal): Promise<string> {
  const result = await runProcess('docker', args, { signal, cwd: WORKSPACE });
  if (result.exitCode !== 0) throw new Error(`docker ${args.join(' ')} failed: ${result.stderr || result.stdout}`);
  return result.stdout.trim();
}

export async function main(): Promise<number> {
  const runId = randomUUID();
  const name = `w6-kustainer-${runId}`;
  const out = path.join(WORKSPACE, '.temp/experiments/schema-evolution', runId);
  const delta = path.join(out, 'delta');
  fs.mkdirSync(path.join(delta, 'foo/_delta_log'), { recursive: true });
  const schema = { type: 'struct', fields: [
    { name: 'client', type: 'string', nullable: false, metadata: {} },
    { name: 'phase', type: 'string', nullable: false, metadata: {} },
  ] };
  // An empty Delta fixture probes command capability only, never end-to-end data delivery.
  fs.writeFileSync(path.join(delta, 'foo/_delta_log/00000000000000000000.json'), [
    JSON.stringify({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }),
    JSON.stringify({ metaData: { id: runId, format: { provider: 'parquet', options: {} },
      schemaString: JSON.stringify(schema), partitionColumns: [], configuration: {}, createdTime: Date.now() } }), '',
  ].join('\n'));
  let running = false;
  let kusto: Kusto | undefined;
  let requestSignal: AbortSignal | undefined;
  const outcome = await runExperiment<{ check: string; observed: string }>({
    id: 'schema-evolution-kustainer-capabilities',
    title: 'Kustainer capability probe (not E2E schema evolution)',
    columns: [{ header: 'Check', value: row => row.check }, { header: 'Observed', value: row => row.observed }],
    stages: [
      { name: 'start', timeoutMs: 4 * 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        await docker(['run', '-d', '--name', name, '--memory', '6g', '-e', 'ACCEPT_EULA=Y',
          '--mount', `type=bind,source=${delta},target=/delta,readonly`, IMAGE], ctx.signal);
        running = true;
        if (fs.existsSync('/.dockerenv')) {
          const networks = JSON.parse(await docker(['inspect', '--format', '{{json .NetworkSettings.Networks}}', hostname()], ctx.signal));
          if (!Object.hasOwn(networks, 'bridge')) await docker(['network', 'connect', 'bridge', hostname()], ctx.signal);
        }
        const ip = await docker(['inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}', name], ctx.signal);
        if (!/^\d+\.\d+\.\d+\.\d+$/.test(ip)) throw new Error('Kustainer has no bridge address.');
        const uri = `http://${ip}:8080`;
        const transport: KustoClient = { c: { uri }, request: async (method, url, _resource, body, headers) => {
          const response = await fetch(url, { method, headers,
            body: body === undefined || body === null ? undefined : body.toString(),
            signal: AbortSignal.any([requestSignal!, AbortSignal.timeout(60_000)]) });
          const data = Buffer.from(await response.arrayBuffer());
          if (!response.ok) throw new Error(`Kustainer HTTP ${response.status}: ${data.toString().slice(0, 1000)}`);
          return [data, Object.fromEntries(response.headers)];
        } };
        kusto = new Kusto(transport, 'NetDefaultDB', runId);
        for (;;) {
          try { await kusto.management('.show databases', ctx.signal); break; }
          catch (error) {
            ctx.record('readiness', { message: error instanceof Error ? error.message : String(error) });
            ctx.signal.throwIfAborted();
            await sleep(2000, undefined, { signal: ctx.signal });
          }
        }
        return { status: 'passed', summary: 'Pinned Kustainer responds to management requests.' };
      } },
      { name: 'string-null-semantics', timeoutMs: 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        if (!kusto) throw new Error('Kustainer did not start.');
        const csl = 'datatable(bar:string)[""] | project isNull=isnull(bar), isEmpty=isempty(bar)';
        const rows = await kusto.query(csl, ctx.signal);
        ctx.record('query', { csl, rows });
        if (rows.length !== 1 || rows[0]?.[0] !== false || rows[0]?.[1] !== true)
          throw new Error('Unexpected Kusto string null representation.');
        return { status: 'passed', summary: 'Kusto empty string is not null.',
          rows: [{ check: 'string null', observed: 'isnull=false; isempty=true' }] };
      } },
      { name: 'native-schema-refresh-capability', timeoutMs: 60_000, run: async ctx => {
        requestSignal = ctx.signal;
        if (!kusto) throw new Error('Kustainer did not start.');
        const csl = ".create external table foo kind=delta (h@'/delta/foo')";
        try {
          const created = await kusto.management(csl, ctx.signal);
          ctx.record('create', { csl, created });
          const policy = ".alter external table foo policy query_acceleration '{\"IsEnabled\":true,\"Hot\":\"7.00:00:00\"}'";
          const response = await kusto.management(policy, ctx.signal);
          ctx.record('qap', { csl: policy, response });
          const auto = ".alter external table foo kind=delta (h@'/delta/foo') with (AutoUpdateSchema=true)";
          const refreshed = await kusto.management(auto, ctx.signal);
          ctx.record('auto-update-schema', { csl: auto, response: refreshed });
          return { status: 'inconclusive', summary: 'Commands accepted; empty capability fixture does not prove native data hydration.' };
        } catch (error) {
          ctx.record('capability-error', { message: error instanceof Error ? error.message : String(error) });
          ctx.signal.throwIfAborted();
          return { status: 'finding', summary: `Native refresh prerequisites unavailable on pinned Kustainer: ${error instanceof Error ? error.message : String(error)}` };
        }
      } },
    ],
    cleanup: async ctx => {
      if (running) await docker(['rm', '--force', name], ctx.signal);
    },
  }, { outDir: out, timeoutMs: 7 * 60_000, cleanupTimeoutMs: 30_000 });
  return outcome.report.status === 'failed' ? 1 : 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main();
