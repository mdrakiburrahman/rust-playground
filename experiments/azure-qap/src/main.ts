import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { runExperiment, type Stage, type StageContext, type StageResult } from '../../../tools/libs/experiment-kit/src/experiment.js';
import type { Column } from '../../../tools/libs/experiment-kit/src/report.js';
import { Resources } from './resources.js';
import { loadProfile } from './profile.js';
import { Kusto, type Emit } from './kusto.js';
import { Scenarios, type ResultRow } from './scenarios.js';
import { Driver } from './driver.js';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

export const columns: readonly Column<ResultRow>[] = [
  { header: 'Scenario', value: row => row.scenario },
  { header: 'Files + / - / same', value: row => `${row.addedFiles} / ${row.removedFiles} / ${row.unchangedFiles}` },
  { header: 'Vacuum deleted', value: row => row.vacuumDeletedFiles, align: 'right' },
  { header: 'QAP catch-up', unit: 'ms', value: row => row.qapLagMs, align: 'right' },
  { header: 'Observed pending peak', value: row => row.peakPendingFiles, align: 'right' },
  { header: 'Ingest time moved', value: row => row.ingestionAdvanced },
  { header: 'Exact parity', value: row => row.correctness },
  { header: 'Query CPU', unit: 'ms', value: row => row.queryCpuMs, align: 'right' },
  { header: 'Whole-table reingestion', value: row => row.reingestion },
  { header: 'Background CPU', unit: 'ms', value: row => row.backgroundCpuMs },
];

function evidenceValue(value: unknown): unknown {
  const json = JSON.stringify(value);
  if (json === undefined) throw new Error('Evidence must have a JSON value.');
  const parsed: unknown = JSON.parse(json);
  return parsed;
}

export async function main(argv: string[]): Promise<number> {
  const args = parseArgs({ args: argv, allowPositionals: true, options: {
    env: { type: 'string', default: '.env' },
    config: { type: 'string', default: 'experiments/azure-qap/rapid.json' },
    out: { type: 'string' },
    manifest: { type: 'string' },
    'allow-immediate-vacuum': { type: 'boolean', default: false },
    execute: { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
  } });
  if (args.values.help) {
    console.log(`Azure QAP experiment (not a loadtest).
Run:     npx nx run azure-qap-experiment:run -- --config experiments/azure-qap/rapid.json --allow-immediate-vacuum
Preview: npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json
Cleanup: npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json --execute
Zero-retention VACUUM deliberately tests potentially broken QAP/old-snapshot readers.
Only experiment-owned resources are created/deleted. Existing sandbox data and cluster policies are not changed.
Runner exit codes: 0 passed; 1 failed; 2 inconclusive; 3 finding; 130 interrupted.
Nx normalizes nonzero runner exits to 1; read report.status and individual stages.`);
    return 0;
  }
  const command = args.positionals[0] ?? 'run';
  if (args.positionals.length > 1 || !['run', 'cleanup'].includes(command))
    throw new Error('Expected run or cleanup, not additional positional arguments.');
  const controller = new AbortController();
  const interrupt = () => controller.abort();
  process.once('SIGINT', interrupt);
  process.once('SIGTERM', interrupt);
  try {
    if (command === 'cleanup') {
      if (!args.values.manifest) throw new Error('Cleanup requires --manifest from the experiment report directory.');
      const resources = new Resources(path.resolve(args.values.env),
        path.resolve(args.values.manifest), true, controller.signal);
      const planned = await resources.cleanup(args.values.execute, controller.signal);
      console.log(planned.length
        ? `${args.values.execute ? 'Cleaned' : 'Would clean'} ${planned.length} manifest-owned resources.`
        : 'No experiment resources remain.');
      return 0;
    }
    if (args.values.execute || args.values.manifest)
      throw new Error('--execute and --manifest apply only to cleanup.');
    const config = loadProfile(path.resolve(args.values.config), args.values['allow-immediate-vacuum']);
    const outDir = path.resolve(args.values.out ??
      path.join(root, '.temp/experiments/azure-qap', new Date().toISOString().replaceAll(/[:.]/g, '-') + '-' + randomUUID()));
    const resources = new Resources(path.resolve(args.values.env), path.join(outDir, 'ownership.json'), false, controller.signal);
    let activeRecord: Emit | undefined;
    const emit: Emit = (type, data) => {
      if (!activeRecord) throw new Error('Experiment evidence was emitted outside an active stage.');
      activeRecord(type, evidenceValue(data));
    };
    const kusto = new Kusto(resources.azure, resources.manifest.runId, emit);
    const driver = new Driver(resources, config, emit);
    const scenarios = new Scenarios(driver, kusto, config);
    const stage = (name: string, run: (ctx: StageContext) => Promise<StageResult<ResultRow>>,
      timeoutMs = (config.qapWaitSeconds + 180) * 1000): Stage<ResultRow> => ({
      name, timeoutMs,
      run: async ctx => {
        resources.useSignal(ctx.signal);
        activeRecord = (type, data) => ctx.record(type, evidenceValue(data));
        try { return await run(ctx); }
        finally { activeRecord = undefined; }
      },
    });
    const result = await runExperiment<ResultRow>({
      id: resources.manifest.runId,
      title: 'Azure QAP: incremental acceleration, maintenance and reader safety',
      columns,
      stages: [
        stage('Declare isolated resources', async ctx => {
          ctx.record('configuration', config);
          await resources.deploy(ctx.signal);
          return { status: 'passed', summary: 'Created only the run-owned database, containers, reader assignment and publisher authority.' };
        }, 1_200_000),
        stage('Warm baseline', ctx => scenarios.initialize(ctx)),
        stage('Idle control', ctx => scenarios.idleControl(ctx)),
        stage('Known full-reload control', ctx => scenarios.reload(ctx)),
        stage('Small append', ctx => scenarios.append(ctx)),
        stage('Partial OPTIMIZE', ctx => scenarios.maintenance('partial-optimize', ctx)),
        stage('Broad OPTIMIZE', ctx => scenarios.maintenance('broad-optimize', ctx)),
        stage('Old-partition DELETE/PURGE', ctx => scenarios.maintenance('purge', ctx)),
        stage('Physical VACUUM LITE', ctx => scenarios.maintenance('vacuum', ctx)),
        stage('Metadata-only policy refresh', ctx => scenarios.policyRefresh(ctx)),
        stage('Concurrent append / maintenance', ctx => scenarios.concurrent(ctx)),
        stage('Immediate VACUUM / lagging reader', ctx => scenarios.immediateVacuum(ctx)),
        stage('Source generation / version downgrade', ctx => scenarios.repoint(ctx)),
      ],
      cleanup: async ctx => {
        resources.useSignal(ctx.signal);
        activeRecord = (type, data) => ctx.record(type, evidenceValue(data));
        try {
          ctx.record('cleanup-start', { scope: 'ownership-manifest', sandboxReset: false });
          await resources.cleanup(true, ctx.signal);
          ctx.record('cleanup-finished', { phase: resources.manifest.phase });
        } finally { activeRecord = undefined; }
      },
    }, {
      outDir, timeoutMs: 7_200_000, signal: controller.signal,
      settleTimeoutMs: 190_000, cleanupTimeoutMs: 660_000,
      evidence: { maxRecords: 100_000, maxBytes: 64 * 1024 * 1024, syncEvery: 1 },
      secrets: Object.values(resources.values).filter(value => value.length >= 8),
    });
    console.log(`Report: ${result.files.markdown}`);
    if (resources.manifest.phase !== 'cleaned')
      console.log(`Cleanup preview: npx nx run azure-qap-experiment:cleanup -- --manifest ${resources.filename}`);
    if (controller.signal.aborted) return 130;
    return { passed: 0, failed: 1, inconclusive: 2, finding: 3 }[result.report.status];
  } finally {
    process.removeListener('SIGINT', interrupt);
    process.removeListener('SIGTERM', interrupt);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).then(code => { process.exitCode = code; }, error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
