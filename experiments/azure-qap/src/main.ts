import fs from 'node:fs';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Azure, Config, LOG_ANALYTICS_API, ROOT, identity_principal, load_env } from '../../../infra/azure/bootstrap.ts';
import { childEnvironment, deletePrefix, Fixtures } from './fixtures.js';
import { Kusto, kql } from './kusto.js';
import { formatDuration, helpText, loadProfile, parseOptions, trialBytes, MIB, type Options } from './profile.js';
import { lateLogHelp, positiveControl, profileSummary, resultsTable, writeReport, Evidence, exitCode, type Cleanup,
  type RunReport } from './report.js';
import { Narrator, planTrials, runTrial, systemClock, type Trial, type TrialEnvironment, type TrialResult } from './scenario.js';
import { logsQuery } from './storage-reads.js';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const CLEANUP_MS = 5 * 60_000;

/** Sortable, lowercase and safe for Kusto table names and storage paths, e.g. `20250102t030405_ab12`. */
export function makeRunId(date: Date, suffix = randomBytes(2).toString('hex')): string {
  const iso = date.toISOString();
  if (!/^[0-9a-f]{4}$/.test(suffix)) throw new Error('Run ID suffix must be four hex digits.');
  return `${iso.slice(0, 10).replaceAll('-', '')}t${iso.slice(11, 19).replaceAll(':', '')}_${suffix}`;
}

export function secretsFrom(values: Record<string, string | undefined>): string[] {
  return Object.entries(values)
    .filter(([key, value]) => /SUBSCRIPTION|TENANT|OPERATOR|SECRET|TOKEN|KEY|PASSWORD|GENERATION/.test(key) &&
      typeof value === 'string' && value.length >= 8)
    .map(([, value]) => value!);
}

/** Provisioning writes the reset generation to `.env.azure` beside the private `.env`. */
export function resetGeneration(values: Record<string, string | undefined>, envFile: string): string | undefined {
  if (values.ARROWCOL_RESET_GENERATION) return values.ARROWCOL_RESET_GENERATION;
  const generated = path.join(path.dirname(envFile), '.env.azure');
  return fs.existsSync(generated) ? load_env(generated).ARROWCOL_RESET_GENERATION : undefined;
}

export async function limited<T>(items: readonly T[], parallel: number, work: (item: T) => Promise<void>): Promise<void> {
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(parallel, items.length) }, async () => {
    while (next < items.length) await work(items[next++]!);
  }));
}

/** Printed only when automatic cleanup failed, so the leftovers can be removed by hand. */
export function manualCleanup(runId: string, tables: string[], c: Pick<Config, 'account' | 'experimentContainer' |
  'experimentDatabase'>): string {
  return [
    `Remove the leftovers by hand. In Kusto database ${c.experimentDatabase}:`,
    ...tables.map(table => `  ${kql.drop(table)}`),
    'Then delete the fixture directory:',
    `  az storage fs directory delete --account-name ${c.account} --file-system ${c.experimentContainer} ` +
      `--name runs/${runId} --auth-mode login --yes`,
  ].join('\n');
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export async function main(argv: readonly string[]): Promise<number> {
  const defaults = loadProfile(path.join(HERE, '..', 'scenario.json'));
  let options: Options;
  try { options = parseOptions(argv, defaults); }
  catch (error) {
    console.error(`${message(error)}\nRun with --help for usage.`);
    return 2;
  }
  if (options.help) {
    console.log(helpText(defaults));
    return 0;
  }
  const { profile } = options;
  const clock = systemClock;
  const started = clock.now();
  const runId = makeRunId(new Date(started));
  const out = path.resolve(ROOT, options.out ?? path.join('.temp/experiments/azure-qap', runId));
  const envFile = path.resolve(ROOT, options.env);
  const values = { ...load_env(envFile), ...process.env } as Record<string, string>;
  const generation = resetGeneration(values, envFile);
  const config = new Config(values);
  const azure = new Azure(config);
  const secrets = secretsFrom({ ...values, ARROWCOL_RESET_GENERATION: generation });
  const redact = (text: string) => config.redact(text);
  fs.mkdirSync(out, { recursive: true });
  const evidence = new Evidence(path.join(out, 'evidence.jsonl'), secrets);
  const narrator = new Narrator(clock, started);
  const say = (text: string) => narrator.say('run', text);
  const kusto = new Kusto(azure, config.experimentDatabase, runId);
  const trials = planTrials(runId, profile);
  const tables = new Set<string>();
  const results: TrialResult[] = [];
  const location: RunReport['location'] = { account: config.account, container: config.experimentContainer,
    database: config.experimentDatabase, workspace: config.logAnalyticsWorkspace };

  const interrupt = new AbortController();
  let signals = 0;
  const onSignal = (name: string) => {
    if (++signals === 1) {
      console.log(`\n${name}: stopping trials; cleanup still runs (up to ${formatDuration(CLEANUP_MS)}), then the report is written.`);
      interrupt.abort(new Error(`Interrupted by ${name}.`));
      return;
    }
    console.log(`\n${name}: already stopping; waiting for cleanup to drop the run's tables and delete runs/${runId}/.`);
  };
  const sigint = () => onSignal('SIGINT');
  const sigterm = () => onSignal('SIGTERM');
  process.on('SIGINT', sigint);
  process.on('SIGTERM', sigterm);

  let error: string | undefined;
  let trialsStarted = false;
  try {
    say(`Azure QAP demo ${runId}: ${profile.operations.join(', ')} x${profile.repetitions}, ${profile.parallel} at a time.`);
    say(`Each trial seeds ${2 * profile.files + 2} files (${trialBytes(profile) / MIB} MiB) under ` +
      `${config.account}/${config.experimentContainer}/runs/${runId}/, accelerates them in Kusto database ` +
      `${config.experimentDatabase}, mutates, watches QAP for ${formatDuration(profile.tailMs)} and waits up to ` +
      `${formatDuration(profile.logWaitMs)} for StorageBlobLogs. Budget ${formatDuration(profile.budgetMs)}.`);
    say(`Evidence and report: ${path.relative(process.cwd(), out)}`);

    // Preflight: everything here is read-only and comes from `npx nx run azure-infra:provision`.
    for (const binary of ['arrowcol-experiment', 'delta-maintenance'])
      if (!fs.existsSync(path.join(ROOT, 'target/release', binary)))
        throw new Error(`target/release/${binary} is missing; run \`npx nx run azure-qap-experiment:build\`.`);
    if (!generation) throw new Error('ARROWCOL_RESET_GENERATION is missing; run `npx nx run azure-infra:provision` first.');
    const cluster = await azure.arm('GET', config.cluster_id);
    if (cluster?.properties?.state !== 'Running')
      throw new Error(`Kusto cluster is ${cluster?.properties?.state ?? 'missing'}; start it with \`npx nx run azure-infra:provision\`.`);
    const principal = await identity_principal(azure);
    location.principal = principal;
    const [, container] = await azure.blob('HEAD', config.experimentContainer, '', { restype: 'container' }, undefined, {}, true);
    if (!Object.keys(container).length)
      throw new Error(`Storage container ${config.experimentContainer} is missing; run \`npx nx run azure-infra:provision\`.`);
    say(`Kusto reads storage as managed identity ${principal} (StorageBlobLogs RequesterObjectId).`);
    const capacity = await kusto.capacity(interrupt.signal).catch(() => undefined);
    say(`QAP capacity (\`.show capacity\`): ${capacity ? `total ${capacity.Total}, consumed ${capacity.Consumed}, ` +
      `remaining ${capacity.Remaining}` : 'not reported'}.`);
    let logs: TrialEnvironment['logs'];
    if (profile.logWaitMs > 0) {
      const workspace = await azure.arm('GET', config.workspace_id, undefined, true, LOG_ANALYTICS_API);
      const customerId = workspace?.properties?.customerId;
      if (typeof customerId !== 'string')
        throw new Error(`Log Analytics workspace ${config.logAnalyticsWorkspace} is missing; run \`npx nx run azure-infra:provision\` ` +
          'or pass --log-wait 0.');
      location.workspaceCustomerId = customerId;
      logs = { query: logsQuery(azure, customerId), scope: { workspace: customerId, account: config.account,
        container: config.experimentContainer, requester: principal } };
      say(`StorageBlobLogs come from workspace ${config.logAnalyticsWorkspace} (customer ID ${customerId}).`);
    }

    const root = `abfss://${config.experimentContainer}@${config.account}.dfs.core.windows.net/runs/${runId}/`;
    const fixtures = new Fixtures({ repo: ROOT, root, redact, env: childEnvironment(
      { ...process.env, ...values, ...config.generated(), ARROWCOL_RESET_GENERATION: generation },
      root, path.join(ROOT, '.temp/qap-stage')) });
    const budget = AbortSignal.timeout(Math.max(1, started + profile.budgetMs - clock.now()));
    const source = (trial: Trial) => `${root}${trial.name};managed_identity=${principal}`;
    const environment: TrialEnvironment = { runId, profile, fixtures, kusto, source, principal, logs, clock, narrator,
      deadline: started + profile.budgetMs, budget, interrupt: interrupt.signal,
      record: (trial: string, kind: string, data: unknown) => evidence.record(trial, kind, data),
      created: (table: string) => { tables.add(table); } };
    trialsStarted = true;
    await limited(trials, profile.parallel, async trial => {
      if (interrupt.signal.aborted) return;
      results.push(await runTrial(environment, trial));
    });
  } catch (failure) {
    error = redact(message(failure));
    say(`ERROR: ${error}`);
  }

  results.sort((a, b) => trials.findIndex(trial => trial.name === a.trial) - trials.findIndex(trial => trial.name === b.trial));
  console.log(`\n${resultsTable(results, secrets)}\n`);
  const control = positiveControl(results);
  say(`Positive control: ${control.status}: ${control.detail}`);

  // Cleanup always runs once any trial may have written blobs or tables: after success, failure, budget overrun or Ctrl+C.
  const cleanup: Cleanup = { status: 'nothing', errors: [] };
  if (trialsStarted) {
    say(`CLEANUP: dropping ${tables.size} table(s) and deleting runs/${runId}/.`);
    const signal = AbortSignal.timeout(CLEANUP_MS);
    for (const table of tables) {
      try { await kusto.drop(table, signal); }
      catch (failure) { cleanup.errors.push(redact(`drop ${table}: ${message(failure)}`)); }
    }
    try { await deletePrefix(azure, config.experimentContainer, `runs/${runId}/`); }
    catch (failure) { cleanup.errors.push(redact(`delete runs/${runId}/: ${message(failure)}`)); }
    cleanup.status = cleanup.errors.length ? 'failed' : 'done';
    say(cleanup.errors.length ? `Cleanup FAILED:\n  ${cleanup.errors.join('\n  ')}\n${manualCleanup(runId, [...tables], config)}`
      : 'Cleanup done.');
  }
  process.off('SIGINT', sigint);
  process.off('SIGTERM', sigterm);

  const finished = clock.now();
  const report: RunReport = { runId, startedAt: new Date(started).toISOString(), finishedAt: new Date(finished).toISOString(),
    durationMs: finished - started, profile: profileSummary(profile), location, trials: results, control,
    cleanup, interrupted: interrupt.signal.aborted, ...(error ? { error } : {}), exitCode: 0 };
  report.exitCode = exitCode(report);
  await writeReport(report, out, secrets);
  const help = lateLogHelp(report, out);
  if (help) console.log(`\n${help}`);
  say(`Report: ${path.relative(process.cwd(), path.join(out, 'report.md'))} (exit ${report.exitCode}).`);
  return report.exitCode;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main(process.argv.slice(2)).catch(error => {
    console.error(message(error));
    return 1;
  });
