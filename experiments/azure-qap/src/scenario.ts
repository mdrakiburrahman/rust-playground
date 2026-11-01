import { anchorMinute, checkMutation, trialName, type Fixtures, type SourceDelta } from './fixtures.js';
import { describeSample, kql, ready, retainedExtentRows, unacceleratedRows, type AccelerationSample, type Kusto,
  type Target } from './kusto.js';
import { formatDuration, MIB, type Operation, type Profile } from './profile.js';
import { bodyReads, rawQuery, readsQuery, waitForLogs, type LogsQuery, type ReadScope, type ReadSummary } from './storage-reads.js';

export class BudgetExceeded extends Error {}

export interface Clock {
  now(): number;
  sleep(ms: number, signal: AbortSignal): Promise<void>;
}

export const systemClock: Clock = {
  now: () => Date.now(),
  sleep: (ms, signal) => new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
  }),
};

export interface Trial { operation: Operation; repetition: number; name: string; table: string; prefix: string }

/** Repetitions run in rounds so `--parallel 4` starts one trial of every operation together. */
export function planTrials(runId: string, profile: Pick<Profile, 'operations' | 'repetitions'>): Trial[] {
  const trials: Trial[] = [];
  for (let repetition = 1; repetition <= profile.repetitions; repetition++)
    for (const operation of profile.operations) {
      const name = trialName(operation, repetition);
      trials.push({ operation, repetition, name, table: `qap_${runId}_${name}`, prefix: `runs/${runId}/${name}/` });
    }
  return trials;
}

/** Prints what each phase is doing, prefixed with elapsed time and trial, plus copy-pasteable commands. */
export class Narrator {
  private readonly shown = new Map<string, string>();
  constructor(readonly clock: Clock, readonly start = clock.now(), readonly write: (text: string) => void = text => console.log(text)) {}

  stamp(): string {
    const seconds = Math.max(0, Math.floor((this.clock.now() - this.start) / 1000));
    return `${String(Math.floor(seconds / 60)).padStart(2, '0')}:${String(seconds % 60).padStart(2, '0')}`;
  }

  say(trial: string, text: string): void {
    this.write(`[${this.stamp()}] [${trial}] ${text}`);
  }

  /** Print a command once in full; later trials get a one-line pointer to keep parallel output readable. */
  code(trial: string, kind: string, text: string, subject: string, always = false): void {
    const first = this.shown.get(kind);
    if (first !== undefined && !always) {
      this.say(trial, `  ${kind}: same as printed above for ${first}, applied to ${subject}`);
      return;
    }
    if (first === undefined) this.shown.set(kind, subject);
    this.write([`[${this.stamp()}] [${trial}] ${kind}:`, ...text.split('\n').map(line => `    ${line}`)].join('\n'));
  }
}

export interface WatchOptions {
  kusto: Pick<Kusto, 'sample'>;
  table: string;
  target: Target;
  /** How long QAP must stay caught up before the watch ends. */
  holdMs: number;
  pollMs: number;
  deadline: number;
  clock: Clock;
  signal: AbortSignal;
  say: (text: string) => void;
  record?: (sample: AccelerationSample) => void;
}
export interface WatchResult { firstCaughtUpAt: number; caughtUpAt: number; regressions: number; samples: number; last: AccelerationSample }

/**
 * Poll QAP until it has stayed caught up for `holdMs`. QAP can report a finished state and then fall behind again
 * (for example while it notices a new Delta version), so a regression restarts the hold instead of ending the watch.
 */
export async function watch(options: WatchOptions): Promise<WatchResult> {
  let firstCaughtUpAt: number | undefined;
  let caughtUpAt: number | undefined;
  let regressions = 0;
  let previous = '';
  for (let samples = 1; ; samples++) {
    options.signal.throwIfAborted();
    const sample = await options.kusto.sample(options.table, options.signal);
    options.record?.(sample);
    const now = options.clock.now();
    const state = describeSample(sample, options.target);
    if (state !== previous) options.say(`QAP: ${state}`);
    previous = state;
    if (ready(sample, options.target)) {
      if (caughtUpAt === undefined) {
        caughtUpAt = now;
        firstCaughtUpAt ??= now;
        if (options.holdMs > 0)
          options.say(`QAP looks caught up; making sure it stays that way for ${formatDuration(options.holdMs)}.`);
      }
      if (now - caughtUpAt >= options.holdMs)
        return { firstCaughtUpAt: firstCaughtUpAt ?? now, caughtUpAt, regressions, samples, last: sample };
    } else if (caughtUpAt !== undefined) {
      regressions++;
      caughtUpAt = undefined;
      options.say('REGRESSION: QAP looked caught up but fell behind again ("caught up, then regresses"); restarting the hold.');
    }
    if (now + options.pollMs > options.deadline)
      throw new BudgetExceeded(`Budget ran out while waiting for QAP (${state}).`);
    await options.clock.sleep(options.pollMs, options.signal);
  }
}

export type TrialStatus = 'complete' | 'late-logs-pending' | 'budget-exceeded' | 'failed';
export type Telemetry = 'complete' | 'pending' | 'skipped' | 'not-reached';
export interface TrialResult {
  trial: string;
  operation: Operation;
  repetition: number;
  table: string;
  prefix: string;
  status: TrialStatus;
  telemetry: Telemetry;
  error?: string;
  startedAt: string;
  mutationAt?: string;
  windowEnd?: string;
  finishedAt?: string;
  durationMs?: number;
  versionBefore?: number;
  versionAfter?: number;
  files?: { before: number; added: number; removed: number; unchanged: number; unchangedBytes: number };
  tombstones?: number;
  filesDeleted?: number;
  catchUpMs?: number;
  recoveryMs?: number;
  regressions: number;
  extents?: { untouchedRows: number; keptRows: number; unacceleratedRows: number };
  reads?: { warmUp: ReadSummary; unchanged: ReadSummary; afterMutation: ReadSummary };
  logPolls?: number;
  readsQuery?: string;
  rawQuery?: string;
}

export interface TrialEnvironment {
  runId: string;
  profile: Profile;
  fixtures: Pick<Fixtures, 'seed' | 'inspect' | 'maintain' | 'seedArgs' | 'maintainCommand'>;
  kusto: Pick<Kusto, 'create' | 'drop' | 'sample' | 'extents' | 'parity' | 'marker'>;
  /** `abfss://...;managed_identity=<principal>` connection string for a trial's Delta directory. */
  source: (trial: Trial) => string;
  principal: string;
  /** Absent when Log Analytics is not configured or `--log-wait 0`. */
  logs?: { query: LogsQuery; scope: Omit<ReadScope, 'prefix'> };
  clock: Clock;
  narrator: Narrator;
  deadline: number;
  /** Aborts when the run budget is spent. */
  budget: AbortSignal;
  /** Aborts on Ctrl+C / SIGTERM. */
  interrupt: AbortSignal;
  record: (trial: string, kind: string, data: unknown) => void;
  /** Registers a Kusto table for cleanup before it is created. */
  created: (table: string) => void;
}

function mib(bytes: number): string {
  return `${(bytes / MIB).toFixed(bytes % MIB ? 1 : 0)} MiB`;
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

const describeOperation: Record<Operation, string> = {
  optimize: 'OPTIMIZE compacts the small files of one partition into a new file; the other partitions are untouched.',
  purge: 'PURGE removes the expired partition; every other file is untouched.',
  vacuum: 'VACUUM physically deletes files that earlier commits already removed; the Delta snapshot does not change.',
  reload: 'RELOAD (positive control) drops and recreates the external table, so QAP must read every file again.',
};

/** seed -> accelerate -> mutate -> tail -> log wait, for one operation. Never throws; failures land in the result. */
export async function runTrial(env: TrialEnvironment, trial: Trial): Promise<TrialResult> {
  const { clock, narrator, profile, kusto, fixtures } = env;
  const say = (text: string) => narrator.say(trial.name, text);
  const show = (kind: string, text: string, subject = trial.table, always = false) =>
    narrator.code(trial.name, kind, text, subject, always);
  const started = clock.now();
  const result: TrialResult = { trial: trial.name, operation: trial.operation, repetition: trial.repetition, table: trial.table,
    prefix: trial.prefix, status: 'failed', telemetry: 'not-reached', startedAt: new Date(started).toISOString(), regressions: 0 };
  const signal = AbortSignal.any([env.interrupt, env.budget]);
  const finish = () => {
    result.finishedAt = new Date(clock.now()).toISOString();
    result.durationMs = clock.now() - started;
    env.record(trial.name, 'result', result);
    return result;
  };
  let delta: SourceDelta;
  let mutationAt: number;
  let windowEnd: number;
  try {
    // 1. Seed: a tiny Hive-partitioned Delta table, built from server-side copies of one Parquet file.
    const anchor = anchorMinute(clock.now());
    const tombstones = trial.operation === 'vacuum';
    say(`SEED: writing ${profile.files} expired + ${profile.files} compactable + 1 control + 1 live file(s) of ${profile.fileMib} MiB ` +
      `under ${trial.prefix}${tombstones ? ', then purging the expired partition so VACUUM has tombstones to delete' : ''}.`);
    show('Fixture command', `target/release/arrowcol-experiment ${fixtures.seedArgs(trial.name, anchor, profile.fileMib, profile.files,
      tombstones, 900).join(' ')}`, trial.name);
    const seed = await fixtures.seed(trial.name, anchor, profile.fileMib, profile.files, tombstones, signal);
    env.record(trial.name, 'seed', seed);
    result.versionBefore = seed.version;
    result.tombstones = seed.tombstones;
    say(`Seeded Delta v${seed.version}: ${seed.files.length} files, ${seed.rows} rows, ${mib(seed.totalBytes)}` +
      (seed.tombstones ? `, ${seed.tombstones} tombstoned file(s) waiting for VACUUM.` : '.'));

    // 2. Accelerate: external table + QAP policy, then wait until QAP has accelerated every row and stays caught up.
    say(`ACCELERATE: creating external table ${trial.table} and enabling Query Acceleration.`);
    show('Kusto commands', `${kql.create(trial.table, env.source(trial))}\n${kql.policy(trial.table, env.principal)}`);
    env.created(trial.table);
    await kusto.create(trial.table, env.source(trial), env.principal, signal, env.deadline);
    const accelerating = clock.now();
    say(`Waiting for QAP to accelerate Delta v${seed.version} (${seed.rows} rows). Poll every ${formatDuration(profile.pollMs)} with:`);
    show('QAP state', `// QueryAccelerationState holds TableVersion, CompletionPercentage, IsHealthy and AcceleratedRowCount\n` +
      `${kql.details(trial.table)}\n\n// Pending files and bytes\n${kql.statistics(trial.table)}`);
    const record = (sample: AccelerationSample) => env.record(trial.name, 'qap', sample);
    const warm = await watch({ kusto, table: trial.table, target: seed, holdMs: profile.settleMs, pollMs: profile.pollMs,
      deadline: env.deadline, clock, signal, say, record });
    result.catchUpMs = warm.caughtUpAt - accelerating;
    result.regressions += warm.regressions;
    say(`QAP caught up after ${formatDuration(result.catchUpMs)}. Checking which extents hold each partition, and row parity:`);
    show('Extent inventory', kql.extents(trial.table));
    const extentsBefore = await kusto.extents(trial.table, signal);
    env.record(trial.name, 'extents-before', extentsBefore);
    show('Row parity', kql.parity(trial.table, seed.expectedGroups.length));
    await kusto.parity(trial.table, seed.expectedGroups, signal);
    say(`${extentsBefore.length} extent group(s); Kusto rows match the Delta oracle.`);

    // 3. Mutate: the real maintenance binary, or the reload positive control.
    say(`MUTATE: ${describeOperation[trial.operation]}`);
    mutationAt = clock.now();
    result.mutationAt = new Date(mutationAt).toISOString();
    let filesDeleted = 0;
    if (trial.operation === 'reload') {
      show('Reload', `${kql.drop(trial.table)}\n${kql.create(trial.table, env.source(trial))}\n${kql.policy(trial.table, env.principal)}`);
      await kusto.drop(trial.table, signal);
      await kusto.create(trial.table, env.source(trial), env.principal, signal, env.deadline);
    } else {
      show(`Maintenance command (${trial.operation})`, fixtures.maintainCommand(trial.name, trial.operation, anchor, clock.now()),
        trial.name);
      const { plan, applied } = await fixtures.maintain(trial.name, trial.operation, anchor, signal, () => clock.now());
      env.record(trial.name, 'maintenance', { plan, applied });
      filesDeleted = applied.filesDeleted;
      result.filesDeleted = filesDeleted;
      say(`delta-maintenance ${trial.operation}: ${applied.outcome}, ${applied.filesRemoved} file(s) removed from the log, ` +
        `${applied.filesDeleted} file(s) deleted from storage.`);
    }
    const after = await fixtures.inspect(trial.name, signal);
    env.record(trial.name, 'inspect', after);
    delta = checkMutation(trial.operation, seed, after, filesDeleted);
    result.versionAfter = after.version;
    result.files = { before: seed.files.length, added: delta.added.length, removed: delta.removed.length,
      unchanged: delta.unchanged.length, unchangedBytes: delta.unchanged.reduce((sum, file) => sum + file.bytes, 0) };
    say(`Delta v${seed.version} -> v${after.version}: +${delta.added.length} / -${delta.removed.length} file(s); ` +
      `${delta.unchanged.length} file(s) (${mib(result.files.unchangedBytes)}) did not change, so QAP has no reason to read them again.`);

    // 4. Tail: watch QAP converge on the new version and stay there.
    say(`TAIL: watching QAP until it has stayed caught up on Delta v${after.version} for ${formatDuration(profile.tailMs)}.`);
    const tail = await watch({ kusto, table: trial.table, target: after, holdMs: profile.tailMs, pollMs: profile.pollMs,
      deadline: env.deadline, clock, signal, say, record });
    result.recoveryMs = tail.caughtUpAt - mutationAt;
    result.regressions += tail.regressions;
    windowEnd = clock.now();
    result.windowEnd = new Date(windowEnd).toISOString();

    // Post-window checks run after windowEnd, so their own reads are not counted as QAP re-reads.
    const extentsAfter = await kusto.extents(trial.table, signal);
    env.record(trial.name, 'extents-after', extentsAfter);
    const touched = new Set([...delta.added, ...delta.removed].map(file => file.partition));
    const untouched = new Set(delta.unchanged.map(file => file.partition).filter(partition => !touched.has(partition)));
    const retained = retainedExtentRows(extentsBefore, extentsAfter, untouched);
    result.extents = { untouchedRows: retained.before, keptRows: retained.kept, unacceleratedRows: unacceleratedRows(extentsAfter) };
    await kusto.parity(trial.table, after.expectedGroups, signal);
    say(`QAP recovered ${formatDuration(result.recoveryMs)} after the mutation (${result.regressions} regression(s)). ` +
      `Untouched partitions kept ${retained.kept}/${retained.before} rows in their original extents; parity OK.`);
  } catch (error) {
    if (env.budget.aborted || error instanceof BudgetExceeded) {
      result.status = 'budget-exceeded';
      result.error = error instanceof BudgetExceeded ? error.message : 'Budget ran out during this phase.';
    } else {
      result.status = 'failed';
      result.error = env.interrupt.aborted ? 'Interrupted.' : message(error);
    }
    say(`${result.status.toUpperCase()}: ${result.error}`);
    return finish();
  }

  // 5. Log wait: count which unchanged files Kusto's identity read again, using StorageBlobLogs.
  const scope = env.logs && { ...env.logs.scope, prefix: trial.prefix };
  if (scope) {
    const window = { from: started - 60_000, mutationAt, windowEnd };
    result.readsQuery = readsQuery(scope, window);
    result.rawQuery = rawQuery(scope, window.from);
  }
  if (!env.logs || !scope || profile.logWaitMs === 0) {
    result.status = 'late-logs-pending';
    result.telemetry = 'skipped';
    say('LOG WAIT skipped; the report prints the Log Analytics query to run later.');
    return finish();
  }
  const deadline = Math.min(clock.now() + profile.logWaitMs, env.deadline);
  say(`LOG WAIT: StorageBlobLogs usually arrive minutes late. Polling every ${formatDuration(profile.logPollMs)} ` +
    `for up to ${formatDuration(deadline - clock.now())} with this Log Analytics query:`);
  show('Log Analytics query', result.readsQuery!, trial.prefix, true);
  let lastStatus = '';
  try {
    const wait = await waitForLogs({ query: env.logs.query, scope, window: { from: started - 60_000, mutationAt, windowEnd },
      deadline, pollMs: profile.logPollMs, signal: env.interrupt, now: () => clock.now(), sleep: clock.sleep,
      nudge: async () => { await kusto.marker(trial.table, env.interrupt); },
      onPoll: (groups, status) => {
        const text = `${status} (${groups.length} log group(s) so far)`;
        if (status !== lastStatus) say(`Logs: ${text}`);
        lastStatus = status;
      } });
    env.record(trial.name, 'storage-reads', wait);
    const unchanged = new Set(delta.unchanged.map(file => file.path));
    result.reads = { warmUp: bodyReads(wait.groups, 'warm-up'), unchanged: bodyReads(wait.groups, 'after-mutation', unchanged),
      afterMutation: bodyReads(wait.groups, 'after-mutation') };
    result.logPolls = wait.polls;
    result.telemetry = wait.status;
    result.status = wait.status === 'complete' ? 'complete' : 'late-logs-pending';
    say(`${wait.status === 'complete' ? 'Logs complete' : 'Logs still arriving (so far)'}: QAP read ${result.reads.warmUp.files} ` +
      `Parquet file(s) while warming up, then re-read ${result.reads.unchanged.files}/${delta.unchanged.length} UNCHANGED file(s) ` +
      `(${mib(result.reads.unchanged.bytes)}) after the ${trial.operation}.`);
  } catch (error) {
    result.status = 'failed';
    result.error = `Log Analytics: ${env.interrupt.aborted ? 'interrupted' : message(error)}`;
    say(`FAILED: ${result.error}`);
  }
  return finish();
}
