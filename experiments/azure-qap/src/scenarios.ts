import { createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import type { StageContext, StageResult } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { assess, deltaChanges, type AccelerationSample, type Snapshot } from './evidence.js';
import { Kusto, repointRequiresPolicyRemoval } from './kusto.js';
import type { Profile } from './profile.js';

export interface Oracle extends Snapshot {
  columns: string[];
  records: unknown[][];
}
export type Operation = 'partial-optimize' | 'broad-optimize' | 'purge' | 'vacuum';
export interface MaintenanceResult {
  deletedFiles: number;
  changed: boolean;
}
export interface Writer {
  finished: Promise<void>;
  stop(): Promise<void>;
}
export interface FixtureDriver {
  seed(table: string, signal: AbortSignal): Promise<Oracle>;
  seedBroad(table: string, signal: AbortSignal): Promise<Oracle>;
  inspect(table: string, signal: AbortSignal): Promise<Oracle>;
  append(table: string, batches: number, signal: AbortSignal): Promise<void>;
  writer(table: string, signal: AbortSignal): Promise<Writer>;
  maintain(table: string, operation: Operation, signal: AbortSignal): Promise<MaintenanceResult>;
}
export interface ResultRow {
  scenario: string;
  status: string;
  addedFiles: number;
  removedFiles: number;
  unchangedFiles: number;
  vacuumDeletedFiles: number | null;
  qapLagMs: number | null;
  peakPendingFiles: number | null;
  ingestionAdvanced: boolean | null;
  correctness: string;
  queryCpuMs: number | null;
  freshQueryCpuMs: number | null;
  reingestion: string;
  backgroundCpuMs: null;
  notes: string;
}

export function pendingWorkWarning(addedFiles: number, unchangedFiles: number, version: number,
  samples: readonly AccelerationSample[]): string | undefined {
  const pending = samples.flatMap(sample => sample.pendingFiles === undefined ? [] : [sample.pendingFiles]);
  const peak = pending.length ? Math.max(...pending) : 0;
  if (unchangedFiles === 0 || peak <= addedFiles) return undefined;
  const versions = [...new Set(samples.filter(sample => (sample.pendingFiles ?? 0) > addedFiles)
    .map(sample => sample.version ?? 'unknown'))];
  return `Observed ${peak} pending data files with only ${addedFiles} new/replacement files and ${unchangedFiles} unchanged files. ` +
    `Reported QAP state versions: ${versions.join(', ')}; target Delta version: ${version}. ` +
    'This is broader pending work, not per-file proof of physical reingestion. ' +
    'Catalog/statistics are separate reads; prior-version or background work may be included.';
}

export function canonicalRecords(records: readonly unknown[][]): string {
  const rows = records.map(row => [...row]);
  rows.sort((a, b) => {
    for (let index = 0; index < Math.max(a.length, b.length); index++) {
      const left = a[index], right = b[index];
      if (left === right) continue;
      if (typeof left === 'number' && typeof right === 'number') return left - right;
      const l = JSON.stringify(left), r = JSON.stringify(right);
      if (l === undefined || r === undefined) throw new Error('Undefined oracle/query values are not comparable.');
      return l < r ? -1 : 1;
    }
    return 0;
  });
  return JSON.stringify(rows);
}

export function fingerprint(records: readonly unknown[][]): string {
  return createHash('sha256').update(canonicalRecords(records)).digest('hex');
}

export function requireMutation(operation: Operation | 'append', before: Oracle, after: Oracle,
  maintenance?: MaintenanceResult): void {
  const change = deltaChanges(before, after);
  if (operation === 'vacuum') {
    if (!maintenance || maintenance.deletedFiles < 1 || after.version !== before.version ||
        canonicalRecords(after.records) !== canonicalRecords(before.records))
      throw new Error('VACUUM coverage requires physical deletion without changing the active snapshot.');
    return;
  }
  if (after.version <= before.version) throw new Error(`${operation} did not publish an actual Delta commit.`);
  if (operation === 'append' && (change.addedFiles < 1 || change.removedFiles !== 0 || after.rows <= before.rows))
    throw new Error('Append must add files and rows without removing existing files.');
  if (operation.endsWith('optimize') &&
      (change.addedFiles < 1 || change.removedFiles < 2 || before.fingerprint !== after.fingerprint))
    throw new Error('OPTIMIZE must replace multiple files and preserve the exact row multiset.');
  if (operation === 'partial-optimize' && change.unchangedFiles === 0)
    throw new Error('Partial OPTIMIZE must retain untouched control files.');
  if (operation === 'broad-optimize') {
    const active = new Set(after.files.map(file => file.path));
    const rewritten = new Set(before.files.filter(file => !active.has(file.path)).map(file => file.partition));
    if (rewritten.size < 2) throw new Error('Broad OPTIMIZE must actually rewrite more than one closed partition.');
  }
  if (operation === 'purge' && (change.removedFiles < 1 || change.addedFiles !== 0 || after.rows >= before.rows))
    throw new Error('PURGE must remove existing old-partition files and rows, not manufacture an empty commit.');
}

export class Scenarios {
  private readonly table = 'fixture';
  private readonly idle = 'idle_control';
  private frozenIdle?: Oracle;

  constructor(readonly driver: FixtureDriver, readonly kusto: Kusto, readonly config: Profile) {}

  async initialize(ctx: StageContext): Promise<StageResult<ResultRow>> {
    await this.kusto.initialize(ctx.signal);
    this.frozenIdle = await this.driver.seed(this.idle, ctx.signal);
    const main = await this.driver.seed(this.table, ctx.signal);
    await Promise.all([this.kusto.expose(this.idle, ctx.signal), this.kusto.expose(this.table, ctx.signal)]);
    await Promise.all([this.wait(this.idle, this.frozenIdle, ctx), this.wait(this.table, main, ctx)]);
    await this.kusto.probeProvenance(this.table, ctx.signal);
    return this.finish('Warm baseline / idle', this.table, main, main, Date.now(), [], null, '', ctx);
  }

  async idleControl(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const before = await this.driver.inspect(this.table, ctx.signal);
    const start = Date.now();
    const samples = await this.observe(this.table,
      signal => sleep(this.config.observeSeconds * 1000, undefined, { signal }), ctx);
    const after = await this.driver.inspect(this.table, ctx.signal);
    if (before.version !== after.version || before.fingerprint !== after.fingerprint)
      throw new Error('Idle fixture changed unexpectedly.');
    return this.finish('Idle control', this.table, before, after, start, samples, null, '', ctx);
  }

  async append(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const before = await this.driver.inspect(this.table, ctx.signal);
    const start = Date.now();
    let after: Oracle | undefined;
    const samples = await this.observe(this.table, async signal => {
      await this.driver.append(this.table, 1, signal);
      after = await this.driver.inspect(this.table, signal);
    }, ctx);
    if (!after) throw new Error('Append completed without a captured Delta oracle.');
    requireMutation('append', before, after);
    return this.finish('Small blind append', this.table, before, after, start, samples, null, '', ctx);
  }

  async maintenance(operation: Operation, ctx: StageContext): Promise<StageResult<ResultRow>> {
    const table = operation === 'broad-optimize' ? 'broad_optimize' : this.table;
    if (operation === 'broad-optimize') {
      const fixture = await this.driver.seedBroad(table, ctx.signal);
      await this.kusto.expose(table, ctx.signal);
      await this.wait(table, fixture, ctx);
    }
    const before = await this.driver.inspect(table, ctx.signal);
    const start = Date.now();
    let maintenance: MaintenanceResult | undefined;
    let after: Oracle | undefined;
    const samples = await this.observe(table, async signal => {
      maintenance = await this.driver.maintain(table, operation, signal);
      after = await this.driver.inspect(table, signal);
    }, ctx);
    if (!after) throw new Error('Maintenance completed without a captured Delta oracle.');
    requireMutation(operation, before, after, maintenance);
    return this.finish(operation, table, before, after, start, samples,
      operation === 'vacuum' ? maintenance!.deletedFiles : null,
      operation === 'broad-optimize' ? 'Broad closed-partition rewrites are legitimate replacement-file acceleration; the open writer frontier stays protected.' : '', ctx);
  }

  async immediateVacuum(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const table = 'lagging_reader';
    const before = await this.driver.seed(table, ctx.signal);
    await this.kusto.expose(table, ctx.signal);
    await this.wait(table, before, ctx);
    const start = Date.now();
    let deleted = 0;
    let after: Oracle | undefined;
    const transient: { lane: string; error?: string; rows?: number; matchedBefore?: boolean }[] = [];
    const samples = await this.observe(table, async signal => {
      const optimized = await this.driver.maintain(table, 'partial-optimize', signal);
      const purged = await this.driver.maintain(table, 'purge', signal);
      if (!optimized.changed || !purged.changed) throw new Error('Lagging-reader scenario did not generate fresh tombstones.');
      const vacuum = await this.driver.maintain(table, 'vacuum', signal);
      if (vacuum.deletedFiles === 0) throw new Error('Immediate VACUUM did not physically delete fresh tombstones.');
      deleted = vacuum.deletedFiles;
      for (const lane of ['default', 'freshness-1s'] as const) {
        try {
          const query = await this.kusto.query(table, before.columns, lane, signal);
          transient.push({ lane, rows: query.records.length,
            matchedBefore: canonicalRecords(query.records) === canonicalRecords(before.records) });
        } catch (error) {
          signal.throwIfAborted();
          transient.push({ lane, error: this.kusto.azure.c.redact(String(error)) });
        }
      }
      after = await this.driver.inspect(table, signal);
    }, ctx);
    ctx.record('lagging-reader-observations', { transient, deletedFiles: deleted });
    if (!after) throw new Error('Lagging-reader mutation completed without a captured Delta oracle.');
    const result = await this.finish('Immediate OPTIMIZE/PURGE -> VACUUM', table, before, after,
      start, samples, deleted, 'Zero retention tests reader safety, not append-conflict safety.', ctx);
    if (result.status !== 'failed' && transient.some(value => value.error)) {
      return { ...result, status: 'finding',
        summary: 'Immediate physical deletion caused an observed query error before QAP converged; later success does not erase it.',
        rows: result.rows?.map(row => ({ ...row, status: 'finding',
          notes: row.notes + ' Transient query failure was preserved in evidence.' })) };
    }
    return result;
  }

  async concurrent(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const table = 'concurrent';
    const before = await this.driver.seed(table, ctx.signal);
    await this.kusto.expose(table, ctx.signal);
    await this.wait(table, before, ctx);
    const start = Date.now();
    let deleted = 0;
    let after: Oracle | undefined;
    const samples = await this.observe(table, async signal => {
      const writer = await this.driver.writer(table, signal);
      let writerFinished = false;
      const writerOutcome = writer.finished.then(() => {
        writerFinished = true;
        return { status: 'completed' as const };
      }, (error: unknown) => {
        writerFinished = true;
        return { status: 'failed' as const, error };
      });
      try {
        const overlapStart = await this.driver.inspect(table, signal);
        if (writerFinished) throw new Error('Writer finished before maintenance started; concurrent coverage is not satisfied.');
        for (const operation of ['partial-optimize', 'purge', 'vacuum'] as const) {
          const result = await this.driver.maintain(table, operation, signal);
          if (!result.changed) throw new Error(`Concurrent ${operation} was a no-op; coverage is not satisfied.`);
          deleted += result.deletedFiles;
          await sleep(this.config.appendIntervalMs, undefined, { signal });
        }
        const overlapEnd = await this.driver.inspect(table, signal);
        const oldRows = new Set(overlapStart.records.map(row => JSON.stringify(row)));
        const newRows = overlapEnd.records.filter(row => !oldRows.has(JSON.stringify(row))).length;
        if (!newRows) throw new Error('No new ArrowCol rows committed during the maintenance window.');
        ctx.record('concurrent-overlap', { newRows, beforeVersion: overlapStart.version, afterVersion: overlapEnd.version });
        const outcome = await writerOutcome;
        if (outcome.status === 'failed') throw outcome.error;
        after = await this.driver.inspect(table, signal);
      } finally { await writer.stop(); }
    }, ctx);
    if (!after) throw new Error('Concurrent writer completed without a frozen Delta oracle.');
    if (after.version <= before.version || deleted === 0) throw new Error('Concurrent lifecycle produced insufficient mutation evidence.');
    return this.finish('Concurrent blind append + maintenance', table, before, after, start, samples,
      deleted, 'Writer was joined and its publisher drained before exact-version comparison.', ctx);
  }

  async reload(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const table = 'reload_control';
    const before = await this.driver.seed(table, ctx.signal);
    await this.kusto.expose(table, ctx.signal);
    await this.wait(table, before, ctx);
    const start = Date.now();
    const samples = [await this.kusto.sample(table, ctx.signal)];
    await this.kusto.reloadControl(table, ctx.signal);
    samples.push(await this.kusto.sample(table, ctx.signal));
    const after = await this.driver.inspect(table, ctx.signal);
    if (after.version !== before.version || after.fingerprint !== before.fingerprint)
      throw new Error('Full-reload control must leave the Delta source unchanged.');
    return this.finish('Known full-cache recreation control', table, before, after, start, samples, null,
      'Cache recreation is intentional. Aggregate diagnostics alone cannot calibrate a negative per-file ingestion verdict.', ctx, true);
  }

  async policyRefresh(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const before = await this.driver.inspect(this.table, ctx.signal);
    const start = Date.now();
    const samples = await this.observe(this.table, signal => this.kusto.enable(this.table, signal), ctx);
    const after = await this.driver.inspect(this.table, ctx.signal);
    return this.finish('Idempotent policy refresh', this.table, before, after, start, samples, null,
      'Metadata/control-plane refresh must not be confused with data cache ingestion.', ctx);
  }

  async repoint(ctx: StageContext): Promise<StageResult<ResultRow>> {
    const alias = 'source_generation';
    await this.driver.seed('source_old', ctx.signal);
    await this.driver.append('source_old', 2, ctx.signal);
    const before = await this.driver.inspect('source_old', ctx.signal);
    const after = await this.driver.seed('source_fresh', ctx.signal);
    if (after.version >= before.version) throw new Error('Source-generation edge requires a genuinely lower-version replacement source.');
    await this.kusto.define(alias, 'source_old', ctx.signal);
    await this.kusto.enable(alias, ctx.signal);
    await this.wait(alias, before, ctx);
    const start = Date.now();
    let blocked = false;
    const samples = await this.observe(alias, async signal => {
      try { await this.kusto.define(alias, 'source_fresh', signal); }
      catch (error) {
        if (!repointRequiresPolicyRemoval(error)) throw error;
        blocked = true;
        ctx.record('native-repoint-rejected', { status: 400, sourceVersion: before.version,
          attemptedVersion: after.version,
          reason: 'Kusto requires removal of the enabled QAP policy before changing the external connection string.',
          resetPerformed: false });
      }
    }, ctx);
    if (blocked) {
      const result = await this.finish('Source repoint rejected (QAP enabled)', alias, before, before,
        start, samples, null,
        'The lower-version source was not applied. Kusto requires QAP policy removal first; no cache reset was performed.', ctx);
      if (result.status === 'failed') return result;
      return { ...result, status: 'finding',
        summary: 'Native QAP rejected the connection-string change; the old exact result is preserved. Lower-version adoption was not exercised.',
        rows: result.rows?.map(row => ({ ...row, status: 'finding' })) };
    }
    return this.finish('Source repoint / lower Delta version', alias, before, after, start, samples, null,
      'A new source generation is an intentional identity change, distinct from ordinary OPTIMIZE/VACUUM.', ctx);
  }

  private async wait(table: string, snapshot: Snapshot, ctx: StageContext): Promise<AccelerationSample[]> {
    return this.kusto.wait(table, snapshot,
      Math.min(ctx.deadline, Date.now() + this.config.qapWaitSeconds * 1000), ctx.signal,
      this.config.sampleIntervalMs, table === this.idle ? undefined : this.idle);
  }

  private async observe(table: string, mutation: (signal: AbortSignal) => Promise<unknown>,
    ctx: StageContext): Promise<AccelerationSample[]> {
    const controller = new AbortController();
    const signal = AbortSignal.any([controller.signal, ctx.signal]);
    const [baseline] = await Promise.all([
      this.kusto.sample(table, signal), this.kusto.sample(this.idle, signal),
    ]);
    const samples: AccelerationSample[] = [baseline];
    let observationFailure: { cause: unknown } | undefined;
    const polling = (async () => {
      await sleep(this.config.sampleIntervalMs, undefined, { signal });
      while (!signal.aborted) {
        const [sample] = await Promise.all([
          this.kusto.sample(table, signal), this.kusto.sample(this.idle, signal),
        ]);
        samples.push(sample);
        await sleep(this.config.sampleIntervalMs, undefined, { signal });
      }
    })().catch(caught => {
      if (!(caught instanceof Error && caught.name === 'AbortError')) {
        observationFailure = { cause: caught };
        controller.abort();
      }
    });
    let mutationFailure: { cause: unknown } | undefined;
    try { await mutation(signal); }
    catch (caught) { mutationFailure = { cause: caught }; }
    finally { controller.abort(); await polling; }
    if (observationFailure) throw observationFailure.cause;
    if (mutationFailure) throw mutationFailure.cause;
    return samples;
  }

  private async finish(scenario: string, table: string, before: Oracle, after: Oracle,
    start: number, samples: AccelerationSample[], deleted: number | null, notes: string,
    ctx: StageContext, intentionalReset = false): Promise<StageResult<ResultRow>> {
    samples.push(...await this.wait(table, after, ctx));
    const lag = Date.now() - start;
    const normal = await this.kusto.query(table, after.columns, 'default', ctx.signal);
    await sleep(1100, undefined, { signal: ctx.signal });
    const fresh = await this.kusto.query(table, after.columns, 'freshness-1s', ctx.signal);
    const expected = canonicalRecords(after.records);
    const correctness = canonicalRecords(normal.records) === expected && canonicalRecords(fresh.records) === expected ? 'passed' : 'failed';
    if (this.frozenIdle) {
      const idle = await this.driver.inspect(this.idle, ctx.signal);
      if (idle.version !== this.frozenIdle.version || idle.fingerprint !== this.frozenIdle.fingerprint)
        throw new Error('Untouched idle control table changed during a trial.');
      await this.kusto.sample(this.idle, ctx.signal);
    }
    const assessment = assess({ before, after, mutationStartedAt: new Date(start).toISOString(),
      observationEndedAt: new Date().toISOString(), samples, originEvents: [],
      capabilities: this.kusto.capabilities, correctness });
    const inventory = (oracle: Oracle): Snapshot => ({ version: oracle.version, files: oracle.files,
      rows: oracle.rows, fingerprint: oracle.fingerprint });
    const warning = intentionalReset ? undefined : pendingWorkWarning(
      assessment.addedFiles, assessment.unchangedFiles, after.version, samples);
    const status = assessment.status === 'failed' ? 'failed' : warning ? 'finding' : assessment.status;
    ctx.record('trial', { scenario, before: inventory(before), after: inventory(after),
      assessment, aggregateWarning: warning ?? null, status, queryParity: correctness, qapLagMs: lag });
    const pending = samples.flatMap(sample => sample.pendingFiles === undefined ? [] : [sample.pendingFiles]);
    const firstIngestion = samples[0]?.lastIngestionAt;
    const lastIngestion = samples.at(-1)?.lastIngestionAt;
    const ingestionAdvanced = firstIngestion && lastIngestion &&
      Number.isFinite(Date.parse(firstIngestion)) && Number.isFinite(Date.parse(lastIngestion))
      ? Date.parse(lastIngestion) > Date.parse(firstIngestion) : null;
    const row: ResultRow = {
      scenario, status, addedFiles: assessment.addedFiles, removedFiles: assessment.removedFiles,
      unchangedFiles: assessment.unchangedFiles, vacuumDeletedFiles: deleted, qapLagMs: lag,
      peakPendingFiles: pending.length ? Math.max(...pending) : null,
      ingestionAdvanced,
      correctness: correctness === 'passed' ? 'PASS' : 'FAIL',
      queryCpuMs: normal.queryCpuMs ?? null, freshQueryCpuMs: fresh.queryCpuMs ?? null,
      reingestion: assessment.status === 'inconclusive' ? 'INCONCLUSIVE' : assessment.classification,
      backgroundCpuMs: null,
      notes: `${warning ? warning + ' ' : ''}${notes}${fresh.remoteDownloadedItems === undefined
        ? ' Freshness-1s query returned no remote-download measurement; it is not asserted to be a cache-bypass query.'
        : ` Freshness-1s remote downloads: ${fresh.remoteDownloadedItems}.`}`.trim(),
    };
    return { status, summary: warning ? `${warning} ${assessment.reason}` : assessment.reason, rows: [row] };
  }
}
