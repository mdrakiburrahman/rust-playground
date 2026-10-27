import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync, lstatSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { runOwnedProcess } from './owned-process.js';
import { collectorSample } from './resource-sampler.js';
import { physicalInventory } from './stress-storage.js';
import type { WaveSample } from './stress-criteria.js';

const execute = promisify(execFile);
const target = 'telemetry.sqlserver.sqlserver_cpu_utilization';
const captureScript = 'import subprocess,sys\nwith open(sys.argv[4],"wb") as out, open(sys.argv[4]+".stderr.private.log","wb") as err:\n subprocess.run([sys.argv[1],sys.argv[2],sys.argv[3],*sys.argv[5:]],stdout=out,stderr=err,check=True)';
const targetFileMiB = 128;
const latestPartitions = 1;

export function maintenanceArguments(root: string, timeoutSeconds: number): string[] {
  return [
    '--root', root, '--table', 'sqlserver_cpu_utilization',
    '--operation', 'optimize', '--operation', 'checkpoint', '--operation', 'vacuum',
    '--granularity', 'minute', '--latest-partitions', String(latestPartitions),
    '--target-file-mib', String(targetFileMiB), '--checkpoint-every-commits', '1',
    '--apply', '--interval-secs', '1', '--max-idle-backoff-secs', '1', '--max-cycles', '1',
    '--operation-timeout-secs', String(timeoutSeconds), '--listen', '0.0.0.0:9467',
  ];
}

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Required stress measurement is unavailable');
  return value as Record<string, unknown>;
}
function number(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Missing/imprecise stress measurement');
  return value;
}
function ok(value: unknown): Record<string, unknown> {
  const result = object(value);
  if (result.ok !== true) throw new Error('Actual stress measurement failed; no zero substitution');
  return object(result.value);
}
function publicReport(value: unknown): unknown {
  if (value === null || typeof value === 'number' || typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    if (/^(?:optimize|checkpoint|vacuum(?:[-_]lite)?|sqlserver_cpu_utilization|noop|applied|skipped)$/u.test(value)) return value;
    return { redactedSha256: createHash('sha256').update(value).digest('hex') };
  }
  if (Array.isArray(value)) return value.map(publicReport);
  return Object.fromEntries(Object.entries(object(value)).map(([key, item]) => [
    /^[a-z][a-z0-9_]*$/iu.test(key) ? key : `redacted-key-${createHash('sha256').update(key).digest('hex')}`,
    publicReport(item),
  ]));
}

export interface LocalStressOptions {
  workspace: string;
  directory: string;
  project: string;
  composeFiles: string[];
  env: NodeJS.ProcessEnv;
  reader: string;
  directAdmin: string;
  dataflowAdmin: string;
  quiver: boolean;
  physicalRoots: Record<string, string>;
  fixture: { batchRows: number; rowGroupRows: number; pageRows: number };
  windowSeconds: number;
  sampleTimeoutSeconds: number;
  measureSeconds: number | undefined;
  loadEnded: AbortSignal;
  checkRunning: () => void;
}

/** Concrete local functional adapter. Azure qualification is intentionally not inferred from it. */
export class LocalStressRuntime {
  private readonly results: string;
  private readonly identities = new Map<string, string>();
  private readonly owner = randomUUID();
  private unexpectedRestarts = 0;
  constructor(private readonly options: LocalStressOptions) {
    this.results = path.join(options.directory, 'results');
  }
  private check(): void {
    this.options.checkRunning();
    if (this.options.loadEnded.aborted) throw new Error('Actual source process stopped during stress');
  }
  private progress(): Record<string, unknown> {
    const progress = object(JSON.parse(readFileSync(path.join(this.results, 'driver-progress.json'), 'utf8')));
    const metadata = object(JSON.parse(readFileSync(path.join(this.results, 'run-metadata.json'), 'utf8')));
    if (typeof progress.run_id !== 'string' || !progress.run_id || progress.run_id !== metadata.run_id ||
        progress.scenario !== 'paired' || Date.now() - number(progress.timestamp) > 5000 ||
        number(progress.timestamp) > Date.now()) throw new Error('Stale/unbound actual driver progress');
    return progress;
  }
  async waitForInput(deadline: number): Promise<{ runId: string; inputDeadline: number }> {
    const enteredAt = Date.now();
    const requiredInputMs = (this.options.windowSeconds + this.options.sampleTimeoutSeconds) * 1000;
    while (Date.now() < deadline) {
      this.check();
      let progress: Record<string, unknown> | undefined;
      try {
        progress = this.progress();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      if (progress?.phase === 'measure') {
        const phaseStartedAt = number(progress.phase_started_at);
        if (phaseStartedAt > number(progress.timestamp) ||
            number(progress.phase_duration_ms) !== number(this.options.measureSeconds) * 1000) {
          throw new Error('Actual generator lifetime does not match the declared measure budget');
        }
        const pools = [];
        for (const pool of ['direct', 'dataflow']) {
          pools.push(await this.readyFixture(pool, deadline));
        }
        const current = this.progress();
        if (current.run_id !== progress.run_id || current.phase !== 'measure' ||
            current.phase_started_at !== phaseStartedAt || current.phase_duration_ms !== progress.phase_duration_ms) {
          throw new Error('Generator changed phase during fixture readiness');
        }
        const observedAt = Date.now();
        if (number(current.phase_remaining_ms) > number(current.phase_duration_ms)) {
          throw new Error('Generator remaining input exceeds its declared phase duration');
        }
        const remainingMs = number(current.phase_remaining_ms) - (observedAt - number(current.timestamp));
        if (remainingMs < requiredInputMs) {
          throw new Error('Remaining generator input cannot cover the immutable window and final sample');
        }
        if (pools.every(pool => pool !== undefined)) {
          if (observedAt >= deadline) break;
          writeFileSync(path.join(this.results, 'stress-readiness.json'), `${JSON.stringify({
            run_id: progress.run_id, observed_at: observedAt, input_deadline: observedAt + remainingMs,
            startup_entered_at: enteredAt,
            startup_deadline: deadline, required_input_ms: requiredInputMs,
            generator: {
              phase_started_at: phaseStartedAt, phase_duration_ms: current.phase_duration_ms,
              phase_remaining_ms: current.phase_remaining_ms, timestamp: current.timestamp,
              conservatively_remaining_ms: remainingMs,
            },
            fixture: this.options.fixture,
            optimizer: { granularity: 'minute', latest_partitions: latestPartitions,
              target_file_mib: targetFileMiB, newest_partition_protected: true },
            pools,
          }, null, 2)}\n`, { flag: 'wx' });
          return { runId: String(progress.run_id), inputDeadline: observedAt + remainingMs };
        }
      }
      await delay(Math.max(0, Math.min(100, deadline - Date.now())));
    }
    throw new Error('Actual BOTH-pool compactable source fixtures missed the startup deadline');
  }
  private async capture(command: string, input: string, output: string, deadline: number, extra: string[] = []): Promise<Record<string, unknown>> {
    this.check();
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error('Actual fixture capture exceeded its absolute deadline');
    await runOwnedProcess('python3', [
      '-c', captureScript, this.options.reader, command, input, output, ...extra,
    ], { cwd: this.options.workspace, env: this.options.env, timeoutMs: remaining });
    this.check();
    if (Date.now() >= deadline) throw new Error('Actual fixture capture exceeded its absolute deadline');
    return object(JSON.parse(readFileSync(output, 'utf8')));
  }
  private sourceFile(pool: string, file: Record<string, unknown>): string {
    if (typeof file.path !== 'string' || path.isAbsolute(file.path) || file.path.includes('\\') ||
        file.path.split('/').some(part => !part || part === '..' || part === '.')) throw new Error('Unsafe source-layout file path');
    return path.join(this.options.directory, pool, ...target.split('.'), file.path);
  }
  private async readyFixture(pool: string, deadline: number): Promise<Record<string, unknown> | undefined> {
    const root = path.join(this.options.directory, pool, ...target.split('.'));
    const initialCommit = lstatSync(path.join(root, '_delta_log', '00000000000000000000.json'), { throwIfNoEntry: false });
    if (!initialCommit) return undefined;
    if (!initialCommit.isFile()) throw new Error('Initial CPU commit is not a regular file');
    const output = path.join(this.results, `stress-readiness-${pool}.json`);
    const snapshot = await this.capture('summarize', path.join(this.options.directory, pool), output, deadline, ['--table', target]);
    const table = object(object(snapshot.tables)[target]);
    const version = number(table.version), rows = number(table.rows);
    if (!Array.isArray(table.files) || JSON.stringify(table.partition_columns) !== '["__arrowcol_ingestion"]') {
      throw new Error('Actual CPU fixture has malformed files or unexpected partitions');
    }
    if (!table.files.length || rows === 0) return undefined;
    const partitions = new Map<number, Record<string, unknown>[]>();
    for (const raw of table.files) {
      const file = object(raw);
      this.sourceFile(pool, file);
      number(file.rows); number(file.bytes);
      const values = object(file.partition_values);
      const value = values.__arrowcol_ingestion;
      if (Object.keys(values).length !== 1 || typeof value !== 'string' ||
          !/^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}$/u.test(value)) throw new Error('Malformed CPU ingestion partition');
      const start = Date.parse(`${value.slice(0, 10)}T${value.slice(11, 13)}:${value.slice(14)}:00Z`);
      if (!Number.isSafeInteger(start) ||
          new Date(start).toISOString().slice(0, 16).replace('T', '-').replace(':', '-') !== value) {
        throw new Error('Malformed CPU ingestion partition');
      }
      const files = partitions.get(start) ?? [];
      files.push(file); partitions.set(start, files);
    }
    const now = Date.now();
    // Match OPTIMIZE: protect the newest partition even when closed, then take latest N eligible partitions.
    const selected = [...partitions].sort(([a], [b]) => b - a)
      .filter(([start], index) => index > 0 && start + 60_000 <= now).slice(0, latestPartitions);
    for (const [, files] of selected) {
      const small = files.filter(file => number(file.bytes) < targetFileMiB * 1024 * 1024 / 2)
        .sort((a, b) => String(a.path) < String(b.path) ? -1 : String(a.path) > String(b.path) ? 1 : 0);
      const bin: Record<string, unknown>[] = [];
      let bytes = 0;
      for (const file of small) {
        if (bin.length >= 64 || bytes + number(file.bytes) > targetFileMiB * 1024 * 1024) break;
        bin.push(file); bytes += number(file.bytes);
      }
      if (bin.length < 2) continue;
      const file = [...bin].sort((a, b) => number(b.rows) - number(a.rows))[0];
      const layout = path.join(this.results, `stress-readiness-${pool}-layout.json`);
      const metadata = await this.capture('file-metadata', this.sourceFile(pool, file), layout, deadline);
      if (!Array.isArray(metadata.row_groups) || number(metadata.rows) !== number(file.rows) ||
          metadata.row_groups.reduce((sum: number, group: unknown) => sum + number(object(group).rows), 0) !== metadata.rows ||
          metadata.row_groups.some(group => number(object(group).rows) < 1 ||
            number(object(group).rows) > this.options.fixture.rowGroupRows)) {
        throw new Error('Actual CPU fixture has malformed or mismatched row-group metadata');
      }
      if (metadata.row_groups.length < 2) continue;
      return { pool, version, rows, active_files: table.files.length, selected_bin_files: bin.length,
        selected_bin_bytes: bytes, row_groups: metadata.row_groups.length,
        snapshot_sha256: createHash('sha256').update(readFileSync(output)).digest('hex'),
        layout_sha256: createHash('sha256').update(readFileSync(layout)).digest('hex') };
    }
    return undefined;
  }
  async sample(wave: number, runId: string, deadline: number): Promise<WaveSample> {
    this.check();
    const [collectors, physical] = await Promise.all([
      Promise.all([
        collectorSample(this.options.project, 'direct-1', this.options.directAdmin),
        collectorSample(this.options.project, 'dataflow-1', this.options.dataflowAdmin),
      ]),
      physicalInventory(this.options.physicalRoots, deadline),
    ]);
    let rssBytes = 0, backlog = 0, restarts = 0, oomEvents = 0;
    for (const [index, raw] of collectors.entries()) {
      const container = ok(raw.container);
      if (container.running !== true || typeof container.id !== 'string') throw new Error('Collector is not running');
      const identity = `${container.id}:${number(container.hostPid)}`;
      const previous = this.identities.get(String(raw.service));
      if (previous !== undefined && previous !== identity) this.unexpectedRestarts++;
      this.identities.set(String(raw.service), identity);
      restarts += number(container.restartCount);
      const resources = ok(raw.resources);
      rssBytes += number(ok(resources.process).rssBytes);
      const events = ok(resources.events);
      oomEvents += number(events.oom) + number(events.oom_kill);
      const admin = object(raw.admin);
      if (ok(admin.readiness).ready !== true) throw new Error('Collector became unready during qualification');
      const metrics = ok(admin.metrics);
      if (index === 0) {
        backlog += number(metrics.arrowcol_direct_active_streams);
        if (this.options.quiver) backlog += number(metrics.arrowcol_direct_quiver_backlog_bundles);
      } else {
        backlog += number(metrics.arrowcol_dfe_buffered_batches) + number(metrics.arrowcol_delta_pending_commits) +
          number(metrics.arrowcol_dataflow_durable_backlog_batches);
      }
    }
    const progress = this.progress();
    if (progress.run_id !== runId || progress.phase !== 'measure') throw new Error('Source left the declared measure phase');
    appendFileSync(path.join(this.results, 'stress-resource-evidence.jsonl'), `${JSON.stringify({
      run_id: runId, wave, collectors, physical, progress,
    })}\n`);
    return {
      timestamp: Date.now(), wave, phase: 'wave-boundary', driverRows: number(progress.scheduled_rows),
      rssBytes, physicalBytes: physical.fileBytes, backlog, restarts: restarts + this.unexpectedRestarts,
      oomEvents, exhaustedRetries: number(progress.send_policy_failures) + number(progress.receipt_failures),
    };
  }
  private async docker(args: string[], timeout = 5000): Promise<string> {
    const { stdout } = await execute('docker', args, {
      cwd: this.options.workspace, env: this.options.env, timeout, maxBuffer: 8 * 1024 * 1024,
    });
    return stdout;
  }
  private async ownedContainer(identity: string): Promise<{ id: string; state: Record<string, unknown> }> {
    const container = object(JSON.parse(await this.docker(['inspect', '--format',
      '{"id":{{json .Id}},"owner":{{json (index .Config.Labels "io.arrowcol.loadtest.stress-owner")}},' +
      '"project":{{json (index .Config.Labels "com.docker.compose.project")}},' +
      '"service":{{json (index .Config.Labels "com.docker.compose.service")}},"state":{{json .State}}}',
      identity,
    ])));
    if (container.owner !== this.owner || container.project !== this.options.project ||
        container.service !== 'delta-maintenance' || typeof container.id !== 'string' ||
        !/^[a-f0-9]{64}$/u.test(container.id)) throw new Error('Maintenance container ownership is unverified; refusing mutation');
    return { id: container.id, state: object(container.state) };
  }
  private async snapshot(pool: string, wave: number, phase: string, deadline: number): Promise<void> {
    const output = path.join(this.results, `stress-${wave}-${pool}-${phase}.json`);
    const snapshot = await this.capture('summarize', path.join(this.options.directory, pool), output, deadline, ['--table', target]);
    const tables = object(snapshot.tables);
    const table = object(tables[target]);
    if (number(table.rows) === 0 || !Array.isArray(table.files) || !table.files.length) {
      throw new Error('Maintenance target has no actual source rows/files');
    }
    const file = table.files.map(object).sort((a, b) => number(b.rows) - number(a.rows))[0];
    const layout = path.join(this.results, `stress-${wave}-${pool}-${phase}-layout.json`);
    const metadata = await this.capture('file-metadata', this.sourceFile(pool, file), layout, deadline);
    if (!Array.isArray(metadata.row_groups) || number(metadata.rows) !== number(file.rows) ||
        wave === 1 && phase === 'before' && metadata.row_groups.length < 2) {
      throw new Error('Actual source file did not prove the declared multi-rowgroup fixture');
    }
  }
  async operate(wave: number, deadline: number): Promise<void> {
    for (const pool of ['direct', 'dataflow']) {
      this.check();
      await this.snapshot(pool, wave, 'before', deadline);
      const name = `${this.options.project}-stress-${wave}-${pool}-${this.owner.slice(0, 8)}`;
      let identity = name;
      let completed = false;
      let failure: unknown;
      try {
        const remaining = deadline - Date.now();
        if (remaining <= 1000) throw new Error('Maintenance wave has no remaining operation budget');
        await runOwnedProcess('docker', [
          'compose', '--project-name', this.options.project, ...this.options.composeFiles,
          'run', '--detach', '--no-deps', '--use-aliases', '--name', name,
          '--label', `io.arrowcol.loadtest.stress-owner=${this.owner}`, 'delta-maintenance',
          ...maintenanceArguments(`file:///tmp/arrowcol-stress/${pool}/telemetry/sqlserver/`,
            Math.max(1, Math.floor(remaining / 1000))),
        ], { cwd: this.options.workspace, env: this.options.env, timeoutMs: remaining });
        identity = (await this.ownedContainer(name)).id;
        await runOwnedProcess('docker', ['wait', identity], {
          cwd: this.options.workspace, env: this.options.env, timeoutMs: Math.max(1, deadline - Date.now()),
        });
        const { state: exit } = await this.ownedContainer(identity);
        const logs = await this.docker(['logs', identity]);
        const structured = logs.split('\n').filter(line => line.trim().startsWith('{')).map(line => JSON.parse(line) as unknown);
        appendFileSync(path.join(this.results, 'stress-operations.jsonl'), `${JSON.stringify({
          wave, pool, table: target, operations: ['optimize', 'checkpoint', 'vacuum'],
          finishedAt: Date.now(), exitCode: exit.ExitCode, reports: structured.map(publicReport),
        })}\n`);
        const reports = structured.map(object).filter(report => report.table === 'sqlserver_cpu_utilization');
        const operations = new Set(reports.map(report => report.operation));
        if (exit.Running !== false || exit.ExitCode !== 0 || reports.length !== 3 ||
            operations.size !== 3 || !['optimize', 'checkpoint', 'vacuum'].every(operation => operations.has(operation)) ||
            reports.some(report => report.applied !== true || report.error_kind != null || report.error != null || !report.report)) {
          throw new Error('Actual maintenance service failed or produced no structured operation evidence');
        }
        const optimize = object(reports.find(report => report.operation === 'optimize')?.report);
        if (wave === 1 && (number(optimize.files_added) === 0 || number(optimize.files_removed) <= number(optimize.files_added))) {
          throw new Error('Initial real OPTIMIZE did not prove net compaction; the fixed fixture was insufficient');
        }
        completed = true;
      } catch (error) {
        failure = error;
        throw error;
      } finally {
        try {
          const container = await this.ownedContainer(identity);
          if (!completed && container.state.Running === true) {
            // A Docker CLI exit/timeout is not proof that its daemon-owned workload stopped.
            await runOwnedProcess('docker', ['stop', '--time', '300', container.id], {
              cwd: this.options.workspace, env: this.options.env, timeoutMs: 310_000,
            });
          }
          const { state } = await this.ownedContainer(container.id);
          if (state.Running !== false) throw new Error('Owned maintenance container is still live');
          // Keep stopped failures for private diagnosis; never publish raw endpoint/credential text.
          if (completed) await this.docker(['rm', container.id]);
        } catch (cleanupError) {
          throw failure ? new AggregateError([failure, cleanupError], 'Maintenance operation and owned cleanup failed') : cleanupError;
        }
      }
      await this.snapshot(pool, wave, 'after', deadline);
    }
  }
}
