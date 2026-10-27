import { execFile } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { appendFileSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { promisify } from 'node:util';
import { runOwnedProcess } from './owned-process.js';
import { collectorSample } from './resource-sampler.js';
import { physicalInventory } from './stress-storage.js';
import type { WaveSample } from './stress-criteria.js';

const execute = promisify(execFile);
const target = 'telemetry.sqlserver.sqlserver_cpu_utilization';
const captureScript = 'import subprocess,sys\nwith open(sys.argv[4],"wb") as out:\n subprocess.run([sys.argv[1],sys.argv[2],sys.argv[3],*sys.argv[5:]],stdout=out,check=True)';

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
    if (/^(?:optimize|checkpoint|vacuum[-_]lite|sqlserver_cpu_utilization|noop|applied|skipped)$/u.test(value)) return value;
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
    if (typeof progress.run_id !== 'string' || progress.run_id !== metadata.run_id ||
        progress.scenario !== 'paired' || Date.now() - number(progress.timestamp) > 5000 ||
        number(progress.timestamp) > Date.now()) throw new Error('Stale/unbound actual driver progress');
    return progress;
  }
  async waitForInput(deadline: number): Promise<string> {
    while (Date.now() < deadline) {
      this.check();
      try {
        const progress = this.progress();
        if (progress.phase === 'measure') return String(progress.run_id);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
      await delay(100);
    }
    throw new Error('Actual source measure phase missed the startup deadline');
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
    await runOwnedProcess('python3', [
      '-c', captureScript, this.options.reader, 'summarize', path.join(this.options.directory, pool), output, '--table', target,
    ], { cwd: this.options.workspace, env: this.options.env, timeoutMs: Math.max(1, deadline - Date.now()) });
    const snapshot = object(JSON.parse(readFileSync(output, 'utf8')));
    const tables = object(snapshot.tables);
    const table = object(tables[target]);
    if (number(table.rows) === 0 || !Array.isArray(table.files) || !table.files.length) {
      throw new Error('Maintenance target has no actual source rows/files');
    }
    const file = table.files.map(object).sort((a, b) => number(b.rows) - number(a.rows))[0];
    if (typeof file.path !== 'string' || path.isAbsolute(file.path) || file.path.includes('\\') ||
        file.path.split('/').some(part => part === '..' || part === '.')) throw new Error('Unsafe source-layout file path');
    const layout = path.join(this.results, `stress-${wave}-${pool}-${phase}-layout.json`);
    await runOwnedProcess('python3', [
      '-c', captureScript, this.options.reader, 'file-metadata',
      path.join(this.options.directory, pool, ...target.split('.'), file.path), layout,
    ], { cwd: this.options.workspace, env: this.options.env, timeoutMs: Math.max(1, deadline - Date.now()) });
    const metadata = object(JSON.parse(readFileSync(layout, 'utf8')));
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
          '--root', `file:///tmp/arrowcol-stress/${pool}/telemetry/sqlserver/`,
          '--table', 'sqlserver_cpu_utilization', '--operation', 'optimize',
          '--operation', 'checkpoint', '--operation', 'vacuum-lite',
          '--checkpoint-every-commits', '1', '--apply', '--interval-secs', '1',
          '--max-idle-backoff-secs', '1', '--max-cycles', '1',
          '--operation-timeout-secs', String(Math.max(1, Math.floor(remaining / 1000))),
          '--listen', '0.0.0.0:9467',
        ], { cwd: this.options.workspace, env: this.options.env, timeoutMs: remaining });
        identity = (await this.ownedContainer(name)).id;
        await runOwnedProcess('docker', ['wait', identity], {
          cwd: this.options.workspace, env: this.options.env, timeoutMs: Math.max(1, deadline - Date.now()),
        });
        const { state: exit } = await this.ownedContainer(identity);
        const logs = await this.docker(['logs', identity]);
        const structured = logs.split('\n').filter(line => line.trim().startsWith('{')).map(line => JSON.parse(line) as unknown);
        appendFileSync(path.join(this.results, 'stress-operations.jsonl'), `${JSON.stringify({
          wave, pool, table: target, operations: ['optimize', 'checkpoint', 'vacuum-lite'],
          finishedAt: Date.now(), exitCode: exit.ExitCode, reports: structured.map(publicReport),
        })}\n`);
        const reports = structured.map(object).filter(report => report.table === 'sqlserver_cpu_utilization');
        const operations = new Set(reports.map(report => report.operation));
        if (exit.Running !== false || exit.ExitCode !== 0 || reports.length !== 3 ||
            operations.size !== 3 || !['optimize', 'checkpoint', 'vacuum_lite'].every(operation => operations.has(operation)) ||
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
