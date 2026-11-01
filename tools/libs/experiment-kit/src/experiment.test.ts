import assert from 'node:assert/strict';
import { readdirSync, readFileSync } from 'node:fs';
import { mkdtemp, readdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  DEFAULT_MAX_OUTPUT_BYTES,
  EvidenceClosedError,
  runExperiment,
  runProcess,
  worstStatus,
  type Experiment,
  type ExperimentReport,
  type Stage,
  type StageContext,
} from './experiment.js';
import type { Column } from './report.js';

interface Probe {
  readonly probe: string;
  readonly latencyMs: number | null;
}

const COLUMNS: readonly Column<Probe>[] = [
  { header: 'Probe', value: (row) => row.probe },
  { header: 'Latency', unit: 'ms', value: (row) => row.latencyMs, align: 'right' },
];

async function scratch(t: test.TestContext): Promise<string> {
  const dir = await mkdtemp(path.join(tmpdir(), 'experiment-kit-test-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return path.join(dir, 'out');
}

function waitForAbort(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    if (signal.aborted) reject(signal.reason);
    signal.addEventListener('abort', () => reject(signal.reason), { once: true });
  });
}

async function evidenceLines(outDir: string): Promise<Array<Record<string, unknown>>> {
  const text = await readFile(path.join(outDir, 'evidence.jsonl'), 'utf8');
  return text
    .split('\n')
    .filter((line) => line !== '')
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function experiment(stages: readonly Stage<Probe>[], cleanup?: Experiment<Probe>['cleanup']): Experiment<Probe> {
  return { id: 'synthetic-probe', title: 'Synthetic probe', stages, columns: COLUMNS, ...(cleanup ? { cleanup } : {}) };
}

test('worstStatus orders failed > inconclusive > finding > passed', () => {
  assert.equal(worstStatus([]), 'passed');
  assert.equal(worstStatus(['passed', 'finding']), 'finding');
  assert.equal(worstStatus(['finding', 'inconclusive', 'passed']), 'inconclusive');
  assert.equal(worstStatus(['inconclusive', 'failed', 'finding']), 'failed');
});

test('success writes evidence, JSON and Markdown identical to console output', async (t) => {
  const outDir = await scratch(t);
  const printed: string[] = [];
  const outcome = await runExperiment(
    experiment([
      {
        name: 'measure',
        run: async (ctx) => {
          ctx.record('sample', { latencyMs: 12 });
          return { status: 'passed', summary: 'ok', rows: [{ probe: 'a', latencyMs: 12 }, { probe: 'b', latencyMs: null }] };
        },
      },
      { name: 'compare', run: async () => ({ status: 'finding', summary: 'b missing latency' }) },
    ]),
    { outDir, timeoutMs: 5_000, output: (md) => printed.push(md) },
  );
  assert.equal(outcome.report.status, 'finding');
  assert.deepEqual(printed, [outcome.markdown]);
  assert.equal(await readFile(outcome.files.markdown, 'utf8'), outcome.markdown);
  const json = JSON.parse(await readFile(outcome.files.json, 'utf8')) as ExperimentReport;
  assert.equal(json.status, 'finding');
  assert.deepEqual(json.rows, [{ probe: 'a', latencyMs: 12 }, { probe: 'b', latencyMs: null }]);
  assert.match(outcome.markdown, /\| Probe \| Latency \(ms\) \|/);
  assert.match(outcome.markdown, /\| b \| n\/a \|/);
  const kinds = (await evidenceLines(outDir)).map((line) => line['kind']);
  assert.deepEqual(kinds, ['experiment.start', 'stage.start', 'sample', 'stage.end', 'stage.start', 'stage.end', 'experiment.end']);
  assert.deepEqual((await readdir(outDir)).sort(), ['evidence.jsonl', 'report.json', 'report.md']);
});

test('stage error fails, skips later stages, runs cleanup and preserves partial evidence', async (t) => {
  const outDir = await scratch(t);
  let cleaned = false;
  const outcome = await runExperiment(
    experiment(
      [
        {
          name: 'first',
          run: async (ctx) => {
            ctx.record('partial', { step: 1 });
            return { status: 'passed', summary: 'ok', rows: [{ probe: 'first', latencyMs: 1 }] };
          },
        },
        {
          name: 'boom',
          run: async (ctx) => {
            ctx.record('before-throw', null);
            throw new Error('exploded');
          },
        },
        { name: 'never', run: async () => ({ status: 'passed', summary: 'unreachable' }) },
      ],
      async () => {
        cleaned = true;
      },
    ),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(outcome.report.status, 'failed');
  assert.deepEqual(outcome.report.stages.map((s) => s.status), ['passed', 'failed', 'not-run']);
  assert.match(outcome.report.stages[1]?.summary ?? '', /stage threw: exploded/);
  assert.equal(cleaned, true);
  assert.equal(outcome.report.cleanup.status, 'ok');
  assert.ok((await evidenceLines(outDir)).some((line) => line['kind'] === 'before-throw'));
  assert.match(outcome.markdown, /\| first \| 1 \|/);
});

test('synchronous throw and invalid StageResult are stage failures', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    experiment([
      {
        name: 'sync-throw',
        run: () => {
          throw new Error('sync');
        },
      },
    ]),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(outcome.report.status, 'failed');

  const outDir2 = `${outDir}-2`;
  t.after(() => rm(outDir2, { recursive: true, force: true }));
  const bad = { status: 'great', summary: 1 } as unknown as { status: 'passed'; summary: string };
  const invalid = await runExperiment(experiment([{ name: 'bad', run: async () => bad }]), {
    outDir: outDir2,
    timeoutMs: 5_000,
    output: () => {},
  });
  assert.equal(invalid.report.stages[0]?.summary, 'stage returned an invalid StageResult');
});

test('caller cancellation propagates; stage settles before cleanup', async (t) => {
  const outDir = await scratch(t);
  const controller = new AbortController();
  const order: string[] = [];
  const outcome = await runExperiment(
    experiment(
      [
        {
          name: 'wait',
          run: async (ctx) => {
            setTimeout(() => controller.abort(new Error('operator cancelled')), 20);
            try {
              await waitForAbort(ctx.signal);
            } finally {
              order.push('stage-settled');
            }
            return { status: 'passed', summary: 'unreachable' };
          },
        },
        { name: 'after', run: async () => ({ status: 'passed', summary: 'unreachable' }) },
      ],
      async (ctx) => {
        order.push(`cleanup:${String(ctx.signal.aborted)}`);
      },
    ),
    { outDir, timeoutMs: 5_000, signal: controller.signal, output: () => {} },
  );
  assert.equal(outcome.report.status, 'inconclusive');
  assert.equal(outcome.report.abortReason, 'operator cancelled');
  assert.deepEqual(outcome.report.stages.map((s) => s.status), ['inconclusive', 'not-run']);
  assert.deepEqual(order, ['stage-settled', 'cleanup:false']);
  assert.match(outcome.markdown, /Aborted: operator cancelled/);
});

test('experiment deadline aborts the running stage and keeps partial rows', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    experiment([
      {
        name: 'slow',
        run: async (ctx) => {
          assert.ok(ctx.deadline > Date.now());
          await waitForAbort(ctx.signal).catch(() => {});
          return { status: 'passed', summary: 'collected 1', rows: [{ probe: 'partial', latencyMs: 3 }] };
        },
      },
    ]),
    { outDir, timeoutMs: 40, output: () => {} },
  );
  assert.equal(outcome.report.status, 'inconclusive');
  assert.match(outcome.report.abortReason ?? '', /experiment deadline exceeded 40ms/);
  assert.match(outcome.report.stages[0]?.summary ?? '', /partial result: collected 1/);
  assert.deepEqual(outcome.report.rows, [{ probe: 'partial', latencyMs: 3 }]);
});

test('stage timeout aborts only that stage and stops later stages', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    experiment([
      { name: 'bounded', timeoutMs: 30, run: async (ctx) => waitForAbort(ctx.signal) },
      { name: 'next', run: async () => ({ status: 'passed', summary: 'unreachable' }) },
    ]),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.deepEqual(outcome.report.stages.map((s) => s.status), ['inconclusive', 'not-run']);
  assert.match(outcome.report.stages[0]?.summary ?? '', /stage bounded timeout exceeded 30ms/);
  assert.equal(outcome.report.abortReason, null);
});

test('stage ignoring abort is reported unsettled and cleanup is skipped', async (t) => {
  const outDir = await scratch(t);
  let cleaned = false;
  const outcome = await runExperiment(
    experiment(
      [
        {
          name: 'stubborn',
          run: () => new Promise((resolve) => setTimeout(() => resolve({ status: 'passed', summary: 'late' }), 400)),
        },
      ],
      async () => {
        cleaned = true;
      },
    ),
    { outDir, timeoutMs: 20, settleTimeoutMs: 30, output: () => {} },
  );
  assert.equal(outcome.report.status, 'failed');
  assert.equal(outcome.report.stages[0]?.settled, false);
  assert.equal(outcome.report.cleanup.status, 'skipped');
  assert.equal(cleaned, false);
  assert.match(outcome.markdown, /did not settle after abort/);
});

test('cleanup failure and cleanup timeout fail the experiment', async (t) => {
  const outDir = await scratch(t);
  const failing = await runExperiment(
    experiment([{ name: 'ok', run: async () => ({ status: 'passed', summary: 'ok' }) }], async () => {
      throw new Error('delete failed');
    }),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(failing.report.status, 'failed');
  assert.deepEqual(failing.report.cleanup, { status: 'failed', detail: 'delete failed' });

  const outDir2 = `${outDir}-2`;
  t.after(() => rm(outDir2, { recursive: true, force: true }));
  const slow = await runExperiment(
    experiment([{ name: 'ok', run: async () => ({ status: 'passed', summary: 'ok' }) }], async (ctx) => {
      await waitForAbort(ctx.signal);
    }),
    { outDir: outDir2, timeoutMs: 5_000, cleanupTimeoutMs: 20, output: () => {} },
  );
  assert.equal(slow.report.status, 'failed');
  assert.match(slow.report.cleanup.detail ?? '', /cleanup timeout exceeded 20ms/);
});

test('non-finite rows and evidence are rejected as stage failures', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    experiment([{ name: 'nan-row', run: async () => ({ status: 'passed', summary: 'ok', rows: [{ probe: 'x', latencyMs: Number.NaN }] }) }]),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(outcome.report.status, 'failed');
  assert.match(outcome.report.stages[0]?.summary ?? '', /rows rejected: .*latencyMs: non-finite number NaN/);
  assert.deepEqual(outcome.report.rows, []);

  const outDir2 = `${outDir}-2`;
  t.after(() => rm(outDir2, { recursive: true, force: true }));
  const evidence = await runExperiment(
    experiment([
      {
        name: 'bad-evidence',
        run: async (ctx) => {
          ctx.record('value', { x: Number.POSITIVE_INFINITY });
          return { status: 'passed', summary: 'unreachable' };
        },
      },
    ]),
    { outDir: outDir2, timeoutMs: 5_000, output: () => {} },
  );
  assert.match(evidence.report.stages[0]?.summary ?? '', /stage threw: \$\.data\.x: non-finite number Infinity/);
});

test('column rendering errors still produce a failed final report', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    {
      id: 'render-error',
      title: 'Render error',
      stages: [{ name: 'ok', run: async () => ({ status: 'passed', summary: 'ok', rows: [{ probe: 'x', latencyMs: 1 }] }) }],
      columns: [{ header: 'Ratio', value: (row: Probe) => (row.latencyMs ?? 0) / 0 }],
    },
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(outcome.report.status, 'failed');
  assert.match(outcome.markdown, /Results table could not be rendered:.*non-finite number Infinity/);
  assert.equal(await readFile(outcome.files.markdown, 'utf8'), outcome.markdown);
});

test('evidence is bounded with an explicit truncation marker', async (t) => {
  const outDir = await scratch(t);
  const outcome = await runExperiment(
    experiment([
      {
        name: 'noisy',
        run: async (ctx) => {
          for (let i = 0; i < 20; i += 1) ctx.record('tick', { i });
          return { status: 'passed', summary: 'ok' };
        },
      },
    ]),
    { outDir, timeoutMs: 5_000, evidence: { maxRecords: 5 }, output: () => {} },
  );
  const lines = await evidenceLines(outDir);
  assert.equal(outcome.report.evidence.records, 5);
  assert.equal(outcome.report.evidence.truncated, true);
  assert.ok(outcome.report.evidence.dropped >= 18);
  assert.equal(lines.length, 6);
  assert.equal(lines.at(-1)?.['kind'], 'evidence.truncated');
  assert.match(outcome.markdown, /Evidence truncated/);
});

test('credentials never reach evidence, JSON or Markdown', async (t) => {
  const outDir = await scratch(t);
  const secret = 'hunter2-super-secret';
  const outcome = await runExperiment(
    experiment([
      {
        name: 'leaky',
        run: async (ctx) => {
          ctx.record('config', { password: 'p@ss', clientSecret: 'cs', note: `uses ${secret}`, header: 'Bearer abc.def' });
          return {
            status: 'finding',
            summary: `connection AccountKey=abc123;x with ${secret}`,
            rows: [{ probe: `probe ${secret}`, latencyMs: 1 }],
          };
        },
      },
    ]),
    { outDir, timeoutMs: 5_000, secrets: [secret], output: () => {} },
  );
  const all = [
    await readFile(outcome.files.evidence, 'utf8'),
    await readFile(outcome.files.json, 'utf8'),
    await readFile(outcome.files.markdown, 'utf8'),
  ].join('\n');
  for (const leaked of [secret, 'p@ss', '"cs"', 'abc.def', 'abc123']) assert.ok(!all.includes(leaked), leaked);
  assert.ok(all.includes('[REDACTED]'));
});

test('rejects unbounded runs and reuse of an evidence directory', async (t) => {
  const outDir = await scratch(t);
  await assert.rejects(runExperiment(experiment([]), { outDir, timeoutMs: Number.POSITIVE_INFINITY }), RangeError);
  await runExperiment(experiment([]), { outDir, timeoutMs: 1_000, output: () => {} });
  await assert.rejects(runExperiment(experiment([]), { outDir, timeoutMs: 1_000, output: () => {} }), /EEXIST/);
});

test('report write failure still emits the Markdown summary', async (t) => {
  const outDir = await scratch(t);
  const { mkdir } = await import('node:fs/promises');
  await mkdir(path.join(outDir, 'report.json'), { recursive: true });
  await writeFile(path.join(outDir, 'report.json', 'keep'), 'x');
  const printed: string[] = [];
  await assert.rejects(
    runExperiment(experiment([{ name: 'ok', run: async () => ({ status: 'passed', summary: 'ok' }) }]), {
      outDir,
      timeoutMs: 1_000,
      output: (md) => printed.push(md),
    }),
  );
  assert.equal(printed.length, 1);
  assert.match(printed[0] ?? '', /\| ok \| passed \|/);
  assert.ok(!(await readdir(outDir)).some((name) => name.endsWith('.tmp')));
});

test('evidence is fsynced on a bounded cadence and readable while running', async (t) => {
  const outDir = await scratch(t);
  let seenWhileRunning = 0;
  const outcome = await runExperiment(
    experiment([
      {
        name: 'observe',
        run: async (ctx) => {
          for (let i = 0; i < 3; i += 1) ctx.record('obs', { i });
          await ctx.flush();
          seenWhileRunning = (await evidenceLines(outDir)).filter((line) => line['kind'] === 'obs').length;
          return { status: 'passed', summary: 'ok' };
        },
      },
    ]),
    { outDir, timeoutMs: 5_000, output: () => {} },
  );
  assert.equal(seenWhileRunning, 3);
  assert.equal(outcome.report.evidence.syncEvery, 1);
  assert.equal(outcome.report.evidence.syncs, outcome.report.evidence.records);

  const outDir2 = `${outDir}-2`;
  t.after(() => rm(outDir2, { recursive: true, force: true }));
  const batched = await runExperiment(
    experiment([
      {
        name: 'observe',
        run: async (ctx) => {
          for (let i = 0; i < 10; i += 1) ctx.record('obs', { i });
          return { status: 'passed', summary: 'ok' };
        },
      },
    ]),
    { outDir: outDir2, timeoutMs: 5_000, evidence: { syncEvery: 4 }, output: () => {} },
  );
  const records = batched.report.evidence.records;
  assert.equal(records, 14);
  assert.equal(batched.report.evidence.syncs, Math.ceil(records / 4));
});

test('invalid evidence or timing options are rejected before any output', async (t) => {
  const outDir = await scratch(t);
  for (const evidence of [{ syncEvery: 0 }, { syncEvery: 1.5 }, { maxRecords: -1 }, { maxBytes: Number.NaN }]) {
    await assert.rejects(runExperiment(experiment([]), { outDir, timeoutMs: 1_000, evidence }), RangeError);
  }
  await assert.rejects(runExperiment(experiment([]), { outDir, timeoutMs: 1_000, settleTimeoutMs: -1 }), RangeError);
  await assert.rejects(readdir(outDir), /ENOENT/);
});

test('records after the log closes throw EvidenceClosedError', async (t) => {
  const outDir = await scratch(t);
  let saved: StageContext | undefined;
  let lateError: unknown;
  let stageDone: () => void = () => {};
  const finished = new Promise<void>((resolve) => {
    stageDone = resolve;
  });
  const outcome = await runExperiment(
    experiment([
      {
        name: 'late',
        run: async (ctx) => {
          saved = ctx;
          await new Promise((resolve) => setTimeout(resolve, 150));
          try {
            ctx.record('late', { x: 1 });
          } catch (error) {
            lateError = error;
          }
          stageDone();
          return { status: 'passed', summary: 'late' };
        },
      },
    ]),
    { outDir, timeoutMs: 20, settleTimeoutMs: 20, output: () => {} },
  );
  await finished;
  assert.equal(outcome.report.stages[0]?.settled, false);
  assert.ok(lateError instanceof EvidenceClosedError);
  assert.match(String(lateError), /"late" was not recorded/);
  assert.throws(() => saved?.record('again'), EvidenceClosedError);
  await assert.rejects(saved?.flush() ?? Promise.resolve(), EvidenceClosedError);
  assert.ok(!(await evidenceLines(outDir)).some((line) => line['kind'] === 'late'));
});

const node = process.execPath;

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
  } catch {
    return false;
  }
  try {
    const stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
    return stat.slice(stat.lastIndexOf(')') + 2)[0] !== 'Z';
  } catch {
    return process.platform !== 'linux';
  }
}

function groupMembers(pgid: number): number[] {
  return readdirSync('/proc')
    .filter((entry) => /^\d+$/.test(entry))
    .filter((entry) => {
      try {
        const stat = readFileSync(`/proc/${entry}/stat`, 'utf8');
        const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
        return Number(fields[2]) === pgid && fields[0] !== 'Z';
      } catch {
        return false;
      }
    })
    .map(Number);
}

const spawnGrandchild = (grandchildCode: string, stdio: 'inherit' | 'ignore', thenCode: string): string =>
  `const { spawn } = require('node:child_process');` +
  `const gc = spawn(process.execPath, ['-e', ${JSON.stringify(grandchildCode)}], { stdio: '${stdio}' });` +
  `console.log(JSON.stringify({ child: process.pid, grandchild: gc.pid }));` +
  thenCode;

const linuxOnly = { skip: process.platform !== 'linux' };

test('runProcess passes argv without a shell and captures output', async () => {
  const result = await runProcess(node, ['-e', 'console.log(process.argv[1]); console.error("err")', '$(echo hi) | x'], {
    signal: new AbortController().signal,
  });
  assert.equal(result.exitCode, 0);
  assert.equal(result.aborted, false);
  assert.equal(result.stdout, '$(echo hi) | x\n');
  assert.equal(result.stderr, 'err\n');
});

test('runProcess captures 32 MiB per stream by default', async () => {
  assert.equal(DEFAULT_MAX_OUTPUT_BYTES, 32 * 1024 * 1024);
  const result = await runProcess(
    node,
    ['-e', 'const b = Buffer.alloc(1024 * 1024, 120); for (let i = 0; i < 33; i += 1) process.stdout.write(b);'],
    { signal: new AbortController().signal },
  );
  assert.equal(result.exitCode, 0);
  assert.equal(result.stdout.length, DEFAULT_MAX_OUTPUT_BYTES);
  assert.equal(result.stdoutTruncated, true);
});

test('runProcess bounds captured output and reports non-zero exit', async () => {
  const result = await runProcess(node, ['-e', 'process.stdout.write("x".repeat(5000)); process.exit(3)'], {
    signal: new AbortController().signal,
    maxOutputBytes: 100,
  });
  assert.equal(result.exitCode, 3);
  assert.equal(result.stdout.length, 100);
  assert.equal(result.stdoutTruncated, true);
});

test('runProcess abort terminates the child and resolves after it exits', async () => {
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 100);
  const result = await runProcess(node, ['-e', 'setTimeout(() => {}, 60000)'], { signal: controller.signal });
  assert.equal(result.aborted, true);
  assert.ok(result.signal === 'SIGTERM' || result.exitCode !== 0);
  assert.ok(result.durationMs < 5_000);
});

test('runProcess escalates to SIGKILL when the child ignores SIGTERM', { skip: process.platform === 'win32' }, async () => {
  const controller = new AbortController();
  const started = runProcess(
    node,
    ['-e', 'process.on("SIGTERM", () => {}); console.log("ready"); setInterval(() => {}, 1000)'],
    { signal: controller.signal, killGraceMs: 100 },
  );
  setTimeout(() => controller.abort(), 300);
  const result = await started;
  assert.equal(result.aborted, true);
  assert.equal(result.signal, 'SIGKILL');
});

test('runProcess abort kills the whole owned group, including a SIGTERM-ignoring grandchild', linuxOnly, async () => {
  const controller = new AbortController();
  setTimeout(() => controller.abort(), 400);
  const result = await runProcess(
    node,
    ['-e', spawnGrandchild('process.on("SIGTERM", () => {}); setInterval(() => {}, 1000)', 'inherit', 'setInterval(() => {}, 1000)')],
    { signal: controller.signal, killGraceMs: 200 },
  );
  const pids = JSON.parse(result.stdout) as { readonly child: number; readonly grandchild: number };
  assert.equal(result.aborted, true);
  assert.equal(result.signal, 'SIGTERM');
  assert.equal(result.outputClosed, true);
  assert.equal(isAlive(pids.grandchild), false);
  assert.deepEqual(groupMembers(pids.child), []);
});

test('runProcess terminates stray group members left after a normal exit', linuxOnly, async () => {
  const result = await runProcess(
    node,
    ['-e', spawnGrandchild('setInterval(() => {}, 1000)', 'ignore', 'process.exit(0)')],
    { signal: new AbortController().signal, killGraceMs: 200 },
  );
  const pids = JSON.parse(result.stdout) as { readonly child: number; readonly grandchild: number };
  assert.equal(result.exitCode, 0);
  assert.equal(result.aborted, false);
  assert.equal(result.strayProcessesKilled, true);
  assert.equal(isAlive(pids.grandchild), false);
  assert.deepEqual(groupMembers(pids.child), []);
});

test('runProcess without strays reports a clean exit', async () => {
  const result = await runProcess(node, ['-e', 'process.exit(0)'], { signal: new AbortController().signal });
  assert.equal(result.strayProcessesKilled, false);
  assert.equal(result.outputClosed, true);
});

test('runProcess rejects spawn errors, pre-aborted signals and invalid bounds', async () => {
  await assert.rejects(runProcess(node, ['-v'], { signal: new AbortController().signal, maxOutputBytes: -1 }), RangeError);
  await assert.rejects(runProcess(node, ['-v'], { signal: new AbortController().signal, killGraceMs: Number.NaN }), RangeError);
  await assert.rejects(runProcess('definitely-not-a-command-xyz', [], { signal: new AbortController().signal }), /ENOENT/);
  await assert.rejects(runProcess(node, ['-v'], { signal: AbortSignal.abort(new Error('stop')) }), /aborted before spawn: stop/);
});
