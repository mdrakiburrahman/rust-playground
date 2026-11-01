# experiment-kit

| Export | Purpose |
| :--- | :--- |
| `runExperiment(experiment, options)` | Run typed stages sequentially under a required deadline; always emit a final report. |
| `Stage<TRow>` / `StageResult<TRow>` | `run(ctx)` returns `{ status, summary, rows? }`; `ctx.signal`, `ctx.deadline`, `ctx.record()`, `ctx.flush()`. |
| `runProcess(cmd, argv, { signal })` | Spawn without a shell in its own process group; on abort SIGTERM then SIGKILL to the group; resolves after the group is gone and stdio closed. Captures up to 32 MiB per stream (`maxOutputBytes`). |
| `renderTable(columns, rows, options?)` | GitHub Markdown table from caller-defined `Column<TRow>`s. |
| `toSafeJsonValue` / `stringifySafeJson` | Validate and redact JSON; reject non-finite, `undefined`, bigint, cycles, non-plain objects. |
| `writeFileAtomic(file, text)` | Temp file + fsync + rename. |
| `redactString` / `isSecretKey` | Mask credential shapes, secret-named keys and caller `secrets`. |

| Status | Meaning |
| :--- | :--- |
| `passed` | The hypothesis held. |
| `finding` | A definite, reportable observation. |
| `inconclusive` | Aborted, deadline hit, or stages not run. |
| `failed` | Stage error, invalid output, unsettled stage, cleanup or evidence failure. |

The experiment status is the worst stage status: `failed` > `inconclusive` > `finding` > `passed`.

| Output in `outDir` | Written |
| :--- | :--- |
| `evidence.jsonl` | Append-only while running, fsynced every `syncEvery` lines (default 1), bounded (`maxRecords` 10000, `maxBytes` 16 MiB), then an `evidence.truncated` marker. |
| `report.json` | Atomically at the end, `schemaVersion: 1`. |
| `report.md` | Atomically at the end; the same text goes to `options.output` (default stdout). |

```ts
import { runExperiment, type Experiment } from './experiment.js';

interface Row { readonly probe: string; readonly latencyMs: number | null }

const experiment: Experiment<Row> = {
  id: 'probe-latency',
  title: 'Probe latency',
  columns: [
    { header: 'Probe', value: (r) => r.probe },
    { header: 'Latency', unit: 'ms', value: (r) => r.latencyMs, align: 'right' },
  ],
  stages: [{
    name: 'measure',
    timeoutMs: 30_000,
    run: async (ctx) => {
      ctx.record('start', { probes: 1 });
      const latencyMs = await measure(ctx.signal); // must honour ctx.signal
      return { status: 'passed', summary: 'measured', rows: [{ probe: 'a', latencyMs }] };
    },
  }],
  cleanup: async (ctx) => deleteResources(ctx.signal),
};

await runExperiment(experiment, { outDir: 'target/experiments/probe-latency', timeoutMs: 600_000, signal });
```

## Contract

- Cancellation is cooperative. The runner aborts `ctx.signal` and waits up to `settleTimeoutMs` (default 5 s) for the stage promise to settle. It never claims a stage stopped: a stage that does not settle is reported `failed` and unsettled, and cleanup is skipped so it cannot race live effects.
- Cleanup runs after all stages settle, including after failure or cancellation, with its own `cleanupTimeoutMs` signal (default 30 s).
- A `failed` or aborted stage stops later stages; they are listed as `not-run`.
- Rows and evidence must be JSON-safe. Use `null` for missing values; tables show `n/a` (configurable), never `0`. Units appear only when the column supplies `unit`.
- Cells escape `\`, `|` and newlines (`<br>`).
- `outDir` must not hold a previous run's `evidence.jsonl`.
- `ctx.flush()` waits until every queued line is written and fsynced. After the run closes the log, `record()` throws and `flush()` rejects with `EvidenceClosedError`.
- `runProcess` on Linux confirms the child leads its own process group before it signals the group. Group members left after a normal exit are also terminated (`strayProcessesKilled`). A descendant that calls `setsid` itself escapes the group.
- `runProcess` output is not redacted. On Windows it kills only the direct child, not the process tree.

```bash
npx nx run experiment-kit:verify   # lint, typecheck, test
```
