# Darwin recipe index

| Item | Current status |
|---|---|
| `darwin.yaml` | External manifest indexing ten complete lane Configs; paths are relative to the manifest. |
| Code lanes and `throughput-config` | Designs only; the pinned evaluator refuses them. |
| `compression-config` | Only Zstd level changes are permitted. One real pair is diagnostic and always returns `valid=0`; it cannot qualify a winner. |
| Command | The trusted runner must supply `/usr/local/bin/arrowcol-darwin-eval <literal-lane>` and own the immutable candidate/seed/evaluator binding. The YAML does not provide that executable. |
| Boundary | The pinned path check restricts files, not production subregions or all unsafe edits within an allowed file. Prompts are not security controls. |
| Validation | `npx --no-install tsx --test experiments/darwin/manifest.test.ts` checks the manifest and flattened settings offline; it does not execute Darwin, install the wrapper, or launch any workload. |

The root uses no shared defaults or include merging. Offline lint rejects unknown, duplicate, missing and redirected lanes, but is not runtime admission; the trusted runner must independently enforce those checks. Metric extraction consumes the trusted terminal `@RESULT` record and rejects malformed, non-finite or inconsistent scores. No tokens or lane clocks are needed for offline checks. The model name and limits are proposed settings, not proof of model availability or authorization to start evolution.

Short screening is not promotion. Future qualification requires at least three matched pairs, at least 150 seconds per local OAT measurement arm, 60 seconds of frozen warmup, independently reconciled evidence and protected holdouts. The outer runner must preserve each lane's original absolute 24-hour deadline across restarts, reserve cleanup and reconciliation time, and refuse a repeat that cannot fit. Per-agent and evaluator timeouts do not enforce that deadline.
