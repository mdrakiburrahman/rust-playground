# Azure QAP experiment

| Purpose | Contract |
| --- | --- |
| Question | Does maintenance accelerate only new/replacement files, or reingest unchanged data? |
| Prerequisites | Devcontainer, private `.env`, Azure sign-in and the [provisioned Azure sandbox](../../docs/operations/AZURE_SANDBOX.md) |
| Execution | Small deterministic fixtures; no loadtest, router, UI or throughput run |
| Writer | Production `BufferManager -> DeltaPublicationSink -> BatchedTable`, driven in-process rather than through a Flight deployment |
| Isolation | Run-owned database, data/checkpoint containers and reader assignment; shared cluster/account/identity are preserved |
| Evidence | Fsynced JSONL observations, exact Delta oracle, JSON and Markdown reports under `.temp/experiments/azure-qap/` |
| Interpretation | [Measured findings and limits](../../docs/experiments/AZURE_QAP.md); correctness is not proof of no reingestion |

```bash
npx nx run azure-qap-experiment:run -- --config experiments/azure-qap/rapid.json --allow-immediate-vacuum
```

**The rapid profile really VACUUMs fresh tombstones with zero retention.**
It intentionally tests QAP/old-snapshot reader failures, only on experiment
resources. Ordinary maintenance retains its seven-day default.

The runner automatically cleans its manifest-owned resources after settled
work. Interrupted or uncertain publishers block deletion. To retry cleanup,
use the ownership manifest printed with the report; preview first:

```bash
npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json
npx nx run azure-qap-experiment:cleanup -- --manifest <report-directory>/ownership.json --execute
```

| Direct runner exit code | Meaning |
| --- | --- |
| `0` | Passed with sufficient calibrated evidence |
| `1` | Execution or exact-result failure |
| `2` | Inconclusive: required evidence is unavailable |
| `3` | Behavioral finding |
| `130` | Interrupted |

Nx normalizes nonzero runner exits to `1`. Read `report.status` and individual
stages: an overall `inconclusive` report can still contain explicit findings.
