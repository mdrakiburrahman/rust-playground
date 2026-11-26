# Bounded QAP Darwin screening

| Item | Contract |
| --- | --- |
| Purpose | Real Darwin evolution of an informative ADX comparison matrix; **screening only, never Azure latency or throughput evidence**. |
| Candidate | `qap-candidate.json`: canonical sorted-key JSON, one final newline; `CandidatePlan` / `Proposal` in `qap-contract.ts`. Maximum eight unique proposals, eight maximum nodes and 64 engine vCPU including autoscale maximum. |
| Mutable | Cores per node, fixed/optimized-autoscale node counts. `queryAccelerationCapacity` stays `null` until an independently trusted controller establishes capacity pressure. No producer, workload, query, credentials, paths or commands. |
| Fitness | Matched-core topology and matched-peak fixed/autoscale comparison coverage. No modeled or measured latency, cloud cost, SKU availability or throughput. |
| Test | `npx --no-install tsx --test experiments/darwin/qap.test.ts` |
| Type check | `npx --no-install tsc --project experiments/darwin/qap-tsconfig.json` |
| Run | Commit a clean tree, then `npx --no-install tsx experiments/darwin/qap-run.ts /absolute/path/to/darwin out/qap-screening-001`. Requires bootstrap-synced `COPILOT_GITHUB_TOKEN`; never put it in arguments. |
| Evidence | Private ignored output: `result.json`, `receipts.jsonl`, `best-screening-plan.json`, `run.json`, `darwin.log`, and Darwin `state/`. Do not publish raw logs. |
| Limits | Four iterations, serial evaluator, 16 total evaluations, 180s agent/30s evaluator, 20 minute outer wall budget. Refuses runs inside the campaign's final hour (2026-11-27 12:00Z onward). No automatic retries. |

For Enterprise Cloud credentials, set `COPILOT_GH_HOST` to the token's issuing
`*.ghe.com` host (or include it in the bootstrap-synced `.env`). The default is
`github.com`; tokens are never retried against a different identity or host.
Copilot auto-update is disabled so the verified runtime stays pinned.

If the standalone Darwin downloader cannot fetch its pinned Copilot CLI runtime,
download that exact official release with its published SHA256 checksums, then
set `COPILOT_CLI_PATH=/absolute/path/to/copilot` on the run command. The runner
validates this trusted executable path and records its hash; it does not expose
the path in candidate files. Do not substitute an unverified runtime.

The tested staticx Darwin0.3.0 binary also needs ABI-compatible SSL, SQLite and C++
libraries on Ubuntu24.04: its bundled glibc cannot load the host versions.
`DARWIN_LIBRARY_PATH=/absolute/private/library/directory` permits the trusted
controller to provide `libssl.so.3`, `libcrypto.so.3`, `libsqlite3.so.0` and `libstdc++.so.6`.
The runner validates containment and records/rechecks every shared-library hash. Use
official distribution packages extracted under ignored `out/`; **never replace
host libraries**. This is a runtime dependency workaround, not a candidate knob.

## Boundary and receipts

The runner materializes a candidate-only Git seed from committed HEAD. It never
clones Darwin or changes the working tree. Evolution agents see only
`candidate.json`, not the workload, scorer, `.env`, other repository files or
receipts. Darwin's documented workspace confinement remains enabled. A fresh
private HOME and an explicit environment allowlist exclude Azure credentials;
the Copilot authentication token is passed only to the Darwin driver, never
serialized or included in prompts. Darwin must be a trusted executable.

The absolute evaluator command is supplied by the outer runner, not the candidate.
The runner compiles the evaluator and contract into a read-only external directory,
hash-binds them in a manifest and freezes the command configuration. Every
evaluation checks code hashes, wall/count budgets and the entire candidate tree.
Only `.git` metadata and a bounded non-executable regular `candidate.json` are
allowed; symlinks, hardlinks, extra files, schema extensions and noncanonical JSON
fail closed. The evaluator never executes or imports candidate content. Fresh
private randomized rejection cases exercise admission on each accepted evaluation.
These are policy-validation cases, not secret workloads or Azure measurements.
The serial Darwin scheduler is required; this is not a multi-writer receipt ledger.

This boundary relies on the trusted Darwin runner's confinement, not Unix mode
bits alone: a hostile same-UID process outside that confinement can change files.
No claim of OS isolation or a sandbox for arbitrary candidate programs is made.
The only public serialized candidate input is numeric/boolean/null JSON.

`@RESULT` / JSONL receipts have `kind:"screening"`, `qualified:false`, integer
`valid`, finite `combined_score`, evaluation count and trusted manifest hash.
Accepted receipts additionally contain the validated plan, its canonical SHA256,
individual proposal hashes, coverage and fresh-private-case evidence. Invalid
candidate receipts score zero. Trusted-runner integrity failures exit nonzero
without manufacturing a fitness record. `result.json` binds binary, seed commit,
repository commit, logs and receipts; a successful run requires at least two
distinct accepted plans, not just scoring the initial seed.

## Trusted live adapter

`assessLiveObservation(observation, proposal, workloadSha256)` is an offline
adapter for the parent's **trusted** live controller; Darwin never supplies or
signs live observations. `LiveObservation` is the exact schema in
`qap-contract.ts`, with all fields required and unknown fields rejected.
`proposalHash()` uses SHA256 of recursively key-sorted JSON (without trailing
newline). SHA256 binds identities, but does not authenticate a dishonest producer
of a receipt; the caller must protect the receipt provenance.

The adapter requires 600 seconds, at least 6.36B durable rows, minimum-window
durable rate at least 10.6M/s, unchanged producer, all measured cohorts covered,
conservative p95 upper-bound latency, no growing steady-state backlog, exact
terminal parity, complete telemetry, verified service SKU, and both configured
and observed engine/node limits. Successful live fitness is `1/(1+p95UpperMs)`;
it is separate from incomparable screening coverage scores. Missing or censored
latency cannot pass. The parent defines and verifies windowing, full content
parity, cohort provenance, workload hash, and pressure evidence; row counts alone
cannot establish exact contents or an unchanged producer.

## Public semantics

| Reference | Interpretation |
| --- | --- |
| [Query acceleration policy](https://learn.microsoft.com/en-us/kusto/management/query-acceleration-policy?view=azure-data-explorer) | Caches external Delta data; not producer ingestion. MaxAge is fallback age, not refresh cadence. |
| [Capacity policy](https://learn.microsoft.com/en-us/kusto/management/capacity-policy?view=azure-data-explorer) | QueryAccelerationCapacity is `min(cap, nodes * max(1, coresPerNode * coefficient))`; **do not subtract an ingestion admin node**. |
| Baseline supplied by controller | E4ads_v5, two nodes, optimized autoscale2..4; cap100/coefficient0.5 gives four operations at two nodes. |
| Capacity proposals | Schema supports only the two documented fields, conservatively bounded cap1..100/coefficient0.1..1. They remain disabled in screening. Service support/readback is the parent's responsibility. |

This standalone lane deliberately does not change the existing ten-lane manifest
or the non-qualifying throughput/compression contracts. The parent owns all Azure
writes, serial campaign execution, restoration, SKU discovery and qualification.
