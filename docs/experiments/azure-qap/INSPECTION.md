# Bounded QAP inspection

| Phase | Behavior |
| --- | --- |
| Default | Trials finish, run-owned tables/blobs are cleaned, artifacts copied, owned Job/ConfigMap/Secret deleted |
| `--keep-running` | Complete trials and read telemetry plus a detected reload control are required; no more maintenance or fixture mutation |
| Inspection | Every 30s the workload reads QAP details/statistics and verifies full row parity against each retained Delta snapshot; uncached queries use the existing federated Azure client and token renewal |
| Handoff | After the first verified observation, AKS copies `report.json`, `report.md`, `evidence.jsonl`, saved queries and `kubernetes-job.json`, then returns; cleanup is explicitly `retained`, not verified |
| Bound | `--inspection-duration` accepts 1m–1h, default 1h, beginning after trials; logs/report/receipt print the UTC deadline |
| Stop | Deadline, TERM or a failed health/parity read stops inspection and runs existing scoped table/blob cleanup; failures are logged and recorded |
| Artifact holder | After cleanup only, the completed workload stays available for copying artifacts; that holder is **not** an active experiment |

## Bug-bash commands

Run inside the Linux devcontainer. The existing rehearsal prerequisites still
apply: a clean expected SHA, private `.env` and authorized sandbox stamp, and a
new absolute evidence directory outside the checkout. **`run` provisions a new
sandbox stamp twice and publishes its runtime image; it is not a read-only
command and must not be pointed at unrelated infrastructure.**

```bash
npx --no-install tsx tools/scripts/src/qap-rehearsal.ts run \
  --subscription <authorized-subscription-UUID> --expected-sha <full-SHA> \
  --evidence-dir <absolute-new-evidence-dir> --keep-running
```

Add `--inspection-duration 10m` to shorten the observation window. The
devcontainer trial phase retains its normal cleanup; **only the AKS phase**
receives the inspection flags. Rehearsal returns evidence while the Job actively
reads, explicitly skips the absent-Job cleanup assertion and records
`cleanupVerification: retained-read-only-not-cleaned`. Historical trial read-log
rechecks still run; inspection reads happen after those trial windows.

For an already provisioned sandbox and published updated image (no rehearsal
reset/provision), use the experiment directly:

```bash
npx nx run azure-qap-experiment:run -- --kubernetes --keep-running \
  --kubernetes-config infra/kubernetes/azure-qap.yaml \
  --inspection-duration 1h --out .temp/experiments/azure-qap/aks
```

The same flags without `--kubernetes` keep the devcontainer driver attached
through inspection; it does not return an AKS receipt.

## Exact owned cleanup

Use the **printed receipt path**, either the copied rehearsal
`<evidence-dir>/aks/kubernetes-job.json` or the experiment's output receipt.
Run from the same checkout with its private configuration and existing
authenticated kubeconfig. Cleanup does not change Azure accounts, obtain
credentials, redeploy, reset a stamp or delete a namespace.

```bash
npx --no-install tsx tools/scripts/src/qap-rehearsal.ts cleanup \
  --receipt <printed-kubernetes-job.json-path>
```

This is destructive **only to the receipt-owned run**. It verifies the configured
namespace/context, exact Job/ConfigMap/Secret UIDs and labels, Pod UID/Job owner
and live run report before sending TERM to that Pod's workload. It waits up to
6m for the runtime's completion marker, copies final artifacts to a new
`final/` directory beside the receipt, verifies scoped table/blob cleanup and
rechecks object ownership before deleting those three exact objects. Failure
to verify cleanup or ownership leaves the objects intact; no selectors,
prefix-wide deletion or user-supplied generic object names are accepted.

Run cleanup **before the inspection deadline** to reliably collect final
artifacts. Automatic deadline cleanup still removes run tables/blobs, but the
Job's hard active deadline (trial budget + inspection bound + cleanup/startup/
collection allowance) eventually terminates its artifact holder. A terminated
Pod cannot serve `kubectl cp`; final evidence may then be unavailable and the
receipt command fails closed rather than deleting unverifiable objects.
Do not treat the initial retained report as successful cleanup, or observation
reads after trial windows as additional mutation experiments.
