# Azure QAP Kubernetes runtime

The Kustomize base and Azure QAP overlay run the complete experiment runtime in
one bounded Kubernetes Job. The devcontainer runtime remains the default; AKS is
opt-in with `--kubernetes`.

| Input | Purpose |
| --- | --- |
| `azure-qap.example.yaml` | Public template with digest-pinned image shape, pod requests/limits, node selector, and optional kubectl context. |
| `base/` | Namespace, service account, empty environment/secret placeholders, and hardened Job. |
| `overlays/azure-qap/` | Selects the base; the TypeScript runner injects validated namespace, UAMI client ID, per-run names, runtime environment and experiment arguments. |
| `infra/azure/stamp.yaml` | Private subscription/stamp, AKS namespace and service account; never place credentials here or commit it. |
| Scheduling | The bounded sandbox Job explicitly tolerates the baseline system pool's `CriticalAddonsOnly=true:NoSchedule` taint; no taint is removed from the nodes. |

Copy the sample to the ignored `azure-qap.yaml` and replace the image with the
digest returned by the image publisher. Do not use a mutable tag. Resource
requests and limits, node selectors and kubectl context are the only user-facing
pod settings; the runtime derives identity and namespace from generated Azure
configuration and does not copy operator credentials into the pod.

```bash
kubectl kustomize infra/kubernetes/overlays/azure-qap
npx nx run azure-qap-experiment:run -- --kubernetes \
  --kubernetes-config infra/kubernetes/azure-qap.yaml \
  --operations optimize,reload
```

The runner creates a unique Job with `backoffLimit: 0`, streams its logs, copies
reports/evidence/query files to the requested local output directory, then
removes only that run's Job and generated resources. It does not replay a failed
Job. `RELOAD` is required in AKS mode as the positive control. Evidence is scoped to
the shared principal and trial path. Enforced HTTP markers and client/server
request receipts exclude fixture/maintenance reads; incomplete receipts fail
attribution. No generic SDK or empty user-agent is assumed to identify ADX.
The explicit stamp setting selects Log Analytics or diagnostic-storage export.
The report records that source and preserves raw evidence and HTTP receipts.
Missing positive-control reads cannot produce a zero-reread success.

See [AKS QAP operations](../../docs/operations/AKS_QAP.md) for provisioning,
image publishing, safety gates and evidence attribution.
