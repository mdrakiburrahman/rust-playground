# Pressure-attested QAP screening

| Item | Contract |
| --- | --- |
| Opt-in | Append `--pressure-attestation /absolute/private/attestation.json` to the [normal runner command](qap-README.md). No ambient or candidate-controlled mode switch. |
| Attestation | Canonical JSON matching `PressureAttestation` in `qap-contract.ts`; supply only through the trusted controller. Keep real observations in ignored private output. |
| Admission | At least five samples spanning40s with pending work; all samples attest QAP Total4/Consumed4/Remaining0 at two four-core nodes, cap100/coefficient0.5. |
| Candidate bounds | Existing eight-proposal/node and64-vCPU bounds; policy is `null`, cap100/coefficient0.5, or cap100/coefficient1. Nothing else. |
| Matched policy pair | Identical complete cluster settings, explicit coefficient0.5 control and coefficient1 treatment. `null` is not a control. |
| Fitness | Baseline10; up to two matched-core budgets15 each; one fixed/autoscale pair15; one matched policy pair40; plus5/proposal count. Experimental-design coverage only. |
| Private checks | Each accepted pressure evaluation generates36 fresh rejection cases, including disallowed coefficients. |
| Exit gate | A successful run must contain an evolved plan with baseline, topology, autoscale and explicit policy comparisons. |

The attestation is a controller assertion about its independently retained
observations, **not cryptographic proof of a service response**. The trusted
caller authenticates its origin. The runner rejects redirected, linked,
executable or noncanonical attestation files, freezes a read-only copy outside
candidate workspaces, and binds its SHA256 in the evaluator manifest and
receipts. Missing, altered, inconsistent or insufficient evidence fails closed.
The evolution agent receives only the approved bounds and comparison objective;
it never receives the private observation file or any new cloud capability.

Slot saturation with pending files is sufficient to justify a controlled test,
not to predict an improvement. Doubling the coefficient changes the public
nominal-capacity formula; it does not establish a measured latency or throughput
gain. Only the parent may mutate/read back supported policy fields, preserve
other capacity-policy sections, restore state, and qualify the unchanged
600-second producer workload with full acceleration and terminal parity.

The original topology-only YAML, seed, proposal matrix, run proof and archived
receipts remain separate. Pressure scores must not be compared with the earlier
topology scoring scale, or with any live p95-derived fitness.
