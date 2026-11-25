# Lane P phase log (pre-registered rerun rule)

Written 2026-11-25T07:57:40Z (file mtime), before pilot3 was submitted at about 07:57:51Z. The first staged copy (sha256 `47f0e993…`) misstated the time as 08:05Z; only this line differs.

## Phase attempts

| Phase | Plan sha256 | Rows | Results sha256 | Admission |
| --- | --- | --- | --- | --- |
| pilot (1) | `8d42e83d…` (21 cells) | 21/21 | `11c4c3d8…` | Refused: `encoding-no_dictionary` window 48.854 s vs 45 s. This led to amendment A1. |
| pilot2 | `2f99b81d…` (20 cells) | 20/20 | `aa3e1b52874fd26ce446a235685e489718bc381aadf82d153d4a2aa3c8ec7706` | Refused: `bookend drift -0.0847 exceeds 0.05` (base 2,054,918 rows/s, base-end 1,880,901 rows/s). |
| pilot3 | `2f99b81d…` (byte-identical) | 20/20 | `fbaa026a5cc98b373e4a895eef54d826c68d922c500dcfb89b54e57458048d0b` | Admitted (base 2,005,402 rows/s, base-end 2,078,937 rows/s, drift +3.67%). This is the screening pilot. Grid: codec x bloom, plan `e1484ce1fa70ea90609488529d5dd1b970b9f3a6a74ef1e00275d6f0d85fd2b1`. |
| grid (1) | `e1484ce1…` (15 cells) | 15/15 | `f32a32facde3e403c0261617818de64cbf1f3129d60a481b6a3570f27d10492d` | Admitted at heatmap (base 2,054,872, base-end 1,980,600 rows/s, drift -3.61%). Job `c1281e233438`. |
| cond (1) | `e91cce7b87238156dc7b9ac6c730c659ad7cb39aaaadf3f5eaee2b65daf893f2` (11 cells) | 11/11 | `443e88550a9e5c76c3d97b3b2c4e3ecf3a986f91dd4befe343082bfdc02d4625` | Refused at selection (job `ca01d39813b7`): `bookend drift -0.1819 exceeds 0.05` (base 2,178,417, base-end 1,782,183 rows/s). |
| room-default (1) | `c13c7161e2ba9877bdf272b4522dcfa09e7ff8950e8c3f91af78b002499fb9aa` (3 cells) | 3/3 | `3cebf1e318744d37fe4aa9b46dd28e51012c7a4686add2d46cc3baa06a7a296c` | Superseded, not scored: it ran before cond2, and `register` admits headroom only after the selection's screening end. Descriptive: l0 2.200x, l1 2.089x the l5 rate (1,831,688 rows/s). |
| cond2 | `e91cce7b…` (byte-identical) | 11/11 | `039a68f29a8de5593f9928887291af4ffdf7fa3c2652947caeae40d6ab54718f` | Refused at selection (job `a3ed578aae00`, `PHASE_RC cond2=0`, `SELECT_RC=1`): `bookend drift -0.0589 exceeds 0.05` (base 2,104,200, base-end 1,980,292 rows/s). This was the second and final conditional attempt. |

## Screening stopped under rule 3 (2026-11-25T09:37Z)

- cond2 used up the conditional phase's two attempts. Screening therefore stops.
- The chain printed `SCREENING_STOP conditional attempts exhausted` and ran nothing after it: no `select`, room-default2, room-fast, `register` or confirmation.
- The kit refuses selection whenever a triggered conditional phase is missing (`screen-plan.mjs:233`). Bypassing that would mean changing the analyzer, which rule 5 forbids.
- The lane verdict is INCONCLUSIVE. All rankings from pilot3, the grid, cond (1) and cond2 are descriptive only.
- cond2 drift looked like cond (1): `compact_stall` per cell rose from 7,292 to 20,283, and collector system CPU per row climbed to about 2,000 ns. That is consistent with the page-cache hypothesis below but does not prove it.

Decision recorded at 2026-11-25T09:21:06Z (file mtime). That was before cond2's first timed data; its PRE snapshot was at 09:21:46Z in job `a3ed578aae00`. Rule 1 applies. cond2 is the second and final conditional attempt, with plan regenerated deterministically and required byte-identical to `e91cce7b…`. If select admits cond2, room-default2 (byte-identical to `c13c7161…`, second headroom attempt) and room-fast follow, then register. If cond2 is refused, rule 3 stops screening.

## cond (1) drift diagnosis (descriptive only)

- Host compaction pressure rose through the phase. The `compact_stall` delta per cell went from 5,388 at base to 17,588 at base-end, peaking at 23,024. Collector system CPU per row went from 1,088 to 1,797 ns, and user CPU from 3,309 to 3,788 ns. The grid showed the same rise (5,411 to 16,593), but its rows/s drift stayed within 5%.
- Page cache grew from 37 to 41 GB at cell start, and MemFree fell from 6 to 4 GB. The prepared harness deliberately keeps each arm's table on disk until the phase ends, for evidence. Each cell writes about 3 GB. The resulting growth in page cache is a candidate contributor to the late-phase slowdown, but it is not proven. Under rule 5, the harness is not changed mid-screen.

## pilot2 drift diagnosis (descriptive only)

- Late cells ran slower than in pilot 1. In rows/s relative to the bookend mean:
  - bloom-disabled: 1.007 in pilot 1, 0.776 in pilot2;
  - version-v2: 0.942 in pilot 1, 0.699 in pilot2;
  - codec-gzip1: 0.889 in pilot 1, 0.812 in pilot2.
- Memory does not explain it. MemFree at cell start stayed between 3 and 8.5 GB and Cached between 33 and 41 GB. Those late cells had zero direct-reclaim scans.
- Collector user CPU per row rose for work that is identical or cheaper. bloom-disabled used 4,158 ns against base's 3,447 ns. This points to unattributed slowdown below the guest.
- Guest `/proc/stat` showed 0% steal at an idle check, but WSL2 under Hyper-V may not expose steal. The CPU clock held at 2445 MHz (AMD EPYC 7763).
- No process of ours overlapped the phase. The phase wrapper's load gate passed before it started.

## Rule for the remaining phases (fixed before more data)

1. A phase refused by the unchanged admission gates is discarded whole and rerun fresh with the byte-identical plan in a new output directory. Arms are never cherry-picked, replaced or resumed.
2. The pilot gets at most two more attempts: `pilot3`, then `pilot4` only if pilot3 is refused. The first admitted attempt is the screening pilot. Every later phase (grid, conditional, headroom, room-fast) gets at most two attempts.
3. If a phase uses up its attempts:
   - screening stops;
   - no grid, selection or A/B/A is run from inadmissible data;
   - the lane reports rankings from all refused attempts as descriptive, inconclusive evidence.
4. The confirmation runs once, as registered, with no replacements (unchanged kit rule).
5. The gates, plans, harness and binaries stay unchanged.
