# Lane P amendment A1: `encoding=no_dictionary` is probe-only

Pre-registered at 2026-11-25T07:20:48Z, before the second pilot (`pilot2`), the grid, headroom, selection, registration or confirmation existed. No screening result after pilot 1 informed it.

## Decision

- Remove `encoding=no_dictionary` from the screening levels. The pilot, the grid and the heatmap use `screenedFactors()`, which drops `SCREEN_EXCLUDED = { encoding: ['no_dictionary'] }`.
- Keep the arm in the readiness probe (`probePlan` uses `factors()`). `screen-probe.json` is unchanged at `7f8e3733318cee43d2a0bbecd13ed1564752eb5c46758d87203b0d250b01008f`.
- Report the arm as a standalone finding from the readiness probe and pilot 1. It is not eligible for the grid, selection or the A/B/A.
- Unchanged:
  - all statistical gates (effect 1.05, t 2.776, bytes ratio 1.10, bookend drift 0.05, n=5, headroom above 1.5x);
  - the duration admission rule `|seconds - duration| <= max(1 s, 1% of duration)`;
  - the runner (`score.ts`, `measure.ts`, `audit.mjs`, `policy.mjs`, `base.yaml`), so the runtime harness identity is unchanged;
  - the collector, every knob and every default.

## Plans

| Plan | Before A1 | After A1 |
| --- | --- | --- |
| pilot | `8d42e83d5d71c0b257bb6e13eb28411c8cea847793ac02e5ed2f90445c84abd2`, 21 cells | `2f99b81dfc8f4759e2e0e6be65a856dbdd7e3e47f6d9e7b52225079159f130d8`, 20 cells |
| probe | `7f8e3733…`, 29 cells | unchanged |
| headroom | `c13c7161…`, 3 cells | unchanged |

The new pilot differs from the old only by the missing `encoding-no_dictionary` cell. Every other field, cell and the cell order are byte-identical (checked by parsing both plans).

## Evidence (pilot 1, results `11c4c3d854d893f15d7fc45789ade69cf4bf9c52f88a81b9cffd7dd72ee82228`)

Pilot 1 completed all 21 rows. Grid generation then refused admission with `encoding-no_dictionary: measured window does not match the planned duration` (48.854 s against 45 s).

| Measure | base | encoding-no_dictionary | base-end |
| --- | --- | --- | --- |
| measured window (s) | 45.022 | **48.854** | 44.995 |
| boundary scan lag start / end (ms) | 19 / 41 | 15 / **3,869** | 26 / 21 |
| committed rows/s | 1,930,184 | 201,220 (**0.105x** the bookend mean) | 1,906,581 |
| Parquet bytes/row | 34.756 | 545.689 (**15.70x**) | 34.756 |
| pgscan_direct delta in window | 0 | **1,350,265** | 0 |
| MemAvailable at window end (kB) | 47,199,168 | **38,397,888** | 46,420,052 |
| collector cores, user / system | | 1.27 / **8.21** | |

The pressure-sample lags in that cell were 515, 14, 6, 633, 23, 1503, 29, 757 and 6,304 ms. The last sample finished after the planned window end, which delayed the end snapshot. Every lag in the base cell was at most 41 ms.

Interpretation: plain encoding on every column writes 15.7x the bytes. That drives host-wide direct reclaim, and the collector spends most of its CPU in the kernel. The unpinned node runner then misses the window boundary, even past its 2 s fetch timeout. The arm cannot win on speed (0.105x) or bytes (15.7x). Kept in the grid, it would overrun again and make the whole grid inadmissible. Relaxing the duration rule or changing the runner would weaken the gates for every arm, so neither was done.

## Validation

- `node --test test\kit.test.mjs`: 15/15 passed. The grid test asserts the arm is absent from the pilot and grid and present in the probe. `SCREEN_EXCLUDED` refuses an exclusion that no longer matches a factor level.
- `node test\mutate.cjs .`: 16/16 mutants killed, control OK.
- `coverage-map.csv`: `dictionary_enabled` moved from `screened` to `screened_partial`, and the `encoding` reason was updated.
