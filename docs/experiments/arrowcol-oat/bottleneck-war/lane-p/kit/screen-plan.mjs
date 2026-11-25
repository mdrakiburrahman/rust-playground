// Lane P screening plans, fail-closed phase admission, conditional probes and heatmaps.
// Offline only: this derives and admits plans and rows; it never starts a workload.
import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { isDeepStrictEqual as equal } from 'node:util';
import { pathToFileURL } from 'node:url';
import { validatePlan, validateWriterConfig, rowProblems, identity, corpus, bookendDrift, DRIFT_BOUND,
  PREPARSER_NOTICE } from './policy.mjs';

export const sha256 = data => createHash('sha256').update(data).digest('hex');
export const planText = plan => JSON.stringify(plan, null, 2) + '\n';
export const insist = (ok, msg) => { if (!ok) throw Error(msg); };
export const TRIGGER_BOUND = 0.05;
const BOOKENDS = ['base', 'base-end'];

export function syntheticMode(env = process.env) {
  const value = env.BWP_SYNTHETIC_NON_PERFORMANCE;
  insist(value === undefined || value === '1', 'BWP_SYNTHETIC_NON_PERFORMANCE must be unset or exactly 1');
  return value === '1';
}
export const stamp = synthetic => synthetic ? { synthetic_non_performance: true,
  synthetic_notice: 'Synthetic harness test data: not a measurement, not performance evidence.' } : {};

export function loadLeaves(footer) {
  insist(Number.isInteger(footer?.row_groups) && footer.row_groups > 0 && Array.isArray(footer.columns) &&
    footer.columns.length > 0 && footer.columns.length % footer.row_groups === 0, 'Bad fixture footer');
  const width = footer.columns.length / footer.row_groups;
  const leaves = footer.columns.slice(0, width).map(c => ({ path: c.path, physical: c.physical }));
  insist(footer.columns.every((c, i) => c.path === leaves[i % width].path && c.physical === leaves[i % width].physical),
    'Row groups disagree on the physical schema');
  insist(new Set(leaves.map(c => c.path)).size === leaves.length, 'Ambiguous physical paths');
  insist(leaves.every(c => ['BYTE_ARRAY', 'INT32', 'INT64'].includes(c.physical)), 'Fixture shape changed');
  return leaves;
}

const f = defaults => ({ writer: { defaults } });
/** The 11 pilot factors. Each non-baseline value is one pilot arm. */
export function factors(leaves) {
  const columns = Object.fromEntries(leaves.map(c => [c.path, {
    encoding: c.physical === 'BYTE_ARRAY' ? 'delta_byte_array' : 'delta_binary_packed',
  }]));
  return {
    encoding: {
      baseline: {}, no_dictionary: f({ dictionary_enabled: false }),
      delta: { writer: { defaults: { dictionary_enabled: false }, columns } },
    },
    codec: {
      baseline: {}, snappy: f({ compression: 'snappy' }), lz4_raw: f({ compression: 'lz4_raw' }),
      zstd1: f({ compression: 'zstd', compression_level: 1 }), zstd9: f({ compression: 'zstd', compression_level: 9 }),
      gzip1: f({ compression: 'gzip', compression_level: 1 }), brotli1: f({ compression: 'brotli', compression_level: 1 }),
    },
    statistics: { baseline: {}, chunk: f({ statistics: 'chunk' }), none: f({ statistics: 'none' }) },
    delta_stats: { baseline: {}, disabled: { stats: [] }, one_column: { stats: ['customer_id'] } },
    bloom: { baseline: {}, disabled: f({ bloom_filter_enabled: false }) },
    version: { baseline: {}, v2: { writer: { writer_version: 'v2' } } },
    page_rows: { baseline: {}, rows8192: { writer: { data_page_row_count_limit: 8192 } } },
    data_page_bytes: { baseline: {}, kib256: f({ data_page_size_limit: 262144 }) },
    dictionary_bytes: { baseline: {}, kib256: f({ dictionary_page_size_limit: 262144 }) },
    write_batch: { baseline: {}, rows8192: { writer: { write_batch_size: 8192 } } },
    row_group: { baseline: {}, rows65536: { writer: { max_row_group_rows: 65536 } } },
  };
}

/** Pre-registered amendment A1 (2026-11-25, before pilot2): levels kept in the readiness probe but not screened.
 * encoding=no_dictionary drove host-wide direct reclaim and overran its measured window in the first pilot; see KIT.md. */
export const SCREEN_EXCLUDED = Object.freeze({ encoding: Object.freeze(['no_dictionary']) });
export function screenedFactors(leaves) {
  const all = factors(leaves);
  for (const [factor, levels] of Object.entries(SCREEN_EXCLUDED)) {
    insist(levels.every(level => level !== 'baseline' && level in (all[factor] ?? {})), 'Stale screening exclusion');
    all[factor] = Object.fromEntries(Object.entries(all[factor]).filter(([level]) => !levels.includes(level)));
  }
  return all;
}

/** Conditional probes run only when a trigger factor moved rate or bytes/row by more than TRIGGER_BOUND in the pilot. */
export const CONDITIONAL = {
  page_index: { triggers: ['statistics'], arms: {
    header_stats: f({ statistics: 'page', write_page_header_statistics: true }),
    offset_index_off: { writer: { defaults: { statistics: 'chunk' }, offset_index_enabled: false } },
    column_index_trunc16: { writer: { column_index_truncate_length: 16 } },
    stats_trunc16: { writer: { statistics_truncate_length: 16 } },
  } },
  bloom_sizing: { triggers: ['bloom'], arms: {
    fpp001: { writer: { columns: { customer_id: { bloom_filter_fpp: 0.01 } } } },
    ndv65536: { writer: { columns: { customer_id: { bloom_filter_ndv: 65536 } } } },
  } },
  v2_ratio: { triggers: ['version', 'codec'], arms: {
    v2_zstd1: { writer: { writer_version: 'v2', defaults: { compression: 'zstd', compression_level: 1 } } },
    v2_zstd1_ratio2: { writer: { writer_version: 'v2',
      defaults: { compression: 'zstd', compression_level: 1, data_page_v2_compression_ratio_threshold: 2 } } },
  } },
  row_group_bytes: { triggers: ['row_group'], arms: { mib64: { writer: { max_row_group_bytes: 67108864 } } } },
};

/** Merges factor values into an environment; leaf options merge per column, Delta statistics may be chosen once. */
export function merge(...values) {
  const writer = {};
  let stats;
  for (const value of values) {
    for (const [key, x] of Object.entries(value.writer ?? {})) {
      if (key === 'defaults') writer.defaults = { ...writer.defaults, ...x };
      else if (key === 'columns') {
        writer.columns ??= {};
        for (const [leaf, options] of Object.entries(x)) writer.columns[leaf] = { ...writer.columns[leaf], ...options };
      } else writer[key] = x;
    }
    if (value.stats) { insist(stats === undefined, 'Two Delta statistics selections in one arm'); stats = value.stats; }
  }
  const env = {};
  if (Object.keys(writer).length) env.ARROWCOL_PARQUET_CONFIG = JSON.stringify(writer);
  if (stats) env.ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS = JSON.stringify(stats);
  return env;
}

const cell = (id, grid, x, y, env) => ({ id, grid, x, y, env });
const control = (id, grid) => cell(id, grid, 'baseline', 'baseline', {});
const order = id => sha256('bw-p-order-v1:' + id);
const permute = cells => [...cells].sort((a, b) => order(a.id).localeCompare(order(b.id)));
function checked(plan, leaves) { validatePlan(plan, leaves); return plan; }
const make = (common, cells, duration, warmup) => ({ duration, warmup, common, cells });

const pilotArms = options => Object.entries(options).flatMap(([factor, values]) => Object.entries(values)
  .filter(([v]) => v !== 'baseline').map(([v, value]) => cell(`${factor}-${v}`, 'screen', v, factor, merge(value))));
const conditionalArms = (groups, grid) => groups.flatMap(group => Object.entries(CONDITIONAL[group].arms)
  .map(([v, value]) => cell(`${group}-${v}`, grid, v, group, merge(value))));

export const pilotPlan = (common, leaves) =>
  checked(make(common, [control('base', 'screen'), ...permute(pilotArms(screenedFactors(leaves))), control('base-end', 'screen')], 45, 20), leaves);
export const probePlan = (common, leaves) => checked(make(common, [control('base', 'probe'),
  ...permute(pilotArms(factors(leaves)).map(c => ({ ...c, grid: 'probe' }))),
  ...permute(conditionalArms(Object.keys(CONDITIONAL), 'probe'))], 15, 10), leaves);
export const headroomPlan = (common, env, label, leaves = null) => checked(make(common, ['l0', 'l1', 'l5'].map(level =>
  cell(level, 'headroom', level, label, { ...env, ...(level === 'l5' ? {} : { BWA_ISOLATION: level }) })), 90, 20), leaves);
export const conditionalPlan = (common, leaves, groups) => checked(make(common, [control('base', 'conditional'),
  ...permute(conditionalArms(groups, 'conditional')), control('base-end', 'conditional')], 45, 20), leaves);

export function gridPlan(common, leaves, x, y) {
  const opts = screenedFactors(leaves), grid = `grid-${x}-${y}`, cells = [], skipped = [];
  for (const [yv, b] of Object.entries(opts[y])) for (const [xv, a] of Object.entries(opts[x])) {
    if (xv === 'baseline' && yv === 'baseline') continue;
    const id = `${x}-${xv}-${y}-${yv}`, env = merge(a, b);
    try {
      if (env.ARROWCOL_PARQUET_CONFIG) validateWriterConfig(env.ARROWCOL_PARQUET_CONFIG, leaves);
      cells.push(cell(id, grid, xv, yv, env));
    } catch (error) { skipped.push({ id, reason: error.message }); }
  }
  return { plan: checked(make(common, [control('base', grid), ...permute(cells), control('base-end', grid)], 45, 20), leaves),
    skipped };
}

export const readRows = file => fs.readFileSync(file, 'utf8').split('\n').filter(line => line.trim()).map((line, i) => {
  try { return JSON.parse(line); } catch (error) { throw Error(`${file}: row ${i + 1} is not JSON (${error.message})`); }
});

/** Reads one phase and requires its plan file to be byte-identical to the deterministic regeneration. */
export function readPhase(planFile, rowsFile, expected) {
  const bytes = fs.readFileSync(planFile);
  insist(bytes.toString('utf8') === planText(expected), `${planFile} is not the deterministic regeneration of this phase`);
  return { plan: expected, planSha: sha256(bytes), rows: readRows(rowsFile), rowsSha: sha256(fs.readFileSync(rowsFile)) };
}

/** Fail-closed admission: complete, ordered, matched, finite, same identity and corpus, and (optionally) drift-checked bookends. */
export function admit(phase, { synthetic, footerSha = null, bookends = false, after = null, cohort = null }) {
  const { plan, planSha, rows } = phase;
  const problems = [];
  if (rows.length !== plan.cells.length) problems.push(`observed ${rows.length}/${plan.cells.length} planned rows`);
  rows.forEach((r, i) => {
    const c = plan.cells[i];
    if (!c) { problems.push(`row ${i + 1}: not in the plan`); return; }
    problems.push(...rowProblems(r, { plan, cell: c, planSha, footerSha, synthetic }));
    if (i > 0 && !(r?.stats?.start_observed_ms >= rows[i - 1]?.stats?.end_observed_ms)) problems.push(`${c.id}: overlaps the previous row`);
  });
  if (after !== null && rows.length && !(rows[0]?.stats?.start_observed_ms >= after)) problems.push('phase starts before its prerequisite ended');
  const runs = rows.map(r => r?.run);
  if (new Set(runs).size !== runs.length) problems.push('run directories are not unique (replayed rows)');
  const reference = cohort ?? (rows[0] && { identity: identity(rows[0]), corpus: corpus(rows[0]) });
  for (const r of rows) {
    if (reference && !equal(identity(r ?? {}), reference.identity)) problems.push(`${r?.cell}: identity differs from the cohort`);
    if (reference && !equal(corpus(r ?? {}), reference.corpus)) problems.push(`${r?.cell}: payload corpus differs from the cohort`);
  }
  let control = null, drift = null;
  if (bookends) {
    if (plan.cells[0]?.id !== BOOKENDS[0] || plan.cells.at(-1)?.id !== BOOKENDS[1]) problems.push('plan lacks base/base-end bookends');
    else if (!problems.length) {
      const [a, b] = [rows[0], rows.at(-1)];
      drift = bookendDrift(a, b);
      if (!(Math.abs(drift) <= DRIFT_BOUND)) problems.push(`bookend drift ${drift.toFixed(4)} exceeds ${DRIFT_BOUND}`);
      control = { committed_rps: (a.committed_rps + b.committed_rps) / 2, bytes_per_row: (a.bytes_per_row + b.bytes_per_row) / 2 };
    }
  }
  insist(!problems.length, `Inadmissible phase (${problems.length} problems): ${problems.join('; ')}`);
  return { ...phase, control, drift, end: rows.at(-1).stats.end_observed_ms,
    cohort: reference };
}

export function effects(phase) {
  return phase.rows.filter(r => !BOOKENDS.includes(r.cell)).map(r => ({
    cell: r.cell, grid: r.grid, x: r.x, y: r.y, env: phase.plan.cells.find(c => c.id === r.cell).env,
    committed_rps: r.committed_rps, rps_ratio: r.committed_rps / phase.control.committed_rps,
    bytes_per_row: r.bytes_per_row, bytes_ratio: r.bytes_per_row / phase.control.bytes_per_row,
    user_ns_per_row: r.physical.user_ns_per_row, system_ns_per_row: r.physical.system_ns_per_row,
    files_per_commit: r.stats.files_per_commit, pressure_samples: r.physical.pressure_samples ?? null,
    degraded_samples: r.physical.degraded_samples ?? null,
  }));
}

export function chooseGrid(pilot, leaves) {
  const score = factor => Math.max(-1, ...effects(pilot).filter(e => e.y === factor).map(e => Math.abs(Math.log(e.rps_ratio))));
  const ranked = Object.keys(factors(leaves)).sort((a, b) => score(b) - score(a) || a.localeCompare(b));
  insist(score(ranked[1]) >= 0, 'Insufficient measured factors');
  return { x: ranked[0], y: ranked[1], scores: Object.fromEntries(ranked.map(k => [k, score(k)])) };
}

export function triggeredGroups(pilot) {
  const moved = new Set(effects(pilot).filter(e => Math.abs(e.rps_ratio - 1) > TRIGGER_BOUND ||
    Math.abs(e.bytes_ratio - 1) > TRIGGER_BOUND).map(e => e.y));
  return Object.keys(CONDITIONAL).filter(group => CONDITIONAL[group].triggers.some(t => moved.has(t)));
}

/** Pilot -> grid -> optional conditional, each regenerated, admitted and chained in time. */
export function screening({ common, leaves, synthetic, pilotPlanFile, pilotRows, gridPlanFile, gridRows, conditionalPlanFile,
  conditionalRows, skipConditional = false }) {
  const pilot = admit(readPhase(pilotPlanFile, pilotRows, pilotPlan(common, leaves)), { synthetic, bookends: true });
  const choice = chooseGrid(pilot, leaves);
  const { plan: expectedGrid, skipped } = gridPlan(common, leaves, choice.x, choice.y);
  const out = { pilot, choice, skipped, groups: triggeredGroups(pilot) };
  if (gridPlanFile === undefined) return out;
  out.grid = admit(readPhase(gridPlanFile, gridRows, expectedGrid),
    { synthetic, bookends: true, after: pilot.end, cohort: pilot.cohort });
  if (skipConditional) return out;
  if (out.groups.length) {
    insist(conditionalPlanFile !== undefined, `Conditional phase is due (${out.groups.join(', ')}) but was not supplied`);
    out.conditional = admit(readPhase(conditionalPlanFile, conditionalRows, conditionalPlan(common, leaves, out.groups)),
      { synthetic, bookends: true, after: out.grid.end, cohort: pilot.cohort });
  } else insist(conditionalPlanFile === undefined, 'No conditional phase was triggered; refusing unplanned conditional rows');
  return out;
}

const esc = s => String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
function svg(title, subtitle, xs, ys, value, better, notice) {
  const cw = 104, ch = 34, left = 150, top = 86 + (notice ? 18 : 0);
  const width = left + xs.length * cw + 24, height = top + ys.length * ch + 52;
  const colour = v => {
    if (!Number.isFinite(v)) return '#f3f2f1';
    const good = better === 'high' ? v >= 1 : v <= 1, t = Math.min(1, Math.abs(Math.log(v)) / Math.log(1.5));
    const mix = (a, b) => Math.round(a + (b - a) * t);
    return good ? `rgb(${mix(255, 0)},${mix(255, 120)},${mix(255, 212)})` : `rgb(${mix(255, 209)},${mix(255, 52)},${mix(255, 56)})`;
  };
  const parts = [`<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" font-family="Segoe UI, sans-serif" font-size="12">`,
    `<rect width="100%" height="100%" fill="#ffffff"/>`,
    `<text x="16" y="26" font-size="16" font-weight="600" fill="#201f1e">${esc(title)}</text>`,
    `<text x="16" y="46" fill="#605e5c">${esc(subtitle)}</text>`];
  if (notice) parts.push(`<text x="16" y="66" fill="#a4262c" font-weight="600">${esc(notice)}</text>`);
  xs.forEach((x, i) => parts.push(`<text x="${left + i * cw + cw / 2}" y="${top - 8}" text-anchor="middle" fill="#323130">${esc(x)}</text>`));
  ys.forEach((y, j) => {
    parts.push(`<text x="${left - 8}" y="${top + j * ch + ch / 2 + 4}" text-anchor="end" fill="#323130">${esc(y)}</text>`);
    xs.forEach((x, i) => {
      const v = value(x, y);
      parts.push(`<rect x="${left + i * cw}" y="${top + j * ch}" width="${cw - 2}" height="${ch - 2}" fill="${colour(v)}" stroke="#e1dfdd"/>`,
        `<text x="${left + i * cw + cw / 2}" y="${top + j * ch + ch / 2 + 4}" text-anchor="middle" fill="#201f1e">${Number.isFinite(v) ? v.toFixed(3) : 'n/a'}</text>`);
    });
  });
  parts.push(`<text x="16" y="${height - 16}" font-style="italic" fill="#605e5c">Ratio to the mean of the grid's base/base-end bookends; baseline x baseline is the bookend mean. n/a = rejected by the pre-parser.</text>`, '</svg>');
  return parts.join('\n') + '\n';
}

export function heatmap(screen, synthetic) {
  const { grid, choice } = screen;
  const opts = screenedFactors([]);
  const xs = Object.keys(opts[choice.x]), ys = Object.keys(opts[choice.y]);
  const byKey = new Map(effects(grid).map(e => [`${e.x}|${e.y}`, e]));
  const at = (x, y, key) => x === 'baseline' && y === 'baseline' ? 1 : byKey.get(`${x}|${y}`)?.[key] ?? NaN;
  const header = ['grid', 'x_factor', 'x', 'y_factor', 'y', 'cell', 'committed_rps', 'rps_ratio', 'bytes_per_row', 'bytes_ratio',
    'user_ns_per_row', 'system_ns_per_row', 'files_per_commit', 'pressure_samples', 'degraded_samples', 'synthetic_non_performance'];
  const lines = [header.join(',')];
  for (const y of ys) for (const x of xs) {
    const e = x === 'baseline' && y === 'baseline'
      ? { cell: 'base+base-end', committed_rps: grid.control.committed_rps, rps_ratio: 1, bytes_per_row: grid.control.bytes_per_row, bytes_ratio: 1 }
      : byKey.get(`${x}|${y}`) ?? { cell: 'rejected_by_preparser' };
    lines.push([`grid-${choice.x}-${choice.y}`, choice.x, x, choice.y, y, e.cell, e.committed_rps, e.rps_ratio, e.bytes_per_row,
      e.bytes_ratio, e.user_ns_per_row, e.system_ns_per_row, e.files_per_commit, e.pressure_samples, e.degraded_samples, synthetic]
      .map(v => v ?? '').join(','));
  }
  const notice = synthetic ? 'SYNTHETIC HARNESS TEST DATA - NOT A MEASUREMENT' : null;
  const subtitle = `${choice.x} (columns) x ${choice.y} (rows); n=1 screening arm per cell, ${grid.plan.duration}s; bookend drift ${(grid.drift * 100).toFixed(2)}%`;
  return {
    csv: lines.join('\n') + '\n',
    rps: svg('Committed rows/s vs defaults', subtitle, xs, ys, (x, y) => at(x, y, 'rps_ratio'), 'high', notice),
    bytes: svg('Parquet bytes/row vs defaults', subtitle, xs, ys, (x, y) => at(x, y, 'bytes_ratio'), 'low', notice),
  };
}

const load = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const put = (file, text) => fs.writeFileSync(file, text, { flag: 'wx' });

function main(argv) {
  const [action, basePath, footerPath, out, ...inputs] = argv;
  const usage = 'Usage: screen-plan.mjs <pilot|probe|headroom|grid|conditional|readiness|heatmap|validate> <base.json> <footer.json> <out> [inputs...]';
  insist(action && basePath && footerPath, usage);
  const synthetic = syntheticMode();
  const base = load(basePath);
  const leaves = loadLeaves(load(footerPath));
  const common = validatePlan({ duration: 5, warmup: 0, common: base.common, cells: [control('x', 'x')] }, leaves).common;
  const arity = n => insist(out && inputs.length === n, usage);
  if (action === 'validate') {
    for (const file of [out, ...inputs]) {
      const plan = validatePlan(load(file), leaves);
      console.log(`OK ${file} sha256=${sha256(fs.readFileSync(file))} cells=${plan.cells.length} duration=${plan.duration} warmup=${plan.warmup}`);
    }
    console.error(PREPARSER_NOTICE);
  } else if (action === 'pilot') { arity(0); put(out, planText(pilotPlan(common, leaves))); }
  else if (action === 'probe') { arity(0); put(out, planText(probePlan(common, leaves))); }
  else if (action === 'headroom') { arity(0); put(out, planText(headroomPlan(common, {}, 'default', leaves))); }
  else if (action === 'grid' || action === 'conditional') {
    arity(2);
    const screen = screening({ common, leaves, synthetic, pilotPlanFile: inputs[0], pilotRows: inputs[1] });
    if (action === 'grid') {
      for (const s of screen.skipped) console.error(`SKIPPED ${s.id}: ${s.reason}`);
      console.error(`GRID ${screen.choice.x} x ${screen.choice.y} scores=${JSON.stringify(screen.choice.scores)}`);
      put(out, planText(gridPlan(common, leaves, screen.choice.x, screen.choice.y).plan));
    } else if (screen.groups.length) {
      console.error(`CONDITIONAL ${screen.groups.join(', ')}`);
      put(out, planText(conditionalPlan(common, leaves, screen.groups)));
    } else console.error('NO_CONDITIONAL_PHASE: no trigger factor moved rate or bytes/row by more than 5%; no plan written');
  } else if (action === 'readiness') {
    arity(2);
    const probe = admit(readPhase(inputs[0], inputs[1], probePlan(common, leaves)), { synthetic });
    put(out, JSON.stringify({ ...stamp(synthetic), readiness: 'PASSED', preparser_notice: PREPARSER_NOTICE,
      plan_sha256: probe.planSha, rows_sha256: probe.rowsSha, identity: probe.cohort.identity, corpus: probe.cohort.corpus,
      arms: probe.rows.map(r => ({ cell: r.cell, env: probe.plan.cells.find(c => c.id === r.cell).env,
        metadata_sha256: r.physical.metadata_sha256, adds_sha256: r.physical.adds_sha256 ?? null,
        stats_properties: r.physical.stats_properties ?? null, max_file_bytes: r.physical.max_file_bytes ?? null })) }, null, 2) + '\n');
  } else if (action === 'heatmap') {
    arity(4);
    const screen = screening({ common, leaves, synthetic, pilotPlanFile: inputs[0], pilotRows: inputs[1],
      gridPlanFile: inputs[2], gridRows: inputs[3], skipConditional: true });
    const maps = heatmap(screen, synthetic);
    put(out + '.csv', maps.csv);
    put(out + '-rps.svg', maps.rps);
    put(out + '-bytes.svg', maps.bytes);
  } else throw Error(usage);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main(process.argv.slice(2));
