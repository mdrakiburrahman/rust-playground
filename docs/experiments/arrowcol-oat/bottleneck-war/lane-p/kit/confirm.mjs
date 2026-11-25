// Lane P candidate selection, pre-registration and frozen n=5 A/B/A scoring.
// Offline only: it admits rows written by the generated runner; it never starts a workload.
import fs from 'node:fs';
import path from 'node:path';
import { isDeepStrictEqual as equal } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { validatePlan, HARNESS_FILES, PREPARSER_NOTICE } from './policy.mjs';
import { sha256, planText, insist, syntheticMode, stamp, loadLeaves, screening, headroomPlan, admit, readPhase,
  effects } from './screen-plan.mjs';

export const ANALYZER_FILES = ['confirm.mjs', 'policy.mjs', 'screen-plan.mjs'];
/** Unchanged campaign gates. */
export const GATES = { effect: 1.05, t: 2.776, bytes_ratio: 1.10, abs_A2_over_A1_minus_1: 0.05, n: 5 };
export const HEADROOM_MIN = 1.5;
const here = path.dirname(fileURLToPath(import.meta.url));
const fileSha = file => sha256(fs.readFileSync(file));
const put = (file, text) => fs.writeFileSync(file, text, { flag: 'wx' });
const load = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const mean = xs => xs.reduce((a, b) => a + b, 0) / xs.length;
export const analyzerHashes = () => Object.fromEntries(ANALYZER_FILES.map(f => [f, fileSha(path.join(here, f))]));

function writePinned(file, data) {
  put(file, JSON.stringify(data, null, 2) + '\n');
  put(file + '.sha256', `${fileSha(file)}  ${path.basename(file)}\n`);
}
export function readPinned(file) {
  const bytes = fs.readFileSync(file);
  insist(fs.readFileSync(file + '.sha256', 'utf8') === `${sha256(bytes)}  ${path.basename(file)}\n`,
    `${file} does not match its .sha256 sidecar (changed after pinning)`);
  return { data: JSON.parse(bytes.toString('utf8')), sha: sha256(bytes) };
}
const modeMatches = (data, synthetic, what) => insist((data.synthetic_non_performance === true) === synthetic,
  synthetic ? `${what} is real evidence; synthetic mode refuses it` : `${what} is synthetic test data, not evidence`);

export const confirmPlan = (common, env, leaves) => validatePlan({ duration: 180, warmup: 30, common,
  cells: Array.from({ length: GATES.n }, (_, i) => ['a1', 'b', 'a2'].map(x =>
    ({ id: `t${i + 1}-${x}`, grid: 'writer-confirm', x, y: String(i + 1), env: x === 'b' ? env : {} }))).flat() }, leaves);

const summary = (phase, r, env, control) => ({
  phase, cell: r.cell, env, committed_rps: r.committed_rps, rps_ratio: r.committed_rps / control.committed_rps,
  bytes_per_row: r.bytes_per_row, bytes_ratio: r.bytes_per_row / control.bytes_per_row,
  user_ns_per_row: r.physical.user_ns_per_row, system_ns_per_row: r.physical.system_ns_per_row,
  files_per_commit: r.stats.files_per_commit, pressure_samples: r.physical.pressure_samples ?? null,
  degraded_samples: r.physical.degraded_samples ?? null,
});

export function select({ common, leaves, synthetic, pilotPlanFile, pilotRows, gridPlanFile, gridRows, conditionalPlanFile,
  conditionalRows, selectionOut, headroomOut }) {
  const s = screening({ common, leaves, synthetic, pilotPlanFile, pilotRows, gridPlanFile, gridRows, conditionalPlanFile, conditionalRows });
  const phases = [['pilot', s.pilot], ['grid', s.grid], ...(s.conditional ? [['conditional', s.conditional]] : [])];
  const arms = phases.flatMap(([name, p]) => effects(p).map(e => summary(name, p.rows.find(r => r.cell === e.cell), e.env, p.control)));
  insist(arms.length, 'No admitted treatment arms');
  const speed = [...arms].sort((a, b) => b.rps_ratio - a.rps_ratio || `${a.phase}/${a.cell}`.localeCompare(`${b.phase}/${b.cell}`))[0];
  const everything = phases.flatMap(([name, p]) =>
    p.rows.map(r => summary(name, r, p.plan.cells.find(c => c.id === r.cell).env, p.control)));
  const smallest = [...everything].sort((a, b) => a.bytes_per_row - b.bytes_per_row || b.rps_ratio - a.rps_ratio ||
    `${a.phase}/${a.cell}`.localeCompare(`${b.phase}/${b.cell}`))[0];
  const headroom = planText(headroomPlan(common, speed.env, 'candidate', leaves));
  put(headroomOut, headroom);
  writePinned(selectionOut, {
    ...stamp(synthetic), screening_only: true,
    notice: 'Screening uses one arm per cell and selects candidates only; a claim needs the registered n=5 A/B/A.',
    preparser_notice: PREPARSER_NOTICE,
    selection_rule: 'speed = highest committed_rps ratio to its own phase bookend mean (ties by phase/cell); ' +
      'smallest = lowest raw bytes_per_row over every admitted row, bookends included (ties by higher rate ratio)',
    controls: Object.fromEntries(phases.map(([name, p]) => [name, p.control])),
    grid: { x: s.choice.x, y: s.choice.y, scores: s.choice.scores, skipped: s.skipped }, conditional_groups: s.groups,
    speed, speed_beats_control: speed.rps_ratio > 1, speed_trades_bytes: speed.bytes_ratio > 1,
    smallest, smallest_throughput_cost: 1 - smallest.rps_ratio,
    identity: s.pilot.cohort.identity, corpus: s.pilot.cohort.corpus, common,
    phases: Object.fromEntries(phases.map(([name, p]) => [name,
      { plan_sha256: p.planSha, rows_sha256: p.rowsSha, bookend_drift: p.drift, end_observed_ms: p.end }])),
    analyzer_files: analyzerHashes(), fastest_headroom_sha256: sha256(headroom),
    screening_end_ms: phases.at(-1)[1].end,
  });
  return { speed, smallest };
}

export function register({ common, leaves, synthetic, selectionFile, defaultPlanFile, defaultRows, candidatePlanFile,
  candidateRows, fixtureDir, planOut, registrationOut }) {
  const { data: s, sha: selectionSha } = readPinned(selectionFile);
  modeMatches(s, synthetic, 'selection');
  insist(equal(s.common, common), 'Selection common settings differ from the base');
  insist(equal(analyzerHashes(), s.analyzer_files), 'Analyzer files changed since selection');
  insist(s.speed_beats_control === true && s.speed.rps_ratio > 1,
    'No screened arm beat its bookend control; an A/B/A would only test a regression');
  const harness = Object.fromEntries(HARNESS_FILES.map(f => [f, fileSha(path.join(fixtureDir, f))]));
  insist(equal(harness, s.identity.harness_files), 'Fixture harness files differ from the screened identity');
  const footer = fileSha(path.join(fixtureDir, 'footer'));
  insist(footer === s.identity.footer_sha256, 'Fixture footer binary differs from the screened identity');
  const cohort = { identity: s.identity, corpus: s.corpus };
  const base = readPhase(defaultPlanFile, defaultRows, headroomPlan(common, {}, 'default', leaves));
  const fast = readPhase(candidatePlanFile, candidateRows, headroomPlan(common, s.speed.env, 'candidate', leaves));
  insist(fast.planSha === s.fastest_headroom_sha256, 'Candidate headroom plan is not the one pinned at selection');
  const room = {};
  let after = s.screening_end_ms;
  for (const [label, phase] of [['default', base], ['candidate', fast]]) {
    const a = admit(phase, { synthetic, footerSha: footer, after, cohort });
    after = a.end;
    const l5 = a.rows[2];
    const ratios = a.rows.slice(0, 2).map(r => ({ level: r.cell, sink: r.diagnostic_rps / l5.committed_rps,
      offered: r.first_attempt_offered_rps / l5.committed_rps }));
    insist(ratios.every(r => Math.min(r.sink, r.offered) > HEADROOM_MIN),
      `Unproven client headroom for ${label}: ${JSON.stringify(ratios)}`);
    room[label] = { ratios, l5_committed_rps: l5.committed_rps, plan_sha256: a.planSha, rows_sha256: a.rowsSha };
  }
  const text = planText(confirmPlan(common, s.speed.env, leaves));
  put(planOut, text);
  writePinned(registrationOut, {
    ...stamp(synthetic), registered_at_utc: new Date().toISOString(), plan_sha256: sha256(text),
    selection_sha256: selectionSha, analyzer_files: analyzerHashes(), footer_binary_sha256: footer, harness_files: harness,
    identity: s.identity, corpus: s.corpus, common, b_env: s.speed.env, b_source: `${s.speed.phase}/${s.speed.cell}`,
    headroom: room, headroom_min: HEADROOM_MIN, headroom_end_ms: after,
    effect: 'B / arithmetic mean(A1,A2) per triple; geometric mean; t on natural logs, df=4',
    gates: GATES, replacements: false,
  });
}

export function score({ common, leaves, synthetic, registrationFile, planFile, resultsFile }) {
  const { data: reg, sha: regSha } = readPinned(registrationFile);
  modeMatches(reg, synthetic, 'registration');
  insist(fileSha(planFile) === reg.plan_sha256 && equal(analyzerHashes(), reg.analyzer_files) && equal(reg.common, common) &&
    equal(reg.gates, GATES), 'Frozen registration mismatch (plan, analyzer, common settings or gates changed)');
  const phase = readPhase(planFile, resultsFile, confirmPlan(common, reg.b_env, leaves));
  const head = { ...stamp(synthetic), registration_sha256: regSha, plan_sha256: phase.planSha, results_sha256: phase.rowsSha };
  try {
    admit(phase, { synthetic, footerSha: reg.footer_binary_sha256, cohort: { identity: reg.identity, corpus: reg.corpus },
      after: Math.max(Date.parse(reg.registered_at_utc) + 1, reg.headroom_end_ms) });
  } catch (error) {
    return { ...head, verdict: 'INCONCLUSIVE', errors: [error.message] };
  }
  const rows = phase.rows;
  const triples = Array.from({ length: GATES.n }, (_, i) => {
    const [a, b, c] = rows.slice(i * 3, i * 3 + 3);
    return { a1: a.committed_rps, b: b.committed_rps, a2: c.committed_rps, ratio: b.committed_rps / mean([a.committed_rps, c.committed_rps]),
      drift: c.committed_rps / a.committed_rps - 1, b_bpr: b.bytes_per_row, a_bpr: mean([a.bytes_per_row, c.bytes_per_row]) };
  });
  const logs = triples.map(x => Math.log(x.ratio)), m = mean(logs);
  const sd = Math.sqrt(logs.reduce((a, v) => a + (v - m) ** 2, 0) / (GATES.n - 1));
  const t = sd ? m * Math.sqrt(GATES.n) / sd : (m > 0 ? Infinity : m < 0 ? -Infinity : 0);
  const effect = Math.exp(m), bytes = mean(triples.map(x => x.b_bpr)) / mean(triples.map(x => x.a_bpr));
  const gates = { drift: triples.every(x => Math.abs(x.drift) <= GATES.abs_A2_over_A1_minus_1), effect: effect >= GATES.effect,
    t: t >= GATES.t, size: bytes <= GATES.bytes_ratio };
  return { ...head, verdict: !gates.drift ? 'INCONCLUSIVE' : Object.values(gates).every(Boolean) ? 'QUALIFIED' : 'NOT_QUALIFIED',
    n: GATES.n, df: GATES.n - 1, gates, triples, effect, t: Number.isFinite(t) ? t : String(t), bytes_ratio: bytes,
    arms: rows.map(r => ({ cell: r.cell, committed_rps: r.committed_rps, bytes_per_row: r.bytes_per_row,
      files_per_commit: r.stats.files_per_commit, user_ns_per_row: r.physical.user_ns_per_row,
      system_ns_per_row: r.physical.system_ns_per_row, degraded_samples: r.physical.degraded_samples ?? null,
      pressure_samples: r.physical.pressure_samples ?? null })) };
}

function main(argv) {
  const usage = [
    'Usage: confirm.mjs select <base.json> <footer.json> <pilot-plan> <pilot-rows> <grid-plan> <grid-rows> <selection-out> <fastest-headroom-out> [<conditional-plan> <conditional-rows>]',
    '       confirm.mjs register <base.json> <footer.json> <selection> <default-headroom-plan> <default-headroom-rows> <candidate-headroom-plan> <candidate-headroom-rows> <fixture-dir> <plan-out> <registration-out>',
    '       confirm.mjs score <base.json> <footer.json> <registration> <plan> <results> <verdict-out>'].join('\n');
  const [action, basePath, footerPath, ...a] = argv;
  insist(action && basePath && footerPath, usage);
  const synthetic = syntheticMode();
  const leaves = loadLeaves(load(footerPath));
  const common = validatePlan({ duration: 5, warmup: 0, common: load(basePath).common,
    cells: [{ id: 'x', grid: 'x', x: 'x', y: 'x', env: {} }] }, leaves).common;
  const ctx = { common, leaves, synthetic };
  if (action === 'select') {
    insist(a.length === 6 || a.length === 8, usage);
    const { speed, smallest } = select({ ...ctx, pilotPlanFile: a[0], pilotRows: a[1], gridPlanFile: a[2], gridRows: a[3],
      selectionOut: a[4], headroomOut: a[5], conditionalPlanFile: a[6], conditionalRows: a[7] });
    console.error(`SPEED ${speed.phase}/${speed.cell} rps_ratio=${speed.rps_ratio.toFixed(4)} bytes_ratio=${speed.bytes_ratio.toFixed(4)}`);
    console.error(`SMALLEST ${smallest.phase}/${smallest.cell} bytes_per_row=${smallest.bytes_per_row} rps_ratio=${smallest.rps_ratio.toFixed(4)}`);
  } else if (action === 'register') {
    insist(a.length === 8, usage);
    register({ ...ctx, selectionFile: a[0], defaultPlanFile: a[1], defaultRows: a[2], candidatePlanFile: a[3], candidateRows: a[4],
      fixtureDir: a[5], planOut: a[6], registrationOut: a[7] });
  } else if (action === 'score') {
    insist(a.length === 4, usage);
    const verdict = score({ ...ctx, registrationFile: a[0], planFile: a[1], resultsFile: a[2] });
    put(a[3], JSON.stringify(verdict, null, 2) + '\n');
    console.error(`VERDICT ${verdict.verdict}`);
  } else throw Error(usage);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main(process.argv.slice(2));
