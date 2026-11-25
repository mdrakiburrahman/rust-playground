// Synthetic, non-performance harness tests for the Lane P screening kit.
// Every row below is generated test data used only to exercise admission and analysis logic.
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { validatePlan, validateWriterConfig, validateStatsColumns, identityProblems, resumeProblems, ENV_RULES,
  WRITER_FIELDS, COLUMN_FIELDS } from '../policy.mjs';
import { sha256, loadLeaves, admit, gridPlan, pilotPlan, probePlan, CONDITIONAL, syntheticMode } from '../screen-plan.mjs';

const KIT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const REFERENCE = path.join(KIT, 'reference');
const BASE = path.join(REFERENCE, 'defaults-rate.json');
const FOOTER = path.join(REFERENCE, 'data', 'defaults-rate', 'defaults-rate-a4az2c', 'footer.json');
const SCRIPTS = ['policy.mjs', 'screen-plan.mjs', 'confirm.mjs', 'prepare-harness.mjs', 'audit.mjs'];
const leaves = loadLeaves(JSON.parse(fs.readFileSync(FOOTER, 'utf8')));
const common = validatePlan({ duration: 5, warmup: 0, common: JSON.parse(fs.readFileSync(BASE, 'utf8')).common,
  cells: [{ id: 'x', grid: 'x', x: 'x', y: 'x', env: {} }] }, leaves).common;
const BYTES_LEAF = leaves.find(l => l.physical === 'BYTE_ARRAY').path;
const INT_LEAF = leaves.find(l => l.physical === 'INT64').path;
const h = s => sha256('synthetic:' + s);
const S = { n: 0 };

function cli(kit, script, args, { real = false } = {}) {
  const env = { ...process.env };
  delete env.BWP_SYNTHETIC_NON_PERFORMANCE;
  if (!real) env.BWP_SYNTHETIC_NON_PERFORMANCE = '1';
  return spawnSync(process.execPath, [path.join(kit, script), ...args], { env, encoding: 'utf8' });
}
const ok = r => { assert.equal(r.status, 0, r.stderr); return r; };
const refused = (r, pattern) => { assert.notEqual(r.status, 0, 'expected refusal'); assert.match(r.stderr, pattern); };
const read = file => fs.readFileSync(file, 'utf8');
const json = file => JSON.parse(read(file));
const jsonl = file => read(file).trim().split('\n').map(line => JSON.parse(line));
const writeRows = (file, rows) => fs.writeFileSync(file, rows.map(r => JSON.stringify(r)).join('\n') + '\n', { flag: 'wx' });
const clone = x => structuredClone(x);
const at = name => path.join(S.run, name);

/** Deterministic synthetic response model. Not a measurement; it only shapes test data. */
function model(env) {
  const w = env.ARROWCOL_PARQUET_CONFIG ? JSON.parse(env.ARROWCOL_PARQUET_CONFIG) : {};
  const d = w.defaults ?? {};
  let rps = 1_000_000, bpr = 100;
  if (d.dictionary_enabled === false) {
    const fallback = Object.values(w.columns ?? {}).some(c => c.encoding);
    rps *= fallback ? 1.25 : 1.2; bpr *= fallback ? 1.05 : 1.6;
  }
  const codec = { snappy: [0.97, 0.7], lz4_raw: [0.98, 0.72], zstd: [0.9, 0.5], gzip: [0.6, 0.45], brotli: [0.7, 0.42] }[d.compression];
  if (codec) { rps *= codec[0]; bpr *= codec[1]; }
  if (d.statistics === 'none') rps *= 1.1;
  if (d.statistics === 'chunk') rps *= 1.04;
  if (env.ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS) rps *= 1.03;
  return { rps, bpr };
}

/** Writes one synthetic runner row per planned cell, sequential in time from clock.t. */
function rowsFor(planFile, clock, { shape = env => model(env), endFactor = 1.01, ident = S.ident, ratio = 2 } = {}) {
  const text = read(planFile), plan = JSON.parse(text), planSha = sha256(text);
  return plan.cells.map(c => {
    const start = clock.t, end = start + plan.duration * 1000;
    clock.t = end + 1000;
    const isolation = c.env.BWA_ISOLATION ?? plan.common.BWA_ISOLATION ?? 'l5';
    let { rps, bpr } = shape(c.env, c);
    if (c.id === 'base-end') rps *= endFactor;
    const stats = { seconds: plan.duration, start_observed_ms: start, end_observed_ms: end };
    const row = { synthetic_non_performance: true, cell: c.id, grid: c.grid, x: c.x, y: c.y, duration: plan.duration,
      warmup: plan.warmup, knobs: { ...plan.common, ...c.env }, run: path.join(S.run, 'out', `${c.id}-${(S.n++).toString(36)}`),
      ...ident, plan_sha256: planSha, isolation_level: isolation, status: 'ok', retry_offered_rps: 0, stats,
      client: [1, 2].map(i => ({ preparation: { payload_corpus_sha256: h('corpus' + i) } })) };
    if (isolation !== 'l5') return { ...row, committed_rps: null, diagnostic_rps: ratio * rps, first_attempt_offered_rps: ratio * rps,
      stats: { ...stats, rows: 0, commits: 0 } };
    const rows = Math.round(rps * plan.duration), bytes = Math.round(bpr * rows), files = 100, commits = 50;
    Object.assign(stats, { rows, commits, files, bytes, committed_rps: rows / plan.duration, files_per_commit: files / commits });
    return { ...row, committed_rps: stats.committed_rps, bytes_per_row: bytes / rows,
      physical: { rows, bytes, files, metadata_sha256: h('meta:' + c.id), user_ns_per_row: 1e10 / stats.committed_rps,
        system_ns_per_row: 1e9 / stats.committed_rps, pressure_samples: 0, degraded_samples: 0 },
      collector: { user_cores: 10, system_cores: 1 } };
  });
}
const confirmShape = ({ b = 1, a2 = 1.002 } = {}) => (env, c) => {
  const m = model(env), i = Number(c.y);
  return { rps: m.rps * (c.x === 'b' ? b * (1 + 0.01 * (i % 3)) : c.x === 'a2' ? a2 : 1), bpr: m.bpr };
};
const phaseOf = (planFile, rows) => { const text = read(planFile); return { plan: JSON.parse(text), planSha: sha256(text), rows }; };

before(() => {
  S.root = fs.mkdtempSync(path.join(os.tmpdir(), 'bwp-kit-test-'));
  S.kit = path.join(S.root, 'kit');
  S.run = path.join(S.root, 'run');
  fs.mkdirSync(S.kit); fs.mkdirSync(S.run);
  for (const f of SCRIPTS) fs.copyFileSync(path.join(KIT, f), path.join(S.kit, f));
  S.fixture = path.join(S.root, 'fixture');
  ok(spawnSync(process.execPath, [path.join(S.kit, 'prepare-harness.mjs'), REFERENCE, S.fixture], { encoding: 'utf8' }));
  fs.writeFileSync(path.join(S.fixture, 'footer'), 'SYNTHETIC stand-in for the compiled footer tool; not a binary\n');
  const files = Object.fromEntries(['score.ts', 'measure.ts', 'audit.mjs', 'policy.mjs', 'base.yaml']
    .map(f => [f, sha256(fs.readFileSync(path.join(S.fixture, f)))]));
  const sha = 'c'.repeat(40);
  const binaries = { 'arrowcol-dataflow-rust': h('b1'), 'arrowcol-dmv': h('b2'), 'arrowcol-catalog-rust': h('b3'),
    'arrowcol-loadgen': h('b4') };
  S.ident = { sha, dirty: '', host: 'synthetic-host', binaries,
    maintenance_sha256: null, build_provenance: { allocator: 'mimalloc', collector_catalog_maintenance_source: sha, binaries: { ...binaries } },
    config_sha256: files['base.yaml'], harness_sha256: files['score.ts'], harness_files: files,
    footer_sha256: sha256(fs.readFileSync(path.join(S.fixture, 'footer'))), node_version: process.version,
    allocator_env: { GLIBC_TUNABLES: null, LD_PRELOAD: null }, storage: 'local_filesystem' };
  const clock = { t: Date.now() - 20 * 86400e3 };
  S.screenStart = clock.t;
  const sp = (...a) => ok(cli(S.kit, 'screen-plan.mjs', a));
  sp('pilot', BASE, FOOTER, at('pilot.json'));
  writeRows(at('pilot.jsonl'), rowsFor(at('pilot.json'), clock));
  S.gridLog = sp('grid', BASE, FOOTER, at('grid.json'), at('pilot.json'), at('pilot.jsonl')).stderr;
  writeRows(at('grid.jsonl'), rowsFor(at('grid.json'), clock));
  S.condLog = sp('conditional', BASE, FOOTER, at('cond.json'), at('pilot.json'), at('pilot.jsonl')).stderr;
  writeRows(at('cond.jsonl'), rowsFor(at('cond.json'), clock));
  sp('headroom', BASE, FOOTER, at('room-default.json'));
  writeRows(at('room-default.jsonl'), rowsFor(at('room-default.json'), clock));
  S.selectLog = ok(cli(S.kit, 'confirm.mjs', ['select', BASE, FOOTER, at('pilot.json'), at('pilot.jsonl'), at('grid.json'),
    at('grid.jsonl'), at('selection.json'), at('room-fast.json'), at('cond.json'), at('cond.jsonl')])).stderr;
  writeRows(at('room-fast.jsonl'), rowsFor(at('room-fast.json'), clock));
  S.clock = clock;
  ok(cli(S.kit, 'confirm.mjs', ['register', BASE, FOOTER, at('selection.json'), at('room-default.json'), at('room-default.jsonl'),
    at('room-fast.json'), at('room-fast.jsonl'), S.fixture, at('confirm.json'), at('registration.json')]));
  S.confirmClock = () => ({ t: Math.max(Date.now(), clock.t) + 60_000 });
  writeRows(at('results.jsonl'), rowsFor(at('confirm.json'), S.confirmClock(), { shape: confirmShape() }));
  S.scoreLog = ok(cli(S.kit, 'confirm.mjs', ['score', BASE, FOOTER, at('registration.json'), at('confirm.json'), at('results.jsonl'),
    at('verdict.json')])).stderr;
  sp('heatmap', BASE, FOOTER, at('heat'), at('pilot.json'), at('pilot.jsonl'), at('grid.json'), at('grid.jsonl'));
  sp('probe', BASE, FOOTER, at('probe.json'));
  writeRows(at('probe.jsonl'), rowsFor(at('probe.json'), { t: clock.t }));
  sp('readiness', BASE, FOOTER, at('readiness.json'), at('probe.json'), at('probe.jsonl'));
});
after(() => { if (S.root) fs.rmSync(S.root, { recursive: true, force: true }); });

test('synthetic pilot -> grid -> conditional -> select -> headroom -> register -> score -> heatmap -> readiness', () => {
  assert.match(S.gridLog, /GRID codec x encoding/);
  assert.match(S.condLog, /CONDITIONAL page_index, v2_ratio/);
  assert.match(S.scoreLog, /VERDICT QUALIFIED/);
  assert.equal(json(at('pilot.json')).cells.length, 20);
  assert.equal(json(at('grid.json')).cells.length, 15);
  assert.deepEqual(json(at('cond.json')).cells.map(c => c.id).sort(), ['base', 'base-end', 'page_index-column_index_trunc16',
    'page_index-header_stats', 'page_index-offset_index_off', 'page_index-stats_trunc16', 'v2_ratio-v2_zstd1', 'v2_ratio-v2_zstd1_ratio2']);
  const s = json(at('selection.json'));
  assert.equal(s.synthetic_non_performance, true);
  assert.equal(`${s.speed.phase}/${s.speed.cell}`, 'grid/codec-baseline-encoding-delta', 'equal ratios tie-break by phase/cell');
  assert.equal(`${s.smallest.phase}/${s.smallest.cell}`, 'grid/codec-brotli1-encoding-baseline');
  assert.ok(Math.abs(s.speed.rps_ratio - 1.25 / 1.005) < 1e-9);
  assert.equal(s.speed_beats_control, true);
  assert.equal(s.speed_trades_bytes, true);
  assert.deepEqual(s.conditional_groups, ['page_index', 'v2_ratio']);
  assert.equal(read(at('selection.json.sha256')), `${sha256(fs.readFileSync(at('selection.json')))}  selection.json\n`);
  const reg = json(at('registration.json'));
  assert.deepEqual(reg.gates, { effect: 1.05, t: 2.776, bytes_ratio: 1.10, abs_A2_over_A1_minus_1: 0.05, n: 5 });
  assert.equal(reg.footer_binary_sha256, S.ident.footer_sha256);
  assert.equal(reg.b_source, 'grid/codec-baseline-encoding-delta');
  assert.equal(json(at('confirm.json')).cells.length, 15);
  const v = json(at('verdict.json'));
  assert.equal(v.verdict, 'QUALIFIED');
  assert.equal(v.synthetic_non_performance, true);
  assert.ok(v.effect > 1.2 && v.bytes_ratio <= 1.10 && v.t >= 2.776);
  const csv = read(at('heat.csv')).trim().split('\n');
  assert.equal(csv.length, 1 + 7 * 2);
  assert.ok(csv.slice(1).every(line => line.endsWith(',true')));
  for (const svg of ['heat-rps.svg', 'heat-bytes.svg']) assert.match(read(at(svg)), /SYNTHETIC HARNESS TEST DATA - NOT A MEASUREMENT/);
  const ready = json(at('readiness.json'));
  assert.equal(ready.arms.length, 29);
  assert.equal(ready.synthetic_non_performance, true);
});

test('pre-parser rejects plan, env and cell typos and forbidden names', () => {
  const plan = json(at('pilot.json'));
  const bad = [
    [{ ...plan, durations: 45 }, /unknown field durations/],
    [{ ...plan, cells: [{ ...plan.cells[0], comment: 'x' }] }, /unknown field comment/],
    [{ ...plan, duration: 4 }, /duration/],
    [{ ...plan, common: { ...plan.common, BWA_CORE: '12' } }, /unknown or forbidden environment name BWA_CORE/],
    [{ ...plan, common: { ...plan.common, BWA_ROOT: '/tmp' } }, /forbidden environment name BWA_ROOT/],
    [{ ...plan, common: { ...plan.common, GLIBC_TUNABLES: 'x' } }, /forbidden environment name GLIBC_TUNABLES/],
    [{ ...plan, common: { ...plan.common, BWA_CORES: 12 } }, /single-line string/],
    [{ ...plan, common: { ...plan.common, BWA_TXN: 'perid' } }, /invalid value/],
    [{ ...plan, common: { ...plan.common, ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS: '2' } }, /must be 1/],
    [{ ...plan, cells: [plan.cells[0], plan.cells[0]] }, /duplicate id/],
  ];
  for (const [p, pattern] of bad) assert.throws(() => validatePlan(p, leaves), pattern);
});

test('pre-parser rejects bad writer enums, fallbacks, levels and combinations', () => {
  const col = o => ({ columns: { [BYTES_LEAF]: o } });
  const cases = [
    [{ defaults: { encoding: 'rle_dictionary', dictionary_enabled: false } }, /not a fallback encoder/],
    [{ defaults: { encoding: 'plain_dictionary', dictionary_enabled: false } }, /not a fallback encoder/],
    [{ defaults: { encoding: 'DELTA_BYTE_ARRAY', dictionary_enabled: false } }, /expected one of/],
    [col({ encoding: 'rle', dictionary_enabled: false }), /invalid for physical type BYTE_ARRAY/],
    [{ columns: { [INT_LEAF]: { encoding: 'delta_byte_array', dictionary_enabled: false } } }, /invalid for physical type INT64/],
    [col({ encoding: 'delta_byte_array' }), /fallback encoding delta_byte_array is unverifiable while the dictionary is enabled/],
    [{ defaults: { dictionary_enabled: 'false' } }, /expected a JSON boolean/],
    [{ defaults: { compression: 'ZSTD' } }, /compression: expected one of/],
    [{ defaults: { compression: 'zstd', compression_level: 0 } }, /zstd compression level must be 1..=22/],
    [{ defaults: { compression: 'gzip', compression_level: 10 } }, /gzip compression level must be 0..=9/],
    [{ defaults: { compression: 'brotli', compression_level: 12 } }, /brotli compression level must be 0..=11/],
    [{ defaults: { compression: 'snappy', compression_level: 1 } }, /snappy does not accept a compression level/],
    [{ defaults: { compression_level: 3 } }, /compression_level requires compression/],
    [{ defaults: { compression: 'zstd', compression_level: 1.5 } }, /i32 integer/],
    [{ defaults: { bloom_filter_enabled: false, bloom_filter_fpp: 0.01 } }, /disabled Bloom filter cannot specify/],
    [{ defaults: { bloom_filter_fpp: 1 } }, /must be in \(0,1\)/],
    [{ writer_version: '2.0' }, /expected v1 or v2/],
    [{ defaults: { compression: null } }, /null is rejected/],
    [{ sorting_columns: [] }, /sorting_columns/],
    [{ columns: { no_such_leaf: { dictionary_enabled: false } } }, /unknown physical Parquet column no_such_leaf/],
    [{ compresion: 'zstd' }, /unknown field compresion/],
    [{ defaults: { statistic: 'none' } }, /unknown field statistic/],
    [{ offset_index_enabled: false }, /offset_index_enabled=false requires explicit statistics/],
    [{ defaults: { statistics: 'page' }, offset_index_enabled: false }, /offset_index_enabled=false requires/],
    [{ defaults: { write_page_header_statistics: true, statistics: 'chunk' } }, /needs page statistics/],
    [{ defaults: { dictionary_enabled: false, dictionary_page_size_limit: 1024 } }, /inactive when the dictionary is disabled/],
    [{ defaults: { compression: 'zstd', data_page_v2_compression_ratio_threshold: 2 } }, /needs explicit writer_version=v2/],
    [{ writer_version: 'v2', defaults: { data_page_v2_compression_ratio_threshold: 2 } }, /compressing codec/],
    [{ max_row_group_rows: 0 }, /positive integer/],
    [{ write_batch_size: '8192' }, /positive integer/],
  ];
  for (const [config, pattern] of cases) assert.throws(() => validateWriterConfig(JSON.stringify(config), leaves), pattern, JSON.stringify(config));
  assert.throws(() => validateWriterConfig('{"defaults":', leaves), /invalid JSON/);
  for (const config of [{ defaults: { compression: 'zstd', compression_level: 22 } }, { defaults: { compression: 'gzip', compression_level: 0 } },
    { defaults: { dictionary_enabled: false, encoding: 'plain' } }, { defaults: { statistics: 'none' }, offset_index_enabled: false }]) {
    assert.doesNotThrow(() => validateWriterConfig(JSON.stringify(config), leaves), JSON.stringify(config));
  }
});

test('pre-parser rejects bad Delta statistics column lists', () => {
  for (const [value, pattern] of [['{}', /JSON array/], ['["customer_id","customer_id"]', /duplicate/], ['["a..b"]', /invalid dotted path/],
    ['["not_a_leaf"]', /not a primitive leaf/], [JSON.stringify(Array.from({ length: 257 }, (_, i) => 'c' + i)), /at most 256/],
    ['[1]', /1\.\.1024-byte/]]) {
    assert.throws(() => validateStatsColumns(value, leaves), pattern, value);
  }
  assert.deepEqual(validateStatsColumns('[]', leaves), []);
});

test('ENV_RULES cover exactly the runner BWA names except BWA_ROOT and BWA_BIN', () => {
  const names = new Set();
  for (const f of ['score.ts', 'base.yaml', 'measure.ts']) for (const m of read(path.join(REFERENCE, 'harness', f)).matchAll(/BWA_[A-Z0-9_]+/g)) names.add(m[0]);
  names.delete('BWA_ROOT'); names.delete('BWA_BIN');
  assert.deepEqual(Object.keys(ENV_RULES).filter(k => k.startsWith('BWA_')).sort(), [...names].sort());
});

test('identity requires every field to be present and well shaped', () => {
  assert.deepEqual(identityProblems(S.ident), []);
  assert.ok(identityProblems({}).length >= 14, 'all-undefined identities must not pass as equal');
  const broken = [
    r => { delete r.footer_sha256; }, r => { r.harness_files = undefined; }, r => { r.dirty = ' M src/lib.rs'; },
    r => { r.binaries['arrowcol-extra'] = h('x'); }, r => { delete r.binaries['arrowcol-dmv']; },
    r => { r.binaries['arrowcol-dmv'] = 'abc'; }, r => { r.build_provenance.collector_catalog_maintenance_source = 'd'.repeat(40); },
    r => { r.build_provenance.allocator = 'system'; }, r => { r.harness_files['score.ts'] = h('stale'); }, r => { r.harness_sha256 = h('other'); },
    r => { delete r.harness_files['audit.mjs']; }, r => { r.harness_files['measure.ts'] = undefined; },
    r => { r.harness_files['policy.mjs'] = 'not-a-sha256'; }, r => { r.allocator_env = {}; }, r => { r.storage = 'abfss'; }, r => { r.sha = 'HEAD'; },
    r => { r.maintenance_sha256 = undefined; }, r => { r.node_version = undefined; },
    r => { delete r.build_provenance.binaries; }, r => { r.build_provenance.binaries['arrowcol-dmv'] = h('rebuilt'); },
    r => { r.maintenance_sha256 = h('maint'); },
  ];
  for (const mutate of broken) { const r = clone(S.ident); mutate(r); assert.ok(identityProblems(r).length > 0, mutate.toString()); }
});

test('resume refuses failed, foreign, reordered or replayed output', () => {
  const plan = json(at('pilot.json')), rows = jsonl(at('pilot.jsonl'));
  const out = path.join(S.run, 'out');
  const current = { ...S.ident, plan_sha256: rows[0].plan_sha256 };
  assert.deepEqual(resumeProblems(rows.slice(0, 3), plan, current, out), []);
  const cases = [
    [r => { r[1].status = 'failed'; }, /failed output needs a fresh directory/],
    [r => { r[2].plan_sha256 = h('other-plan'); }, /written for a different plan/],
    [r => { [r[0], r[1]] = [r[1], r[0]]; }, /reordered, replayed or different plan/],
    [r => { r[1].run = path.join(S.root, 'elsewhere', 'x'); }, /run directory is not unique/],
    [r => { r[2].run = r[1].run; }, /run directory is not unique/],
    [r => { r[0].binaries['arrowcol-dmv'] = h('swapped'); }, /identity binaries differs/],
    [r => { r[0].knobs.BWA_PD = '8'; }, /knobs differ/],
  ];
  for (const [mutate, pattern] of cases) {
    const prior = clone(rows.slice(0, 3)); mutate(prior);
    assert.match(resumeProblems(prior, plan, current, out).join('\n'), pattern, mutate.toString());
  }
  assert.match(resumeProblems(rows, plan, { ...current, footer_sha256: h('new-footer') }, out).join('\n'), /identity footer_sha256 differs/);
  assert.match(resumeProblems([...rows, rows[0]], plan, current, out).join('\n'), /exceed 20 planned cells/);
});

test('phase admission refuses incomplete, mismatched, drifting or failed evidence', () => {
  const phase = phaseOf(at('pilot.json'), jsonl(at('pilot.jsonl')));
  assert.doesNotThrow(() => admit(clone(phase), { synthetic: true, bookends: true }));
  const cases = [
    [p => { p.rows.pop(); }, /observed 19\/20 planned rows/],
    [p => { p.rows[3].duration = 30; }, /duration\/warmup differ/],
    [p => { p.rows[3].warmup = 0; }, /duration\/warmup differ/],
    [p => { p.rows[3].stats.seconds = 30; }, /measured window does not match/],
    [p => { p.rows[4].committed_rps = NaN; }, /committed_rps is not finite/],
    [p => { p.rows[4].bytes_per_row = Infinity; }, /bytes_per_row inconsistent/],
    [p => { p.rows[4].physical.rows += 1; }, /physical footer reconciliation/],
    [p => { p.rows[4].retry_offered_rps = 12; }, /retries were offered/],
    [p => { p.rows[4].status = 'failed_physical_audit'; }, /status failed_physical_audit/],
    [p => { p.rows[5].stats.start_observed_ms = p.rows[4].stats.start_observed_ms; }, /overlaps the previous row/],
    [p => { p.rows[6].run = p.rows[5].run; }, /run directories are not unique/],
    [p => { p.rows[7].binaries['arrowcol-dataflow-rust'] = h('swap'); }, /identity differs from the cohort/],
    [p => { p.rows[7].client[0].preparation.payload_corpus_sha256 = h('other-corpus'); }, /payload corpus differs/],
    [p => { p.rows[8].knobs.ARROWCOL_PARQUET_CONFIG = '{}'; }, /knobs differ/],
    [p => { p.rows[9].plan_sha256 = h('other'); }, /plan_sha256 differs/],
    [p => { p.rows[9].physical.user_ns_per_row = undefined; }, /CPU ns\/row missing/],
    [p => { p.rows[10].isolation_level = 'l1'; }, /isolation l1, expected l5/],
    [p => { [p.rows[2], p.rows[3]] = [p.rows[3], p.rows[2]]; }, /expected cell/],
    [p => { p.rows[11].footer_sha256 = undefined; }, /identity.footer_sha256: expected SHA-256/],
  ];
  for (const [mutate, pattern] of cases) {
    const p = clone(phase); mutate(p);
    assert.throws(() => admit(p, { synthetic: true, bookends: true }), pattern, mutate.toString());
  }
  const drifted = phaseOf(at('pilot.json'), rowsFor(at('pilot.json'), { t: S.screenStart }, { endFactor: 1.08 }));
  assert.throws(() => admit(drifted, { synthetic: true, bookends: true }), /bookend drift 0\.0800 exceeds 0\.05/);
  const noEnd = clone(phase); noEnd.plan.cells.pop(); noEnd.rows.pop();
  assert.throws(() => admit(noEnd, { synthetic: true, bookends: true }), /lacks base\/base-end bookends/);
  assert.throws(() => admit(clone(phase), { synthetic: false, bookends: true }), /synthetic_non_performance rows are not evidence/);
  assert.throws(() => admit(clone(phase), { synthetic: true, bookends: true, after: Date.now() }), /before its prerequisite/);
  assert.throws(() => admit(clone(phase), { synthetic: true, footerSha: h('compiled-elsewhere') }), /footer_sha256 differs/);
  const stale = phaseOf(at('grid.json'), jsonl(at('grid.jsonl')).map(r => ({ ...r, binaries: { ...r.binaries, 'arrowcol-dmv': h('rebuilt') } })));
  const pilot = admit(clone(phase), { synthetic: true, bookends: true });
  assert.throws(() => admit(stale, { synthetic: true, bookends: true, after: pilot.end, cohort: pilot.cohort }), /identity differs from the cohort/);
});

test('screening CLI refuses real-mode use of synthetic rows, stale plans and unplanned or missing phases', () => {
  refused(cli(S.kit, 'screen-plan.mjs', ['grid', BASE, FOOTER, at('x1.json'), at('pilot.json'), at('pilot.jsonl')], { real: true }),
    /synthetic_non_performance rows are not evidence/);
  const env = { ...process.env, BWP_SYNTHETIC_NON_PERFORMANCE: 'yes' };
  refused(spawnSync(process.execPath, [path.join(S.kit, 'screen-plan.mjs'), 'pilot', BASE, FOOTER, at('x2.json')], { env, encoding: 'utf8' }),
    /must be unset or exactly 1/);
  fs.writeFileSync(at('pilot-edited.json'), read(at('pilot.json')).replace('"duration": 45', '"duration": 46'));
  refused(cli(S.kit, 'screen-plan.mjs', ['grid', BASE, FOOTER, at('x3.json'), at('pilot-edited.json'), at('pilot.jsonl')]),
    /not the deterministic regeneration/);
  refused(cli(S.kit, 'confirm.mjs', ['select', BASE, FOOTER, at('pilot.json'), at('pilot.jsonl'), at('grid.json'), at('grid.jsonl'),
    at('x4.json'), at('x4-room.json')]), /Conditional phase is due \(page_index, v2_ratio\)/);
  refused(cli(S.kit, 'screen-plan.mjs', ['pilot', BASE, FOOTER, at('pilot.json')]), /EEXIST/);
  writeRows(at('pilot-flat.jsonl'), rowsFor(at('pilot.json'), { t: S.screenStart }, { shape: () => ({ rps: 1e6, bpr: 100 }), endFactor: 1 }));
  const quiet = ok(cli(S.kit, 'screen-plan.mjs', ['conditional', BASE, FOOTER, at('x5.json'), at('pilot.json'), at('pilot-flat.jsonl')]));
  assert.match(quiet.stderr, /NO_CONDITIONAL_PHASE/);
  assert.equal(fs.existsSync(at('x5.json')), false);
  const valid = ok(cli(S.kit, 'screen-plan.mjs', ['validate', BASE, FOOTER, at('pilot.json'), at('probe.json'), at('cond.json'), at('confirm.json')]));
  assert.equal(valid.stdout.trim().split('\n').filter(l => l.startsWith('OK ')).length, 4);
  assert.match(valid.stderr, /Offline pre-parser only.*The Rust parser/s);
  const typo = JSON.parse(read(at('pilot.json')));
  typo.cells[1].env = { ARROWCOL_PARQUET_CONFG: '{}' };
  fs.writeFileSync(at('typo.json'), JSON.stringify(typo));
  refused(cli(S.kit, 'screen-plan.mjs', ['validate', BASE, FOOTER, at('typo.json')]), /unknown or forbidden environment name ARROWCOL_PARQUET_CONFG/);
});

test('grid generation skips illegal combinations deterministically', () => {
  const { plan, skipped } = gridPlan(common, leaves, 'encoding', 'dictionary_bytes');
  assert.deepEqual(skipped.map(s => s.id), ['encoding-delta-dictionary_bytes-kib256']);
  assert.ok(skipped.every(s => /inactive when the dictionary is disabled/.test(s.reason)));
  assert.deepEqual(gridPlan(common, leaves, 'encoding', 'dictionary_bytes').plan, plan);
  assert.deepEqual(pilotPlan(common, leaves), json(at('pilot.json')));
  const ids = p => p.cells.map(c => c.id);
  assert.ok(!ids(plan).some(id => id.includes('no_dictionary')) && !ids(pilotPlan(common, leaves)).includes('encoding-no_dictionary'));
  assert.ok(ids(probePlan(common, leaves)).includes('encoding-no_dictionary'), 'amendment A1 leaves the readiness probe unchanged');
});

test('registration refuses tampered selection, changed analyzer, stale fixture and unproven headroom', () => {
  const rooms = [at('room-default.json'), at('room-default.jsonl'), at('room-fast.json'), at('room-fast.jsonl')];
  const reg = (tag, { selection = at('selection.json'), fixture = S.fixture, room = rooms, kit = S.kit } = {}) =>
    cli(kit, 'confirm.mjs', ['register', BASE, FOOTER, selection, ...room, fixture, at(`${tag}-plan.json`), at(`${tag}-reg.json`)]);
  fs.writeFileSync(at('sel-tampered.json'), read(at('selection.json')).replace('"screening_only": true', '"screening_only": false'));
  fs.writeFileSync(at('sel-tampered.json.sha256'), read(at('selection.json.sha256')).replace('selection.json', 'sel-tampered.json'));
  refused(reg('r1', { selection: at('sel-tampered.json') }), /does not match its .sha256 sidecar/);
  const kit2 = path.join(S.root, 'kit-changed');
  fs.cpSync(S.kit, kit2, { recursive: true });
  fs.appendFileSync(path.join(kit2, 'screen-plan.mjs'), '\n// changed after selection\n');
  refused(reg('r2', { kit: kit2 }), /Analyzer files changed since selection/);
  const fx2 = path.join(S.root, 'fixture-footer');
  fs.cpSync(S.fixture, fx2, { recursive: true });
  fs.writeFileSync(path.join(fx2, 'footer'), 'a different footer tool\n');
  refused(reg('r3', { fixture: fx2 }), /footer binary differs/);
  const fx3 = path.join(S.root, 'fixture-yaml');
  fs.cpSync(S.fixture, fx3, { recursive: true });
  fs.appendFileSync(path.join(fx3, 'base.yaml'), '# edited\n');
  refused(reg('r4', { fixture: fx3 }), /harness files differ/);
  writeRows(at('room-weak.jsonl'), rowsFor(at('room-default.json'), { t: S.clock.t + 1e7 }, { ratio: 1.2 }));
  refused(reg('r5', { room: [at('room-default.json'), at('room-weak.jsonl'), ...rooms.slice(2)] }), /Unproven client headroom for default/);
  writeRows(at('room-early.jsonl'), rowsFor(at('room-default.json'), { t: S.screenStart }));
  refused(reg('r6', { room: [at('room-default.json'), at('room-early.jsonl'), ...rooms.slice(2)] }), /before its prerequisite/);
  refused(reg('r7', { room: [...rooms.slice(0, 2), ...rooms.slice(0, 2)] }), /not the deterministic regeneration/);
  for (const tag of ['r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'r7']) assert.equal(fs.existsSync(at(`${tag}-reg.json`)), false, tag);
});

test('frozen scoring is inconclusive or refused on incomplete, failed, stale or tampered evidence', () => {
  let n = 0;
  const score = (rows, { reg = at('registration.json'), plan = at('confirm.json') } = {}) => {
    const results = at(`res-${++n}.jsonl`), out = at(`verdict-${n}.json`);
    writeRows(results, rows);
    const r = cli(S.kit, 'confirm.mjs', ['score', BASE, FOOTER, reg, plan, results, out]);
    return r.status === 0 ? json(out) : r;
  };
  const fresh = opts => rowsFor(at('confirm.json'), S.confirmClock(), { shape: confirmShape(opts) });
  const fourteen = score(fresh().slice(0, 14));
  assert.equal(fourteen.verdict, 'INCONCLUSIVE');
  assert.match(fourteen.errors.join(), /observed 14\/15 planned rows/);
  const failed = fresh(); failed[4].status = 'failed_storage_reserve';
  const failedVerdict = score(failed);
  assert.equal(failedVerdict.verdict, 'INCONCLUSIVE');
  assert.match(failedVerdict.errors.join(), /status failed_storage_reserve/);
  const stale = fresh().map(r => ({ ...r, footer_sha256: h('old-footer') }));
  assert.match(score(stale).errors.join(), /footer_sha256 differs from the registered footer binary/);
  const swapped = fresh().map(r => ({ ...r, binaries: { ...r.binaries, 'arrowcol-dmv': h('rebuilt') } }));
  assert.match(score(swapped).errors.join(), /identity differs from the cohort/);
  const early = rowsFor(at('confirm.json'), { t: S.screenStart }, { shape: confirmShape() });
  assert.match(score(early).errors.join(), /before its prerequisite/);
  const drift = score(fresh({ a2: 1.08 }));
  assert.equal(drift.verdict, 'INCONCLUSIVE');
  assert.equal(drift.gates.drift, false);
  const weak = score(fresh({ b: 0.81 }));
  assert.equal(weak.verdict, 'NOT_QUALIFIED');
  assert.equal(weak.gates.effect, false);
  fs.writeFileSync(at('registration-edit.json'), read(at('registration.json')).replace('"replacements": false', '"replacements": true'));
  fs.writeFileSync(at('registration-edit.json.sha256'), read(at('registration.json.sha256')).replace('registration.json', 'registration-edit.json'));
  refused(score(fresh(), { reg: at('registration-edit.json') }), /does not match its .sha256 sidecar/);
  fs.writeFileSync(at('confirm-edit.json'), read(at('confirm.json')).replace('"duration": 180', '"duration": 120'));
  refused(score(fresh(), { plan: at('confirm-edit.json') }), /Frozen registration mismatch/);
  refused(cli(S.kit, 'confirm.mjs', ['score', BASE, FOOTER, at('registration.json'), at('confirm.json'), at('results.jsonl'), at('v-real.json')],
    { real: true }), /registration is synthetic test data, not evidence/);
});

test('prepare-harness is deterministic, matches the kit fixture and refuses reuse or altered references', () => {
  const a = path.join(S.root, 'prep-a'), b = path.join(S.root, 'prep-b');
  for (const d of [a, b]) ok(spawnSync(process.execPath, [path.join(KIT, 'prepare-harness.mjs'), REFERENCE, d], { encoding: 'utf8' }));
  const files = fs.readdirSync(a).sort();
  assert.deepEqual(files, ['audit.mjs', 'base.yaml', 'measure.ts', 'policy.mjs', 'score.ts']);
  for (const f of files) {
    assert.ok(fs.readFileSync(path.join(a, f)).equals(fs.readFileSync(path.join(b, f))), f);
    assert.ok(fs.readFileSync(path.join(a, f)).equals(fs.readFileSync(path.join(KIT, 'fixture', f))), `kit fixture/${f} is stale`);
  }
  ok(spawnSync(process.execPath, ['--experimental-strip-types', '--check', path.join(a, 'score.ts')], { encoding: 'utf8' }));
  refused(spawnSync(process.execPath, [path.join(KIT, 'prepare-harness.mjs'), REFERENCE, a], { encoding: 'utf8' }), /EEXIST/);
  const alt = path.join(S.root, 'reference-altered');
  fs.cpSync(path.join(REFERENCE, 'harness'), path.join(alt, 'harness'), { recursive: true });
  fs.appendFileSync(path.join(alt, 'harness', 'score.ts'), '\n');
  refused(spawnSync(process.execPath, [path.join(KIT, 'prepare-harness.mjs'), alt, path.join(S.root, 'prep-c')], { encoding: 'utf8' }),
    /Unrecognized reference score.ts/);
  assert.equal(fs.existsSync(path.join(S.root, 'prep-c')), false);
  const runner = read(path.join(a, 'score.ts'));
  for (const s of ["from './audit.mjs'", 'resumeProblems(prior, plan, identity, output)', "footer_sha256: sha(path.join(here, 'footer'))",
    'Untracked ambient override', '^(ARROWCOL_|BWA_|MIMALLOC_)', 'failed_storage_reserve', 'Footer tool changed after startup',
    "'initialize-local', '--partition-by-ingestion-date'", 'BINARIES.map((name: string)', 'if (!probeOnly) break;',
    'memoryBefore = hostMemory()', 'retry_offered_rps=']) assert.ok(runner.includes(s), s);
});

test('coverage map accounts for all 33 inventory rows without overstating exercise', () => {
  const parse = file => read(file).trim().split(/\r?\n/).slice(1).map(line => {
    const cells = []; let cur = '', q = false;
    for (const ch of line) { if (ch === '"') q = !q; else if (ch === ',' && !q) { cells.push(cur); cur = ''; } else cur += ch; }
    return [...cells, cur];
  });
  const inventory = parse(path.join(KIT, 'writer-inventory.csv')).map(r => r[0]);
  const map = parse(path.join(KIT, 'coverage-map.csv'));
  assert.equal(inventory.length, 33);
  assert.deepEqual(map.map(r => r[0]), inventory);
  const pilot = new Map(json(at('pilot.json')).cells.map(c => [c.id, c.env]));
  const probe = new Map(json(at('probe.json')).cells.map(c => [c.id, c.env]));
  const conditional = new Set(Object.entries(CONDITIONAL).flatMap(([g, x]) => Object.keys(x.arms).map(a => `${g}-${a}`)));
  const keys = env => {
    const found = new Set(Object.keys(env));
    const walk = o => { for (const [k, v] of Object.entries(o)) { found.add(k); if (v && typeof v === 'object' && !Array.isArray(v)) walk(v); } };
    if (env.ARROWCOL_PARQUET_CONFIG) walk(JSON.parse(env.ARROWCOL_PARQUET_CONFIG));
    return found;
  };
  const token = surface => surface.startsWith('delta') && surface !== 'delta_byte_array' ? 'ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS' : surface;
  const statuses = new Set(['screened', 'screened_partial', 'conditional_probe', 'not_expressible', 'non_performance_or_unsupported']);
  for (const [surface, status, arms, reason, ...extra] of map) {
    assert.equal(extra.length, 0, surface);
    assert.ok(statuses.has(status), `${surface}: ${status}`);
    assert.ok(reason && reason.length > 20, `${surface}: missing reason`);
    const ids = arms ? arms.split(';') : [];
    if (status === 'screened' || status === 'screened_partial') {
      assert.ok(ids.length > 0, surface);
      for (const id of ids) assert.ok(pilot.has(id) && keys(pilot.get(id)).has(token(surface)), `${surface}: ${id}`);
    } else if (status === 'conditional_probe') {
      assert.ok(ids.length > 0, surface);
      for (const id of ids) assert.ok(conditional.has(id) && probe.has(id) && keys(probe.get(id)).has(surface), `${surface}: ${id}`);
      assert.ok(![...pilot.values()].some(env => keys(env).has(surface)), `${surface} is in the pilot; not conditional`);
    } else {
      assert.equal(ids.length, 0, surface);
      assert.ok(!COLUMN_FIELDS.includes(surface) && (!WRITER_FIELDS.includes(surface) || surface === 'sorting_columns'), surface);
    }
  }
  assert.deepEqual(validateStatsColumns(pilot.get('delta_stats-disabled').ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS), []);
});

test('synthetic mode switch is strict', () => {
  assert.equal(syntheticMode({}), false);
  assert.equal(syntheticMode({ BWP_SYNTHETIC_NON_PERFORMANCE: '1' }), true);
  for (const v of ['0', 'true', '']) assert.throws(() => syntheticMode({ BWP_SYNTHETIC_NON_PERFORMANCE: v }), /unset or exactly 1/);
});
