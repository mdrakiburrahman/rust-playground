// Consolidated Lane P screening table. Usage: node summary-csv.mjs <runs-dir> <out.csv>
import fs from 'node:fs';
import path from 'node:path';

const [runs, out] = process.argv.slice(2);
const PHASES = [
  ['pilot', 'refused: encoding-no_dictionary window 48.854 s > 45 s'],
  ['pilot2', 'refused: bookend drift'],
  ['pilot3', 'admitted'],
  ['grid', 'admitted'],
  ['cond', 'refused: bookend drift'],
  ['cond2', 'refused: bookend drift (final attempt; screening stopped)'],
  ['room-default', 'superseded: ran before cond2; descriptive only'],
];
const load = (p) => fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
const num = (v, d) => (Number.isFinite(v) ? v.toFixed(d) : '');
const delta = (r, k) => (r.host_memory?.end?.[k] ?? NaN) - (r.host_memory?.start?.[k] ?? NaN);
const cols = ['phase', 'admission', 'phase_drift', 'cell', 'x', 'y', 'status', 'duration_s', 'committed_rps', 'rps_vs_bookend_mean',
  'bytes_per_row', 'bytes_vs_bookend_mean', 'user_ns_per_row', 'system_ns_per_row', 'files_per_commit', 'commits',
  'pressure_samples', 'degraded_samples', 'delta_compact_stall', 'kustainer_rows'];
const lines = [cols.join(',')];
for (const [phase, admission] of PHASES) {
  const rows = load(path.join(runs, phase, 'results.jsonl'));
  const b = rows.find((r) => r.cell === 'base'), e = rows.find((r) => r.cell === 'base-end');
  const ref = b && e ? { rps: (b.committed_rps + e.committed_rps) / 2, bpr: (b.bytes_per_row + e.bytes_per_row) / 2 } : null;
  const drift = b && e ? num(e.committed_rps / b.committed_rps - 1, 4) : '';
  for (const r of rows) {
    const p = r.physical ?? {}, s = r.stats ?? {};
    const v = [phase, admission, drift, r.cell, r.x, r.y, r.status, r.duration, num(r.committed_rps, 0),
      ref ? num(r.committed_rps / ref.rps, 4) : '', num(r.bytes_per_row, 3), ref ? num(r.bytes_per_row / ref.bpr, 4) : '',
      num(p.user_ns_per_row, 0), num(p.system_ns_per_row, 0), num(s.files_per_commit, 3), s.commits ?? '',
      p.pressure_samples ?? '', p.degraded_samples ?? '', num(delta(r, 'compact_stall'), 0), r.kustainer_rows ?? 'null'];
    lines.push(v.map((x) => (/[",]/.test(String(x)) ? `"${String(x).replace(/"/g, '""')}"` : String(x ?? ''))).join(','));
  }
}
fs.writeFileSync(out, lines.join('\n') + '\n');
console.log(`${lines.length - 1} rows -> ${out}`);
