// Summarize Lane P runner rows: node summarize.mjs <results.jsonl> [--md]
import fs from 'node:fs';
const [file, flag] = process.argv.slice(2);
const rows = fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
const base = rows.find((r) => r.cell === 'base' && r.status === 'ok');
const f = (v, d = 0) => (Number.isFinite(v) ? v.toFixed(d) : '-');
const delta = (r, k) => (r.host_memory ? (r.host_memory.end?.[k] ?? NaN) - (r.host_memory.start?.[k] ?? NaN) : NaN);
const out = rows.map((r) => {
  const p = r.physical ?? {}, s = r.stats ?? {};
  const init = (r.table_initializer ?? []).map((x) => JSON.stringify(x.properties ?? {})).join(' ');
  return {
    cell: r.cell, status: r.status, rps: f(r.committed_rps), vs_base: base && r.committed_rps ? f(r.committed_rps / base.committed_rps, 3) : '-',
    bpr: f(r.bytes_per_row, 2), user_ns: f(p.user_ns_per_row), sys_ns: f(p.system_ns_per_row),
    files_commit: f(s.files_per_commit, 2), commits: s.commits ?? '-', press: p.pressure_samples ?? '-', degr: p.degraded_samples ?? '-',
    d_allocstall: f(delta(r, 'allocstall_movable')), d_compact: f(delta(r, 'compact_stall')), init,
    error: r.error ? String(r.error).slice(0, 160) : '',
  };
});
if (flag === '--md') {
  const cols = Object.keys(out[0]);
  console.log('| ' + cols.join(' | ') + ' |\n|' + cols.map(() => '---').join('|') + '|');
  for (const o of out) console.log('| ' + cols.map((c) => String(o[c]).replace(/\|/g, '&#124;')).join(' | ') + ' |');
} else console.table(out);
