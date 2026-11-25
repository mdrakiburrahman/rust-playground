// Lane P report charts from measured runner rows. Usage: node charts.mjs <runs-dir> <out-dir>
import fs from 'node:fs';
import path from 'node:path';

const [runs, out] = process.argv.slice(2);
fs.mkdirSync(out, { recursive: true });
const load = (p) => fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map((l) => JSON.parse(l));
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

function ratios(rows) {
  const b = rows.find((r) => r.cell === 'base'), e = rows.find((r) => r.cell === 'base-end');
  const rps = (b.committed_rps + e.committed_rps) / 2, bpr = (b.bytes_per_row + e.bytes_per_row) / 2;
  const m = new Map();
  for (const r of rows) {
    if (r.cell === 'base' || r.cell === 'base-end' || r.status !== 'ok' || !(r.committed_rps > 0)) continue;
    m.set(r.cell, { rps: r.committed_rps / rps, bytes: r.bytes_per_row / bpr,
      user: r.physical?.user_ns_per_row, sys: r.physical?.system_ns_per_row, fpc: r.stats?.files_per_commit });
  }
  return { m, drift: e.committed_rps / b.committed_rps - 1,
    base: { user: (b.physical.user_ns_per_row + e.physical.user_ns_per_row) / 2, sys: (b.physical.system_ns_per_row + e.physical.system_ns_per_row) / 2 } };
}

function effectsChart({ file, title, subtitle, main, others, note }) {
  const arms = [...main.m.entries()].sort((a, b) => b[1].rps - a[1].rps);
  const lo = 0.6, hi = 1.15, x0 = 230, w = 360, rowH = 24, top = 92;
  const sx = (v) => x0 + ((Math.min(Math.max(v, lo), hi) - lo) / (hi - lo)) * w;
  const H = top + arms.length * rowH + 64, W = 1000;
  const s = [];
  s.push(`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}" font-family="Segoe UI, sans-serif" font-size="12">`);
  s.push(`<rect width="100%" height="100%" fill="#ffffff"/><rect x="0.5" y="0.5" width="${W - 1}" height="${H - 1}" fill="none" stroke="#0078d4"/>`);
  s.push(`<text x="16" y="28" font-size="16" font-weight="600" fill="#201f1e">${esc(title)}</text>`);
  s.push(`<text x="16" y="48" fill="#605e5c">${esc(subtitle)}</text>`);
  const hy = top - 12;
  s.push(`<text x="${x0}" y="${hy}" fill="#323130" font-weight="600">Committed rows/s vs bookend mean</text>`);
  for (const [lbl, x] of [['bytes/row', 700], ['user ns/row', 785], ['sys ns/row', 865], ['files/commit', 945]])
    s.push(`<text x="${x}" y="${hy}" text-anchor="middle" fill="#323130" font-weight="600">${lbl}</text>`);
  for (const t of [0.6, 0.7, 0.8, 0.9, 1.0, 1.1]) {
    const x = sx(t);
    s.push(`<line x1="${x}" y1="${top - 4}" x2="${x}" y2="${top + arms.length * rowH}" stroke="${t === 1 ? '#605e5c' : '#edebe9'}" stroke-width="${t === 1 ? 1.2 : 1}"/>`);
    s.push(`<text x="${x}" y="${top + arms.length * rowH + 14}" text-anchor="middle" fill="#605e5c">${t.toFixed(1)}</text>`);
  }
  arms.forEach(([cell, v], i) => {
    const y = top + i * rowH;
    if (i % 2 === 0) s.push(`<rect x="8" y="${y}" width="${W - 16}" height="${rowH}" fill="#f8f9fb"/>`);
    s.push(`<text x="${x0 - 8}" y="${y + 16}" text-anchor="end" fill="#201f1e">${esc(cell)}</text>`);
    const a = sx(1), b = sx(v.rps);
    s.push(`<rect x="${Math.min(a, b)}" y="${y + 6}" width="${Math.max(1, Math.abs(b - a))}" height="12" rx="2" fill="${main.admitted ? (v.rps >= 1 ? '#0078d4' : '#9ec5ea') : (v.rps >= 1 ? '#8a8886' : '#d2d0ce')}"/>`);
    s.push(`<text x="${x0 + w + 50}" y="${y + 16}" text-anchor="end" fill="#201f1e">${v.rps.toFixed(3)}</text>`);
    others.forEach((o, k) => {
      const ov = o.ratios.m.get(cell);
      if (ov) s.push(`<circle cx="${sx(ov.rps)}" cy="${y + 12}" r="3.5" fill="#ffffff" stroke="${o.color}" stroke-width="1.6"><title>${esc(o.label)}: ${ov.rps.toFixed(3)}</title></circle>`);
    });
    const vals = [[700, v.bytes.toFixed(3) + 'x'], [785, Math.round(v.user)], [865, Math.round(v.sys)], [945, v.fpc?.toFixed(2)]];
    for (const [x, t] of vals) s.push(`<text x="${x}" y="${y + 16}" text-anchor="middle" fill="#323130">${t}</text>`);
  });
  const ly = top + arms.length * rowH + 34;
  s.push(`<rect x="16" y="${ly - 9}" width="14" height="10" rx="2" fill="${main.admitted ? '#0078d4' : '#8a8886'}"/><text x="36" y="${ly}" fill="#323130">${esc(main.label)} (${main.admitted ? 'admitted' : 'refused'}, drift ${(main.ratios.drift * 100).toFixed(1)}%)</text>`);
  let lx = 260;
  for (const o of others) {
    s.push(`<circle cx="${lx}" cy="${ly - 4}" r="3.5" fill="#ffffff" stroke="${o.color}" stroke-width="1.6"/><text x="${lx + 8}" y="${ly}" fill="#323130">${esc(o.label)}</text>`);
    lx += 250;
  }
  s.push(`<text x="16" y="${ly + 20}" font-style="italic" fill="#605e5c">${esc(note)}</text>`);
  s.push('</svg>');
  fs.writeFileSync(path.join(out, file), s.join('\n') + '\n');
}

const p1 = ratios(load(path.join(runs, 'pilot/results.jsonl')));
const p2 = ratios(load(path.join(runs, 'pilot2/results.jsonl')));
const p3 = ratios(load(path.join(runs, 'pilot3/results.jsonl')));
effectsChart({
  file: 'pilot-effects.svg', title: 'Pilot screening: one-factor arms vs defaults',
  subtitle: `pilot3 (admitted, drift ${(p3.drift * 100).toFixed(2)}%); base user ${Math.round(p3.base.user)} / sys ${Math.round(p3.base.sys)} ns/row; n=1 arm per cell, 45 s window, 20 s warm-up`,
  main: { label: 'pilot3', ratios: p3, m: p3.m, admitted: true }, others: [
    { label: `pilot1 (refused, drift ${(p1.drift * 100).toFixed(1)}%)`, ratios: p1, color: '#8a8886' },
    { label: `pilot2 (refused, drift ${(p2.drift * 100).toFixed(1)}%)`, ratios: p2, color: '#d13438' }],
  note: 'Screening only: single arms are candidates, not claims. Ratios use each phase\'s own base/base-end mean.',
});
for (const name of fs.readdirSync(runs)) {
  const f = path.join(runs, name, 'results.jsonl');
  if (!/^cond/.test(name) || !fs.existsSync(f)) continue;
  const c = ratios(load(f));
  effectsChart({
    file: `${name}-effects.svg`, title: `Conditional probes (${name})`,
    subtitle: `bookend drift ${(c.drift * 100).toFixed(2)}%; n=1 arm per cell, 45 s window`,
    main: { label: name, ratios: c, m: c.m, admitted: Math.abs(c.drift) <= 0.05 }, others: [],
    note: Math.abs(c.drift) > 0.05 ? 'Refused phase (drift above 5%): descriptive only.' : 'Admitted phase. Screening only.',
  });
}
console.log(fs.readdirSync(out).join(' '));
