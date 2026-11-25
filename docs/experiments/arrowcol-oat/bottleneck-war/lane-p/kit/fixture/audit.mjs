import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { isDeepStrictEqual } from 'node:util';

const check = (ok, msg) => { if (!ok) throw Error(msg); };
const json = p => JSON.parse(fs.readFileSync(p, 'utf8'));
const actions = p => fs.readFileSync(p, 'utf8').trim().split('\n').map(JSON.parse);
const hash = value => createHash('sha256').update(value).digest('hex');
const tokens = value => value.match(/[A-Z][A-Z_0-9]*/g) ?? [];
function within(root, child) {
  const delta = path.relative(fs.realpathSync(root), fs.realpathSync(child));
  check(delta && delta !== '..' && !delta.startsWith(`..${path.sep}`) && !path.isAbsolute(delta), 'Outside private table');
}

export function audit(r, root, run, footerTool) {
  const w = JSON.parse(r.knobs.ARROWCOL_PARQUET_CONFIG || '{}');
  const selection = r.knobs.ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS;
  const boundary = json(path.join(run, 'commit-boundaries.json'));
  const old = new Set(boundary.start.map(([name]) => name));
  const paths = new Set(), tables = new Set(), seen = new Map();
  const tableLogs = Object.keys(r.stats.table_logs_end);
  check(tableLogs.length === 1 && r.stats.rows > 0, 'Expected one non-empty table');
  const table = path.join(root, tableLogs[0]);
  within(root, table);
  const startup = r.effective_startup.filter(s => s.parquet_writer && s.runtime);
  check(startup.length > 0 && startup.every(s => s.runtime.parquet_column_workers === 1), 'Column worker readback absent or wrong');
  const build = r.effective_startup.filter(s => s.component === 'build');
  check(build.length === 1 && build[0].config?.allocator === 'mimalloc' && build[0].config?.source_revision === r.sha,
    'Runtime build receipt absent or not the mimalloc build of the identity sha');
  function agrees(a, b) {
    return b !== null && (typeof a === 'object' && a !== null
      ? typeof b === 'object' && Object.entries(a).every(([k,v]) => agrees(v, b[k]))
      : isDeepStrictEqual(a,b));
  }
  check(startup.every(s => agrees(w, s.parquet_writer)), 'Process writer policy differs from request');
  const resolved = r.effective_startup.filter(s => s.component === 'layout' && s.scope === 'table_effective');
  check(resolved.length && new Set(resolved.map(s => s.table_id)).size === 1, 'Ambiguous or missing table receipt');
  for (const s of resolved) {
    for (const [key, v] of Object.entries(w)) {
      if (!['columns','defaults'].includes(key)) check(isDeepStrictEqual(s.parquet_properties?.[key], v), `Table ${key}`);
    }
    const cols = Object.entries(s.parquet_column_options ?? {});
    check(cols.length && Object.keys(w.columns ?? {}).every(key => key in s.parquet_column_options), 'Missing physical column');
    for (const [name, values] of cols) {
      for (const [key, v] of Object.entries({ ...w.defaults, ...w.columns?.[name] })) {
        const actual = ['compression','compression_level'].includes(key) ? s.parquet_columns?.[name]?.[key] : values[key];
        check(isDeepStrictEqual(actual,v), `Table ${name}.${key}: expected ${v}, got ${actual}`);
      }
    }
  }
  let properties;
  for (const [log] of boundary.end.slice().sort(([a],[b]) => a.localeCompare(b))) {
    const file = path.join(root, log);
    within(table, file);
    for (const action of actions(file)) if (action.metaData) properties = action.metaData.configuration;
  }
  if (selection !== undefined) {
    const cols = JSON.parse(selection);
    check(properties && (cols.length ? properties['delta.dataSkippingStatsColumns'] === cols.join(',') :
      properties['delta.dataSkippingNumIndexedCols'] === '0' && !('delta.dataSkippingStatsColumns' in properties)),
    'Delta metadata does not match the requested skipping statistics');
  }
  const sizes = [], adds = [], shapes = new Set();
  let files = 0, rows = 0, bytes = 0, first;
  for (const [name] of boundary.end) {
    if (old.has(name)) continue;
    const log = path.join(root, name);
    tables.add(path.dirname(path.dirname(log)));
    for (const a of actions(log)) {
      check(!a.remove, 'Remove action in ingestion window');
      if (!a.add) continue;
      const add = a.add, file = path.resolve(table, decodeURIComponent(add.path));
      within(table,file);
      check(add.dataChange !== false && !path.isAbsolute(add.path) && !paths.has(file), 'Non-ingestion or duplicate Add');
      paths.add(file);
      const f = JSON.parse(execFileSync(footerTool,[file],{encoding:'utf8',maxBuffer:32*1024*1024}));
      const stats = JSON.parse(add.stats);
      check(f.rows === stats.numRecords && f.bytes === add.size && f.row_groups > 0, `Delta Add/footer mismatch: ${name}`);
      if (selection !== undefined) {
        const sel = JSON.parse(selection);
        for (const kind of ['minValues','maxValues','nullCount']) {
          const fields = Object.keys(stats[kind] || {});
          check(fields.every(k => sel.includes(k)), `Unexpected ${kind} in Delta Add`);
          if (sel.length && kind === 'nullCount') check(sel.every(k => fields.includes(k)), 'Missing selected nullCount');
        }
      }
      for (const c of f.columns) {
        const o = { ...w.defaults, ...w.columns?.[c.path] };
        const codec = tokens(c.codec)[0], enc = tokens(c.encodings);
        if (o.compression) check(codec === o.compression.toUpperCase(), `${c.path} footer codec ${c.codec}`);
        if (o.dictionary_enabled === false) check(!c.dictionary && !enc.includes('RLE_DICTIONARY'), `${c.path} dictionary`);
        if (o.encoding && o.dictionary_enabled === false) check(enc.includes(o.encoding.toUpperCase()), `${c.path} fallback encoder not exercised`);
        if (o.statistics) check(c.stats === (o.statistics !== 'none'), `${c.path} footer stats`);
        if (o.bloom_filter_enabled !== undefined) check(c.bloom === o.bloom_filter_enabled, `${c.path} footer bloom`);
        const key = JSON.stringify([c.path,c.physical,codec,c.dictionary,enc,c.stats,c.bloom]);
        seen.set(key, (seen.get(key) ?? 0) + 1);
      }
      const n = f.columns.length / f.row_groups;
      check(Number.isInteger(n) && n > 0,'Malformed row group');
      shapes.add(JSON.stringify(f.columns.slice(0,n).map(c => [c.path,c.physical])));
      first ??= f; sizes.push(f.bytes); files++; rows += f.rows; bytes += f.bytes;
      adds.push({log:name, path:add.path, rows:f.rows, bytes:f.bytes, row_groups:f.row_groups});
    }
  }
  check(files === r.stats.files && rows === r.stats.rows && bytes === r.stats.bytes && files > 0 &&
    shapes.size === 1 && tables.size === 1 && tables.has(table),'Physical reconciliation failed');
  const encoded = JSON.stringify([...seen].sort(([a],[b]) => a.localeCompare(b)));
  fs.writeFileSync(path.join(run,'footer.json'), JSON.stringify(first));
  fs.writeFileSync(path.join(run,'physical-chunks.json'), encoded);
  fs.writeFileSync(path.join(run,'reconciled-adds.json'), JSON.stringify(adds));
  const rich = r.pressure.map(s => s.rich).filter(Boolean);
  return { files, rows, bytes, metadata_sha256:hash(encoded), adds_sha256:hash(JSON.stringify(adds)),
    max_file_bytes:Math.max(...sizes), stats_properties:properties ?? null, user_ns_per_row:1e9*r.collector.user_cores/r.committed_rps,
    system_ns_per_row:1e9*r.collector.system_cores/r.committed_rps, pressure_samples:rich.length,
    degraded_samples:rich.length ? rich.filter(v => v.delta_state === 'degraded' || v.dfe_state === 'degraded').length : null };
}
