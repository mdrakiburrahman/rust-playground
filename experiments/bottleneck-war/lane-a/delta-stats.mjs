#!/usr/bin/env node
// Usage: delta-stats.mjs <root> <window_start_ms> <window_end_ms>
import fs from 'node:fs';
import path from 'node:path';

const [root, startArg, endArg] = process.argv.slice(2);
const start = Number(startArg);
const end = Number(endArg);

function findLogs(dir, out = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const full = path.join(dir, entry.name);
    if (entry.name === '_delta_log') out.push(full);
    else if (!entry.name.startsWith('__arrowcol_ingestion=')) findLogs(full, out);
  }
  return out;
}

const result = { tables: [], rows: 0, bytes: 0, files: 0, commits: 0, all_rows: 0, all_commits: 0, checkpoints: 0 };
const gaps = [];
const inWindow = [];
for (const log of findLogs(root)) {
  const names = fs.readdirSync(log);
  result.checkpoints += names.filter((n) => n.includes('.checkpoint')).length;
  const commits = names.filter((n) => /^\d{20}\.json$/.test(n)).sort();
  let prevTs = null;
  const table = { log, versions: commits.length, rows: 0, bytes: 0, files: 0, commits: 0, max_files_per_commit: 0 };
  for (const name of commits) {
    let ts = null;
    let rows = 0;
    let bytes = 0;
    let files = 0;
    for (const line of fs.readFileSync(path.join(log, name), 'utf8').split('\n')) {
      if (!line) continue;
      const action = JSON.parse(line);
      if (action.commitInfo) ts = action.commitInfo.timestamp ?? action.commitInfo.inCommitTimestamp ?? ts;
      if (action.add) {
        files += 1;
        bytes += action.add.size;
        const stats = action.add.stats ? JSON.parse(action.add.stats) : null;
        rows += stats?.numRecords ?? 0;
      }
    }
    if (ts === null) ts = fs.statSync(path.join(log, name)).mtimeMs;
    result.all_rows += rows;
    if (files > 0) result.all_commits += 1;
    if (ts >= start && ts < end && files > 0) {
      table.rows += rows;
      table.bytes += bytes;
      table.files += files;
      table.commits += 1;
      table.max_files_per_commit = Math.max(table.max_files_per_commit, files);
      inWindow.push([ts, rows]);
      if (prevTs !== null) gaps.push(ts - prevTs);
    }
    if (files > 0) prevTs = ts;
  }
  result.rows += table.rows;
  result.bytes += table.bytes;
  result.files += table.files;
  result.commits += table.commits;
  result.tables.push(table);
}
const seconds = (end - start) / 1000;
gaps.sort((a, b) => a - b);
const pct = (p) => (gaps.length ? gaps[Math.min(gaps.length - 1, Math.floor(p * gaps.length))] : null);
inWindow.sort((a, b) => a[0] - b[0]);
const span = inWindow.length >= 3 ? inWindow.at(-1)[0] - inWindow[0][0] : 0;
Object.assign(result, {
  seconds,
  committed_rows_per_second: result.rows / seconds,
  interval_rows_per_second: span > 0 ? (inWindow.slice(1).reduce((s, [, r]) => s + r, 0) * 1000) / span : null,
  parquet_bytes_per_row: result.rows ? result.bytes / result.rows : null,
  rows_per_file: result.files ? result.rows / result.files : null,
  files_per_commit: result.commits ? result.files / result.commits : null,
  commit_gap_ms_p50: pct(0.5),
  commit_gap_ms_p90: pct(0.9),
});
delete result.tables;
console.log(JSON.stringify(result));
