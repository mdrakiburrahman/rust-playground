import fs from 'node:fs';
import path from 'node:path';

interface Action {
  add?: { path: string; stats?: string };
  remove?: { path: string };
  txn?: { appId: string };
}

/** One Delta commit that added data files. */
export interface DataCommit {
  readonly version: number;
  readonly files: number;
  readonly rows: number;
  /** `txn` actions in the commit: ArrowCol records one per upstream publication. */
  readonly transactions: number;
}

export interface TableLog {
  readonly commits: readonly DataCommit[];
  /** Files left after replaying every `add` and `remove`. */
  readonly activeFiles: number;
  /** Sum of `numRecords` over the active files. */
  readonly rows: number;
}

function records(stats: string | undefined): number {
  const rows = stats ? (JSON.parse(stats) as { numRecords?: unknown }).numRecords : undefined;
  if (typeof rows !== 'number') throw new Error('Delta add action has no numRecords statistic');
  return rows;
}

export function readLog(table: string): TableLog {
  const log = path.join(table, '_delta_log');
  const names = fs.readdirSync(log).filter(name => /^\d{20}\.json$/.test(name)).sort();
  const active = new Map<string, number>();
  const commits: DataCommit[] = [];
  for (const name of names) {
    let files = 0, rows = 0, transactions = 0;
    for (const line of fs.readFileSync(path.join(log, name), 'utf8').split('\n')) {
      if (line.trim() === '') continue;
      const action = JSON.parse(line) as Action;
      if (action.add) {
        const count = records(action.add.stats);
        active.set(action.add.path, count);
        files++;
        rows += count;
      }
      if (action.remove) active.delete(action.remove.path);
      if (action.txn) transactions++;
    }
    if (files > 0) commits.push({ version: Number(name.slice(0, 20)), files, rows, transactions });
  }
  return { commits, activeFiles: active.size, rows: [...active.values()].reduce((sum, count) => sum + count, 0) };
}

/** Every Delta table under `root`, as paths relative to it. */
export function deltaTables(root: string): string[] {
  const found: string[] = [];
  const walk = (directory: string): void => {
    if (!fs.existsSync(directory)) return;
    const entries = fs.readdirSync(directory, { withFileTypes: true });
    if (entries.some(entry => entry.isDirectory() && entry.name === '_delta_log')) {
      found.push(path.relative(root, directory));
      return;
    }
    for (const entry of entries) if (entry.isDirectory()) walk(path.join(directory, entry.name));
  };
  walk(root);
  return found.sort();
}