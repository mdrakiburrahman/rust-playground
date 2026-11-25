import fs from 'node:fs';
import path from 'node:path';

export type Commit = { rows: number; bytes: number; files: number; timestamp: number;
  nonDataChangingFiles: number; removes: number };
type TableLog = { version: number | null; bytes: number };
export type Snapshot = { observedAt: number; commits: Map<string, Commit>; checkpoints: Set<string>;
  tableLogs: Record<string, TableLog> };

function logSize(dir: string): number {
  let bytes = 0;
  for (const item of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, item.name);
    if (item.isFile()) bytes += fs.statSync(full).size;
    else if (item.isDirectory()) bytes += logSize(full);
  }
  return bytes;
}

export function snapshot(root: string): Snapshot {
  const commits = new Map<string, Commit>();
  const checkpoints = new Set<string>();
  const tableLogs: Record<string, TableLog> = {};
  function walk(dir: string): void {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const full = path.join(dir, entry.name);
      if (entry.name !== '_delta_log') {
        if (!entry.name.startsWith('__arrowcol_ingestion=')) walk(full);
        continue;
      }
      const table: TableLog = { version: null, bytes: 0 };
      tableLogs[path.relative(root, dir)] = table;
      for (const entry of fs.readdirSync(full, { withFileTypes: true })) {
        const name = entry.name;
        const file = path.join(full, name);
        if (entry.isDirectory()) { table.bytes += logSize(file); continue; }
        if (!entry.isFile()) continue;
        const stat = fs.statSync(file);
        table.bytes += stat.size;
        if (/^\d{20}\.checkpoint(?:\.(?:\d+\.\d+|[a-f0-9-]+))?\.(?:parquet|json)$/i.test(name)) {
          checkpoints.add(path.relative(root, path.join(full, name)));
        }
        if (!/^\d{20}\.json$/.test(name)) continue;
        const version = Number(name.slice(0, 20));
        if (!Number.isSafeInteger(version)) throw new Error(`Unrepresentable Delta version ${name}`);
        table.version = Math.max(table.version ?? 0, version);
        const commit: Commit = { rows: 0, bytes: 0, files: 0, timestamp: stat.mtimeMs,
          nonDataChangingFiles: 0, removes: 0 };
        for (const line of fs.readFileSync(file, 'utf8').split('\n').filter(Boolean)) {
          const action = JSON.parse(line);
          if ('remove' in action) commit.removes++;
          if (!('add' in action)) continue;
          const records = typeof action.add?.stats === 'string' ? JSON.parse(action.add.stats)?.numRecords : undefined;
          if (!Number.isSafeInteger(records) || records < 0 ||
              !Number.isSafeInteger(action.add.size) || action.add.size < 0) {
            throw new Error(`Missing or invalid Delta add statistics: ${file}`);
          }
          if (typeof action.add.dataChange !== 'boolean') {
            throw new Error(`Missing or invalid Delta add dataChange: ${file}`);
          }
          if (!action.add.dataChange) {
            commit.nonDataChangingFiles++;
            continue;
          }
          commit.rows += records;
          commit.bytes += action.add.size;
          commit.files++;
          if (!Number.isSafeInteger(commit.rows) || !Number.isSafeInteger(commit.bytes)) {
            throw new Error(`Unrepresentable Delta add totals: ${file}`);
          }
        }
        commits.set(path.relative(root, file), commit);
      }
    }
  }
  walk(root);
  return { observedAt: Date.now(), commits, checkpoints, tableLogs };
}

export function difference(start: Snapshot, end: Snapshot) {
  for (const key of start.commits.keys()) {
    if (!end.commits.has(key)) throw new Error(`Delta history disappeared during cell: ${key}`);
  }
  const newCommits = [...end.commits].filter(([key]) => !start.commits.has(key));
  for (const [key, commit] of newCommits) {
    if (commit.removes > 0 || commit.nonDataChangingFiles > 0) {
      throw new Error(`Cannot score new ingestion from rewriting/mixed Delta commit: ${key} ` +
        `(removes=${commit.removes}, nonDataChangingFiles=${commit.nonDataChangingFiles})`);
    }
  }
  const added = newCommits.filter(([, c]) => c.files > 0).map(([, c]) => c);
  const sum = (field: 'rows' | 'bytes' | 'files') => added.reduce((total, c) => total + c[field], 0);
  const seconds = (end.observedAt - start.observedAt) / 1000;
  if (seconds <= 0) throw new Error('Nonpositive observed measurement interval');
  const rows = sum('rows'), bytes = sum('bytes'), files = sum('files');
  if (![rows, bytes, files].every(Number.isSafeInteger)) throw new Error('Unrepresentable Delta window totals');
  return {
    rows, bytes, files, commits: added.length, seconds,
    committed_rps: rows / seconds,
    bytes_per_row: rows ? bytes / rows : null,
    rows_per_file: files ? rows / files : null,
    files_per_commit: added.length ? files / added.length : null,
    checkpoints: end.checkpoints.size,
    checkpoint_files_added: [...end.checkpoints].filter((key) => !start.checkpoints.has(key)).length,
    table_logs_start: start.tableLogs, table_logs_end: end.tableLogs,
    start_observed_ms: start.observedAt, end_observed_ms: end.observedAt,
    semantics: 'new_data_changing_add_only_Delta_versions_between_boundary_scans_rewrites_rejected_not_client_ACKs',
  };
}

export function parseProcStat(text: string) {
  const fields = text.slice(text.lastIndexOf(')') + 2).trim().split(/\s+/);
  const values = [fields[7], fields[9], fields[11], fields[12], fields[21]].map(Number);
  if (values.some((value) => !Number.isFinite(value))) throw new Error('Invalid /proc process stat');
  return { minorFaults: values[0], majorFaults: values[1], userTicks: values[2], systemTicks: values[3], rssPages: values[4] };
}

export function proc(pid: number) {
  const result = parseProcStat(fs.readFileSync(`/proc/${pid}/stat`, 'utf8'));
  const io = fs.readFileSync(`/proc/${pid}/io`, 'utf8');
  const bytes = (key: string) => {
    const match = io.match(new RegExp(`^${key}: (\\d+)$`, 'm'));
    if (!match) throw new Error(`Missing process IO counter ${key}`);
    return Number(match[1]);
  };
  return { ...result, readBytes: bytes('read_bytes'), writeBytes: bytes('write_bytes') };
}

export function procDifference(start: ReturnType<typeof proc>, end: ReturnType<typeof proc>, seconds: number, hz: number) {
  return {
    user_cores: (end.userTicks - start.userTicks) / hz / seconds,
    system_cores: (end.systemTicks - start.systemTicks) / hz / seconds,
    minor_faults: end.minorFaults - start.minorFaults,
    major_faults: end.majorFaults - start.majorFaults,
    disk_read_bytes: end.readBytes - start.readBytes,
    disk_write_bytes: end.writeBytes - start.writeBytes,
    rss_end_bytes: end.rssPages * 4096,
  };
}

export function clientMetrics(text: string) {
  const lines = text.split('\n').filter((line) => line.startsWith('OAT_CLIENT_METRICS '));
  if (!lines.length) throw new Error('Client did not emit final OAT_CLIENT_METRICS');
  const metrics = JSON.parse(lines.at(-1)!.slice('OAT_CLIENT_METRICS '.length));
  const window = metrics.measurement_window;
  if (!window || !Number.isFinite(window.completed_rows)) throw new Error('Missing client measurement window');
  const optionalCount = (value: unknown): number | null => {
    if (value === undefined) return null;
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Invalid offered-row counter');
    return value;
  };
  const count = (name: string): number => {
    const value = optionalCount(window[name]);
    if (value === null) throw new Error(`Missing client counter: ${name}`);
    return value;
  };
  const completionLine = text.split('\n').filter((line) => line.startsWith('OAT_COLLECTOR_COMPLETION ')).at(-1);
  return {
    completed_rows: count('completed_rows'),
    completed_ipc_bytes: count('completed_flight_data_bytes_uncompressed'),
    ambiguous_rows: count('ambiguous_rows'),
    rejected_rows: count('rejected_rows'),
    given_up_rows: count('given_up_rows'),
    retried_rows: count('retried_rows'),
    offered_rows: optionalCount(window.offered_rows),
    retry_offered_rows: optionalCount(window.retry_offered_rows),
    window_start_ms: count('start_unix_ms'),
    window_end_ms: count('end_unix_ms'),
    completion: completionLine ? JSON.parse(completionLine.slice('OAT_COLLECTOR_COMPLETION '.length)) : null,
    resources: window.client_resources,
    send_windows: window.windows,
    preparation: metrics.preparation,
    semantics: metrics.completion_semantics as string,
    error_logs: text.split('\n').filter((line) => /^Error(?:$|:|\s)|^\s*(?:\x1b\[[0-9;]*m)*ERROR(?:\s|:)/.test(line)).length,
  };
}
