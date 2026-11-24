import fs from 'node:fs';
import path from 'node:path';

export type Commit = { rows: number; bytes: number; files: number; timestamp: number };
export type Snapshot = { observedAt: number; commits: Map<string, Commit>; checkpoints: number };

export function snapshot(root: string): Snapshot {
  const commits = new Map<string, Commit>();
  let checkpoints = 0;
  function walk(dir: string): void {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const full = path.join(dir, entry.name);
      if (entry.name !== '_delta_log') {
        if (!entry.name.startsWith('__arrowcol_ingestion=')) walk(full);
        continue;
      }
      for (const name of fs.readdirSync(full).sort()) {
        if (name.includes('.checkpoint')) checkpoints++;
        if (!/^\d{20}\.json$/.test(name)) continue;
        const file = path.join(full, name);
        const commit = { rows: 0, bytes: 0, files: 0, timestamp: fs.statSync(file).mtimeMs };
        for (const line of fs.readFileSync(file, 'utf8').split('\n').filter(Boolean)) {
          const action = JSON.parse(line);
          if (!action.add) continue;
          const records = action.add.stats && JSON.parse(action.add.stats).numRecords;
          if (!Number.isSafeInteger(records) || records < 0 || !Number.isSafeInteger(action.add.size)) {
            throw new Error(`Missing or invalid Delta add statistics: ${file}`);
          }
          commit.rows += records;
          commit.bytes += action.add.size;
          commit.files++;
        }
        commits.set(path.relative(root, file), commit);
      }
    }
  }
  walk(root);
  return { observedAt: Date.now(), commits, checkpoints };
}

export function difference(start: Snapshot, end: Snapshot) {
  for (const key of start.commits.keys()) {
    if (!end.commits.has(key)) throw new Error(`Delta history disappeared during cell: ${key}`);
  }
  const added = [...end.commits].filter(([key, c]) => !start.commits.has(key) && c.files > 0).map(([, c]) => c);
  const sum = (field: 'rows' | 'bytes' | 'files') => added.reduce((total, c) => total + c[field], 0);
  const seconds = (end.observedAt - start.observedAt) / 1000;
  if (seconds <= 0) throw new Error('Nonpositive observed measurement interval');
  const rows = sum('rows'), bytes = sum('bytes'), files = sum('files');
  return {
    rows, bytes, files, commits: added.length, seconds,
    committed_rps: rows / seconds,
    bytes_per_row: rows ? bytes / rows : null,
    rows_per_file: files ? rows / files : null,
    files_per_commit: added.length ? files / added.length : null,
    checkpoints: end.checkpoints,
    start_observed_ms: start.observedAt, end_observed_ms: end.observedAt,
    semantics: 'new_local_Delta_versions_visible_between_boundary_scans_not_client_ACKs',
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
  return {
    completed_rows: window.completed_rows as number,
    completed_ipc_bytes: window.completed_flight_data_bytes_uncompressed as number,
    ambiguous_rows: window.ambiguous_rows as number,
    rejected_rows: window.rejected_rows as number,
    given_up_rows: window.given_up_rows as number,
    retried_rows: window.retried_rows as number,
    resources: window.client_resources,
    send_windows: window.windows,
    preparation: metrics.preparation,
    semantics: metrics.completion_semantics as string,
    error_logs: text.split('\n').filter((line) => /^Error(?:$|:|\s)|^\s*(?:\x1b\[[0-9;]*m)*ERROR(?:\s|:)/.test(line)).length,
  };
}
