export type Values = Record<string, number | null>;
export type Level = 'L0' | 'L1' | 'L2' | 'L3' | 'L4' | 'L5';
export const stages = ['collect', 'build', 'sort', 'encode', 'upload', 'commit'] as const;
export const states = ['pipeline', 'flush_wait', 'flush_slot', 'preparing', 'commit_wait', 'receipt_wait'] as const;
export const physicalStages = ['encode', 'fsync', 'readback', 'upload'] as const;
export const finite = (v: unknown): number | null =>
  typeof v === 'number' && Number.isFinite(v) && v >= 0 && v <= Number.MAX_SAFE_INTEGER ? v : null;

type Series = {labels: Map<string, string>; value: number | null};
export function collectorMetrics(text: string): Values {
  const byName = new Map<string, Series[]>();
  for (const line of text.split('\n')) {
    if (!line.startsWith('arrowcol_') || line.includes('_bucket{')) continue;
    const m = /^(\w+)(?:\{((?:[^"}]|"(?:\\.|[^"\\])*")*)\})?\s+(\S+)\s*$/.exec(line);
    if (!m) continue;
    const labels = new Map<string, string>();
    for (const l of (m[2] ?? '').matchAll(/(\w+)="((?:\\.|[^"\\])*)"/g)) labels.set(l[1], l[2]);
    const series = byName.get(m[1]) ?? [];
    series.push({labels, value: finite(Number(m[3]))});
    byName.set(m[1], series);
  }
  function one(suffix: string, labels: Record<string, string> = {}): number | null {
    const found = byName.get(`arrowcol_${suffix}`)?.filter(s => Object.entries(labels).every(([k, v]) => s.labels.get(k) === v));
    return found?.length === 1 ? found[0].value : null;
  }
  const v: Values = {};
  for (const metric of ['received_rows_total', 'received_bytes_total', 'delta_rows_total', 'delta_bytes_total',
    'delta_failures_total', 'downstream_nacks_total', 'dfe_enqueue_failures_total', 'flight_active_streams',
    'dfe_buffered_bytes', 'dfe_buffered_rows', 'delta_pending_commits', 'publication_backlog_available',
    'publication_settled_bytes_total', 'publication_settled_byte_seconds_total', 'parquet_file_physical_bytes_sum', 'parquet_file_rows_sum']) v[metric] = one(metric);
  for (const unit of ['rows', 'bytes', 'publications', 'oldest_age_seconds']) {
    v[`backlog.${unit}`] = v.publication_backlog_available === 1 ? one(`publication_backlog_${unit}`) : null;
  }
  for (const scope of ['global', 'table']) for (const resource of ['bytes', 'rows', 'batches']) {
    for (const metric of ['reserved', 'waiters','acquire_seconds_total','acquire_attempts_total']) v[`${metric}.${scope}.${resource}`] = one(`exporter_capacity_${metric}`, {scope, resource});
  }
  for (const state of states) {
    v[`groups.${state}`] = one('exporter_group_occupancy', {state});
    v[`groupSeconds.${state}`] = one('exporter_group_occupancy_seconds_total', {state});
  }
  for (const stage of stages) for (const field of ['sum', 'count']) {
    v[`duration.${stage}.${field}`] = one(`pipeline_stage_duration_seconds_${field}`, {stage});
  }
  return v;
}
