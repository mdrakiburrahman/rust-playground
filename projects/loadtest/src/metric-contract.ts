export interface RequiredMetric {
  name: string;
  query: string;
  allowZero: boolean;
}

/** Actual Kernel 0.29 adapter instruments after Prometheus OTLP name translation. */
export const sdkMetricFamilies = [
  'delta_kernel_events_total',
  'delta_kernel_duration_seconds_count',
  'delta_kernel_values_count',
  'delta_kernel_bytes_count',
  'delta_kernel_version',
  'delta_publication_events_total',
  'delta_publication_duration_seconds_count',
] as const;

export function requiredMetrics(scope: 'source-sdk' | 'service-qualification', bridgeEnabled: boolean): RequiredMetric[] {
  const metrics: RequiredMetric[] = [];
  for (const [pool, service] of [['direct', 'arrowcol-rust'], ['dataflow', 'arrowcol-dataflow-rust']]) {
    for (const name of sdkMetricFamilies) {
      metrics.push({ name: `${pool}:${name}`, query: `sum(${name}{service_name="${service}"})`,
        allowZero: name === 'delta_kernel_version' });
    }
  }
  metrics.push({ name: 'collector_scrapes', query: 'count(up{job=~"direct|dataflow"} == 1) == bool 2', allowZero: false });
  if (scope === 'service-qualification') {
    for (const name of ['runs_total', 'cycles_total', 'heartbeat_timestamp_ms', 'backlog_files',
      'retention_cutoff_ms', 'purge_watermark_ms', 'files_removed_total', 'files_added_total',
      'rows_removed_total', 'rows_added_total', 'bytes_removed_total', 'bytes_added_total',
      'files_deleted_total', 'files_already_absent_total', 'collisions_total', 'retries_total']) {
      metrics.push({ name: `delta_maintenance_${name}`, query: `sum(delta_maintenance_${name}{job="delta-maintenance"})`,
        allowZero: !['runs_total', 'cycles_total', 'heartbeat_timestamp_ms', 'files_deleted_total'].includes(name) });
    }
    for (const operation of ['optimize', 'purge']) {
      metrics.push({ name: `${operation}:actual_removals`,
        query: `sum(delta_maintenance_files_removed_total{job="delta-maintenance",operation="${operation}"})`, allowZero: false });
    }
    for (const name of ['batches_total', 'checkpoint_commits_total', 'sink_replays_total', 'noop_polls_total',
      'txn_only_batches_total', 'snapshot_batches_total', 'tail_batches_total', 'offsets_prepared_total',
      'sink_commits_total', 'bootstrap_files_remaining', 'failures_total', 'source_version', 'processed_version',
      'version_lag', 'files_considered_total', 'rows_returned_total']) {
      metrics.push({ name: `tenant_stream_${name}`, query: `sum(tenant_stream_${name}{job="tenant-stream"})`,
        allowZero: !['batches_total', 'checkpoint_commits_total', 'rows_returned_total'].includes(name) });
    }
    for (const name of ['bytes_scanned', 'row_groups_pruned_statistics', 'row_groups_matched_statistics',
      'row_groups_pruned_bloom_filter', 'row_groups_matched_bloom_filter', 'page_index_rows_pruned',
      'page_index_rows_matched', 'pushdown_rows_pruned', 'pushdown_rows_matched',
      'files_ranges_pruned_statistics', 'files_ranges_matched_statistics']) {
      const family = `tenant_stream_datafusion_${name}_total`;
      metrics.push({ name: family, query: `sum(${family}{job="tenant-stream"})`,
        allowZero: !['bytes_scanned', 'row_groups_pruned_statistics'].includes(name) });
    }
    for (const pool of ['direct', 'dataflow']) {
      for (const name of ['completion_percent', 'pending_artifacts', 'cached_bytes', 'unhealthy_catalogs', 'lag_seconds']) {
        const family = `qap_${name}`;
        metrics.push({ name: `${pool}:${family}`, query: `sum(${family}{pool="${pool}"})`,
          allowZero: !['completion_percent', 'cached_bytes'].includes(name) });
      }
    }
    for (const job of ['delta-maintenance', 'tenant-stream']) {
      metrics.push({ name: `${job}:scrape`, query: `min(up{job="${job}"})`, allowZero: false });
    }
  }
  if (bridgeEnabled) {
    for (const name of ['pending_files', 'lag_versions', 'lag_seconds', 'degraded_tables']) {
      metrics.push({ name: `kusto_bridge_${name}`, query: `sum(kusto_bridge_${name})`, allowZero: true });
    }
    metrics.push({ name: 'bridge_scrape', query: 'min(up{job="kusto-bridge"})', allowZero: false });
  }
  return metrics;
}
