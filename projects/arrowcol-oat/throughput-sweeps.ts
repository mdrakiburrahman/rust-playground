import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { TextDecoder } from 'node:util';
import { fileURLToPath, pathToFileURL } from 'node:url';

type JsonObject = Record<string, unknown>;
type Candidate = { id: string; overrides: JsonObject; qualification?: 'exploratory' };
type GrpcCompression = 'none' | 'gzip' | 'zstd';
type Comparison = {
  id: string; factor: string; candidates: Candidate[]; candidate_limit: number;
  matched_control?: string; comparison_family?: string;
  comparison_type?: 'single-factor' | 'interaction-top-pick'; selection_prerequisite?: string;
};
type PendingCandidateSet = { id: string; factor: string; status: string; candidates: Candidate[] };
type Cohort = { id: string; comparisons: Comparison[]; refinement?: JsonObject; pendingCandidateSets?: PendingCandidateSet[] };

const root = resolve(fileURLToPath(new URL('../..', import.meta.url)));
const maintenance = {
  enabled: true, apply: true, operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'],
  interval_secs: 15, max_idle_backoff_secs: 15, granularity: 'hour', purge_retention_secs: 2_592_000,
};
const candidate = (id: string, value: JsonObject, rootOverrides: JsonObject = {}): Candidate => ({
  id, overrides: { ...rootOverrides, tunables: value },
});
const maxCandidatesPerComparison = 4;
const compare = (id: string, factor: string, candidates: Candidate[]): Comparison => ({
  id, factor, candidates, candidate_limit: maxCandidatesPerComparison,
});
const compareWithControl = (
  id: string, factor: string, control: Candidate, candidates: Candidate[], comparisonFamily?: string,
): Comparison => ({
  ...compare(id, factor, [control, ...candidates]),
  matched_control: control.id,
  ...(comparisonFamily ? { comparison_family: comparisonFamily } : {}),
});
const codecControl = candidate('uncompressed', {
  parquet_writer: { defaults: { compression: 'uncompressed', compression_level: null } },
});
const directionalGrpcCandidate = (id: string, clientToReceiver: GrpcCompression, receiverToClient: GrpcCompression): Candidate => candidate(id, {
  transport: { grpc_send_compression: clientToReceiver, grpc_accept_compression: receiverToClient },
  collector: { limits: { grpc_accept_compression: clientToReceiver, grpc_send_compression: receiverToClient } },
});
const grpcNoneControl = directionalGrpcCandidate('grpc-none', 'none', 'none');
const cohorts: Cohort[] = [
  { id: 'C1', comparisons: [
    compareWithControl('codec-zstd-levels', 'parquet_writer.defaults.compression+compression_level', codecControl, [
      candidate('zstd-1', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 1 } } }),
      candidate('zstd-3', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 3 } } }),
      candidate('zstd-6', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 6 } } }),
    ], 'codec'),
    compareWithControl('codec-snappy-lz4raw', 'parquet_writer.defaults.compression+compression_level', codecControl, [
      candidate('snappy', { parquet_writer: { defaults: { compression: 'snappy', compression_level: null } } }),
      candidate('lz4-raw', { parquet_writer: { defaults: { compression: 'lz4_raw', compression_level: null } } }),
    ], 'codec'),
    compareWithControl('codec-gzip-brotli', 'parquet_writer.defaults.compression+compression_level', codecControl, [
      candidate('gzip', { parquet_writer: { defaults: { compression: 'gzip', compression_level: null } } }),
      candidate('brotli', { parquet_writer: { defaults: { compression: 'brotli', compression_level: null } } }),
    ], 'codec'),
  ], refinement: {
    comparison: 'codec',
    factor: 'parquet_writer.defaults.compression_level',
    trigger: 'zstd wins the coarse codec comparison',
    selection: 'winner plus up to three adjacent levels accepted by the E1 registry bounds',
    max_candidates: 4,
  } },
  { id: 'C2', comparisons: [compare('dictionary-encoding', 'parquet_writer.columns', [
    candidate('availability-group-name-dictionary-off', { parquet_writer: { columns: { availability_group_name: { dictionary_enabled: false } } } }),
    candidate('availability-group-name-dictionary-on', { parquet_writer: { columns: { availability_group_name: { dictionary_enabled: true } } } }),
    candidate('last-connect-error-delta-binary-packed', { parquet_writer: { columns: { last_connect_error_number: { dictionary_enabled: false, encoding: 'delta_binary_packed' } } } }),
    candidate('last-connect-error-byte-stream-split', { parquet_writer: { columns: { last_connect_error_number: { dictionary_enabled: false, encoding: 'byte_stream_split' } } } }),
  ]), compare('string-encoding', 'parquet_writer.columns.availability_group_name.encoding', ([
    ['plain', 'plain'],
    ['delta-length-byte-array', 'delta_length_byte_array'],
    ['delta-byte-array', 'delta_byte_array'],
  ] as const).map(([id, encoding]) => candidate(id, {
    parquet_writer: { columns: { availability_group_name: { dictionary_enabled: false, encoding } } },
  }))) ] },
  { id: 'C3', comparisons: [
    compare('row-groups', 'parquet_writer.max_row_group_rows', [16384, 65536, 131072, 1000000].map(value => candidate('row-group-' + value, {
      parquet_writer: { max_row_group_rows: value }, collector: { layout: { enabled: true, row_group_rows: value } },
    }))),
    compare('max-rows', 'collector.layout.max_rows', [16384, 65536, 131072, 1000000].map(value => candidate('max-rows-' + value, {
      collector: { layout: { enabled: true, max_rows: value }, batching: { max_rows_per_flush: value } },
    }))),
    compare('page-rows', 'collector.layout.page_rows', [1024, 2048, 4096].map(value => candidate('page-' + value, {
      collector: { layout: { enabled: true, page_rows: value } },
    }))),
    compare('rollover-cap', 'collector.layout.max_input_bytes', [134217728, 268435456, 536870912].map(value => candidate('input-' + value, {
      collector: { layout: { enabled: true, max_input_bytes: value }, batching: {
        max_arrow_memory_per_flush: value, adaptive_file: { max_input_bytes: value },
      } },
    }))),
    compare('coalescing', 'collector.batching.coalesce_publications', [false, true].map(value => candidate('coalesce-' + value, {
      collector: { batching: { coalesce_publications: value } },
    }))),
    compare('flush-age', 'collector.batching.max_age', ['1s', '5s', '15s', '60s'].map(value => candidate('age-' + value, {
      collector: { batching: { max_age: value } },
    }))),
    compare('age-flush-policy', 'collector.batching.age_flush_policy', ['immediate', 'drain_ready'].map(value => candidate('age-policy-' + value, {
      collector: { batching: { age_flush_policy: value } },
    }))),
  ] },
  { id: 'C4', comparisons: [compare('sort-key', 'collector.layout.sort_columns', [
    candidate('none', { collector: { layout: { enabled: true, sort_columns: [] } }, parquet_writer: { sorting_columns: [] } }),
    candidate('sample-time', { collector: { layout: { enabled: true, sort_columns: ['sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
    candidate('replica-sample-time', { collector: { layout: { enabled: true, sort_columns: ['replica_server_name', 'sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'replica_server_name', descending: false, nulls_first: false }, { path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
    candidate('group-replica-sample-time', { collector: { layout: { enabled: true, sort_columns: ['availability_group_id', 'replica_server_name', 'sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'availability_group_id', descending: false, nulls_first: false }, { path: 'replica_server_name', descending: false, nulls_first: false }, { path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
  ])] },
  { id: 'C5', comparisons: [
    compare('workers', 'tunables.client.workers', [8, 16, 32, 64].map(value => candidate('workers-' + value, {
      client: { workers: value },
    }))),
    compare('streams', 'tunables.client.streams_per_worker', [1, 2, 4].map(value => candidate('streams-' + value, {
      client: { streams_per_worker: value },
    }))),
    compare('connections', 'tunables.client.connections_per_worker', [1, 2, 4].map(value => candidate('connections-' + value, {
      client: { connections_per_worker: value },
    }))),
    compareWithControl('data-mode', 'tunables.client.data_mode',
      candidate('data-mode-shared', { client: { data_mode: 'shared' } }),
      [candidate('data-mode-independent', { client: { data_mode: 'independent' } })], 'data-mode'),
    compare('corpus', 'tunables.client.corpus_rows', [16384, 65536, 262144].map(value => candidate('corpus-' + value, { client: { corpus_rows: value } }))),
    compare('interval', 'tunables.client.interval_us', [0, 1000, 10000].map(value => candidate('interval-' + value, { client: { interval_us: value } }))),
    compare('ipc-compression', 'tunables.transport.ipc_compression', ['none', 'lz4_frame', 'zstd'].map(value => candidate('ipc-' + value, { transport: { ipc_compression: value } }))),
    compareWithControl('grpc-client-to-receiver', 'transport.grpc_send_compression + collector.limits.grpc_accept_compression', grpcNoneControl,
      (['gzip', 'zstd'] as const).map(codec => directionalGrpcCandidate('grpc-client-to-receiver-' + codec, codec, 'none')), 'grpc-direction'),
    compareWithControl('grpc-receiver-to-client', 'collector.limits.grpc_send_compression + transport.grpc_accept_compression', grpcNoneControl,
      (['gzip', 'zstd'] as const).map(codec => directionalGrpcCandidate('grpc-receiver-to-client-' + codec, 'none', codec)), 'grpc-direction'),
    {
      ...compareWithControl('grpc-mixed-directions', 'paired client-to-receiver and receiver-to-client compression', grpcNoneControl, [
        directionalGrpcCandidate('grpc-mixed-request-gzip-response-zstd', 'gzip', 'zstd'),
        directionalGrpcCandidate('grpc-mixed-request-zstd-response-gzip', 'zstd', 'gzip'),
      ], 'grpc-direction'),
      comparison_type: 'interaction-top-pick',
      selection_prerequisite: 'Select the request and response codecs independently from the pure-direction top picks; do not score as single-factor evidence.',
    },
  ] },
  { id: 'C6', comparisons: [
    compare('dfe-concurrency', 'dfe_worker_concurrency', [1, 2, 4, 8].map(value => candidate('dfe-' + value, {}, { dfe_worker_concurrency: [value] }))),
    compare('cpu-workers', 'tunables.collector.runtime.cpu_workers', [1, 2, 4].map(value => candidate('cpu-' + value, { collector: { runtime: { cpu_workers: value } } }))),
    compare('upload-concurrency', 'tunables.collector.runtime.upload_limit', [1, 4, 8].map(value => candidate('upload-' + value, { collector: { runtime: { upload_limit: value } } }))),
    compare('flush-concurrency', 'tunables.collector.batching.max_concurrent_flushes', [1, 2, 4].map(value => candidate('flush-' + value, { collector: { batching: { max_concurrent_flushes: value } } }))),
  ] },
  { id: 'C7', comparisons: [
    compare('maintenance-cadence', 'tunables.maintenance.interval_secs', [30, 60, 120].map(value => ({
      ...candidate('cadence-' + value, { maintenance: { interval_secs: value, max_idle_backoff_secs: value } }),
      qualification: 'exploratory' as const,
    }))),
    compare('max-consecutive-stale-plans', 'tunables.maintenance.max_consecutive_stale_plans',
      [1, 8, 1024].map(value => candidate('stale-plans-' + value, {
        maintenance: { max_consecutive_stale_plans: value },
      }))),
    compare('sealed-partitions-only', 'tunables.maintenance.sealed_partitions_only',
      [true, false].map(value => candidate('sealed-partitions-' + value, {
        maintenance: { sealed_partitions_only: value },
      }))),
    compare('small-file-threshold', 'tunables.maintenance.small_file_bytes', [16777216, 33554432, 67108864].map(value => candidate('small-file-' + value, { maintenance: { small_file_bytes: value } }))),
    compare('retry-budget', 'tunables.maintenance.commit_attempts', [1, 3, 5].map(value => candidate('attempts-' + value, { maintenance: { commit_attempts: value } }))),
    compare('checkpoint-interval', 'tunables.maintenance.checkpoint_every_commits', [64, 256, 1024].map(value => candidate('checkpoint-' + value, { maintenance: { checkpoint_every_commits: value } }))),
    compare('input-read-strategy', 'tunables.maintenance.input_read_strategy', ['memory', 'spool', 'range'].map(value => candidate('input-' + value, {
      maintenance: { input_read_strategy: value },
    }))),
    compare('granularity', 'tunables.maintenance.granularity', ['minute', 'hour', 'day'].map(value => candidate('granularity-' + value, {
      maintenance: { granularity: value },
    }))),
    compare('optimize-upload-mode', 'tunables.maintenance.optimize_upload_mode', [
      candidate('put-create', { maintenance: { optimize_upload_mode: 'put_create' } }),
      {
        ...candidate('buffered-multipart', { maintenance: {
          optimize_upload_mode: 'buffered_multipart', optimize_upload_buffer_bytes: 8_388_608,
        } }),
        qualification: 'exploratory' as const,
      },
    ]),
  ] },
  { id: 'C8', comparisons: [
    compare('commit-window', 'tunables.collector.commit.batch_window', ['1s', '2s', '5s'].map(value => candidate('window-' + value, { collector: { commit: { batch_window: value } } }))),
    compare('files-per-commit', 'tunables.collector.commit.max_files_per_commit', [16, 64, 128].map(value => candidate('files-' + value, { collector: { commit: { max_files_per_commit: value } } }))),
  ], pendingCandidateSets: [{
    id: 'staging-overlap',
    factor: 'tunables.collector.commit.staging_overlap',
    status: 'pending-L1b',
    candidates: [false, true].map(value => candidate('overlap-' + value, { collector: { commit: { staging_overlap: value } } })),
  }] },
];

function isRecord(value: unknown): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validateComparisons(comparisons: unknown, cohort: string): asserts comparisons is Comparison[] {
  if (!Array.isArray(comparisons)) throw new Error(cohort + ' comparisons must be an array');
  const controlFamilies = new Map<string, { id: string; overrides: string }>();
  for (const comparison of comparisons) {
    if (!isRecord(comparison) || typeof comparison.id !== 'string' || typeof comparison.factor !== 'string' ||
        !Array.isArray(comparison.candidates)) throw new Error(cohort + ' contains an invalid comparison');
    if (comparison.candidate_limit !== maxCandidatesPerComparison || comparison.candidates.length > maxCandidatesPerComparison) {
      throw new Error(cohort + '/' + comparison.id + ' must enforce the ' + maxCandidatesPerComparison + '-candidate limit');
    }
    const candidateIds = new Set<string>();
    for (const item of comparison.candidates) {
      if (!isRecord(item) || typeof item.id !== 'string' || !isRecord(item.overrides) || candidateIds.has(item.id)) {
        throw new Error(cohort + '/' + comparison.id + ' contains an invalid or duplicate candidate');
      }
      candidateIds.add(item.id);
    }
    if (comparison.matched_control !== undefined) {
      if (typeof comparison.matched_control !== 'string') throw new Error(cohort + '/' + comparison.id + ' has an invalid matched control');
      const control = comparison.candidates.find(item => isRecord(item) && item.id === comparison.matched_control);
      if (!isRecord(control) || typeof control.id !== 'string' || !isRecord(control.overrides)) {
        throw new Error(cohort + '/' + comparison.id + ' is missing its declared matched control');
      }
      if (comparison.comparison_family !== undefined) {
        if (typeof comparison.comparison_family !== 'string' || comparison.comparison_family.length === 0) {
          throw new Error(cohort + '/' + comparison.id + ' has an invalid comparison family');
        }
        const signature = JSON.stringify(control.overrides);
        const prior = controlFamilies.get(comparison.comparison_family);
        if (prior && (prior.id !== control.id || prior.overrides !== signature)) {
          throw new Error(cohort + '/' + comparison.id + ' does not use the same matched control as its family');
        }
        controlFamilies.set(comparison.comparison_family, { id: control.id, overrides: signature });
      }
    } else if (comparison.comparison_family !== undefined) {
      throw new Error(cohort + '/' + comparison.id + ' declares a family without a matched control');
    }
  }
}

export function buildThroughputManifests(registry: unknown): JsonObject[] {
  if (!isRecord(registry) || !isRecord(registry.tunables)) throw new Error('registry must contain the PR125 tunables object');
  const tunables = registry.tunables;
  const registryHash = createHash('sha256').update(JSON.stringify(tunables)).digest('hex');
  return cohorts.map(cohort => {
    const client = isRecord(tunables.client) ? tunables.client : {};
    const transport = isRecord(tunables.transport) ? tunables.transport : {};
    const collector = isRecord(tunables.collector) ? tunables.collector : {};
    const upload = isRecord(collector.upload) ? collector.upload : {};
    const batching = isRecord(collector.batching) ? collector.batching : {};
    const collectorLimits = isRecord(collector.limits) ? collector.limits : {};
    const maintenanceSeed = isRecord(tunables.maintenance) ? tunables.maintenance : {};
    validateComparisons(cohort.comparisons, cohort.id);
    return {
      schema_version: 1,
      cohort: cohort.id,
      mode: 'local',
      base_config: 'oat-config.tuned.json',
      registry_source: 'PR125 arrowcol-tunables profile',
      registry_status: 'seed-only-not-capability-registry',
      registry_sha256: registryHash,
      box_scope: 'one box for every comparison in this cohort',
      campaign: 'single_hot_table_campaign',
      campaign_profile: 'single_hot_table',
      baseline_overrides: {
        campaign_profile: 'single_hot_table',
        overload_guard: { enabled: true, cap_fraction: 0.90, zero_progress_windows: 2 },
        platform: { local: { collector_memory_limit_bytes: 4_294_967_296 } },
        tables: ['sqlserver_availability_replica_states'],
        replica_counts: [1],
        rows_per_table: 65_536,
        warmup_seconds: 10,
        measure_seconds: 150,
      },
      invariants: {
        table: 'sqlserver_availability_replica_states',
        replicas: [1],
        wal_enabled: false,
        maintenance: { ...maintenance, continuous: true },
        compare_within_box: true,
        one_variable_at_a_time: true,
      },
      maintenance_setting_semantics: {
        max_consecutive_stale_plans: { omitted_requested: null, default_effective: 8, min: 1, max: 1024 },
        sealed_partitions_only: {
          omitted_requested: null,
          default_effective_without_table_override: true,
          effective_table_property: 'arrowcol.optimize.sealedPartitionsOnly',
          table_policy_overrides_startup_request: true,
        },
      },
      baseline_tunables: {
        ...tunables,
        maintenance: {
          ...(isRecord(tunables.maintenance) ? tunables.maintenance : {}),
          ...maintenance,
        },
      },
      comparisons: cohort.comparisons,
      ...(cohort.id === 'C1' ? { parquet_enum_coverage: {
        kind: 'compression',
        accepted: ['uncompressed', 'snappy', 'gzip', 'brotli', 'lz4', 'lz4_raw', 'zstd'],
        configured: ['uncompressed', 'snappy', 'gzip', 'brotli', 'lz4_raw', 'zstd'],
        excluded: { lz4: 'legacy Hadoop framing; accepted for compatibility but excluded by campaign consumer/deprecation policy' },
        pending: [],
        status: 'configured-not-runtime-screened',
      } } : {}),
      ...(cohort.id === 'C2' ? { parquet_enum_coverage: {
        kind: 'value_encoding',
        accepted: ['plain', 'rle', 'delta_binary_packed', 'delta_length_byte_array', 'delta_byte_array', 'byte_stream_split'],
        configured: ['plain', 'delta_binary_packed', 'delta_length_byte_array', 'delta_byte_array', 'byte_stream_split'],
        excluded: { rle: 'BOOLEAN-only; frozen table has no Boolean column' },
        pending: [],
        status: 'configured-not-runtime-screened',
      } } : {}),
      ...(cohort.id === 'C2' ? { source_enum_coverage: {
        dictionary_handling: {
          path: 'tunables.transport.dictionary_handling',
          accepted: ['hydrate', 'resend'],
          configured: [],
          baseline_requested: transport.dictionary_handling ?? null,
          effective_default: 'hydrate (bin/collector-chain/src/dmv_client.rs::Options)',
          consumer: 'ARROWCOL_CLIENT_DICTIONARY_HANDLING -> bin/collector-chain/src/dmv_client.rs::Options.dictionary_handling',
          excluded: { resend: 'Pinned Tunables validation rejects resend for the frozen plain DMV schema.' },
          pending: [],
          status: 'single-runtime-compatible-value-no-contrast',
        },
      } } : {}),
      ...(cohort.id === 'C3' ? { source_enum_coverage: {
        age_flush_policy: {
          path: 'tunables.collector.batching.age_flush_policy',
          accepted: ['immediate', 'drain_ready'],
          configured: ['immediate', 'drain_ready'],
          baseline_requested: batching.age_flush_policy ?? null,
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
      } } : {}),
      ...(cohort.id === 'C5' ? { source_enum_coverage: {
        data_mode: {
          path: 'tunables.client.data_mode',
          accepted: ['shared', 'independent'],
          configured: ['shared', 'independent'],
          baseline_requested: client.data_mode ?? null,
          effective_default: 'shared (bin/collector-chain/src/dmv_client.rs::Options)',
          consumer: 'ARROWCOL_CLIENT_DATA_MODE -> bin/collector-chain/src/dmv_client.rs::Options.data_mode; shared uses one corpus copy and independent uses workers copies under max_memory_bytes',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        ipc_compression: {
          path: 'tunables.transport.ipc_compression',
          accepted: ['none', 'lz4_frame', 'zstd'],
          configured: ['none', 'lz4_frame', 'zstd'],
          baseline_requested: transport.ipc_compression ?? null,
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        grpc_send_compression: {
          path: 'tunables.transport.grpc_send_compression',
          accepted: ['none', 'gzip', 'zstd'],
          configured: ['none', 'gzip', 'zstd'],
          baseline_requested: transport.grpc_send_compression ?? null,
          effective_default: 'none (bin/collector-chain/src/dmv_client.rs::Options)',
          client_build_supported: ['none', 'gzip', 'zstd'],
          consumer: 'ARROWCOL_CLIENT_GRPC_SEND_COMPRESSION -> bin/collector-chain/src/dmv_client.rs::send_compressed; paired with tunables.collector.limits.grpc_accept_compression',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        grpc_accept_compression: {
          path: 'tunables.transport.grpc_accept_compression',
          accepted: ['none', 'gzip', 'zstd'],
          configured: ['none', 'gzip', 'zstd'],
          baseline_requested: transport.grpc_accept_compression ?? null,
          effective_default: 'none (bin/collector-chain/src/dmv_client.rs::Options)',
          client_build_supported: ['none', 'gzip', 'zstd'],
          consumer: 'ARROWCOL_CLIENT_GRPC_ACCEPT_COMPRESSION -> bin/collector-chain/src/dmv_client.rs::accept_compressed; paired with tunables.collector.limits.grpc_send_compression',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        grpc_receiver_accept_compression: {
          path: 'tunables.collector.limits.grpc_accept_compression',
          accepted: ['none', 'gzip', 'zstd'],
          configured: ['none', 'gzip', 'zstd'],
          baseline_requested: collectorLimits.grpc_accept_compression ?? null,
          effective_default: 'none (crates/arrowcol-dataflow-components/src/config.rs::RuntimeLimitsConfig)',
          receiver_build_supported: ['none', 'gzip', 'zstd'],
          consumer: 'ARROWCOL_COLLECTOR_LIMITS_CONFIG -> crates/arrowcol-dataflow-components/src/config.rs::RuntimeLimitsConfig -> crates/arrowcol-flight/src/service.rs::into_tonic_service; paired with transport.grpc_send_compression',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        grpc_receiver_send_compression: {
          path: 'tunables.collector.limits.grpc_send_compression',
          accepted: ['none', 'gzip', 'zstd'],
          configured: ['none', 'gzip', 'zstd'],
          baseline_requested: collectorLimits.grpc_send_compression ?? null,
          effective_default: 'none (crates/arrowcol-dataflow-components/src/config.rs::RuntimeLimitsConfig)',
          receiver_build_supported: ['none', 'gzip', 'zstd'],
          consumer: 'ARROWCOL_COLLECTOR_LIMITS_CONFIG -> crates/arrowcol-dataflow-components/src/config.rs::RuntimeLimitsConfig -> crates/arrowcol-flight/src/service.rs::into_tonic_service; paired with transport.grpc_accept_compression',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
      } } : {}),
      ...(cohort.id === 'C6' ? { source_enum_coverage: {
        collector_upload_mode: {
          path: 'tunables.collector.upload.mode',
          accepted: ['put_create', 'buffered_multipart'],
          configured: [],
          baseline_requested: upload.mode ?? null,
          effective_default: 'put_create',
          excluded: {
            buffered_multipart: 'Pinned capability table marks collector multipart upload unsupported because the backend lacks conditional completion; not a scored candidate.',
          },
          pending: [],
          status: 'safety-excluded-by-pinned-backend-capability',
        },
      } } : {}),
      ...(cohort.id === 'C7' ? { source_enum_coverage: {
        input_read_strategy: {
          path: 'tunables.maintenance.input_read_strategy',
          accepted: ['memory', 'spool', 'range'],
          configured: ['memory', 'spool', 'range'],
          baseline_requested: maintenanceSeed.input_read_strategy ?? null,
          effective_default: 'memory',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        granularity: {
          path: 'tunables.maintenance.granularity',
          accepted: ['minute', 'hour', 'day'],
          configured: ['minute', 'hour', 'day'],
          baseline_requested: 'hour',
          excluded: {},
          pending: [],
          status: 'config-screened-not-runtime-qualified',
        },
        optimize_upload_mode: {
          path: 'tunables.maintenance.optimize_upload_mode',
          accepted: ['put_create', 'buffered_multipart'],
          configured: ['put_create', 'buffered_multipart'],
          baseline_requested: maintenanceSeed.optimize_upload_mode ?? null,
          effective_default: 'put_create',
          excluded: {},
          pending: [],
          coupled_setting: 'buffered_multipart candidate requests optimize_upload_buffer_bytes=8388608; source bounds are 4194304..67108864.',
          status: 'config-screened-buffered-multipart-exploratory',
        },
        operations: {
          path: 'tunables.maintenance.operations',
          accepted: ['purge', 'optimize', 'vacuum', 'checkpoint', 'prune_log'],
          configured: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'],
          excluded: {},
          pending: [],
          status: 'fixed-campaign-invariant-never-varied',
        },
      } } : {}),
      ...(cohort.id === 'C3' ? { flush_age_semantics: {
        requested_path: 'tunables.collector.batching.max_age',
        baseline_requested: batching.max_age ?? null,
        candidate_requests: ['1s', '5s', '15s', '60s'].map(value => ({ candidate: 'age-' + value, requested: value })),
        effective_readback_path: 'consumer_effective_config.batching.max_age',
        effective_status: 'requires-runtime-startup-readback',
        flush_reason_metric: 'arrowcol_pipeline_flushes_total',
        flush_reason_labels: ['age', 'target_parquet_bytes', 'min_input_bytes', 'max_input_bytes', 'row_cap', 'batch_cap', 'memory_cap', 'schema_change', 'explicit', 'shutdown_drain'],
        interpretation: 'Compare per-run counter deltas and physical file sizes; the first satisfied time, row, input-byte, or memory cap determines flush behavior.',
      } } : {}),
      runnable: false,
      config_validation: { status: 'not-validated' },
      runtime_qualification: 'unqualified',
      ...(cohort.refinement ? { refinement: cohort.refinement } : {}),
      ...(cohort.pendingCandidateSets ? { pending_candidate_sets: cohort.pendingCandidateSets } : {}),
    };
  });
}

function mergeObjects(base: JsonObject, overrides: JsonObject): JsonObject {
  const result = { ...base };
  for (const [key, value] of Object.entries(overrides)) {
    result[key] = isRecord(value) && isRecord(result[key]) ? mergeObjects(result[key] as JsonObject, value) : value;
  }
  return result;
}

function sha256(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}

function isJsonValue(value: unknown, seen = new Set<object>()): boolean {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value !== 'object' || seen.has(value)) return false;
  seen.add(value);
  const valid = Array.isArray(value)
    ? value.every(item => isJsonValue(item, seen))
    : isRecord(value) && Object.values(value).every(item => isJsonValue(item, seen));
  seen.delete(value);
  return valid;
}

function jsonPointer(path: string[]): string {
  return '/' + path.map(segment => segment.replace(/~/g, '~0').split('/').join('~1')).join('/');
}

function overrideLeafPaths(value: unknown, path: string[] = []): string[] {
  if (!isRecord(value)) return path.length > 0 ? [jsonPointer(path)] : [];
  const keys = Object.keys(value);
  if (keys.length === 0) return path.length > 0 ? [jsonPointer(path)] : [];
  return keys.flatMap(key => overrideLeafPaths(value[key], [...path, key]));
}

function changedJsonPaths(
  before: unknown, after: unknown, path: string[] = [], beforePresent = true, afterPresent = true,
): string[] {
  if (beforePresent && afterPresent && isRecord(before) && isRecord(after)) {
    const keys = [...new Set([...Object.keys(before), ...Object.keys(after)])].sort();
    return keys.flatMap(key => changedJsonPaths(
      before[key], after[key], [...path, key], Object.prototype.hasOwnProperty.call(before, key),
      Object.prototype.hasOwnProperty.call(after, key),
    ));
  }
  if (afterPresent && isRecord(after) && (!beforePresent || !isRecord(before))) {
    const keys = Object.keys(after).sort();
    if (keys.length === 0) return path.length > 0 ? [jsonPointer(path)] : [];
    return keys.flatMap(key => changedJsonPaths(undefined, after[key], [...path, key], false, true));
  }
  if (beforePresent && !afterPresent && isRecord(before)) {
    const keys = Object.keys(before).sort();
    if (keys.length === 0) return path.length > 0 ? [jsonPointer(path)] : [];
    return keys.flatMap(key => changedJsonPaths(before[key], undefined, [...path, key], true, false));
  }
  if (beforePresent === afterPresent && (!beforePresent || JSON.stringify(before) === JSON.stringify(after))) return [];
  return path.length > 0 ? [jsonPointer(path)] : ['/'];
}

export function renderThroughputBaseline(base: unknown, manifest: unknown): JsonObject {
  if (!isRecord(base) || !isRecord(manifest) || !isRecord(manifest.baseline_tunables) ||
      !isRecord(manifest.baseline_overrides) || !Array.isArray(manifest.comparisons)) {
    throw new Error('invalid base config or throughput manifest');
  }
  validateComparisons(manifest.comparisons, String(manifest.cohort ?? 'unknown'));
  if (!isRecord(base.tunables) ||
      createHash('sha256').update(JSON.stringify(base.tunables)).digest('hex') !== manifest.registry_sha256) {
    throw new Error('base tunables do not match manifest seed');
  }
  return mergeObjects(base, { ...manifest.baseline_overrides, tunables: manifest.baseline_tunables });
}

export function renderThroughputConfig(base: unknown, manifest: unknown, comparisonId: string, candidateId: string): JsonObject {
  const baseline = renderThroughputBaseline(base, manifest);
  if (!isRecord(manifest) || !Array.isArray(manifest.comparisons)) throw new Error('invalid throughput manifest');
  const comparison = manifest.comparisons.find(item => isRecord(item) && item.id === comparisonId);
  if (!isRecord(comparison) || !Array.isArray(comparison.candidates)) throw new Error('unknown comparison: ' + comparisonId);
  const selected = comparison.candidates.find(item => isRecord(item) && item.id === candidateId);
  if (!isRecord(selected) || !isRecord(selected.overrides)) throw new Error('unknown candidate: ' + candidateId);
  return mergeObjects(baseline, selected.overrides);
}

export function renderFrozenThroughputConfig(
  base: unknown, manifest: unknown, comparisonId: string, candidateId: string,
): { config: JsonObject; delta_paths: string[] } {
  if (!isRecord(base) || !isRecord(base.tunables) || !isJsonValue(base)) {
    throw new Error('frozen config must be a JSON OAT object with a tunables object');
  }
  if (!isRecord(manifest) || !Array.isArray(manifest.comparisons)) throw new Error('invalid frozen throughput manifest');
  validateComparisons(manifest.comparisons, String(manifest.cohort ?? 'unknown'));
  const comparison = manifest.comparisons.find(item => isRecord(item) && item.id === comparisonId);
  if (!isRecord(comparison) || !Array.isArray(comparison.candidates)) throw new Error('unknown comparison: ' + comparisonId);
  const selected = comparison.candidates.find(item => isRecord(item) && item.id === candidateId);
  if (!isRecord(selected) || !isRecord(selected.overrides) || !isJsonValue(selected.overrides)) {
    throw new Error('unknown candidate or non-JSON candidate overrides: ' + candidateId);
  }
  const config = mergeObjects(base, selected.overrides);
  const deltaPaths = changedJsonPaths(base, config);
  const declaredPaths = overrideLeafPaths(selected.overrides);
  const undeclaredPaths = deltaPaths.filter(path =>
    !declaredPaths.some(declared => path === declared || path.startsWith(declared + '/')));
  if (undeclaredPaths.length > 0) {
    throw new Error('candidate mutated paths outside its declared overrides: ' + undeclaredPaths.join(', '));
  }
  return { config, delta_paths: deltaPaths };
}

async function main(args: string[]): Promise<void> {
  let registryPath = resolve(root, 'docs/experiments/arrowcol-oat/oat-config.tuned.json');
  let outputDirectory = resolve(root, 'docs/experiments/arrowcol-oat');
  let manifestDirectory = resolve(root, 'docs/experiments/arrowcol-oat');
  let outputDirectoryProvided = false;
  let renderAll = false;
  let supplement: string | undefined;
  let frozenConfigPath: string | undefined;
  let frozenConfigSha256: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const option = args[index];
    if (option === '--render-all') {
      renderAll = true;
      continue;
    }
    if (option === '--frozen-config' || option === '--frozen-config-sha256') {
      const value = args[++index];
      if (!value || value.startsWith('--')) throw new Error('expected a value after ' + option);
      if (option === '--frozen-config') {
        if (frozenConfigPath !== undefined) throw new Error('duplicate --frozen-config option');
        frozenConfigPath = resolve(value);
      } else {
        if (frozenConfigSha256 !== undefined) throw new Error('duplicate --frozen-config-sha256 option');
        frozenConfigSha256 = value;
      }
      continue;
    }
    const value = args[++index];
    if (!value || value.startsWith('--')) throw new Error('expected a value after ' + option);
    if (option === '--registry') registryPath = resolve(value);
    else if (option === '--out') {
      outputDirectory = resolve(value);
      outputDirectoryProvided = true;
    } else if (option === '--manifests') manifestDirectory = resolve(value);
    else if (option === '--supplement') {
      if (value !== 'C5-grpc') throw new Error('unsupported supplemental manifest: ' + value);
      supplement = value;
    } else throw new Error('unknown option: ' + option);
  }
  const frozenRequested = frozenConfigPath !== undefined || frozenConfigSha256 !== undefined;
  if (frozenRequested && (!frozenConfigPath || !frozenConfigSha256)) {
    throw new Error('--frozen-config and --frozen-config-sha256 must be supplied together');
  }
  if (frozenRequested && !renderAll) throw new Error('frozen config rendering requires --render-all');
  let frozenConfig: JsonObject | undefined;
  let frozenConfigBytes: Buffer | undefined;
  let frozenConfigHash: string | undefined;
  if (frozenConfigPath && frozenConfigSha256) {
    if (!/^[a-f0-9]{64}$/i.test(frozenConfigSha256)) throw new Error('--frozen-config-sha256 must be a 64-character SHA-256 hex digest');
    frozenConfigBytes = await readFile(frozenConfigPath);
    frozenConfigHash = sha256(frozenConfigBytes);
    if (frozenConfigHash !== frozenConfigSha256.toLowerCase()) {
      throw new Error('frozen config SHA-256 mismatch: expected ' + frozenConfigSha256.toLowerCase() + ', got ' + frozenConfigHash);
    }
    let text: string;
    try {
      text = new TextDecoder('utf-8', { fatal: true }).decode(frozenConfigBytes);
    } catch {
      throw new Error('frozen config must be valid UTF-8 JSON');
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      throw new Error('frozen config is not valid JSON');
    }
    if (!isRecord(parsed) || !isRecord(parsed.tunables) || !isJsonValue(parsed)) {
      throw new Error('frozen config must be a JSON OAT object with a tunables object');
    }
    frozenConfig = parsed;
  }
  let registry: unknown = undefined;
  if (!frozenConfig) registry = JSON.parse(await readFile(registryPath, 'utf8'));
  if (!renderAll) {
    const generated = buildThroughputManifests(registry);
    if (supplement) {
      if (!outputDirectoryProvided || outputDirectory === manifestDirectory) {
        throw new Error('--supplement generation requires --out distinct from --manifests to preserve checked-in evidence');
      }
      const manifest = generated.find(item => item.cohort === 'C5');
      if (!manifest) throw new Error('could not generate C5 supplement');
      await mkdir(outputDirectory, { recursive: true });
      await writeFile(resolve(outputDirectory, 'oat-config-throughput-' + supplement + '.json'), JSON.stringify(manifest, null, 2) + '\n');
      return;
    }
    await mkdir(outputDirectory, { recursive: true });
    for (const manifest of generated) {
      const cohort = String(manifest.cohort);
      if (cohort === 'C5') continue;
      await writeFile(resolve(outputDirectory, 'oat-config-throughput-' + cohort + '.json'), JSON.stringify(manifest, null, 2) + '\n');
    }
    console.log('C5 is frozen; use --supplement C5-grpc to generate the additive profile.');
    return;
  }
  const selectedCohorts = cohorts.filter(cohort => !supplement || cohort.id !== 'C5');
  const manifests: Array<{ manifest: unknown; outputName: string }> = await Promise.all(selectedCohorts.map(async cohort => {
    const path = resolve(manifestDirectory, 'oat-config-throughput-' + cohort.id + '.json');
    const manifest: unknown = JSON.parse(await readFile(path, 'utf8'));
    if (!isRecord(manifest) || manifest.cohort !== cohort.id || !Array.isArray(manifest.comparisons)) {
      throw new Error('invalid manifest: ' + path);
    }
    return { manifest, outputName: cohort.id };
  }));
  if (supplement) {
    const path = resolve(manifestDirectory, 'oat-config-throughput-' + supplement + '.json');
    const manifest: unknown = JSON.parse(await readFile(path, 'utf8'));
    if (!isRecord(manifest) || manifest.cohort !== 'C5' || !Array.isArray(manifest.comparisons)) {
      throw new Error('invalid supplemental manifest: ' + path);
    }
    manifests.push({ manifest, outputName: supplement });
  }
  await mkdir(outputDirectory, { recursive: true });
  for (const selected of manifests) {
    const { manifest, outputName } = selected;
    if (!isRecord(manifest)) throw new Error('invalid manifest: ' + outputName);
    const cohort = String(manifest.cohort);
    const cohortDirectory = resolve(outputDirectory, outputName);
    await mkdir(cohortDirectory, { recursive: true });
    if (frozenConfig && frozenConfigBytes) {
      await writeFile(resolve(cohortDirectory, 'baseline.json'), frozenConfigBytes);
    } else {
      await writeFile(resolve(cohortDirectory, 'baseline.json'),
        JSON.stringify(renderThroughputBaseline(registry, manifest), null, 2) + '\n');
    }
    const provenance = frozenConfigHash ? {
      schema_version: 1,
      frozen_submitted_config_sha256: frozenConfigHash,
      baseline_config_sha256: frozenConfigHash,
      candidate_configs: [] as Array<{
        comparison_id: string; candidate_id: string; config_path: string; config_sha256: string; delta_paths: string[];
      }>,
    } : undefined;
    for (const comparison of manifest.comparisons as Comparison[]) {
      if (!isRecord(comparison) || typeof comparison.id !== 'string' || !/^[a-z0-9-]+$/.test(comparison.id) ||
          !Array.isArray(comparison.candidates)) throw new Error('invalid comparison in ' + cohort);
      const directory = resolve(cohortDirectory, comparison.id);
      await mkdir(directory, { recursive: true });
      for (const selectedCandidate of comparison.candidates) {
        if (!isRecord(selectedCandidate) || typeof selectedCandidate.id !== 'string' || !/^[a-z0-9_-]+$/.test(selectedCandidate.id)) {
          throw new Error('invalid candidate in ' + outputName + '/' + comparison.id);
        }
        const frozenRender = frozenConfig
          ? renderFrozenThroughputConfig(frozenConfig, manifest, comparison.id, selectedCandidate.id)
          : undefined;
        const config = frozenRender?.config ?? renderThroughputConfig(registry, manifest, comparison.id, selectedCandidate.id);
        const outputBytes = Buffer.from(JSON.stringify(config, null, 2) + '\n');
        const outputPath = resolve(directory, selectedCandidate.id + '.json');
        await writeFile(outputPath, outputBytes);
        if (provenance && frozenRender) {
          provenance.candidate_configs.push({
            comparison_id: comparison.id,
            candidate_id: selectedCandidate.id,
            config_path: comparison.id + '/' + selectedCandidate.id + '.json',
            config_sha256: sha256(outputBytes),
            delta_paths: frozenRender.delta_paths,
          });
        }
      }
    }
    if (provenance) {
      await writeFile(resolve(cohortDirectory, 'render-provenance.json'), JSON.stringify(provenance, null, 2) + '\n');
    }
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
