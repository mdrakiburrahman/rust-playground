import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

type JsonObject = Record<string, unknown>;
type Candidate = { id: string; overrides: JsonObject };
type Comparison = { id: string; factor: string; candidates: Candidate[]; candidate_limit: number };
type PendingCandidateSet = { id: string; factor: string; status: string; candidates: Candidate[] };
type Cohort = { id: string; comparisons: Comparison[]; refinement?: JsonObject; pendingCandidateSets?: PendingCandidateSet[] };

const root = resolve(fileURLToPath(new URL('../..', import.meta.url)));
const maintenance = { enabled: true, apply: true, operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'] };
const candidate = (id: string, value: JsonObject, rootOverrides: JsonObject = {}): Candidate => ({
  id, overrides: { ...rootOverrides, tunables: value },
});
const compare = (id: string, factor: string, candidates: Candidate[], candidateLimit = 4): Comparison => ({
  id, factor, candidates, candidate_limit: candidateLimit,
});
const cohorts: Cohort[] = [
  { id: 'C1', comparisons: [
    compare('codec', 'parquet_writer.defaults.compression', [
      candidate('uncompressed', { parquet_writer: { defaults: { compression: 'uncompressed', compression_level: null } } }),
      candidate('snappy', { parquet_writer: { defaults: { compression: 'snappy', compression_level: null } } }),
      candidate('lz4-raw', { parquet_writer: { defaults: { compression: 'lz4_raw', compression_level: null } } }),
      candidate('gzip', { parquet_writer: { defaults: { compression: 'gzip', compression_level: null } } }),
      candidate('brotli', { parquet_writer: { defaults: { compression: 'brotli', compression_level: null } } }),
      candidate('zstd-1', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 1 } } }),
      candidate('zstd-3', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 3 } } }),
      candidate('zstd-6', { parquet_writer: { defaults: { compression: 'zstd', compression_level: 6 } } }),
    ], 8),
  ], refinement: {
    comparison: 'codec',
    factor: 'parquet_writer.defaults.compression_level',
    trigger: 'zstd wins the coarse codec comparison',
    selection: 'winner plus up to three adjacent levels accepted by the E1 registry bounds',
    max_candidates: 4,
  } },
  { id: 'C2', comparisons: [compare('dictionary-encoding', 'parquet_writer.columns', [
    candidate('database-name-dictionary-off', { parquet_writer: { columns: { database_name: { dictionary_enabled: false } } } }),
    candidate('database-name-dictionary-on', { parquet_writer: { columns: { database_name: { dictionary_enabled: true } } } }),
    candidate('session-id-delta', { parquet_writer: { columns: { session_id: { dictionary_enabled: false, encoding: 'delta_binary_packed' } } } }),
    candidate('session-id-byte-stream-split', { parquet_writer: { columns: { session_id: { dictionary_enabled: false, encoding: 'byte_stream_split' } } } }),
  ])] },
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
  ] },
  { id: 'C4', comparisons: [compare('sort-key', 'collector.layout.sort_columns', [
    candidate('none', { collector: { layout: { enabled: false, sort_columns: [] } }, parquet_writer: { sorting_columns: [] } }),
    candidate('sample-time', { collector: { layout: { enabled: true, sort_columns: ['sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
    candidate('replica-sample-time', { collector: { layout: { enabled: true, sort_columns: ['replica_server_name', 'sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'replica_server_name', descending: false, nulls_first: false }, { path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
    candidate('group-replica-sample-time', { collector: { layout: { enabled: true, sort_columns: ['availability_group_id', 'replica_server_name', 'sample_time_utc'] } }, parquet_writer: { sorting_columns: [{ path: 'availability_group_id', descending: false, nulls_first: false }, { path: 'replica_server_name', descending: false, nulls_first: false }, { path: 'sample_time_utc', descending: false, nulls_first: false }] } }),
  ])] },
  { id: 'C5', comparisons: [
    compare('workers', 'tunables.client.workers', [8, 16, 32, 64].map(value => candidate('workers-' + value, {
      client: { workers: value, streams_per_worker: 1, connections_per_worker: 1 },
    }))),
    compare('streams', 'tunables.client.streams_per_worker', [1, 2, 4].map(value => candidate('streams-' + value, {
      client: { workers: 16, streams_per_worker: value, connections_per_worker: 1 },
    }))),
    compare('connections', 'tunables.client.connections_per_worker', [1, 2, 4].map(value => candidate('connections-' + value, {
      client: { workers: 16, streams_per_worker: 1, connections_per_worker: value },
    }))),
    compare('corpus', 'tunables.client.corpus_rows', [16384, 65536, 262144].map(value => candidate('corpus-' + value, { client: { corpus_rows: value } }))),
    compare('interval', 'tunables.client.interval_us', [0, 1000, 10000].map(value => candidate('interval-' + value, { client: { interval_us: value } }))),
    compare('ipc-compression', 'tunables.transport.ipc_compression', ['none', 'lz4_frame', 'zstd'].map(value => candidate('ipc-' + value, { transport: { ipc_compression: value } }))),
  ] },
  { id: 'C6', comparisons: [
    compare('dfe-concurrency', 'dfe_worker_concurrency', [1, 2, 4, 8].map(value => candidate('dfe-' + value, {}, { dfe_worker_concurrency: [value] }))),
    compare('cpu-workers', 'tunables.collector.runtime.cpu_workers', [1, 2, 4].map(value => candidate('cpu-' + value, { collector: { runtime: { cpu_workers: value } } }))),
    compare('upload-concurrency', 'tunables.collector.runtime.upload_limit', [1, 4, 8].map(value => candidate('upload-' + value, { collector: { runtime: { upload_limit: value } } }))),
    compare('flush-concurrency', 'tunables.collector.batching.max_concurrent_flushes', [1, 2, 4].map(value => candidate('flush-' + value, { collector: { batching: { max_concurrent_flushes: value } } }))),
  ] },
  { id: 'C7', comparisons: [
    compare('maintenance-cadence', 'tunables.maintenance.interval_secs', [30, 60, 120].map(value => candidate('cadence-' + value, { maintenance: { interval_secs: value } }))),
    compare('small-file-threshold', 'tunables.maintenance.small_file_bytes', [16777216, 33554432, 67108864].map(value => candidate('small-file-' + value, { maintenance: { small_file_bytes: value } }))),
    compare('retry-budget', 'tunables.maintenance.commit_attempts', [1, 3, 5].map(value => candidate('attempts-' + value, { maintenance: { commit_attempts: value } }))),
    compare('checkpoint-interval', 'tunables.maintenance.checkpoint_every_commits', [64, 256, 1024].map(value => candidate('checkpoint-' + value, { maintenance: { checkpoint_every_commits: value } }))),
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

export function buildThroughputManifests(registry: unknown): JsonObject[] {
  if (!isRecord(registry) || !isRecord(registry.tunables)) throw new Error('registry must contain the PR125 tunables object');
  const tunables = registry.tunables;
  const registryHash = createHash('sha256').update(JSON.stringify(tunables)).digest('hex');
  return cohorts.map(cohort => {
    for (const comparison of cohort.comparisons) {
      if (comparison.candidates.length > comparison.candidate_limit) throw new Error(cohort.id + '/' + comparison.id + ' exceeds its candidate limit');
    }
    return {
      schema_version: 1,
      cohort: cohort.id,
      mode: 'local',
      base_config: 'oat-config.tuned.json',
      registry_source: 'PR125 arrowcol-tunables profile',
      registry_status: 'pending-E1-registry',
      registry_sha256: registryHash,
      box_scope: 'one box for every comparison in this cohort',
      campaign: 'single_hot_table_campaign',
      campaign_profile: 'single_hot_table',
      invariants: {
        table: 'sqlserver_availability_replica_states',
        replicas: [1],
        wal_enabled: false,
        maintenance: { ...maintenance, continuous: true },
        compare_within_box: true,
        one_variable_at_a_time: true,
      },
      baseline_tunables: {
        ...tunables,
        maintenance: {
          ...(isRecord(tunables.maintenance) ? tunables.maintenance : {}),
          ...maintenance,
        },
      },
      comparisons: cohort.comparisons,
      ...(cohort.refinement ? { refinement: cohort.refinement } : {}),
      ...(cohort.pendingCandidateSets ? { pending_candidate_sets: cohort.pendingCandidateSets } : {}),
    };
  });
}

async function main(args: string[]): Promise<void> {
  let registryPath = resolve(root, 'docs/experiments/arrowcol-oat/oat-config.tuned.json');
  let outputDirectory = resolve(root, 'docs/experiments/arrowcol-oat');
  for (let index = 0; index < args.length; index++) {
    const option = args[index];
    const value = args[++index];
    if (!value || value.startsWith('--')) throw new Error('expected a value after ' + option);
    if (option === '--registry') registryPath = resolve(value);
    else if (option === '--out') outputDirectory = resolve(value);
    else throw new Error('unknown option: ' + option);
  }
  const registry: unknown = JSON.parse(await readFile(registryPath, 'utf8'));
  const manifests = buildThroughputManifests(registry);
  await mkdir(outputDirectory, { recursive: true });
  for (const manifest of manifests) {
    const cohort = String(manifest.cohort);
    await writeFile(resolve(outputDirectory, 'oat-config-throughput-' + cohort + '.json'), JSON.stringify(manifest, null, 2) + '\n');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
