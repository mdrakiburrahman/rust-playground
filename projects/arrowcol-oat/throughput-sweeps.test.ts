import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import test from 'node:test';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { buildThroughputManifests, renderThroughputBaseline, renderThroughputConfig } from './throughput-sweeps.js';

function at(value: unknown, path: string[]): unknown {
  let current = value;
  for (const key of path) {
    if (typeof current !== 'object' || current === null || Array.isArray(current)) return undefined;
    current = (current as Record<string, unknown>)[key];
  }
  return current;
}

test('screens applicable BYTE_ARRAY encodings with an explicit matched plain control', () => {
  const base = { platform: { kind: 'local' }, tunables: { parquet_writer: { columns: {} } } };
  const manifest = buildThroughputManifests(base).find(item => item.cohort === 'C2')!;
  const comparison = (manifest.comparisons as Array<{ id: string; factor: string; candidates: Array<{ id: string }> }>)
    .find(item => item.id === 'string-encoding');
  assert.ok(comparison);
  assert.equal(comparison.factor, 'parquet_writer.columns.availability_group_name.encoding');
  assert.deepEqual(comparison.candidates.map(item => item.id), ['plain', 'delta-length-byte-array', 'delta-byte-array']);
  const configs = comparison.candidates.map(item => renderThroughputConfig(base, manifest, comparison.id, item.id));
  const columns = configs.map(config => at(config, ['tunables', 'parquet_writer', 'columns', 'availability_group_name']));
  assert.deepEqual(columns, ['plain', 'delta_length_byte_array', 'delta_byte_array'].map(encoding => ({
    dictionary_enabled: false, encoding,
  })));
  const withoutEncoding = configs.map(config => {
    const copy = structuredClone(config);
    const column = at(copy, ['tunables', 'parquet_writer', 'columns', 'availability_group_name']) as Record<string, unknown>;
    delete column.encoding;
    return copy;
  });
  assert.deepEqual(withoutEncoding[0], withoutEncoding[1]);
  assert.deepEqual(withoutEncoding[1], withoutEncoding[2]);
  assert.deepEqual(manifest.parquet_enum_coverage, {
    kind: 'value_encoding',
    accepted: ['plain', 'rle', 'delta_binary_packed', 'delta_length_byte_array', 'delta_byte_array', 'byte_stream_split'],
    configured: ['plain', 'delta_binary_packed', 'delta_length_byte_array', 'delta_byte_array', 'byte_stream_split'],
    excluded: { rle: 'BOOLEAN-only; frozen table has no Boolean column' },
    pending: [],
    status: 'configured-not-runtime-screened',
  });
});

test('generates the C3 flush-age comparison as a single-factor sweep', () => {
  const base = { platform: { kind: 'local' }, tunables: { collector: { batching: { max_age: '1s' } } } };
  const manifest = buildThroughputManifests(base).find(item => item.cohort === 'C3')!;
  const comparisons = manifest.comparisons as Array<{
    id: string; factor: string; candidate_limit: number; candidates: Array<{ id: string; overrides: Record<string, unknown> }>;
  }>;
  const age = comparisons.find(comparison => comparison.id === 'flush-age');
  assert.ok(age);
  assert.equal(age.factor, 'collector.batching.max_age');
  assert.equal(age.candidate_limit, 4);
  assert.deepEqual(age.candidates.map(item => [
    item.id, at(item.overrides, ['tunables', 'collector', 'batching', 'max_age']),
  ]), [['age-1s', '1s'], ['age-5s', '5s'], ['age-15s', '15s'], ['age-60s', '60s']]);
  const configs = age.candidates.map(item => renderThroughputConfig(base, manifest, age.id, item.id));
  const fixedSettings = configs.map(config => {
    const copy = structuredClone(config);
    delete (at(copy, ['tunables', 'collector', 'batching']) as Record<string, unknown>).max_age;
    return copy;
  });
  assert.deepEqual(fixedSettings.slice(1), [fixedSettings[0], fixedSettings[0], fixedSettings[0]]);
  assert.equal((manifest.flush_age_semantics as { baseline_requested: unknown }).baseline_requested, '1s');
  const nonDefaultManifest = buildThroughputManifests({
    tunables: { collector: { batching: { max_age: '5s' } } },
  }).find(item => item.cohort === 'C3')!;
  assert.equal((nonDefaultManifest.flush_age_semantics as { baseline_requested: unknown }).baseline_requested, '5s');
  const omittedManifest = buildThroughputManifests({ tunables: { collector: { batching: {} } } })
    .find(item => item.cohort === 'C3')!;
  assert.equal((omittedManifest.flush_age_semantics as { baseline_requested: unknown }).baseline_requested, null);
});

test('covers applicable enum factors and records unsupported typed values without promoting them', () => {
  const base = {
    platform: { kind: 'local' },
    tunables: { collector: { batching: {} }, maintenance: { operations: ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log'] } },
  };
  const manifests = buildThroughputManifests(base);
  const getManifest = (cohort: string) => manifests.find(item => item.cohort === cohort)!;
  const getComparison = (cohort: string, id: string) =>
    (getManifest(cohort).comparisons as Array<{ id: string; factor: string; candidates: Array<{ id: string; qualification?: string }> }>)
      .find(item => item.id === id)!;
  const coverage = (cohort: string) => getManifest(cohort).source_enum_coverage as Record<string, {
    accepted: string[]; configured: string[]; excluded: Record<string, string>; pending: string[]; status: string;
    baseline_requested?: unknown; effective_default?: string; consumer?: string; client_build_supported?: string[]; receiver_build_supported?: string[];
  }>;
  const renderWithout = (cohort: string, comparison: string, candidates: string[], paths: string[][]) =>
    candidates.map(candidate => {
      const config = renderThroughputConfig(base, getManifest(cohort), comparison, candidate);
      for (const path of paths) {
        const parent = at(config, path.slice(0, -1));
        assert.ok(parent && typeof parent === 'object' && !Array.isArray(parent));
        delete (parent as Record<string, unknown>)[path.at(-1)!];
      }
      return config;
    });

  const agePolicy = getComparison('C3', 'age-flush-policy');
  assert.equal(agePolicy.factor, 'collector.batching.age_flush_policy');
  const agePolicyIds = ['age-policy-immediate', 'age-policy-drain_ready'];
  assert.deepEqual(agePolicy.candidates.map(item => item.id), agePolicyIds);
  const agePolicyConfigs = renderWithout('C3', agePolicy.id, agePolicyIds, [['tunables', 'collector', 'batching', 'age_flush_policy']]);
  assert.deepEqual(agePolicyConfigs[0], agePolicyConfigs[1]);
  assert.deepEqual(coverage('C3').age_flush_policy.configured, ['immediate', 'drain_ready']);

  const inputRead = getComparison('C7', 'input-read-strategy');
  assert.equal(inputRead.factor, 'tunables.maintenance.input_read_strategy');
  const inputIds = ['input-memory', 'input-spool', 'input-range'];
  assert.deepEqual(inputRead.candidates.map(item => item.id), inputIds);
  assert.deepEqual(inputIds.map(id => at(renderThroughputConfig(base, getManifest('C7'), inputRead.id, id),
    ['tunables', 'maintenance', 'input_read_strategy'])), ['memory', 'spool', 'range']);
  const inputConfigs = renderWithout('C7', inputRead.id, inputIds, [['tunables', 'maintenance', 'input_read_strategy']]);
  assert.deepEqual(inputConfigs[0], inputConfigs[1]);
  assert.deepEqual(inputConfigs[1], inputConfigs[2]);
  assert.deepEqual(coverage('C7').input_read_strategy.accepted, ['memory', 'spool', 'range']);
  const granularity = getComparison('C7', 'granularity');
  const granularityIds = ['granularity-minute', 'granularity-hour', 'granularity-day'];
  assert.deepEqual(granularity.candidates.map(item => item.id), granularityIds);
  assert.deepEqual(granularityIds.map(id => at(renderThroughputConfig(base, getManifest('C7'), granularity.id, id),
    ['tunables', 'maintenance', 'granularity'])), ['minute', 'hour', 'day']);
  const granularityConfigs = renderWithout('C7', granularity.id, granularityIds, [['tunables', 'maintenance', 'granularity']]);
  assert.deepEqual(granularityConfigs[0], granularityConfigs[1]);
  assert.deepEqual(granularityConfigs[1], granularityConfigs[2]);
  const optimizeUpload = getComparison('C7', 'optimize-upload-mode');
  const uploadIds = ['put-create', 'buffered-multipart'];
  assert.deepEqual(optimizeUpload.candidates.map(item => item.id), uploadIds);
  assert.equal(optimizeUpload.candidates[1]!.qualification, 'exploratory');
  const bufferedUpload = renderThroughputConfig(base, getManifest('C7'), optimizeUpload.id, 'buffered-multipart');
  assert.equal(at(bufferedUpload, ['tunables', 'maintenance', 'optimize_upload_buffer_bytes']), 8_388_608);
  const uploadConfigs = renderWithout('C7', optimizeUpload.id, uploadIds, [
    ['tunables', 'maintenance', 'optimize_upload_mode'],
    ['tunables', 'maintenance', 'optimize_upload_buffer_bytes'],
  ]);
  assert.deepEqual(uploadConfigs[0], uploadConfigs[1]);
  assert.deepEqual(coverage('C7').operations.configured, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);

  const dictionary = coverage('C2').dictionary_handling;
  assert.deepEqual(dictionary.accepted, ['hydrate', 'resend']);
  assert.deepEqual(dictionary.configured, []);
  assert.equal(dictionary.baseline_requested, null);
  assert.match(dictionary.effective_default!, /hydrate/);
  assert.match(dictionary.consumer!, /ARROWCOL_CLIENT_DICTIONARY_HANDLING/);
  assert.match(dictionary.excluded.resend!, /plain DMV schema/);
  const clientAndTransport = coverage('C5');
  assert.deepEqual(clientAndTransport.ipc_compression.configured, ['none', 'lz4_frame', 'zstd']);
  assert.deepEqual(clientAndTransport.data_mode.accepted, ['shared', 'independent']);
  assert.deepEqual(clientAndTransport.data_mode.configured, ['shared', 'independent']);
  assert.deepEqual(clientAndTransport.data_mode.excluded, {});
  assert.match(clientAndTransport.data_mode.consumer!, /ARROWCOL_CLIENT_DATA_MODE/);
  assert.equal(clientAndTransport.data_mode.status, 'config-screened-not-runtime-qualified');
  assert.deepEqual(clientAndTransport.grpc_send_compression.accepted, ['none', 'gzip', 'zstd']);
  assert.deepEqual(clientAndTransport.grpc_send_compression.configured, ['none', 'gzip', 'zstd']);
  assert.deepEqual(clientAndTransport.grpc_send_compression.baseline_requested, null);
  assert.deepEqual(clientAndTransport.grpc_send_compression.client_build_supported, ['none', 'gzip', 'zstd']);
  assert.deepEqual(clientAndTransport.grpc_send_compression.excluded, {});
  assert.deepEqual(clientAndTransport.grpc_accept_compression.accepted, ['none', 'gzip', 'zstd']);
  assert.deepEqual(clientAndTransport.grpc_accept_compression.configured, ['none', 'gzip', 'zstd']);
  assert.deepEqual(clientAndTransport.grpc_accept_compression.baseline_requested, null);
  assert.deepEqual(clientAndTransport.grpc_accept_compression.excluded, {});
  for (const field of ['grpc_receiver_accept_compression', 'grpc_receiver_send_compression']) {
    assert.deepEqual(clientAndTransport[field].accepted, ['none', 'gzip', 'zstd']);
    assert.deepEqual(clientAndTransport[field].configured, ['none', 'gzip', 'zstd']);
    assert.deepEqual(clientAndTransport[field].excluded, {});
    assert.deepEqual(clientAndTransport[field].receiver_build_supported, ['none', 'gzip', 'zstd']);
  }
  const collectorUpload = coverage('C6').collector_upload_mode;
  assert.deepEqual(collectorUpload.configured, []);
  assert.equal(collectorUpload.baseline_requested, null);
  assert.equal(collectorUpload.effective_default, 'put_create');
  assert.match(collectorUpload.excluded.buffered_multipart!, /lacks conditional completion/);

  const requested = buildThroughputManifests({ tunables: { transport: {
    grpc_send_compression: 'gzip', grpc_accept_compression: 'zstd',
  } } }).find(item => item.cohort === 'C5')!;
  const requestedCoverage = requested.source_enum_coverage as Record<string, { baseline_requested: unknown }>;
  assert.equal(requestedCoverage.grpc_send_compression.baseline_requested, 'gzip');
  assert.equal(requestedCoverage.grpc_accept_compression.baseline_requested, 'zstd');
});

test('renders C5 candidates against the committed seed without changing non-factor axes', () => {
  const seed = JSON.parse(readFileSync(new URL('../../docs/experiments/arrowcol-oat/oat-config.tuned.json', import.meta.url), 'utf8')) as unknown;
  const manifests = buildThroughputManifests(seed);
  const manifest = manifests.find(item => item.cohort === 'C5')!;
  const successor = JSON.parse(readFileSync(new URL('../../docs/experiments/arrowcol-oat/oat-config-throughput-C5-grpc.json', import.meta.url), 'utf8')) as Record<string, unknown>;
  assert.deepEqual(successor.comparisons, manifest.comparisons);
  assert.deepEqual(successor.source_enum_coverage, manifest.source_enum_coverage);
  assert.equal(successor.runnable, false);
  assert.equal(successor.runtime_qualification, 'unqualified');
  const baseline = renderThroughputBaseline(seed, manifest);
  const baselineClient = at(baseline, ['tunables', 'client']) as Record<string, unknown>;
  assert.deepEqual({
    workers: baselineClient.workers,
    streams_per_worker: baselineClient.streams_per_worker,
    connections_per_worker: baselineClient.connections_per_worker,
  }, { workers: 2, streams_per_worker: 2, connections_per_worker: 1 });
  const comparisons = manifest.comparisons as Array<{
    id: string; factor: string; candidates: Array<{ id: string }>; candidate_limit: number;
    matched_control?: string; comparison_family?: string; comparison_type?: string; selection_prerequisite?: string;
  }>;
  assert.ok(comparisons.every(comparison => comparison.candidate_limit === 4 && comparison.candidates.length <= 4));
  const axisCases = [
    { id: 'workers', factor: 'tunables.client.workers', candidates: ['workers-8', 'workers-16', 'workers-32', 'workers-64'], leaf: 'workers' },
    { id: 'streams', factor: 'tunables.client.streams_per_worker', candidates: ['streams-1', 'streams-2', 'streams-4'], leaf: 'streams_per_worker' },
    { id: 'connections', factor: 'tunables.client.connections_per_worker', candidates: ['connections-1', 'connections-2', 'connections-4'], leaf: 'connections_per_worker' },
  ];
  for (const axis of axisCases) {
    const comparison = comparisons.find(item => item.id === axis.id)!;
    assert.equal(comparison.factor, axis.factor);
    assert.deepEqual(comparison.candidates.map(item => item.id), axis.candidates);
    for (const candidate of comparison.candidates) {
      const client = at(renderThroughputConfig(seed, manifest, comparison.id, candidate.id), ['tunables', 'client']) as Record<string, unknown>;
      const candidateWithoutFactor = structuredClone(client);
      const baselineWithoutFactor = structuredClone(baselineClient);
      delete candidateWithoutFactor[axis.leaf];
      delete baselineWithoutFactor[axis.leaf];
      assert.deepEqual(candidateWithoutFactor, baselineWithoutFactor, axis.id + ' changed a non-factor client setting');
    }
  }
  const dataMode = comparisons.find(item => item.id === 'data-mode')!;
  assert.deepEqual(dataMode.candidates.map(item => item.id), ['data-mode-shared', 'data-mode-independent']);
  for (const candidate of dataMode.candidates) {
    const client = structuredClone(at(renderThroughputConfig(seed, manifest, dataMode.id, candidate.id), ['tunables', 'client'])) as Record<string, unknown>;
    delete client.data_mode;
    const baselineWithoutMode = structuredClone(baselineClient);
    delete baselineWithoutMode.data_mode;
    assert.deepEqual(client, baselineWithoutMode);
  }
  const grpcComparisonCases = [
    { comparison: 'grpc-client-to-receiver', candidates: ['grpc-none', 'grpc-client-to-receiver-gzip', 'grpc-client-to-receiver-zstd'] },
    { comparison: 'grpc-receiver-to-client', candidates: ['grpc-none', 'grpc-receiver-to-client-gzip', 'grpc-receiver-to-client-zstd'] },
    { comparison: 'grpc-mixed-directions', candidates: ['grpc-none', 'grpc-mixed-request-gzip-response-zstd', 'grpc-mixed-request-zstd-response-gzip'] },
  ];
  const mixedComparison = comparisons.find(item => item.id === 'grpc-mixed-directions')!;
  assert.equal(mixedComparison.comparison_type, 'interaction-top-pick');
  assert.match(mixedComparison.selection_prerequisite!, /independently.*top picks.*not score as single-factor evidence/);
  for (const id of ['grpc-client-to-receiver', 'grpc-receiver-to-client']) {
    assert.notEqual(comparisons.find(item => item.id === id)!.comparison_type, 'interaction-top-pick');
  }
  const expectedGrpc = [
    { comparison: 'grpc-client-to-receiver', id: 'grpc-client-to-receiver-gzip', send: 'gzip', accept: 'none', receiverAccept: 'gzip', receiverSend: 'none' },
    { comparison: 'grpc-client-to-receiver', id: 'grpc-client-to-receiver-zstd', send: 'zstd', accept: 'none', receiverAccept: 'zstd', receiverSend: 'none' },
    { comparison: 'grpc-receiver-to-client', id: 'grpc-receiver-to-client-gzip', send: 'none', accept: 'gzip', receiverAccept: 'none', receiverSend: 'gzip' },
    { comparison: 'grpc-receiver-to-client', id: 'grpc-receiver-to-client-zstd', send: 'none', accept: 'zstd', receiverAccept: 'none', receiverSend: 'zstd' },
    { comparison: 'grpc-mixed-directions', id: 'grpc-mixed-request-gzip-response-zstd', send: 'gzip', accept: 'zstd', receiverAccept: 'gzip', receiverSend: 'zstd' },
    { comparison: 'grpc-mixed-directions', id: 'grpc-mixed-request-zstd-response-gzip', send: 'zstd', accept: 'gzip', receiverAccept: 'zstd', receiverSend: 'gzip' },
  ];
  const withoutGrpcSettings = (value: unknown): unknown => {
    const config = structuredClone(value) as { tunables: Record<string, unknown> };
    const transport = config.tunables.transport as Record<string, unknown>;
    delete transport.grpc_send_compression;
    delete transport.grpc_accept_compression;
    const collector = config.tunables.collector as Record<string, unknown>;
    const limits = collector.limits as Record<string, unknown> | undefined;
    if (limits) {
      delete limits.grpc_accept_compression;
      delete limits.grpc_send_compression;
      if (Object.keys(limits).length === 0) delete collector.limits;
    }
    return config;
  };
  const grpcBaseline = renderThroughputConfig(seed, manifest, 'grpc-client-to-receiver', 'grpc-none');
  for (const group of grpcComparisonCases) {
    const comparison = comparisons.find(item => item.id === group.comparison)!;
    assert.equal(comparison.candidate_limit, 4);
    assert.equal(comparison.matched_control, 'grpc-none');
    assert.equal(comparison.comparison_family, 'grpc-direction');
    assert.deepEqual(comparison.candidates.map(item => item.id), group.candidates);
    assert.deepEqual(renderThroughputConfig(seed, manifest, group.comparison, 'grpc-none'), grpcBaseline);
  }
  for (const expected of expectedGrpc) {
    const config = renderThroughputConfig(seed, manifest, expected.comparison, expected.id);
    assert.equal(at(config, ['tunables', 'transport', 'grpc_send_compression']), expected.send);
    assert.equal(at(config, ['tunables', 'transport', 'grpc_accept_compression']), expected.accept);
    assert.equal(at(config, ['tunables', 'collector', 'limits', 'grpc_accept_compression']), expected.receiverAccept);
    assert.equal(at(config, ['tunables', 'collector', 'limits', 'grpc_send_compression']), expected.receiverSend);
    assert.deepEqual(withoutGrpcSettings(config), withoutGrpcSettings(baseline), expected.id + ' changed a non-gRPC setting');
  }
});

test('generates bounded local single-writer manifests from the tunables registry', () => {
  const manifests = buildThroughputManifests({ tunables: { maintenance: { enabled: true } } });
  assert.deepEqual(manifests.map(manifest => manifest.cohort), ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8']);
  for (const manifest of manifests) {
    assert.equal(manifest.mode, 'local');
    assert.equal(manifest.campaign, 'single_hot_table_campaign');
    assert.equal(manifest.campaign_profile, 'single_hot_table');
    assert.equal(manifest.registry_status, 'seed-only-not-capability-registry');
    assert.equal(manifest.runnable, false);
    assert.equal(manifest.runtime_qualification, 'unqualified');
    const invariants = manifest.invariants as { table: string; replicas: number[]; wal_enabled: boolean; maintenance: { enabled: boolean; apply: boolean; continuous: boolean; operations: string[] } };
    assert.equal(invariants.table, 'sqlserver_availability_replica_states');
    assert.deepEqual(invariants.replicas, [1]);
    assert.equal(invariants.wal_enabled, false);
    assert.equal(invariants.maintenance.enabled, true);
    assert.equal(invariants.maintenance.apply, true);
    assert.equal(invariants.maintenance.continuous, true);
    assert.deepEqual(invariants.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
    const comparisons = manifest.comparisons as Array<{
      id: string; candidates: Array<{ id: string; overrides: Record<string, unknown> }>;
      candidate_limit: number; matched_control?: string; comparison_family?: string;
    }>;
    assert.ok(comparisons.every(comparison => comparison.candidate_limit === 4 && comparison.candidates.length <= 4));
    if (manifest.cohort === 'C1') {
      const codecGroups = comparisons.filter(comparison => comparison.comparison_family === 'codec');
      assert.deepEqual(codecGroups.map(comparison => comparison.id), [
        'codec-zstd-levels', 'codec-snappy-lz4raw', 'codec-gzip-brotli',
      ]);
      assert.ok(codecGroups.every(comparison => comparison.matched_control === 'uncompressed'));
      assert.deepEqual(codecGroups.map(comparison => comparison.candidates[0]!.id), [
        'uncompressed', 'uncompressed', 'uncompressed',
      ]);
      const controlOverrides = codecGroups.map(comparison => comparison.candidates[0]!.overrides);
      assert.deepEqual(controlOverrides[1], controlOverrides[0]);
      assert.deepEqual(controlOverrides[2], controlOverrides[0]);
      const codecCandidates = codecGroups.flatMap(comparison => comparison.candidates.slice(1));
      assert.deepEqual(codecCandidates.map(item => item.id), [
        'zstd-1', 'zstd-3', 'zstd-6', 'snappy', 'lz4-raw', 'gzip', 'brotli',
      ]);
      assert.deepEqual(codecCandidates.map(item => [
        at(item.overrides, ['tunables', 'parquet_writer', 'defaults', 'compression']),
        at(item.overrides, ['tunables', 'parquet_writer', 'defaults', 'compression_level']),
      ]), [
        ['zstd', 1], ['zstd', 3], ['zstd', 6], ['snappy', null], ['lz4_raw', null], ['gzip', null], ['brotli', null],
      ]);
      assert.equal((manifest.refinement as { max_candidates: number }).max_candidates, 4);
    }
    if (manifest.cohort === 'C2') {
      const columns = comparisons.find(comparison => comparison.id === 'dictionary-encoding')!;
      assert.deepEqual(columns.candidates.map(item => item.id), [
        'availability-group-name-dictionary-off', 'availability-group-name-dictionary-on',
        'last-connect-error-delta-binary-packed', 'last-connect-error-byte-stream-split',
      ]);
      assert.deepEqual(columns.candidates.map(item => Object.keys(at(item.overrides, ['tunables', 'parquet_writer', 'columns']) as object)), [
        ['availability_group_name'], ['availability_group_name'], ['last_connect_error_number'], ['last_connect_error_number'],
      ]);
    }
    if (manifest.cohort === 'C3') {
      const rows = comparisons.find(comparison => comparison.id === 'max-rows')!;
      assert.deepEqual(rows.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'max_rows'])), [16384, 65536, 131072, 1000000]);
      assert.deepEqual(rows.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'max_rows_per_flush'])), [16384, 65536, 131072, 1000000]);
      const groups = comparisons.find(comparison => comparison.id === 'row-groups')!;
      assert.deepEqual(groups.candidates.map(item => at(item.overrides, ['tunables', 'parquet_writer', 'max_row_group_rows'])), [16384, 65536, 131072, 1000000]);
      const caps = comparisons.find(comparison => comparison.id === 'rollover-cap')!;
      const capValues = [134217728, 268435456, 536870912];
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'max_input_bytes'])), capValues);
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'max_arrow_memory_per_flush'])), capValues);
      assert.deepEqual(caps.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'batching', 'adaptive_file', 'max_input_bytes'])), capValues);
    }
    if (manifest.cohort === 'C4') {
      const sort = comparisons.find(comparison => comparison.id === 'sort-key')!;
      assert.deepEqual(sort.candidates.map(item => item.id), ['none', 'sample-time', 'replica-sample-time', 'group-replica-sample-time']);
      assert.equal(at(sort.candidates[0]!.overrides, ['tunables', 'collector', 'layout', 'enabled']), true);
      assert.deepEqual(sort.candidates.map(item => at(item.overrides, ['tunables', 'collector', 'layout', 'sort_columns'])), [
        [], ['sample_time_utc'], ['replica_server_name', 'sample_time_utc'],
        ['availability_group_id', 'replica_server_name', 'sample_time_utc'],
      ]);
      assert.deepEqual(at(sort.candidates[0]!.overrides, ['tunables', 'parquet_writer', 'sorting_columns']), []);
      assert.deepEqual(sort.candidates.map(item => (at(item.overrides, ['tunables', 'parquet_writer', 'sorting_columns']) as Array<{ path: string; descending: boolean; nulls_first: boolean }>).map(column => [column.path, column.descending, column.nulls_first])), [
        [], [['sample_time_utc', false, false]],
        [['replica_server_name', false, false], ['sample_time_utc', false, false]],
        [['availability_group_id', false, false], ['replica_server_name', false, false], ['sample_time_utc', false, false]],
      ]);
    }
    if (manifest.cohort === 'C8') {
      const pending = manifest.pending_candidate_sets as Array<{ id: string; status: string; candidates: unknown[] }>;
      assert.deepEqual(pending.map(item => [item.id, item.status]), [['staging-overlap', 'pending-L1b']]);
      assert.equal(comparisons.some(item => item.id === 'staging-overlap'), false);
    }
    if (manifest.cohort === 'C7') {
      const cadence = comparisons.find(item => item.id === 'maintenance-cadence')!;
      for (const item of cadence.candidates) {
        assert.equal((item as { qualification?: string }).qualification, 'exploratory');
        assert.equal(at(item.overrides, ['tunables', 'maintenance', 'interval_secs']),
          at(item.overrides, ['tunables', 'maintenance', 'max_idle_backoff_secs']));
      }
      const stalePlans = comparisons.find(item => item.id === 'max-consecutive-stale-plans')!;
      assert.deepEqual(stalePlans.candidates.map(item =>
        at(item.overrides, ['tunables', 'maintenance', 'max_consecutive_stale_plans'])), [1, 8, 1024]);
      const sealedPartitions = comparisons.find(item => item.id === 'sealed-partitions-only')!;
      assert.deepEqual(sealedPartitions.candidates.map(item =>
        at(item.overrides, ['tunables', 'maintenance', 'sealed_partitions_only'])), [true, false]);
      assert.ok(sealedPartitions.candidates.every(item =>
        at(item.overrides, ['tunables', 'maintenance', 'operations']) === undefined &&
        at(item.overrides, ['tunables', 'maintenance', 'sealed_final']) === undefined));
      assert.deepEqual(sealedPartitions.candidates.map(item => item.id),
        ['sealed-partitions-true', 'sealed-partitions-false']);
      const semantics = manifest.maintenance_setting_semantics as {
        max_consecutive_stale_plans: { omitted_requested: null; default_effective: number; min: number; max: number };
        sealed_partitions_only: { omitted_requested: null; default_effective_without_table_override: boolean;
          effective_table_property: string; table_policy_overrides_startup_request: boolean };
      };
      assert.deepEqual(semantics.max_consecutive_stale_plans,
        { omitted_requested: null, default_effective: 8, min: 1, max: 1024 });
      assert.deepEqual(semantics.sealed_partitions_only, {
        omitted_requested: null,
        default_effective_without_table_override: true,
        effective_table_property: 'arrowcol.optimize.sealedPartitionsOnly',
        table_policy_overrides_startup_request: true,
      });
    }
    const baseline = manifest.baseline_tunables as { maintenance: { enabled: boolean; apply: boolean; operations: string[] } };
    assert.equal(baseline.maintenance.enabled, true);
    assert.equal(baseline.maintenance.apply, true);
    assert.deepEqual(baseline.maintenance.operations, ['optimize', 'purge', 'vacuum', 'checkpoint', 'prune_log']);
    assert.equal(at(baseline, ['maintenance', 'interval_secs']), 15);
    assert.equal(at(baseline, ['maintenance', 'max_idle_backoff_secs']), 15);
    assert.equal(at(baseline, ['maintenance', 'granularity']), 'hour');
    assert.equal(at(baseline, ['maintenance', 'purge_retention_secs']), 2_592_000);
    assert.equal(at(baseline, ['maintenance', 'max_consecutive_stale_plans']), undefined);
    assert.equal(at(baseline, ['maintenance', 'sealed_partitions_only']), undefined);
  }
  assert.throws(() => buildThroughputManifests({}), /registry must contain/);
});

test('renders candidate overlays as OAT configs without changing the baseline or admission invariants', () => {
  const base = {
    platform: { kind: 'local' },
    tables: ['sqlserver_active_sessions'],
    tunables: { client: { workers: 2, streams_per_worker: 2 }, maintenance: { enabled: false } },
  };
  const original = structuredClone(base);
  const manifests = buildThroughputManifests(base);
  const invalidLimit = structuredClone(manifests[0]!) as Record<string, unknown>;
  const invalidComparisons = invalidLimit.comparisons as Array<{ candidate_limit: number }>;
  invalidComparisons[0]!.candidate_limit = 8;
  assert.throws(() => renderThroughputBaseline(base, invalidLimit), /4-candidate limit/);
  for (const manifest of manifests) {
    const baseline = renderThroughputBaseline(base, manifest);
    assert.deepEqual(baseline.overload_guard, { enabled: true, cap_fraction: 0.90, zero_progress_windows: 2 });
    assert.equal(at(baseline, ['platform', 'local', 'collector_memory_limit_bytes']), 4_294_967_296);
    const comparisons = manifest.comparisons as Array<{ id: string; candidates: Array<{ id: string }> }>;
    for (const comparison of comparisons) {
      for (const candidate of comparison.candidates) {
        const config = renderThroughputConfig(base, manifest, comparison.id, candidate.id);
        assert.deepEqual(config.tables, ['sqlserver_availability_replica_states']);
        assert.deepEqual(config.replica_counts, [1]);
        assert.equal(config.campaign_profile, 'single_hot_table');
        assert.deepEqual(config.overload_guard, baseline.overload_guard);
        assert.equal(at(config, ['platform', 'local', 'collector_memory_limit_bytes']), 4_294_967_296);
        assert.equal(config.measure_seconds, 150);
        if (!(manifest.cohort === 'C7' && comparison.id === 'granularity')) {
          assert.equal(at(config, ['tunables', 'maintenance', 'granularity']), 'hour');
        }
        assert.equal(at(config, ['tunables', 'maintenance', 'purge_retention_secs']), 2_592_000);
        assert.equal(at(config, ['tunables', 'maintenance', 'enabled']), true);
        assert.equal(at(config, ['tunables', 'maintenance', 'apply']), true);
      }
    }
    if (manifest.cohort === 'C7') {
      const stale = (manifest.comparisons as Array<{ id: string; candidates: Array<{ id: string }> }>)
        .find(item => item.id === 'max-consecutive-stale-plans')!;
      for (const item of stale.candidates) {
        const config = renderThroughputConfig(base, manifest, 'max-consecutive-stale-plans', item.id);
        assert.equal(at(config, ['tunables', 'maintenance', 'max_consecutive_stale_plans']),
          Number(item.id.slice('stale-plans-'.length)));
      }
      for (const [id, requested] of [['sealed-partitions-true', true], ['sealed-partitions-false', false]] as const) {
        const config = renderThroughputConfig(base, manifest, 'sealed-partitions-only', id);
        assert.equal(at(config, ['tunables', 'maintenance', 'sealed_partitions_only']), requested);
        assert.equal(at(config, ['tunables', 'maintenance', 'sealed_final']), undefined);
        assert.ok((at(config, ['tunables', 'maintenance', 'operations']) as string[]).includes('optimize'));
      }
    }
  }
  assert.deepEqual(base, original);
  assert.throws(() => renderThroughputConfig(base, manifests[0], 'missing', 'none'), /unknown comparison/);
  assert.throws(() => renderThroughputConfig(base, manifests[7], 'staging-overlap', 'overlap-true'), /unknown comparison/);
  assert.throws(() => renderThroughputConfig({ ...base, tunables: {} }, manifests[0], 'codec', 'snappy'), /manifest seed/);
});


test('CLI selects the C5 gRPC supplement without overwriting the frozen C5 manifest', () => {
  const repositoryRoot = fileURLToPath(new URL('../..', import.meta.url));
  const script = fileURLToPath(new URL('./throughput-sweeps.ts', import.meta.url));
  const manifestDirectory = fileURLToPath(new URL('../../docs/experiments/arrowcol-oat/', import.meta.url));
  const registry = fileURLToPath(new URL('../../docs/experiments/arrowcol-oat/oat-config.tuned.json', import.meta.url));
  const originalC5Path = join(manifestDirectory, 'oat-config-throughput-C5.json');
  const originalC5 = readFileSync(originalC5Path);
  const temporaryDirectory = mkdtempSync(join(tmpdir(), 'oat-sweep-cli-'));
  const runCli = (...args: string[]) => {
    const result = spawnSync(process.execPath, ['--import', 'tsx', script, ...args], {
      cwd: repositoryRoot, encoding: 'utf8',
    });
    assert.equal(result.error, undefined);
    assert.equal(result.status, 0, result.stderr || result.stdout);
    return result.stdout;
  };
  try {
    const standardOutput = join(temporaryDirectory, 'standard');
    mkdirSync(standardOutput);
    writeFileSync(join(standardOutput, 'oat-config-throughput-C5.json'), originalC5);
    assert.match(runCli('--registry', registry, '--out', standardOutput), /C5 is frozen/);
    assert.deepEqual(readFileSync(join(standardOutput, 'oat-config-throughput-C5.json')), originalC5);
    assert.equal(existsSync(join(standardOutput, 'oat-config-throughput-C5-grpc.json')), false);

    const generatedOutput = join(temporaryDirectory, 'generated');
    runCli('--registry', registry, '--supplement', 'C5-grpc', '--out', generatedOutput);
    const generatedSupplement = JSON.parse(readFileSync(join(generatedOutput, 'oat-config-throughput-C5-grpc.json'), 'utf8')) as {
      comparisons: Array<{ id: string; candidates: Array<{ id: string }> }>;
      config_validation: { status: string };
    };
    assert.equal(generatedSupplement.config_validation.status, 'not-validated');
    assert.equal(generatedSupplement.comparisons.flatMap(item => item.candidates).filter(item => item.id.startsWith('grpc-')).length, 9);
    assert.equal(existsSync(join(generatedOutput, 'oat-config-throughput-C5.json')), false);

    const renderedOutput = join(temporaryDirectory, 'rendered');
    runCli('--registry', registry, '--render-all', '--supplement', 'C5-grpc', '--manifests', manifestDirectory, '--out', renderedOutput);
    const comparisonGroups: Array<[string, string[]]> = [
      ['grpc-client-to-receiver', ['grpc-none.json', 'grpc-client-to-receiver-gzip.json', 'grpc-client-to-receiver-zstd.json']],
      ['grpc-receiver-to-client', ['grpc-none.json', 'grpc-receiver-to-client-gzip.json', 'grpc-receiver-to-client-zstd.json']],
      ['grpc-mixed-directions', ['grpc-none.json', 'grpc-mixed-request-gzip-response-zstd.json', 'grpc-mixed-request-zstd-response-gzip.json']],
    ];
    let renderedEntries = 0;
    for (const [group, expectedFiles] of comparisonGroups) {
      const files = readdirSync(join(renderedOutput, 'C5-grpc', group)).filter(name => name.endsWith('.json')).sort();
      assert.deepEqual(files, [...expectedFiles].sort());
      renderedEntries += files.length;
    }
    assert.equal(renderedEntries, 9);
    assert.equal(existsSync(join(renderedOutput, 'C5')), false);
    assert.deepEqual(readFileSync(originalC5Path), originalC5);
  } finally {
    rmSync(temporaryDirectory, { recursive: true, force: true });
  }
});
