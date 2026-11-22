import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import {
  allScaledClientsReady,
  applyCandidateImageManifest,
  assertScaledRuntimeObservation,
  assertTrustedImageStatement,
  effectiveWorkloadConfig,
  CandidateImageManifest,
  planScaledClientPool,
  planScaledClientPoolRestore,
  renderScaledProfileManifest,
  scaledClientJobs,
  summarizeScaledClientWindow,
  ScaledPoolSettings,
} from './aks-scaled-profile.js';

const runId = '00000000-0000-4000-8000-000000000001';
const registry = 'arrowcollector68590.azurecr.io';
const sourceCommit = 'a'.repeat(40);
const digest = 'sha256:' + 'b'.repeat(64);

test('client summary binds final half-open-window counters and preserves latency samples without averaging quantiles', () => {
  const binding = { podUid: 'uid-client', clientId: 'client-1', start: 1000, end: 601000, completedRows: 17, workers: 8, streamsPerWorker: 2 };
  const receipt = { version: 1, phase: 'completed', completion_semantics: 'validated_admission_response_not_delta_commit',
    bytes_semantics: {
      configured_ipc_compression_flight_data_bytes: 'sum_of_FlightData_protobuf_encoded_lengths_after_configured_Arrow_IPC_compression',
      uncompressed_flight_data_bytes: 'sum_of_FlightData_protobuf_encoded_lengths_before_Arrow_IPC_compression' },
    measurement_window: { start_unix_ms: binding.start, end_unix_ms: binding.end, completed_rows: 17,
      scope: 'per_stream_admission_completion_observed_in_half_open_shared_unix_window',
      completed_flight_data_bytes_configured_ipc_compression: 100, completed_flight_data_bytes_uncompressed: 200,
      rejected_streams: 3, retried_streams: 2,
      windows: [7, 1023].map((p99, index) => ({ start_unix_ms: binding.start + index * 300000, end_unix_ms: binding.start + (index + 1) * 300000,
        send_latency_scope: 'attempts_grouped_by_fresh_send_timestamp_in_half_open_window',
        send_latency_us: { samples: index + 1, p50: 1, p95: p99, p99, precision: 'power_of_two_upper_bound' } })) } };
  const summary = summarizeScaledClientWindow('OAT_CLIENT_METRICS ' + JSON.stringify(receipt), binding);
  assert.equal(summary.pod_uid, binding.podUid);
  assert.equal(summary.configured_transport_slots, 16);
  assert.equal(summary.completed_rows_per_stream, null);
  assert.equal(summary.send_to_commit_latency_us.p99, null);
  assert.match(summary.send_to_commit_latency_us.unavailable_reason, /No actual send-to-Delta-version join/);
  assert.equal(summary.measurement?.completed_streams, null);
  assert.equal(summary.measurement?.configured_ipc_flight_data_bytes, 100);
  assert.deepEqual(summary.measurement?.send_latency_us, { scope: 'attempts_grouped_by_fresh_send_timestamp_in_half_open_window',
    samples: 3, p50: null, p95: null, p99: null,
    unavailable_reason: 'Only subwindow quantiles are recorded; aggregate percentiles cannot be reconstructed or averaged.',
    windows: receipt.measurement_window.windows.map(window => ({ start_unix_ms: window.start_unix_ms, end_unix_ms: window.end_unix_ms,
      ...window.send_latency_us })) });
  assert.throws(() => summarizeScaledClientWindow('OAT_CLIENT_METRICS ' + JSON.stringify(receipt), { ...binding, end: binding.end + 1 }), /global window/);
  const missing = summarizeScaledClientWindow('measured_rows=17', binding);
  assert.equal(missing.measurement, null);
  assert.match(missing.measurement_unavailable_reason!, /not substituted/);
});

test('trusted image claims require primary source and exact role build path, not a matching unrelated material', () => {
  const image = registry + '/arrowcol-oat-client';
  const statement = { _type: 'https://in-toto.io/Statement/v0.1', predicateType: 'https://slsa.dev/provenance/v0.2',
    subject: [{ name: image, digest: { sha256: digest.slice(7) } }], predicate: {
      builder: { id: 'urn:arrowcol:oat:publish-images:buildLocalImages:v1' },
      invocation: { configSource: { uri: 'git+https://github.com/mdrakiburrahman/rust-playground', digest: { sha1: sourceCommit },
        entryPoint: 'projects/arrowcol-oat/publish-images.ts#buildLocalImages' },
      parameters: { revision: sourceCommit, role: 'client', dockerfile: 'projects/arrowcol-oat-rust/Dockerfile.client', context: '.' } },
      materials: [{ uri: 'git+https://github.com/mdrakiburrahman/rust-playground', digest: { sha1: sourceCommit } }] } };
  assert.doesNotThrow(() => assertTrustedImageStatement(statement, digest, sourceCommit, image));
  const wrongSource = structuredClone(statement);
  wrongSource.predicate.invocation.configSource.digest.sha1 = 'c'.repeat(40);
  assert.throws(() => assertTrustedImageStatement(wrongSource, digest, sourceCommit, image), /primary source/);
  const wrongRole = structuredClone(statement);
  wrongRole.predicate.invocation.parameters.dockerfile = 'unrelated/Dockerfile';
  assert.throws(() => assertTrustedImageStatement(wrongRole, digest, sourceCommit, image), /Dockerfile/);
});

function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}

function resources(source: string): Record<string, unknown>[] {
  return parseAllDocuments(source, { version: '1.1' }).flatMap(document => {
    const value = document.toJSON();
    return value === null || value === undefined ? [] : [record(value)];
  });
}

function loadArgs(resource: Record<string, unknown>): unknown[] {
  const spec = record(resource.spec);
  const template = record(spec.template);
  const podSpec = record(template.spec);
  const generator = (podSpec.containers as unknown[]).map(record).find(container => container.name === 'generator');
  assert.ok(generator && Array.isArray(generator.args));
  return generator.args;
}

function imageManifest(workloadConfigSha256: string): CandidateImageManifest {
  return {
    schemaVersion: 1,
    sourceCommit,
    workloadConfigSha256,
    images: {
      'arrowcol-sqlserver@sha256:bb8f334a5287326345be56fb5722c28c6995051b8ee5aff4426bc3e38acd5b97': 'sha256:' + '1'.repeat(64),
      'arrowcol-catalog-rust@sha256:7c9030311da40ed3098574ff87bc47fb87bd596525d02fe654645ca555cdf300': 'sha256:' + '2'.repeat(64),
      'arrowcol-router-rust@sha256:f2d41fe2c672f8ad8d932acef9c41102899af353b873a92a62ac1c2c84e1be70': 'sha256:' + '3'.repeat(64),
      'delta-maintenance@sha256:93969473da125879ffdb4a66d235e946799497959eafb8190adf61e040205cf5': 'sha256:' + '4'.repeat(64),
      'arrowcol-dataflow-rust@sha256:427eeba2b2eb1b90d0d88f48b3b6463d88f5bdbca619a26328d881f5c326d8c0': 'sha256:' + '5'.repeat(64),
      'arrowcol-oat-client@sha256:29666c4b093393a27bf24c142153491ec320eb43ee9883c76b7d89cdfcfb4e30': 'sha256:' + '6'.repeat(64),
      'arrowcol-oat-client@sha256:f84be43a9fcf0c1c98071477ae9f20159caf4d294ac6b06986634093611dd892': 'sha256:' + '7'.repeat(64),
    },
  };
}

test('scaled rendering creates bounded clients with unique ownership and required distinct-node placement', async () => {
  const path = resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const source = (await readFile(path, 'utf8')).replaceAll('BENCHRUNUUID', runId).replaceAll('BENCHDURATIONSECS', '600');
  const jobs = scaledClientJobs(runId, 3);
  const rendered = renderScaledProfileManifest(source, runId, 3);
  const docs = resources(rendered);
  const loadJobs = docs.filter(resource => resource.kind === 'Job' &&
    String(record(resource.metadata).name).startsWith('direct-load-'));
  assert.equal(loadJobs.length, 3);
  assert.deepEqual(loadJobs.map(job => record(job.metadata).name), jobs.map(job => job.name));
  assert.equal(new Set(jobs.map(job => job.clientId)).size, 3);
  const baseArgs = loadArgs(loadJobs[0]!);
  for (const [index, job] of loadJobs.entries()) {
    const metadata = record(job.metadata);
    const labels = record(metadata.labels);
    assert.equal(labels['arrowcol-oat/client-profile'], 'scaled');
    assert.equal(labels['arrowcol-oat/client-index'], String(index + 1));
    const args = loadArgs(job);
    assert.equal(args[args.indexOf('--client-id') + 1], jobs[index]!.clientId);
    assert.deepEqual(args.filter((_, position) => position !== args.indexOf('--client-id') + 1 && position !== args.indexOf('--pod-ordinal') + 1),
      baseArgs.filter((_, position) => position !== baseArgs.indexOf('--client-id') + 1 && position !== baseArgs.indexOf('--pod-ordinal') + 1));
    const podSpec = record(record(record(job.spec).template).spec);
    const affinity = record(podSpec.affinity);
    const antiAffinity = record(affinity.podAntiAffinity);
    const rules = antiAffinity.requiredDuringSchedulingIgnoredDuringExecution as unknown[];
    assert.equal(rules.length, 1);
    assert.equal(record(rules[0]).topologyKey, 'kubernetes.io/hostname');
    assert.deepEqual(record(record(rules[0]).labelSelector).matchLabels, {
      app: 'arrowcol-oat-client',
      'arrowcol-oat/run-id': runId,
    });
  }
  assert.equal(docs.filter(resource => resource.kind === 'ConfigMap' && record(resource.metadata).name === 'direct-schedule-confirmwide').length, 1);
  assert.ok(rendered.includes('# -> load (one job;'));
});

test('scaled rendering rejects out-of-bound client counts and malformed source ownership', async () => {
  const source = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8')).replaceAll('BENCHRUNUUID', runId).replaceAll('BENCHDURATIONSECS', '600');
  for (const count of [0, 9, 1.5, Number.NaN]) assert.throws(() => renderScaledProfileManifest(source, runId, count), /integer from 1 to 8/);
  assert.throws(() => renderScaledProfileManifest(source.replaceAll('--client-id', '--client'), runId, 2), /--client-id/);
  assert.throws(() => renderScaledProfileManifest(source.replaceAll(`arrowcol-oat/run-id: ${runId}`, `arrowcol-oat/other: ${runId}`), runId, 2), /ownership labels/);
});

test('workload provenance hashes every tunable and rejects uneven client arguments', async () => {
  const source = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8'))
    .replaceAll('BENCHRUNUUID', runId).replaceAll('BENCHDURATIONSECS', '600');
  const oneClient = effectiveWorkloadConfig(source);
  const scaled = renderScaledProfileManifest(source, runId, 3);
  const threeClients = effectiveWorkloadConfig(scaled);
  assert.equal(threeClients.clientCount, 3);
  assert.notEqual(threeClients.sha256, oneClient.sha256);
  assert.throws(() => effectiveWorkloadConfig(scaled.replace('            - \"65536\"', '            - \"32768\"')), /equivalent workload arguments/);
});

test('diagnostic input IDs and ordinals are stable across run UUIDs and are bound to parity eligibility', async () => {
  const template = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8'))
    .replaceAll('BENCHDURATIONSECS', '600');
  const otherRunId = '11111111-1111-4111-8111-111111111111';
  for (const count of [1, 8]) {
    const first = renderScaledProfileManifest(template.replaceAll('BENCHRUNUUID', runId), runId, count);
    const second = renderScaledProfileManifest(template.replaceAll('BENCHRUNUUID', otherRunId), otherRunId, count);
    const jobs = (manifest: string) => resources(manifest).filter(resource => resource.kind === 'Job' &&
      String(record(resource.metadata).name).startsWith('direct-load-'));
    assert.equal(JSON.stringify(jobs(first).map(loadArgs)), JSON.stringify(jobs(second).map(loadArgs)));
    assert.notEqual(record(jobs(first)[0]!.metadata).name, record(jobs(second)[0]!.metadata).name);
    const config = effectiveWorkloadConfig(first);
    assert.equal(config.sha256, effectiveWorkloadConfig(second).sha256);
    assert.ok(config.dataIdentities.every(identity => identity.clientId.length === 'direct-e64-confirmwide'.length));
    assert.equal(config.historicalDataIdentityEligible, count === 1);
    assert.equal(config.profile, count === 1 ? 'diagnostic-replay' : 'non-parity');
    if (count === 1) {
      assert.deepEqual(config.dataIdentities, [{ clientId: 'direct-e64-confirmwide', podOrdinal: 0 }]);
      assert.equal(config.sha256, effectiveWorkloadConfig(template).sha256);
      const changed = first.replaceAll('direct-e64-confirmwide', 'direct-e64-not-parity');
      assert.notEqual(config.sha256, effectiveWorkloadConfig(changed).sha256);
      assert.equal(effectiveWorkloadConfig(changed).profile, 'non-parity');
    }
  }
});

test('readiness fails closed for failed clients and remains false until every client is ready', () => {
  const jobs = scaledClientJobs(runId, 2).map(job => job.name);
  assert.equal(allScaledClientsReady(jobs, [{ jobName: jobs[0]!, ready: true, failed: false }]), false);
  assert.equal(allScaledClientsReady(jobs, jobs.map(jobName => ({ jobName, ready: true, failed: false }))), true);
  assert.throws(() => allScaledClientsReady(jobs, [{ jobName: jobs[0]!, ready: false, failed: true }]), /failed before synchronized start/);
  assert.throws(() => allScaledClientsReady(jobs, [{ jobName: 'unexpected', ready: true, failed: false }]), /unexpected or duplicate/);
});

test('pool expansion is bounded and restoration refuses drift or nonzero nodes', () => {
  const original: ScaledPoolSettings = { enableAutoScaling: true, minCount: 0, maxCount: 1, count: 0 };
  const plan = planScaledClientPool(original, 0, 4);
  assert.deepEqual(plan.active, { enableAutoScaling: true, minCount: 0, maxCount: 4 });
  assert.deepEqual(planScaledClientPoolRestore(plan, { ...original, maxCount: 4 }, 0), {
    enableAutoScaling: true, minCount: 0, maxCount: 1,
  });
  assert.throws(() => planScaledClientPool(original, 1, 4), /observed at zero nodes/);
  assert.throws(() => planScaledClientPool({ ...original, count: 1 }, 0, 4), /observed at zero nodes/);
  assert.throws(() => planScaledClientPool({ ...original, enableAutoScaling: false }, 0, 4), /autoscaled min 0/);
  assert.throws(() => planScaledClientPoolRestore(plan, { ...original, maxCount: 5 }, 0), /changed or is not drained/);
  assert.throws(() => planScaledClientPoolRestore(plan, { ...original, maxCount: 4 }, 1), /changed or is not drained/);
});

test('runtime evidence requires collector identity, exact images, all clients and distinct nodes', () => {
  const jobs = scaledClientJobs(runId, 2).map(job => job.name);
  const observation = {
    collectorPodUid: 'collector-uid',
    collectorNodeName: 'collector-node',
    collectorImageId: `${registry}/arrowcol-dataflow-rust@${digest}`,
    clients: jobs.map((jobName, index) => ({
      jobName,
      podName: `client-${index}`,
      podUid: `client-uid-${index}`,
      nodeName: `node-${index}`,
      imageId: `${registry}/arrowcol-oat-client@${digest}`,
    })),
  };
  assertScaledRuntimeObservation(jobs, observation, digest, digest);
  assert.throws(() => assertScaledRuntimeObservation(jobs, { ...observation, collectorPodUid: '' }, digest, digest), /collector pod identity or placement/);
  assert.throws(() => assertScaledRuntimeObservation(jobs, { ...observation, clients: observation.clients.map(client => ({ ...client, podUid: '' })) }, digest, digest), /not on a distinct node/);
  assert.throws(() => assertScaledRuntimeObservation(jobs, { ...observation, clients: observation.clients.map(client => ({ ...client, nodeName: 'same-node' })) }, digest, digest), /distinct node/);
  assert.throws(() => assertScaledRuntimeObservation(jobs, { ...observation, clients: observation.clients.slice(0, 1) }, digest, digest), /missing or duplicating/);
  assert.throws(() => assertScaledRuntimeObservation(jobs, { ...observation, clients: observation.clients.map(client => ({ ...client, imageId: `${registry}/arrowcol-oat-client@sha256:${'c'.repeat(64)}` })) }, digest, digest), /candidate image digest/);
});

test('candidate images must bind all pinned workload digests to the exact source commit', async () => {
  const source = await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8');
  const rendered = source.replaceAll('BENCHREGISTRY', registry).replaceAll('BENCHDURATIONSECS', '600');
  const workload = effectiveWorkloadConfig(rendered);
  assert.equal(workload.profile, 'diagnostic-replay');
  assert.equal(workload.dataProfile, 'diagnostic');
  assert.equal(workload.ipcCompression, 'none');
  assert.ok(workload.generatorArgs.includes('--batch-rows'));
  assert.ok(workload.generatorArgs.includes('65536'));
  const provenance = imageManifest(workload.sha256);
  const result = applyCandidateImageManifest(rendered, registry, sourceCommit, provenance);
  for (const [sourceReference, expectedDigest] of Object.entries(provenance.images)) {
    const repository = sourceReference.split('@')[0];
    assert.ok(result.includes(`${registry}/${repository}@${expectedDigest}`));
  }
  assert.throws(() => applyCandidateImageManifest(rendered, registry, 'c'.repeat(40), provenance), /does not match/);
  assert.throws(() => applyCandidateImageManifest(rendered.replace('            - \"65536\"', '            - \"32768\"'), registry, sourceCommit, provenance), /does not bind/);
  const firstImageReference = Object.keys(provenance.images)[0]!;
  assert.throws(() => applyCandidateImageManifest(rendered, registry, sourceCommit, {
    ...provenance,
    images: { ...provenance.images, [firstImageReference]: 'latest' },
  }), /missing a valid digest/);
  const incomplete = Object.fromEntries(Object.entries(provenance.images).slice(1));
  assert.throws(() => applyCandidateImageManifest(rendered, registry, sourceCommit, {
    ...provenance,
    images: incomplete,
  }), /missing a valid digest/);
});

test('effective config rejects conflicting/unknown env and never labels realistic or Zstd as parity', async () => {
  const source = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8'))
    .replaceAll('BENCHDURATIONSECS', '600');
  const documents = parseAllDocuments(source);
  const load = documents.find(document => document.getIn(['metadata', 'name']) === 'direct-load-confirmwide')!;
  const path = ['spec', 'template', 'spec', 'containers', 0];
  load.setIn([...path, 'env'], [{ name: 'ARROWCOL_CLIENT_DATA_PROFILE', value: 'realistic' },
    { name: 'ARROWCOL_CLIENT_IPC_COMPRESSION', value: 'zstd' }]);
  const realistic = documents.map(document => document.toString()).join('');
  const effective = effectiveWorkloadConfig(realistic);
  assert.equal(effective.profile, 'non-parity');
  assert.equal(effective.dataProfile, 'realistic');
  assert.equal(effective.ipcCompression, 'zstd');
  assert.equal(effective.performanceConfig['string-width'], '96');
  assert.notEqual(effective.sha256, effectiveWorkloadConfig(source).sha256);
  const args = loadArgs(record(load.toJSON()));
  load.setIn([...path, 'args'], [...args, '--data-profile', 'diagnostic']);
  assert.throws(() => effectiveWorkloadConfig(documents.map(document => document.toString()).join('')), /Conflicting/);
  load.setIn([...path, 'env'], [{ name: 'ARROWCOL_CLIENT_UNKNOWN', value: '1' }]);
  assert.throws(() => effectiveWorkloadConfig(documents.map(document => document.toString()).join('')), /unknown/);
  load.setIn([...path, 'envFrom'], [{ configMapRef: { name: 'hidden-defaults' } }]);
  assert.throws(() => effectiveWorkloadConfig(documents.map(document => document.toString()).join('')), /envFrom/);
});

test('candidate substitution transforms original container image nodes once, without alias cascades or non-image edits', async () => {
  const source = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8'))
    .replaceAll('BENCHREGISTRY', registry).replaceAll('BENCHDURATIONSECS', '600');
  const binding = imageManifest(effectiveWorkloadConfig(source).sha256);
  const first = 'arrowcol-oat-client@sha256:29666c4b093393a27bf24c142153491ec320eb43ee9883c76b7d89cdfcfb4e30';
  const second = 'arrowcol-oat-client@sha256:f84be43a9fcf0c1c98071477ae9f20159caf4d294ac6b06986634093611dd892';
  binding.images[first] = second.split('@')[1]!;
  const documents = parseAllDocuments(source);
  documents[0]!.setIn(['metadata', 'annotations', 'image'], registry + '/' + first);
  const result = resources(applyCandidateImageManifest(documents.map(document => document.toString()).join(''), registry, sourceCommit, binding));
  const load = result.find(resource => record(resource.metadata).name === 'direct-load-confirmwide')!;
  const container = record((record(record(record(load.spec).template).spec).containers as unknown[])[0]);
  assert.equal(container.image, registry + '/' + second);
  assert.equal(record(record(result[0]!.metadata).annotations).image, registry + '/' + first);
  const registration = result.find(resource => record(resource.metadata).name === 'direct-catalog-verified')!;
  assert.equal(record((record(record(record(registration.spec).template).spec).containers as unknown[])[0]).image,
    registry + '/arrowcol-oat-client@' + binding.images[second]);
});

test('effective workload explicitly covers every performance input exposed by the compiled client source', async () => {
  const manifest = (await readFile(resolve(process.cwd(), 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8'))
    .replaceAll('BENCHDURATIONSECS', '600');
  const config = effectiveWorkloadConfig(manifest).performanceConfig;
  const sources = await Promise.all([
    readFile(resolve(process.cwd(), 'bin/collector-chain/src/dmv_client.rs'), 'utf8'),
    readFile(resolve(process.cwd(), 'bin/collector-chain/src/bin/arrowcol-dmv.rs'), 'utf8'),
  ]);
  const flags = new Set(sources.flatMap(source => [...source.matchAll(/env = "ARROWCOL_CLIENT_([A-Z_]+)"/g)]
    .map(match => match[1]!.toLowerCase().replaceAll('_', '-'))));
  for (const flag of flags) {
    if (['client-id', 'pod-ordinal', 'run-start-unix-ms'].includes(flag)) continue;
    assert.ok(config[flag] !== undefined, 'Unpinned effective client input: ' + flag);
  }
});
