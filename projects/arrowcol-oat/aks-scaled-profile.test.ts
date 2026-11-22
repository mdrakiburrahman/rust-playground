import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import {
  allScaledClientsReady,
  applyCandidateImageManifest,
  assertScaledRuntimeObservation,
  effectiveWorkloadConfig,
  CandidateImageManifest,
  planScaledClientPool,
  planScaledClientPoolRestore,
  renderScaledProfileManifest,
  scaledClientJobs,
  ScaledPoolSettings,
} from './aks-scaled-profile.js';

const runId = '00000000-0000-4000-8000-000000000001';
const registry = 'arrowcollector68590.azurecr.io';
const sourceCommit = 'a'.repeat(40);
const digest = 'sha256:' + 'b'.repeat(64);

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
    assert.deepEqual(args.filter((_, position) => position !== args.indexOf('--client-id') + 1),
      baseArgs.filter((_, position) => position !== baseArgs.indexOf('--client-id') + 1));
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
