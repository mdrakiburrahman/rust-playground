import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import { ROOT } from '../../infra/azure/bootstrap.js';
import { assertRenderedManifest, errorMessages, isSchemaFingerprint, poolBlockingPods, poolCleanupDecision, renderManifest, replaceInitializerExpiry, verifyBenchmarkPlacement } from './aks-direct-benchmark.js';

function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}

test('pool cleanup waits for natural drain while any non-DaemonSet pod remains', () => {
  const daemon = { metadata: { ownerReferences: [{ kind: 'DaemonSet' }] }, status: { phase: 'Running' } };
  const job = { metadata: { namespace: 'kube-system', name: 'system-job', ownerReferences: [{ kind: 'Job' }] }, status: { phase: 'Succeeded' } };
  const blockers = poolBlockingPods([daemon, job]);
  assert.deepEqual(blockers, ['kube-system/system-job (Succeeded)']);
  assert.equal(poolCleanupDecision(1, 1, blockers), 'wait');
  assert.equal(poolCleanupDecision(1, 1, poolBlockingPods([daemon])), 'scale');
  assert.equal(poolCleanupDecision(0, 0, []), 'drained');
  assert.equal(poolCleanupDecision(1, 1, poolBlockingPods([null])), 'wait');
  for (const phase of ['Running', 'Pending', 'Failed']) {
    assert.equal(poolCleanupDecision(1, 1, poolBlockingPods([{ ...job, status: { phase } }])), 'wait');
  }
});

test('cleanup errors include nested pool blockers', () => {
  const blocker = new Error('Non-DaemonSet pod kube-system/system-job (Succeeded)');
  const stage = new Error('pool stage failed', { cause: blocker });
  const aggregate = new AggregateError([stage], 'pool restoration failed');
  assert.deepEqual(errorMessages(aggregate), ['pool restoration failed', 'pool stage failed', blocker.message]);
});

test('catalog registration accepts the protocol SHA-256 fingerprint encoding', () => {
  assert.equal(isSchemaFingerprint('sha256:' + 'a'.repeat(64)), true);
  assert.equal(isSchemaFingerprint('a'.repeat(64)), false);
  assert.equal(isSchemaFingerprint('sha256:' + 'g'.repeat(64)), false);
});

test('rendered benchmark manifest enforces ownership, stage, image, and timed-window invariants', async () => {
  const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const source = await readFile(manifestPath, 'utf8');
  const runId = '00000000-0000-4000-8000-000000000001';
  const owner = 'a'.repeat(48);
  const values = (duration: string) => ({
    BENCHRUNUUID: runId,
    BENCHRUNHEX: runId.replaceAll('-', ''),
    BENCHOWNER: owner,
    BENCHCLIENTID: '11111111-2222-4333-8444-555555555555',
    BENCHREGISTRY: 'arrowcollector54321.azurecr.io',
    BENCHDELTA_ROOT: 'abfss://arrowcol@arrowcollector54321.dfs.core.windows.net/direct-baseline-1/runs/' + runId + '/dataflow',
    BENCHSTAMP: '54321',
    BENCHSUBSCRIPTION: '00000000-0000-4000-8000-000000000000',
    BENCHRESOURCEGROUP: 'fixture-rg',
    BENCHSTORAGEACCOUNT: 'arrowcollector54321',
    BENCHCHECKPOINTS: 'checkpoints',
    BENCHDURATIONSECS: duration,
  });
  for (const duration of ['60', '600']) {
    const rendered = renderManifest(source, values(duration));
    assertRenderedManifest(rendered, runId, owner, 'arrowcollector54321.azurecr.io');
    assert.throws(() => assertRenderedManifest(rendered.replace('benchmark-stage: load', 'benchmark-stage: unknown'), runId, owner, 'arrowcollector54321.azurecr.io'), /stage label/);
    assert.throws(() => assertRenderedManifest(rendered.replace('@sha256:', ':latest'), runId, owner, 'arrowcollector54321.azurecr.io'), /pinned SHA-256 digest/);
    assert.throws(() => assertRenderedManifest(rendered.replace('arrowcol-oat/owner-token: ' + owner, 'arrowcol-oat/owner-token: ' + 'b'.repeat(48)), runId, owner, 'arrowcollector54321.azurecr.io'), /ownership markers/);
    assert.throws(() => assertRenderedManifest(rendered.replace('kubernetes.azure.com/agentpool: benche64', 'kubernetes.azure.com/agentpool: arrowe192'), runId, owner, 'arrowcollector54321.azurecr.io'), /must select agent pool benche64/);
    assert.throws(() => assertRenderedManifest(rendered.replace('        kubernetes.azure.com/agentpool: benche64\n', ''), runId, owner, 'arrowcollector54321.azurecr.io'), /must select agent pool benche64/);
  }
});

test('pre-load placement rejects an escaped collector stack or unknown node', () => {
  const nodes = [
    { metadata: { name: 'collector-node', labels: { 'kubernetes.azure.com/agentpool': 'benche64' } } },
    { metadata: { name: 'client-node', labels: { 'kubernetes.azure.com/agentpool': 'benchd64' } } },
    { metadata: { name: 'other-node', labels: { 'kubernetes.azure.com/agentpool': 'arrowe192' } } },
  ];
  const collector = { metadata: { name: 'collector' }, spec: { nodeName: 'collector-node' } };
  const client = {
    metadata: { name: 'client', labels: { 'batch.kubernetes.io/job-name': 'direct-load-confirmwide' } },
    spec: { nodeName: 'client-node' },
  };
  assert.deepEqual(verifyBenchmarkPlacement([collector, client], nodes).map(pod => pod.pool), ['benche64', 'benchd64']);
  assert.throws(() => verifyBenchmarkPlacement([{ ...collector, spec: { nodeName: 'other-node' } }, client], nodes), /on arrowe192, expected benche64/);
  assert.throws(() => verifyBenchmarkPlacement([collector, { ...client, spec: { nodeName: 'collector-node' } }], nodes), /expected benchd64/);
  assert.throws(() => verifyBenchmarkPlacement([collector], []), /on unknown/);
});

test('rendered Kubernetes env values remain strings for YAML 1.1 consumers', async () => {
  const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const manifest = await readFile(manifestPath, 'utf8');
  const rendered = manifest.replaceAll('BENCHSTAMP', '54321');
  const documents = parseAllDocuments(rendered, { version: '1.1' });
  const nonStringValues: string[] = [];
  const eulaValues: unknown[] = [];

  for (const document of documents) {
    const resource = record(document.toJSON());
    if (resource.kind !== 'Deployment') continue;

    const metadata = record(resource.metadata ?? {});
    const resourceName = typeof metadata.name === 'string' ? metadata.name : '(unnamed)';
    const podSpec = record(record(record(resource.spec ?? {}).template ?? {}).spec ?? {});
    if (!Array.isArray(podSpec.containers)) continue;
    for (const container of podSpec.containers) {
      const containerRecord = record(container);
      const containerName = typeof containerRecord.name === 'string' ? containerRecord.name : '(unnamed)';
      const env = containerRecord.env;
      if (!Array.isArray(env)) continue;
      for (const entry of env) {
        const variable = record(entry);
        const variableName = typeof variable.name === 'string' ? variable.name : '(unnamed)';
        if (variable.value !== undefined && typeof variable.value !== 'string') {
          nonStringValues.push(`${resourceName}/${containerName}/${variableName}`);
        }
        if (variableName === 'ACCEPT_EULA') eulaValues.push(variable.value);
      }
    }
  }

  assert.deepEqual(nonStringValues, []);
  assert.deepEqual(eulaValues, ['Y']);
});

test('initializer expiry replacement targets the JSON field and ignores comments', async () => {
  const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const manifest = await readFile(manifestPath, 'utf8');
  const expiry = 1_800_000_000_000;
  const replaced = replaceInitializerExpiry(manifest, expiry);

  assert.match(replaced, /# 9876543210000 is a placeholder/);
  assert.equal((replaced.match(/"expires_at_unix_ms":1800000000000/g) ?? []).length, 1);
  assert.equal((replaced.match(/"expires_at_unix_ms":9876543210000/g) ?? []).length, 0);
});
