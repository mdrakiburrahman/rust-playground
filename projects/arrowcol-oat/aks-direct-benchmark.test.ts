import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import { ROOT } from '../../infra/azure/bootstrap.js';
import { assertRenderedManifest, renderManifest, replaceInitializerExpiry } from './aks-direct-benchmark.js';

function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}

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
    BENCHREGISTRY: 'arrowcollector68590.azurecr.io',
    BENCHDELTA_ROOT: 'abfss://arrowcol@arrowcollector68590.dfs.core.windows.net/direct-baseline-1/runs/' + runId + '/dataflow',
    BENCHSTAMP: '68590',
    BENCHSUBSCRIPTION: '00000000-0000-4000-8000-000000000000',
    BENCHRESOURCEGROUP: 'arrow-collector-rg',
    BENCHSTORAGEACCOUNT: 'arrowcollector68590',
    BENCHCHECKPOINTS: 'checkpoints',
    BENCHDURATIONSECS: duration,
  });
  for (const duration of ['60', '600']) {
    const rendered = renderManifest(source, values(duration));
    assertRenderedManifest(rendered, runId, owner, 'arrowcollector68590.azurecr.io');
    assert.throws(() => assertRenderedManifest(rendered.replace('benchmark-stage: load', 'benchmark-stage: unknown'), runId, owner, 'arrowcollector68590.azurecr.io'), /stage label/);
    assert.throws(() => assertRenderedManifest(rendered.replace('@sha256:', ':latest'), runId, owner, 'arrowcollector68590.azurecr.io'), /pinned SHA-256 digest/);
    assert.throws(() => assertRenderedManifest(rendered.replace('arrowcol-oat/owner-token: ' + owner, 'arrowcol-oat/owner-token: ' + 'b'.repeat(48)), runId, owner, 'arrowcollector68590.azurecr.io'), /ownership markers/);
  }
});

test('rendered Kubernetes env values remain strings for YAML 1.1 consumers', async () => {
  const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const manifest = await readFile(manifestPath, 'utf8');
  const rendered = manifest.replaceAll('BENCHSTAMP', '68590');
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
