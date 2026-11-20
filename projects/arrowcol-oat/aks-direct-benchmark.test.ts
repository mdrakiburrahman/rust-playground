import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import { ROOT } from '../../infra/azure/bootstrap.js';

function record(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value));
  return value as Record<string, unknown>;
}

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
