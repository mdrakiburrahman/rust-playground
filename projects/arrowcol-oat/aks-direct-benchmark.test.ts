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

test('SQL Server EULA remains a string for YAML 1.1 consumers', async () => {
  const manifestPath = resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml');
  const documents = parseAllDocuments(await readFile(manifestPath, 'utf8'), { version: '1.1' });
  const values: unknown[] = [];

  for (const document of documents) {
    const resource = record(document.toJSON());
    if (resource.kind !== 'Deployment') continue;

    const podSpec = record(record(record(resource.spec ?? {}).template ?? {}).spec ?? {});
    if (!Array.isArray(podSpec.containers)) continue;
    for (const container of podSpec.containers) {
      const env = record(container).env;
      if (!Array.isArray(env)) continue;
      for (const entry of env) {
        const variable = record(entry);
        if (variable.name === 'ACCEPT_EULA') values.push(variable.value);
      }
    }
  }

  assert.deepEqual(values, ['Y']);
});
