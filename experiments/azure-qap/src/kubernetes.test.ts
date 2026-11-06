import assert from 'node:assert/strict';
import test from 'node:test';
import { kubernetesRuntimeArgs, parseKubernetesConfig } from './kubernetes.js';

const digest = 'a'.repeat(64);
const source = (overrides = '') => `image: registry.azurecr.io/qap/runtime@sha256:${digest}
resources:
  requests:
    cpu: 1
    memory: 2Gi
  limits:
    cpu: 2
    memory: 4Gi
nodeSelector:
  kubernetes.io/os: linux
${overrides}`;

test('validates a digest-pinned QAP image and configurable Job resources', () => {
  const config = parseKubernetesConfig(source('context: aks-eastus2\n'));
  assert.equal(config.context, 'aks-eastus2');
  assert.equal(config.resources.requests.memory, '2Gi');
  assert.equal(config.nodeSelector['kubernetes.io/os'], 'linux');
});

test('rejects unpinned images and unknown config keys', () => {
  assert.throws(() => parseKubernetesConfig(source().replace(`@sha256:${digest}`, ':latest')), /image is invalid/);
  assert.throws(() => parseKubernetesConfig(`${source()}unexpected: true\n`), /unsupported key/);
});

test('rejects resource requests above their limits', () => {
  assert.throws(() => parseKubernetesConfig(source().replace('cpu: 1\n    memory: 2Gi', 'cpu: 3\n    memory: 2Gi')), /requests must be positive and no greater/);
});

test('forwards only experiment arguments to the Job', () => {
  assert.deepEqual(kubernetesRuntimeArgs(['--kubernetes', '--kubernetes-config', 'secret.yaml', '--env', '.env',
    '--operations', 'reload', '--out', 'local-report']), ['--out', '/artifacts', '--operations', 'reload']);
});