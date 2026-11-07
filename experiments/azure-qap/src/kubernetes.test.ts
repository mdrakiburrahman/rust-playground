import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { kubernetesRuntimeArgs, parseKubernetesConfig } from './kubernetes.js';

const digest = 'a'.repeat(64);

test('Job keeps failed-run artifacts readable until collection, even after cancellation', { timeout: 15_000 }, async () => {
  for (const cancelDuringRun of [false, true]) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'qap-job-lifecycle-'));
    const job = path.join(directory, 'job.ts');
    const marker = path.join(directory, 'done');
    const exitCode = path.join(directory, 'exit-code');
    fs.writeFileSync(path.join(directory, 'package.json'), '{"type":"module"}');
    fs.writeFileSync(job, fs.readFileSync(new URL('./job.ts', import.meta.url), 'utf8')
      .replace("import { main } from './main.js';",
        "const main = async () => { console.log('running'); await new Promise(resolve => setTimeout(resolve, 200)); return 1; };")
      .replace("'/tmp/azure-qap.env'", JSON.stringify(path.join(directory, 'env')))
      .replace("'/artifacts/done'", JSON.stringify(marker))
      .replace("'/artifacts/exit-code'", JSON.stringify(exitCode)));
    const child = spawn(process.execPath, ['--import', 'tsx', job], { stdio: ['ignore', 'pipe', 'pipe'] });
    const exited = once(child, 'exit');
    let stderr = '';
    child.stderr.on('data', chunk => { stderr += String(chunk); });
    try {
      await once(child.stdout, 'data');
      if (cancelDuringRun) child.kill('SIGTERM');
      const deadline = Date.now() + 5000;
      while (!fs.existsSync(marker) && child.exitCode === null && Date.now() < deadline) await sleep(10);
      assert.ok(fs.existsSync(marker), stderr);
      await sleep(50);
      assert.equal(child.exitCode, null, stderr);
      assert.equal(child.signalCode, null, stderr);
      assert.equal(fs.readFileSync(exitCode, 'utf8'), '1\n');
      child.kill('SIGTERM');
      assert.deepEqual(await exited, [1, null]);
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.kill('SIGKILL');
        await exited;
      }
      fs.rmSync(directory, { recursive: true, force: true });
    }
  }
});
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