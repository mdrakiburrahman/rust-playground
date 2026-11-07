import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseAllDocuments } from 'yaml';
import { jobDeadlineSeconds, kubernetesRuntimeArgs, parseKubernetesConfig, writeKustomizeOverlay } from './kubernetes.js';

const digest = 'a'.repeat(64);

test('Job keeps failed-run artifacts readable until collection, even after cancellation', { timeout: 15_000 }, async () => {
  for (const [cancelDuringRun, inspectionRequested] of [[false, false], [true, false], [true, true]]) {
    const scratch = path.resolve('.temp/qap-job-tests');
    fs.mkdirSync(scratch, { recursive: true });
    const directory = fs.mkdtempSync(path.join(scratch, 'lifecycle-'));
    const job = path.join(directory, 'job.ts');
    const marker = path.join(directory, 'done');
    const exitCode = path.join(directory, 'exit-code');
    fs.writeFileSync(path.join(directory, 'package.json'), '{"type":"module"}');
    fs.writeFileSync(job, fs.readFileSync(new URL('./job.ts', import.meta.url), 'utf8')
      .replace("import { main } from './main.js';",
        "const main = async () => { console.log('running'); await new Promise(resolve => setTimeout(resolve, 200)); return 1; };")
      .replaceAll("'/app/.temp/qap-stage'", JSON.stringify(directory))
      .replace("'/app/.temp/qap-stage/runtime.env'", JSON.stringify(path.join(directory, 'env')))
      .replace("'/artifacts/done'", JSON.stringify(marker))
      .replace("'/artifacts/exit-code'", JSON.stringify(exitCode)));
    const child = spawn(process.execPath, ['--import', 'tsx', job, ...(inspectionRequested ? ['--keep-running'] : [])],
      { stdio: ['ignore', 'pipe', 'pipe'] });
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
      if (inspectionRequested) {
        await sleep(50);
        assert.equal(child.exitCode, null, 'retained artifact holder must survive late TERM after cleanup');
        child.kill('SIGUSR1');
      }
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
test('inspection flags reach the runtime and its active deadline includes inspection plus cleanup', () => {
  assert.deepEqual(kubernetesRuntimeArgs(['--kubernetes', '--keep-running', '--inspection-duration', '10m']),
    ['--out', '/artifacts', '--keep-running', '--inspection-duration', '10m']);
  assert.equal(jobDeadlineSeconds({ budgetMs: 1800_000 }, { keepRunning: false, inspectionMs: 3600_000 }), 2220);
  assert.equal(jobDeadlineSeconds({ budgetMs: 1800_000 }, { keepRunning: true, inspectionMs: 3600_000 }), 5940);
});

test('real offline Kustomize overlay renders owned labels, inspection args and bounded Job deadline', () => {
  const jobName = 'azure-qap-20261107123456-1234abcd';
  const args = ['--out', '/artifacts', '--keep-running', '--inspection-duration', '10m'];
  const root = writeKustomizeOverlay(parseKubernetesConfig(source()), 'owned-namespace', 'azure-qap',
    '11111111-2222-3333-4444-555555555555', jobName, args, {}, 'test-generation', 2940);
  try {
    const rendered = execFileSync('kustomize', ['build', path.join(root, 'infra/kubernetes/overlays/azure-qap')], { encoding: 'utf8' });
    const objects = parseAllDocuments(rendered).map(document => document.toJS());
    for (const kind of ['Job', 'ConfigMap', 'Secret']) {
      const item = objects.find(object => object.kind === kind);
      assert.equal(item.metadata.namespace, 'owned-namespace');
      assert.deepEqual(item.metadata.labels, { 'app.kubernetes.io/name': 'azure-qap', run: jobName });
    }
    const job = objects.find(object => object.kind === 'Job');
    assert.equal(job.spec.activeDeadlineSeconds, 2940);
    assert.deepEqual(job.spec.template.spec.containers[0].args, args);
    assert.equal(job.spec.template.spec.containers[0].envFrom[0].configMapRef.name, `${jobName}-env`);
    assert.equal(job.spec.template.spec.containers[0].env[0].valueFrom.secretKeyRef.name, `${jobName}-secret`);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});