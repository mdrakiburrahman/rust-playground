import { strict as assert } from 'node:assert';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { test } from 'node:test';
import { observeWriter, observeWriterCapture, observeWriterExit, validateArm, type ArmIdentity } from './benchmark-arm.js';

const identity: ArmIdentity = {
  runId: '01234567-89ab-cdef-0123-456789abcdef',
  namespace: 'arrow-collector-0123456789abcdef0123456789abcdef',
  namespaceUid: '11234567-89ab-cdef-0123-456789abcdef',
  owner: 'owner-token-123456',
  clients: 2,
};
const labels = { 'arrowcol-oat/run-id': identity.runId, 'arrowcol-oat/owner-token': identity.owner };

function fixture() {
  const namespace = { metadata: { name: identity.namespace, uid: identity.namespaceUid, labels, annotations: labels } };
  const job = { metadata: { name: 'direct-load-confirmwide', namespace: identity.namespace, uid: 'job-uid', labels },
    spec: { suspend: false, completionMode: 'Indexed', parallelism: 2, completions: 2 }, status: {} };
  const schedule = { metadata: { name: 'direct-schedule-confirmwide', namespace: identity.namespace,
    resourceVersion: '17', labels }, data: { start: 'pending', writerLifetime: 'pending', clientLifetimes: 'pending' } };
  const pods = { items: [0, 1].map(index => ({
    metadata: { name: `pod-${index}`, uid: `uid-${index}`, namespace: identity.namespace, labels,
      annotations: { 'batch.kubernetes.io/job-completion-index': String(index) },
      ownerReferences: [{ kind: 'Job', uid: 'job-uid', controller: true }] },
    spec: { nodeName: `node-${index}`, volumes: [{ name: 'evidence', ephemeral: {} }],
      containers: [{ name: 'generator', image: `example/client@sha256:${'a'.repeat(64)}`,
        volumeMounts: [{ name: 'evidence', mountPath: '/evidence' }] }] },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }],
      containerStatuses: [{ name: 'generator', ready: true, restartCount: 0, imageID: `sha256:${'a'.repeat(64)}`,
        containerID: `containerd://client-${index}`, state: { running: { startedAt: '2026-01-01T00:00:00Z' } } }] },
  })) };
  const logs = new Map([0, 1].map(index =>
    [`pod-${index}`, `OAT_CLIENT_READY client_id=direct-${identity.runId}-${index} preflight_tables=1\n`]));
  const writerMeta = { namespace: identity.namespace, labels, annotations: labels };
  const deployment = { apiVersion: 'apps/v1', kind: 'Deployment',
    metadata: { ...writerMeta, name: 'arrowcol', uid: 'writer-deployment' }, spec: { replicas: 1 } };
  const replicaSets = { items: [{ metadata: { ...writerMeta, name: 'writer-rs', uid: 'writer-rs-uid',
    ownerReferences: [{ kind: 'Deployment', uid: 'writer-deployment', controller: true }] } }] };
  const writerPods = { items: [{ apiVersion: 'v1', kind: 'Pod',
    metadata: { ...writerMeta, name: 'writer-pod', uid: 'writer-pod-uid',
      ownerReferences: [{ kind: 'ReplicaSet', name: 'writer-rs', uid: 'writer-rs-uid', controller: true }] },
    spec: { nodeName: 'writer-node', volumes: [{ name: 'staging', persistentVolumeClaim: { claimName: 'arrowcol-writer-staging' } }],
      containers: [{ name: 'dataflow', image: `example/writer@sha256:${'b'.repeat(64)}`,
        volumeMounts: [{ name: 'staging', mountPath: '/var/lib/arrowcol/staging' }] }] },
    status: { phase: 'Running', podIP: '10.0.0.47', containerStatuses: [{ name: 'dataflow', ready: true, restartCount: 0,
      imageID: `example/writer@sha256:${'b'.repeat(64)}`, containerID: 'containerd://original',
      state: { running: { startedAt: '2026-01-01T00:00:00Z' } } }] },
  }] };
  const pvc = { apiVersion: 'v1', kind: 'PersistentVolumeClaim', metadata: { ...writerMeta, name: 'arrowcol-writer-staging',
    uid: 'original-claim' }, spec: { volumeName: 'bound-pv',
      storageClassName: `arrowcol-${identity.runId.replaceAll('-', '')}-local-csi` }, status: { phase: 'Bound' } };
  const claims = Object.fromEntries(pods.items.map(pod => {
    const name = `${pod.metadata.name}-evidence`;
    return [name, { ...pvc, metadata: { ...pvc.metadata, name, uid: `claim-${pod.metadata.uid}`,
      ownerReferences: [{ kind: 'Pod', uid: pod.metadata.uid, controller: true }] },
    spec: { ...pvc.spec, storageClassName: 'managed-csi' } }];
  }));
  return { namespace, job, schedule, pods, logs, deployment, replicaSets, writerPods, pvc, claims };
}

test('requires a run-owned retained PVC and a positive live writer redirection probe', () => {
  const { pvc } = fixture();
  assert.throws(() => observeWriterCapture(identity, pvc, ''), /not confirmed/);
  assert.throws(() => observeWriterCapture(identity, {
    ...pvc, metadata: { ...pvc.metadata, ownerReferences: [{ kind: 'Pod' }] },
  }, 'writer-capture-active\n'), /not confirmed/);
  assert.equal(observeWriterCapture(identity, pvc, 'writer-capture-active\n').volumeName, 'bound-pv');
});

test('pins the exact running writer and refuses a restarted or foreign replacement', () => {
  const f = fixture();
  const observe = () => observeWriter(identity, f.deployment, f.replicaSets, f.writerPods);
  assert.equal(observe().containerId, 'containerd://original');
  f.writerPods.items[0].status.containerStatuses[0].restartCount = 1;
  assert.throws(observe, /not the original healthy lifetime/);
  f.writerPods.items[0].status.containerStatuses[0].restartCount = 0;
  const imageId = f.writerPods.items[0].status.containerStatuses[0].imageID;
  f.writerPods.items[0].status.containerStatuses[0].imageID = `sha256:${'c'.repeat(64)}`;
  assert.throws(observe, /runtime digest differs/);
  f.writerPods.items[0].status.containerStatuses[0].imageID = imageId;
  f.replicaSets.items[0].metadata.ownerReferences[0].uid = 'different-deployment';
  assert.throws(observe, /not owned by the exact Deployment/);
});

test('requires a clean terminal observation of the original container, not absence or a substitute', () => {
  const f = fixture();
  const original = observeWriter(identity, f.deployment, f.replicaSets, f.writerPods);
  const pod = f.writerPods.items[0];
  const startedAt = original.startedAt, finishedAt = '2026-01-01T00:01:02Z';
  const terminated = { containerID: original.containerId, startedAt, finishedAt, exitCode: 0, reason: 'Completed' };
  const end = { ...pod, metadata: { ...pod.metadata, resourceVersion: '21' }, status: { containerStatuses: [
    { ...pod.status.containerStatuses[0], state: { terminated } },
  ] } };
  const begin = Date.parse('2026-01-01T00:01:01.789Z');
  assert.equal(observeWriterExit(identity, original, end, begin).finishedAt, finishedAt);
  assert.throws(() => observeWriterExit(identity, original, undefined, begin), /terminal writer Pod/);
  assert.throws(() => observeWriterExit(identity, original, { ...end, metadata: { ...end.metadata, uid: 'replacement' } }, begin), /replaced/);
  terminated.containerID = 'containerd://other-lifetime';
  assert.throws(() => observeWriterExit(identity, original, end, begin), /exact clean exit/);
  terminated.containerID = original.containerId;
  terminated.exitCode = 137;
  assert.throws(() => observeWriterExit(identity, original, end, begin), /exact clean exit/);
});

test('requires every distinct, owned, healthy indexed client before arming', () => {
  const f = fixture();
  assert.deepEqual(validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs),
    { scheduleVersion: '17', podUids: ['uid-0', 'uid-1'] });
  f.pods.items[1].metadata.annotations['batch.kubernetes.io/job-completion-index'] = '0';
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /Duplicate or unexpected/);
});

test('refuses stale ownership, restarted clients and a previously armed schedule', () => {
  const f = fixture();
  f.namespace.metadata.uid = 'wrong';
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /Namespace UID/);
  f.namespace.metadata.uid = identity.namespaceUid;
  f.pods.items[0].status.containerStatuses[0].restartCount = 1;
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /not healthy/);
  f.pods.items[0].status.containerStatuses[0].restartCount = 0;
  f.schedule.data.start = '123';
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /pending object/);
});

test('refuses missing readiness and pods owned by a different job', () => {
  const f = fixture();
  f.logs.delete('pod-1');
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /pod count/);
  f.logs.set('pod-1', 'not ready');
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /readiness/);
  f.pods.items[1].metadata.ownerReferences[0].uid = 'other';
  assert.throws(() => validateArm(identity, f.namespace, f.job, f.schedule, f.pods, f.logs), /not owned/);
});

test('prepares one shared timestamp and atomic pending/version preconditions without arming', () => {
  const dir = mkdtempSync(join(tmpdir(), 'benchmark-arm-fixture-'));
  try {
    const f = fixture();
    const data = join(dir, 'fixture.json');
    writeFileSync(data, JSON.stringify({ ...f, logs: Object.fromEntries(f.logs) }));
    const executable = join(dir, 'kubectl');
    writeFileSync(executable, `#!/usr/bin/env node
const fs=require('node:fs');
const d=JSON.parse(fs.readFileSync(process.env.ARM_FIXTURE,'utf8'));
const a=process.argv.slice(2);
if(a.includes('logs')) { process.stdout.write(d.logs[a[a.indexOf('logs')+1]]); }
else if(a.includes('exec')) {
  const pod=a[a.indexOf('exec')+1];
  process.stdout.write(pod==='writer-pod'?'writer-capture-active\\n':d.logs[pod]);
}
else if(a.includes('pvc')) { process.stdout.write(JSON.stringify(d.claims[a[a.indexOf('pvc')+1]] ?? d.pvc)); }
else if(a.includes('patch')) { process.exit(2); }
else if(a.includes('namespace')&&!a.includes('pods')) { process.stdout.write(JSON.stringify(d.namespace)); }
else if(a.includes('job')) { process.stdout.write(JSON.stringify(d.job)); }
else if(a.includes('configmap')) { process.stdout.write(JSON.stringify(d.schedule)); }
else if(a.includes('deployment')) { process.stdout.write(JSON.stringify(d.deployment)); }
else if(a.includes('replicasets')) { process.stdout.write(JSON.stringify(d.replicaSets)); }
else if(a.includes('pods')) { process.stdout.write(JSON.stringify(a.includes('app=arrowcol') ? d.writerPods : d.pods)); }
else process.exit(3);
`);
    chmodSync(executable, 0o700);
    const module = new URL('./benchmark-arm.ts', import.meta.url).href;
    const run = spawnSync(process.execPath, ['--import', 'tsx', '--input-type=module', '--eval',
      `import {prepareDirectBarrier} from ${JSON.stringify(module)}; console.log(JSON.stringify(prepareDirectBarrier('fixture', ${JSON.stringify(identity)})));`],
    { encoding: 'utf8', env: { ...process.env, PATH: `${dir}${delimiter}${process.env.PATH}`,
      ARM_FIXTURE: data } });
    assert.equal(run.status, 0, run.stderr);
    const output = JSON.parse(run.stdout) as { runStartUnixMs: number; measurementStartUnixMs: number; measurementEndUnixMs: number;
      patch: Array<{ op: string; path: string; value: string }> };
    const ops = output.patch;
    assert.deepEqual(ops.filter(op => op.op === 'test').map(({ path, value }) => [path, value]), [
      ['/metadata/resourceVersion', '17'], ['/data/start', 'pending'], ['/data/writerLifetime', 'pending'],
      ['/data/clientLifetimes', 'pending']]);
    assert.equal(Number(ops[2].value), output.runStartUnixMs);
    assert.equal(JSON.parse(ops[4].value).podUid, 'writer-pod-uid');
    assert.equal(JSON.parse(ops[4].value).capture.claimUid, 'original-claim');
    const pinnedClients: Array<{ claimUid: string }> = JSON.parse(ops[6].value);
    assert.deepEqual(pinnedClients.map(client => client.claimUid), ['claim-uid-0', 'claim-uid-1']);
    assert.equal(output.measurementStartUnixMs, output.runStartUnixMs + 60_000);
    assert.equal(output.measurementEndUnixMs, output.runStartUnixMs + 660_000);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
