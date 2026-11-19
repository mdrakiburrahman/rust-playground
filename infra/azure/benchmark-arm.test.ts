import { strict as assert } from 'node:assert';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { validateArm, type ArmIdentity } from './benchmark-arm.js';

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
    resourceVersion: '17', labels }, data: { start: 'pending' } };
  const pods = { items: [0, 1].map(index => ({
    metadata: { name: `pod-${index}`, uid: `uid-${index}`, namespace: identity.namespace, labels,
      annotations: { 'batch.kubernetes.io/job-completion-index': String(index) },
      ownerReferences: [{ kind: 'Job', uid: 'job-uid', controller: true }] },
    spec: { containers: [{ name: 'generator', image: `example/client@sha256:${'a'.repeat(64)}` }] },
    status: { phase: 'Running', conditions: [{ type: 'Ready', status: 'True' }],
      containerStatuses: [{ name: 'generator', ready: true, restartCount: 0, state: { running: {} } }] },
  })) };
  const logs = new Map([0, 1].map(index =>
    [`pod-${index}`, `OAT_CLIENT_READY client_id=direct-${identity.runId}-${index} preflight_tables=1\n`]));
  return { namespace, job, schedule, pods, logs };
}

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

test('arms one shared timestamp with an atomic pending/version precondition', () => {
  const dir = mkdtempSync(join(tmpdir(), 'benchmark-arm-fixture-'));
  try {
    const f = fixture();
    const data = join(dir, 'fixture.json');
    const patch = join(dir, 'patch.json');
    writeFileSync(data, JSON.stringify({ ...f, logs: Object.fromEntries(f.logs) }));
    const executable = join(dir, 'kubectl');
    writeFileSync(executable, `#!/usr/bin/env node
const fs=require('node:fs');
const d=JSON.parse(fs.readFileSync(process.env.ARM_FIXTURE,'utf8'));
const a=process.argv.slice(2);
if(a.includes('logs')) { process.stdout.write(d.logs[a[a.indexOf('logs')+1]]); }
else if(a.includes('patch')) {
  const p=JSON.parse(a[a.indexOf('--patch')+1]);
  if(p[0].value!=='17'||p[1].value!=='pending'||p[2].path!=='/data/start') process.exit(2);
  fs.writeFileSync(process.env.ARM_PATCH,JSON.stringify(p));
  process.stdout.write('{}');
} else if(a.includes('namespace')&&!a.includes('pods')) { process.stdout.write(JSON.stringify(d.namespace)); }
else if(a.includes('job')) { process.stdout.write(JSON.stringify(d.job)); }
else if(a.includes('configmap')) {
  if(fs.existsSync(process.env.ARM_PATCH)) d.schedule.data.start=JSON.parse(fs.readFileSync(process.env.ARM_PATCH,'utf8'))[2].value;
  process.stdout.write(JSON.stringify(d.schedule));
} else if(a.includes('pods')) { process.stdout.write(JSON.stringify(d.pods)); }
else process.exit(3);
`);
    chmodSync(executable, 0o700);
    const run = spawnSync(process.execPath, ['--import', 'tsx',
      fileURLToPath(new URL('./benchmark-arm.ts', import.meta.url)), 'fixture', identity.namespace,
      identity.namespaceUid, identity.runId, identity.owner, '2', '--confirm-authorized'],
    { encoding: 'utf8', env: { ...process.env, PATH: `${dir}${delimiter}${process.env.PATH}`,
      ARM_FIXTURE: data, ARM_PATCH: patch } });
    assert.equal(run.status, 0, run.stderr);
    const output = JSON.parse(run.stdout) as { runStartUnixMs: number; measurementStartUnixMs: number; measurementEndUnixMs: number };
    const ops = JSON.parse(readFileSync(patch, 'utf8')) as Array<{ value: string }>;
    assert.equal(Number(ops[2].value), output.runStartUnixMs);
    assert.equal(output.measurementStartUnixMs, output.runStartUnixMs + 60_000);
    assert.equal(output.measurementEndUnixMs, output.runStartUnixMs + 660_000);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
