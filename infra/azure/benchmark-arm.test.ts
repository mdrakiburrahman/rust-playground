import { strict as assert } from 'node:assert';
import { test } from 'node:test';
import { validateArm, type ArmIdentity } from './benchmark-arm.js';

const identity: ArmIdentity = {
  runId: '01234567-89ab-cdef-0123-456789abcdef',
  namespace: 'arrow-collector-0123456789abcdef0123456789abcdef',
  namespaceUid: '11234567-89ab-cdef-0123-456789abcdef',
  owner: 'owner-1',
  clients: 2,
};
const labels = { 'arrowcol-oat/run-id': identity.runId, 'arrowcol-oat/owner-token': identity.owner };

function fixture() {
  const namespace = { metadata: { name: identity.namespace, uid: identity.namespaceUid, labels } };
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
