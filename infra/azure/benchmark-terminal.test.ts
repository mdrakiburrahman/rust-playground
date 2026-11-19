import assert from 'node:assert/strict';
import test from 'node:test';
import { acceptRouterFencePatch, planRouterFence, routerFenceObserved } from './benchmark-router-fence.js';
import { observeStoppedWriterRegistration, pinWriterRegistration } from './benchmark-writer-sql.js';

const run = {
  runId: '01234567-89ab-cdef-0123-456789abcdef',
  owner: '76543210-89ab-cdef-0123-456789abcdef',
  namespace: 'arrow-collector-0123456789abcdef0123456789abcdef',
  namespaceUid: 'abacabac-1234-1234-1234-abacabacabac',
  clients: 1,
};
const uid = 'cdcdcdcd-1234-1234-1234-cdcdcdcdcdcd';
const markers = { 'arrowcol-oat/run-id': run.runId, 'arrowcol-oat/owner-token': run.owner };
const router = {
  apiVersion: 'apps/v1', kind: 'DaemonSet',
  metadata: { uid, name: 'arrowcol-router', namespace: run.namespace, resourceVersion: '42', generation: 1,
    labels: markers, annotations: markers },
  spec: { template: { spec: {} } },
  status: { observedGeneration: 1, desiredNumberScheduled: 1, currentNumberScheduled: 1, numberMisscheduled: 0 },
};

test('requires a positive original router fence, controller convergence and no pod or service endpoints', () => {
  const [uidTest, versionTest, affinity] = planRouterFence(run, uid, router);
  assert.deepEqual([uidTest, versionTest], [
    { op: 'test', path: '/metadata/uid', value: uid },
    { op: 'test', path: '/metadata/resourceVersion', value: '42' },
  ]);
  assert.throws(() => acceptRouterFencePatch(run, uid, 1, router), /PATCH response/);
  const response = { ...router, metadata: { ...router.metadata, generation: 2 },
    spec: { template: { spec: { affinity: affinity.value } } } };
  const generation = acceptRouterFencePatch(run, uid, 1, response);
  const emptyPods = { kind: 'PodList', items: [] };
  const emptySlices = { kind: 'EndpointSliceList', items: [] };
  const coverage = { 'arrowcol-router': emptySlices, 'arrowcol-router-node-local': emptySlices };
  assert.equal(routerFenceObserved(run, uid, generation, response, emptyPods, coverage), false);
  const stopped = { ...response, status: { observedGeneration: 2, desiredNumberScheduled: 0, currentNumberScheduled: 0,
    numberMisscheduled: 0 } };
  const lingering = { metadata: { namespace: run.namespace,
    labels: { 'kubernetes.io/service-name': 'arrowcol-router-node-local' } }, endpoints: [{ conditions: { ready: false } }] };
  assert.equal(routerFenceObserved(run, uid, generation, stopped, emptyPods,
    { ...coverage, 'arrowcol-router-node-local': { ...emptySlices, items: [lingering] } }), false);
  assert.throws(() => routerFenceObserved(run, uid, generation,
    { ...stopped, metadata: { ...stopped.metadata, uid: 'replacement' } }, emptyPods, coverage), /identity changed/);
  assert.equal(routerFenceObserved(run, uid, generation, stopped, emptyPods, coverage), true);
});

test('binds SQL stop to one original run generation, endpoint and registration lifetime', () => {
  const generation = '98765432-89ab-cdef-0123-456789abcdef';
  const writer = { deploymentUid: 'deployment', replicaSetUid: 'replica', podName: 'pod', podUid: uid,
    podIp: '10.0.0.47', image: 'pinned', imageId: 'image-id', containerId: 'container',
    startedAt: '2026-01-01T00:00:00Z', finishedAt: '2026-01-01T00:10:01Z', terminalPodResourceVersion: '21' };
  const ready = { instance_id: uid, instance_key: `direct-single-writer-${run.runId}|cloud=${generation}`,
    implementation_pool: 'dataflow', flight_endpoint: '10.0.0.47:32010', state: 'ready',
    started_at_utc: '2026-01-01T00:00:02.0000000Z', row_version: '0x0020000000000000' };
  const stopped = { ...ready, state: 'stopped', row_version: '0x0020000000000001' };
  assert.throws(() => pinWriterRegistration(run, generation, writer, [ready, ready]), /No unique SQL registration/);
  assert.throws(() => pinWriterRegistration(run, generation, writer, [{ ...ready, flight_endpoint: '10.0.0.48:32010' }]), /No unique SQL registration/);
  const pin = pinWriterRegistration(run, generation, writer, [ready]);
  for (const bad of [{ ...stopped, state: 'failed' }, { ...stopped, instance_id: run.owner }, { ...stopped, row_version: ready.row_version }]) {
    assert.throws(() => observeStoppedWriterRegistration(run, generation, writer, pin, [bad]), /newer SQL stopped observation/);
  }
  assert.deepEqual(observeStoppedWriterRegistration(run, generation, writer, pin, [stopped]), stopped);
});
