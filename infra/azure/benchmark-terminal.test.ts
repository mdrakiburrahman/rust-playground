import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import test from 'node:test';
import { acceptRouterFencePatch, planRouterFence, routerFenceObserved } from './benchmark-router-fence.js';
import { persistDirectStopPatch } from './benchmark-stop-patch.js';
import { observeCompletedClients } from './benchmark-client-terminal.js';
import { persistDirectEvidence } from './benchmark-evidence-custody.js';
import { clientLogExportPod, exportWriterLogs, writerLogExportPod } from './benchmark-writer-export.js';
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
test('seals only read-back-verified original bytes and refuses an altered input', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'bench-custody-'));
  try {
    const raw = Buffer.alloc(4 * 1024 ** 2 + 3, 0xab);
    raw.set([0, 0xff, 10], raw.length - 3);
    const path = join(directory, 'raw');
    await writeFile(path, raw);
    const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
    const sha256 = createHash('sha256').update(raw).digest('hex');
    for (const failure of ['readback', 'source', 'none']) {
      const blobs = new Map<string, { bytes: Buffer; etag: string }>();
      const azure = {
        c: { checkpoints: 'checkpoint' }, blob: async (method: string, _container: string, name: string, _query: unknown,
          body: Buffer | undefined, headers: Record<string, string>) => {
          if (method === 'PUT') {
            assert.equal(headers['If-None-Match'], '*');
            assert.ok(!blobs.has(name));
            const etag = `"${blobs.size}"`;
            blobs.set(name, { bytes: Buffer.from(body!), etag });
            return [null, { ':status': '201', etag }];
          }
          const entry = blobs.get(name)!;
          assert.equal(headers['If-Match'], entry.etag);
          return [failure === 'readback' ? Buffer.from('not the original') : entry.bytes, { etag: entry.etag }];
        },
      } as unknown as Parameters<typeof persistDirectEvidence>[0];
      const result = persistDirectEvidence(azure, { assertCurrent: async () => {} }, origin,
        [{ key: 'writer-stderr.raw', bytes: raw.length, sha256: failure === 'source' ? '0'.repeat(64) : sha256, path }]);
      if (failure === 'none') {
        assert.match((await result).path, /\/seal\.json$/);
        const [first, second] = [...blobs.values()];
        assert.deepEqual(Buffer.concat([first.bytes, second.bytes]), raw);
      } else {
        await assert.rejects(result, /readback mismatch|changed during durable transfer/);
        assert.ok(![...blobs.keys()].some(name => name.endsWith('/seal.json')));
      }
    }
  } finally { await rm(directory, { recursive: true, force: true }); }
});
test('requires a clean original indexed client lifetime and its exact retained claim', () => {
  const c = { index: 0, name: 'client', uid, jobUid: run.owner, nodeName: 'client-node', image: `test@sha256:${'a'.repeat(64)}`,
    imageId: `sha256:${'a'.repeat(64)}`, containerId: 'original', startedAt: '2026-01-01T00:00:00Z',
    claimName: 'client-evidence', claimUid: run.namespaceUid, volumeName: 'client-pv' };
  const metadata = { namespace: run.namespace, labels: markers, annotations: markers };
  const job = { kind: 'Job', apiVersion: 'batch/v1', metadata: { ...metadata, uid: c.jobUid, name: 'direct-load-confirmwide' },
    spec: { completionMode: 'Indexed', completions: 1 }, status: { succeeded: 1, conditions: [{ type: 'Complete', status: 'True' }] } };
  const terminated = { containerID: c.containerId, startedAt: c.startedAt, finishedAt: '2026-01-01T00:02:01Z', exitCode: 0, reason: 'Completed' };
  const pod = { kind: 'Pod', apiVersion: 'v1', metadata: { ...metadata, uid, name: c.name, resourceVersion: '31',
    annotations: { ...markers, 'batch.kubernetes.io/job-completion-index': '0' },
    ownerReferences: [{ kind: 'Job', controller: true, uid: c.jobUid }] },
    spec: { nodeName: c.nodeName, containers: [{ name: 'generator', image: c.image }] },
    status: { phase: 'Succeeded', containerStatuses: [{ name: 'generator', imageID: c.imageId, containerID: c.containerId,
      restartCount: 0, state: { terminated } }] } };
  const claim = { kind: 'PersistentVolumeClaim', apiVersion: 'v1', metadata: { ...metadata, uid: c.claimUid, name: c.claimName,
    ownerReferences: [{ kind: 'Pod', controller: true, uid }] }, spec: { storageClassName: 'managed-csi', volumeName: c.volumeName },
    status: { phase: 'Bound' } };
  const finish = () => observeCompletedClients(run, [c], job, { kind: 'PodList', items: [pod] },
    new Map([[c.claimName, claim]]), Date.parse('2026-01-01T00:02:00Z'))[0];
  const helper = clientLogExportPod(run, finish());
  assert.equal(helper.spec.volumes[0].persistentVolumeClaim.claimName, c.claimName);
  claim.metadata.uid = 'another-claim';
  assert.throws(finish, /claim changed/);
  claim.metadata.uid = c.claimUid;
  terminated.exitCode = 137;
  assert.throws(finish, /exact clean exit/);
});
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
    podIp: '10.0.0.47', nodeName: 'writer-node', image: 'pinned', imageId: 'image-id', containerId: 'container',
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

test('never replays an unknown PATCH and only persists the exact original positive response', async () => {
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'a'.repeat(40), fencingToken: 1 };
  const original = { ...router, kind: 'Deployment', metadata: { ...router.metadata, name: 'arrowcol' }, spec: { replicas: 1 } };
  const response = JSON.stringify({ ...original, metadata: { ...original.metadata, resourceVersion: '43', generation: 2 },
    spec: { replicas: 0 } });
  let dispatches = 0;
  for (const [mode, expected] of [['unknown', /connection lost/], ['wrong-uid', /identity changed/], ['success', null]] as const) {
    let bytes: Buffer | undefined;
    let version = '"0"';
    const azure = {
      c: { checkpoints: 'checkpoint' },
      blob: async (method: string, _container: string, _path: string, _query: unknown, body: string | undefined,
        headers: Record<string, string>) => {
        if (method === 'GET') return [bytes, { etag: version }];
        if (headers['If-None-Match'] === '*' && bytes) throw new Error('Already has an intent');
        if (bytes) assert.equal(headers['If-Match'], version);
        bytes = Buffer.from(body!);
        version = bytes.includes('"responded"') ? '"2"' : '"1"';
        return [null, { ':status': '201', etag: version }];
      },
    } as unknown as Parameters<typeof persistDirectStopPatch>[0];
    const invoke = () => persistDirectStopPatch(azure, { assertCurrent: async () => {} }, origin, 'arrowcol', uid,
      original, async ops => {
        dispatches++;
        assert.match(bytes!.toString(), /"phase":"intent"/);
        assert.deepEqual(ops.slice(0, 2).map(op => op.value), [uid, '42']);
        if (mode === 'unknown') throw new Error('connection lost');
        return mode === 'wrong-uid' ? response.replace(uid, run.owner) : response;
      });
    if (expected) await assert.rejects(invoke, expected);
    else {
      const observed = await invoke();
      assert.equal(observed.receipt.response, response);
      assert.equal(observed.version, '"2"');
    }
    await assert.rejects(invoke, /Already has an intent/);
  }
  assert.equal(dispatches, 3);
});

test('exports the original writer directory on the original node with no writeable claim mount', () => {
  const capture = { claimUid: run.owner, volumeName: 'bound-pv' };
  const claim = { kind: 'PersistentVolumeClaim', apiVersion: 'v1',
    metadata: { name: 'arrowcol-writer-staging', namespace: run.namespace, uid: capture.claimUid, labels: markers, annotations: markers },
    spec: { volumeName: capture.volumeName, storageClassName: `arrowcol-${run.runId.replaceAll('-', '')}-local-csi` },
    status: { phase: 'Bound' } };
  const writer = { deploymentUid: 'deployment', replicaSetUid: 'replica', podName: 'pod', podUid: uid,
    podIp: '10.0.0.47', nodeName: 'original-node', image: `example/writer@sha256:${'a'.repeat(64)}`, imageId: 'image-id', containerId: 'container',
    startedAt: '2026-01-01T00:00:00Z', finishedAt: '2026-01-01T00:10:01Z', terminalPodResourceVersion: '21' };
  assert.throws(() => writerLogExportPod(run, writer, capture, { ...claim, metadata: { ...claim.metadata, uid } }), /unverified original/);
  const { spec } = writerLogExportPod(run, writer, capture, claim);
  assert.equal(spec.volumes[0].persistentVolumeClaim.readOnly, true);
  assert.deepEqual(spec.containers[0].volumeMounts[0], {
    name: 'writer-logs', mountPath: '/evidence', readOnly: true, subPath: `benchmark-logs/${uid}`,
  });
  assert.deepEqual(spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].values,
    [writer.nodeName]);
});

  test('streams both writer files unchanged and fails on helper replacement or a checksum discrepancy', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'direct-export-'));
    const savedPath = process.env.PATH;
    const fixture = join(dir, 'kubectl');
    try {
      await writeFile(fixture, `#!/usr/bin/env node
  const args=process.argv.slice(2);
  const [exe,,path]=args.slice(args.indexOf('--')+1);
  const bytes=path.endsWith('stdout')?Buffer.from([0,13,10,255]):Buffer.from('stderr');
  const sha=require('node:crypto').createHash('sha256').update(bytes).digest('hex');
  if(exe.endsWith('sha256sum')) console.log((process.env.CORRUPT_EXPORT === 'yes'?'a'.repeat(64):sha)+'  '+path);
  else process.stdout.write(bytes);
  `);
      await chmod(fixture, 0o700);
      process.env.PATH = `${dir}${delimiter}${savedPath}`;
      const observed = { podUid: uid, containerId: 'original-instance' };
      const guard = { assertCurrent: async () => {} };
      const execute = (directory: string, helper = async () => observed) =>
        exportWriterLogs(guard, run, 'https://test.example', 'none', directory, helper);
      const { files } = await execute(dir);
      assert.deepEqual(await readFile(files[0].local), Buffer.from([0, 13, 10, 255]));
      assert.deepEqual(await readFile(files[1].local), Buffer.from('stderr'));
      let seen = false;
      await assert.rejects(execute(dir, async () => {
        if (seen) return { ...observed, podUid: 'replacement' };
        seen = true;
        return observed;
      }), /helper was replaced/);
      const corrupt = join(dir, 'corrupt');
      await mkdir(corrupt, { mode: 0o700 });
      process.env.CORRUPT_EXPORT = 'yes';
      await assert.rejects(execute(corrupt), /changed during export/);
    } finally {
      process.env.PATH = savedPath;
      delete process.env.CORRUPT_EXPORT;
      await rm(dir, { recursive: true, force: true });
    }
  });
