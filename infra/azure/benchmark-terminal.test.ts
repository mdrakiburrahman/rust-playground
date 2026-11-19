import assert from 'node:assert/strict';
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { tmpdir } from 'node:os';
import { delimiter, join } from 'node:path';
import test from 'node:test';
import { acceptRouterFencePatch, observeRouterLifetimes, planRouterFence, routerFenceObserved } from './benchmark-router-fence.js';
import { persistDirectStopPatch } from './benchmark-stop-patch.js';
import { observeCompletedClients } from './benchmark-client-terminal.js';
import { persistDirectEvidence } from './benchmark-evidence-custody.js';
import { captureDirectBaseline, captureFrozenDelta, directDeltaReplay } from './benchmark-delta-evidence.js';
import { directCatalogRoot, type DirectRelease } from './benchmark-direct-arm.js';
import { checkDirectClocks, directClockBound, directClockOffsets } from './benchmark-clock.js';
import { directProducerAuthority } from './benchmark-run-authority.js';
import type { CampaignStore, StoredCampaign } from '../../projects/arrowcol-oat/aks-campaign.js';
import type { Azure } from './bootstrap.js';
import { validateDirectTerminal, type DirectTerminalArm, type DirectTerminalObservations } from './benchmark-terminal-custody.js';
import { positiveDirectExitEvent, readDirectPodExit, stopDirectPodWithExit } from './benchmark-pod-exit.js';
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
test('requires a current conditional campaign owner and exact direct-run inventory', async () => {
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
  const now = Date.now();
  const state = { status: 'active', campaignId: uid, ownerId: run.owner, fencingToken: 1, heartbeatAtMs: now - 1,
    expiresAtMs: now + 60_000, runInventory: { campaign_id: uid, owner_id: run.owner, cell_run_id: run.runId, fencing_token: 1,
      namespace: { name: run.namespace, uid: run.namespaceUid, owner_token: run.owner }, load_admission: 'authorized',
      fic: { state: 'observed' }, qap: null, cleanup: { handoff: { state: 'pending' } } } };
  const store: CampaignStore = { read: async () => ({ state, etag: '"1"' } as StoredCampaign),
    write: async () => { throw new Error('Authority checks must not upgrade or rewrite the campaign'); } };
  const authority = directProducerAuthority({ store, campaignId: uid, ownerId: run.owner }, origin);
  await authority.assertCurrent();
  state.runInventory.namespace.uid = 'replacement';
  await assert.rejects(() => authority.assertCurrent(), /does not own/);
  state.runInventory.namespace.uid = run.namespaceUid;
  state.fencingToken = 2;
  await assert.rejects(() => authority.assertCurrent(), /fenced/);
});
test('requires a unique original catalog receipt and conservative clock brackets', () => {
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
  const image = `test@sha256:${'a'.repeat(64)}`;
  const release: DirectRelease = { revision: origin.sourceRevision, schemaFingerprint: 'schema',
    images: { writer: image, maintenance: image, sql: image, client: image, router: image, catalog: image } };
  const root = `direct-baseline-1/runs/${run.runId}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states`;
  const receipt = { kind: 'oat_run_table_registered', run_id: run.runId, provider: 'delta', schema_version: 1,
    catalog_table_count: 1, schema_fingerprint: 'schema', table: 'telemetry.sqlserver.sqlserver_availability_replica_states',
    location: `abfss://data@account.dfs.core.windows.net/${root}` };
  const check = (text: string) => directCatalogRoot(text, origin, release, 'account', 'data');
  const log = JSON.stringify(receipt);
  assert.equal(check(log), root);
  assert.throws(() => check(`${log}\n${log}`), /Original catalog receipt/);
  assert.throws(() => check(log.replace('abfss://data@account', 'abfss://data@foreign')), /Original catalog receipt/);
  assert.throws(() => check(log.replace('"schema_version":1', '"schema_version":1,"schema_version":1')), /duplicate/i);
  const now = 1767225600000, ns = BigInt(now) * 1_000_000n;
  assert.equal(directClockBound(now, now + 98, String(ns)).lowerNs, '-99000000');
  assert.throws(() => directClockBound(now, now + 100, String(ns)), /uncertainty/);
  assert.throws(() => directClockBound(now, now + 1, String(ns + 101_000_000n)), /uncertainty/);
  const before = [{ node: 'n', podUid: 'original', beforeMs: now, afterMs: now, observedNs: String(ns), timeNamespace: 'time:[1]' }];
  const after = [{ node: 'n', podUid: 'router', beforeMs: now + 10_000, afterMs: now + 10_000,
    observedNs: String(ns + 10_000_000_000n), timeNamespace: 'time:[1]' }];
  const nodes = new Map([['n', { before: 'original', after: 'router' }]]);
  checkDirectClocks(nodes, now + 100, now + 10_000, before, after);
  assert.throws(() => checkDirectClocks(nodes, now + 100, now + 10_000, before, []), /coverage/);
  assert.throws(() => checkDirectClocks(nodes, now + 100, now + 10_000, before,
    [{ ...after[0], podUid: 'replacement' }]), /bracket/);
  assert.throws(() => checkDirectClocks(nodes, now + 100, now + 10_000, before,
    [{ ...after[0], timeNamespace: 'time:[2]' }]), /time namespace/);
  assert.throws(() => checkDirectClocks(nodes, now + 100, now + 10_001, before, after), /bracket/);
  assert.deepEqual(directClockOffsets(nodes, now + 100, now + 10_000, before, after).get('n'),
    { lowerNs: -1_000_000n, upperNs: 0n });
});
test('requires a clean MODIFIED event for the original Pod; disappearance, restart and watch expiry fail', async () => {
  const imageId = `sha256:${'a'.repeat(64)}`, image = `test@${imageId}`, startedAt = '2026-01-01T00:00:00Z';
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
  const original = { deploymentUid: uid, replicaSetUid: uid, podName: 'pod', podUid: uid, podIp: '10.0.0.1',
    nodeName: 'node', image, imageId, containerId: 'original', startedAt };
  const status = { name: 'dataflow', imageID: imageId, restartCount: 0, containerID: 'original',
    state: { terminated: { startedAt, finishedAt: '2026-01-01T00:01:01Z', containerID: 'original', exitCode: 0, reason: 'Completed' } } };
  const pod = { apiVersion: 'v1', kind: 'Pod', metadata: { uid, name: 'pod', namespace: run.namespace, resourceVersion: '8',
    labels: markers, annotations: markers, ownerReferences: [{ kind: 'ReplicaSet', uid, controller: true }] },
  spec: { nodeName: 'node', containers: [{ name: 'dataflow', image }] }, status: { containerStatuses: [status] } };
  async function* watch(...events: unknown[]) {
    for (const event of events) yield JSON.stringify(event);
  }
  const verify = (...events: unknown[]) => positiveDirectExitEvent(watch(...events), origin, 'arrowcol', original,
    Date.parse('2026-01-01T00:01:00Z'));
  assert.deepEqual(await verify({ type: 'MODIFIED', object: pod }), pod);
  await assert.rejects(verify({ type: 'DELETED', object: pod }), /positive status event/);
  await assert.rejects(verify({ type: 'ERROR', object: { code: 410 } }), /positive status event/);
  await assert.rejects(verify(), /ended without positive terminal evidence/);
  status.restartCount = 1;
  await assert.rejects(verify({ type: 'MODIFIED', object: pod }), /lifetime changed/);
});
test('real stop adapter consumes a fenced PATCH and durably replays an exact pre-PATCH Pod watch', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'direct-stop-kubectl-'));
  const oldPath = process.env.PATH;
  const trace = join(dir, 'trace');
  const fake = join(dir, 'kubectl');
  const imageId = `sha256:${'a'.repeat(64)}`, image = `test@${imageId}`, startedAt = new Date(Date.now() - 10_000).toISOString();
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
  const original = { deploymentUid: uid, replicaSetUid: uid, podName: 'pod', podUid: uid, podIp: '10.0.0.1',
    nodeName: 'node', image, imageId, containerId: 'original', startedAt };
  const meta = { uid, name: 'pod', namespace: run.namespace, resourceVersion: '17', labels: markers, annotations: markers,
    ownerReferences: [{ kind: 'ReplicaSet', uid, controller: true }] };
  const live = { apiVersion: 'v1', kind: 'Pod', metadata: meta,
    spec: { terminationGracePeriodSeconds: 30, nodeName: 'node', containers: [{ name: 'dataflow', image }] },
    status: { containerStatuses: [{ name: 'dataflow', imageID: imageId, restartCount: 0, containerID: 'original',
      state: { running: { startedAt } } }] } };
  const deployment = { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { ...meta, name: 'arrowcol', generation: 1, resourceVersion: '27' },
    spec: { replicas: 1 } };
  const objects = new Map<string, { bytes: Buffer; etag: string }>();
  const blob: Azure['blob'] = async (method, _, path, __, data, headers) => {
    assert.ok(path);
    const current = objects.get(path);
    if (method === 'PUT') {
      assert.equal(current?.etag, headers?.['If-Match']);
      if (!current) assert.equal(headers?.['If-None-Match'], '*');
      const body = Buffer.isBuffer(data) ? data : Buffer.from(String(data));
      const etag = `"${createHash('sha256').update(body).digest('hex')}"`;
      objects.set(path, { bytes: body, etag });
      return [null, { ':status': '201', etag }];
    }
    assert.equal(method, 'GET');
    assert.ok(current);
    return [current.bytes, { ':status': '200', etag: current.etag }];
  };
  try {
    await writeFile(fake, `#!${process.execPath}
const fs=require('node:fs');
const a=process.argv.slice(2);
const assert=require('node:assert/strict');
const live=${JSON.stringify(live)};
const deployment=${JSON.stringify(deployment)};
const trace=${JSON.stringify(trace)};
assert.equal(a[a.indexOf('--server')+1],'https://fixture.example');
if(a.includes('--raw')){
 const url=new URL(a.at(-1),'https://fixture.example');
 assert.equal(url.searchParams.get('resourceVersion'),'17');
 assert.equal(url.searchParams.get('fieldSelector'),'metadata.name=pod');
 assert.equal(fs.readFileSync(trace,'utf8'),'patched');
 live.metadata.resourceVersion='28';
 live.status.containerStatuses[0].state={terminated:{startedAt:${JSON.stringify(startedAt)},finishedAt:new Date().toISOString(),
   containerID:'original',exitCode:0,reason:'Completed'}};
 console.log(JSON.stringify({type:'MODIFIED',object:live}));
}else if(a.includes('patch')){
 const ops=JSON.parse(a[a.indexOf('--patch')+1]);
 assert.deepEqual(ops.map(v=>v.value),[${JSON.stringify(uid)},'27',0]);
 fs.writeFileSync(trace,'patched',{flag:'wx'});
 deployment.metadata.resourceVersion='28';deployment.metadata.generation=2;deployment.spec.replicas=0;
 console.log(JSON.stringify(deployment));
}else console.log(JSON.stringify(a.includes('deployment')?deployment:live));`);
    await chmod(fake, 0o700);
    process.env.PATH = `${dir}${delimiter}${oldPath}`;
    const azure = { c: { checkpoints: 'checkpoint' }, blob } as Azure, authority = { assertCurrent: async () => {} };
    const stop = await stopDirectPodWithExit(azure, authority, 'fixture', 'https://fixture.example', origin, 'arrowcol', original);
    const readback = await readDirectPodExit(azure, authority, origin, 'arrowcol', original, stop);
    assert.match(readback.bytes.toString(), /"podResourceVersionBeforeStop":"17"/);
    assert.match(readback.bytes.toString(), /"exitCode":0/);
    assert.equal(objects.size, 2);
  } finally {
    process.env.PATH = oldPath;
    await rm(dir, { recursive: true });
  }
});
test('replays the direct invariants protocol and rejects deletion vectors and unknown actions', () => {
  const replay = directDeltaReplay();
  const commit = (...actions: unknown[]) => Buffer.from(actions.map(value => JSON.stringify(value)).join('\n') + '\n');
  replay.apply(commit({ protocol: { minReaderVersion: 3, minWriterVersion: 7, readerFeatures: [], writerFeatures: ['invariants'] } },
    { metaData: { id: uid } }, { add: { path: 'hour=00/first.parquet', size: 237 } }));
  replay.apply(commit({ remove: { path: 'hour=00/first.parquet', dataChange: false } },
    { add: { path: 'hour=00/optimized.parquet', size: 67, dataChange: false } }));
  assert.deepEqual(replay.snapshot, { table_id: uid, version: 1, active_files: [{ path: 'hour=00/optimized.parquet', size: 67n }] });
  assert.throws(() => replay.apply(commit({ add: { path: 'bad.parquet', size: 10, deletionVector: {} } })), /unsupported Delta add/);
  assert.throws(() => replay.apply(commit({ cdc: { path: 'bad.parquet' } })), /Unsupported direct Delta action/);
  assert.equal(replay.snapshot.version, 1, 'a rejected commit must not advance the replay');
  assert.throws(() => directDeltaReplay().apply(commit({ protocol: { minReaderVersion: 3, minWriterVersion: 7,
    readerFeatures: ['deletionVectors'], writerFeatures: ['invariants'] } })), /Unsupported or changed direct Delta protocol/);
});
test('refuses a checkpoint beyond the available JSON before downloading any commits', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'direct-checkpoint-'));
  const tableRoot = `direct-baseline-1/runs/${run.runId}/dataflow/table`;
  const calls: string[] = [];
  const blob: Azure['blob'] = async (method, _, path, query) => {
    calls.push(`${method} ${path}`);
    const prefix = `${tableRoot}/_delta_log/`;
    assert.equal(query?.prefix, prefix);
    const names = ['00000000000000000000.json', '00000000000000000001.json', '00000000000000000009.checkpoint.parquet'];
    const objects = names.map(name => `<Blob><Name>${prefix}${name}</Name><Properties><Etag>"original"</Etag></Properties></Blob>`).join('');
    return [Buffer.from(`<EnumerationResults><Prefix>${prefix}</Prefix><Blobs>${objects}</Blobs><NextMarker /></EnumerationResults>`),
      { ':status': '200', 'x-ms-request-id': 'read' }];
  };
  try {
    await assert.rejects(() => captureFrozenDelta({ c: { data: 'data' }, blob } as Azure, { assertCurrent: async () => {} }, run.runId,
      tableRoot, { table_root: tableRoot, table_id: uid, version: 0, last_commit_sha256: '0'.repeat(64), history_sha256: '0'.repeat(64) }, directory),
    /checkpoint extends beyond the complete JSON/);
    assert.deepEqual(calls, ['GET ']);
  } finally { await rm(directory, { recursive: true }); }
});
test('binds the whole pre-load Delta history, not just its last commit', async () => {
  const baselineDirectory = await mkdtemp(join(tmpdir(), 'direct-baseline-'));
  const frozenDirectory = await mkdtemp(join(tmpdir(), 'direct-frozen-'));
  const tableRoot = `direct-baseline-1/runs/${run.runId}/dataflow/table`, prefix = `${tableRoot}/_delta_log/`;
  const serialize = (...actions: unknown[]) => Buffer.from(actions.map(action => JSON.stringify(action)).join('\n') + '\n');
  const creation = serialize({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }, { metaData: { id: uid } });
  const commits = [creation, serialize({ commitInfo: { operation: 'WRITE' } })];
  const blob: Azure['blob'] = async (method, _, path, query, __, headers) => {
    assert.equal(method, 'GET');
    if (!path) {
      assert.equal(query?.prefix, prefix);
      const objects = commits.map((_, v) => `<Blob><Name>${prefix}${String(v).padStart(20, '0')}.json</Name>` +
        `<Properties><Etag>"v${v}"</Etag></Properties></Blob>`).join('');
      return [Buffer.from(`<EnumerationResults><Prefix>${prefix}</Prefix><Blobs>${objects}</Blobs><NextMarker /></EnumerationResults>`),
        { ':status': '200', 'x-ms-request-id': 'read' }];
    }
    const v = Number(path.slice(prefix.length, -5));
    assert.equal(headers?.['If-Match'], `"v${v}"`);
    return [commits[v], { ':status': '200', etag: `"v${v}"` }];
  };
  const azure = { c: { data: 'data' }, blob } as Azure, authority = { assertCurrent: async () => {} };
  try {
    const { observedBaseline } = await captureDirectBaseline(azure, authority, run.runId, tableRoot, baselineDirectory);
    assert.equal(observedBaseline.version, 1);
    commits[0] = Buffer.concat([creation, serialize({ commitInfo: { operation: 'ALTER' } })]);
    commits.push(serialize({ commitInfo: { operation: 'WRITE' } }));
    await assert.rejects(() => captureFrozenDelta(azure, authority, run.runId, tableRoot, observedBaseline, frozenDirectory),
      /pre-load Delta baseline no longer matches/);
  } finally {
    await rm(baselineDirectory, { recursive: true });
    await rm(frozenDirectory, { recursive: true });
  }
});
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

test('pins original ready router pod lifetimes to the six-role image', () => {
  const image = `registry/router@sha256:${'a'.repeat(64)}`;
  const daemon = { ...router, status: { observedGeneration: 1, desiredNumberScheduled: 1, currentNumberScheduled: 1, numberReady: 1 },
    spec: { template: { spec: { containers: [{ name: 'router', image }] } } } };
  const status = { name: 'router', ready: true, restartCount: 0, imageID: `sha256:${'a'.repeat(64)}`,
    containerID: 'container', state: { running: { startedAt: '2026-01-01T00:00:00Z' } } };
  const pod = { kind: 'Pod', apiVersion: 'v1', metadata: { uid, name: 'router-pod', namespace: run.namespace,
    labels: markers, annotations: markers, ownerReferences: [{ kind: 'DaemonSet', uid, controller: true }] },
    spec: { nodeName: 'node', containers: [{ name: 'router', image }] }, status: { phase: 'Running', containerStatuses: [status] } };
  const pods = { kind: 'PodList', items: [pod] };
  assert.equal(observeRouterLifetimes(run, uid, image, daemon, pods)[0].containerId, 'container');
  status.imageID = `sha256:${'b'.repeat(64)}`;
  assert.throws(() => observeRouterLifetimes(run, uid, image, daemon, pods), /lifetime/);
  status.imageID = `sha256:${'a'.repeat(64)}`;
  status.restartCount = 1;
  assert.throws(() => observeRouterLifetimes(run, uid, image, daemon, pods), /lifetime/);
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

test('direct terminal gate blocks reordered stop receipts before consuming terminal observations', () => {
  const endMs = Date.parse('2026-01-01T00:02:00Z');
  const origin = { ...run, cloudGeneration: uid, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
  const original = {
    deploymentUid: uid, replicaSetUid: uid, podName: 'pod', podUid: uid, podIp: '10.0.0.1', nodeName: 'node',
    image: `test@sha256:${'a'.repeat(64)}`, imageId: `sha256:${'a'.repeat(64)}`, containerId: 'original', startedAt: '2026-01-01T00:00:00Z',
  };
  const arm: DirectTerminalArm = {
    origin, startMs: endMs - 60000, endMs, clients: [], writer: original, maintenance: original, sql: original,
    catalogLifetime: original, routers: [], routerUid: uid,
    provenance: { clocks: [], release: {}, catalog: {}, catalogLog: '', schedule: [] },
    writerClaim: { claimUid: run.owner, volumeName: 'pv' }, baseline: {
      table_id: uid, table_root: `direct-baseline-1/runs/${run.runId}/dataflow/table`, version: 1, last_commit_sha256: '0'.repeat(64),
      history_sha256: '0'.repeat(64),
    }, registration: { instance_id: uid, instance_key: '', implementation_pool: 'dataflow', flight_endpoint: '',
      state: 'ready', started_at_utc: original.startedAt, row_version: '0x0000000000000001' },
  };
  const make = (role: 'arrowcol-router' | 'arrowcol' | 'delta-maintenance', at: number) => ({
    path: `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/stop-${role}.json`, version: '"1"',
    receipt: { phase: 'responded', intent: { schema_version: 1, origin, role, uid, ops: [], observed_generation: 1, intent_at_ms: at },
      response: '{}', response_sha256: createHash('sha256').update('{}').digest('hex') },
  });
  const observation: DirectTerminalObservations = {
    stops: { router: make('arrowcol-router', endMs + 2), writer: make('arrowcol', endMs + 1), maintenance: make('delta-maintenance', endMs + 3) },
    job: null, clients: null, claims: new Map(), router: null, routerPods: null,
    routerEndpoints: { 'arrowcol-router': null, 'arrowcol-router-node-local': null },
    writer: null, writerClaim: null, writerDeployment: null, writerPods: null,
    maintenance: null, maintenanceDeployment: null, maintenancePods: null, sql: [],
  };
  assert.throws(() => validateDirectTerminal(arm, observation), /sequence is out of order/);
  observation.stops.writer.receipt.response = '{"wrong":true}';
  assert.throws(() => validateDirectTerminal(arm, observation), /original response/);
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
