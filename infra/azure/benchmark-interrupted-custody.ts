import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { create_namespace_deletion_journal } from './aks-namespace-custody.js';
import { canonical_json, runtime, type Azure } from './bootstrap.js';
import { observeWriter, observeWriterExit } from './benchmark-arm.js';
import { evidenceJson } from './benchmark-audit.js';
import { captureInterruptedDelta } from './benchmark-delta-evidence.js';
import { evidenceObject, requireEvidence } from './benchmark-evidence-json.js';
import { inspectDirectRecovery } from './benchmark-direct-recovery.js';
import { acceptRouterFencePatch, routerFenceObserved } from './benchmark-router-fence.js';
import { deploymentStopped } from './benchmark-terminal-custody.js';
import { readDirectPodExit, stopDirectPodWithExit } from './benchmark-pod-exit.js';
import { readDirectStopPatch, stopOriginalWorkload } from './benchmark-stop-patch.js';
import { observeStoppedWriterRegistration, readWriterRegistrations } from './benchmark-writer-sql.js';
import { FENCED, PublisherControl } from './publisher_control.js';
import { assert_owned_namespace_absent, cleanup_run_fic, delete_owned_namespace, observe_run_fic_cleanup,
  recover_run_fic_record, type RunNamespaceIdentity } from './run_fic.js';

type Bound = Awaited<ReturnType<typeof inspectDirectRecovery>>;
const sha = (data: Buffer) => createHash('sha256').update(data).digest('hex');
const strong = (value: string | undefined) => !!value && /^"[^"\r\n]+"$/.test(value);

export async function recoverInterruptedDirectRun(azure: Azure, binding: Bound, context: string,
  workDirectory: string, signal: AbortSignal) {
  const { arm, fic, authority, seal: producerSeal } = binding;
  const { origin } = arm;
  requireEvidence(!producerSeal, 'Interrupted recovery may not replace an original producer terminal seal');
  requireEvidence(/^[a-f0-9]{64}$/.test(arm.baseline.active_files_sha256 ?? ''),
    'Interrupted recovery needs the original physical Delta baseline; no stop authorized');
  const prefix = `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/interrupted`;
  const path = `${prefix}/seal.json`;
  const identity: RunNamespaceIdentity = { version: 1, runId: fic.runId, namespace: fic.namespace, namespaceUid: fic.namespaceUid,
    namespaceOwner: fic.namespaceOwner, aksId: fic.aksId, server: fic.server };
  const journal = create_namespace_deletion_journal(azure, identity, authority);
  const control = new PublisherControl(azure);
  const read = async (target: string, missing: boolean, version?: string) => {
    await authority.assertCurrent();
    const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, target, undefined, undefined,
      version ? { 'If-Match': version } : {}, missing);
    if (missing && bytes === null && Object.keys(headers).length === 0) return null;
    requireEvidence(bytes && bytes.length <= 32 * 1024 ** 2 && headers[':status'] === '200' && strong(headers.etag) &&
      (!version || version === headers.etag), 'Interrupted evidence read outcome UNKNOWN');
    return { bytes, etag: headers.etag! };
  };
  const persist = async (target: string, bytes: Buffer) => {
    const prior = await read(target, true);
    if (prior) {
      requireEvidence(prior.bytes.equals(bytes), 'Original interrupted evidence differs');
      return prior;
    }
    await authority.assertCurrent();
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, target, undefined, bytes,
      { 'x-ms-blob-type': 'BlockBlob', 'If-None-Match': '*' });
    requireEvidence(headers[':status'] === '201' && strong(headers.etag), 'Interrupted evidence write outcome UNKNOWN');
    const stored = await read(target, false, headers.etag);
    requireEvidence(stored?.bytes.equals(bytes), 'Interrupted evidence readback outcome UNKNOWN');
    return stored;
  };
  const observePublisher = async (sealRequired: boolean) => {
    await authority.assertCurrent();
    const [meta, registry, etag] = await control.read();
    requireEvidence(meta.resetgeneration === origin.cloudGeneration && meta.occupancy === 'released' &&
      Object.keys(registry).length === 0 && (!sealRequired || meta.resetstate === FENCED),
    'Original publisher generation still has active or uncertain publishers');
    requireEvidence(strong(etag), 'Original publisher generation has no strong version');
    return { meta, registry, etag };
  };
  const get = async (...args: string[]) => {
    await authority.assertCurrent();
    const result = await runtime.run_tree(['kubectl', '--context', context, '--server', fic.server, '--insecure-skip-tls-verify=false',
      '--request-timeout=15s', '-n', origin.namespace, 'get', ...args, '-o', 'json'], 20, undefined, undefined, undefined, 8_388_608);
    requireEvidence(result.returncode === 0, 'Original workload observation failed');
    return JSON.parse(result.stdout) as unknown;
  };
  const assertNamespace = async () => {
    const ns = evidenceObject(await get('namespace', origin.namespace)), meta = evidenceObject(ns.metadata);
    requireEvidence(ns.kind === 'Namespace' && meta.uid === origin.namespaceUid && meta.name === origin.namespace && !meta.deletionTimestamp,
      'Original namespace UID changed');
    for (const field of ['annotations', 'labels']) {
      const owner = evidenceObject(meta[field]);
      requireEvidence(owner['arrowcol-oat/run-id'] === origin.runId && owner['arrowcol-oat/owner-token'] === origin.owner,
        'Original namespace ownership changed');
    }
  };
  const pods = (role: string) => get('pods', '-l', `app=${role}`);
  const routeClosed = async (generation: number) => {
    const [router, routerPods, shared, local] = await Promise.all([get('daemonset', 'arrowcol-router'), pods('arrowcol-router'),
      get('endpointslices', '-l', 'kubernetes.io/service-name=arrowcol-router'),
      get('endpointslices', '-l', 'kubernetes.io/service-name=arrowcol-router-node-local')]);
    return routerFenceObserved(origin, arm.routerUid, generation, router, routerPods,
      { 'arrowcol-router': shared, 'arrowcol-router-node-local': local });
  };
  const readStop = (role: 'arrowcol' | 'delta-maintenance', lifetime: typeof arm.writer) =>
    stopDirectPodWithExit(azure, authority, context, fic.server, origin, role, lifetime, true);
  const freeze = async () => {
    await assertNamespace();
    const router = await stopOriginalWorkload(azure, authority, context, fic.server, origin, 'arrowcol-router', arm.routerUid,
      await get('daemonset', 'arrowcol-router'));
    const generation = acceptRouterFencePatch(origin, arm.routerUid, router.receipt.intent.observed_generation, JSON.parse(router.receipt.response));
    const deadline = Date.now() + 420_000;
    while (!await routeClosed(generation)) {
      requireEvidence(Date.now() < deadline, 'Original router still admits traffic');
      await delay(1000, undefined, { signal });
    }
    const writer = await readStop('arrowcol', arm.writer);
    const writerExit = await readDirectPodExit(azure, authority, origin, 'arrowcol', arm.writer, writer);
    const sql = await readWriterRegistrations(authority, origin, origin.cloudGeneration, context, fic.server,
      { name: arm.sql.podName, uid: arm.sql.podUid }, async () => {
        const [deployment, sets, running] = await Promise.all([
          get('deployment', 'arrowcol-sql'), get('replicasets', '-l', 'app=arrowcol-sql'), pods('arrowcol-sql')]);
        requireEvidence(canonical_json(observeWriter(origin, deployment, sets, running, 'arrowcol-sql')) === canonical_json(arm.sql),
          'Original SQL lifetime changed');
      }, true);
    const stopped = observeStoppedWriterRegistration(origin, origin.cloudGeneration,
      observeWriterExit(origin, arm.writer, writerExit.pod, writer.receipt.intent.intent_at_ms), arm.registration, sql);
    const maintenance = await readStop('delta-maintenance', arm.maintenance);
    const maintenanceExit = await readDirectPodExit(azure, authority, origin, 'delta-maintenance', arm.maintenance, maintenance);
    requireEvidence(router.receipt.intent.intent_at_ms <= writer.receipt.intent.intent_at_ms &&
      writer.receipt.intent.intent_at_ms <= maintenance.receipt.intent.intent_at_ms, 'Interrupted stop order changed');
    deploymentStopped(origin, writer, await get('deployment', 'arrowcol'), await pods('arrowcol'), arm.writer.podUid);
    deploymentStopped(origin, maintenance, await get('deployment', 'delta-maintenance'), await pods('delta-maintenance'), arm.maintenance.podUid);
    requireEvidence(await routeClosed(generation), 'Original router reopened admission');
    const before = await observePublisher(false);
    if (before.meta.resetstate !== FENCED) {
      await authority.assertCurrent();
      await control.cas({ ...before.meta, resetstate: FENCED }, before.registry, before.etag);
    }
    await observePublisher(true);
    return { router, writer, writerExit: writerExit.etag, maintenance, maintenanceExit: maintenanceExit.etag, sql: stopped };
  };
  const capture = async () => {
    const publisher = await observePublisher(true);
    const folder = await mkdtemp(join(workDirectory, 'delta-interrupted-'));
    try {
      const current = await captureInterruptedDelta(azure, authority, origin.runId, arm.baseline.table_root, arm.baseline, folder);
      requireEvidence((await observePublisher(true)).etag === publisher.etag, 'Original publisher fence changed during Delta inspection');
      return { proof: { snapshot: current.snapshot, physical: current.physical,
        commits: current.evidence.map(({ key, bytes, sha256 }) => ({ key, bytes, sha256 })) },
      bytes: await Promise.all(current.evidence.map(async file => {
        return { key: file.key, content: await readFile(file.path) };
      })) };
    } finally { await rm(folder, { recursive: true, force: true }); }
  };
  const original = { origin, armHash: sha(Buffer.from(canonical_json(arm))), schema_version: 1, result: 'interrupted_not_scored' };
  let sealed = await read(path, true);
  if (!sealed) {
    requireEvidence(fic.phase === 'ready', 'Original cleanup has no interrupted evidence seal');
    if (binding.preAdmission) {
      await assertNamespace();
      const cm = evidenceObject(await get('configmap', 'direct-schedule-confirmwide'));
      const meta = evidenceObject(cm.metadata), fields = evidenceObject(cm.data);
      requireEvidence(cm.kind === 'ConfigMap' && meta.namespace === origin.namespace && meta.name === 'direct-schedule-confirmwide' &&
        !meta.deletionTimestamp && meta.uid === arm.heldBarrier?.uid && meta.resourceVersion === arm.heldBarrier?.resourceVersion &&
        fields.start === 'pending' && fields.writerLifetime === 'pending' && fields.clientLifetimes === 'pending',
      'Original pre-admission pending barrier changed; no stop authorized.');
      for (const field of ['annotations', 'labels']) {
        const owner = evidenceObject(meta[field]);
        requireEvidence(owner['arrowcol-oat/run-id'] === origin.runId && owner['arrowcol-oat/owner-token'] === origin.owner,
          'Original pre-admission pending barrier ownership changed.');
      }
    }
    const drain = await freeze();
    const delta = await capture();
    for (const file of delta.bytes) await persist(`${prefix}/${file.key}`, file.content);
    sealed = await persist(path, Buffer.from(evidenceJson({ ...original, drain, deltaHash: sha(Buffer.from(evidenceJson(delta.proof))) })));
  }
  const originalSeal = sealed;
  requireEvidence(originalSeal, 'Interrupted terminal seal is not confirmed.');
  const checkSeal = async () => {
    const value = evidenceObject(JSON.parse(originalSeal.bytes.toString('utf8')));
    const { drain, deltaHash, ...identityFields } = value;
    requireEvidence(canonical_json(identityFields) === canonical_json(original) && typeof deltaHash === 'string',
      'Interrupted evidence seal belongs to a different original arm');
    const saved = evidenceObject(drain);
    for (const [key, role, uid] of [['router', 'arrowcol-router', arm.routerUid],
      ['writer', 'arrowcol', arm.writer.deploymentUid], ['maintenance', 'delta-maintenance', arm.maintenance.deploymentUid]] as const) {
      const stop = await readDirectStopPatch(azure, authority, origin, role, uid);
      requireEvidence(stop && canonical_json(saved[key]) === canonical_json(stop), 'Interrupted stop evidence changed');
      if (key !== 'router') {
        const exit = await readDirectPodExit(azure, authority, origin, role as 'arrowcol' | 'delta-maintenance', arm[key], stop);
        requireEvidence(exit.etag === saved[`${key}Exit`], 'Interrupted exit evidence changed');
      }
    }
    const delta = await capture();
    requireEvidence(sha(Buffer.from(evidenceJson(delta.proof))) === deltaHash, 'Interrupted original Delta history or physical files changed');
    for (const file of delta.bytes) {
      const copy = await read(`${prefix}/${file.key}`, false);
      requireEvidence(copy?.bytes.equals(file.content), 'Interrupted original Delta commit copy is missing or changed');
    }
    const reread = await read(path, false, originalSeal.etag);
    requireEvidence(reread?.bytes.equals(originalSeal.bytes), 'Interrupted original evidence seal changed');
  };
  await checkSeal();
  if (fic.phase === 'ready') {
    const folder = join(workDirectory, 'cleanup-interrupted');
    await mkdir(folder, { mode: 0o700, recursive: true });
    const namespace = await delete_owned_namespace(azure, identity, join(folder, 'namespace.json'), authority, journal, true);
    await journal.complete(namespace);
    const record = join(folder, 'fic.json');
    await recover_run_fic_record(azure, fic, record, authority);
    await cleanup_run_fic(azure, origin.runId, record, authority);
  }
  await journal.stopped();
  await assert_owned_namespace_absent(identity, authority);
  requireEvidence(await observe_run_fic_cleanup(azure, fic, authority) === 'deleted', 'Original FIC cleanup remains unsettled');
  await checkSeal();
  return 'direct_recovery_preserved_not_scored' as const;
}
