import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { observeWriter, prepareDirectBarrier } from './benchmark-arm.js';
import { captureDirectBaseline } from './benchmark-delta-evidence.js';
import { evidenceList, evidenceObject, evidenceText, parseEvidence, requireEvidence } from './benchmark-evidence-json.js';
import { directProducerAuthority, type DirectCampaignScope } from './benchmark-run-authority.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import { directArmRecordPath, type DirectTerminalArm } from './benchmark-terminal-custody.js';
import { pinWriterRegistration, readWriterRegistrations } from './benchmark-writer-sql.js';
import { canonical_json, type Azure } from './bootstrap.js';
import { PublisherControl, READY } from './publisher_control.js';
import { read_run_fic_record, verify_run_fic } from './run_fic.js';

const exec = promisify(execFile);
const digest = /^[^\s@]+@sha256:[a-f0-9]{64}$/;
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const table = 'telemetry.sqlserver.sqlserver_availability_replica_states';
const registration = 'direct-catalog-verified';
export type DirectRelease = {
  revision: string;
  schemaFingerprint: string;
  images: Record<'writer' | 'client' | 'maintenance' | 'sql', string>;
};

export function directCatalogRoot(log: string, origin: DirectStopOrigin, release: DirectRelease, account: string, data: string) {
  const entries = log.trimEnd().split('\n').filter(line => line.includes('"oat_run_table_registered"'))
    .map(line => evidenceObject(parseEvidence(line)));
  const relative = `direct-baseline-1/runs/${origin.runId}/dataflow/${table.replaceAll('.', '/')}`;
  const [receipt] = entries;
  requireEvidence(entries.length === 1 && receipt.kind === 'oat_run_table_registered' && receipt.run_id === origin.runId &&
    receipt.table === table && receipt.location === `abfss://${data}@${account}.dfs.core.windows.net/${relative}` &&
    receipt.provider === 'delta' && receipt.schema_version === 1n && receipt.catalog_table_count === 1n &&
    receipt.schema_fingerprint === release.schemaFingerprint, 'Original catalog receipt does not prove the configured table, path and schema');
  return relative;
}

export function directClockBound(beforeMs: number, afterMs: number, observedNs: string) {
  requireEvidence(Number.isSafeInteger(beforeMs) && Number.isSafeInteger(afterMs) && beforeMs > 0 && afterMs >= beforeMs &&
    /^[1-9]\d{17,18}$/.test(observedNs), 'Original clock sample is invalid');
  const epoch = BigInt(observedNs), lower = epoch - (BigInt(afterMs) + 1n) * 1_000_000n,
    upper = epoch - BigInt(beforeMs) * 1_000_000n;
  requireEvidence(lower >= -100_000_000n && upper <= 100_000_000n, 'Original clock uncertainty exceeds 100 ms');
  return { beforeMs, afterMs, observedNs, lowerNs: String(lower), upperNs: String(upper) };
}

/** Call only with the approved immutable release pins and an existing exclusive run authority. */
export async function armDirectBenchmark(azure: Azure, campaign: DirectCampaignScope, origin: DirectStopOrigin,
  release: DirectRelease, context: string, ficRecordPath: string, baselineDirectory: string): Promise<DirectTerminalArm> {
  const authority = directProducerAuthority(campaign, origin);
  requireEvidence([origin.runId, origin.namespaceUid, origin.owner, origin.cloudGeneration].every(id => uuid.test(id)) &&
    Number.isSafeInteger(origin.fencingToken) && origin.fencingToken > 0 && /^[a-z0-9-]+$/.test(origin.namespace) &&
    Number.isInteger(origin.clients) && origin.clients >= 1 && origin.clients <= 8 && /^[a-f0-9]{40}$/.test(release.revision) &&
    release.revision === origin.sourceRevision && release.schemaFingerprint.length > 0 && release.schemaFingerprint.length < 129 &&
    Object.values(release.images).every(image => digest.test(image)), 'Direct arm lacks original authority, source or release pins');
  const fic = read_run_fic_record(ficRecordPath, azure.c);
  requireEvidence(fic.runId === origin.runId && fic.namespaceUid === origin.namespaceUid && fic.namespace === origin.namespace &&
    fic.namespaceOwner === origin.owner && fic.phase === 'ready' && /^https:\/\/[a-z0-9.-]+$/.test(fic.server), 'Original run FIC is not ready');
  const guard = () => authority.assertCurrent();
  const run = async (...args: string[]) => {
    await guard();
    const { stdout } = await exec('kubectl', ['--context', context, '--server', fic.server, '--insecure-skip-tls-verify=false',
      '--request-timeout=15s', '-n', origin.namespace, ...args], { timeout: 20_000, maxBuffer: 8_388_608 });
    return stdout;
  };
  const get = async (...args: string[]) => JSON.parse(await run('get', ...args, '-o', 'json')) as unknown;
  const owned = (raw: unknown, kind: string, name: string) => {
    const obj = evidenceObject(raw), metadata = evidenceObject(obj.metadata);
    requireEvidence(obj.kind === kind && metadata.name === name && metadata.namespace === origin.namespace &&
      !metadata.deletionTimestamp && uuid.test(evidenceText(metadata.uid)), `Original ${kind} identity is unavailable`);
    for (const field of ['annotations', 'labels']) {
      const owner = evidenceObject(metadata[field]);
      requireEvidence(owner['arrowcol-oat/run-id'] === origin.runId && owner['arrowcol-oat/owner-token'] === origin.owner,
        `Original ${kind} ownership changed`);
    }
    return { obj, metadata };
  };
  const workload = async (role: 'arrowcol-sql' | 'delta-maintenance') => {
    const [deployment, sets, pods] = await Promise.all([get('deployment', role), get('replicasets', '-l', `app=${role}`),
      get('pods', '-l', `app=${role}`)]);
    const result = observeWriter(origin, deployment, sets, pods, role);
    requireEvidence(result.image === release.images[role === 'arrowcol-sql' ? 'sql' : 'maintenance'],
      `Original ${role} image differs from the approved release pin`);
    return result;
  };
  await guard();
  await verify_run_fic(azure, fic);
  const control = new PublisherControl(azure);
  const checkPublisher = async () => {
    await guard();
    const [metadata, registry] = await control.read();
    requireEvidence(metadata.resetgeneration === origin.cloudGeneration && metadata.resetstate === READY &&
      metadata.occupancy === 'released' && Object.keys(registry).length > 0 &&
      Object.values(registry).every(publisher => publisher.state === 'active'), 'Original publisher generation is not ready');
  };
  await checkPublisher();
  const readRegistrationPod = async () => {
    const job = owned(await get('job', registration), 'Job', registration);
    const state = evidenceObject(job.obj.status);
    requireEvidence(state.succeeded === 1 && !state.failed && !state.active &&
      evidenceList(state.conditions).map(evidenceObject).some(condition => condition.type === 'Complete' && condition.status === 'True'),
    'Original catalog registration did not finish cleanly');
    const items = evidenceList(evidenceObject(await get('pods', '-l', `batch.kubernetes.io/job-name=${registration}`)).items);
    requireEvidence(items.length === 1, 'Original catalog registration Pod coverage is ambiguous');
    const candidate = evidenceObject(items[0]);
    const podName = evidenceText(evidenceObject(candidate.metadata).name);
    const pod = owned(candidate, 'Pod', podName), spec = evidenceObject(pod.obj.spec), status = evidenceObject(pod.obj.status);
    const containers = evidenceList(spec.containers).map(evidenceObject), statuses = evidenceList(status.containerStatuses).map(evidenceObject);
    const owners = evidenceList(pod.metadata.ownerReferences).map(evidenceObject).filter(owner => owner.controller === true);
    requireEvidence(owners.length === 1 && owners[0].kind === 'Job' && owners[0].uid === job.metadata.uid &&
      status.phase === 'Succeeded' && containers.length === 1 && containers[0].name === 'register' &&
      containers[0].image === release.images.client && statuses.length === 1 && statuses[0].name === 'register' &&
      evidenceText(statuses[0].imageID).split(/[@/]/).at(-1) === release.images.client.split('@')[1] &&
      statuses[0].restartCount === 0 && evidenceObject(evidenceObject(statuses[0].state).terminated).exitCode === 0,
    'Original catalog registration lifetime is not the pinned successful client image');
    return { name: podName, uid: pod.metadata.uid, jobUid: job.metadata.uid, containerId: evidenceText(statuses[0].containerID) };
  };
  const catalog = await readRegistrationPod();
  const catalogLog = await run('logs', catalog.name, '-c', 'register');
  requireEvidence(canonical_json(catalog) === canonical_json(await readRegistrationPod()), 'Original catalog registration was replaced');
  const root = directCatalogRoot(catalogLog, origin, release, azure.c.account, azure.c.data);
  const { observedBaseline: baseline } = await captureDirectBaseline(azure, authority, origin.runId, root, baselineDirectory);
  const [router, maintenance, sql] = await Promise.all([
    get('daemonset', 'arrowcol-router').then(value => owned(value, 'DaemonSet', 'arrowcol-router')), workload('delta-maintenance'), workload('arrowcol-sql')]);
  const assertSql = async () => requireEvidence(canonical_json(sql) === canonical_json(await workload('arrowcol-sql')), 'Original SQL lifetime changed');
  await guard();
  const prepared = prepareDirectBarrier(context, origin, fic.server);
  const { capture: writerClaim, ...writer } = prepared.writer;
  requireEvidence(writer.image === release.images.writer && prepared.clients.every(client => client.image === release.images.client),
    'Original writer or clients differ from the approved release pins');
  const ready = pinWriterRegistration(origin, origin.cloudGeneration, writer,
    await readWriterRegistrations(authority, origin, origin.cloudGeneration, context, fic.server,
      { name: sql.podName, uid: sql.podUid }, assertSql));
  const nodePods = new Map<string, { name: string; uid: string; container: string }>();
  for (const c of prepared.clients) nodePods.set(c.nodeName, { name: c.name, uid: c.uid, container: 'generator' });
  nodePods.set(writer.nodeName, { name: writer.podName, uid: writer.podUid, container: 'dataflow' });
  const clocks = await Promise.all([...nodePods].map(async ([node, pod]) => {
    const before = Date.now();
    const clock = (await run('exec', pod.name, '-c', pod.container, '--', 'date', '-u', '+%s%N')).trim();
    return { node, podUid: pod.uid, ...directClockBound(before, Date.now(), clock) };
  }));
  const arm: DirectTerminalArm = {
    origin, baseline, startMs: prepared.measurementStartUnixMs, endMs: prepared.measurementEndUnixMs,
    clients: prepared.clients, writer, writerClaim, maintenance, sql, registration: ready,
    routerUid: evidenceText(router.metadata.uid), provenance: { release, catalog, catalogLog, clocks, schedule: prepared.patch },
  };
  const path = directArmRecordPath(origin);
  const raw = Buffer.from(canonical_json(arm));
  await guard();
  const [, stored] = await azure.blob('PUT', azure.c.checkpoints, path, undefined, raw,
    { 'If-None-Match': '*', 'x-ms-blob-type': 'BlockBlob' }, false, true);
  requireEvidence(stored[':status'] === '201' && stored.etag && /^"[^"\r\n]+"$/.test(stored.etag),
    'Original arm record write outcome UNKNOWN');
  await guard();
  const [read, headers] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, { 'If-Match': stored.etag }, false, true);
  requireEvidence(headers[':status'] === '200' && headers.etag === stored.etag && read?.equals(raw), 'Original arm record readback mismatch');
  const fresh = prepareDirectBarrier(context, origin, fic.server);
  requireEvidence(canonical_json(fresh.writer) === canonical_json(prepared.writer) &&
    canonical_json(fresh.clients) === canonical_json(prepared.clients) && fresh.patch[0].value === prepared.patch[0].value &&
    Date.now() <= prepared.runStartUnixMs - 30_000, 'Original lifetimes, pending schedule or release lead time changed');
  await checkPublisher();
  const response = evidenceObject(JSON.parse(await run('patch', 'configmap', 'direct-schedule-confirmwide', '--type=json',
    '--patch', JSON.stringify(prepared.patch), '-o', 'json')));
  const verify = (value: unknown) => {
    const config = owned(value, 'ConfigMap', 'direct-schedule-confirmwide'), data = evidenceObject(config.obj.data);
    requireEvidence(data.start === String(prepared.runStartUnixMs) && data.writerLifetime === prepared.patch[4].value &&
      data.clientLifetimes === prepared.patch[6].value, 'Original schedule release outcome UNKNOWN');
    return config.metadata.uid;
  };
  requireEvidence(verify(response) === verify(await get('configmap', 'direct-schedule-confirmwide')), 'Original schedule identity changed');
  return arm;
}
