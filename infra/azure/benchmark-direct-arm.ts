import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { observeWriter, prepareDirectBarrier } from './benchmark-arm.js';
import { directClockBound } from './benchmark-clock.js';
import { captureDirectBaseline } from './benchmark-delta-evidence.js';
import { evidenceList, evidenceObject, evidenceText, parseEvidence, requireEvidence } from './benchmark-evidence-json.js';
import { directProducerAuthority, type DirectCampaignScope } from './benchmark-run-authority.js';
import { observeRouterLifetimes } from './benchmark-router-fence.js';
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
const streamWindow = 1_048_576n, connectionWindow = 16_777_216n;
export type DirectRelease = {
  revision: string;
  schemaFingerprint: string;
  images: Record<'writer' | 'client' | 'maintenance' | 'sql' | 'router' | 'catalog', string>;
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

function effectiveConfig(log: string, marker: string) {
  const lines = log.split(/\r?\n/).filter(line => line.startsWith(marker));
  requireEvidence(lines.length === 1, `Missing or duplicate ${marker.trim()} readback`);
  return { raw: lines[0], config: evidenceObject(parseEvidence(lines[0].slice(marker.length))) };
}

export function directClientTransport(log: string) {
  const { config, raw } = effectiveConfig(log, 'OAT_CLIENT_EFFECTIVE_CONFIG ');
  requireEvidence(config.stream_window_bytes === streamWindow && config.connection_window_bytes === connectionWindow &&
    config.adaptive_window === false && config.tcp_nodelay === true, 'Client effective transport differs from the approved settings');
  return { raw, stream_window_bytes: Number(config.stream_window_bytes), connection_window_bytes: Number(config.connection_window_bytes),
    adaptive_window: config.adaptive_window, tcp_nodelay: config.tcp_nodelay };
}

export function directRouterTransport(log: string) {
  const { config, raw } = effectiveConfig(log, 'arrowcol_router_effective_config ');
  const downstream = evidenceObject(config.downstream_h2), upstream = evidenceObject(config.upstream);
  requireEvidence([downstream, upstream].every(part => part.stream_window_bytes === streamWindow &&
    part.connection_window_bytes === connectionWindow), 'Router effective transport differs from the approved settings');
  return { raw, downstream: { stream_window_bytes: Number(downstream.stream_window_bytes), connection_window_bytes: Number(downstream.connection_window_bytes) },
    upstream: { stream_window_bytes: Number(upstream.stream_window_bytes), connection_window_bytes: Number(upstream.connection_window_bytes) } };
}

export function directReceiverTransport(log: string) {
  const marker = 'arrowcol_effective_config ';
  const entries = log.split(/\r?\n/).filter(line => line.startsWith(marker))
    .map(raw => ({ raw, event: evidenceObject(parseEvidence(raw.slice(marker.length))) }))
    .filter(({ event }) => event.component === 'receiver');
  requireEvidence(entries.length === 1, 'Missing or duplicate receiver effective transport readback');
  const { raw, event } = entries[0];
  const transport = evidenceObject(evidenceObject(event.config).transport);
  requireEvidence(transport.initial_stream_window_size === streamWindow &&
    transport.initial_connection_window_size === connectionWindow && transport.adaptive_window === false &&
    transport.tcp_nodelay === true, 'Receiver effective transport differs from the approved settings');
  return { raw, stream_window_bytes: Number(transport.initial_stream_window_size),
    connection_window_bytes: Number(transport.initial_connection_window_size), adaptive_window: transport.adaptive_window,
    tcp_nodelay: transport.tcp_nodelay };
}

export function directParquetConfig(log: string) {
  const marker = 'arrowcol_effective_config ';
  const entries = log.split(/\r?\n/).filter(line => line.startsWith(marker))
    .map(raw => ({ raw, event: evidenceObject(parseEvidence(raw.slice(marker.length))) }))
    .filter(({ event }) => event.parquet_writer !== undefined);
  requireEvidence(entries.length === 1, 'Missing or duplicate Parquet startup policy readback');
  const { raw, event } = entries[0];
  const defaults = evidenceObject(evidenceObject(event.parquet_writer).defaults);
  requireEvidence(defaults.compression === 'zstd' && defaults.compression_level === 2n,
    'Parquet startup policy differs from the approved settings');
  return { raw, compression: defaults.compression, compression_level: Number(defaults.compression_level) };
}

/** Call only with the approved immutable release pins and an existing exclusive run authority. */
export async function armDirectBenchmark(azure: Azure, campaign: DirectCampaignScope, origin: DirectStopOrigin,
  release: DirectRelease, context: string, ficRecordPath: string, baselineDirectory: string,
  assertInventory?: (arm: DirectTerminalArm) => Promise<void>): Promise<DirectTerminalArm> {
  const authority = directProducerAuthority(campaign, origin);
  requireEvidence([origin.runId, origin.namespaceUid, origin.owner, origin.cloudGeneration].every(id => uuid.test(id)) &&
    Number.isSafeInteger(origin.fencingToken) && origin.fencingToken > 0 && /^[a-z0-9-]+$/.test(origin.namespace) &&
    Number.isInteger(origin.clients) && origin.clients >= 1 && origin.clients <= 8 && /^[a-f0-9]{40}$/.test(release.revision) &&
    release.revision === origin.sourceRevision && release.schemaFingerprint.length > 0 && release.schemaFingerprint.length < 129 &&
    (['writer', 'client', 'maintenance', 'sql', 'router', 'catalog'] as const).every(role =>
      typeof release.images[role] === 'string' && digest.test(release.images[role])), 'Direct arm lacks original authority, source or release pins');
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
  const workload = async (role: 'arrowcol-sql' | 'delta-maintenance' | 'arrowcol-catalog') => {
    const [deployment, sets, pods] = await Promise.all([get('deployment', role), get('replicasets', '-l', `app=${role}`),
      get('pods', '-l', `app=${role}`)]);
    const result = observeWriter(origin, deployment, sets, pods, role);
    const key = role === 'arrowcol-sql' ? 'sql' : role === 'arrowcol-catalog' ? 'catalog' : 'maintenance';
    requireEvidence(result.image === release.images[key],
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
  const [router, routerPods, maintenance, sql, catalogLifetime] = await Promise.all([
    get('daemonset', 'arrowcol-router').then(value => owned(value, 'DaemonSet', 'arrowcol-router')),
    get('pods', '-l', 'app=arrowcol-router'), workload('delta-maintenance'), workload('arrowcol-sql'), workload('arrowcol-catalog')]);
  const routerUid = evidenceText(router.metadata.uid);
  const routers = observeRouterLifetimes(origin, routerUid, release.images.router, router.obj, routerPods);
  const assertSql = async () => requireEvidence(canonical_json(sql) === canonical_json(await workload('arrowcol-sql')), 'Original SQL lifetime changed');
  await guard();
  const prepared = prepareDirectBarrier(context, origin, fic.server);
  const { capture: writerClaim, ...writer } = prepared.writer;
  requireEvidence(writer.image === release.images.writer && prepared.clients.every(client => client.image === release.images.client),
    'Original writer or clients differ from the approved release pins');
  requireEvidence(prepared.clients.every(client => routers.some(route => route.nodeName === client.nodeName)),
    'Original client node has no pinned local router');
  const [clientTransport, routerTransport, writerConfig] = await Promise.all([
    Promise.all(prepared.clients.map(async client => ({ podUid: client.uid, config: directClientTransport(
      await run('exec', client.name, '-c', 'generator', '--', '/bin/cat', `/evidence/${client.uid}/stdout`)) }))),
    Promise.all(routers.map(async router => ({ podUid: router.uid, config: directRouterTransport(
      await run('logs', router.name, '-c', 'router')) }))),
    run('exec', writer.podName, '-c', 'dataflow', '--', '/bin/cat',
      `/var/lib/arrowcol/staging/benchmark-logs/${writer.podUid}/stderr`),
  ]);
  const receiverTransport = directReceiverTransport(writerConfig), parquetConfig = directParquetConfig(writerConfig);
  const ready = pinWriterRegistration(origin, origin.cloudGeneration, writer,
    await readWriterRegistrations(authority, origin, origin.cloudGeneration, context, fic.server,
      { name: sql.podName, uid: sql.podUid }, assertSql));
  const nodePods = new Map<string, { name: string; uid: string; container: string }>();
  for (const c of prepared.clients) nodePods.set(c.nodeName, { name: c.name, uid: c.uid, container: 'generator' });
  nodePods.set(writer.nodeName, { name: writer.podName, uid: writer.podUid, container: 'dataflow' });
  const clocks = await Promise.all([...nodePods].map(async ([node, pod]) => {
    const timeNamespace = (await run('exec', pod.name, '-c', pod.container, '--', 'readlink', '/proc/self/ns/time')).trim();
    requireEvidence(/^time:\[\d+\]$/.test(timeNamespace), 'Original clock observer time namespace is unavailable');
    const before = Date.now();
    const clock = (await run('exec', pod.name, '-c', pod.container, '--', 'date', '-u', '+%s%N')).trim();
    return { node, podUid: pod.uid, timeNamespace, ...directClockBound(before, Date.now(), clock) };
  }));
  await Promise.all(prepared.clients.map(async client => {
    const timeNamespace = (await run('exec', client.name, '-c', 'generator', '--', 'readlink', '/proc/self/ns/time')).trim();
    requireEvidence(timeNamespace === clocks.find(clock => clock.node === client.nodeName)?.timeNamespace,
      'Original clients on a node do not share the sampled time namespace');
  }));
  const arm: DirectTerminalArm = {
    origin, baseline, startMs: prepared.measurementStartUnixMs, endMs: prepared.measurementEndUnixMs,
    clients: prepared.clients, writer, writerClaim, maintenance, sql, catalogLifetime, routers, registration: ready,
    routerUid, provenance: { release, catalog, catalogLog, clocks, schedule: prepared.patch,
      transport: { clients: clientTransport, routers: routerTransport, receiver: { podUid: writer.podUid, config: receiverTransport } },
      parquet: { podUid: writer.podUid, config: parquetConfig } },
  };
  const path = directArmRecordPath(origin);
  const raw = Buffer.from(canonical_json(arm));
  await guard();
  await assertInventory?.(arm);
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
    canonical_json(await workload('delta-maintenance')) === canonical_json(maintenance) &&
    canonical_json(await workload('arrowcol-sql')) === canonical_json(sql) &&
    canonical_json(await workload('arrowcol-catalog')) === canonical_json(catalogLifetime) &&
    canonical_json(observeRouterLifetimes(origin, routerUid, release.images.router,
      await get('daemonset', 'arrowcol-router'), await get('pods', '-l', 'app=arrowcol-router'))) === canonical_json(routers) &&
    Date.now() <= prepared.runStartUnixMs - 30_000, 'Original lifetimes, pending schedule or release lead time changed');
  await assertInventory?.(arm);
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
