import { execFile, spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { lstat, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { setTimeout as delay } from 'node:timers/promises';
import { create_namespace_deletion_journal } from './aks-namespace-custody.js';
import { observeWriterClaim, observeWriterExit, type ClientLifetime, type WriterLifetime } from './benchmark-arm.js';
import { auditEvidenceFiles, evidenceJson } from './benchmark-audit.js';
import { observeCompletedClients } from './benchmark-client-terminal.js';
import { captureFrozenDelta, type DirectDeltaBaseline } from './benchmark-delta-evidence.js';
import { persistDirectEvidence, type CapturedEvidence } from './benchmark-evidence-custody.js';
import { evidenceList, evidenceObject, evidenceText, requireEvidence } from './benchmark-evidence-json.js';
import { acceptRouterFencePatch, routerFenceObserved } from './benchmark-router-fence.js';
import type { persistDirectStopPatch, DirectStopOrigin } from './benchmark-stop-patch.js';
import { observeStoppedWriterRegistration, type WriterRegistration } from './benchmark-writer-sql.js';
import { clientLogExportPod, exportClientLogs, exportWriterLogs, writerLogExportPod } from './benchmark-writer-export.js';
import { canonical_json, type Azure } from './bootstrap.js';
import { FENCED, PublisherControl } from './publisher_control.js';
import { cleanup_run_fic, delete_owned_namespace, read_run_fic_record, type RunCleanupAuthority, type RunNamespaceIdentity } from './run_fic.js';

type Stop = Awaited<ReturnType<typeof persistDirectStopPatch>>;
type RouterEndpoints = Parameters<typeof routerFenceObserved>[5];
export type DirectTerminalArm = {
  origin: DirectStopOrigin;
  startMs: number;
  endMs: number;
  baseline: DirectDeltaBaseline;
  clients: ClientLifetime[];
  writer: WriterLifetime;
  writerClaim: ReturnType<typeof observeWriterClaim>;
  maintenance: WriterLifetime;
  registration: WriterRegistration;
  routerUid: string;
};
export type DirectTerminalObservations = {
  job: unknown;
  clients: unknown;
  claims: ReadonlyMap<string, unknown>;
  router: unknown;
  routerPods: unknown;
  routerEndpoints: RouterEndpoints;
  writer: unknown;
  writerClaim: unknown;
  writerDeployment: unknown;
  writerPods: unknown;
  maintenance: unknown;
  maintenanceDeployment: unknown;
  maintenancePods: unknown;
  sql: WriterRegistration[];
  stops: { router: Stop; writer: Stop; maintenance: Stop };
};
const sha = (data: string | Buffer) => createHash('sha256').update(data).digest('hex');
const exec = promisify(execFile);
type ExportPod = ReturnType<typeof writerLogExportPod>;
function exportArgs(context: string, server: string, expected: ExportPod) {
  requireEvidence(/^https:\/\/[a-z0-9.-]+$/.test(server), 'Invalid original AKS server');
  return ['--context', context, '--server', server, '--insecure-skip-tls-verify=false',
    '--request-timeout=15s', '-n', expected.metadata.namespace];
}
async function observeExactExportPod(context: string, server: string, expected: ExportPod, originalUid: string) {
  const { stdout } = await exec('kubectl', [...exportArgs(context, server, expected), 'get', 'pod', expected.metadata.name, '-o', 'json'],
  { maxBuffer: 1_048_576, timeout: 20_000 });
  const pod = evidenceObject(JSON.parse(stdout)), meta = evidenceObject(pod.metadata), spec = evidenceObject(pod.spec),
    status = evidenceObject(pod.status);
  const containers = evidenceList(spec.containers).map(evidenceObject);
  const env = expected.spec.containers[0];
  requireEvidence(pod.kind === 'Pod' && pod.apiVersion === 'v1' && meta.namespace === expected.metadata.namespace &&
    meta.name === expected.metadata.name && meta.uid === originalUid && !meta.deletionTimestamp &&
    spec.automountServiceAccountToken === false && !spec.initContainers && !spec.ephemeralContainers &&
    spec.restartPolicy === expected.spec.restartPolicy && containers.length === 1 && containers[0].name === env.name &&
    containers[0].image === env.image && canonical_json(containers[0].command) === canonical_json(env.command) &&
    canonical_json(containers[0].securityContext) === canonical_json(env.securityContext) &&
    canonical_json(containers[0].volumeMounts) === canonical_json(env.volumeMounts) &&
    canonical_json(spec.volumes) === canonical_json(expected.spec.volumes),
  'Read-only export helper identity or lifetime differs from its original claim');
  for (const [k, v] of Object.entries(expected.metadata.annotations))
    requireEvidence(evidenceObject(meta.annotations)[k] === v, 'Read-only export helper owner changed');
  if (status.phase === 'Pending') return;
  const statuses = evidenceList(status.containerStatuses).map(evidenceObject);
  requireEvidence(status.phase === 'Running' &&
    spec.nodeName === expected.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].values[0] &&
    statuses.length === 1 && statuses[0].restartCount === 0 && statuses[0].name === env.name &&
    evidenceObject(statuses[0].state).running && evidenceText(statuses[0].imageID).split(/[@/]/).at(-1) === env.image.split('@')[1],
  'Original export helper is not running its pinned image on the original node');
  return { podUid: originalUid, containerId: evidenceText(statuses[0].containerID) };
}
async function startExactExportPod(authority: RunCleanupAuthority, context: string, server: string, expected: ExportPod) {
  await authority.assertCurrent();
  const child = spawn('kubectl', [...exportArgs(context, server, expected), 'create', '-f', '-', '-o', 'json'],
    { timeout: 20_000, stdio: ['pipe', 'pipe', 'ignore'] });
  const complete = new Promise<void>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', code => code === 0 ? resolve() : reject(new Error('Export helper CREATE outcome is unknown')));
    child.stdin.once('error', reject);
  });
  child.stdin.end(JSON.stringify(expected));
  const body = async () => {
    let text = '';
    child.stdout.setEncoding('utf8');
    for await (const part of child.stdout) {
      text += part;
      if (text.length > 1_048_576) throw new Error('Export helper CREATE response is oversized');
    }
    return text;
  };
  let raw: string;
  try { [raw] = await Promise.all([body(), complete]); } catch (error) { child.kill(); throw error; }
  const response = evidenceObject(JSON.parse(raw)), meta = evidenceObject(response.metadata);
  const uid = evidenceText(meta.uid);
  requireEvidence(response.kind === 'Pod' && meta.namespace === expected.metadata.namespace &&
    meta.name === expected.metadata.name && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(uid),
  'Export helper CREATE lacks a positive original identity');
  const observe = async () => {
    await authority.assertCurrent();
    return observeExactExportPod(context, server, expected, uid);
  };
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    if (await observe()) return async () => {
      const current = await observe();
      requireEvidence(current, 'Original export helper ceased running');
      return current;
    };
    await delay(1000);
  }
  throw new Error('Original read-only export helper did not become ready');
}
function deploymentStopped(origin: DirectStopOrigin, stop: Stop, deployment: unknown, pods: unknown, originalPodUid: string) {
  const resolved = evidenceObject(JSON.parse(stop.receipt.response)), response = evidenceObject(resolved.metadata);
  const current = evidenceObject(deployment), meta = evidenceObject(current.metadata), status = evidenceObject(current.status);
  const items = evidenceObject(pods), podList = evidenceList(items.items).map(evidenceObject);
  requireEvidence(current.apiVersion === 'apps/v1' && current.kind === 'Deployment' && meta.namespace === origin.namespace &&
    meta.uid === stop.receipt.intent.uid && meta.name === stop.receipt.intent.role && !meta.deletionTimestamp &&
    response.uid === meta.uid && response.generation === meta.generation && meta.generation === status.observedGeneration &&
    evidenceObject(current.spec).replicas === 0 && evidenceObject(resolved.spec).replicas === 0 &&
    items.kind === 'PodList' && podList.length <= 1 && podList.every(pod => evidenceObject(pod.metadata).uid === originalPodUid),
  'Original stopped Deployment or Pod inventory changed');
  for (const field of ['annotations', 'labels']) {
    const markers = evidenceObject(meta[field]);
    requireEvidence(markers['arrowcol-oat/run-id'] === origin.runId && markers['arrowcol-oat/owner-token'] === origin.owner,
      'Original stopped Deployment owner changed');
  }
}
export function validateDirectTerminal(arm: DirectTerminalArm, t: DirectTerminalObservations) {
  for (const [stop, role, uid] of [
    [t.stops.router, 'arrowcol-router', arm.routerUid],
    [t.stops.writer, 'arrowcol', arm.writer.deploymentUid],
    [t.stops.maintenance, 'delta-maintenance', arm.maintenance.deploymentUid],
  ] as const) {
    const { intent, response, response_sha256 } = stop.receipt;
    requireEvidence(stop.receipt.phase === 'responded' && intent.schema_version === 1 &&
      canonical_json(intent.origin) === canonical_json(arm.origin) && intent.role === role && intent.uid === uid &&
      response_sha256 === sha(response) && intent.intent_at_ms >= arm.endMs && intent.intent_at_ms <= Date.now() &&
      stop.path === `_arrowcol-custody/direct/${arm.origin.runId}/${arm.origin.namespaceUid}/stop-${role}.json`,
    'Stop receipt origin, role, original response or drain time changed');
  }
  const router = t.stops.router.receipt, uid = router.intent.uid;
  requireEvidence(router.intent.intent_at_ms <= t.stops.writer.receipt.intent.intent_at_ms &&
    t.stops.writer.receipt.intent.intent_at_ms <= t.stops.maintenance.receipt.intent.intent_at_ms, 'Stop sequence is out of order');
  const generation = acceptRouterFencePatch(arm.origin, uid, router.intent.observed_generation, JSON.parse(router.response));
  requireEvidence(routerFenceObserved(arm.origin, uid, generation, t.router, t.routerPods, t.routerEndpoints), 'Original router still admits traffic');
  const clients = observeCompletedClients(arm.origin, arm.clients, t.job, t.clients, t.claims, arm.endMs);
  const writer = observeWriterExit(arm.origin, arm.writer, t.writer, t.stops.writer.receipt.intent.intent_at_ms);
  const maintenance = observeWriterExit(arm.origin, arm.maintenance, t.maintenance, t.stops.maintenance.receipt.intent.intent_at_ms, 'maintenance');
  deploymentStopped(arm.origin, t.stops.writer, t.writerDeployment, t.writerPods, writer.podUid);
  deploymentStopped(arm.origin, t.stops.maintenance, t.maintenanceDeployment, t.maintenancePods, maintenance.podUid);
  const sql = observeStoppedWriterRegistration(arm.origin, arm.origin.cloudGeneration, writer, arm.registration, t.sql);
  return { clients, writer, maintenance, sql };
}

/**
 * Finish a loaded direct run, not an OAT no-load handoff. The source-owned
 * Kubernetes adapter supplies raw observations and captured bytes, never PASS flags.
 */
export async function finishDirectTerminalCustody(azure: Azure, authority: RunCleanupAuthority, arm: DirectTerminalArm,
  observeTerminal: () => Promise<DirectTerminalObservations>, context: string, directory: string,
  deltaDirectory: string, ficRecordPath: string) {
  const { origin } = arm;
  const dir = await lstat(directory);
  requireEvidence(dir.isDirectory() && (dir.mode & 0o077) === 0, 'Terminal custody directory is not private');
  requireEvidence(Number.isSafeInteger(arm.startMs) && Number.isSafeInteger(arm.endMs) &&
    arm.startMs > 0 && arm.endMs > arm.startMs && arm.endMs <= Date.now(), 'Direct measurement clock bounds are invalid');
  const fic = read_run_fic_record(ficRecordPath, azure.c);
  requireEvidence(fic.runId === origin.runId && fic.namespace === origin.namespace && fic.namespaceUid === origin.namespaceUid &&
    fic.namespaceOwner === origin.owner && fic.phase === 'ready', 'Run FIC does not identify this original namespace');
  const identity: RunNamespaceIdentity = { version: 1, runId: fic.runId, namespace: fic.namespace, namespaceUid: fic.namespaceUid,
    namespaceOwner: fic.namespaceOwner, aksId: fic.aksId, server: fic.server };
  const armRecordPath = `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/original-arm.json`;
  const armBytes = Buffer.from(canonical_json(arm));
  const fresh = async () => {
    await authority.assertCurrent();
    const [durableArm, armHeaders] = await azure.blob('GET', azure.c.checkpoints, armRecordPath, undefined, undefined, {}, false, true);
    requireEvidence(armHeaders[':status'] === '200' && armHeaders.etag && !armHeaders.etag.startsWith('W/') &&
      durableArm?.equals(armBytes), 'Original off-node arm record is missing or changed');
    const t = await observeTerminal();
    const proved = validateDirectTerminal(arm, t);
    for (const receipt of Object.values(t.stops)) {
      const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, receipt.path, undefined, undefined, { 'If-Match': receipt.version });
      requireEvidence(headers.etag === receipt.version && bytes?.equals(Buffer.from(canonical_json(receipt.receipt))),
        'Original durable positive PATCH response is unavailable');
    }
    const [metadata, registry] = await new PublisherControl(azure).read();
    requireEvidence(metadata.resetgeneration === origin.cloudGeneration && metadata.resetstate === FENCED &&
      Object.keys(registry).length === 0, 'Original generation is not fenced and free of active or uncertain publishers');
    return { t, proved };
  };
  const proof = await fresh();
  const capturedLogs: CapturedEvidence[] = [];
  const acceptExport = (owner: string, files: Awaited<ReturnType<typeof exportWriterLogs>>['files']) => {
    for (const file of files) capturedLogs.push({ key: `${owner}-${file.stream}.raw`, path: file.local,
      bytes: file.bytes, sha256: file.sha256 });
  };
  const helper = writerLogExportPod(origin, proof.proved.writer, arm.writerClaim, proof.t.writerClaim);
  const readyWriter = await startExactExportPod(authority, context, fic.server, helper);
  const exported = await exportWriterLogs(authority, origin, fic.server, context, directory, readyWriter);
  acceptExport('writer', exported.files);
  for (const client of proof.proved.clients) {
    const helper = clientLogExportPod(origin, client);
    const readyClient = await startExactExportPod(authority, context, fic.server, helper);
    const exported = await exportClientLogs(authority, origin, fic.server, context, directory, client.index, readyClient);
    acceptExport(`client-${client.index}`, exported.files);
  }
  const byKey = new Map(capturedLogs.map(item => [item.key, item]));
  const expected = ['writer-stdout.raw', 'writer-stderr.raw',
    ...proof.proved.clients.flatMap(client => [`client-${client.index}-stdout.raw`, `client-${client.index}-stderr.raw`])];
  requireEvidence(byKey.size === expected.length && capturedLogs.length === expected.length &&
    expected.every(key => byKey.has(key)) && new Set(capturedLogs.map(log => log.path)).size === expected.length,
  'Raw log custody has missing, duplicated or foreign streams');
  const delta = await captureFrozenDelta(azure, authority, origin.runId, arm.baseline.table_root, arm.baseline, deltaDirectory);
  const report = await auditEvidenceFiles(proof.proved.clients.map(client => byKey.get(`client-${client.index}-stdout.raw`)!.path),
    byKey.get('writer-stderr.raw')!.path, deltaDirectory, BigInt(arm.baseline.version), { expectedFinalVersion: BigInt(delta.snapshot.version) });
  requireEvidence(report.measurement_start_unix_ms === BigInt(arm.startMs) && report.measurement_end_unix_ms === BigInt(arm.endMs) &&
    report.clients === origin.clients && [...delta.evidence,
      ...capturedLogs.filter(file => file.key === 'writer-stderr.raw' || /^client-\d+-stdout\.raw$/.test(file.key))]
      .every(input => report.evidence_sha256[input.path] === input.sha256),
  'Audited bytes or measurement bounds differ from the original capture');
  const record = Buffer.from(evidenceJson({ arm, proof: proof.proved, raw: { stops: proof.t.stops,
    writerDeployment: proof.t.writerDeployment, writerPods: proof.t.writerPods, writerClaim: proof.t.writerClaim,
    maintenanceDeployment: proof.t.maintenanceDeployment, maintenancePods: proof.t.maintenancePods, sql: proof.t.sql,
    clientClaims: [...proof.t.claims], writer: proof.t.writer,
    maintenance: proof.t.maintenance, clients: proof.t.clients, job: proof.t.job, router: proof.t.router,
    routerPods: proof.t.routerPods, routerEndpoints: proof.t.routerEndpoints },
    delta: { snapshot: delta.snapshot, physical: delta.physical }, report }));
  const path = join(directory, 'direct-terminal.json');
  await writeFile(path, record, { flag: 'wx', mode: 0o600, flush: true });
  const seal = await persistDirectEvidence(azure, authority, origin, [...capturedLogs, ...delta.evidence,
    { key: 'direct-terminal.json', path, bytes: record.length, sha256: sha(record) }]);
  // Repeat the independent scope/authority checks after the potentially long upload.
  await fresh();
  const [stored, headers] = await azure.blob('GET', azure.c.checkpoints, seal.path, undefined, undefined, { 'If-Match': seal.etag });
  requireEvidence(headers.etag === seal.etag && stored && sha(stored) === seal.sha256, 'Terminal evidence seal changed');
  const journal = create_namespace_deletion_journal(azure, identity, authority);
  const receipt = await delete_owned_namespace(azure, identity, join(directory, 'namespace-stopped.json'), authority, journal);
  await journal.complete(receipt);
  await journal.stopped();
  await cleanup_run_fic(azure, origin.runId, ficRecordPath, authority);
  return { seal, receipt, qualification: report.qualification };
}
