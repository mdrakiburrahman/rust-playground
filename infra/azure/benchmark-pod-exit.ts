import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import { observeWriterExit, type WriterLifetime } from './benchmark-arm.js';
import { evidenceList, evidenceObject, evidenceText, requireEvidence } from './benchmark-evidence-json.js';
import { persistDirectStopPatch, readDirectStopPatch, stopOriginalWorkload, type DirectStopOrigin } from './benchmark-stop-patch.js';
import { dispatchExternalEffect, effectSignal, processEffectRequest } from './effect-boundary.js';
import { canonical_json, type Azure } from './bootstrap.js';
import type { RunCleanupAuthority } from './run_fic.js';

type Role = 'arrowcol' | 'delta-maintenance';
const exec = promisify(execFile);
const MAX_EVENT_BYTES = 1_048_576;
const dns = /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/;
const strong = /^"[^"\r\n]+"$/;
const journalPath = (origin: DirectStopOrigin, role: Role) =>
  `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/exit-${role}.json`;
const container = (role: Role) => role === 'arrowcol' ? 'dataflow' : 'maintenance';

export async function positiveDirectExitEvent(lines: AsyncIterable<string>, origin: DirectStopOrigin, role: Role,
  original: WriterLifetime, startedMs: number) {
  for await (const line of lines) {
    const event = evidenceObject(JSON.parse(line));
    requireEvidence(event.type === 'MODIFIED', 'Original Pod watch did not provide a positive status event');
    const pod = evidenceObject(event.object), meta = evidenceObject(pod.metadata);
    requireEvidence(meta.uid === original.podUid && meta.name === original.podName && meta.namespace === origin.namespace,
      'Original Pod watch identity changed');
    const statuses = evidenceList(evidenceObject(pod.status).containerStatuses);
    requireEvidence(statuses.length === 1, 'Original Pod watch container coverage changed');
    const status = evidenceObject(statuses[0]), state = evidenceObject(status.state);
    requireEvidence(status.containerID === original.containerId && status.imageID === original.imageId && status.restartCount === 0,
      'Original Pod watch lifetime changed');
    if (state.terminated) {
      observeWriterExit(origin, original, pod, startedMs, container(role));
      return pod;
    }
    requireEvidence(evidenceObject(state.running).startedAt === original.startedAt, 'Original Pod watch process state changed');
  }
  throw new Error('Original Pod watch ended without positive terminal evidence');
}

async function* eventLines(chunks: AsyncIterable<Buffer>) {
  let pending = Buffer.alloc(0);
  for await (const chunk of chunks) {
    pending = Buffer.concat([pending, chunk]);
    let split: number;
    while ((split = pending.indexOf(10)) >= 0) {
      requireEvidence(split > 0 && split < MAX_EVENT_BYTES, 'Original Pod watch event exceeded its bound');
      yield new TextDecoder('utf-8', { fatal: true }).decode(pending.subarray(0, split));
      pending = pending.subarray(split + 1);
    }
    requireEvidence(pending.length < MAX_EVENT_BYTES, 'Original Pod watch event exceeded its bound');
  }
  requireEvidence(!pending.length, 'Original Pod watch ended mid-event');
}

/** Replays from this Pod's fresh pre-PATCH resourceVersion, not a Deployment's version or a later GET. */
export async function stopDirectPodWithExit(azure: Azure, authority: RunCleanupAuthority, context: string, server: string,
  origin: DirectStopOrigin, role: Role, original: WriterLifetime, recovery = false) {
  if (recovery) {
    const prior = await readDirectStopPatch(azure, authority, origin, role, original.deploymentUid);
    if (prior) return prior; // Caller must still require the positive original exit journal.
  }
  requireEvidence(/^https:\/\/[a-z0-9.-]+$/.test(server) && dns.test(origin.namespace) && dns.test(original.podName),
    'Original Pod watch target is invalid');
  const kubectl = ['--context', context, '--server', server, '--insecure-skip-tls-verify=false'];
  const run = async (args: string[]) => {
    await authority.assertCurrent();
    const command = ['kubectl', ...kubectl, '--request-timeout=15s', '-n', origin.namespace, ...args];
    const forward = () => exec('kubectl', command.slice(1),
      { timeout: 20_000, maxBuffer: MAX_EVENT_BYTES, signal: effectSignal() });
    const { stdout } = recovery ? await dispatchExternalEffect(processEffectRequest(command), forward) : await forward();
    return stdout;
  };
  const deployment = JSON.parse(await run(['get', 'deployment', role, '-o', 'json']));
  const pod = evidenceObject(JSON.parse(await run(['get', 'pod', original.podName, '-o', 'json'])));
  const meta = evidenceObject(pod.metadata), status = evidenceObject(evidenceList(evidenceObject(pod.status).containerStatuses)[0]);
  const rv = evidenceText(meta.resourceVersion);
  const grace = evidenceObject(pod.spec).terminationGracePeriodSeconds;
  requireEvidence(typeof grace === 'number' && Number.isSafeInteger(grace) &&
    grace >= 1 && grace <= (role === 'arrowcol' ? 360 : 4800), 'Original Pod has an unsupported termination grace period');
  requireEvidence(meta.uid === original.podUid && meta.name === original.podName && meta.namespace === origin.namespace &&
    !meta.deletionTimestamp && /^[0-9]+$/.test(rv) && status.containerID === original.containerId &&
    status.imageID === original.imageId && status.restartCount === 0 &&
    evidenceObject(evidenceObject(status.state).running).startedAt === original.startedAt,
  'Original Pod must still be running before the stop intent');
  const stop = recovery ? await stopOriginalWorkload(azure, authority, context, server, origin, role, original.deploymentUid, deployment) :
    await persistDirectStopPatch(azure, authority, origin, role, original.deploymentUid, deployment,
    ops => run(['patch', 'deployment', role, '--type=json', '--patch', JSON.stringify(ops), '-o', 'json']));
  const watchSeconds = grace + 60;
  const query = new URLSearchParams({ watch: 'true', resourceVersion: rv, timeoutSeconds: String(watchSeconds),
    fieldSelector: `metadata.name=${original.podName}` });
  await authority.assertCurrent();
  const command = ['kubectl', ...kubectl, `--request-timeout=${watchSeconds + 10}s`, 'get', '--raw',
    `/api/v1/namespaces/${origin.namespace}/pods?${query}`];
  const watch = async () => {
    const child = spawn('kubectl', command.slice(1), { timeout: (watchSeconds + 15) * 1000,
      signal: effectSignal(), stdio: ['ignore', 'pipe', 'ignore'] });
    const done = new Promise<never>((_, reject) => child.once('error', reject));
    try {
      return await Promise.race([done,
      positiveDirectExitEvent(eventLines(child.stdout), origin, role, original, stop.receipt.intent.intent_at_ms)]);
    } finally { child.kill(); }
  };
  const observed = recovery ? await dispatchExternalEffect({ ...processEffectRequest(command),
    method: 'KUBECTL_DIRECT_EXIT_WATCH', path: JSON.stringify({ context, server, namespace: origin.namespace, pod: original.podName,
      uid: original.podUid }) }, watch) : await watch();
  const bytes = Buffer.from(canonical_json({ origin, role, podResourceVersionBeforeStop: rv, stop, pod: observed }));
  const path = journalPath(origin, role);
  await authority.assertCurrent();
  const [, stored] = await azure.blob('PUT', azure.c.checkpoints, path, undefined, bytes,
    { 'If-None-Match': '*', 'x-ms-blob-type': 'BlockBlob' }, false, true);
  requireEvidence(stored[':status'] === '201' && stored.etag && strong.test(stored.etag),
    'Original terminal Pod evidence write outcome UNKNOWN');
  const read = await readDirectPodExit(azure, authority, origin, role, original, stop);
  requireEvidence(read.etag === stored.etag && read.bytes.equals(bytes), 'Original terminal Pod evidence readback mismatch');
  return stop;
}

export async function readDirectPodExit(azure: Azure, authority: RunCleanupAuthority, origin: DirectStopOrigin, role: Role,
  original: WriterLifetime, stop: Awaited<ReturnType<typeof persistDirectStopPatch>>) {
  await authority.assertCurrent();
  const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, journalPath(origin, role), undefined, undefined, {}, false, true);
  requireEvidence(headers[':status'] === '200' && headers.etag && strong.test(headers.etag) && bytes && bytes.length < MAX_EVENT_BYTES * 3,
    'Original durable terminal Pod evidence is unavailable');
  const record = evidenceObject(JSON.parse(bytes.toString('utf8')));
  requireEvidence(record.role === role && /^[0-9]+$/.test(evidenceText(record.podResourceVersionBeforeStop)) &&
    canonical_json(record.origin) === canonical_json(origin) && canonical_json(record.stop) === canonical_json(stop),
    'Original terminal Pod evidence belongs to another stop');
  observeWriterExit(origin, original, record.pod, stop.receipt.intent.intent_at_ms, container(role));
  return { pod: record.pod, etag: headers.etag, bytes };
}
