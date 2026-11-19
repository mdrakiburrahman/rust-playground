import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from './bootstrap.js';
import type { RunCleanupAuthority } from './run_fic.js';
import type { ArmIdentity } from './benchmark-arm.js';
import { evidenceObject, evidenceText, requireEvidence } from './benchmark-evidence-json.js';
import { acceptRouterFencePatch, planRouterFence } from './benchmark-router-fence.js';

type Role = 'arrowcol-router' | 'arrowcol' | 'delta-maintenance';
export interface DirectStopOrigin extends ArmIdentity {
  cloudGeneration: string;
  fencingToken: number;
  sourceRevision: string;
}
type Json = Record<string, unknown>;
function sha(bytes: string | Buffer) { return createHash('sha256').update(bytes).digest('hex'); }
function etag(value?: string): value is string { return !!value && /^"[^"\r\n]+"$/.test(value); }

function workload(origin: ArmIdentity, role: Role, uid: string, value: unknown) {
  const body = evidenceObject(value), metadata = evidenceObject(body.metadata), spec = evidenceObject(body.spec);
  requireEvidence(body.apiVersion === 'apps/v1' && body.kind === (role === 'arrowcol-router' ? 'DaemonSet' : 'Deployment') &&
    metadata.uid === uid && metadata.name === role && metadata.namespace === origin.namespace && !metadata.deletionTimestamp,
  'Original stop workload identity changed');
  for (const field of ['annotations', 'labels']) {
    const markers = evidenceObject(metadata[field]);
    requireEvidence(markers['arrowcol-oat/run-id'] === origin.runId && markers['arrowcol-oat/owner-token'] === origin.owner,
      'Original stop workload ownership changed');
  }
  const version = evidenceText(metadata.resourceVersion), generation = metadata.generation;
  requireEvidence(version && typeof generation === 'number' && Number.isSafeInteger(generation) && generation > 0,
    'Stop workload has no positive resource version or generation');
  return { version, generation, spec };
}

/**
 * Persist intent and the original exact PATCH response. This is deliberately not terminal proof.
 * The transport must dispatch only the fixed workload and pass the server's response body unchanged.
 */
export async function persistDirectStopPatch(azure: Azure, authority: RunCleanupAuthority, origin: DirectStopOrigin,
  role: Role, uid: string, original: unknown, patch: (ops: Json[]) => Promise<string>) {
  const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
  requireEvidence([origin.runId, origin.namespaceUid, origin.owner, origin.cloudGeneration, uid].every(s => uuid.test(s)) &&
    /^[a-f0-9]{40}$/.test(origin.sourceRevision) && Number.isSafeInteger(origin.fencingToken) && origin.fencingToken > 0,
  'Direct stop origin is incomplete');
  const before = workload(origin, role, uid, original);
  const router = role === 'arrowcol-router';
  requireEvidence(router || before.spec.replicas === 1, 'Expected one original workload replica before drain');
  const ops = router ? planRouterFence(origin, uid, original) : [
    { op: 'test', path: '/metadata/uid', value: uid },
    { op: 'test', path: '/metadata/resourceVersion', value: before.version },
    { op: 'replace', path: '/spec/replicas', value: 0 },
  ];
  const path = `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/stop-${role}.json`;
  const save = async (value: Json, prior?: string) => {
    await authority.assertCurrent();
    const raw = canonical_json(value);
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, path, undefined, raw, {
      'x-ms-blob-type': 'BlockBlob', ...(prior ? { 'If-Match': prior } : { 'If-None-Match': '*' }),
    });
    requireEvidence(headers[':status'] === '201' && etag(headers.etag), 'Direct stop journal write outcome UNKNOWN');
    await authority.assertCurrent();
    const [bytes, readBack] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, { 'If-Match': headers.etag });
    requireEvidence(bytes?.equals(Buffer.from(raw)) && readBack.etag === headers.etag, 'Direct stop journal readback outcome UNKNOWN');
    return headers.etag;
  };
  const intent = { schema_version: 1, origin, role, uid, ops, observed_generation: before.generation, intent_at_ms: Date.now() };
  const initial = await save({ phase: 'intent', intent });
  await authority.assertCurrent();
  const raw = await patch(ops);
  requireEvidence(Buffer.byteLength(raw) <= 1_048_576, 'Direct stop PATCH response is oversized');
  const response = JSON.parse(raw) as unknown;
  const after = workload(origin, role, uid, response);
  requireEvidence(after.generation > before.generation && after.version !== before.version &&
    (router ? acceptRouterFencePatch(origin, uid, before.generation, response) === after.generation : after.spec.replicas === 0),
  'Direct stop PATCH lacks an exact positive response');
  const receipt = { phase: 'responded', intent, response: raw, response_sha256: sha(raw) };
  const version = await save(receipt, initial);
  return { receipt, version, path };
}
