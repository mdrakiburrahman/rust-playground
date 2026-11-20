import { createHash } from 'node:crypto';
import { canonical_json, run_tree, type Azure } from './bootstrap.js';
import { dispatchExternalEffect, isEffectBoundaryActive } from './effect-boundary.js';
import type { RunCleanupAuthority } from './run_fic.js';
import type { ArmIdentity } from './benchmark-arm.js';
import { evidenceList, evidenceObject, evidenceText, requireEvidence } from './benchmark-evidence-json.js';
import { acceptRouterFencePatch, planRouterFence, routerStopPatch } from './benchmark-router-fence.js';

type Role = 'arrowcol-router' | 'arrowcol' | 'delta-maintenance';
export interface DirectStopOrigin extends ArmIdentity {
  cloudGeneration: string;
  fencingToken: number;
  sourceRevision: string;
}
type Json = Record<string, unknown>;
function sha(bytes: string | Buffer) { return createHash('sha256').update(bytes).digest('hex'); }
function etag(value?: string): value is string { return !!value && /^"[^"\r\n]+"$/.test(value); }
const stopPath = (origin: DirectStopOrigin, role: Role) =>
  `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}/stop-${role}.json`;
type PositiveStop = Awaited<ReturnType<typeof persistDirectStopPatch>>;

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

export async function readDirectStopPatch(azure: Azure, authority: RunCleanupAuthority, origin: DirectStopOrigin,
  role: Role, uid: string): Promise<PositiveStop | null> {
  const path = stopPath(origin, role);
  await authority.assertCurrent();
  const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, {}, true);
  if (!bytes) return null;
  requireEvidence(headers[':status'] === '200' && etag(headers.etag) && bytes.length <= 1_100_000,
    'Direct stop journal has no bounded strong version');
  const saved = evidenceObject(JSON.parse(bytes.toString('utf8')));
  requireEvidence(saved.phase === 'responded', 'Original direct stop outcome UNKNOWN; no PATCH replay');
  const raw = evidenceText(saved.response), intent = evidenceObject(saved.intent);
  const response = JSON.parse(raw);
  const after = workload(origin, role, uid, response);
  const ops = evidenceList(intent.ops).map(evidenceObject);
  const observed = intent.observed_generation, started = intent.intent_at_ms;
  const oldVersion = evidenceText(ops[1]?.value);
  requireEvidence(intent.schema_version === 1 && canonical_json(intent.origin) === canonical_json(origin) &&
    intent.role === role && intent.uid === uid && saved.response_sha256 === sha(raw) &&
    Number.isSafeInteger(observed) && typeof observed === 'number' && observed > 0 && after.generation > observed &&
    Number.isSafeInteger(started) && typeof started === 'number' && started > 0 && started <= Date.now() &&
    oldVersion && oldVersion !== after.version &&
    (role === 'arrowcol-router' ? acceptRouterFencePatch(origin, uid, observed, response) === after.generation :
      after.spec.replicas === 0),
  'Direct stop journal does not prove a positive response for the original workload');
  const expectedOps = role === 'arrowcol-router' ? routerStopPatch(uid, oldVersion) : replicaStop(uid, oldVersion);
  requireEvidence(canonical_json(ops) === canonical_json(expectedOps), 'Original direct stop journal PATCH body changed');
  return { path, version: headers.etag, receipt: { phase: 'responded', response: raw, response_sha256: sha(raw),
    intent: { schema_version: 1, origin, role, uid, ops, observed_generation: observed, intent_at_ms: started } } };
}

const replicaStop = (uid: string, version: string): Json[] => [
  { op: 'test', path: '/metadata/uid', value: uid },
  { op: 'test', path: '/metadata/resourceVersion', value: version },
  { op: 'replace', path: '/spec/replicas', value: 0 },
];

/**
 * The allocation names a fixed Kubernetes workload and exact argv. The only
 * late-bound data are the original UID and the freshly read resourceVersion,
 * both tested by the server before applying the fixed stop operation.
 */
export function directConditionalStopArgv(context: string, server: string, origin: DirectStopOrigin, role: Role) {
  return ['--context', context, '--server', server, '--insecure-skip-tls-verify=false',
    '--request-timeout=15s', '-n', origin.namespace, 'patch',
    role === 'arrowcol-router' ? 'daemonset' : 'deployment', role,
    '--type=json', '--patch-file=/dev/stdin', '-o', 'json'];
}

export async function stopOriginalWorkload(azure: Azure, authority: RunCleanupAuthority, context: string, server: string,
  origin: DirectStopOrigin, role: Role, uid: string, original: unknown) {
  requireEvidence(isEffectBoundaryActive(), 'Conditional direct stop requires the selected effect boundary');
  const prior = await readDirectStopPatch(azure, authority, origin, role, uid);
  if (prior) return prior;
  const before = workload(origin, role, uid, original);
  requireEvidence(/^[0-9]+$/.test(before.version) && /^https:\/\/[a-z0-9.-]+$/.test(server),
    'Original conditional stop requires the exact TLS server and a Kubernetes resourceVersion');
  const argv = directConditionalStopArgv(context, server, origin, role);
  const expected = role === 'arrowcol-router' ? routerStopPatch(uid, before.version) : replicaStop(uid, before.version);
  return persistDirectStopPatch(azure, authority, origin, role, uid, original, async ops => {
    requireEvidence(canonical_json(ops) === canonical_json(expected), 'Conditional stop has non-allowlisted operations');
    const body = Buffer.from(canonical_json(ops));
    const result = await dispatchExternalEffect({ transport: 'process', method: 'KUBECTL_DIRECT_STOP',
      resource: 'kubectl', path: JSON.stringify({ argv, uid }), body }, async () => {
      await authority.assertCurrent();
      return run_tree(['kubectl', ...argv], 20, undefined, undefined, undefined, 1_048_576, body);
    });
    requireEvidence(result.returncode === 0, 'Conditional stop outcome UNKNOWN; no PATCH replay');
    return result.stdout;
  });
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
  const ops = router ? planRouterFence(origin, uid, original) : replicaStop(uid, before.version);
  const path = stopPath(origin, role);
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
