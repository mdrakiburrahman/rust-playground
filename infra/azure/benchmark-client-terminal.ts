import type { ArmIdentity, ClientLifetime } from './benchmark-arm.js';
import { evidenceList, evidenceObject, evidenceText, requireEvidence } from './benchmark-evidence-json.js';

function controller(value: unknown, kind: string, uid: string) {
  const refs = evidenceList(value).map(evidenceObject).filter(ref => ref.controller === true);
  return refs.length === 1 && refs[0].kind === kind && refs[0].uid === uid;
}

function owned(value: unknown, run: ArmIdentity) {
  const item = evidenceObject(value), metadata = evidenceObject(item.metadata);
  requireEvidence(metadata.namespace === run.namespace && !metadata.deletionTimestamp, 'Run namespace mismatch or object is deleting');
  for (const field of ['annotations', 'labels']) {
    const markers = evidenceObject(metadata[field]);
    requireEvidence(markers['arrowcol-oat/run-id'] === run.runId && markers['arrowcol-oat/owner-token'] === run.owner,
      'Original client ownership changed');
  }
  return { item, metadata };
}

export function observeCompletedClients(run: ArmIdentity, originals: readonly ClientLifetime[], job: unknown,
  pods: unknown, claims: ReadonlyMap<string, unknown>, measurementEndMs: number) {
  const observed = owned(job, run), status = evidenceObject(observed.item.status), spec = evidenceObject(observed.item.spec);
  requireEvidence(originals.length === run.clients && new Set(originals.map(pod => pod.index)).size === run.clients &&
    originals.every(pod => Number.isInteger(pod.index) && pod.index >= 0 && pod.index < run.clients && pod.jobUid === observed.metadata.uid) &&
    observed.item.kind === 'Job' && observed.item.apiVersion === 'batch/v1' && observed.metadata.name === 'direct-load-confirmwide' &&
    spec.completionMode === 'Indexed' && spec.completions === run.clients && status.succeeded === run.clients &&
    (status.active === undefined || status.active === 0) && (status.failed === undefined || status.failed === 0) &&
    evidenceList(status.conditions).map(evidenceObject).some(condition => condition.type === 'Complete' && condition.status === 'True'),
  'Exact original client Job is not complete and idle');
  const list = evidenceObject(pods), items = evidenceList(list.items).map(pod => owned(pod, run));
  requireEvidence(list.kind === 'PodList' && items.length === run.clients && claims.size === run.clients,
    'Incomplete original client Pod or evidence claim inventory');
  return originals.map(original => {
    const found = items.find(value => value.metadata.uid === original.uid);
    requireEvidence(found, 'An original client Pod was replaced or disappeared');
    const { item: pod, metadata } = found, podSpec = evidenceObject(pod.spec), podStatus = evidenceObject(pod.status);
    requireEvidence(pod.kind === 'Pod' && pod.apiVersion === 'v1' && metadata.name === original.name &&
      evidenceObject(metadata.annotations)['batch.kubernetes.io/job-completion-index'] === String(original.index) &&
      controller(metadata.ownerReferences, 'Job', original.jobUid) && podSpec.nodeName === original.nodeName &&
      podStatus.phase === 'Succeeded', 'Original client Pod is not terminal');
    const containers = evidenceList(podSpec.containers).map(evidenceObject), statuses = evidenceList(podStatus.containerStatuses).map(evidenceObject);
    requireEvidence(containers.length === 1 && containers[0].name === 'generator' && containers[0].image === original.image &&
      statuses.length === 1 && statuses[0].name === 'generator' && statuses[0].imageID === original.imageId &&
      statuses[0].containerID === original.containerId && statuses[0].restartCount === 0, 'Original client generator lifetime changed');
    const terminated = evidenceObject(evidenceObject(statuses[0].state).terminated);
    const finish = Date.parse(evidenceText(terminated.finishedAt));
    requireEvidence(Number.isFinite(finish) && Number.isSafeInteger(measurementEndMs) &&
      finish >= Math.floor(measurementEndMs / 1000) * 1000 &&
      terminated.containerID === original.containerId && terminated.startedAt === original.startedAt &&
      terminated.exitCode === 0 && terminated.reason === 'Completed' && (terminated.signal === undefined || terminated.signal === 0),
    'Original client did not produce an exact clean exit after the measurement window');
    const claim = owned(claims.get(original.claimName), run), claimSpec = evidenceObject(claim.item.spec);
    requireEvidence(claim.item.kind === 'PersistentVolumeClaim' && claim.item.apiVersion === 'v1' &&
      claim.metadata.name === original.claimName && claim.metadata.uid === original.claimUid &&
      controller(claim.metadata.ownerReferences, 'Pod', original.uid) && claimSpec.volumeName === original.volumeName &&
      claimSpec.storageClassName === 'managed-csi' && evidenceObject(claim.item.status).phase === 'Bound',
    'Client evidence claim changed before export');
    return { ...original, finishedAt: evidenceText(terminated.finishedAt), podVersion: evidenceText(metadata.resourceVersion) };
  });
}
