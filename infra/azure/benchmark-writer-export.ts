import { observeWriterClaim, WRITER_CLAIM, type ArmIdentity, type observeWriterExit } from './benchmark-arm.js';

type Capture = ReturnType<typeof observeWriterClaim>;
type WriterExit = ReturnType<typeof observeWriterExit>;

/** Render after positive terminal evidence; the helper cannot modify the retained claim. */
export function writerLogExportPod(run: ArmIdentity, writer: WriterExit, pinnedClaim: Capture, observedClaim: unknown) {
  const cap = observeWriterClaim(run, observedClaim);
  if (cap.claimUid !== pinnedClaim.claimUid || cap.volumeName !== pinnedClaim.volumeName ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(writer.podUid) ||
      !/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/.test(writer.nodeName) ||
      !/^.+@sha256:[0-9a-f]{64}$/.test(writer.image) || !writer.terminalPodResourceVersion) {
    throw new Error('Cannot export from an unverified original writer claim, node or lifetime');
  }
  const markers = { 'arrowcol-oat/run-id': run.runId, 'arrowcol-oat/owner-token': run.owner };
  return {
    apiVersion: 'v1', kind: 'Pod',
    metadata: { namespace: run.namespace, name: 'direct-writer-log-export', labels: markers,
      annotations: { ...markers, 'arrowcol-oat/original-writer-pod-uid': writer.podUid, 'arrowcol-oat/original-claim-uid': cap.claimUid } },
    spec: {
      restartPolicy: 'Never', activeDeadlineSeconds: 1800, automountServiceAccountToken: false, enableServiceLinks: false,
      affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: {
        nodeSelectorTerms: [{ matchFields: [{ key: 'metadata.name', operator: 'In', values: [writer.nodeName] }] }],
      } } },
      tolerations: [{ key: 'arrow-collector/role', operator: 'Equal', value: 'arrowcol', effect: 'NoSchedule' }],
      securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{
        name: 'export', image: writer.image, command: ['/bin/sleep', '1800'],
        resources: { requests: { cpu: '100m', memory: '32Mi' }, limits: { cpu: '1', memory: '128Mi' } },
        securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } },
        volumeMounts: [{ name: 'writer-logs', mountPath: '/evidence', readOnly: true, subPath: `benchmark-logs/${writer.podUid}` }],
      }],
      volumes: [{ name: 'writer-logs', persistentVolumeClaim: { claimName: WRITER_CLAIM, readOnly: true } }],
    },
  };
}
