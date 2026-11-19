import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { lstat } from 'node:fs/promises';
import { join } from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { observeWriterClaim, WRITER_CLAIM, type ArmIdentity, type observeWriterExit } from './benchmark-arm.js';
import type { RunCleanupAuthority } from './run_fic.js';
import type { observeCompletedClients } from './benchmark-client-terminal.js';

type Capture = ReturnType<typeof observeWriterClaim>;
type WriterExit = ReturnType<typeof observeWriterExit>;
const maxStreamBytes = 4 * 1024 ** 3;

/** Render after positive terminal evidence; the helper cannot modify the retained claim. */
export function writerLogExportPod(run: ArmIdentity, writer: WriterExit, pinnedClaim: Capture, observedClaim: unknown) {
  const cap = observeWriterClaim(run, observedClaim);
  if (cap.claimUid !== pinnedClaim.claimUid || cap.volumeName !== pinnedClaim.volumeName ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(writer.podUid) ||
      !/^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/.test(writer.nodeName) ||
      !/^.+@sha256:[0-9a-f]{64}$/.test(writer.image) || !writer.terminalPodResourceVersion) {
    throw new Error('Cannot export from an unverified original writer claim, node or lifetime');
  }
  return renderExportPod(run, { ...writer, originalUid: writer.podUid, claimName: WRITER_CLAIM, claimUid: cap.claimUid,
    subPath: `benchmark-logs/${writer.podUid}`, helperName: 'direct-writer-log-export', role: 'arrowcol' });
}

export function clientLogExportPod(run: ArmIdentity, client: ReturnType<typeof observeCompletedClients>[number]) {
  if (!Number.isInteger(client.index) || client.index < 0 || client.index >= run.clients ||
      !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(client.uid) ||
      !/^.+@sha256:[0-9a-f]{64}$/.test(client.image) || client.claimName !== `${client.name}-evidence` || !client.podVersion) {
    throw new Error('Cannot export from an unverified original client claim, node or lifetime');
  }
  return renderExportPod(run, { ...client, originalUid: client.uid, subPath: client.uid,
    helperName: `direct-client-${client.index}-log-export`, role: 'client' });
}
export async function exportWriterLogs(authority: RunCleanupAuthority, run: ArmIdentity, server: string, context: string,
    directory: string, observeHelper: () => Promise<{ podUid: string; containerId: string }>) {
  return exportLogs(authority, run, server, context, directory, observeHelper, 'writer', maxStreamBytes);
}

export async function exportClientLogs(authority: RunCleanupAuthority, run: ArmIdentity, server: string, context: string,
  directory: string, index: number, observeHelper: () => Promise<{ podUid: string; containerId: string }>) {
  if (!Number.isInteger(index) || index < 0 || index >= run.clients) throw new Error('Invalid original client export index');
  return exportLogs(authority, run, server, context, directory, observeHelper, `client-${index}`, 128 * 1024 ** 2);
}

async function exportLogs(authority: RunCleanupAuthority, run: ArmIdentity, server: string, context: string,
  directory: string, observeHelper: () => Promise<{ podUid: string; containerId: string }>, streamOwner: string, byteLimit: number) {
    const stat = await lstat(directory);
    if (!stat.isDirectory() || (stat.mode & 0o077) !== 0 || !/^https:\/\/[a-z0-9.-]+$/.test(server)) {
      throw new Error('Writer export needs a private evidence directory and exact TLS AKS server');
    }
    await authority.assertCurrent();
    const pin = await observeHelper();
    if (!pin.podUid || !pin.containerId) throw new Error('Writer export helper identity is unknown');
    async function checkCurrent() {
      await authority.assertCurrent();
      const now = await observeHelper();
      if (now.podUid !== pin.podUid || now.containerId !== pin.containerId) throw new Error('Writer export helper was replaced');
    }
    async function exec(command: string[], transfer: (stdout: NodeJS.ReadableStream) => Promise<void>) {
      await checkCurrent();
      const child = spawn('kubectl', ['--context', context, '--server', server, '--insecure-skip-tls-verify=false',
        '--request-timeout=900s', '-n', run.namespace, 'exec', `direct-${streamOwner}-log-export`, '-c', 'export', '--', ...command],
      { stdio: ['ignore', 'pipe', 'ignore'], timeout: 900_000 });
      const done = new Promise<void>((resolve, reject) => {
        child.once('error', reject);
        child.once('close', (status, signal) => status === 0 && signal === null ? resolve() : reject(new Error('Writer export transport failed')));
      });
      try { await Promise.all([done, transfer(child.stdout)]); }
      catch (error) { child.kill(); throw error; }
      await checkCurrent();
    }
    async function fingerprint(file: string) {
      const parts: Buffer[] = [];
      let bytes = 0;
      await exec(['/usr/bin/sha256sum', '--', file], async stdout => {
        for await (const chunk of stdout) {
          const part = Buffer.from(chunk);
          bytes += part.length;
          if (bytes > 256) throw new Error('Writer export fingerprint is oversized');
          parts.push(part);
        }
      });
      const match = /^([a-f0-9]{64}) [ *]\/evidence\/(?:stdout|stderr)\n$/.exec(Buffer.concat(parts).toString('ascii'));
      if (!match) throw new Error('Writer export checksum response is invalid');
      return match[1];
    }
    const files = [];
    for (const stream of ['stdout', 'stderr']) {
      const remote = `/evidence/${stream}`, local = join(directory, `${streamOwner}-${stream}.raw`);
      const before = await fingerprint(remote);
      const sha = createHash('sha256');
      let bytes = 0;
      const hash = new Transform({ transform(chunk: Buffer, _encoding, done) {
        bytes += chunk.length;
        if (bytes >= byteLimit) return done(new Error('Original log reached its capture capacity'));
        sha.update(chunk);
        done(null, chunk);
      } });
      await exec(['/bin/cat', '--', remote], async stdout => {
        await pipeline(stdout, hash, createWriteStream(local, { flags: 'wx', mode: 0o600, flush: true }));
      });
      const sha256 = sha.digest('hex');
      if (before !== sha256 || await fingerprint(remote) !== sha256) throw new Error('Original writer log changed during export');
      files.push({ stream, local, bytes, sha256 });
    }
    return { helper: pin, files };
}

function renderExportPod(run: ArmIdentity, origin: {
  originalUid: string; nodeName: string; image: string; claimName: string; claimUid: string;
  subPath: string; helperName: string; role: 'arrowcol' | 'client';
}) {
  const markers = { 'arrowcol-oat/run-id': run.runId, 'arrowcol-oat/owner-token': run.owner };
  return {
    apiVersion: 'v1', kind: 'Pod',
    metadata: { namespace: run.namespace, name: origin.helperName, labels: markers,
      annotations: { ...markers, 'arrowcol-oat/original-pod-uid': origin.originalUid, 'arrowcol-oat/original-claim-uid': origin.claimUid } },
    spec: {
      restartPolicy: 'Never', activeDeadlineSeconds: 1800, automountServiceAccountToken: false, enableServiceLinks: false,
      affinity: { nodeAffinity: { requiredDuringSchedulingIgnoredDuringExecution: {
        nodeSelectorTerms: [{ matchFields: [{ key: 'metadata.name', operator: 'In', values: [origin.nodeName] }] }],
      } } },
      tolerations: [{ key: 'arrow-collector/role', operator: 'Equal', value: origin.role, effect: 'NoSchedule' }],
      securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: { type: 'RuntimeDefault' } },
      containers: [{
        name: 'export', image: origin.image, command: ['/bin/sleep', '1800'],
        resources: { requests: { cpu: '100m', memory: '32Mi' }, limits: { cpu: '1', memory: '128Mi' } },
        securityContext: { allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: { drop: ['ALL'] } },
        volumeMounts: [{ name: 'writer-logs', mountPath: '/evidence', readOnly: true, subPath: origin.subPath }],
      }],
      volumes: [{ name: 'writer-logs', persistentVolumeClaim: { claimName: origin.claimName, readOnly: true } }],
    },
  };
}
