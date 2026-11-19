import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

type Json = Record<string, unknown>;

function object(value: unknown, name: string): Json {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${name}`);
  return value as Json;
}

function text(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0) throw new Error(`Invalid ${name}`);
  return value;
}

function array(value: unknown, name: string): unknown[] {
  if (!Array.isArray(value)) throw new Error(`Invalid ${name}`);
  return value;
}

export interface ArmIdentity {
  namespace: string;
  namespaceUid: string;
  runId: string;
  owner: string;
  clients: number;
}
export interface ClientLifetime {
  index: number;
  name: string;
  uid: string;
  jobUid: string;
  nodeName: string;
  image: string;
  imageId: string;
  containerId: string;
  startedAt: string;
  claimName: string;
  claimUid: string;
  volumeName: string;
}

function labels(metadata: Json, identity: ArmIdentity): void {
  const found = object(metadata.labels, 'labels');
  if (found['arrowcol-oat/run-id'] !== identity.runId || found['arrowcol-oat/owner-token'] !== identity.owner) {
    throw new Error('Run or owner label mismatch');
  }
}

function ownership(metadata: Json, identity: ArmIdentity): void {
  labels(metadata, identity);
  const found = object(metadata.annotations, 'annotations');
  if (found['arrowcol-oat/run-id'] !== identity.runId || found['arrowcol-oat/owner-token'] !== identity.owner) {
    throw new Error('Run or owner annotation mismatch');
  }
}

function controller(metadata: Json, kind: string): Json {
  const owners = array(metadata.ownerReferences, 'controller references').map(value => object(value, 'owner'));
  const controllers = owners.filter(value => value.controller === true);
  if (controllers.length !== 1 || controllers[0].kind !== kind) throw new Error(`Missing exact ${kind} controller`);
  return controllers[0];
}

export function observeWriter(identity: ArmIdentity, deployment: unknown, replicaSets: unknown, pods: unknown) {
  const workload = object(deployment, 'writer Deployment');
  const dm = object(workload.metadata, 'writer metadata');
  ownership(dm, identity);
  if (workload.apiVersion !== 'apps/v1' || workload.kind !== 'Deployment' || dm.name !== 'arrowcol' ||
      dm.namespace !== identity.namespace || dm.deletionTimestamp ||
      object(workload.spec, 'writer spec').replicas !== 1) throw new Error('Writer Deployment is not the expected single replica');
  const deploymentUid = text(dm.uid, 'writer Deployment UID');
  const items = array(object(pods, 'writer pods').items, 'writer pod list');
  if (items.length !== 1) throw new Error('Expected exactly one writer pod');
  const pod = object(items[0], 'writer pod');
  const pm = object(pod.metadata, 'writer pod metadata');
  ownership(pm, identity);
  const replica = controller(pm, 'ReplicaSet');
  const sets = array(object(replicaSets, 'writer ReplicaSets').items, 'writer ReplicaSet list')
    .map(value => object(object(value, 'writer ReplicaSet').metadata, 'writer ReplicaSet metadata'))
    .filter(value => value.uid === replica.uid && value.name === replica.name);
  if (pod.apiVersion !== 'v1' || pod.kind !== 'Pod' || pm.namespace !== identity.namespace || pm.deletionTimestamp ||
      sets.length !== 1 || sets[0].namespace !== identity.namespace || sets[0].deletionTimestamp ||
      controller(sets[0], 'Deployment').uid !== deploymentUid) throw new Error('Writer pod is not owned by the exact Deployment');
  const spec = object(pod.spec, 'writer pod spec');
  const containers = array(spec.containers, 'writer containers');
  if (containers.length !== 1) throw new Error('Expected exactly one writer container');
  const container = object(containers[0], 'writer container');
  const image = text(container.image, 'writer image');
  if (container.name !== 'dataflow' || !/^.+@sha256:[0-9a-f]{64}$/.test(image)) throw new Error('Writer image is not digest-pinned');
  const status = object(pod.status, 'writer pod status');
  const statuses = array(status.containerStatuses, 'writer container statuses');
  if (statuses.length !== 1) throw new Error('Expected exactly one writer container status');
  const active = object(statuses[0], 'writer container status');
  const startedAt = text(object(object(active.state, 'writer state').running, 'running writer').startedAt, 'writer start');
  const imageId = text(active.imageID, 'writer runtime image');
  const containerId = text(active.containerID, 'writer runtime container');
  if (!matchesImageDigest(image, imageId)) throw new Error('Writer runtime digest differs from its pinned image');
  if (status.phase !== 'Running' || active.name !== 'dataflow' || active.ready !== true || active.restartCount !== 0 ||
      !Number.isFinite(Date.parse(startedAt))) {
    throw new Error('Writer container is not the original healthy lifetime');
  }
  const podIp = text(status.podIP, 'writer pod IP');
  const nodeName = text(spec.nodeName, 'writer node name');
  const staging = array(spec.volumes, 'writer volumes').map(v => object(v, 'writer volume')).filter(v => v.name === 'staging');
  const mounts = array(container.volumeMounts, 'writer mounts').map(v => object(v, 'writer mount')).filter(v => v.name === 'staging');
  if (staging.length !== 1 || object(staging[0].persistentVolumeClaim, 'writer claim').claimName !== WRITER_CLAIM ||
      mounts.length !== 1 || mounts[0].mountPath !== '/var/lib/arrowcol/staging' || mounts[0].subPath !== undefined ||
      mounts[0].subPathExpr !== undefined) throw new Error('Writer pod is not using the retained staging PVC');
  return { deploymentUid, replicaSetUid: text(replica.uid, 'writer ReplicaSet UID'),
    podName: text(pm.name, 'writer pod name'), podUid: text(pm.uid, 'writer pod UID'), podIp, nodeName, image, imageId, containerId, startedAt };
}

export type WriterLifetime = ReturnType<typeof observeWriter>;

export const WRITER_CLAIM = 'arrowcol-writer-staging';
export const WRITER_CAPTURE = '/var/lib/arrowcol/staging/benchmark-logs';
export function observeWriterClaim(identity: ArmIdentity, value: unknown) {
  const claim = object(value, 'writer staging PVC'), metadata = object(claim.metadata, 'writer staging PVC metadata');
  ownership(metadata, identity);
  const spec = object(claim.spec, 'writer staging PVC spec'), status = object(claim.status, 'writer staging PVC status');
  if (claim.apiVersion !== 'v1' || claim.kind !== 'PersistentVolumeClaim' || metadata.deletionTimestamp ||
      metadata.name !== WRITER_CLAIM || metadata.namespace !== identity.namespace ||
      (metadata.ownerReferences !== undefined && array(metadata.ownerReferences, 'PVC owner references').length > 0) ||
      spec.storageClassName !== `arrowcol-${identity.runId.replaceAll('-', '')}-local-csi` || status.phase !== 'Bound') {
    throw new Error('Writer capture PVC is not confirmed');
  }
  return { claimUid: text(metadata.uid, 'writer claim UID'), volumeName: text(spec.volumeName, 'writer PV name') };
}

export function observeWriterCapture(identity: ArmIdentity, value: unknown, probe: string) {
  if (probe !== 'writer-capture-active\n') throw new Error('Writer live stdout/stderr redirection is not confirmed');
  return observeWriterClaim(identity, value);
}

const probeCapture = `
set -eu
d="${WRITER_CAPTURE}/$1"
p=$(cat "$d/pid")
case "$p" in ''|*[!0-9]*) exit 1 ;; esac
test "$(readlink "/proc/$p/exe")" = /usr/local/bin/arrowcol-dataflow-rust
test "$(readlink "/proc/$p/fd/1")" = "$d/stdout"
test "$(readlink "/proc/$p/fd/2")" = "$d/stderr"
for f in stdout stderr; do
  test -f "$d/$f"
  test "$(stat -c %b "$d/$f")" -ge 8388608
  test "$(stat -c %s "$d/$f")" -lt 4294967296
done
printf 'writer-capture-active\\n'
`;
const probeClientCapture = `
set -eu
d="/evidence/$1"
p=$(cat "$d/pid")
test "$p" = 1
test "$(readlink "/proc/$p/exe")" = /usr/local/bin/arrowcol-dmv
test "$(readlink "/proc/$p/fd/1")" = "$d/stdout"
test "$(readlink "/proc/$p/fd/2")" = "$d/stderr"
for f in stdout stderr; do
  test -f "$d/$f"
  test "$(stat -c %b "$d/$f")" -ge 262144
  test "$(stat -c %s "$d/$f")" -lt 134217728
done
grep -F -x -- "$2" "$d/stdout"
`;

function matchesImageDigest(image: string, imageId: string): boolean {
  const expected = /@(sha256:[0-9a-f]{64})$/.exec(image)?.[1];
  return expected !== undefined && /(?:^|[@/])(sha256:[0-9a-f]{64})$/.exec(imageId)?.[1] === expected;
}

/** Call on a positive Pod API observation; absence is never terminal evidence. */
export function observeWriterExit(identity: ArmIdentity, original: WriterLifetime, value: unknown, drainStartedMs: number,
  containerName: 'dataflow' | 'maintenance' = 'dataflow') {
  const pod = object(value, 'terminal writer Pod');
  const pm = object(pod.metadata, 'terminal writer metadata');
  ownership(pm, identity);
  if (pod.apiVersion !== 'v1' || pod.kind !== 'Pod' || pm.namespace !== identity.namespace ||
      pm.name !== original.podName || pm.uid !== original.podUid ||
      controller(pm, 'ReplicaSet').uid !== original.replicaSetUid) throw new Error('Original writer pod was replaced');
  const spec = object(pod.spec, 'terminal writer spec');
  if (spec.nodeName !== original.nodeName) throw new Error('Original writer node changed');
  const containers = array(spec.containers, 'terminal writer containers');
  const statuses = array(object(pod.status, 'terminal writer status').containerStatuses, 'terminal writer container statuses');
  if (containers.length !== 1 || statuses.length !== 1) throw new Error('Terminal writer coverage changed');
  const container = object(containers[0], 'terminal writer container');
  const status = object(statuses[0], 'terminal writer container status');
  const state = object(status.state, 'terminal writer state');
  const ended = object(state.terminated, 'writer termination');
  const startedAt = text(ended.startedAt, 'writer termination start');
  const finishedAt = text(ended.finishedAt, 'writer termination finish');
  const finishMs = Date.parse(finishedAt);
  if (container.name !== containerName || container.image !== original.image ||
      status.name !== containerName || status.restartCount !== 0 || status.imageID !== original.imageId ||
      !matchesImageDigest(original.image, original.imageId) ||
      status.containerID !== original.containerId || ended.containerID !== original.containerId ||
      Object.keys(state).length !== 1 || startedAt !== original.startedAt || !Number.isFinite(Date.parse(startedAt)) ||
      !Number.isSafeInteger(drainStartedMs) || drainStartedMs < Date.parse(startedAt) ||
      !Number.isFinite(finishMs) || finishMs < Math.floor(drainStartedMs / 1000) * 1000 ||
      ended.exitCode !== 0 || (ended.signal !== undefined && ended.signal !== 0) || ended.reason !== 'Completed') {
    throw new Error('Original writer did not produce an exact clean exit after drain began');
  }
  return { ...original, finishedAt, terminalPodResourceVersion: text(pm.resourceVersion, 'terminal pod resourceVersion') };
}

export function validateArm(
  identity: ArmIdentity, namespace: unknown, job: unknown, schedule: unknown,
  pods: unknown, logs: ReadonlyMap<string, string>,
): { scheduleVersion: string; podUids: string[] } {
  if (!/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(identity.runId) ||
      identity.namespace !== `arrow-collector-${identity.runId.replaceAll('-', '')}` ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(identity.namespaceUid) ||
      !/^[A-Za-z0-9][A-Za-z0-9_-]{14,61}[A-Za-z0-9]$/.test(identity.owner) ||
      ![1, 2, 4, 8].includes(identity.clients)) throw new Error('Invalid run identity');
  const ns = object(object(namespace, 'namespace').metadata, 'namespace metadata');
  if (ns.name !== identity.namespace || ns.uid !== identity.namespaceUid) throw new Error('Namespace UID mismatch');
  labels(ns, identity);
  const annotations = object(ns.annotations, 'namespace annotations');
  if (annotations['arrowcol-oat/run-id'] !== identity.runId ||
      annotations['arrowcol-oat/owner-token'] !== identity.owner || ns.deletionTimestamp) {
    throw new Error('Namespace annotation or deletion state mismatch');
  }
  const jobObject = object(job, 'job');
  const jm = object(jobObject.metadata, 'job metadata');
  labels(jm, identity);
  if (jm.name !== 'direct-load-confirmwide' || jm.namespace !== identity.namespace) throw new Error('Job identity mismatch');
  const uid = text(jm.uid, 'job UID');
  const spec = object(jobObject.spec, 'job spec');
  if (spec.suspend !== false || spec.completionMode !== 'Indexed' ||
      spec.parallelism !== identity.clients || spec.completions !== identity.clients) throw new Error('Job is not armed for expected clients');
  const status = object(jobObject.status ?? {}, 'job status');
  if ((typeof status.failed === 'number' && status.failed > 0) ||
      (typeof status.succeeded === 'number' && status.succeeded > 0)) throw new Error('Job has terminal pods');
  const cm = object(schedule, 'schedule');
  const metadata = object(cm.metadata, 'schedule metadata');
  labels(metadata, identity);
  const data = object(cm.data, 'schedule data');
  if (metadata.name !== 'direct-schedule-confirmwide' || metadata.namespace !== identity.namespace ||
      data.start !== 'pending' || data.writerLifetime !== 'pending' || data.clientLifetimes !== 'pending')
    throw new Error('Schedule is not the expected pending object');
  const scheduleVersion = text(metadata.resourceVersion, 'schedule resourceVersion');
  const items = array(object(pods, 'pod list').items, 'pods');
  if (items.length !== identity.clients || logs.size !== identity.clients) throw new Error('Wrong client pod count');
  const seen = new Set<number>();
  const podUids: string[] = [];
  for (const item of items) {
    const pod = object(item, 'pod');
    const pm = object(pod.metadata, 'pod metadata');
    labels(pm, identity);
    if (pm.namespace !== identity.namespace) throw new Error('Pod namespace mismatch');
    const name = text(pm.name, 'pod name');
    const podUid = text(pm.uid, 'pod UID');
    const owners = array(pm.ownerReferences, 'pod owners');
    if (!owners.some(value => {
      const owner = object(value, 'pod owner');
      return owner.uid === uid && owner.kind === 'Job' && owner.controller === true;
    })) throw new Error('Pod is not owned by the expected Job');
    const podSpec = object(pod.spec, 'pod spec');
    const image = array(podSpec.containers, 'pod containers')
      .map(value => object(value, 'pod container')).find(value => value.name === 'generator')?.image;
    if (typeof image !== 'string' || !/^.+@sha256:[0-9a-f]{64}$/.test(image)) {
      throw new Error('Client image is not pinned to an immutable digest');
    }
    const annotation = object(pm.annotations, 'pod annotations')['batch.kubernetes.io/job-completion-index'];
    if (typeof annotation !== 'string' || !/^(0|[1-9][0-9]*)$/.test(annotation)) throw new Error('Invalid pod index');
    const index = Number(annotation);
    if (!Number.isSafeInteger(index) || index >= identity.clients || seen.has(index)) throw new Error('Duplicate or unexpected pod index');
    seen.add(index);
    const ps = object(pod.status, 'pod status');
    if (ps.phase !== 'Running' || !array(ps.conditions, 'pod conditions').some(value => {
      const condition = object(value, 'pod condition');
      return condition.type === 'Ready' && condition.status === 'True';
    })) throw new Error('Client pod is not Ready');
    const containers = array(ps.containerStatuses, 'container statuses');
    const generator = containers.map(value => object(value, 'container status')).find(value => value.name === 'generator');
    if (!generator || generator.ready !== true || generator.restartCount !== 0 ||
        !object(generator.state, 'generator state').running) throw new Error('Client generator is not healthy');
    const ready = `OAT_CLIENT_READY client_id=direct-${identity.runId}-${index} preflight_tables=1`;
    const log = logs.get(name);
    if (log === undefined || log.split(/\r?\n/).filter(line => line === ready).length !== 1) {
      throw new Error(`Missing or duplicate client readiness for ${name}`);
    }
    podUids.push(podUid);
  }
  if (new Set(podUids).size !== identity.clients) throw new Error('Duplicate pod UID');
  return { scheduleVersion, podUids: podUids.sort() };
}

function kubectl(context: string, args: string[]): string {
  const result = spawnSync('kubectl', ['--context', context, '--request-timeout=15s', ...args],
    { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024, timeout: 20_000 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`kubectl failed (${result.status}): ${result.stderr}`);
  return result.stdout;
}

function readJson(context: string, args: string[]): unknown {
  return JSON.parse(kubectl(context, [...args, '-o', 'json'])) as unknown;
}

function main(): void {
  const [context, namespace, namespaceUid, runId, owner, count, confirmation, ...extra] = process.argv.slice(2);
  if (extra.length || !context || !namespace || !namespaceUid || !runId || !owner ||
      !/^[1-8]$/.test(count ?? '') || confirmation !== '--confirm-authorized') {
    throw new Error('Usage: tsx benchmark-arm.ts CONTEXT NAMESPACE NAMESPACE_UID RUN_UUID OWNER CLIENTS --confirm-authorized');
  }
  const identity = { namespace, namespaceUid, runId, owner, clients: Number(count) };
  const get = (kind: string, name: string) => readJson(context, ['-n', namespace, 'get', kind, name]);
  const readWriter = () => observeWriter(identity, get('deployment', 'arrowcol'),
    readJson(context, ['-n', namespace, 'get', 'replicasets', '-l', 'app=arrowcol']),
    readJson(context, ['-n', namespace, 'get', 'pods', '-l', 'app=arrowcol']));
  const readCapturedWriter = () => {
    const lifetime = readWriter();
    const claim = get('pvc', WRITER_CLAIM);
    const probe = kubectl(context, ['-n', namespace, 'exec', lifetime.podName, '-c', 'dataflow', '--',
      '/bin/sh', '-c', probeCapture, 'capture', lifetime.podUid]);
    return { ...lifetime, capture: observeWriterCapture(identity, claim, probe) };
  };
  const ns = readJson(context, ['get', 'namespace', namespace]);
  const job = get('job', 'direct-load-confirmwide');
  const schedule = get('configmap', 'direct-schedule-confirmwide');
  const selector = `arrowcol-oat/run-id=${runId},batch.kubernetes.io/job-name=direct-load-confirmwide`;
  const pods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const clientLogs = (list: unknown) => new Map(array(object(list, 'pod list').items, 'pods').map(item => {
    const meta = object(object(item, 'pod').metadata, 'pod metadata'), name = text(meta.name, 'pod name');
    const uid = text(meta.uid, 'client Pod UID');
    if (!/^[a-z0-9-]+$/.test(uid)) throw new Error('Invalid client Pod UID');
    const index = object(meta.annotations, 'client Pod annotations')['batch.kubernetes.io/job-completion-index'];
    const pattern = `OAT_CLIENT_READY client_id=direct-${runId}-${index} preflight_tables=1`;
    return [name, kubectl(context, ['-n', namespace, 'exec', name, '-c', 'generator', '--',
      '/bin/sh', '-c', probeClientCapture, 'capture', uid, pattern])];
  }));
  const clientLifetimes = (list: unknown) => JSON.stringify(array(object(list, 'client pods').items, 'pod list').map((item): ClientLifetime => {
    const pod = object(item, 'client pod'), meta = object(pod.metadata, 'client metadata'), name = text(meta.name, 'client name');
    const uid = text(meta.uid, 'client UID'), spec = object(pod.spec, 'client spec');
    const index = Number(object(meta.annotations, 'client annotations')['batch.kubernetes.io/job-completion-index']);
    const containers = array(spec.containers, 'client containers'), status = object(pod.status, 'client status');
    const statuses = array(status.containerStatuses, 'client statuses');
    if (containers.length !== 1 || statuses.length !== 1) throw new Error('Client has unexpected container coverage');
    const container = object(containers[0], 'client container'), active = object(statuses[0], 'client runtime');
    const volume = array(spec.volumes, 'client volumes').map(v => object(v, 'client volume')).find(v => v.name === 'evidence');
    const mount = array(container.volumeMounts, 'client mounts').map(v => object(v, 'client mount')).find(v => v.name === 'evidence');
    if (!volume?.ephemeral || mount?.mountPath !== '/evidence' || mount.subPath !== undefined || mount.subPathExpr !== undefined) {
      throw new Error('Client evidence mount does not use its generic ephemeral PVC');
    }
    const image = text(container.image, 'client image'), imageId = text(active.imageID, 'client runtime image');
    if (!matchesImageDigest(image, imageId)) throw new Error('Client runtime digest differs from its pinned image');
    const claim = object(get('pvc', `${name}-evidence`), 'client evidence PVC');
    const cm = object(claim.metadata, 'client claim metadata'), cs = object(claim.spec, 'client claim spec');
    ownership(cm, identity);
    if (claim.kind !== 'PersistentVolumeClaim' || claim.apiVersion !== 'v1' || cm.deletionTimestamp ||
        cm.namespace !== namespace || cm.name !== `${name}-evidence` || controller(cm, 'Pod').uid !== uid ||
        cs.storageClassName !== 'managed-csi' || object(claim.status, 'client PVC status').phase !== 'Bound') {
      throw new Error('Client evidence claim does not belong to its original Pod');
    }
    return { index, name, uid, jobUid: text(controller(meta, 'Job').uid, 'client Job UID'), nodeName: text(spec.nodeName, 'client node'), image, imageId,
      containerId: text(active.containerID, 'client container ID'),
      startedAt: text(object(object(active.state, 'client state').running, 'client running').startedAt, 'client start'),
      claimName: text(cm.name, 'client claim name'), claimUid: text(cm.uid, 'client claim UID'), volumeName: text(cs.volumeName, 'client PV name') };
  }).sort((a, b) => a.index - b.index));
  const logs = clientLogs(pods);
  const checked = validateArm(identity, ns, job, schedule, pods, logs);
  const clients = clientLifetimes(pods);
  const writer = JSON.stringify(readCapturedWriter());
  const freshPods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const fresh = validateArm(identity, readJson(context, ['get', 'namespace', namespace]),
    get('job', 'direct-load-confirmwide'), get('configmap', 'direct-schedule-confirmwide'), freshPods, clientLogs(freshPods));
  if (fresh.scheduleVersion !== checked.scheduleVersion || fresh.podUids.join(',') !== checked.podUids.join(',') ||
      clientLifetimes(freshPods) !== clients || JSON.stringify(readCapturedWriter()) !== writer) {
    throw new Error('Client fleet, writer lifetime, or schedule changed before arming');
  }
  const now = Date.now();
  const start = now + 150_000;
  const patch = [
    { op: 'test', path: '/metadata/resourceVersion', value: checked.scheduleVersion },
    { op: 'test', path: '/data/start', value: 'pending' },
    { op: 'replace', path: '/data/start', value: String(start) },
    { op: 'test', path: '/data/writerLifetime', value: 'pending' },
    { op: 'replace', path: '/data/writerLifetime', value: writer },
    { op: 'test', path: '/data/clientLifetimes', value: 'pending' },
    { op: 'replace', path: '/data/clientLifetimes', value: clients },
  ];
  kubectl(context, ['-n', namespace, 'patch', 'configmap', 'direct-schedule-confirmwide',
    '--type=json', '--patch', JSON.stringify(patch)]);
  const observed = object(object(get('configmap', 'direct-schedule-confirmwide'), 'schedule').data, 'schedule data');
  if (observed.start !== String(start) || observed.writerLifetime !== writer || observed.clientLifetimes !== clients)
    throw new Error('Schedule patch readback mismatch');
  console.log(JSON.stringify({ runId, namespaceUid, podUids: checked.podUids, writer: JSON.parse(writer), runStartUnixMs: start,
    measurementStartUnixMs: start + 60_000, measurementEndUnixMs: start + 660_000 }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
