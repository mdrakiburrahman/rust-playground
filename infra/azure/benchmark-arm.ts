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
  return { deploymentUid, replicaSetUid: text(replica.uid, 'writer ReplicaSet UID'),
    podName: text(pm.name, 'writer pod name'), podUid: text(pm.uid, 'writer pod UID'), podIp, image, imageId, containerId, startedAt };
}

export type WriterLifetime = ReturnType<typeof observeWriter>;

function matchesImageDigest(image: string, imageId: string): boolean {
  const expected = /@(sha256:[0-9a-f]{64})$/.exec(image)?.[1];
  return expected !== undefined && /(?:^|[@/])(sha256:[0-9a-f]{64})$/.exec(imageId)?.[1] === expected;
}

/** Call on a positive Pod API observation; absence is never terminal evidence. */
export function observeWriterExit(identity: ArmIdentity, original: WriterLifetime, value: unknown, drainStartedMs: number) {
  const pod = object(value, 'terminal writer Pod');
  const pm = object(pod.metadata, 'terminal writer metadata');
  ownership(pm, identity);
  if (pod.apiVersion !== 'v1' || pod.kind !== 'Pod' || pm.namespace !== identity.namespace ||
      pm.name !== original.podName || pm.uid !== original.podUid ||
      controller(pm, 'ReplicaSet').uid !== original.replicaSetUid) throw new Error('Original writer pod was replaced');
  const spec = object(pod.spec, 'terminal writer spec');
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
  if (container.name !== 'dataflow' || container.image !== original.image ||
      status.name !== 'dataflow' || status.restartCount !== 0 || status.imageID !== original.imageId ||
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
      data.start !== 'pending' || data.writerLifetime !== 'pending') throw new Error('Schedule is not the expected pending object');
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
  const ns = readJson(context, ['get', 'namespace', namespace]);
  const job = get('job', 'direct-load-confirmwide');
  const schedule = get('configmap', 'direct-schedule-confirmwide');
  const selector = `arrowcol-oat/run-id=${runId},batch.kubernetes.io/job-name=direct-load-confirmwide`;
  const pods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const names = array(object(pods, 'pod list').items, 'pods').map(item =>
    text(object(object(item, 'pod').metadata, 'pod metadata').name, 'pod name'));
  const logs = new Map(names.map(name => [name, kubectl(context, ['-n', namespace, 'logs', name, '-c', 'generator'])]));
  const checked = validateArm(identity, ns, job, schedule, pods, logs);
  const writer = JSON.stringify(readWriter());
  const freshPods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const fresh = validateArm(identity, readJson(context, ['get', 'namespace', namespace]),
    get('job', 'direct-load-confirmwide'), get('configmap', 'direct-schedule-confirmwide'), freshPods, logs);
  if (fresh.scheduleVersion !== checked.scheduleVersion || fresh.podUids.join(',') !== checked.podUids.join(',') ||
      JSON.stringify(readWriter()) !== writer) {
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
  ];
  kubectl(context, ['-n', namespace, 'patch', 'configmap', 'direct-schedule-confirmwide',
    '--type=json', '--patch', JSON.stringify(patch)]);
  const observed = object(object(get('configmap', 'direct-schedule-confirmwide'), 'schedule').data, 'schedule data');
  if (observed.start !== String(start) || observed.writerLifetime !== writer) throw new Error('Schedule patch readback mismatch');
  console.log(JSON.stringify({ runId, namespaceUid, podUids: checked.podUids, writer: JSON.parse(writer), runStartUnixMs: start,
    measurementStartUnixMs: start + 60_000, measurementEndUnixMs: start + 660_000 }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
