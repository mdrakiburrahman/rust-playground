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

export function validateArm(
  identity: ArmIdentity, namespace: unknown, job: unknown, schedule: unknown,
  pods: unknown, logs: ReadonlyMap<string, string>,
): { scheduleVersion: string; podUids: string[] } {
  if (!/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(identity.runId) ||
      identity.namespace !== `arrow-collector-${identity.runId.replaceAll('-', '')}` ||
      !/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/.test(identity.namespaceUid) ||
      !/^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$/.test(identity.owner) ||
      ![1, 2, 4, 8].includes(identity.clients)) throw new Error('Invalid run identity');
  const ns = object(object(namespace, 'namespace').metadata, 'namespace metadata');
  if (ns.name !== identity.namespace || ns.uid !== identity.namespaceUid) throw new Error('Namespace UID mismatch');
  labels(ns, identity);
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
  if (metadata.name !== 'direct-schedule-confirmwide' || metadata.namespace !== identity.namespace ||
      object(cm.data, 'schedule data').start !== 'pending') throw new Error('Schedule is not the expected pending object');
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
  const result = spawnSync('kubectl', ['--context', context, ...args], { encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
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
  const ns = readJson(context, ['get', 'namespace', namespace]);
  const job = get('job', 'direct-load-confirmwide');
  const schedule = get('configmap', 'direct-schedule-confirmwide');
  const selector = `arrowcol-oat/run-id=${runId},batch.kubernetes.io/job-name=direct-load-confirmwide`;
  const pods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const names = array(object(pods, 'pod list').items, 'pods').map(item =>
    text(object(object(item, 'pod').metadata, 'pod metadata').name, 'pod name'));
  const logs = new Map(names.map(name => [name, kubectl(context, ['-n', namespace, 'logs', name, '-c', 'generator'])]));
  const checked = validateArm(identity, ns, job, schedule, pods, logs);
  const freshPods = readJson(context, ['-n', namespace, 'get', 'pods', '-l', selector]);
  const fresh = validateArm(identity, readJson(context, ['get', 'namespace', namespace]),
    get('job', 'direct-load-confirmwide'), get('configmap', 'direct-schedule-confirmwide'), freshPods, logs);
  if (fresh.scheduleVersion !== checked.scheduleVersion || fresh.podUids.join(',') !== checked.podUids.join(',')) {
    throw new Error('Client fleet or schedule changed before arming');
  }
  const now = Date.now();
  const start = now + 150_000;
  const patch = [
    { op: 'test', path: '/metadata/resourceVersion', value: checked.scheduleVersion },
    { op: 'test', path: '/data/start', value: 'pending' },
    { op: 'replace', path: '/data/start', value: String(start) },
  ];
  kubectl(context, ['-n', namespace, 'patch', 'configmap', 'direct-schedule-confirmwide',
    '--type=json', '--patch', JSON.stringify(patch)]);
  const observed = object(object(get('configmap', 'direct-schedule-confirmwide'), 'schedule').data, 'schedule data');
  if (observed.start !== String(start)) throw new Error('Schedule patch readback mismatch');
  console.log(JSON.stringify({ runId, namespaceUid, podUids: checked.podUids, runStartUnixMs: start,
    measurementStartUnixMs: start + 60_000, measurementEndUnixMs: start + 660_000 }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
