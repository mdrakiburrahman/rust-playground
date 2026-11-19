import { canonical_json } from './bootstrap.js';
import type { ArmIdentity } from './benchmark-arm.js';

type Json = Record<string, unknown>;

function object(value: unknown): Json {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Missing Kubernetes observation');
  return value as Json;
}

const noEligibleNode = {
  nodeAffinity: {
    requiredDuringSchedulingIgnoredDuringExecution: {
      nodeSelectorTerms: [{ matchExpressions: [
        { key: 'kubernetes.io/os', operator: 'Exists' },
        { key: 'kubernetes.io/os', operator: 'DoesNotExist' },
      ] }],
    },
  },
};
const routerServices = ['arrowcol-router', 'arrowcol-router-node-local'] as const;

export function observeRouterLifetimes(run: ArmIdentity, uid: string, image: string, value: unknown, pods: unknown) {
  const { metadata, status, podSpec } = router(value, run, uid);
  const template = object(podSpec), templates = template.containers;
  const list = object(pods);
  if (!/^.+@sha256:[0-9a-f]{64}$/.test(image) || !Array.isArray(templates) || templates.length !== 1 ||
      object(templates[0]).name !== 'router' || object(templates[0]).image !== image ||
      status.observedGeneration !== metadata.generation || !Number.isSafeInteger(status.desiredNumberScheduled) ||
      typeof status.desiredNumberScheduled !== 'number' || status.desiredNumberScheduled < 1 ||
      status.currentNumberScheduled !== status.desiredNumberScheduled || status.numberReady !== status.desiredNumberScheduled ||
      list.kind !== 'PodList' || !Array.isArray(list.items) || list.items.length !== status.desiredNumberScheduled)
    throw new Error('Original router DaemonSet is not ready with the pinned image');
  const result = list.items.map(value => {
    const pod = object(value), meta = object(pod.metadata), spec = object(pod.spec), state = object(pod.status);
    const owners = meta.ownerReferences;
    const containers = spec.containers, statuses = state.containerStatuses;
    if (pod.kind !== 'Pod' || pod.apiVersion !== 'v1' || meta.namespace !== run.namespace || meta.deletionTimestamp ||
        typeof meta.uid !== 'string' || typeof meta.name !== 'string' || typeof spec.nodeName !== 'string' ||
        !Array.isArray(owners) || owners.filter(ref => object(ref).controller === true).length !== 1 ||
        !owners.some(ref => object(ref).controller === true && object(ref).kind === 'DaemonSet' && object(ref).uid === uid) ||
        !Array.isArray(containers) || containers.length !== 1 || object(containers[0]).name !== 'router' ||
        object(containers[0]).image !== image || !Array.isArray(statuses) || statuses.length !== 1)
      throw new Error('Original router Pod ownership or image changed');
    for (const field of ['annotations', 'labels']) {
      const markers = object(meta[field]);
      if (markers['arrowcol-oat/run-id'] !== run.runId || markers['arrowcol-oat/owner-token'] !== run.owner)
        throw new Error('Original router Pod ownership markers changed');
    }
    const active = object(statuses[0]), running = object(object(active.state).running);
    if (state.phase !== 'Running' || active.name !== 'router' || active.ready !== true || active.restartCount !== 0 ||
        typeof active.imageID !== 'string' || active.imageID.split(/[@/]/).at(-1) !== image.split('@')[1] ||
        typeof active.containerID !== 'string' || !active.containerID || typeof running.startedAt !== 'string' ||
        !Number.isFinite(Date.parse(running.startedAt))) throw new Error('Original router Pod lifetime is not healthy');
    return { uid: meta.uid, name: meta.name, nodeName: spec.nodeName, image, imageId: active.imageID,
      containerId: active.containerID, startedAt: running.startedAt };
  });
  if (new Set(result.map(item => item.uid)).size !== result.length || new Set(result.map(item => item.nodeName)).size !== result.length)
    throw new Error('Original router Pod or node identity is duplicated');
  return result.sort((a, b) => String(a.nodeName).localeCompare(String(b.nodeName)));
}

function router(value: unknown, run: ArmIdentity, uid: string) {
  const daemonSet = object(value), metadata = object(daemonSet.metadata);
  if (daemonSet.kind !== 'DaemonSet' || daemonSet.apiVersion !== 'apps/v1' ||
      metadata.namespace !== run.namespace || metadata.name !== 'arrowcol-router' || metadata.uid !== uid ||
      metadata.deletionTimestamp || typeof metadata.resourceVersion !== 'string' || !metadata.resourceVersion ||
      typeof metadata.generation !== 'number' || !Number.isSafeInteger(metadata.generation) || metadata.generation < 1) {
    throw new Error('Exact router DaemonSet identity changed');
  }
  for (const field of ['annotations', 'labels']) {
    const markers = object(metadata[field]);
    if (markers['arrowcol-oat/run-id'] !== run.runId || markers['arrowcol-oat/owner-token'] !== run.owner) {
      throw new Error('Exact router DaemonSet ownership changed');
    }
  }
  const spec = object(daemonSet.spec), template = object(spec.template);
  return { metadata, status: object(daemonSet.status), podSpec: object(template.spec) };
}

export function planRouterFence(run: ArmIdentity, uid: string, value: unknown) {
  const { metadata, podSpec } = router(value, run, uid);
  if (podSpec.affinity !== undefined) throw new Error('Unexpected original router affinity; do not overwrite or replay fence');
  return [
    { op: 'test', path: '/metadata/uid', value: uid },
    { op: 'test', path: '/metadata/resourceVersion', value: metadata.resourceVersion },
    { op: 'add', path: '/spec/template/spec/affinity', value: noEligibleNode },
  ];
}

export function acceptRouterFencePatch(run: ArmIdentity, uid: string, originalGeneration: number, response: unknown) {
  const { metadata, podSpec } = router(response, run, uid);
  const generation = metadata.generation as number;
  if (generation <= originalGeneration || canonical_json(podSpec.affinity) !== canonical_json(noEligibleNode)) {
    throw new Error('Router fence lacks an exact positive PATCH response');
  }
  return generation;
}

export function routerFenceObserved(run: ArmIdentity, uid: string, generation: number, value: unknown,
  pods: unknown, endpointSlices: Record<typeof routerServices[number], unknown>): boolean {
  const { metadata, status, podSpec } = router(value, run, uid);
  if (metadata.generation !== generation || canonical_json(podSpec.affinity) !== canonical_json(noEligibleNode)) {
    throw new Error('Router fence changed after its positive PATCH response');
  }
  const list = object(pods);
  if (list.kind !== 'PodList' || !Array.isArray(list.items)) {
    throw new Error('Exact router PodList or service EndpointSliceList coverage is missing');
  }
  const remainingEndpoints = routerServices.map(service => {
    const slices = object(endpointSlices[service]);
    if (slices.kind !== 'EndpointSliceList' || !Array.isArray(slices.items)) {
      throw new Error('Exact router EndpointSliceList is missing');
    }
    return slices.items.some(slice => {
      const { metadata, endpoints } = object(slice);
      if (object(metadata).namespace !== run.namespace || object(object(metadata).labels)['kubernetes.io/service-name'] !== service ||
          !Array.isArray(endpoints)) throw new Error('Unknown router endpoint coverage');
      return endpoints.length > 0;
    });
  });
  return status.observedGeneration === generation && status.desiredNumberScheduled === 0 &&
    status.currentNumberScheduled === 0 && status.numberMisscheduled === 0 && list.items.length === 0 &&
    !remainingEndpoints.some(Boolean);
}
