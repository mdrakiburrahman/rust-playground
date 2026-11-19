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
