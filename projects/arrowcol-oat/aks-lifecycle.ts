import { createHash } from 'node:crypto';
import { canonical_json, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, processEffectRequest } from '../../infra/azure/effect-boundary.js';
import { plan_run_fic } from '../../infra/azure/run_fic.js';
import { hasVerifiedUnsettledDrain, validateCampaignState, type CampaignStore, type RunInventory, type RunInventoryResource } from './aks-campaign.js';
import { assertRunCleanupActor, observeExactKubernetes, type CustodyCommandRunner } from './aks-setup-cleanup.js';
import type { CleanupActionResult, CleanupMutationAuthority, ExternalCleanupActions } from './aks-cleanup-executor.js';
import { cleanupBudget, CleanupDeadlineExpiredError, withCleanupDeadline } from './aks-cleanup-budget.js';
import { captureTerminalData } from './aks-data-safety.js';

type LifecycleField = 'stop_admission' | 'bounded_drain' | 'evidence_capture' | 'stop_maintenance' | 'terminal_capture';
export type LifecycleActions = Pick<ExternalCleanupActions, 'stopAdmission' | 'boundedDrain' | 'preserveEvidence' |
  'stopMaintenanceIfUidAndOwnerMatch' | 'captureTerminalState' | 'recoverLifecycle'>;
interface PodProof { name: string; uid: string; resource_version: string;
  containers: Array<{ name: string; exit_code: number | null; finished_at: string | null; oom_killed: boolean; restart_count?: number }>; }
interface LogProof {
  pod_name: string; pod_uid: string; container: string; phase: 'pre_stop' | 'terminal' | 'evidence' | 'previous';
  observed_at_unix_ms: number; body_sha256: string; bytes: number;
  numeric_samples: Array<Record<string, number>>; redacted_lines: number;
  drain_complete_marker?: boolean;
}
interface StopRecord {
  schema_version: 1; origin: ReturnType<typeof origin>; field: LifecycleField; resource: ReturnType<typeof identity>;
  phase: 'planned' | 'intent' | 'responded' | 'complete'; deadline_unix_ms: number; expected_resource_version: string;
  initial_desired: number; initial_pods: PodProof[]; terminal_pods: PodProof[];
  response: { source: 'patch_response' | 'exact_state_recovery' | 'already_stopped_observation';
    uid: string; resource_version: string; generation: number; body_sha256: string } | null;
  observed_at_unix_ms: number;
  logs: LogProof[];
}
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Lifecycle observation is UNKNOWN.');
  return value as Record<string, unknown>;
}
function integer(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Lifecycle count/generation is UNKNOWN.');
  return value;
}
function text(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0 || value.length > 512 || /[\u0000-\u001f]/.test(value))
    throw new Error('Lifecycle identity/version is UNKNOWN.');
  return value;
}
function origin(inventory: RunInventory) {
  return { campaign_id: inventory.campaign_id, owner_id: inventory.owner_id, fencing_token: inventory.fencing_token,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id, namespace: inventory.namespace };
}
function identity(resource: RunInventoryResource) {
  return { api_version: resource.api_version, kind: resource.kind, name: resource.name, namespace: resource.namespace, uid: resource.uid };
}
const stopBlob = (inventory: RunInventory, field: LifecycleField, resource: RunInventoryResource) =>
  `_arrowcol-custody/campaigns/${inventory.campaign_id}/cells/${inventory.cell_run_id}/lifecycle/${field}-${hash(canonical_json(identity(resource)))}.json`;
function marker(metadata: Record<string, unknown>, inventory: RunInventory) {
  for (const field of ['labels', 'annotations']) {
    const values = object(metadata[field]);
    if (values['arrowcol-oat/run-id'] !== inventory.cell_run_id || values['arrowcol-oat/owner-token'] !== inventory.namespace.owner_token)
      throw new Error('Lifecycle namespace/pod origin marker differs.');
  }
}
function podProof(value: unknown, inventory: RunInventory): PodProof {
  const pod = object(value), metadata = object(pod.metadata), spec = object(pod.spec), status = object(pod.status);
  if (pod.apiVersion !== 'v1' || pod.kind !== 'Pod' || metadata.namespace !== inventory.namespace.name)
    throw new Error('Lifecycle pod type/namespace differs.');
  marker(metadata, inventory);
  const names = new Set<string>(), containers: PodProof['containers'] = [];
  for (const [specField, statusField] of [['containers', 'containerStatuses'],
    ['initContainers', 'initContainerStatuses'], ['ephemeralContainers', 'ephemeralContainerStatuses']] as const) {
    const expected = spec[specField] === undefined ? [] : spec[specField], observed = status[statusField] === undefined ? [] : status[statusField];
    if (!Array.isArray(expected) || !Array.isArray(observed) || expected.length !== observed.length ||
        specField === 'containers' && expected.length === 0)
      throw new Error('Lifecycle expected container coverage is UNKNOWN.');
    const statuses = new Map<string, Record<string, unknown>>();
    for (const value of observed) {
      const container = object(value), name = text(container.name);
      if (statuses.has(name)) throw new Error('Lifecycle container status identity is ambiguous.');
      statuses.set(name, container);
    }
    for (const value of expected) {
      const name = text(object(value).name), container = statuses.get(name);
      if (names.has(name) || !container) throw new Error('Lifecycle expected container status is UNKNOWN.');
      names.add(name);
      const state = object(container.state), terminal = state.terminated === undefined ? undefined : object(state.terminated);
      if (Object.keys(state).length !== 1 || !['running', 'waiting', 'terminated'].includes(Object.keys(state)[0]!))
        throw new Error('Lifecycle container state is UNKNOWN.');
      if (terminal && (typeof terminal.exitCode !== 'number' || !Number.isSafeInteger(terminal.exitCode) || typeof terminal.finishedAt !== 'string' ||
          !Number.isFinite(Date.parse(terminal.finishedAt)))) throw new Error('Actual container termination receipt is UNKNOWN.');
      containers.push({ name, exit_code: terminal ? Number(terminal.exitCode) : null,
        finished_at: terminal ? String(terminal.finishedAt) : null, oom_killed: terminal?.reason === 'OOMKilled',
        restart_count: integer(container.restartCount) });
    }
  }
  if (containers.length > 32) throw new Error('Lifecycle container observation is unbounded.');
  return { name: text(metadata.name), uid: text(metadata.uid), resource_version: text(metadata.resourceVersion),
    containers };
}
function validatePods(value: unknown): asserts value is PodProof[] {
  if (!Array.isArray(value) || value.length > 256) throw new Error('Lifecycle pod journal is unbounded.');
  for (const item of value) {
    const pod = object(item);
    text(pod.name); text(pod.uid); text(pod.resource_version);
    if (!Array.isArray(pod.containers) || pod.containers.length === 0 || pod.containers.length > 32)
      throw new Error('Lifecycle journal container identities are UNKNOWN.');
    for (const item of pod.containers) {
      const container = object(item);
      text(container.name);
      if (container.restart_count !== undefined) integer(container.restart_count);

      if (typeof container.oom_killed !== 'boolean' || !(container.exit_code === null ||
          typeof container.exit_code === 'number' && Number.isSafeInteger(container.exit_code)) ||
          !(container.finished_at === null || typeof container.finished_at === 'string' && Number.isFinite(Date.parse(container.finished_at))) ||
          (container.exit_code === null) !== (container.finished_at === null))
        throw new Error('Lifecycle journal terminal receipt is malformed.');
    }
  }
}
function parseStop(value: unknown, inventory: RunInventory, field: LifecycleField, resource: RunInventoryResource): StopRecord {
  const record = object(value);
  if (Object.keys(record).sort().join() !== ['schema_version', 'origin', 'field', 'resource', 'phase', 'deadline_unix_ms',
    'initial_desired', 'initial_pods', 'terminal_pods', 'response', 'observed_at_unix_ms', 'logs', 'expected_resource_version'].sort().join() ||
      record.schema_version !== 1 || record.field !== field || !['planned', 'intent', 'responded', 'complete'].includes(String(record.phase)) ||
      canonical_json(record.origin) !== canonical_json(origin(inventory)) ||
      canonical_json(record.resource) !== canonical_json(identity(resource)) ||
      record.deadline_unix_ms !== inventory.cleanup[field].deadline_unix_ms)
    throw new Error('Lifecycle journal differs from exact original resource/run/deadline.');
  integer(record.initial_desired); integer(record.observed_at_unix_ms);
  text(record.expected_resource_version);
  validatePods(record.initial_pods); validatePods(record.terminal_pods);
  if (!Array.isArray(record.logs) || record.logs.length > 512) throw new Error('Lifecycle log proof is incomplete.');
  for (const item of record.logs) {
    const log = object(item);
    text(log.pod_name); text(log.pod_uid); text(log.container); integer(log.observed_at_unix_ms); integer(log.bytes);
    if (!/^[a-f0-9]{64}$/.test(String(log.body_sha256)) || !['pre_stop', 'terminal', 'evidence', 'previous'].includes(String(log.phase)) ||
        !Array.isArray(log.numeric_samples) || !Number.isSafeInteger(log.redacted_lines))
      throw new Error('Lifecycle sanitized log proof is malformed.');
    if (log.drain_complete_marker !== undefined && typeof log.drain_complete_marker !== 'boolean')
      throw new Error('Lifecycle drain marker proof is malformed.');
    for (const sample of log.numeric_samples) {
      const values = object(sample);
      if (Object.keys(values).length === 0 || Object.entries(values).some(([key, value]) =>
        !['rows', 'committed_rows', 'committed_bytes', 'pending_rows', 'pending_bytes', 'duration_ms', 'elapsed_ms'].includes(key) ||
        typeof value !== 'number' || !Number.isFinite(value) || value < 0))
        throw new Error('Lifecycle log proof contains unsupported or unsanitized fields.');
    }
  }
  if (['planned', 'intent'].includes(String(record.phase)) ? record.response !== null : !record.response)
    throw new Error('Lifecycle response phase is UNKNOWN.');
  if (record.response) {
    const response = object(record.response);
    if (response.uid !== resource.uid || !/^[a-f0-9]{64}$/.test(String(response.body_sha256)) ||
        !['patch_response', 'exact_state_recovery', 'already_stopped_observation'].includes(String(response.source)) ||
        response.source !== 'patch_response' && field !== 'terminal_capture')
      throw new Error('Lifecycle positive response UID/body differs.');
    text(response.resource_version); integer(response.generation);
  }
  return record as unknown as StopRecord;
}

export function createDefaultLifecycleActions(store: CampaignStore, azure: Azure,
  runCommand: CustodyCommandRunner = runtime.run_tree): LifecycleActions {
  const run: CustodyCommandRunner = (command, timeout, cwd, env, onStderr) =>
    dispatchExternalEffect(processEffectRequest(command), () => runCommand(command, timeout, cwd, env, onStderr));
  const captureLogs = async (server: string, inventory: RunInventory, pods: PodProof[], phase: LogProof['phase'],
    authority: CleanupMutationAuthority): Promise<LogProof[]> => {
    const proofs: LogProof[] = [];
    for (const pod of pods) for (const container of pod.containers)
      for (const previous of container.restart_count ? [false, true] : [false]) {
      await authority.assertCurrent();
      const budget = cleanupBudget(authority), remaining = budget ? budget.deadlineUnixMs - Date.now() : 20_000;
      if (remaining <= 0) throw new Error('Log capture deadline expired before command admission.');
      const response = await run(['kubectl', '--server', server, '--insecure-skip-tls-verify=false',
        '--request-timeout', `${Math.max(1, Math.floor(Math.min(15_000, remaining)))}ms`, 'logs', pod.name,
        '--namespace', inventory.namespace.name, '--container', container.name, '--timestamps=true', '--tail=500', '--limit-bytes=262144',
        ...(previous ? ['--previous'] : [])],
        Math.min(20, remaining / 1000));
      if (response.returncode !== 0 || Buffer.byteLength(response.stdout) > 262_144)
        throw new Error('Exact pod log response is unavailable; never substitute successful empty logs.');
      const samples: LogProof['numeric_samples'] = [];
      let redacted = 0;
      let drainCompleteMarker = false;
      for (const line of response.stdout.split('\n').filter(Boolean)) {
        const start = line.indexOf('{');
        if (start < 0) { redacted++; continue; }
        let value: unknown;
        try { value = JSON.parse(line.slice(start)) as unknown; } catch { redacted++; continue; }
        const allowed = ['rows', 'committed_rows', 'committed_bytes', 'pending_rows', 'pending_bytes', 'duration_ms', 'elapsed_ms'];
        const record = value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
        if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z arrowcol_shutdown $/.test(line.slice(0, start)) &&
            Object.keys(record).length === 1 && record.event === 'drain_complete')
          drainCompleteMarker = true;
        const safe = Object.fromEntries(allowed.filter(key => typeof record[key] === 'number' && Number.isFinite(record[key]) &&
          Number(record[key]) >= 0).map(key => [key, Number(record[key])]));
        if (Object.keys(safe).length) samples.push(safe); else redacted++;
      }
      proofs.push({ pod_name: pod.name, pod_uid: pod.uid, container: container.name, phase: previous ? 'previous' : phase, observed_at_unix_ms: Date.now(),
        body_sha256: hash(response.stdout), bytes: Buffer.byteLength(response.stdout), numeric_samples: samples, redacted_lines: redacted,
        drain_complete_marker: drainCompleteMarker });
    }
    return proofs;
  };
  const current = async (supplied: RunInventory, authority: CleanupMutationAuthority) => {
    const inventory = await assertRunCleanupActor(store, authority), state = (await store.read())!.state;
    validateCampaignState(state);
    if (canonical_json(origin(inventory)) !== canonical_json(origin(supplied))) throw new Error('Lifecycle original run identity changed.');
    const plan = inventory.fic?.plan;
    if (!plan || canonical_json(plan) !== canonical_json(await plan_run_fic(azure, plan.runId, plan.namespace, plan.serviceAccount)))
      throw new Error('Lifecycle requires the original authoritative FIC plan/server.');
    const namespace = await observeExactKubernetes(plan.server, ['namespace', inventory.namespace.name], authority, run);
    if (!namespace || namespace.kind !== 'Namespace' || namespace.apiVersion !== 'v1' ||
        object(namespace.metadata).uid !== inventory.namespace.uid) throw new Error('Lifecycle exact namespace UID is UNKNOWN.');
    marker(object(namespace.metadata), inventory);
    return { inventory, plan, state };
  };
  const read = async (blob: string, authority: CleanupMutationAuthority) => {
    await authority.assertCurrent();
    const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, blob, undefined, undefined, {}, true, true);
    if (bytes === null) return undefined;
    if (bytes.length > 1_048_576 || !headers.etag || headers.etag.startsWith('W/')) throw new Error('Lifecycle journal version/size is UNKNOWN.');
    return { value: JSON.parse(bytes.toString()) as unknown, etag: headers.etag };
  };
  const write = async (blob: string, value: unknown, authority: CleanupMutationAuthority, expected?: string) => {
    await authority.assertCurrent();
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, blob, undefined, canonical_json(value),
      { 'x-ms-blob-type': 'BlockBlob', ...(expected ? { 'If-Match': expected } : { 'If-None-Match': '*' }) });
    const confirmed = await read(blob, authority);
    if (headers[':status'] !== '201' || !headers.etag || confirmed?.etag !== headers.etag ||
        canonical_json(confirmed.value) !== canonical_json(value)) throw new Error('Lifecycle journal write outcome UNKNOWN; never replay effects.');
    return headers.etag;
  };
  const observe = async (inventory: RunInventory, resource: RunInventoryResource, authority: CleanupMutationAuthority) => {
    const { plan } = await current(inventory, authority);
    const deployment = resource.api_version === 'apps/v1' && resource.kind === 'Deployment';
    const job = resource.api_version === 'batch/v1' && resource.kind === 'Job';
    if ((!deployment && !job) || !resource.uid)
      throw new Error('Lifecycle stop requires an exact UID-bound Deployment or Job; other kinds remain UNKNOWN.');
    const value = await observeExactKubernetes(plan.server, [deployment ? 'deployment.apps' : 'job.batch', resource.name, '--namespace', resource.namespace], authority, run);
    if (!value || value.apiVersion !== resource.api_version || value.kind !== resource.kind ||
        object(value.metadata).uid !== resource.uid || object(value.metadata).namespace !== resource.namespace ||
        object(value.metadata).name !== resource.name) throw new Error('Original lifecycle Deployment UID/type/name differs.');
    const spec = object(value.spec), selector = object(spec.selector), labels = object(selector.matchLabels);
    if (selector.matchExpressions !== undefined || Object.keys(labels).length === 0 ||
        Object.entries(labels).some(([key, value]) => !/^[A-Za-z0-9_.\/-]+$/.test(key) ||
          typeof value !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(value)))
      throw new Error('Lifecycle requires an exact nonempty label selector without unsupported expressions.');
    const selected = Object.entries(labels).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => key + '=' + value).join(',');
    const replicaUids = new Set<string>();
    if (deployment) {
      const replicas = await observeExactKubernetes(plan.server,
        ['replicaset.apps', '--namespace', resource.namespace, '--selector', selected], authority, run);
      if (!replicas || replicas.kind !== 'ReplicaSetList' || !Array.isArray(replicas.items)) throw new Error('Exact ReplicaSet list is UNKNOWN.');
      text(object(replicas.metadata).resourceVersion);
      for (const item of replicas.items) {
        const metadata = object(object(item).metadata);
        if (!Array.isArray(metadata.ownerReferences) || !metadata.ownerReferences.some(value => {
          const owner = object(value); return owner.uid === resource.uid && owner.kind === 'Deployment' && owner.controller === true;
        })) throw new Error('Selected ReplicaSet belongs to another original Deployment.');
        replicaUids.add(text(metadata.uid));
      }
    } else replicaUids.add(resource.uid);
    const list = await observeExactKubernetes(plan.server, ['pods', '--namespace', resource.namespace, '--selector', selected], authority, run);
    if (!list || list.kind !== 'PodList' || !Array.isArray(list.items)) throw new Error('Exact owned PodList is UNKNOWN.');
    text(object(list.metadata).resourceVersion);
    const pods = list.items.map(item => {
      const metadata = object(object(item).metadata);
      if (!Array.isArray(metadata.ownerReferences) || !metadata.ownerReferences.some(value => {
        const owner = object(value); return replicaUids.has(String(owner.uid)) && owner.kind === (deployment ? 'ReplicaSet' : 'Job') && owner.controller === true;
      })) throw new Error('Selected pod belongs to another controller.');
      return podProof(item, inventory);
    });
    if (deployment) {
      const autoscalers = await observeExactKubernetes(plan.server, ['horizontalpodautoscalers.autoscaling', '--namespace', resource.namespace], authority, run);
      if (!autoscalers || autoscalers.kind !== 'HorizontalPodAutoscalerList' || !Array.isArray(autoscalers.items))
        throw new Error('Lifecycle recreation policy observation is UNKNOWN.');
      text(object(autoscalers.metadata).resourceVersion);
      if (autoscalers.items.some(value => {
        const target = object(object(object(value).spec).scaleTargetRef);
        return target.apiVersion === resource.api_version && target.kind === resource.kind && target.name === resource.name;
      })) throw new Error('An autoscaler can recreate this exact workload; no unowned autoscaler mutation is authorized.');
    }
    return { value, spec, pods, plan, deployment };
  };
  const stop = async (inventory: RunInventory, resource: RunInventoryResource, field: LifecycleField,
    authority: CleanupMutationAuthority, recoveryOnly = false): Promise<CleanupActionResult> => {
    const evidence: CleanupMutationAuthority = { mode: authority.mode, campaignId: authority.campaignId,
      ownerId: authority.ownerId, fencingToken: authority.fencingToken, assertCurrent: () => authority.assertEvidenceCurrent ?
        authority.assertEvidenceCurrent() : authority.assertCurrent() };
    const blob = stopBlob(inventory, field, resource);
    let saved = await read(blob, evidence), record = saved ? parseStop(saved.value, inventory, field, resource) : undefined;
    if (!record || record.phase === 'planned') {
      if (!record && recoveryOnly) throw new Error('Lifecycle recovery has no durable per-resource intent/response; never execute as fallback.');
      const deadline = inventory.cleanup[field].deadline_unix_ms;
      const bounded = withCleanupDeadline(authority, deadline);
      const observation = await observe(inventory, resource, bounded), metadata = object(observation.value.metadata);
      const logs = await captureLogs(observation.plan.server, inventory, observation.pods, 'pre_stop', bounded);
      if (field === 'terminal_capture' && physicallyStopped(observation)) {
        const proof: StopRecord = { schema_version: 1, origin: origin(inventory), field, resource: identity(resource),
          phase: 'complete', deadline_unix_ms: bounded.budget.deadlineUnixMs, initial_desired: 0,
          expected_resource_version: text(metadata.resourceVersion),
          initial_pods: observation.pods, terminal_pods: observation.pods, logs, observed_at_unix_ms: Date.now(),
          response: { source: 'already_stopped_observation', uid: resource.uid!, resource_version: text(metadata.resourceVersion),
            generation: integer(metadata.generation), body_sha256: hash(canonical_json(observation.value)) } };
        await write(blob, proof, bounded, saved?.etag);
        return 'complete';
      }
      record = { schema_version: 1, origin: origin(inventory), field, resource: identity(resource), phase: 'intent',
        deadline_unix_ms: bounded.budget.deadlineUnixMs,
        expected_resource_version: text(metadata.resourceVersion),
        initial_desired: integer(observation.deployment ? observation.spec.replicas : observation.spec.parallelism), initial_pods: observation.pods, terminal_pods: [],
        response: null, observed_at_unix_ms: Date.now(), logs };
      const etag = await write(blob, record, bounded, saved?.etag);
      await bounded.assertCurrent();
      const remaining = cleanupBudget(bounded)!.deadlineUnixMs - Date.now();
      if (remaining <= 0) throw new CleanupDeadlineExpiredError('Lifecycle stop deadline expired before PATCH admission.');
      const patch = [{ op: 'test', path: '/metadata/uid', value: resource.uid },
        { op: 'test', path: '/metadata/resourceVersion', value: text(metadata.resourceVersion) },
        { op: 'add', path: observation.deployment ? '/spec/replicas' : '/spec/suspend', value: observation.deployment ? 0 : true }];
      const response = await run(['kubectl', '--server', observation.plan.server, '--insecure-skip-tls-verify=false',
        '--request-timeout', `${Math.max(1, Math.floor(Math.min(15_000, remaining)))}ms`,
        'patch', observation.deployment ? 'deployment.apps' : 'job.batch', resource.name, '--namespace', resource.namespace,
        '--type=json', '--patch', canonical_json(patch), '--output=json'], Math.min(20, remaining / 1000));
      if (response.returncode !== 0 || response.stdout.length > 1_048_576) throw new Error('Lifecycle PATCH outcome UNKNOWN; never replay.');
      const body = object(JSON.parse(response.stdout) as unknown), returned = object(body.metadata);
      if (body.apiVersion !== resource.api_version || body.kind !== resource.kind || returned.uid !== resource.uid ||
          returned.name !== resource.name || returned.namespace !== resource.namespace ||
          (observation.deployment ? object(body.spec).replicas !== 0 : object(body.spec).suspend !== true))
        throw new Error('Lifecycle PATCH lacks an exact positive typed response.');
      record = { ...record, phase: 'responded', response: { source: 'patch_response', uid: text(returned.uid), resource_version: text(returned.resourceVersion),
        generation: integer(returned.generation), body_sha256: hash(response.stdout) } };
      saved = { value: record, etag: await write(blob, record, evidence, etag) };
    } else if (record.phase === 'intent') {
      if (field !== 'terminal_capture') throw new Error('Lifecycle PATCH outcome UNKNOWN; no durable positive response, never replay or infer from absence.');
      const observed = await observe(inventory, resource, withCleanupDeadline(authority, record.deadline_unix_ms));
      if (!physicallyStopped(observed)) throw new Error('Residual collector PATCH outcome UNKNOWN; no positive exact stopped state, never replay.');
      const metadata = object(observed.value.metadata);
      record = { ...record, phase: 'complete', observed_at_unix_ms: Date.now(), terminal_pods: observed.pods,
        response: { source: 'exact_state_recovery', uid: text(metadata.uid), resource_version: text(metadata.resourceVersion),
          generation: integer(metadata.generation), body_sha256: hash(canonical_json(observed.value)) } };
      await write(blob, record, withCleanupDeadline(authority, record.deadline_unix_ms), saved!.etag);
      return 'complete';
    }
    if (record.phase === 'complete') {
      if (!physicallyStopped(await observe(inventory, resource, evidence)))
        throw new Error('Workload is no longer physically stopped; never reuse stale lifecycle success.');
      return 'complete';
    }
    do {
      if (Date.now() >= record.deadline_unix_ms) return 'pending';
      const observation = await observe(inventory, resource, withCleanupDeadline(authority, record.deadline_unix_ms));
      const active: StopRecord = record;
      const terminal: Map<string, PodProof> = new Map(active.terminal_pods.map(pod => [pod.uid, pod]));
      const newlyTerminal = observation.pods.filter(pod => pod.containers.every(container => container.exit_code !== null) && !terminal.has(pod.uid));
      for (const pod of newlyTerminal)
        terminal.set(pod.uid, pod);
      if (observation.pods.some(pod => !active.initial_pods.some(initial => initial.uid === pod.uid)))
        throw new Error('The original workload recreated or replaced a pod after its stop intent.');
      const stopped: boolean = physicallyStopped(observation);
      const cleanDrain: boolean = field !== 'bounded_drain' || active.initial_pods.length === 0 && active.initial_desired === 0 ||
        active.initial_pods.length > 0 && active.initial_pods.every(pod => {
          const ended = terminal.get(pod.uid);
          return ended && ended.containers.length === pod.containers.length &&
            ended.containers.every(container => container.exit_code === 0 && !container.oom_killed);
        });
      record = { ...record, terminal_pods: [...terminal.values()], observed_at_unix_ms: Date.now(),
        logs: [...record.logs, ...await captureLogs(observation.plan.server, inventory, newlyTerminal, 'terminal', evidence)],
        phase: stopped && cleanDrain && Date.now() < active.deadline_unix_ms ? 'complete' : 'responded' };
      saved = { value: record, etag: await write(blob, record, evidence, saved!.etag) };
      if (record.phase === 'complete') return 'complete';
      if (Date.now() >= record.deadline_unix_ms) return 'pending';
      await runtime.sleep(Math.min(1, (record.deadline_unix_ms - Date.now()) / 1000));
    } while (true);
  };
  const physicallyStopped = (observation: Awaited<ReturnType<typeof observe>>) => {
    const status = object(observation.value.status);
    return observation.pods.every(pod => pod.containers.every(container => container.exit_code !== null)) && (observation.deployment ?
      observation.spec.replicas === 0 && integer(status.observedGeneration) >= integer(object(observation.value.metadata).generation) :
      observation.spec.suspend === true && Array.isArray(status.conditions) && status.conditions.some(value => {
        const condition = object(value); return ['Suspended', 'Complete', 'Failed'].includes(String(condition.type)) && condition.status === 'True';
      }));
  };
  const stopRoles = async (supplied: RunInventory, roles: RunInventoryResource['role'][], field: LifecycleField,
    authority: CleanupMutationAuthority, recoveryOnly = false): Promise<CleanupActionResult> => {
    const { inventory } = await current(supplied, authority);
    for (const role of roles) for (const resource of inventory.resources.filter(resource => resource.role === role)) {
      if (resource.state === 'absent' || resource.state === 'removed') continue;
      if (resource.state !== 'observed' || !resource.uid) throw new Error('Lifecycle workload identity is unresolved.');
      if (await stop(inventory, resource, field, authority, recoveryOnly) !== 'complete') return 'pending';
    }
    return 'complete';
  };
  const capture = async (supplied: RunInventory, field: 'pre_stop_capture' | 'evidence_capture' | 'terminal_capture', authority: CleanupMutationAuthority,
    recoveryOnly = false) => {
    if (field === 'pre_stop_capture') authority = withCleanupDeadline(authority, supplied.cleanup.bounded_drain.deadline_unix_ms);
    if (field === 'terminal_capture') authority = withCleanupDeadline(authority, supplied.cleanup.terminal_capture.deadline_unix_ms);
    const { inventory, state } = await current(supplied, authority), destination = inventory.evidence_destination;
    if (!destination || destination.account !== azure.c.account || destination.container !== azure.c.checkpoints)
      throw new Error('Lifecycle evidence requires the exact persisted selected-stamp checkpoints destination.');
    if (field === 'terminal_capture' && hasVerifiedUnsettledDrain(state)) {
      if (authority.mode !== 'controller' || state.status !== 'cleaning' || inventory.cleanup.stop_admission.state !== 'complete' ||
          inventory.cleanup.stop_maintenance.state !== 'complete')
        throw new Error('Residual collector stop requires current fenced unsettled authority and completed admission/maintenance stops.');
      const collectors = inventory.resources.filter(value => value.role === 'collector' && value.state !== 'absent');
      if (!recoveryOnly) for (const resource of collectors) {
        const blob = stopBlob(inventory, 'terminal_capture', resource);
        if (await read(blob, authority)) continue;
        const observation = await observe(inventory, resource, authority), metadata = object(observation.value.metadata);
        const planned: StopRecord = { schema_version: 1, origin: origin(inventory), field, resource: identity(resource),
          phase: 'planned', deadline_unix_ms: inventory.cleanup.terminal_capture.deadline_unix_ms!,
          expected_resource_version: text(metadata.resourceVersion),
          initial_desired: integer(observation.deployment ? observation.spec.replicas : observation.spec.parallelism),
          initial_pods: observation.pods, terminal_pods: [], response: null, logs: [], observed_at_unix_ms: Date.now() };
        await write(blob, planned, authority);
      }
      for (const resource of collectors) {
        if (resource.state !== 'observed' || !resource.uid) throw new Error('Residual collector exact identity is UNKNOWN.');
        if (await stop(inventory, resource, 'terminal_capture', authority, recoveryOnly) !== 'complete') return 'pending' as const;
      }
    }
    const blob = destination.blob_prefix.replace(/\/$/, '') + `/custody/${inventory.cell_run_id}/${field}.json`;
    const prior = await read(blob, authority);
    if (prior) {
      const value = object(prior.value);
      if (value.schema_version !== 1 || value.field !== field || canonical_json(value.origin) !== canonical_json(origin(inventory)) ||
          value.scope !== 'exact_kubernetes_status_and_sanitized_log_receipts' || !Array.isArray(value.workloads))
        throw new Error('Lifecycle evidence belongs to another original run.');
      if (field === 'terminal_capture') await freezeTerminal(inventory, authority);
      return 'complete' as const;
    }
    const workloads = [];
    for (const resource of inventory.resources) {
      if (field === 'pre_stop_capture' && resource.role !== 'collector') continue;
      if (resource.state === 'absent') continue;
      if (resource.state !== 'observed' || !resource.uid) throw new Error('Evidence workload identity is unresolved.');
      const observed = await observe(inventory, resource, authority);
      const metadata = object(observed.value.metadata);
      const stopField = resource.role === 'maintenance' ? 'stop_maintenance' : resource.role === 'collector' ? 'bounded_drain' : 'stop_admission';
      const savedStop = await read(stopBlob(inventory, stopField, resource), authority);
      const stop = savedStop ? parseStop(savedStop.value, inventory, stopField, resource) : null;
      const logs = await captureLogs(observed.plan.server, inventory, observed.pods, field === 'terminal_capture' ? 'terminal' : 'evidence', authority);
      const terminalSamples = field === 'terminal_capture' && resource.role === 'collector' && stop ?
        stop.initial_pods.map(pod => {
          const ended = stop.terminal_pods.find(value => value.uid === pod.uid && value.name === pod.name);
          return {
            pod_name: pod.name, pod_uid: pod.uid,
            terminal_sample: ended ? 'observed' :
              observed.pods.some(current => current.uid === pod.uid) ? 'unobserved' : 'pod_gone',
            containers: pod.containers.map(initial => {
              const terminal = ended?.containers.find(value => value.name === initial.name);
              const sameInstance = terminal && initial.restart_count !== undefined &&
                terminal.restart_count === initial.restart_count;
              const marker = sameInstance && stop.logs.some(log => log.phase === 'terminal' &&
                log.pod_uid === pod.uid && log.pod_name === pod.name && log.container === initial.name &&
                log.drain_complete_marker === true);
              const classification = !sameInstance || terminal.exit_code === null ? 'terminal_status_unavailable' :
                terminal.oom_killed ? 'exit_oom_killed' :
                terminal.exit_code === 0 ? 'exit_0' :
                terminal.exit_code === 143 ? marker ? 'sigterm_after_drain_pending_reconciliation' : 'exit_143_without_drain_marker' :
                'exit_nonzero';
              return { container: initial.name, restart_count: initial.restart_count ?? null,
                exit_code: terminal?.exit_code ?? null, classification };
            }),
          };
        }) : null;
      const podGone = terminalSamples?.some(sample => sample.terminal_sample === 'pod_gone') ?? false;
      workloads.push({ identity: identity(resource), resource_version: text(metadata.resourceVersion),
        desired_instances: integer(observed.deployment ? observed.spec.replicas : observed.spec.parallelism), pods: observed.pods,
        current_logs: logs, original_stop: stop, terminal_samples: terminalSamples,
        log_custody: podGone ? 'incomplete' : 'unavailable',
        log_metrics: podGone ? 'provisional' : 'unavailable',
        missing_terminal_reason: !stop ? 'no_positive_original_stop_record' :
          stop.initial_pods.some(pod => !stop.terminal_pods.some(terminal => terminal.uid === pod.uid)) ?
            'original_pod_disappeared_before_terminal_observation' : null });
    }
    await write(blob, { schema_version: 1, field, origin: origin(inventory), observed_at_unix_ms: Date.now(),
      bounded_drain: inventory.cleanup.bounded_drain, unsettled: hasVerifiedUnsettledDrain(state),
      ...(field === 'terminal_capture' ? { drain_timing: {
        budget_ms: inventory.drain_budget?.budget_ms ?? null,
        collector_grace_ms: inventory.drain_budget?.collector_grace_ms ?? null,
        pre_stop_capture_margin_ms: inventory.drain_budget?.pre_stop_capture_margin_ms ?? null,
        actual_drain_duration_ms: null,
        actual_drain_started_at_unix_ms: null,
        actual_drain_finished_at_unix_ms: null,
        actual_drain_source: null,
        unavailable_reason: 'collector_sigterm_timestamp_unavailable',
      } } : {}),
      workloads, scope: 'exact_kubernetes_status_and_sanitized_log_receipts' }, authority);
    if (field === 'terminal_capture') await freezeTerminal(inventory, authority);
    return 'complete' as const;
  };
  const assertWritersStopped = async (inventory: RunInventory, authority: CleanupMutationAuthority) => {
    for (const resource of inventory.resources.filter(value => ['collector', 'maintenance'].includes(value.role))) {
      if (resource.state === 'absent') continue;
      const observed = await observe(inventory, resource, authority);
      if (!physicallyStopped(observed))
        throw new Error('Terminal Delta freeze requires positive physical writer stop with recreation disabled.');
    }
  };
  const freezeTerminal = async (inventory: RunInventory, authority: CleanupMutationAuthority) => {
    await assertWritersStopped(inventory, authority);
    if (inventory.qap === null) {
      const current = (await store.read())!.state;
      if (authority.mode !== 'controller' || current.status !== 'cleaning' || inventory.fencing_token >= authority.fencingToken ||
          inventory.load_admission !== 'not_authorized')
        throw new Error('No QAP/reference is not proof of no load; missing/pending/authorized remains UNKNOWN.');
      return;
    }
    await captureTerminalData(store, azure, authority, () => assertWritersStopped(inventory, authority));
  };
  return {
    stopAdmission: (inventory, _deadline, _key, authority) => stopRoles(inventory, ['client', 'router'], 'stop_admission', authority),
    boundedDrain: async (inventory, _deadline, _key, authority) => {
      if (await capture(inventory, 'pre_stop_capture', authority) !== 'complete') return 'pending';
      return stopRoles(inventory, ['collector'], 'bounded_drain', authority);
    },
    stopMaintenanceIfUidAndOwnerMatch: async (resource, owner, _key, authority) => {
      const inventory = await assertRunCleanupActor(store, authority);
      if (owner !== inventory.namespace.owner_token || !inventory.resources.some(value =>
        canonical_json(value) === canonical_json(resource))) throw new Error('Maintenance exact workload/origin differs.');
      await current(inventory, authority);
      return stop(inventory, resource, 'stop_maintenance', authority);
    },
    preserveEvidence: (inventory, _key, authority) => capture(inventory, 'evidence_capture', authority),
    captureTerminalState: (inventory, _key, authority) => capture(inventory, 'terminal_capture', authority),
    recoverLifecycle: async (field, inventory, _deadline, _key, authority) => {
      if (field === 'evidence_capture' || field === 'terminal_capture') return capture(inventory, field, authority, true);
      return stopRoles(inventory, field === 'stop_admission' ? ['client', 'router'] :
        field === 'bounded_drain' ? ['collector'] : ['maintenance'], field, authority, true);
    },
  };
}
