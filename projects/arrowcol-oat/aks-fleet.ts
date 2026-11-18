import { setTimeout as delay } from 'node:timers/promises';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import type { FleetNodeObservation, FleetPoolOutcome } from './fleet-config.js';
import { cpuMilli as parseCpuMilli, memoryBytes as parseMemoryBytes } from './runtime.js';
export type { FleetNodeObservation, FleetPoolOutcome } from './fleet-config.js';

export interface FleetPoolRequest {
  pool: string;
  sku: string;
  vcpu: number;
  count?: number;
}

export interface FleetPoolSnapshot {
  name: string;
  sku: string;
  vcpu: number;
  count: number;
  provisioningState: string;
}

export interface AksNodeInventory {
  node: string;
  pool: string;
  role: string;
  observedSku: string;
  skuLabelsConsistent: boolean;
  architecture: string;
  ready: boolean;
  amd64: boolean;
  placementLabelsValid: boolean;
  allocatable_cpu_millicores: number;
  allocatable_memory_bytes: number;
  requested_cpu_millicores: number;
  requested_memory_bytes: number;
}

export type FleetScaleRejection = 'quota_exceeded' | 'allocation_failed' | 'sku_capacity_unavailable' | 'request_rejected';

export type FleetScaleResult =
  | { state: 'accepted' }
  | { state: 'conflict'; status: 409; code: 'OperationNotAllowed' }
  | { state: 'rejected'; reason: FleetScaleRejection }
  | { state: 'uncertain' };

export interface AksFleetAdapter {
  listUserPools(authority: CleanupMutationAuthority): Promise<string[]>;
  readPool(name: string, authority: CleanupMutationAuthority): Promise<FleetPoolSnapshot>;
  scalePool(name: string, count: number, authority: CleanupMutationAuthority): Promise<FleetScaleResult>;
  waitForPool(name: string, count: number, deadlineAtMs: number,
    authority: CleanupMutationAuthority, signal?: AbortSignal): Promise<FleetPoolSnapshot>;
  listNodes(authority: CleanupMutationAuthority): Promise<unknown>;
  listPods(authority: CleanupMutationAuthority): Promise<unknown>;
}

export interface AksFleetRequest {
  enabled: boolean;
  collector: FleetPoolRequest;
  clients: FleetPoolRequest[];
  collectorSupportReservation: { cpu_millicores: number; memory_bytes: number };
  deadlineAtMs: number;
  cleanupDeadlineAtMs: number;
  maxConflictRetries?: number;
  now?: () => number;
  signal?: AbortSignal;
}

export interface AksFleetResult<T> {
  resolved: T | null;
  nodes: FleetNodeObservation[];
  pools: FleetPoolOutcome[];
  cleanup: { verified: boolean; failed: string[] };
}

export interface AksFleetCleanupOptions {
  deadlineAtMs: number;
  includePoolNames?: string[];
  expectedScaleCounts?: Readonly<Record<string, number>>;
  maxConflictRetries?: number;
  now?: () => number;
}

export interface AksFleetCleanupResult {
  verified: boolean;
  failed: string[];
}

const MAX_CLIENTS = 128;
const MAX_PARALLEL_SCALES = 8;
const DEFAULT_CONFLICT_RETRIES = 3;
const NODE_READY_POLL_MS = 500;
const SCALE_CONFLICT_BACKOFF_MAX_MS = 15_000;
const READY = 'Succeeded';
const SETTLED = new Set(['Succeeded', 'Failed', 'Canceled']);
const REJECTION_REASONS = new Set<FleetScaleRejection>([
  'quota_exceeded', 'allocation_failed', 'sku_capacity_unavailable', 'request_rejected',
]);

function object(value: unknown, label = 'AKS fleet node inventory'): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(label + ' is invalid.');
  return value as Record<string, unknown>;
}

function positiveInt(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) throw new Error(label + ' must be a positive safe integer.');
  return value as number;
}

function assertNotAborted(signal?: AbortSignal): void {
  if (!signal?.aborted) return;
  const error = new Error('AKS fleet startup was aborted.');
  error.name = 'AbortError';
  throw error;
}

async function cancellableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (!signal) {
    await delay(milliseconds);
    return;
  }
  assertNotAborted(signal);
  try {
    await delay(milliseconds, undefined, { signal });
  } catch (error) {
    assertNotAborted(signal);
    throw error;
  }
}

function cpuMilli(value: unknown, allowZero = false): number {
  if (allowZero && value === undefined) return 0;
  if (allowZero && (value === '0' || value === '0m')) return 0;
  const amount = parseCpuMilli(value);
  if (amount > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('Node CPU allocatable is invalid.');
  return Number(amount);
}

function memoryBytes(value: unknown, allowZero = false): number {
  if (allowZero && value === undefined) return 0;
  if (allowZero && value === '0') return 0;
  const bytes = parseMemoryBytes(value);
  if (bytes > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('Node memory allocatable is invalid.');
  return Number(bytes);
}

function containerRequest(containers: unknown): { cpu: number; memory: number } {
  if (!Array.isArray(containers)) return { cpu: 0, memory: 0 };
  return containers.reduce<{ cpu: number; memory: number }>((sum, raw) => {
    const container = object(raw, 'pod container'), resources = object(container.resources ?? {}, 'pod container resources');
    const requests = object(resources.requests ?? {}, 'pod container requests');
    sum.cpu += cpuMilli(requests.cpu, true);
    sum.memory += memoryBytes(requests.memory, true);
    return sum;
  }, { cpu: 0, memory: 0 });
}

function podRequests(pod: Record<string, unknown>): { cpu: number; memory: number } {
  const spec = object(pod.spec ?? {}, 'pod spec');
  let app = containerRequest(spec.containers), initPeak = { cpu: 0, memory: 0 }, sidecars = { cpu: 0, memory: 0 };
  if (Array.isArray(spec.initContainers)) {
    for (const raw of spec.initContainers) {
      const request = containerRequest([raw]), init = object(raw, 'pod init container');
      if (init.restartPolicy === 'Always') {
        sidecars.cpu += request.cpu;
        sidecars.memory += request.memory;
        initPeak = { cpu: Math.max(initPeak.cpu, sidecars.cpu), memory: Math.max(initPeak.memory, sidecars.memory) };
      } else {
        initPeak = { cpu: Math.max(initPeak.cpu, sidecars.cpu + request.cpu),
          memory: Math.max(initPeak.memory, sidecars.memory + request.memory) };
      }
    }
  }
  app = { cpu: app.cpu + sidecars.cpu, memory: app.memory + sidecars.memory };
  const overhead = object(spec.overhead ?? {}, 'pod overhead');
  return {
    cpu: Math.max(app.cpu, initPeak.cpu) + cpuMilli(overhead.cpu, true),
    memory: Math.max(app.memory, initPeak.memory) + memoryBytes(overhead.memory, true),
  };
}

export function parseAksNodeInventory(input: unknown, podInput: unknown = { items: [] }): AksNodeInventory[] {
  const list = object(input, 'node list');
  if (!Array.isArray(list.items)) throw new Error('AKS fleet node inventory has no item list.');
  const pods = object(podInput, 'pod list');
  if (!Array.isArray(pods.items)) throw new Error('AKS fleet pod inventory has no item list.');
  const requested = new Map<string, { cpu: number; memory: number }>();
  for (const raw of pods.items) {
    const pod = object(raw, 'pod'), spec = object(pod.spec ?? {}, 'pod spec');
    if (typeof spec.nodeName !== 'string' || ['Succeeded', 'Failed'].includes(String(object(pod.status ?? {}).phase))) continue;
    const total = requested.get(spec.nodeName) ?? { cpu: 0, memory: 0 }, podTotal = podRequests(pod);
    total.cpu += podTotal.cpu;
    total.memory += podTotal.memory;
    if (!Number.isSafeInteger(total.cpu) || !Number.isSafeInteger(total.memory)) throw new Error('Pod requests exceed supported inventory bounds.');
    requested.set(spec.nodeName, total);
  }
  return list.items.map((raw: unknown) => {
    const node = object(raw), metadata = object(node.metadata), labels = object(metadata.labels ?? {});
    const spec = object(node.spec ?? {}, 'node spec'), status = object(node.status ?? {}, 'node status');
    const allocatable = object(status.allocatable ?? {}, 'node allocatable');
    const nodeInfo = object(status.nodeInfo ?? {}, 'node status.nodeInfo');
    const conditions = Array.isArray(status.conditions) ? status.conditions : [];
    const demand = requested.get(String(metadata.name)) ?? { cpu: 0, memory: 0 };
    const name = typeof metadata.name === 'string' ? metadata.name : '';
    const role = typeof labels['arrow-collector/role'] === 'string' ? labels['arrow-collector/role'] : '';
    const labeledSku = typeof labels['arrow-collector/sku'] === 'string' ? labels['arrow-collector/sku'] : '';
    const instanceType = typeof labels['node.kubernetes.io/instance-type'] === 'string'
      ? labels['node.kubernetes.io/instance-type'] : '';
    const architecture = typeof nodeInfo.architecture === 'string' ? nodeInfo.architecture : '';
    const skuLabelsConsistent = !labeledSku || !instanceType || labeledSku === instanceType;
    return {
      node: name,
      pool: typeof labels['kubernetes.azure.com/agentpool'] === 'string' ? labels['kubernetes.azure.com/agentpool'] : '',
      role,
      observedSku: labeledSku || instanceType,
      skuLabelsConsistent,
      architecture,
      ready: conditions.some((condition: unknown) => {
        const item = object(condition);
        return item.type === 'Ready' && item.status === 'True';
      }) && spec.unschedulable !== true && metadata.deletionTimestamp == null,
      amd64: architecture === 'amd64' && labels['kubernetes.io/arch'] === 'amd64',
      placementLabelsValid: labels['kubernetes.io/hostname'] === name,
      allocatable_cpu_millicores: cpuMilli(allocatable.cpu, true),
      allocatable_memory_bytes: memoryBytes(allocatable.memory, true),
      requested_cpu_millicores: demand.cpu,
      requested_memory_bytes: demand.memory,
    };
  });
}

function validateRequest(request: AksFleetRequest, now: number): void {
  if (request.enabled !== true) throw new Error('AKS fleet placement is disabled; enable the explicit fleet CLI switch to proceed.');
  if (!Number.isSafeInteger(request.deadlineAtMs) || !Number.isSafeInteger(request.cleanupDeadlineAtMs) ||
      request.deadlineAtMs <= now || request.cleanupDeadlineAtMs <= request.deadlineAtMs)
    throw new Error('AKS fleet requires distinct bounded scale and cleanup deadlines.');
  if (!Array.isArray(request.clients) || request.clients.length < 1 || request.clients.length > MAX_CLIENTS)
    throw new Error('AKS fleet requires 1 to 128 client pools.');
  const poolNames = [request.collector.pool, ...request.clients.map(client => client.pool)];
  if (new Set(poolNames).size !== poolNames.length || poolNames.some(name => !/^[a-z0-9][a-z0-9-]{0,11}$/.test(name)))
    throw new Error('AKS fleet pool names must be distinct Kubernetes-compatible names.');
  if ((request.collector.count ?? 1) !== 1) throw new Error('AKS fleet requires exactly one collector node.');
  const clientNodes = request.clients.reduce((sum, pool) => sum + (pool.count ?? 1), 0);
  if (!Number.isSafeInteger(clientNodes) || clientNodes < 1 || clientNodes > MAX_CLIENTS)
    throw new Error('AKS fleet requires 1 to 128 client nodes.');
  for (const [label, pool] of [['collector', request.collector] as const, ...request.clients.map((client, index) =>
    [`client ${index}`, client] as const)]) {
    if (!/^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/.test(pool.sku)) throw new Error(label + ' has an invalid VM SKU.');
    positiveInt(pool.vcpu, label + ' vCPU');
    positiveInt(pool.count ?? 1, label + ' node count');
  }
  if (!Number.isSafeInteger(request.collectorSupportReservation.cpu_millicores) || request.collectorSupportReservation.cpu_millicores < 0 ||
      !Number.isSafeInteger(request.collectorSupportReservation.memory_bytes) || request.collectorSupportReservation.memory_bytes < 0)
    throw new Error('Collector support reservation must be nonnegative integer CPU and memory quantities.');
}

function outcome(request: FleetPoolRequest, snapshot: FleetPoolSnapshot | undefined, state: FleetPoolOutcome['state'],
  reason: string | null): FleetPoolOutcome {
  return { name: request.pool, sku: snapshot?.sku ?? request.sku, vcpu: snapshot?.vcpu ?? request.vcpu, state, reason };
}

function checkSnapshot(snapshot: FleetPoolSnapshot, expected: FleetPoolRequest): void {
  if (snapshot.name !== expected.pool || snapshot.sku !== expected.sku || snapshot.vcpu !== expected.vcpu ||
      !Number.isSafeInteger(snapshot.count) || snapshot.count < 0 || typeof snapshot.provisioningState !== 'string')
    throw new Error('Pool identity or configuration differs from the fleet request.');
}

function settled(snapshot: FleetPoolSnapshot): boolean {
  return SETTLED.has(snapshot.provisioningState);
}

function succeeded(snapshot: FleetPoolSnapshot): boolean {
  return snapshot.provisioningState === READY;
}

async function assertAuthority(authority: CleanupMutationAuthority): Promise<void> {
  await authority.assertCurrent();
}

function explicitConflict(result: FleetScaleResult): boolean {
  return result.state === 'conflict' && result.status === 409 && result.code === 'OperationNotAllowed';
}

function sanitizedRejectionReason(reason: unknown): FleetScaleRejection {
  return typeof reason === 'string' && REJECTION_REASONS.has(reason as FleetScaleRejection)
    ? reason as FleetScaleRejection
    : 'request_rejected';
}

function targetCount(request: FleetPoolRequest): number {
  return request.count ?? 1;
}

async function retryBackoff(pool: string, attempt: number, deadlineAtMs: number, now: () => number,
  signal?: AbortSignal): Promise<boolean> {
  const hash = [...pool].reduce((value, char) => (value * 31 + char.charCodeAt(0)) >>> 0, 0);
  const waitMs = Math.min(SCALE_CONFLICT_BACKOFF_MAX_MS, 1_000 * 2 ** Math.min(attempt, 4)) + hash % 500;
  const remaining = deadlineAtMs - now();
  if (remaining <= 0) return false;
  await cancellableDelay(Math.min(waitMs, remaining), signal);
  return now() < deadlineAtMs;
}

interface FleetArmGate {
  held: boolean;
}

async function withFleetArmGate<T>(gate: FleetArmGate, deadlineAtMs: number, now: () => number,
  authority: CleanupMutationAuthority, signal: AbortSignal | undefined,
  action: () => Promise<T>): Promise<{ acquired: boolean; value?: T }> {
  while (gate.held) {
    await assertAuthority(authority);
    assertNotAborted(signal);
    const remaining = deadlineAtMs - now();
    if (remaining <= 0) return { acquired: false };
    await cancellableDelay(Math.min(250, remaining), signal);
  }
  if (now() >= deadlineAtMs) return { acquired: false };
  assertNotAborted(signal);
  gate.held = true;
  try {
    await assertAuthority(authority);
    assertNotAborted(signal);
    return { acquired: true, value: await action() };
  } finally {
    gate.held = false;
  }
}

async function scaleToCount(request: FleetPoolRequest, adapter: AksFleetAdapter, authority: CleanupMutationAuthority,
  deadlineAtMs: number, now: () => number, gate: FleetArmGate,
  signal?: AbortSignal): Promise<{ snapshot?: FleetPoolSnapshot; reason: string | null }> {
  const desired = targetCount(request);
  for (let attempt = 0; now() < deadlineAtMs; attempt++) {
    assertNotAborted(signal);
    const operation = await withFleetArmGate(gate, deadlineAtMs, now, authority, signal, async () => {
      const before = await adapter.readPool(request.pool, authority);
      checkSnapshot(before, request);
      if (before.count !== 0 || before.provisioningState !== READY)
        return { retry: false, snapshot: before, reason: 'pool_not_zero_and_idle' };
      await assertAuthority(authority);
      assertNotAborted(signal);
      let result: FleetScaleResult;
      try { result = await adapter.scalePool(request.pool, desired, authority); }
      catch { result = { state: 'uncertain' }; }
      if (result.state === 'conflict' && !explicitConflict(result)) result = { state: 'uncertain' };
      const observed = await adapter.readPool(request.pool, authority);
      checkSnapshot(observed, request);
      if (result.state === 'rejected')
        return { retry: false, snapshot: observed, reason: sanitizedRejectionReason(result.reason) };
      if (result.state === 'accepted') {
        const settled = await adapter.waitForPool(request.pool, desired, deadlineAtMs, authority, signal);
        checkSnapshot(settled, request);
        return { retry: false, snapshot: settled,
          reason: settled.count === desired && settled.provisioningState === READY ? null : 'pool_did_not_reach_ready_state' };
      }
      if (result.state === 'uncertain') {
        if (observed.count === desired || observed.provisioningState !== READY) {
          const settled = await adapter.waitForPool(request.pool, desired, deadlineAtMs, authority, signal);
          checkSnapshot(settled, request);
          if (settled.count === desired && settled.provisioningState === READY)
            return { retry: false, snapshot: settled, reason: null };
        }
        return { retry: false, snapshot: observed, reason: 'scale_outcome_uncertain' };
      }
      if (observed.count === desired || observed.provisioningState !== READY) {
        const settled = await adapter.waitForPool(request.pool, observed.count, deadlineAtMs, authority, signal);
        checkSnapshot(settled, request);
        if (settled.count === desired && settled.provisioningState === READY)
          return { retry: false, snapshot: settled, reason: null };
        if (settled.count !== 0 || settled.provisioningState !== READY)
          return { retry: false, snapshot: settled, reason: 'pool_did_not_reach_ready_state' };
        return { retry: true, snapshot: settled, reason: null };
      }
      return { retry: true, snapshot: observed, reason: null };
    });
    if (!operation.acquired) return { reason: 'scale_deadline_exceeded' };
    const result = operation.value!;
    if (!result.retry) return { snapshot: result.snapshot, reason: result.reason };
    if (!await retryBackoff(request.pool, attempt, deadlineAtMs, now, signal))
      return { snapshot: result.snapshot, reason: 'scale_deadline_exceeded' };
  }
  return { reason: 'scale_deadline_exceeded' };
}

async function scaleSelectedPools(selected: FleetPoolRequest[], adapter: AksFleetAdapter, authority: CleanupMutationAuthority,
  deadlineAtMs: number, now: () => number, gate: FleetArmGate, results: Map<string, FleetPoolOutcome>,
  completed: Set<string>, signal?: AbortSignal): Promise<void> {
  let next = 0;
  const worker = async () => {
    while (true) {
      assertNotAborted(signal);
      const index = next++;
      if (index >= selected.length) return;
      const pool = selected[index]!;
      try {
        const result = await scaleToCount(pool, adapter, authority, deadlineAtMs, now, gate, signal);
        assertNotAborted(signal);
        results.set(pool.pool, outcome(pool, result.snapshot, result.reason === null ? 'ready' : 'failed', result.reason));
      } catch {
        await assertAuthority(authority);
        assertNotAborted(signal);
        results.set(pool.pool, outcome(pool, undefined, 'failed', 'pool_observation_failed'));
      } finally {
        completed.add(pool.pool);
      }
    }
  };
  const workers = Array.from({ length: Math.min(MAX_PARALLEL_SCALES, selected.length) }, worker);
  const settledWorkers = await Promise.allSettled(workers);
  const failure = settledWorkers.find(result => result.status === 'rejected');
  if (failure?.status === 'rejected') throw failure.reason;
}

function nodeReadyForPool(node: AksNodeInventory): boolean {
  return node.ready && node.amd64 && node.placementLabelsValid &&
    node.allocatable_cpu_millicores > 0 && node.allocatable_memory_bytes > 0;
}

function expectedNodeRole(pool: FleetPoolRequest, selected: FleetPoolRequest[]): string {
  return pool.pool === selected[0]!.pool ? 'arrowcol' : 'client';
}

function nodeMatchesSelectedPool(node: AksNodeInventory, pool: FleetPoolRequest, selected: FleetPoolRequest[]): boolean {
  return node.role === expectedNodeRole(pool, selected) && node.observedSku === pool.sku && node.skuLabelsConsistent;
}

async function waitForFleetNodes(selected: FleetPoolRequest[], adapter: AksFleetAdapter, authority: CleanupMutationAuthority,
  deadlineAtMs: number, now: () => number, results: Map<string, FleetPoolOutcome>,
  scalesCompleted: Set<string>, signal?: AbortSignal): Promise<FleetNodeObservation[]> {
  let rawNodes: unknown;
  const nodesByPool = new Map<string, AksNodeInventory[]>();
  while (true) {
    assertNotAborted(signal);
    const readyPools = selected.filter(pool => scalesCompleted.has(pool.pool) && results.get(pool.pool)?.state === 'ready');
    const waitingForScale = selected.some(pool => !scalesCompleted.has(pool.pool));
    const waitingForNodes = readyPools.some(pool => !nodesByPool.has(pool.pool));
    if (!waitingForScale && !waitingForNodes) break;
    const remaining = deadlineAtMs - now();
    if (remaining <= 0) break;
    await assertAuthority(authority);
    assertNotAborted(signal);
    rawNodes = await adapter.listNodes(authority);
    const inventory = parseAksNodeInventory(rawNodes);
    for (const pool of readyPools) {
      if (nodesByPool.has(pool.pool)) continue;
      const ready = inventory.filter(node => node.pool === pool.pool && node.ready);
      if (ready.some(node => !nodeReadyForPool(node) || !nodeMatchesSelectedPool(node, pool, selected))) {
        const current = results.get(pool.pool)!;
        results.set(pool.pool, { ...current, state: 'failed', reason: 'node_architecture_role_sku_or_identity_mismatch' });
        continue;
      }
      const available = ready.filter(nodeReadyForPool);
      if (available.length > targetCount(pool)) {
        const current = results.get(pool.pool)!;
        results.set(pool.pool, { ...current, state: 'failed', reason: 'node_count_mismatch' });
      } else if (available.length === targetCount(pool)) {
        nodesByPool.set(pool.pool, available.sort((a, b) => a.node.localeCompare(b.node)));
      }
    }
    const stillWaiting = selected.some(pool => !scalesCompleted.has(pool.pool)) ||
      selected.some(pool => results.get(pool.pool)?.state === 'ready' && !nodesByPool.has(pool.pool));
    if (!stillWaiting || now() >= deadlineAtMs) break;
    await cancellableDelay(Math.min(NODE_READY_POLL_MS, Math.max(1, deadlineAtMs - now())), signal);
  }
  for (const pool of selected) {
    if (results.get(pool.pool)?.state === 'ready' && !nodesByPool.has(pool.pool)) {
      const current = results.get(pool.pool)!;
      results.set(pool.pool, { ...current, state: 'failed', reason: 'node_ready_deadline_exceeded' });
    }
  }
  const successful = selected.filter(pool => results.get(pool.pool)?.state === 'ready');
  if (!successful.length || rawNodes === undefined) return [];
  await assertAuthority(authority);
  assertNotAborted(signal);
  const inventory = parseAksNodeInventory(rawNodes, await adapter.listPods(authority));
  const observations: FleetNodeObservation[] = [];
  for (const pool of successful) {
    const nodes = inventory.filter(node => node.pool === pool.pool && node.ready).sort((a, b) => a.node.localeCompare(b.node));
    const snapshot = nodesByPool.get(pool.pool) ?? [];
    if (nodes.length !== targetCount(pool) || nodes.some(node => !nodeReadyForPool(node)) ||
        nodes.some((node, index) => node.node !== snapshot[index]?.node ||
          !nodeMatchesSelectedPool(node, pool, selected))) {
      const current = results.get(pool.pool)!;
      results.set(pool.pool, { ...current, state: 'failed', reason: 'node_inventory_changed' });
      continue;
    }
    const current = results.get(pool.pool)!;
    observations.push(...nodes.map(node => ({ node: node.node, pool: pool.pool, sku: node.observedSku,
      architecture: node.architecture, allocatableCpuMillicores: node.allocatable_cpu_millicores,
      allocatableMemoryBytes: node.allocatable_memory_bytes, requestedCpuMillicores: node.requested_cpu_millicores,
      requestedMemoryBytes: node.requested_memory_bytes })));
  }
  return observations;
}

async function cleanupPool(name: string, adapter: AksFleetAdapter, authority: CleanupMutationAuthority,
  deadlineAtMs: number, retries: number, now: () => number, expectedScaleCount?: number): Promise<boolean> {
  await assertAuthority(authority);
  let snapshot = await adapter.readPool(name, authority);
  if (!settled(snapshot)) {
    await assertAuthority(authority);
    const settlingCount = snapshot.count === 0 && expectedScaleCount !== undefined
      ? expectedScaleCount : snapshot.count;
    snapshot = await adapter.waitForPool(name, settlingCount, deadlineAtMs, authority);
    if (!settled(snapshot)) return false;
  }
  if (snapshot.count !== 0 || !succeeded(snapshot)) {
    let done = false;
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (now() >= deadlineAtMs) break;
      if (snapshot.count === 0 && succeeded(snapshot)) { done = true; break; }
      await assertAuthority(authority);
      let result: FleetScaleResult;
      try { result = await adapter.scalePool(name, 0, authority); }
      catch { result = { state: 'uncertain' }; }
      if (result.state === 'conflict' && !explicitConflict(result)) result = { state: 'uncertain' };
      if (result.state === 'rejected') return false;
      await assertAuthority(authority);
      snapshot = await adapter.readPool(name, authority);
      if (result.state === 'uncertain') {
        if (!settled(snapshot)) {
          await assertAuthority(authority);
          snapshot = await adapter.waitForPool(name, 0, deadlineAtMs, authority);
        }
        done = snapshot.count === 0 && succeeded(snapshot);
        break;
      }
      if (result.state === 'accepted' || snapshot.count === 0 || !settled(snapshot)) {
        await assertAuthority(authority);
        snapshot = await adapter.waitForPool(name, 0, deadlineAtMs, authority);
      }
      done = snapshot.count === 0 && succeeded(snapshot);
      if (done) break;
      if (result.state !== 'conflict' || snapshot.provisioningState !== READY) break;
      if (!await retryBackoff(name, attempt, deadlineAtMs, now)) break;
    }
    if (!done) return false;
  }
  await assertAuthority(authority);
  const finalPool = await adapter.readPool(name, authority);
  if (finalPool.count !== 0 || !succeeded(finalPool)) return false;
  while (true) {
    await assertAuthority(authority);
    const nodes = parseAksNodeInventory(await adapter.listNodes(authority));
    if (!nodes.some(node => node.pool === name)) return true;
    const remaining = deadlineAtMs - now();
    if (remaining <= 0) return false;
    await delay(Math.min(NODE_READY_POLL_MS, remaining));
  }
}

export async function cleanupAksFleet(adapter: AksFleetAdapter, authority: CleanupMutationAuthority,
  options: AksFleetCleanupOptions): Promise<AksFleetCleanupResult> {
  if (!Number.isSafeInteger(options.deadlineAtMs)) throw new Error('Fleet cleanup requires a bounded deadline.');
  const now = options.now ?? Date.now;
  const retries = options.maxConflictRetries ?? DEFAULT_CONFLICT_RETRIES;
  if (!Number.isSafeInteger(retries) || retries < 0 || retries > 5)
    throw new Error('Fleet cleanup conflict retries must be between zero and five.');
  for (const [name, count] of Object.entries(options.expectedScaleCounts ?? {})) {
    if (!/^[a-z0-9][a-z0-9-]{0,11}$/.test(name) || !Number.isSafeInteger(count) || count < 1 || count > MAX_CLIENTS)
      throw new Error('Fleet cleanup expected scale counts must be bounded pool node counts.');
  }
  await assertAuthority(authority);
  const names = new Set(options.includePoolNames ?? []);
  const failed: string[] = [];
  let discoveryFailed = false;
  try {
    for (const name of await adapter.listUserPools(authority)) names.add(name);
  } catch {
    await assertAuthority(authority);
    discoveryFailed = true;
  }
  if (discoveryFailed) failed.push('user-pool-discovery');
  for (const name of [...names].sort()) {
    try {
      await assertAuthority(authority);
      if (!await cleanupPool(name, adapter, authority, options.deadlineAtMs, retries, now,
        options.expectedScaleCounts?.[name])) failed.push(name);
    } catch {
      await assertAuthority(authority);
      failed.push(name);
    }
  }
  return { verified: failed.length === 0, failed };
}

export async function runAksFleet<T>(request: AksFleetRequest, adapter: AksFleetAdapter,
  authority: CleanupMutationAuthority,
  run: (nodes: FleetNodeObservation[], pools: FleetPoolOutcome[], request: AksFleetRequest) => Promise<T>): Promise<AksFleetResult<T>> {
  const now = request.now ?? Date.now;
  validateRequest(request, now());
  const retries = request.maxConflictRetries ?? DEFAULT_CONFLICT_RETRIES;
  if (!Number.isSafeInteger(retries) || retries < 0 || retries > 5) throw new Error('Fleet conflict retries must be between zero and five.');
  assertNotAborted(request.signal);
  await assertAuthority(authority);
  const selected = [request.collector, ...request.clients];
  let initialUserPools: string[] = [];
  const results = new Map<string, FleetPoolOutcome>();
  let nodes: FleetNodeObservation[] = [], resolved: T | null = null, runError: unknown, runFailed = false;
  let cleanup: AksFleetCleanupResult = { verified: false, failed: [] };
  try {
    assertNotAborted(request.signal);
    initialUserPools = await adapter.listUserPools(authority);
    if (selected.some(pool => !initialUserPools.includes(pool.pool)))
      throw new Error('A selected fleet pool is not a user pool in the custodied cluster.');
    const scalesCompleted = new Set<string>();
    const armGate: FleetArmGate = { held: false };
    const [scaleResult, readinessResult] = await Promise.allSettled([
      scaleSelectedPools(selected, adapter, authority, request.deadlineAtMs, now, armGate, results, scalesCompleted, request.signal),
      waitForFleetNodes(selected, adapter, authority, request.deadlineAtMs, now, results, scalesCompleted, request.signal),
    ]);
    const errors = [scaleResult, readinessResult].filter(result => result.status === 'rejected')
      .map(result => result.status === 'rejected' ? result.reason : undefined);
    if (errors.length) throw new AggregateError(errors, 'AKS fleet scale or node inventory failed.');
    if (readinessResult.status === 'fulfilled') nodes = readinessResult.value;
    const pools = selected.map(pool => results.get(pool.pool)!);
    assertNotAborted(request.signal);
    if (results.get(request.collector.pool)?.state === 'ready')
      resolved = await run(nodes, pools, request);
  } catch (error) {
    runError = error;
    runFailed = true;
  } finally {
    cleanup = await cleanupAksFleet(adapter, authority, { deadlineAtMs: request.cleanupDeadlineAtMs,
      includePoolNames: initialUserPools,
      expectedScaleCounts: Object.fromEntries(selected.map(pool => [pool.pool, targetCount(pool)])),
      maxConflictRetries: retries, now });
  }
  const pools = selected.map(pool => results.get(pool.pool) ?? outcome(pool, undefined, 'failed', 'pool_not_attempted'));
  if (runFailed && !cleanup.verified)
    throw new AggregateError([runError, new Error('AKS fleet cleanup could not verify zero nodes for every user pool: ' + cleanup.failed.join(', '))],
      'Fleet execution and cleanup both failed.');
  if (runFailed) throw runError;
  if (!cleanup.verified) throw new Error('AKS fleet cleanup could not verify zero nodes for every user pool: ' + cleanup.failed.join(', '));
  return { resolved, nodes, pools, cleanup };
}
