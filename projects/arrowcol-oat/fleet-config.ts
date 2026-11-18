import { cpuMilli, memoryBytes, type ResourceRequirements } from './runtime.js';

type AutoNumber = number | 'auto';
export interface FleetSizing {
  collector_pool: string;
  collector: {
    dfe_workers: AutoNumber;
    runtime_workers: AutoNumber;
    blocking_workers: AutoNumber;
    cpu_workers: AutoNumber;
  };
  client: {
    workers: AutoNumber;
    runtime_threads: AutoNumber;
    connections_per_worker: AutoNumber;
    streams_per_worker: AutoNumber;
    rows_per_table: AutoNumber;
    dmv_concurrency: AutoNumber;
    corpus_bytes: AutoNumber;
    max_memory_bytes: AutoNumber;
  };
}
export interface FleetNodeObservation {
  node: string;
  pool: string;
  sku: string;
  architecture: string;
  allocatableCpuMillicores: number;
  allocatableMemoryBytes: number;
  requestedCpuMillicores: number;
  requestedMemoryBytes: number;
}
export interface FleetPoolOutcome {
  name: string;
  sku: string;
  vcpu: number;
  state: 'ready' | 'failed';
  reason: string | null;
}
export interface FleetNode {
  node: string;
  pool: string;
  sku: string;
  allocatable_cpu_millicores: number;
  allocatable_memory_bytes: number;
  resources: ResourceRequirements;
}
const MiB = 1024 ** 2;
const GiB = 1024 ** 3;
export const FLEET_CLIENT_RESERVATION = { cpu: 250, memory: 256 * MiB };
const bounds = {
  collector: { dfe_workers: 256, runtime_workers: 64, blocking_workers: 256, cpu_workers: 128 },
  client: { workers: 1024, runtime_threads: 1024, connections_per_worker: 256, streams_per_worker: 1024,
    rows_per_table: 1_000_000, dmv_concurrency: 64, corpus_bytes: Number.MAX_SAFE_INTEGER, max_memory_bytes: Number.MAX_SAFE_INTEGER },
};
function object(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as Record<string, unknown>;
}
function assertFleetSizing(input: unknown): asserts input is FleetSizing {
  const c = object(input, 'fleet sizing');
  if (Object.keys(c).sort().join() !== 'client,collector,collector_pool' ||
      typeof c.collector_pool !== 'string' || !/^[a-z][a-z0-9]{0,11}$/.test(c.collector_pool))
    throw new Error('fleet sizing requires an explicit collector_pool and collector/client sizing');
  for (const role of ['collector', 'client'] as const) {
    const settings = object(c[role], `fleet ${role}`);
    if (Object.keys(settings).sort().join() !== Object.keys(bounds[role]).sort().join())
      throw new Error(`fleet ${role} sizing has missing or unknown fields`);
    for (const [name, maximum] of Object.entries(bounds[role])) {
      const value = settings[name];
      if (value !== 'auto' && (typeof value !== 'number' || !Number.isSafeInteger(value) || value < (name === 'blocking_workers' ? 2 : 1) || value > maximum))
        throw new Error(`fleet ${role}.${name} must be auto or a bounded positive integer (maximum ${maximum})`);
    }
  }
}
export function parseFleetSizing(input: unknown): FleetSizing {
  assertFleetSizing(input);
  return input;
}
function numeric(value: AutoNumber, automatic: number, maximum: number): number {
  return value === 'auto' ? Math.min(maximum, Math.max(1, Math.floor(automatic))) : value;
}
function cpu(value: unknown): number {
  const result = Number(cpuMilli(value));
  if (!Number.isSafeInteger(result) || result <= 0) throw new Error('invalid fleet support CPU request');
  return result;
}
function memory(value: unknown): number {
  const result = Number(memoryBytes(value));
  if (!Number.isSafeInteger(result) || result <= 0) throw new Error('invalid fleet support memory request');
  return result;
}
export function supportReservation(base: Record<string, unknown>): { cpu: number; memory: number } {
  const platform = object(base.platform, 'platform');
  const resources = platform.resources === undefined ? {} : object(platform.resources, 'platform.resources');
  let reservedCpu = 4000, reservedMemory = 5 * GiB;
  for (const [role, defaultCpu, defaultMemory] of [
    ['router', '2', '1Gi'], ['catalog', '2', '4Gi'], ['maintenance', '1', '2Gi'],
  ] as const) {
    const configured = resources[role] === undefined ? undefined : object(resources[role], role);
    const limit = configured === undefined ? {} : object(configured.limits, `${role}.limits`);
    const request = configured === undefined ? {} : object(configured.requests, `${role}.requests`);
    reservedCpu += Math.max(cpu(limit.cpu ?? defaultCpu), cpu(request.cpu ?? defaultCpu));
    reservedMemory += Math.max(memory(limit.memory ?? defaultMemory), memory(request.memory ?? defaultMemory));
  }
  return { cpu: reservedCpu, memory: reservedMemory };
}
function nodeBudget(observed: FleetNodeObservation, reserved: { cpu: number; memory: number }): FleetNode {
  if (!/^[a-z0-9][a-z0-9.-]{0,252}$/.test(observed.node) || !/^[a-z][a-z0-9]{0,11}$/.test(observed.pool) ||
      !/^Standard_[A-Za-z0-9_]+$/.test(observed.sku) || observed.architecture !== 'amd64')
    throw new Error('fleet nodes require observed identities and amd64 architecture for the published images');
  for (const value of [observed.allocatableCpuMillicores, observed.allocatableMemoryBytes,
    observed.requestedCpuMillicores, observed.requestedMemoryBytes]) {
    if (!Number.isSafeInteger(value) || value < 0) throw new Error('fleet allocatable/requested resources must be nonnegative safe integers');
  }
  const availableCpu = observed.allocatableCpuMillicores - observed.requestedCpuMillicores - reserved.cpu;
  const availableMemory = Math.floor((observed.allocatableMemoryBytes - observed.requestedMemoryBytes - reserved.memory) / MiB) * MiB;
  if (availableCpu < 1000 || availableMemory < 512 * MiB) throw new Error(`fleet node ${observed.node} has insufficient unrequested pod budget`);
  const values = { cpu: `${availableCpu}m`, memory: String(availableMemory) };
  return { node: observed.node, pool: observed.pool, sku: observed.sku,
    allocatable_cpu_millicores: observed.allocatableCpuMillicores, allocatable_memory_bytes: observed.allocatableMemoryBytes,
    resources: { requests: { ...values }, limits: { ...values } } };
}

export function resolveFleetExperiment(
  base: Record<string, unknown>, input: unknown, observations: FleetNodeObservation[], pools: FleetPoolOutcome[],
): Record<string, unknown> {
  const sizing = parseFleetSizing(input);
  if (!Array.isArray(observations) || !Array.isArray(pools) ||
      new Set(pools.map(pool => pool.name)).size !== pools.length ||
      pools.some(pool => !/^[a-z][a-z0-9]{0,11}$/.test(pool.name) ||
        !/^Standard_[A-Za-z0-9_]+$/.test(pool.sku) || !Number.isSafeInteger(pool.vcpu) || pool.vcpu < 1 ||
        !['ready', 'failed'].includes(pool.state) ||
        (pool.reason !== null && (typeof pool.reason !== 'string' || pool.reason.length > 512 || /[\r\n]/.test(pool.reason))) ||
        pool.state === 'failed' && !pool.reason))
    throw new Error('fleet pool outcomes require unique identities, integer vCPUs and sanitized failure reasons');
  if (base.campaign_profile !== 'scale_out_single_table' || !Array.isArray(base.tables) || base.tables.length !== 1)
    throw new Error('full fleet requires the existing scale_out_single_table single-table topology');
  const collectors = observations.filter(node => node.pool === sizing.collector_pool);
  if (collectors.length !== 1) throw new Error('full fleet requires exactly one Ready node in the explicit collector pool');
  if (new Set(observations.map(node => node.node)).size !== observations.length)
    throw new Error('fleet observations contain duplicate nodes');
  for (const node of observations) {
    if (!pools.some(pool => pool.name === node.pool && pool.sku === node.sku && pool.state === 'ready'))
      throw new Error('fleet node does not match a ready pool outcome');
  }
  const resolved = structuredClone(base);
  const platform = object(resolved.platform, 'platform');
  if (platform.kind !== 'aks') throw new Error('full fleet is AKS-only');
  const collector = nodeBudget(collectors[0], supportReservation(base));
  const collectorCores = Math.floor(cpu(collector.resources.requests.cpu) / 1000);
  const existingTunables = resolved.tunables === undefined ? {} : object(resolved.tunables, 'tunables');
  const collectorTunables = existingTunables.collector === undefined ? {} : object(existingTunables.collector, 'collector');
  const existingRuntime = collectorTunables.runtime === undefined ? {} : object(collectorTunables.runtime, 'collector.runtime');
  collectorTunables.runtime = { ...existingRuntime,
    workers: numeric(sizing.collector.runtime_workers, collectorCores, 64),
    blocking_workers: numeric(sizing.collector.blocking_workers, collectorCores * 2, 256),
    cpu_workers: numeric(sizing.collector.cpu_workers, collectorCores, 128) };
  const existingClient = existingTunables.client === undefined ? {} : object(existingTunables.client, 'client');
  const clients = observations.filter(node => node.pool !== sizing.collector_pool).sort((a, b) => a.node.localeCompare(b.node)).map((node, index) => {
    const placement = nodeBudget(node, FLEET_CLIENT_RESERVATION);
    const cores = Math.floor(cpu(placement.resources.requests.cpu) / 1000);
    const bytes = memory(placement.resources.requests.memory);
    const maxMemory = numeric(sizing.client.max_memory_bytes, bytes / 2, Number.MAX_SAFE_INTEGER);
    const corpus = numeric(sizing.client.corpus_bytes, Math.min(maxMemory / 4, cores * 8 * MiB), Number.MAX_SAFE_INTEGER);
    if (maxMemory > bytes || corpus > maxMemory) throw new Error('client corpus/memory override exceeds its node budget');
    const client = { ...existingClient,
      workers: numeric(sizing.client.workers, cores, 1024),
      runtime_threads: numeric(sizing.client.runtime_threads, cores, 1024),
      connections_per_worker: numeric(sizing.client.connections_per_worker, Math.max(1, Math.floor(cores / 16)), 4),
      streams_per_worker: numeric(sizing.client.streams_per_worker, Math.ceil(cores / 4), 16),
      max_memory_bytes: maxMemory, corpus_bytes: corpus, pod_ordinal: index + 1 };
    delete (client as Record<string, unknown>).corpus_rows;
    return { placement,
      rows_per_table: numeric(sizing.client.rows_per_table, bytes / (Math.max(1, cores) * 4096), 1_000_000),
      dmv_concurrency: numeric(sizing.client.dmv_concurrency, cores, 64), tunables: client };
  });
  if (clients.length < 1 || clients.length > 128) throw new Error('full fleet requires 1 through 128 client nodes');
  platform.resources = { ...(platform.resources === undefined ? {} : object(platform.resources, 'resources')),
    collector: collector.resources };
  platform.fleet = { collector, clients, pools };
  resolved.tunables = { ...existingTunables, collector: collectorTunables, client: clients[0].tunables };
  resolved.dfe_worker_concurrency = [numeric(sizing.collector.dfe_workers, collectorCores, 256)];
  resolved.replica_counts = [1];
  resolved.clients_per_table = clients.length;
  delete resolved.clients_per_table_sweep;
  resolved.rows_per_table = clients[0].rows_per_table;
  resolved.dmv_concurrency = clients[0].dmv_concurrency;
  return resolved;
}
