import { createHash } from 'node:crypto';
import { Failure, owned, type Azure, type HttpReceipt } from './bootstrap.js';

type JsonObject = Record<string, unknown>;
function object(value: unknown): JsonObject {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure('Safe-idle ARM observation is UNKNOWN: invalid object.');
  return value as JsonObject;
}
function count(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0)
    throw new Failure('Safe-idle ARM observation is UNKNOWN: missing or invalid count.');
  return value;
}
function text(value: unknown): string {
  if (typeof value !== 'string' || value.length === 0)
    throw new Failure('Safe-idle ARM observation is UNKNOWN: missing resource field.');
  return value;
}
export interface CapacityObservationReceipt {
  resource_id: string;
  api_version: string;
  observed_at_unix_ms: number;
  http_status: 200;
  request_id: string;
  body_sha256: string;
}
export interface SafeIdleCapacityObservation {
  schema_version: 1;
  scope: 'arm_capacity_only';
  status: 'capacity_idle' | 'capacity_not_idle';
  workload_and_data_verification: 'unknown';
  aks_provisioning_state: string;
  system_pool: { name: string; count: number; vm_size: string; provisioning_state: string;
    autoscale_enabled: boolean; autoscale_min: number | null; autoscale_max: number | null };
  user_pools: Array<{ name: string; count: number; autoscale_min: number; autoscale_max: number;
    autoscale_enabled: boolean; provisioning_state: string }>;
  kusto: { sku: string; capacity: number; autoscale_min: number; autoscale_max: number;
    autoscale_enabled: boolean; auto_stop_enabled: boolean; provisioning_state: string; state: string };
  failures: string[];
  receipts: CapacityObservationReceipt[];
}

export async function observe_custody_idle_capacity(azure: Azure): Promise<SafeIdleCapacityObservation> {
  if (azure.c.mode !== 'stamped') throw new Failure('Safe-idle capacity observation requires the selected stamp.');
  const receipts: CapacityObservationReceipt[] = [];
  const read = async (id: string, type: string, api: string): Promise<JsonObject> => {
    let response: HttpReceipt | undefined;
    const [bytes, headers] = await azure.request('GET', `https://management.azure.com${id}?api-version=${api}`,
      'https://management.azure.com/', undefined, {}, false, true, value => { response = value; });
    if (!bytes || bytes.length > 4_194_304 || headers[':status'] !== '200' || response?.status !== 200 ||
        typeof response.headers['x-ms-request-id'] !== 'string' || !response.headers['x-ms-request-id'] ||
        !Buffer.from(response.bodyBase64, 'base64').equals(bytes))
      throw new Failure('Safe-idle ARM observation is UNKNOWN: no original successful response receipt.');
    const resource = object(JSON.parse(bytes.toString()) as unknown);
    if (text(resource.id).toLowerCase() !== id.toLowerCase() || text(resource.type).toLowerCase() !== type.toLowerCase())
      throw new Failure('Safe-idle ARM observation is UNKNOWN: response identity mismatch.');
    receipts.push({ resource_id: id, api_version: api, observed_at_unix_ms: Date.now(), http_status: 200,
      request_id: response.headers['x-ms-request-id'], body_sha256: createHash('sha256').update(bytes).digest('hex') });
    return resource;
  };
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  const aksType = 'Microsoft.ContainerService/managedClusters';
  const [aks, system, client, collector, cluster] = await Promise.all([
    read(aksId, aksType, '2024-10-01'),
    ...['systempool', 'benchd64', 'benche64'].map(name =>
      read(aksId + '/agentPools/' + name, aksType + '/agentPools', '2024-10-01')),
    read(azure.c.cluster_id, 'Microsoft.Kusto/clusters', '2024-04-13'),
  ]);
  owned(aks, azure.c); owned(cluster, azure.c);
  const aksProperties = object(aks.properties), profiles = aksProperties.agentPoolProfiles;
  if (!Array.isArray(profiles) ||
      profiles.map(profile => text(object(profile).name)).sort().join(',') !== 'benchd64,benche64,systempool')
    throw new Failure('Safe-idle ARM observation is UNKNOWN: incomplete or unexpected pool inventory.');
  const systemProperties = object(system.properties), kustoProperties = object(cluster.properties);
  const autoscale = object(kustoProperties.optimizedAutoscale), sku = object(cluster.sku);
  if (typeof systemProperties.enableAutoScaling !== 'boolean')
    throw new Failure('Safe-idle ARM observation is UNKNOWN: missing system autoscaler flag.');
  const systemPool = { name: 'systempool', count: count(systemProperties.count), vm_size: text(systemProperties.vmSize),
    provisioning_state: text(systemProperties.provisioningState), autoscale_enabled: systemProperties.enableAutoScaling,
    autoscale_min: systemProperties.enableAutoScaling ? count(systemProperties.minCount) : null,
    autoscale_max: systemProperties.enableAutoScaling ? count(systemProperties.maxCount) : null };
  const userPools = [client, collector].map((resource, index) => {
    const properties = object(resource.properties);
    if (typeof properties.enableAutoScaling !== 'boolean') throw new Failure('Safe-idle ARM observation is UNKNOWN: missing autoscaler flag.');
    return { name: ['benchd64', 'benche64'][index], count: count(properties.count),
      autoscale_min: count(properties.minCount), autoscale_max: count(properties.maxCount),
      autoscale_enabled: properties.enableAutoScaling, provisioning_state: text(properties.provisioningState) };
  });
  if (typeof autoscale.isEnabled !== 'boolean' || typeof kustoProperties.enableAutoStop !== 'boolean')
    throw new Failure('Safe-idle ARM observation is UNKNOWN: missing Kusto scale/auto-stop flags.');
  const kusto = { sku: text(sku.name), capacity: count(sku.capacity), autoscale_min: count(autoscale.minimum),
    autoscale_max: count(autoscale.maximum), autoscale_enabled: autoscale.isEnabled,
    auto_stop_enabled: kustoProperties.enableAutoStop, provisioning_state: text(kustoProperties.provisioningState),
    state: text(kustoProperties.state) };
  const aksProvisioningState = text(aksProperties.provisioningState), failures: string[] = [];
  if (aksProvisioningState !== 'Succeeded' || systemPool.provisioning_state !== 'Succeeded' ||
      userPools.some(pool => pool.provisioning_state !== 'Succeeded') || kusto.provisioning_state !== 'Succeeded')
    failures.push('arm_provisioning_not_succeeded');
  if (![2, 3].includes(systemPool.count) || systemPool.vm_size !== 'Standard_D8ds_v6' || systemPool.autoscale_enabled)
    failures.push('system_pool_outside_safe_idle');
  if (userPools.some(pool => pool.count !== 0 || !pool.autoscale_enabled || pool.autoscale_min !== 0 || pool.autoscale_max !== 1))
    failures.push('benchmark_pool_outside_safe_idle');
  if (kusto.sku !== 'Standard_E4ads_v5' || kusto.capacity !== 2 || !kusto.autoscale_enabled ||
      kusto.autoscale_min !== 2 || kusto.autoscale_max !== 4 || !kusto.auto_stop_enabled || kusto.state !== 'Running')
    failures.push('kusto_outside_baseline_safe_idle');
  return { schema_version: 1, scope: 'arm_capacity_only', status: failures.length ? 'capacity_not_idle' : 'capacity_idle',
    workload_and_data_verification: 'unknown', aks_provisioning_state: aksProvisioningState,
    system_pool: systemPool, user_pools: userPools, kusto, failures,
    receipts: receipts.sort((a, b) => a.resource_id.localeCompare(b.resource_id)) };
}
