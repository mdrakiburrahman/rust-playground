import { randomUUID } from 'node:crypto';
import { lstat, readFile, rename, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { API, Azure, Config, load_env } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { Kusto, identifier, object, primary, tableRows } from '../../experiments/azure-qap/src/kusto.js';

const CORES = { Standard_E4ads_v5: 4, Standard_E8ads_v5: 8, Standard_E16ads_v5: 16, Standard_E32ads_v5: 32 } as const;
const OWNER_TAG = 'qapCampaignOwner';
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
type Row = Record<string, unknown>;
export interface ClusterProfile {
  sku: keyof typeof CORES;
  nodes: number;
  autoscale: { enabled: boolean; minimum: number; maximum: number };
}
interface Mutation {
  kind: 'apply' | 'restore';
  submittedAt: string;
  profile: ClusterProfile;
  state: 'intent' | 'accepted' | 'verified' | 'uncertain';
  requestId?: string;
  operationUrl?: string;
}
export interface QapCapacity {
  ClusterMaximumConcurrentOperations: number;
  CoreUtilizationCoefficient: number;
}
export interface PolicyMutation {
  kind: 'apply' | 'restore';
  submittedAt: string;
  before: QapCapacity;
  desired: QapCapacity;
  state: 'intent' | 'accepted' | 'verified' | 'uncertain' | 'not-submitted';
  beforePolicy: unknown;
  readback?: unknown;
  observedAt?: string;
  effective?: Row;
  error?: string;
}
export interface BackgroundTable {
  database: string;
  name: string;
  definition: Row;
  policy: Row;
  eligibility: Row;
}
export interface BackgroundAttestation {
  observedAt: string;
  databases: string[];
  tables: BackgroundTable[];
  effective: Row;
  runningQueries: Row[];
  recentQueries: Row[];
}
export interface Receipt {
  schemaVersion: 1 | 2;
  expanded?: true;
  owner: string;
  clusterId: string;
  deadlineMs: number;
  initial: Row;
  initialCapacityPolicy: unknown;
  initialProfile: ClusterProfile;
  mutations: Mutation[];
  background?: BackgroundAttestation;
  policyMutations?: PolicyMutation[];
  restoredAt?: string;
}

function integer(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) throw new Error(`${name} must be a safe integer.`);
  return value;
}

function keys(row: Row, expected: string[], name: string): void {
  if (Object.keys(row).sort().join(',') !== expected.sort().join(',')) throw new Error(`${name} has unexpected fields.`);
}

export function validateClusterProfile(value: unknown, expanded = false): ClusterProfile {
  const row = object(value);
  keys(row, ['sku', 'nodes', 'autoscale'], 'Cluster profile');
  if (typeof row.sku !== 'string' || !Object.hasOwn(CORES, row.sku)) throw new Error('Unsupported benchmark SKU.');
  const sku = row.sku as keyof typeof CORES;
  const nodes = integer(row.nodes, 'nodes');
  const scale = object(row.autoscale);
  keys(scale, ['enabled', 'minimum', 'maximum'], 'Autoscale');
  if (typeof scale.enabled !== 'boolean') throw new Error('Autoscale enabled must be boolean.');
  const minimum = integer(scale.minimum, 'minimum'), maximum = integer(scale.maximum, 'maximum');
  const expandedFixedE32 = expanded && sku === 'Standard_E32ads_v5' && !scale.enabled;
  const maxNodes = expandedFixedE32 ? 16 : 8;
  const maxCores = expandedFixedE32 ? 512 : 64;
  if (minimum < 2 || maximum > maxNodes || minimum > nodes || nodes > maximum || maximum * CORES[sku] > maxCores)
    throw new Error(`Profile exceeds ${maxCores} engine vCPUs / ${maxNodes} nodes or has invalid node bounds.`);
  if (!scale.enabled && (minimum !== nodes || maximum !== nodes)) throw new Error('Fixed profiles must pin both bounds to nodes.');
  return { sku, nodes, autoscale: { enabled: scale.enabled, minimum, maximum } };
}

export function profileFromCluster(value: unknown): ClusterProfile {
  const cluster = object(value), sku = object(cluster.sku), properties = object(cluster.properties);
  const scale = object(properties.optimizedAutoscale);
  return validateClusterProfile({ sku: sku.name, nodes: sku.capacity,
    autoscale: { enabled: scale.isEnabled, minimum: scale.minimum, maximum: scale.maximum } }, true);
}

export function profileMatches(actual: ClusterProfile, desired: ClusterProfile, allowAutoscaleMovement: boolean): boolean {
  return actual.sku === desired.sku && actual.autoscale.enabled === desired.autoscale.enabled &&
    actual.autoscale.minimum === desired.autoscale.minimum && actual.autoscale.maximum === desired.autoscale.maximum &&
    (allowAutoscaleMovement && desired.autoscale.enabled
      ? actual.nodes >= desired.autoscale.minimum && actual.nodes <= desired.autoscale.maximum
      : actual.nodes === desired.nodes);
}

export function observedQapPressure(observations: unknown[]): boolean {
  const samples = observations.map(object).filter(row => !row.error);
  const valid = samples.flatMap(row => {
    if (!row.before || !row.capacity) return [];
    const state = object(row.before);
    const qap = tableRows(primary(row.capacity)).filter(item => item.Resource === 'QueryAcceleration'), capacity = qap[0];
    if (!Number.isSafeInteger(state.pendingFiles) || Number(state.pendingFiles) <= 0 || qap.length !== 1 || !capacity ||
        typeof capacity.Total !== 'number' || !Number.isSafeInteger(capacity.Total) ||
        typeof capacity.Consumed !== 'number' || !Number.isSafeInteger(capacity.Consumed) ||
        !Number.isSafeInteger(row.endedAtMs)) return [];
    return [{ at: integer(row.endedAtMs, 'observation time'), pending: state.pendingFiles,
      saturated: capacity.Total > 0 && capacity.Consumed >= capacity.Total }];
  });
  const saturated = [...new Set(valid.filter(row => row.saturated).map(row => row.at))].sort((a, b) => a - b);
  return saturated.length >= 3 && saturated.at(-1)! - saturated[0]! >= 30_000;
}

function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value && typeof value === 'object') return `{${Object.entries(object(value)).sort(([a], [b]) => a.localeCompare(b))
    .map(([key, item]) => `${JSON.stringify(key)}:${canonical(item)}`).join(',')}}`;
  return JSON.stringify(value);
}

function dynamic(value: unknown): Row {
  return object(typeof value === 'string' ? JSON.parse(value) : value);
}

function rows(value: unknown): Row[] { return tableRows(primary(value)); }

function single(value: unknown): Row {
  const result = rows(value);
  if (result.length !== 1) throw new Error('Expected exactly one management result row.');
  return result[0]!;
}

export function validateQapCapacity(value: unknown, expanded = false): QapCapacity {
  const policy = object(value);
  keys(policy, ['ClusterMaximumConcurrentOperations', 'CoreUtilizationCoefficient'], 'QueryAccelerationCapacity');
  const cap = integer(policy.ClusterMaximumConcurrentOperations, 'ClusterMaximumConcurrentOperations');
  const coefficient = policy.CoreUtilizationCoefficient;
  const maxCap = expanded ? 256 : 100;
  if (cap < 1 || cap > maxCap || typeof coefficient !== 'number' || !Number.isFinite(coefficient) ||
      coefficient < 0.1 || coefficient > 1) throw new Error(`QueryAccelerationCapacity requires cap 1..${maxCap} and coefficient 0.1..1.`);
  return { ClusterMaximumConcurrentOperations: cap, CoreUtilizationCoefficient: coefficient };
}

export function qapPolicy(value: unknown, expanded = false): QapCapacity {
  return validateQapCapacity(dynamic(single(value).Policy).QueryAccelerationCapacity, expanded);
}

export function effectiveQapCapacity(value: unknown): Row {
  const found = rows(value).filter(row => row.Resource === 'QueryAcceleration');
  if (found.length !== 1 || ['Total', 'Consumed', 'Remaining'].some(key =>
    typeof found[0]![key] !== 'number' || !Number.isFinite(found[0]![key]) || Number(found[0]![key]) < 0))
    throw new Error('Effective QueryAcceleration capacity is missing or invalid.');
  return found[0]!;
}

export function attestBackgroundTable(database: string, definition: Row, details: Row, policy: Row, statistics: Row): BackgroundTable {
  const name = definition.TableName;
  if (typeof name !== 'string' || details.TableName !== name || statistics.ExternalTableName !== name ||
      typeof definition.TableType !== 'string' || definition.TableType.toLowerCase() !== 'delta')
    throw new Error('External table identity/type is missing or inconsistent.');
  identifier(name);
  const state = dynamic(details.QueryAccelerationState);
  const detailPolicy = dynamic(details.QueryAccelerationPolicy);
  const reason = state.NotHealthyReason;
  if (state.IsHealthy !== false || typeof reason !== 'string' || !/^DeltaTableNotFound(?::|$)/.test(reason) ||
      statistics.NotHealthyReason !== reason || policy.IsEnabled !== true || statistics.IsEnabled !== true ||
      canonical(detailPolicy) !== canonical(policy) || state.RowCount !== 0 || state.AcceleratedRowCount !== 0 ||
      statistics.HotSize !== 0 || statistics.AccelerationPendingDataFilesCount !== 0 ||
      statistics.AccelerationPendingDataFilesSize !== 0 ||
      (state.PendingArtifactCount !== undefined && state.PendingArtifactCount !== 0) ||
      (state.PendingArtifactSize !== undefined && state.PendingArtifactSize !== 0))
    throw new Error(`External table ${database}/${name} is not strictly idle with a missing Delta source.`);
  return { database, name, definition: { ...definition, Properties: dynamic(definition.Properties) },
    policy, eligibility: { IsHealthy: false, NotHealthyReason: reason, RowCount: 0, AcceleratedRowCount: 0,
      HotSize: 0, AccelerationPendingDataFilesCount: 0, AccelerationPendingDataFilesSize: 0 } };
}

export function validateBackground(attestation: BackgroundAttestation): void {
  if (!Number.isFinite(Date.parse(attestation.observedAt)) || !Array.isArray(attestation.databases) ||
      !attestation.databases.length || attestation.databases.some(db => typeof db !== 'string' || !db) ||
      new Set(attestation.databases).size !== attestation.databases.length || !Array.isArray(attestation.tables) ||
      !Array.isArray(attestation.runningQueries) || attestation.runningQueries.length ||
      !Array.isArray(attestation.recentQueries) || object(attestation.effective).Resource !== 'QueryAcceleration' ||
      attestation.effective.Consumed !== 0 || typeof attestation.effective.Total !== 'number' ||
      !Number.isFinite(attestation.effective.Total) || attestation.effective.Total <= 0 ||
      attestation.effective.Remaining !== attestation.effective.Total)
    throw new Error('Background attestation requires no active queries and no consumed QAP slots.');
  const identities = new Set<string>();
  for (const table of attestation.tables) {
    const identity = `${table.database}/${table.name}`, eligibility = object(table.eligibility);
    object(table.definition.Properties);
    object(table.definition.schema);
    if (!attestation.databases.includes(table.database) || identities.has(identity) ||
        table.definition.TableName !== table.name || table.policy.IsEnabled !== true ||
        eligibility.IsHealthy !== false || typeof eligibility.NotHealthyReason !== 'string' ||
        !/^DeltaTableNotFound(?::|$)/.test(eligibility.NotHealthyReason) ||
        ['RowCount', 'AcceleratedRowCount', 'HotSize', 'AccelerationPendingDataFilesCount',
          'AccelerationPendingDataFilesSize'].some(key => eligibility[key] !== 0))
      throw new Error('Invalid passive background table attestation.');
    identifier(table.name);
    identities.add(identity);
    const reference = new RegExp(`(^|[^A-Za-z0-9_])${table.name}([^A-Za-z0-9_]|$)`);
    if (attestation.recentQueries.some(query => typeof query.Text !== 'string' || typeof query.Database !== 'string' ||
        reference.test(query.Text)))
      throw new Error('A background table has recent query activity or uninspectable query evidence.');
  }
}

export function requireSameBackground(expected: BackgroundAttestation, actual: BackgroundAttestation): void {
  validateBackground(expected);
  validateBackground(actual);
  if (canonical([...expected.databases].sort()) !== canonical([...actual.databases].sort()) ||
      canonical(expected.tables) !== canonical(actual.tables))
    throw new Error('Background external table set, definition, policy, or health changed; refusing apply.');
}

type Management = (database: string, command: string) => Promise<unknown>;

export async function captureBackground(databases: string[], management: Management): Promise<BackgroundAttestation> {
  const tables: BackgroundTable[] = [];
  const selected = [...new Set(databases)].sort();
  if (!selected.length) throw new Error('Background inventory requires selected databases.');
  for (const database of selected) {
    const orderedDefinitions = (value: unknown) => rows(value).sort((a, b) => String(a.TableName).localeCompare(String(b.TableName)));
    const definitions = orderedDefinitions(await management(database, '.show external tables'));
    const statistics = rows(await management(database, '.show external tables operations query_acceleration statistics'));
    if (statistics.length !== definitions.length) throw new Error('External table/statistics inventory differs.');
    for (const definition of definitions) {
      if (typeof definition.TableName !== 'string') throw new Error('External table name is absent.');
      const name = definition.TableName, quoted = identifier(name);
      const [detailsResponse, schemaResponse, policyResponse] = await Promise.all([
        management(database, `.show external table ${quoted} details`),
        management(database, `.show external table ${quoted} schema as json`),
        management(database, `.show external table ${quoted} policy query_acceleration`),
      ]);
      const details = single(detailsResponse), schema = single(schemaResponse);
      if (schema.TableName !== name || schema.DatabaseName !== database) throw new Error('External table schema identity differs.');
      const policy = dynamic(single(policyResponse).Policy);
      const matching = statistics.filter(row => row.ExternalTableName === name);
      if (matching.length !== 1) throw new Error('External table statistics are absent or duplicated.');
      tables.push(attestBackgroundTable(database, { ...definition, schema: dynamic(schema.Schema) }, details, policy, matching[0]!));
    }
    if (canonical(definitions) !== canonical(orderedDefinitions(await management(database, '.show external tables'))))
      throw new Error('External table definitions changed during inventory.');
  }
  tables.sort((a, b) => `${a.database}/${a.name}`.localeCompare(`${b.database}/${b.name}`));
  const recentQueries = rows(await management(selected[0]!, '.show queries | where StartedOn > ago(5m)'));
  const runningQueries = rows(await management(selected[0]!, '.show running queries'));
  const effective = effectiveQapCapacity(await management(selected[0]!, '.show capacity'));
  const result = { observedAt: new Date().toISOString(), databases: selected, tables, effective, runningQueries, recentQueries };
  validateBackground(result);
  return result;
}

async function writeReceipt(path: string, receipt: Receipt): Promise<void> {
  const temp = `${path}.${randomUUID()}.tmp`;
  await writeFile(temp, JSON.stringify(receipt, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  await rename(temp, path);
}

async function loadReceipt(path: string, azure: Azure): Promise<Receipt> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) throw new Error('Receipt must be an owner-only regular file.');
  return validateReceipt(JSON.parse(await readFile(path, 'utf8')), azure.c.cluster_id);
}

export function validateReceipt(value: unknown, clusterId: string): Receipt {
  const receipt = object(value) as unknown as Receipt;
  if (![1, 2].includes(receipt.schemaVersion) || receipt.clusterId !== clusterId ||
      !/^[0-9a-f-]{36}$/.test(receipt.owner) || !Array.isArray(receipt.mutations) ||
      !Number.isSafeInteger(receipt.deadlineMs)) throw new Error('Receipt identity is invalid.');
  validateClusterProfile(receipt.initialProfile);
  object(receipt.initial.tags);
  qapPolicy(receipt.initialCapacityPolicy);
  for (const mutation of receipt.mutations) {
    validateClusterProfile(mutation.profile, receipt.expanded);
    if (!['apply', 'restore'].includes(mutation.kind) ||
        !['intent', 'accepted', 'verified', 'uncertain'].includes(mutation.state) ||
        !Number.isFinite(Date.parse(mutation.submittedAt))) throw new Error('Invalid cluster mutation receipt.');
  }
  if (receipt.mutations.slice(0, -1).some(mutation => mutation.state !== 'verified'))
    throw new Error('A cluster mutation was appended before the previous intent was reconciled.');
  if (receipt.expanded !== undefined && (receipt.expanded !== true || receipt.schemaVersion !== 2))
    throw new Error('Expanded trials require an explicitly opted-in version 2 receipt.');
  if (receipt.schemaVersion === 2) {
    if (!receipt.background || !Array.isArray(receipt.policyMutations)) throw new Error('Version 2 background attestation is required.');
    validateBackground(receipt.background);
  } else if (receipt.policyMutations !== undefined || receipt.background !== undefined) {
    throw new Error('Version 1 receipts cannot contain version 2 authority.');
  }
  let expected = qapPolicy(receipt.initialCapacityPolicy);
  if (receipt.policyMutations?.slice(0, -1).some(mutation => !['verified', 'not-submitted'].includes(mutation.state)))
    throw new Error('A capacity mutation was appended before the previous intent was reconciled.');
  for (const mutation of receipt.policyMutations ?? []) {
    validateQapCapacity(mutation.before, receipt.expanded);
    validateQapCapacity(mutation.desired, receipt.expanded);
    if (!['apply', 'restore'].includes(mutation.kind) ||
        !['intent', 'accepted', 'verified', 'uncertain', 'not-submitted'].includes(mutation.state) ||
        !Number.isFinite(Date.parse(mutation.submittedAt)) || canonical(mutation.before) !== canonical(expected) ||
        canonical(qapPolicy(mutation.beforePolicy, receipt.expanded)) !== canonical(mutation.before))
      throw new Error('Invalid capacity policy mutation receipt.');
    expected = mutation.state === 'not-submitted' ? mutation.before : mutation.desired;
  }
  return receipt;
}

function requireHealthy(cluster: Row): void {
  const p = object(cluster.properties);
  if (p.state !== 'Running' || p.provisioningState !== 'Succeeded') throw new Error('Cluster is not stable and running.');
}

export interface ControllerIO {
  readCluster(): Promise<Row>;
  patchCluster(cluster: Row, body: Row): Promise<Record<string, string>>;
  management(command: string): Promise<unknown>;
  background(): Promise<BackgroundAttestation>;
  availableSkus(): Promise<unknown>;
  save(receipt: Receipt): Promise<void>;
  now(): number;
  sleep(ms: number): Promise<void>;
}

function requireReserve(receipt: Receipt, io: ControllerIO): void {
  if (io.now() >= receipt.deadlineMs - 90 * 60_000) throw new Error('Campaign is inside its 90-minute restoration reserve.');
}

function requireOwned(receipt: Receipt, cluster: Row): void {
  requireHealthy(cluster);
  if (object(cluster.tags)[OWNER_TAG] !== receipt.owner) throw new Error('Campaign ownership was lost; refusing overwrite.');
}

function expectedPolicy(receipt: Receipt): QapCapacity {
  const latest = receipt.policyMutations?.at(-1);
  return latest?.state === 'not-submitted' ? latest.before : latest?.desired ?? qapPolicy(receipt.initialCapacityPolicy);
}

async function reconcilePolicy(receipt: Receipt, io: ControllerIO): Promise<unknown> {
  const live = await io.management('.show cluster policy capacity'), latest = receipt.policyMutations?.at(-1);
  const expected = expectedPolicy(receipt);
  if (latest && !['verified', 'not-submitted'].includes(latest.state)) {
    requireOwned(receipt, await io.readCluster());
    latest.readback = live;
    latest.effective = effectiveQapCapacity(await io.management('.show capacity'));
    latest.observedAt = new Date(io.now()).toISOString();
    latest.state = canonical(qapPolicy(live, receipt.expanded)) === canonical(expected) ? 'verified' : 'uncertain';
    await io.save(receipt);
    if (latest.state !== 'verified')
      throw new Error('Capacity policy outcome remains uncertain or changed; intent retained, never resubmit.');
  }
  if (canonical(qapPolicy(live, receipt.expanded)) !== canonical(expected))
    throw new Error('QueryAccelerationCapacity changed outside the campaign; refusing overwrite.');
  return live;
}

async function changeCapacity(receipt: Receipt, desired: QapCapacity, kind: 'apply' | 'restore', io: ControllerIO): Promise<void> {
  validateQapCapacity(desired, receipt.expanded);
  const beforePolicy = await reconcilePolicy(receipt, io), before = qapPolicy(beforePolicy, receipt.expanded);
  if (canonical(before) === canonical(desired)) {
    effectiveQapCapacity(await io.management('.show capacity'));
    return;
  }
  requireOwned(receipt, await io.readCluster());
  if (kind === 'apply') requireReserve(receipt, io);
  const mutation: PolicyMutation = { kind, submittedAt: new Date(io.now()).toISOString(),
    before, desired, beforePolicy, state: 'intent' };
  if (kind === 'apply') delete receipt.restoredAt;
  (receipt.policyMutations ??= []).push(mutation);
  await io.save(receipt);
  // Kusto has no capacity-policy compare-and-swap. Recheck the owned part, and merge only that part.
  try {
    const guarded = qapPolicy(await io.management('.show cluster policy capacity'), receipt.expanded);
    if (canonical(guarded) !== canonical(before)) throw new Error('QueryAccelerationCapacity changed before submission.');
  } catch (error) {
    mutation.state = 'not-submitted';
    mutation.error = error instanceof Error ? error.message : String(error);
    await io.save(receipt);
    throw error;
  }
  try {
    rows(await io.management(`.alter-merge cluster policy capacity '${JSON.stringify({ QueryAccelerationCapacity: desired })}'`));
    mutation.state = 'accepted';
    await io.save(receipt);
  } catch (error) {
    mutation.state = 'uncertain';
    mutation.error = error instanceof Error ? error.message : String(error);
    await io.save(receipt);
  }
  // Even a lost/partial response is reconciled by reads, never a second management submission.
  await reconcilePolicy(receipt, io);
}

async function reconcileCluster(receipt: Receipt, cluster: Row, io: ControllerIO): Promise<void> {
  const latest = receipt.mutations.at(-1);
  if (!latest || latest.state === 'verified') return;
  requireHealthy(cluster);
  const expectedTags = latest.kind === 'apply' ? { ...object(receipt.initial.tags), [OWNER_TAG]: receipt.owner } : receipt.initial.tags;
  if (!profileMatches(profileFromCluster(cluster), latest.profile, true) || canonical(cluster.tags) !== canonical(expectedTags))
    throw new Error('Cluster mutation remains uncertain; reconcile the saved intent without resubmission.');
  latest.state = 'verified';
  await io.save(receipt);
}

async function changeCluster(receipt: Receipt, cluster: Row, profile: ClusterProfile, nextTags: Row,
  kind: 'apply' | 'restore', io: ControllerIO): Promise<void> {
  const sameProfile = profileMatches(profileFromCluster(cluster), profile, true);
  if (sameProfile && canonical(cluster.tags) === canonical(nextTags)) return;
  if (typeof cluster.etag !== 'string') throw new Error('Cluster ETag is unavailable; conditional mutation refused.');
  if (kind === 'apply') requireReserve(receipt, io);
  const mutation: Mutation = { kind, submittedAt: new Date(io.now()).toISOString(), profile, state: 'intent' };
  if (kind === 'apply') delete receipt.restoredAt;
  receipt.mutations.push(mutation);
  await io.save(receipt);
  const body: Row = sameProfile ? { tags: nextTags } :
    { sku: { name: profile.sku, tier: 'Standard', capacity: profile.nodes }, tags: nextTags,
      properties: { optimizedAutoscale: { version: 1, isEnabled: profile.autoscale.enabled,
        minimum: profile.autoscale.minimum, maximum: profile.autoscale.maximum } } };
  try {
    const headers = await io.patchCluster(cluster, body);
    mutation.state = 'accepted';
    mutation.requestId = headers['x-ms-request-id'];
    mutation.operationUrl = headers['azure-asyncoperation'];
    await io.save(receipt);
  } catch (error) {
    mutation.state = 'uncertain';
    await io.save(receipt);
    throw error;
  }
  const until = io.now() + 45 * 60_000;
  while (io.now() < until) {
    const current = await io.readCluster(), properties = object(current.properties);
    if (properties.provisioningState === 'Failed') throw new Error('Cluster update failed; receipt retained for reconciliation.');
    if (properties.provisioningState === 'Succeeded' && properties.state === 'Running' &&
        profileMatches(profileFromCluster(current), profile, true) && canonical(current.tags) === canonical(nextTags)) {
      await reconcileCluster(receipt, current, io);
      return;
    }
    await io.sleep(15_000);
  }
  throw new Error('Cluster convergence timed out; do not resubmit. Reconcile the saved receipt.');
}

export async function clusterStatus(receipt: Receipt, io: ControllerIO): Promise<Row> {
  const cluster = await io.readCluster(), tags = object(cluster.tags), properties = object(cluster.properties);
  const capacity = qapPolicy(await io.management('.show cluster policy capacity'), receipt.expanded);
  const effective = effectiveQapCapacity(await io.management('.show capacity'));
  const policyRestored = canonical(capacity) === canonical(qapPolicy(receipt.initialCapacityPolicy)) &&
    !(receipt.policyMutations ?? []).some(mutation => !['verified', 'not-submitted'].includes(mutation.state));
  const profileRestored = profileMatches(profileFromCluster(cluster), receipt.initialProfile, true);
  return { profile: profileFromCluster(cluster), state: properties.state, provisioningState: properties.provisioningState,
    owned: tags[OWNER_TAG] === receipt.owner, capacity, effective, policyRestored, profileRestored,
    restored: policyRestored && profileRestored && canonical(tags) === canonical(receipt.initial.tags) &&
      properties.state === 'Running' && properties.provisioningState === 'Succeeded' &&
      !receipt.mutations.some(mutation => mutation.state !== 'verified'),
    lastMutation: receipt.mutations.at(-1), lastPolicyMutation: receipt.policyMutations?.at(-1) };
}

export async function controlCluster(command: 'apply' | 'restore', receipt: Receipt, options: {
  profile?: unknown; capacity?: unknown; observations?: unknown[];
}, io: ControllerIO): Promise<Row> {
  validateReceipt(receipt, receipt.clusterId);
  let cluster = await io.readCluster();
  requireHealthy(cluster);
  await reconcileCluster(receipt, cluster, io);
  const actual = profileFromCluster(cluster), tags = object(cluster.tags), initialTags = object(receipt.initial.tags);
  const previous = receipt.mutations.at(-1);
  const withoutOwner = { ...tags };
  delete withoutOwner[OWNER_TAG];
  if (canonical(withoutOwner) !== canonical(initialTags)) throw new Error('Unrelated tags changed; refusing overwrite.');
  if (tags[OWNER_TAG] ? tags[OWNER_TAG] !== receipt.owner : previous && previous.kind !== 'restore')
    throw new Error('Campaign ownership was lost.');
  const expected = previous?.profile ?? receipt.initialProfile;
  if (!profileMatches(actual, expected, true)) throw new Error('Cluster changed outside the campaign; refusing overwrite.');
  let requestedProfile = actual;
  if (command === 'apply') {
    if (receipt.schemaVersion !== 2 || !receipt.background) throw new Error('Apply requires a new version 2 background snapshot.');
    requireReserve(receipt, io);
    if ((!options.profile && !options.capacity) || !options.observations || !observedQapPressure(options.observations))
      throw new Error('Apply requires a sanitized profile/capacity and sustained measured QAP pressure.');
    const profile = options.profile ? validateClusterProfile(options.profile, receipt.expanded) : actual;
    requestedProfile = profile;
    const capacity = options.capacity ? validateQapCapacity(options.capacity, receipt.expanded) : undefined;
    if (capacity && capacity.ClusterMaximumConcurrentOperations > 100 &&
        (profile.sku !== 'Standard_E32ads_v5' || profile.autoscale.enabled ||
          capacity.ClusterMaximumConcurrentOperations > profile.nodes * CORES[profile.sku]))
      throw new Error('Expanded QAP cap must fit the fixed E32ads_v5 trial topology.');
    await reconcilePolicy(receipt, io);
    requireSameBackground(receipt.background, await io.background());
    const available = await io.availableSkus();
    if (!Array.isArray(available) || !available.some(item => object(object(item).sku).name === profile.sku))
      throw new Error('Requested SKU is not available on this cluster.');
    await reconcilePolicy(receipt, io);
    await changeCluster(receipt, cluster, profile, { ...tags, [OWNER_TAG]: receipt.owner }, 'apply', io);
    // Resizing can take minutes. Re-attest before a second, independent management mutation.
    if (capacity) {
      requireSameBackground(receipt.background, await io.background());
      await changeCapacity(receipt, capacity, 'apply', io);
    }
    delete receipt.restoredAt;
  } else {
    const initialCapacity = qapPolicy(receipt.initialCapacityPolicy);
    // Policy restoration must complete before any SKU restoration or ownership release.
    await changeCapacity(receipt, initialCapacity, 'restore', io);
    cluster = await io.readCluster();
    requireHealthy(cluster);
    if (!profileMatches(profileFromCluster(cluster), expected, true) || canonical(cluster.tags) !== canonical(tags))
      throw new Error('Cluster changed during policy restoration; refusing overwrite.');
    await changeCluster(receipt, cluster, receipt.initialProfile, initialTags, 'restore', io);
  }
  const status = await clusterStatus(receipt, io);
  if (command === 'restore') {
    if (!status.restored) throw new Error('Restoration readback is incomplete; ownership/policy receipts require reconciliation.');
    receipt.restoredAt = new Date(io.now()).toISOString();
  } else if (!status.owned || status.state !== 'Running' || status.provisioningState !== 'Succeeded' ||
      !profileMatches(validateClusterProfile(status.profile, receipt.expanded), requestedProfile, true) ||
      canonical(status.capacity) !== canonical(expectedPolicy(receipt))) {
    throw new Error('Final campaign readback differs from the owned profile or capacity policy.');
  }
  await io.save(receipt);
  return status;
}

async function main(argv: string[]): Promise<void> {
  const { values, positionals } = parseArgs({ args: argv, allowPositionals: true, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, receipt: { type: 'string' },
    profile: { type: 'string' }, capacity: { type: 'string' }, pressure: { type: 'string' }, deadline: { type: 'string' },
    'expanded-fixed-e32': { type: 'boolean' },
  } });
  if (positionals.length !== 1 || !['snapshot', 'apply', 'restore', 'status'].includes(positionals[0]!) ||
      !values.receipt || !values['env-file'] || !values['stamp-config'])
    throw new Error('Usage: aks-qap-cluster.ts snapshot|apply|restore|status --env-file PATH --stamp-config YAML --receipt PATH ' +
      '[--deadline ISO | --profile JSON_FILE and/or --capacity JSON_FILE --pressure JSONL]');
  const c = new Config(load_env(resolve(values['env-file'])), loadStampConfig(resolve(values['stamp-config'])));
  const azure = new Azure(c, 120);
  const monitors = new Map<string, Kusto>();
  const management: Management = (database, command) => {
    if (!monitors.has(database)) monitors.set(database, new Kusto(azure, database, 'qap-cluster-control', undefined, 1));
    return monitors.get(database)!.management(command, AbortSignal.timeout(60_000));
  };
  const file = resolve(values.receipt), command = positionals[0]!;
  if (values['expanded-fixed-e32'] && command !== 'snapshot')
    throw new Error('--expanded-fixed-e32 must be bound at snapshot time.');
  const io: ControllerIO = {
    readCluster: async () => {
      const cluster = object(await azure.arm('GET', c.cluster_id)), tags = object(cluster.tags);
      if (tags.project !== 'rust-playground' || tags.purpose !== 'disposable-arrowcol-sandbox' || tags.sandboxId !== `stamp-${c.stamp}`)
        throw new Error('Cluster ownership tags do not identify the selected sandbox.');
      return cluster;
    },
    patchCluster: async (cluster, body) => {
      const [, headers] = await azure.request('PATCH',
        `https://management.azure.com${c.cluster_id}?api-version=${API}`, 'https://management.azure.com/',
        JSON.stringify(body), { 'Content-Type': 'application/json', 'If-Match': String(cluster.etag) });
      return headers;
    },
    management: command => management(c.experimentDatabase, command),
    background: () => captureBackground([c.database, c.experimentDatabase], management),
    availableSkus: () => azure.cli('kusto', 'cluster', 'list-sku', '-g', c.group, '-n', c.cluster),
    save: receipt => writeReceipt(file, receipt), now: Date.now, sleep,
  };
  if (command === 'snapshot') {
    const deadlineMs = Date.parse(values.deadline ?? '');
    if (!Number.isSafeInteger(deadlineMs) || deadlineMs <= Date.now() || deadlineMs - Date.now() > 24 * 3600_000)
      throw new Error('A future campaign deadline within 24 hours is required.');
    const cluster = await io.readCluster();
    requireHealthy(cluster);
    if (object(cluster.tags)[OWNER_TAG]) throw new Error('Another QAP campaign owns this cluster.');
    const initialCapacityPolicy = await io.management('.show cluster policy capacity');
    const background = await io.background();
    const current = await io.readCluster();
    if (canonical(current) !== canonical(cluster) ||
        canonical(qapPolicy(await io.management('.show cluster policy capacity'))) !== canonical(qapPolicy(initialCapacityPolicy)))
      throw new Error('Cluster or capacity policy changed during snapshot.');
    const receipt: Receipt = { schemaVersion: 2, ...(values['expanded-fixed-e32'] ? { expanded: true } : {}),
      owner: randomUUID(), clusterId: c.cluster_id, deadlineMs,
      initial: cluster, initialProfile: profileFromCluster(cluster), mutations: [], policyMutations: [],
      initialCapacityPolicy, background };
    validateReceipt(receipt, c.cluster_id);
    await writeFile(file, JSON.stringify(receipt, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log(`SNAPSHOT v2 saved; ${background.tables.length} idle missing-source tables recorded as catalog-refresh confounders; no cloud changes.`);
    return;
  }
  const receipt = await loadReceipt(file, azure);
  if (command === 'status') { console.log(JSON.stringify(await clusterStatus(receipt, io))); return; }
  const json = async (path: string | undefined) => path ? JSON.parse(await readFile(resolve(path), 'utf8')) as unknown : undefined;
  const observations = command === 'apply' && values.pressure ?
    (await readFile(resolve(values.pressure), 'utf8')).trim().split('\n').map(line => JSON.parse(line)) : undefined;
  console.log(JSON.stringify(await controlCluster(command as 'apply' | 'restore', receipt,
    command === 'apply' ? { profile: await json(values.profile), capacity: await json(values.capacity), observations } : {}, io)));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
