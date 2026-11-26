import { randomUUID } from 'node:crypto';
import { lstat, readFile, rename, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { API, Azure, Config, load_env } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { Kusto, object, primary, tableRows } from '../../experiments/azure-qap/src/kusto.js';

const CORES = { Standard_E4ads_v5: 4, Standard_E8ads_v5: 8, Standard_E16ads_v5: 16, Standard_E32ads_v5: 32 } as const;
const OWNER_TAG = 'qapCampaignOwner';
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
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
interface Receipt {
  schemaVersion: 1;
  owner: string;
  clusterId: string;
  deadlineMs: number;
  initial: Row;
  initialCapacityPolicy: unknown;
  initialProfile: ClusterProfile;
  mutations: Mutation[];
  restoredAt?: string;
}

function integer(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value)) throw new Error(`${name} must be a safe integer.`);
  return value;
}

function keys(row: Row, expected: string[], name: string): void {
  if (Object.keys(row).sort().join(',') !== expected.sort().join(',')) throw new Error(`${name} has unexpected fields.`);
}

export function validateClusterProfile(value: unknown): ClusterProfile {
  const row = object(value);
  keys(row, ['sku', 'nodes', 'autoscale'], 'Cluster profile');
  if (typeof row.sku !== 'string' || !Object.hasOwn(CORES, row.sku)) throw new Error('Unsupported benchmark SKU.');
  const sku = row.sku as keyof typeof CORES;
  const nodes = integer(row.nodes, 'nodes');
  const scale = object(row.autoscale);
  keys(scale, ['enabled', 'minimum', 'maximum'], 'Autoscale');
  if (typeof scale.enabled !== 'boolean') throw new Error('Autoscale enabled must be boolean.');
  const minimum = integer(scale.minimum, 'minimum'), maximum = integer(scale.maximum, 'maximum');
  if (minimum < 2 || maximum > 8 || minimum > nodes || nodes > maximum || maximum * CORES[sku] > 64)
    throw new Error('Profile exceeds 64 engine vCPUs / 8 nodes or has invalid node bounds.');
  if (!scale.enabled && (minimum !== nodes || maximum !== nodes)) throw new Error('Fixed profiles must pin both bounds to nodes.');
  return { sku, nodes, autoscale: { enabled: scale.enabled, minimum, maximum } };
}

export function profileFromCluster(value: unknown): ClusterProfile {
  const cluster = object(value), sku = object(cluster.sku), properties = object(cluster.properties);
  const scale = object(properties.optimizedAutoscale);
  return validateClusterProfile({ sku: sku.name, nodes: sku.capacity,
    autoscale: { enabled: scale.isEnabled, minimum: scale.minimum, maximum: scale.maximum } });
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
    const capacity = tableRows(primary(row.capacity)).find(item => item.Resource === 'QueryAcceleration');
    if (typeof state.pendingFiles !== 'number' || !capacity ||
        typeof capacity.Total !== 'number' || typeof capacity.Consumed !== 'number') return [];
    return [{ at: integer(row.endedAtMs, 'observation time'), pending: state.pendingFiles,
      saturated: capacity.Total > 0 && capacity.Consumed >= capacity.Total }];
  });
  if (valid.length < 3 || valid.at(-1)!.at - valid[0]!.at < 30_000) return false;
  return valid.filter(row => row.pending > 0 && row.saturated).length >= 3 ||
    valid.at(-1)!.pending > valid[0]!.pending && valid.at(-1)!.pending > 0;
}

async function writeReceipt(path: string, receipt: Receipt): Promise<void> {
  const temp = `${path}.${randomUUID()}.tmp`;
  await writeFile(temp, JSON.stringify(receipt, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
  await rename(temp, path);
}

async function loadReceipt(path: string, azure: Azure): Promise<Receipt> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) throw new Error('Receipt must be an owner-only regular file.');
  const receipt = JSON.parse(await readFile(path, 'utf8')) as Receipt;
  if (receipt.schemaVersion !== 1 || receipt.clusterId !== azure.c.cluster_id ||
      !/^[0-9a-f-]{36}$/.test(receipt.owner) || !Array.isArray(receipt.mutations) ||
      !Number.isSafeInteger(receipt.deadlineMs)) throw new Error('Receipt identity is invalid.');
  validateClusterProfile(receipt.initialProfile);
  return receipt;
}

function requireHealthy(cluster: Row): void {
  const p = object(cluster.properties);
  if (p.state !== 'Running' || p.provisioningState !== 'Succeeded') throw new Error('Cluster is not stable and running.');
}

async function main(argv: string[]): Promise<void> {
  const { values, positionals } = parseArgs({ args: argv, allowPositionals: true, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, receipt: { type: 'string' },
    profile: { type: 'string' }, pressure: { type: 'string' }, deadline: { type: 'string' },
  } });
  if (positionals.length !== 1 || !['snapshot', 'apply', 'restore', 'status'].includes(positionals[0]!) ||
      !values.receipt || !values['env-file'] || !values['stamp-config'])
    throw new Error('Usage: aks-qap-cluster.ts snapshot|apply|restore|status --env-file PATH --stamp-config YAML --receipt PATH [--deadline ISO | --profile JSON --pressure JSONL]');
  const c = new Config(load_env(resolve(values['env-file'])), loadStampConfig(resolve(values['stamp-config'])));
  const azure = new Azure(c, 120), kusto = new Kusto(azure, c.experimentDatabase, 'qap-cluster-control', undefined, 1);
  const file = resolve(values.receipt), command = positionals[0]!;
  const cluster = object(await azure.arm('GET', c.cluster_id));
  const tags = object(cluster.tags);
  if (tags.project !== 'rust-playground' || tags.purpose !== 'disposable-arrowcol-sandbox' || tags.sandboxId !== `stamp-${c.stamp}`)
    throw new Error('Cluster ownership tags do not identify the selected sandbox.');
  if (command === 'snapshot') {
    const deadlineMs = Date.parse(values.deadline ?? '');
    if (!Number.isSafeInteger(deadlineMs) || deadlineMs <= Date.now() || deadlineMs - Date.now() > 24 * 3600_000)
      throw new Error('A future campaign deadline within 24 hours is required.');
    requireHealthy(cluster);
    if (tags[OWNER_TAG]) throw new Error('Another QAP campaign owns this cluster.');
    const receipt: Receipt = { schemaVersion: 1, owner: randomUUID(), clusterId: c.cluster_id, deadlineMs,
      initial: cluster, initialProfile: profileFromCluster(cluster), mutations: [],
      initialCapacityPolicy: await kusto.management('.show cluster policy capacity', AbortSignal.timeout(60_000)) };
    await writeFile(file, JSON.stringify(receipt, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    console.log('SNAPSHOT saved; no cloud configuration changed.');
    return;
  }
  const receipt = await loadReceipt(file, azure);
  const actual = profileFromCluster(cluster);
  if (command === 'status') {
    console.log(JSON.stringify({ profile: actual, state: object(cluster.properties).state,
      provisioningState: object(cluster.properties).provisioningState, owned: tags[OWNER_TAG] === receipt.owner,
      restored: profileMatches(actual, receipt.initialProfile, true) && !tags[OWNER_TAG], lastMutation: receipt.mutations.at(-1) }));
    return;
  }
  requireHealthy(cluster);
  const initialTags = object(receipt.initial.tags);
  let profile: ClusterProfile;
  let nextTags: Row;
  if (command === 'apply') {
    if (Date.now() >= receipt.deadlineMs - 90 * 60_000) throw new Error('Campaign is inside its 90-minute restoration reserve.');
    if (!values.profile || !values.pressure) throw new Error('Apply requires a sanitized profile and actual pressure evidence.');
    profile = validateClusterProfile(JSON.parse(await readFile(resolve(values.profile), 'utf8')));
    const observations = (await readFile(resolve(values.pressure), 'utf8')).trim().split('\n').map(line => JSON.parse(line));
    if (!observedQapPressure(observations)) throw new Error('No measured QAP pressure justifies a configuration experiment.');
    const previous = receipt.mutations.at(-1);
    if (tags[OWNER_TAG] ? tags[OWNER_TAG] !== receipt.owner : previous && previous.kind !== 'restore')
      throw new Error('Campaign ownership was lost.');
    const expected = previous?.state === 'verified' ? previous.profile : receipt.initialProfile;
    if (!profileMatches(actual, expected, true)) throw new Error('Cluster changed outside the campaign; refusing overwrite.');
    for (const database of new Set([c.database, c.experimentDatabase])) {
      const monitor = new Kusto(azure, database, 'qap-cluster-preflight', undefined, 1);
      const acceleration = primary(await monitor.management('.show external tables operations query_acceleration statistics',
        AbortSignal.timeout(60_000)));
      if (acceleration.rows.length) throw new Error('An accelerated table is still active; settle its owner before resizing.');
    }
    const available = await azure.cli('kusto', 'cluster', 'list-sku', '-g', c.group, '-n', c.cluster);
    if (!Array.isArray(available) || !available.some(item => object(object(item).sku).name === profile.sku))
      throw new Error('Requested SKU is not available on this cluster.');
    nextTags = { ...tags, [OWNER_TAG]: receipt.owner };
  } else {
    profile = receipt.initialProfile;
    if (!tags[OWNER_TAG] && profileMatches(actual, profile, true)) {
      receipt.restoredAt = new Date().toISOString();
      await writeReceipt(file, receipt);
      console.log('RESTORED verified by fresh read; no mutation needed.');
      return;
    }
    if (tags[OWNER_TAG] !== receipt.owner) throw new Error('Campaign ownership was lost; refusing restoration overwrite.');
    const last = receipt.mutations.at(-1);
    if (!last || !profileMatches(actual, last.profile, true)) throw new Error('Live configuration differs from the last campaign intent.');
    const withoutOwner = { ...tags };
    delete withoutOwner[OWNER_TAG];
    if (JSON.stringify(Object.entries(withoutOwner).sort()) !== JSON.stringify(Object.entries(initialTags).sort()))
      throw new Error('Unrelated tags changed; refusing restoration overwrite.');
    nextTags = initialTags;
  }
  const mutation: Mutation = { kind: command === 'apply' ? 'apply' : 'restore',
    submittedAt: new Date().toISOString(), profile, state: 'intent' };
  receipt.mutations.push(mutation);
  await writeReceipt(file, receipt);
  if (typeof cluster.etag !== 'string') throw new Error('Cluster ETag is unavailable; conditional mutation refused.');
  try {
    const [, headers] = await azure.request('PATCH',
      `https://management.azure.com${c.cluster_id}?api-version=${API}`, 'https://management.azure.com/',
      JSON.stringify({ sku: { name: profile.sku, tier: 'Standard', capacity: profile.nodes }, tags: nextTags,
        properties: { optimizedAutoscale: { version: 1, isEnabled: profile.autoscale.enabled,
          minimum: profile.autoscale.minimum, maximum: profile.autoscale.maximum } } }),
      { 'Content-Type': 'application/json', 'If-Match': cluster.etag });
    mutation.state = 'accepted';
    mutation.requestId = headers['x-ms-request-id'];
    mutation.operationUrl = headers['azure-asyncoperation'];
    await writeReceipt(file, receipt);
  } catch (error) {
    mutation.state = 'uncertain';
    await writeReceipt(file, receipt);
    throw error;
  }
  const until = Date.now() + 45 * 60_000;
  while (Date.now() < until) {
    const current = object(await azure.arm('GET', c.cluster_id));
    const properties = object(current.properties);
    if (properties.provisioningState === 'Failed') throw new Error('Cluster update failed; receipt retained for reconciliation.');
    if (properties.provisioningState === 'Succeeded' && properties.state === 'Running' &&
        profileMatches(profileFromCluster(current), profile, command === 'restore' || profile.autoscale.enabled)) {
      if (JSON.stringify(Object.entries(object(current.tags)).sort()) !== JSON.stringify(Object.entries(nextTags).sort()))
        throw new Error('Configuration converged with unexpected ownership tags.');
      mutation.state = 'verified';
      if (command === 'restore') receipt.restoredAt = new Date().toISOString();
      await writeReceipt(file, receipt);
      console.log(`${command.toUpperCase()} verified: ${profile.sku}, ${profile.nodes} initial nodes, ` +
        `${profile.autoscale.enabled ? 'optimized' : 'fixed'} ${profile.autoscale.minimum}..${profile.autoscale.maximum}.`);
      return;
    }
    await sleep(15_000);
  }
  throw new Error('Cluster convergence timed out; do not resubmit the mutation. Reconcile status from the saved receipt.');
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
}
