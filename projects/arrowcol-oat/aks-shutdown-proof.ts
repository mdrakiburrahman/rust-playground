import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import type { RunInventory } from './aks-campaign.js';
import { readBlob, baselineDelta } from './aks-data-safety.js';
import { readFrozenDeltaAdds, type DeltaVersionAdds } from './aks-delta-preservation.js';
import { parseStop } from './aks-lifecycle.js';

export interface ShutdownRef { blob: string; etag: string; sha256: string }
export interface ShutdownMember { kind: 'collector' | 'client'; workload: {
  api_version: string; kind: string; name: string; namespace: string; uid: string | null };
  pod_name: string; pod_uid: string; container: string; restart_count: number }
export interface ShutdownProof { schema_version: 1; membership: ShutdownRef;
  collector_receipts: ShutdownRef[]; client_receipts: ShutdownRef[] }
export type NumericReconciliation = 'exact' | 'mismatch' | 'unavailable';
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> => object(value) &&
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const same = (a: unknown, b: unknown) => canonical_json(a) === canonical_json(b);
export const shutdownDigest = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
export const shutdownOrigin = (inventory: RunInventory) => ({ campaign_id: inventory.campaign_id, owner_id: inventory.owner_id,
  fencing_token: inventory.fencing_token, orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id,
  namespace: inventory.namespace });
export const shutdownRoot = (inventory: RunInventory) => `_arrowcol-custody/campaigns/${inventory.campaign_id}` +
  `/runs/${inventory.orchestration_run_id}/cells/${inventory.cell_run_id}`;
const uint = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const bounded = (value: unknown): value is string => typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,252}$/.test(value);
function requireThat(value: unknown, reason: string): asserts value { if (!value) throw new Error(reason); }
export function parseShutdownRef(value: unknown): ShutdownRef {
  requireThat(exact(value, ['blob', 'etag', 'sha256']) && typeof value.blob === 'string' &&
    value.blob.length <= 2048 && value.blob.split('/').every(part => /^[a-zA-Z0-9_-][a-zA-Z0-9_.-]*$/.test(part) &&
      part !== '.' && part !== '..') && typeof value.etag === 'string' && /^"[^"\r\n]+"$/.test(value.etag) &&
    value.etag.length <= 512 && typeof value.sha256 === 'string' && /^[a-f0-9]{64}$/.test(value.sha256),
    'Invalid exact shutdown source reference.');
  return { blob: value.blob, etag: value.etag, sha256: value.sha256 };
}
export function parseShutdownProof(value: unknown): ShutdownProof {
  requireThat(exact(value, ['schema_version', 'membership', 'collector_receipts', 'client_receipts']) && value.schema_version === 1 &&
    Array.isArray(value.collector_receipts) && Array.isArray(value.client_receipts) &&
    value.collector_receipts.length > 0 && value.client_receipts.length > 0 &&
    value.collector_receipts.length <= 512 && value.client_receipts.length <= 512, 'Invalid numeric shutdown proof.');
  const collectors = value.collector_receipts.map(parseShutdownRef), clients = value.client_receipts.map(parseShutdownRef);
  requireThat(new Set([...collectors, ...clients].map(value => value.blob)).size === collectors.length + clients.length,
    'Duplicate numeric shutdown receipt.');
  return { schema_version: 1, membership: parseShutdownRef(value.membership), collector_receipts: collectors, client_receipts: clients };
}
export function parseShutdownMember(value: unknown, inventory: RunInventory): ShutdownMember {
  requireThat(exact(value, ['kind', 'workload', 'pod_name', 'pod_uid', 'container', 'restart_count']) &&
    (value.kind === 'collector' || value.kind === 'client') && bounded(value.pod_name) && bounded(value.pod_uid) &&
    bounded(value.container) && uint(value.restart_count) && value.restart_count === 0 &&
    exact(value.workload, ['api_version', 'kind', 'name', 'namespace', 'uid']), 'Invalid original shutdown member.');
  const resource = inventory.resources.find(resource => resource.role === value.kind && resource.state !== 'absent' &&
    same({ api_version: resource.api_version, kind: resource.kind, name: resource.name,
      namespace: resource.namespace, uid: resource.uid }, value.workload));
  requireThat(resource?.uid, 'Shutdown member does not bind an original inventory workload.');
  return { kind: value.kind, workload: { api_version: resource.api_version, kind: resource.kind,
    name: resource.name, namespace: resource.namespace, uid: resource.uid }, pod_name: value.pod_name,
    pod_uid: value.pod_uid, container: value.container, restart_count: value.restart_count };
}
export function parseShutdownMembership(value: unknown, inventory: RunInventory) {
  requireThat(object(value) && exact(value.schedule, ['name', 'uid', 'resource_version', 'body_sha256']), 'Invalid schedule witness.');
  const schedule = value.schedule;
  requireThat(exact(value, ['schema_version', 'origin', 'actor', 'inventory_etag', 'table_root', 'collector_table_id',
    'collector_table_name', 'schedule', 'members']) && value.schema_version === 1 &&
    same(value.origin, shutdownOrigin(inventory)) && exact(value.actor, ['mode', 'owner_id', 'fencing_token']) &&
    value.actor.mode === 'producer' && value.actor.owner_id === inventory.owner_id &&
    value.actor.fencing_token === inventory.fencing_token &&
    typeof value.inventory_etag === 'string' && /^"[^"\r\n]+"$/.test(value.inventory_etag) &&
    typeof value.table_root === 'string' && inventory.qap?.bindings.some(binding => binding.relative_path === value.table_root) &&
    typeof value.collector_table_id === 'string' && /^[a-f0-9]{64}$/.test(value.collector_table_id) &&
    typeof value.collector_table_name === 'string' && /^[a-z0-9_]+\.[a-z0-9_]+\.[a-z0-9_]+$/.test(value.collector_table_name) &&
    exact(value.schedule, ['name', 'uid', 'resource_version', 'body_sha256']) &&
    bounded(schedule.name) && bounded(schedule.uid) &&
    typeof schedule.resource_version === 'string' && schedule.resource_version.length > 0 &&
    typeof schedule.body_sha256 === 'string' && /^[a-f0-9]{64}$/.test(schedule.body_sha256) &&
    Array.isArray(value.members) && value.members.length > 0 && value.members.length <= 512, 'Invalid authenticated pre-arm membership.');
  const members = value.members.map(member => parseShutdownMember(member, inventory));
  requireThat(new Set(members.map(member => member.pod_uid + ':' + member.container)).size === members.length &&
    members.some(member => member.kind === 'client') && members.some(member => member.kind === 'collector') &&
    inventory.resources.filter(resource => ['collector', 'client'].includes(resource.role) && ['Deployment', 'Job'].includes(resource.kind) && resource.state !== 'absent')
      .every(resource => members.some(member => member.workload.uid === resource.uid)), 'Incomplete original shutdown membership.');
  return { ...value, members, schedule, table_root: value.table_root, collector_table_id: value.collector_table_id,
    collector_table_name: value.collector_table_name };
}
export function shutdownReceiptPath(inventory: RunInventory, member: ShutdownMember) {
  return `${shutdownRoot(inventory)}/${member.kind}-receipts/${member.pod_uid}-${member.container}-${member.restart_count}.json`;
}
function parseCommit(value: unknown): DeltaVersionAdds {
  requireThat(exact(value, ['delta_version', 'rows', 'files', 'parquet_bytes']) &&
    uint(value.delta_version) && uint(value.rows) && uint(value.files) && uint(value.parquet_bytes) &&
    value.files > 0 && value.rows > 0 && value.parquet_bytes > 0, 'Invalid numeric commit projection.');
  return { delta_version: value.delta_version, rows: value.rows, files: value.files, parquet_bytes: value.parquet_bytes };
}
export function projectShutdownLogs(kind: ShutdownMember['kind'], text: string, tableId: string, tableName: string) {
  const commits: DeltaVersionAdds[] = [];
  let capability = false, drained = false, ambiguity: number | null = null;
  const snapshots: Array<Record<string, unknown>> = [];
  let invalid = false;
  for (const line of text.split('\n').filter(Boolean)) {
    const match = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z (.*)$/.exec(line);
    if (!match) { invalid = true; continue; }
    const payload = match[1];
    try {
      if (payload.startsWith('arrowcol_effective_config ')) {
        const value: unknown = JSON.parse(payload.slice('arrowcol_effective_config '.length));
        if (object(value) && same(value.commit_telemetry,
          { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' })) capability = true;
      } else if (payload.startsWith('arrowcol_data_commit ')) {
        const value: unknown = JSON.parse(payload.slice('arrowcol_data_commit '.length));
        requireThat(object(value) && value.schema_version === 1 && value.outcome === 'committed' &&
          value.table_id === tableId && value.table_name === tableName, 'Unbound collector commit event.');
        const commit = parseCommit({ delta_version: value.delta_version, rows: value.rows, files: value.files, parquet_bytes: value.parquet_bytes });
        requireThat(!commits.some(prior => prior.delta_version === commit.delta_version), 'Duplicate collector commit.');
        commits.push(commit);
      } else if (payload.startsWith('arrowcol_shutdown ')) {
        const value: unknown = JSON.parse(payload.slice('arrowcol_shutdown '.length));
        if (object(value) && value.event === 'drain_complete') drained = true;
        if (object(value) && uint(value.cancelled_ambiguous)) ambiguity = value.cancelled_ambiguous;
      } else if (payload.startsWith('OAT_CLIENT_METRICS ')) {
        const value: unknown = JSON.parse(payload.slice('OAT_CLIENT_METRICS '.length));
        requireThat(object(value) && value.version === 1 && value.scope === 'process_cumulative' &&
          Array.isArray(value.workers), 'Invalid terminal client snapshot.');
        snapshots.push(value);
      } else if (payload.includes('cancelled admissions had no observed publication outcome')) invalid = true;
    } catch { invalid = true; }
  }
  let completed: number | null = null, clientAmbiguity: number | null = null;
  if (kind === 'client') {
    const initial = snapshots.filter(snapshot => snapshot.phase === 'measurement_start');
    const ended = snapshots.filter(snapshot => snapshot.phase === 'completed');
    if (initial.length === 1 && ended.length === 1 && Array.isArray(initial[0].workers) && Array.isArray(ended[0].workers)) {
      const startWorkers = initial[0].workers, endWorkers = ended[0].workers;
      if (startWorkers.length > 0 && startWorkers.length <= 1024 && endWorkers.length === startWorkers.length) {
        let rows = 0, ambiguous = 0;
        const ids = new Set<number>();
        for (const value of endWorkers) {
          if (!object(value) || !uint(value.worker) || ids.has(value.worker) || !uint(value.completed_rows) ||
              !uint(value.ambiguous_rows)) { invalid = true; break; }
          ids.add(value.worker);
          const start = startWorkers.find(before => object(before) && before.worker === value.worker);
          if (!object(start) || !uint(start.completed_rows) || start.completed_rows > value.completed_rows) { invalid = true; break; }
          rows += value.completed_rows; ambiguous += value.ambiguous_rows;
          if (!uint(rows) || !uint(ambiguous)) { invalid = true; break; }
        }
        if (!invalid) { completed = rows; clientAmbiguity = ambiguous; }
      }
    }
  }
  return { commits: commits.sort((a, b) => a.delta_version - b.delta_version),
    cancelled_ambiguous: ambiguity, completed_rows: completed, ambiguous_rows: clientAmbiguity,
    ingestion_only: capability, drain_complete: drained, malformed: invalid,
    continuity: 'unproven' as const };
}
export function compareShutdownNumbers(delta: DeltaVersionAdds[], collectors: DeltaVersionAdds[][],
  clients: Array<{ completed_rows: number; ambiguous_rows: number }>): NumericReconciliation {
  const projected = collectors.flat().map(parseCommit).sort((a, b) => a.delta_version - b.delta_version);
  if (new Set(projected.map(value => value.delta_version)).size !== projected.length) return 'mismatch';
  let acknowledged = 0, rows = 0;
  for (const client of clients) {
    if (!uint(client.completed_rows) || !uint(client.ambiguous_rows)) return 'unavailable';
    if (client.ambiguous_rows !== 0) return 'mismatch';
    acknowledged += client.completed_rows;
    if (!uint(acknowledged)) return 'unavailable';
  }
  for (const value of delta) { rows += value.rows; if (!uint(rows)) return 'unavailable'; }
  return same(delta, projected) && rows === acknowledged ? 'exact' : 'mismatch';
}
export async function readShutdownNumericProof(azure: Azure, inventory: RunInventory, raw: unknown,
  sources: { terminal_capture: ShutdownRef; delta_before: ShutdownRef; delta_terminal: ShutdownRef },
  guard: () => Promise<void>): Promise<NumericReconciliation> {
  const proof = parseShutdownProof(raw);
  const read = async (ref: ShutdownRef, path: string) => {
    requireThat(ref.blob === path, 'Shutdown proof source escaped exact original path.');
    const saved = await readBlob(azure, path, guard);
    await guard();
    requireThat(saved && saved.etag === ref.etag && shutdownDigest(saved.value) === ref.sha256, 'Shutdown proof source identity changed.');
    return saved.value;
  };
  const membership = parseShutdownMembership(await read(proof.membership, `${shutdownRoot(inventory)}/shutdown-membership.json`), inventory);
  const capture = await read(sources.terminal_capture, sources.terminal_capture.blob);
  requireThat(Array.isArray(capture.workloads), 'Terminal membership source unavailable.');
  const collectorVersions: DeltaVersionAdds[][] = [], clients: Array<{ completed_rows: number; ambiguous_rows: number }> = [];
  let coverage = true;
  for (const member of membership.members) {
    const resource = inventory.resources.find(value => value.uid === member.workload.uid && value.role === member.kind)!;
    const workloads = capture.workloads.filter(value => object(value) && same(value.identity, member.workload));
    requireThat(workloads.length === 1 && object(workloads[0]), 'Original terminal workload coverage differs.');
    const stop = parseStop(workloads[0].original_stop, inventory, member.kind === 'collector' ? 'bounded_drain' : 'stop_admission', resource);
    const first = stop.initial_pods.find(pod => pod.name === member.pod_name && pod.uid === member.pod_uid);
    const ended = stop.terminal_pods.find(pod => pod.name === member.pod_name && pod.uid === member.pod_uid);
    requireThat(first?.containers.some(container => container.name === member.container && container.restart_count === member.restart_count) &&
      ended?.containers.some(container => container.name === member.container && container.restart_count === member.restart_count &&
        container.exit_code !== null && !container.oom_killed), 'Original terminal instance coverage differs.');
    const expected = shutdownReceiptPath(inventory, member);
    const refs = member.kind === 'collector' ? proof.collector_receipts : proof.client_receipts;
    const matches = refs.filter(ref => ref.blob === expected);
    requireThat(matches.length === 1, 'Expected numeric receipt unavailable.');
    const receipt = await read(matches[0], expected);
    requireThat(exact(receipt, ['schema_version', 'origin', 'actor', 'inventory_etag', 'membership', 'member', 'table_root',
      'collector_table_id', 'collector_table_name', 'source', 'projection']) && receipt.schema_version === 1 &&
      same(receipt.origin, shutdownOrigin(inventory)) && same(receipt.membership, proof.membership) &&
      same(receipt.member, member) && receipt.table_root === membership.table_root &&
      receipt.collector_table_id === membership.collector_table_id && receipt.collector_table_name === membership.collector_table_name &&
      exact(receipt.actor, ['mode', 'owner_id', 'fencing_token']) && receipt.actor.owner_id === inventory.owner_id &&
      (receipt.actor.mode === 'producer' ? receipt.actor.fencing_token === inventory.fencing_token :
        receipt.actor.mode === 'controller' && uint(receipt.actor.fencing_token) && receipt.actor.fencing_token > inventory.fencing_token) &&
      typeof receipt.inventory_etag === 'string' && /^"[^"\r\n]+"$/.test(receipt.inventory_etag) &&
      exact(receipt.source, ['body_sha256', 'bytes']) && typeof receipt.source.body_sha256 === 'string' &&
      /^[a-f0-9]{64}$/.test(receipt.source.body_sha256) && uint(receipt.source.bytes) && receipt.source.bytes > 0 &&
      receipt.source.bytes <= 16_777_216 && exact(receipt.projection, ['commits', 'cancelled_ambiguous', 'completed_rows',
        'ambiguous_rows', 'ingestion_only', 'drain_complete', 'malformed', 'continuity']) &&
      receipt.projection.continuity === 'unproven', 'Invalid authenticated numeric receipt.');
    const projection = receipt.projection;
    requireThat(Array.isArray(projection.commits) && projection.commits.length <= 20_000, 'Projected commit bound exceeded.');
    coverage &&= projection.malformed === false;
    if (member.kind === 'collector') {
      // Kubernetes log responses have no lossless continuity contract. Neither markers nor caller flags upgrade them.
      coverage = false;
      collectorVersions.push(projection.commits.map(parseCommit));
    } else {
      if (!uint(projection.completed_rows) || !uint(projection.ambiguous_rows)) { coverage = false; continue; }
      clients.push({ completed_rows: projection.completed_rows, ambiguous_rows: projection.ambiguous_rows });
    }
  }
  requireThat(proof.collector_receipts.length === membership.members.filter(member => member.kind === 'collector').length &&
    proof.client_receipts.length === membership.members.filter(member => member.kind === 'client').length,
    'Numeric proof contains unexpected receipt membership.');
  const before = baselineDelta(await read(sources.delta_before, sources.delta_before.blob), inventory, azure);
  const terminal = baselineDelta(await read(sources.delta_terminal, sources.delta_terminal.blob), inventory, azure, 'terminal_after_writers_stopped');
  const versions = await readFrozenDeltaAdds(azure, before, terminal, guard);
  if (!coverage) return 'unavailable';
  return compareShutdownNumbers(versions, collectorVersions, clients);
}
