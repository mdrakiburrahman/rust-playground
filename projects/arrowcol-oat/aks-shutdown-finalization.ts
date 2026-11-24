import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CampaignWriteOutcomeUnknownError, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { readCellRunInventory, type CellCleanupActor } from './aks-cell-cleanup.js';
import { baselineDelta, immutableBlob, readBlob } from './aks-data-safety.js';
import { createDefaultLifecycleActions, parseStop, type LifecycleActions } from './aks-lifecycle.js';
import { parseShutdownMembership, parseShutdownProof, parseShutdownRef, projectShutdownLogs, readShutdownNumericProof,
  shutdownReceiptPath, shutdownRoot, type ShutdownProof } from './aks-shutdown-proof.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

interface Ref { blob: string; etag: string; sha256: string }
interface Input { schema_version: 1; campaign_id: string; orchestration_run_id: string; cell_run_id: string;
  expected_etag: string; actor: CellCleanupActor }
interface Verdict { pod_name: string; pod_uid: string; container: string; restart_count: number | null; classification: string }
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> => object(value) &&
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const digest = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
const origin = (inventory: RunInventory) => ({ campaign_id: inventory.campaign_id, owner_id: inventory.owner_id,
  fencing_token: inventory.fencing_token, orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id,
  namespace: inventory.namespace });
const cellBlob = (inventory: RunInventory, name: string) => `_arrowcol-custody/campaigns/${inventory.campaign_id}` +
  `/runs/${inventory.orchestration_run_id}/cells/${inventory.cell_run_id}/${name}.json`;
function parseInput(value: unknown, extra: string[] = []): Input {
  if (!exact(value, ['schema_version', 'campaign_id', 'orchestration_run_id', 'cell_run_id', 'expected_etag', 'actor', ...extra]) ||
      value.schema_version !== 1 || typeof value.campaign_id !== 'string' || typeof value.orchestration_run_id !== 'string' ||
      typeof value.cell_run_id !== 'string' || typeof value.expected_etag !== 'string' ||
      !exact(value.actor, ['mode', 'owner_id', 'fencing_token']) ||
      !['producer', 'controller'].includes(String(value.actor.mode)) || typeof value.actor.owner_id !== 'string' ||
      typeof value.actor.fencing_token !== 'number' || !Number.isSafeInteger(value.actor.fencing_token))
    throw new Error('Invalid exact shutdown finalization request.');
  return { schema_version: 1, campaign_id: value.campaign_id, orchestration_run_id: value.orchestration_run_id,
    cell_run_id: value.cell_run_id, expected_etag: value.expected_etag,
    actor: { mode: value.actor.mode as CellCleanupActor['mode'], owner_id: value.actor.owner_id, fencing_token: value.actor.fencing_token } };
}
function classify(inventory: RunInventory, capture: Record<string, unknown>): Verdict[] {
  if (capture.schema_version !== 1 || capture.field !== 'terminal_capture' ||
      canonical_json(capture.origin) !== canonical_json(origin(inventory)) ||
      capture.scope !== 'exact_kubernetes_status_and_sanitized_log_receipts' || !Array.isArray(capture.workloads))
    throw new Error('Terminal capture origin or schema is UNKNOWN.');
  const verdicts: Verdict[] = [];
  const collectors = inventory.resources.filter(value => value.role === 'collector' && value.state !== 'absent');
  if (!collectors.length && inventory.load_admission !== 'not_authorized') throw new Error('Authorized run lacks collector identity.');
  for (const resource of collectors) {
    const identity = { api_version: resource.api_version, kind: resource.kind, name: resource.name, namespace: resource.namespace, uid: resource.uid };
    const matches = capture.workloads.filter(value => object(value) && canonical_json(value.identity) === canonical_json(identity));
    if (matches.length !== 1 || !object(matches[0]) || !matches[0].original_stop)
      throw new Error('Exact collector terminal stop evidence is UNKNOWN.');
    const stop = parseStop(matches[0].original_stop, inventory, 'bounded_drain', resource);
    if (stop.phase !== 'complete' || stop.initial_pods.length === 0 && (stop.initial_desired > 0 || inventory.load_admission !== 'not_authorized'))
      throw new Error('Collector stop completion or original pod coverage is UNKNOWN.');
    for (const pod of stop.initial_pods) for (const initial of pod.containers) {
      const ended = stop.terminal_pods.find(value => value.uid === pod.uid && value.name === pod.name);
      const terminal = ended?.containers.find(value => value.name === initial.name);
      const sameInstance = initial.restart_count !== undefined && terminal?.restart_count === initial.restart_count;
      const marker = sameInstance && stop.logs.some(log => log.phase === 'terminal' && log.pod_uid === pod.uid &&
        log.pod_name === pod.name && log.container === initial.name && log.drain_complete_marker === true);
      const classification = !sameInstance || terminal?.exit_code === null || terminal?.exit_code === undefined ? 'terminal_status_unavailable' :
        terminal.oom_killed ? 'exit_oom_killed' : terminal.exit_code === 0 ? 'exit_0' :
          terminal.exit_code === 143 ? marker ? 'exit_143_terminal_reconciliation_unavailable' : 'exit_143_without_drain_marker' : 'exit_nonzero';
      verdicts.push({ pod_name: pod.name, pod_uid: pod.uid, container: initial.name,
        restart_count: initial.restart_count ?? null, classification });
    }
  }
  return verdicts.sort((a, b) => canonical_json([a.pod_uid, a.container]).localeCompare(canonical_json([b.pod_uid, b.container])));
}
async function refs(azure: Azure, inventory: RunInventory, guard: () => Promise<void>) {
  const destination = inventory.evidence_destination;
  if (!destination || destination.account !== azure.c.account || destination.container !== azure.c.checkpoints ||
      inventory.cleanup.terminal_capture.state !== 'complete') throw new Error('Finalization requires completed terminal capture and exact evidence destination.');
  const names = { terminal_capture: destination.blob_prefix.replace(/\/$/, '') + `/custody/${inventory.cell_run_id}/terminal_capture.json`,
    delta_before: cellBlob(inventory, 'delta-before'), delta_terminal: cellBlob(inventory, 'delta-terminal') };
  const capture = await readBlob(azure, names.terminal_capture, guard), before = await readBlob(azure, names.delta_before, guard),
    terminal = await readBlob(azure, names.delta_terminal, guard);
  await guard();
  if (!capture || !before || !terminal) throw new Error('Terminal capture or immutable Delta proof is unavailable.');
  const initial = baselineDelta(before.value, inventory, azure), final = baselineDelta(terminal.value, inventory, azure, 'terminal_after_writers_stopped');
  if (terminal.value.baseline_etag !== before.etag || initial.table_root !== final.table_root ||
      initial.observation === 'table' && (final.observation !== 'table' || initial.table_id !== final.table_id || final.version < initial.version))
    throw new Error('Terminal Delta proof differs from its original immutable baseline.');
  const make = (blob: string, value: { etag: string; value: Record<string, unknown> }): Ref =>
    ({ blob, etag: value.etag, sha256: digest(value.value) });
  return { terminal_capture: make(names.terminal_capture, capture), delta_before: make(names.delta_before, before),
    delta_terminal: make(names.delta_terminal, terminal), verdicts: classify(inventory, capture.value) };
}
async function baseContext(store: CampaignStore, input: Input, env: NodeJS.ProcessEnv) {
  const request = { schema_version: 1, campaign_id: input.campaign_id, orchestration_run_id: input.orchestration_run_id,
    cell_run_id: input.cell_run_id, actor: input.actor };
  const current = await readCellRunInventory(store, request, env);
  if (current.etag !== input.expected_etag) throw new CampaignConflictError();
  const guard = async () => {
    const next = await readCellRunInventory(store, request, env);
    if (next.etag !== input.expected_etag || canonical_json(next.inventory) !== canonical_json(current.inventory))
      throw new CampaignConflictError();
  };
  return { inventory: current.inventory, guard };
}
async function context(store: CampaignStore, azure: Azure, input: Input, env: NodeJS.ProcessEnv) {
  const current = await baseContext(store, input, env);
  return { ...current, evidence: await refs(azure, current.inventory, current.guard) };
}

function captureAuthority(input: Input, guard: () => Promise<void>): CleanupMutationAuthority {
  return { mode: input.actor.mode, campaignId: input.campaign_id, ownerId: input.actor.owner_id,
    fencingToken: input.actor.fencing_token, assertCurrent: guard };
}
async function createOnly(azure: Azure, blob: string, value: unknown, guard: () => Promise<void>): Promise<Ref> {
  const previous = await readBlob(azure, blob, guard);
  await guard();
  let etag: string;
  if (previous) {
    if (canonical_json(previous.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
    etag = previous.etag;
  } else {
    try { etag = await immutableBlob(azure, blob, value, guard); }
    catch {
      const confirmed = await readBlob(azure, blob, guard);
      await guard();
      if (!confirmed || canonical_json(confirmed.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
      etag = confirmed.etag;
    }
  }
  await guard();
  return { blob, etag, sha256: digest(value) };
}
export async function shutdownMembershipCapture(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv,
  lifecycle: LifecycleActions = createDefaultLifecycleActions(store, azure)) {
  const input = parseInput(raw, ['schedule_name', 'table_root', 'collector_table_id', 'collector_table_name']);
  if (!object(raw) || input.actor.mode !== 'producer' || typeof raw.schedule_name !== 'string' ||
      !/^[a-z0-9][a-z0-9-]{0,252}$/.test(raw.schedule_name) || typeof raw.table_root !== 'string' ||
      typeof raw.collector_table_id !== 'string' || !/^[a-f0-9]{64}$/.test(raw.collector_table_id) ||
      typeof raw.collector_table_name !== 'string' || !/^[a-z0-9_]+[.][a-z0-9_]+[.][a-z0-9_]+$/.test(raw.collector_table_name))
    throw new Error('Invalid exact shutdown membership capture request.');
  const current = await baseContext(store, input, env), blob = shutdownRoot(current.inventory) + '/shutdown-membership.json';
  const existing = await readBlob(azure, blob, current.guard);
  if (existing) {
    const membership = parseShutdownMembership(existing.value, current.inventory);
    if (membership.table_root !== raw.table_root || membership.collector_table_id !== raw.collector_table_id ||
        membership.collector_table_name !== raw.collector_table_name || membership.schedule.name !== raw.schedule_name)
      throw new Error('Immutable shutdown membership differs.');
    await current.guard();
    return { blob, etag: existing.etag, sha256: digest(existing.value) };
  }
  if (!current.inventory.qap?.bindings.some(binding => binding.relative_path === raw.table_root) ||
      !lifecycle.observeShutdownMembership) throw new Error('Exact shutdown readiness observer is unavailable.');
  const observed = await lifecycle.observeShutdownMembership(current.inventory, raw.schedule_name, captureAuthority(input, current.guard));
  await current.guard();
  const value = { schema_version: 1, origin: origin(current.inventory), actor: input.actor, inventory_etag: input.expected_etag,
    table_root: raw.table_root, collector_table_id: raw.collector_table_id, collector_table_name: raw.collector_table_name,
    schedule: observed.schedule, members: observed.members };
  parseShutdownMembership(value, current.inventory);
  return createOnly(azure, blob, value, current.guard);
}
export async function shutdownReceiptCapture(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv,
  lifecycle: LifecycleActions = createDefaultLifecycleActions(store, azure)) {
  const input = parseInput(raw, ['membership', 'kind', 'pod_uid', 'container', 'restart_count']);
  if (!object(raw) || !['collector', 'client'].includes(String(raw.kind)) || typeof raw.pod_uid !== 'string' ||
      typeof raw.container !== 'string' || typeof raw.restart_count !== 'number' || !Number.isSafeInteger(raw.restart_count))
    throw new Error('Invalid shutdown receipt capture request.');
  const current = await context(store, azure, input, env), ref = parseShutdownRef(raw.membership);
  if (ref.blob !== shutdownRoot(current.inventory) + '/shutdown-membership.json') throw new Error('Wrong shutdown membership path.');
  const saved = await readBlob(azure, ref.blob, current.guard);
  if (!saved || saved.etag !== ref.etag || digest(saved.value) !== ref.sha256) throw new Error('Shutdown membership changed.');
  const membership = parseShutdownMembership(saved.value, current.inventory);
  const members = membership.members.filter(member => member.kind === raw.kind && member.pod_uid === raw.pod_uid &&
    member.container === raw.container && member.restart_count === raw.restart_count);
  if (members.length !== 1 || !lifecycle.readShutdownLogs) throw new Error('Original shutdown source instance unavailable.');
  const member = members[0], blob = shutdownReceiptPath(current.inventory, member);
  const previous = await readBlob(azure, blob, current.guard);
  if (previous) {
    if (canonical_json(previous.value.member) !== canonical_json(member) || canonical_json(previous.value.membership) !== canonical_json(ref) ||
        canonical_json(previous.value.origin) !== canonical_json(origin(current.inventory))) throw new Error('Immutable numeric receipt differs.');
    await current.guard();
    return { blob, etag: previous.etag, sha256: digest(previous.value) };
  }
  const logs = await lifecycle.readShutdownLogs(current.inventory, member, captureAuthority(input, current.guard));
  await current.guard();
  const value = { schema_version: 1, origin: origin(current.inventory), actor: input.actor, inventory_etag: input.expected_etag,
    membership: ref, member, table_root: membership.table_root, collector_table_id: membership.collector_table_id,
    collector_table_name: membership.collector_table_name, source: { body_sha256: logs.body_sha256, bytes: logs.bytes },
    projection: projectShutdownLogs(member.kind, logs.text, membership.collector_table_id, membership.collector_table_name) };
  return createOnly(azure, blob, value, current.guard);
}

export async function shutdownFollowerPublish(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw, ['membership', 'pod_uid', 'container', 'restart_count', 'source', 'commits', 'gaps']);
  const uint = (value: unknown): value is number => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
  const gapNames = ['continuity_unproven', 'interrupted', 'restarted', 'pod_gone', 'truncated', 'malformed',
    'terminal_unbound', 'ambiguous_admission', 'identity_unavailable'];
  if (!object(raw) || typeof raw.pod_uid !== 'string' || typeof raw.container !== 'string' || !uint(raw.restart_count) ||
      !exact(raw.source, ['sha256', 'bytes', 'lines']) || typeof raw.source.sha256 !== 'string' ||
      !/^[a-f0-9]{64}$/.test(raw.source.sha256) || !uint(raw.source.bytes) || raw.source.bytes > 67_108_864 ||
      !uint(raw.source.lines) || raw.source.lines > 1_000_000 || !Array.isArray(raw.commits) || raw.commits.length > 20_000 ||
      !Array.isArray(raw.gaps) || raw.gaps.length > gapNames.length ||
      raw.gaps.some(gap => typeof gap !== 'string' || !gapNames.includes(gap)) ||
      new Set(raw.gaps).size !== raw.gaps.length) throw new Error('Invalid bounded diagnostic follower projection.');
  const current = await baseContext(store, input, env), ref = parseShutdownRef(raw.membership);
  if (ref.blob !== shutdownRoot(current.inventory) + '/shutdown-membership.json') throw new Error('Wrong follower membership path.');
  const saved = await readBlob(azure, ref.blob, current.guard);
  if (!saved || saved.etag !== ref.etag || digest(saved.value) !== ref.sha256) throw new Error('Follower membership changed.');
  const membership = parseShutdownMembership(saved.value, current.inventory);
  const members = membership.members.filter(member => member.kind === 'collector' && member.pod_uid === raw.pod_uid &&
    member.container === raw.container && member.restart_count === raw.restart_count);
  if (members.length !== 1) throw new Error('Diagnostic follower original instance unavailable.');
  const commits = raw.commits.map(value => {
    if (!exact(value, ['delta_version', 'rows', 'files', 'parquet_bytes']) || !uint(value.delta_version) ||
        !uint(value.rows) || !uint(value.files) || !uint(value.parquet_bytes))
      throw new Error('Diagnostic follower contains unsupported fields or numeric values.');
    return { delta_version: value.delta_version, rows: value.rows, files: value.files, parquet_bytes: value.parquet_bytes };
  });
  const gaps = [...new Set([...raw.gaps, 'continuity_unproven'])].sort();
  const value = { schema_version: 1, origin: origin(current.inventory), actor: input.actor, inventory_etag: input.expected_etag,
    membership: ref, member: members[0], table_root: membership.table_root, collector_table_id: membership.collector_table_id,
    collector_table_name: membership.collector_table_name, source: { sha256: raw.source.sha256, bytes: raw.source.bytes, lines: raw.source.lines },
    commits, gaps, status: 'diagnostic_only', continuity: 'unproven' };
  return createOnly(azure, shutdownRoot(current.inventory) + '/collector-followers/' +
    members[0].pod_uid + '-' + members[0].container + '-' + members[0].restart_count + '.json', value, current.guard);
}

async function applyProof(azure: Azure, input: Input, current: Awaited<ReturnType<typeof context>>, raw: unknown, persist = true) {
  let proof: ShutdownProof | null = null;
  if (raw !== null) {
    try { proof = parseShutdownProof(raw); } catch { /* A claim is not evidence. */ }
  }
  let classification: 'exact' | 'mismatch' | 'unavailable' = 'unavailable';
  if (proof) {
    try { classification = await readShutdownNumericProof(azure, current.inventory, proof, current.evidence, current.guard); }
    catch { await current.guard(); }
    const value = { schema_version: 1, origin: origin(current.inventory), actor: input.actor, inventory_etag: input.expected_etag,
      terminal_capture: current.evidence.terminal_capture, delta_before: current.evidence.delta_before,
      delta_terminal: current.evidence.delta_terminal, proof };
    if (persist) await createOnly(azure, shutdownRoot(current.inventory) + '/shutdown-reconciliation.json', value, current.guard);
  }
  return current.evidence.verdicts.map(verdict => verdict.classification === 'exit_143_terminal_reconciliation_unavailable' ?
    { ...verdict, classification: classification === 'exact' ? 'clean_sigterm_after_drain' :
      classification === 'mismatch' ? 'exit_143_terminal_reconciliation_failed' : 'exit_143_terminal_reconciliation_unavailable' } : verdict);
}
async function persistedProof(azure: Azure, input: Input, current: Awaited<ReturnType<typeof context>>) {
  const saved = await readBlob(azure, shutdownRoot(current.inventory) + '/shutdown-reconciliation.json', current.guard);
  await current.guard();
  if (!saved) return current.evidence.verdicts;
  const value = saved.value;
  if (!exact(value, ['schema_version', 'origin', 'actor', 'inventory_etag', 'terminal_capture', 'delta_before', 'delta_terminal', 'proof']) ||
      value.schema_version !== 1 || canonical_json(value.origin) !== canonical_json(origin(current.inventory)) ||
      canonical_json(value.actor) !== canonical_json(input.actor) || value.inventory_etag !== input.expected_etag ||
      canonical_json(value.terminal_capture) !== canonical_json(current.evidence.terminal_capture) ||
      canonical_json(value.delta_before) !== canonical_json(current.evidence.delta_before) ||
      canonical_json(value.delta_terminal) !== canonical_json(current.evidence.delta_terminal))
    throw new Error('Immutable shutdown reconciliation origin differs.');
  return applyProof(azure, input, current, value.proof, false);
}

export async function shutdownFinalizationInputs(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw), current = await context(store, azure, input, env);
  return { schema_version: 1, inventory_etag: input.expected_etag, terminal_capture: current.evidence.terminal_capture,
    delta_before: current.evidence.delta_before, delta_terminal: current.evidence.delta_terminal };
}
function valueFor(input: Input, current: Awaited<ReturnType<typeof context>>) {
  return { schema_version: 1, inventory_etag: input.expected_etag, actor: input.actor, origin: origin(current.inventory),
    terminal_capture: current.evidence.terminal_capture, delta_before: current.evidence.delta_before,
    delta_terminal: current.evidence.delta_terminal, verdicts: current.evidence.verdicts };
}
export async function shutdownFinalize(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw, ['terminal_capture', 'delta_before', 'delta_terminal', 'reconciliation']);
  if (!object(raw) || !exact(raw.reconciliation, ['profile', 'proof']) ||
      !['single_hot_table', 'other'].includes(String(raw.reconciliation.profile)) ||
      !(raw.reconciliation.proof === null || object(raw.reconciliation.proof))) throw new Error('Invalid exact reconciliation input.');
  const current = await context(store, azure, input, env);
  for (const key of ['terminal_capture', 'delta_before', 'delta_terminal'] as const)
    if (canonical_json(raw[key]) !== canonical_json(current.evidence[key])) throw new Error(`Finalization ${key} reference changed.`);
  const verdicts = raw.reconciliation.profile === 'single_hot_table' ? await applyProof(azure, input, current, raw.reconciliation.proof) :
    current.evidence.verdicts;
  const value = { ...valueFor(input, current), verdicts }, blob = cellBlob(current.inventory, 'shutdown-finalization');
  const previous = await readBlob(azure, blob, current.guard);
  await current.guard();
  let etag: string;
  if (previous) {
    if (canonical_json(previous.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
    etag = previous.etag;
  } else {
    try { etag = await immutableBlob(azure, blob, value, current.guard); }
    catch {
      let saved: Awaited<ReturnType<typeof readBlob>>;
      try { saved = await readBlob(azure, blob, current.guard); await current.guard(); }
      catch { throw new CampaignWriteOutcomeUnknownError(); }
      if (!saved || canonical_json(saved.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
      etag = saved.etag;
    }
  }
  return { ...value, blob, etag, sha256: digest(value) };
}
export async function shutdownFinalizationRead(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw), current = await context(store, azure, input, env), blob = cellBlob(current.inventory, 'shutdown-finalization');
  const saved = await readBlob(azure, blob, current.guard), value = { ...valueFor(input, current), verdicts: await persistedProof(azure, input, current) };
  await current.guard();
  if (!saved || canonical_json(saved.value) !== canonical_json(value)) throw new Error('Matching shutdown finalization is unavailable.');
  return { ...value, blob, etag: saved.etag, sha256: digest(value) };
}
export async function requireFinalizedCollectorStop(store: CampaignStore, azure: Azure, inventory: RunInventory,
  authority: CleanupMutationAuthority): Promise<void> {
  if (!inventory.resources.some(resource => resource.role === 'collector' && resource.state !== 'absent')) return;
  await authority.assertCurrent();
  const snapshot = await store.read();
  if (!snapshot || authority.campaignId !== inventory.campaign_id || authority.ownerId !== inventory.owner_id ||
      authority.fencingToken < inventory.fencing_token || canonical_json(snapshot.state.runInventory) !== canonical_json(inventory))
    throw new Error('Finalization gate inventory changed.');
  const guard = async () => {
    await authority.assertCurrent();
    const current = await store.read();
    if (!current || current.etag !== snapshot.etag || canonical_json(current.state.runInventory) !== canonical_json(inventory))
      throw new CampaignConflictError();
  };
  const destination = inventory.evidence_destination;
  if (!destination) {
    if (inventory.drain_budget === undefined || inventory.load_admission === 'not_authorized') return;
    throw new Error('Collector terminal shutdown evidence unavailable.');
  }
  const capture = await readBlob(azure, (destination.blob_prefix.endsWith('/') ? destination.blob_prefix.slice(0, -1) : destination.blob_prefix) +
    '/custody/' + inventory.cell_run_id + '/terminal_capture.json', guard);
  await guard();
  if (!capture) {
    if (inventory.drain_budget === undefined && inventory.cleanup.bounded_drain.state === 'complete') return;
    throw new Error('Collector terminal shutdown evidence unavailable.');
  }
  const verdicts = classify(inventory, capture.value);
  if (verdicts.every(verdict => verdict.classification === 'exit_0')) return;
  const evidence = await refs(azure, inventory, guard);
  const saved = await readBlob(azure, cellBlob(inventory, 'shutdown-finalization'), guard);
  await guard();
  const value = saved?.value, actor = value?.actor;
  if (!exact(value, ['schema_version', 'inventory_etag', 'actor', 'origin', 'terminal_capture', 'delta_before', 'delta_terminal', 'verdicts']) ||
      value.schema_version !== 1 || typeof value.inventory_etag !== 'string' || value.inventory_etag.length === 0 ||
      value.inventory_etag.length > 512 || value.inventory_etag === '*' || value.inventory_etag.startsWith('W/') ||
      /[\u0000-\u001f]/.test(value.inventory_etag) ||
      !exact(actor, ['mode', 'owner_id', 'fencing_token']) || actor.owner_id !== inventory.owner_id ||
      actor.owner_id !== authority.ownerId || typeof actor.fencing_token !== 'number' ||
      !Number.isSafeInteger(actor.fencing_token) ||
      !(actor.mode === 'producer' ? actor.fencing_token === inventory.fencing_token :
        actor.mode === 'controller' && authority.mode === 'controller' && actor.fencing_token > inventory.fencing_token &&
          actor.fencing_token <= authority.fencingToken) ||
      canonical_json(value.origin) !== canonical_json(origin(inventory)) ||
      canonical_json(value.terminal_capture) !== canonical_json(evidence.terminal_capture) ||
      canonical_json(value.delta_before) !== canonical_json(evidence.delta_before) ||
      canonical_json(value.delta_terminal) !== canonical_json(evidence.delta_terminal) ||
      !Array.isArray(value.verdicts))
    throw new Error('Collector shutdown finalization is unavailable or failed; do not hand off.');
  const input = parseInput({ schema_version: 1, campaign_id: inventory.campaign_id,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id,
    expected_etag: value.inventory_etag, actor: value.actor });
  const verified = await persistedProof(azure, input, { inventory, evidence, guard });
  if (canonical_json(value.verdicts) !== canonical_json(verified) ||
      verified.some(verdict => !['exit_0', 'clean_sigterm_after_drain'].includes(verdict.classification)))
    throw new Error('Collector shutdown finalization is unavailable or failed; do not hand off.');
  await guard();
}
