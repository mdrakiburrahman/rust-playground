import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { assertCampaignEnvironment, CampaignConflictError, CampaignFencedError, CampaignWriteOutcomeUnknownError, compareAndSwapRunInventory,
  CUSTODY_CLEANUP_STEPS, validateCampaignState, type CampaignState, type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';
import type { CleanupMutationAuthority, ExternalCleanupActions } from './aks-cleanup-executor.js';

export const CELL_CLEANUP_SELECTORS = {
  stop_admission: 'stop-admission', bounded_drain: 'bounded-drain', evidence_capture: 'preserve-evidence',
  stop_maintenance: 'stop-maintenance', terminal_capture: 'record-terminal-state', remove_namespace: 'remove-run-namespace',
  remove_qap: 'remove-run-qap', remove_fic: 'remove-run-fic', handoff: 'cell-handoff',
} as const;
export type CellCleanupSelector = keyof typeof CELL_CLEANUP_SELECTORS;
const selectors = Object.keys(CELL_CLEANUP_SELECTORS) as CellCleanupSelector[];
export interface CellCleanupActor { mode: 'producer' | 'controller'; owner_id: string; fencing_token: number }
export interface CellCleanupEnvelope { schema_version: 1; inventory: RunInventory; expected_etag: string; actor: CellCleanupActor }
export interface ReadInventoryEnvelope {
  schema_version: 1; campaign_id: string; orchestration_run_id: string; cell_run_id: string; actor: CellCleanupActor;
}
export interface ReadInventoryResult { ok: true; inventory: RunInventory; etag: string }
export interface CellCleanupReceipt {
  status: 'complete'; step: CellCleanupSelector; campaign_id: string; owner_id: string; fencing_token: number;
  orchestration_run_id: string; cell_run_id: string; namespace_uid: string | null; actor: CellCleanupActor;
  completed_at_unix_ms: number; etag: string;
}
export interface CellStepRecord {
  schema_version: 1; selector: CellCleanupSelector; origin: Pick<RunInventory,
    'campaign_id' | 'owner_id' | 'fencing_token' | 'orchestration_run_id' | 'cell_run_id'>;
  phase: 'running' | 'effect-confirmed' | 'complete'; inventory: RunInventory | null; receipt: CellCleanupReceipt | null;
}
export interface CellStepJournal {
  read(selector: CellCleanupSelector): Promise<{ value: CellStepRecord; etag: string } | undefined>;
  write(value: CellStepRecord, expectedEtag?: string): Promise<string>;
}
export interface CellStepProvider {
  execute(inventory: RunInventory, key: string, authority: CleanupMutationAuthority): Promise<RunInventory>;
  recover?(inventory: RunInventory, key: string, authority: CleanupMutationAuthority): Promise<RunInventory>;
}
export type CellStepProviders = Partial<Record<Exclude<CellCleanupSelector, 'handoff'>, CellStepProvider>>;
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> =>
  object(value) && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
function origin(inventory: CellStepRecord['origin']): CellStepRecord['origin'] {
  const { campaign_id, owner_id, fencing_token, orchestration_run_id, cell_run_id } = inventory;
  return { campaign_id, owner_id, fencing_token, orchestration_run_id, cell_run_id };
}
function strongEtag(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 512 && value !== '*' &&
    !value.startsWith('W/') && !/[\u0000-\u001f]/.test(value);
}
function validActor(value: unknown): value is CellCleanupActor {
  return exact(value, ['mode', 'owner_id', 'fencing_token']) && ['producer', 'controller'].includes(String(value.mode)) &&
    typeof value.owner_id === 'string' && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value.owner_id) &&
    typeof value.fencing_token === 'number' && Number.isSafeInteger(value.fencing_token) && value.fencing_token > 0;
}
export function parseCellCleanupEnvelope(value: unknown): CellCleanupEnvelope {
  if (!exact(value, ['schema_version', 'inventory', 'expected_etag', 'actor']) || value.schema_version !== 1 ||
      !strongEtag(value.expected_etag) || !validActor(value.actor) ||
      !object(value.inventory)) throw new Error('Invalid exact cleanup-step envelope.');
  return value as unknown as CellCleanupEnvelope;
}
export async function readCellRunInventory(store: CampaignStore, input: unknown, env: NodeJS.ProcessEnv): Promise<ReadInventoryResult> {
  if (!exact(input, ['schema_version', 'campaign_id', 'orchestration_run_id', 'cell_run_id', 'actor']) ||
      input.schema_version !== 1 || !validActor(input.actor) ||
      ['campaign_id', 'orchestration_run_id', 'cell_run_id'].some(key => typeof input[key] !== 'string' ||
        !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(String(input[key]))))
    throw new Error('Invalid exact read-inventory envelope.');
  const stored = await store.read();
  if (!stored) throw new CampaignFencedError('No durable campaign inventory exists.');
  const state = validateCampaignState(stored.state), inventory = state.runInventory, actor = input.actor, now = Date.now();
  assertCampaignEnvironment(env, state);
  if (!inventory || !strongEtag(stored.etag) || state.campaignId !== input.campaign_id || inventory.campaign_id !== input.campaign_id ||
      inventory.orchestration_run_id !== input.orchestration_run_id || inventory.cell_run_id !== input.cell_run_id ||
      actor.owner_id !== state.ownerId || actor.fencing_token !== state.fencingToken ||
      (actor.mode === 'producer' ? state.status !== 'active' || inventory.fencing_token !== actor.fencing_token ||
        inventory.owner_id !== actor.owner_id || now < state.heartbeatAtMs || now - state.heartbeatAtMs >= 180_000 ||
        now >= state.expiresAtMs : state.status !== 'cleaning' || inventory.fencing_token >= actor.fencing_token))
    throw new CampaignFencedError('Read-inventory actor, immutable origin or exact requested run/cell differs.');
  return { ok: true, inventory: structuredClone(inventory), etag: stored.etag };
}
function assertActor(state: CampaignState, envelope: CellCleanupEnvelope, selector: CellCleanupSelector) {
  const actor = envelope.actor, inventory = state.runInventory, now = Date.now();
  if (!inventory || canonical_json(origin(inventory)) !== canonical_json(origin(envelope.inventory)) ||
      state.ownerId !== actor.owner_id || state.fencingToken !== actor.fencing_token)
    throw new CampaignFencedError();
  if (actor.mode === 'producer') {
    if (state.status !== 'active' || actor.owner_id !== inventory.owner_id || actor.fencing_token !== inventory.fencing_token ||
        now < state.heartbeatAtMs || now - state.heartbeatAtMs >= 180_000 || now >= state.expiresAtMs) throw new CampaignFencedError();
  } else {
    const step = selector === 'handoff' ? 'verify-safe-idle' : CELL_CLEANUP_SELECTORS[selector];
    if (state.status !== 'cleaning' || inventory.fencing_token >= actor.fencing_token ||
        !state.cleanupSteps.some(receipt => receipt.step === step && receipt.status === 'running' &&
          receipt.idempotencyKey === `${state.campaignId}:${actor.fencing_token}:${step}`))
      throw new CampaignFencedError('Controller cleanup requires the exact current running outer step/key.');
  }
}
function field(selector: CellCleanupSelector): keyof RunInventory['cleanup'] | undefined {
  return selector === 'stop_admission' || selector === 'bounded_drain' || selector === 'evidence_capture' || selector === 'stop_maintenance' ||
    selector === 'terminal_capture' || selector === 'handoff' ? selector : undefined;
}
function mark(inventory: RunInventory, selector: CellCleanupSelector, state: 'running' | 'complete'): RunInventory {
  const next = structuredClone(inventory), name = field(selector);
  if (name) next.cleanup[name] = { ...next.cleanup[name], state, reason_code: null,
    deadline_unix_ms: next.cleanup[name].deadline_unix_ms ??
      (selector === 'stop_admission' ? Date.now() + 60_000 :
        selector === 'bounded_drain' || selector === 'stop_maintenance' ? Date.now() + 120_000 : null) };
  return next;
}
function terminal(inventory: RunInventory) {
  if (Object.entries(inventory.cleanup).some(([name, value]) => name !== 'handoff' && value.state !== 'complete') ||
      inventory.resources.some(resource => !['removed', 'absent'].includes(resource.state)) ||
      inventory.fic && !['removed', 'absent'].includes(inventory.fic.state) ||
      inventory.qap && !['removed', 'absent'].includes(inventory.qap.state))
    throw new Error('Cell handoff requires all lifecycle steps and exact resource removal/verified absence.');
}
function assertRecord(record: CellStepRecord, envelope: CellCleanupEnvelope, selector: CellCleanupSelector) {
  if (!exact(record, ['schema_version', 'selector', 'origin', 'phase', 'inventory', 'receipt']) ||
      record.schema_version !== 1 || record.selector !== selector ||
      canonical_json(record.origin) !== canonical_json(origin(envelope.inventory)) ||
      !['running', 'effect-confirmed', 'complete'].includes(record.phase) ||
      (record.phase === 'running' ? record.inventory !== null || record.receipt !== null : !record.inventory) ||
      (record.phase === 'effect-confirmed' && record.receipt !== null) ||
      (record.phase === 'complete' && !record.receipt))
    throw new Error('Cell cleanup journal identity or positive completion proof is UNKNOWN.');
  if (record.receipt) {
    const receipt = record.receipt;
    if (!exact(receipt, ['status', 'step', 'campaign_id', 'owner_id', 'fencing_token', 'orchestration_run_id',
      'cell_run_id', 'namespace_uid', 'actor', 'completed_at_unix_ms', 'etag']) ||
        receipt.status !== 'complete' || receipt.step !== selector ||
        canonical_json(origin(receipt)) !== canonical_json(record.origin) || receipt.namespace_uid !== record.inventory?.namespace.uid ||
        !Number.isSafeInteger(receipt.completed_at_unix_ms) || receipt.completed_at_unix_ms < 1 ||
        receipt.completed_at_unix_ms > Date.now() || typeof receipt.etag !== 'string' || !receipt.etag ||
        !exact(receipt.actor, ['mode', 'owner_id', 'fencing_token']) ||
        !['producer', 'controller'].includes(receipt.actor.mode) || receipt.actor.owner_id !== record.origin.owner_id ||
        !Number.isSafeInteger(receipt.actor.fencing_token) ||
        (receipt.actor.mode === 'producer' ? receipt.actor.fencing_token !== record.origin.fencing_token :
          receipt.actor.fencing_token <= record.origin.fencing_token))
      throw new Error('Cell cleanup journal lacks an exact positive origin/actor receipt.');
  }
}
export function createBlobCellStepJournal(azure: Azure, envelope: CellCleanupEnvelope,
  assertCurrent: () => Promise<void>): CellStepJournal {
  const blob = (selector: CellCleanupSelector) =>
    `_arrowcol-custody/campaigns/${envelope.inventory.campaign_id}/cells/${envelope.inventory.cell_run_id}/${selector}.json`;
  const read: CellStepJournal['read'] = async selector => {
    await assertCurrent();
    const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, blob(selector), undefined, undefined, {}, true, true);
    if (bytes === null) return undefined;
    if (bytes.length > 1_048_576 || !headers.etag) throw new Error('Unbounded/unversioned cell cleanup journal.');
    const value = JSON.parse(bytes.toString()) as CellStepRecord;
    assertRecord(value, envelope, selector);
    return { value, etag: headers.etag };
  };
  return { read, write: async (value, expectedEtag) => {
    assertRecord(value, envelope, value.selector);
    const prior = await read(value.selector), rank = { running: 0, 'effect-confirmed': 1, complete: 2 };
    if (prior?.etag !== expectedEtag) throw new CampaignConflictError();
    if (prior && (rank[prior.value.phase] > rank[value.phase] ||
        prior.value.inventory && canonical_json(prior.value.inventory) !== canonical_json(value.inventory) ||
        prior.value.receipt && canonical_json(prior.value.receipt) !== canonical_json(value.receipt)))
      throw new Error('Positive cleanup effect and receipt proofs are immutable.');
    await assertCurrent();
    try {
      const [, headers] = await azure.blob('PUT', azure.c.checkpoints, blob(value.selector), undefined, canonical_json(value),
        { 'x-ms-blob-type': 'BlockBlob', ...(expectedEtag ? { 'If-Match': expectedEtag } : { 'If-None-Match': '*' }) }, false, false);
      const confirmed = await read(value.selector);
      if (headers[':status'] !== '201' || !headers.etag || confirmed?.etag !== headers.etag ||
          canonical_json(confirmed.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
      return headers.etag;
    } catch (error) {
      if (error instanceof CampaignFencedError || error instanceof CampaignConflictError) throw error;
      throw new CampaignWriteOutcomeUnknownError();
    }
  } };
}

export async function executeCellCleanupStep(store: CampaignStore, input: unknown, selector: CellCleanupSelector,
  providers: CellStepProviders, journalFactory: (assertCurrent: () => Promise<void>) => CellStepJournal): Promise<CellCleanupReceipt> {
  if (!Object.hasOwn(CELL_CLEANUP_SELECTORS, selector)) throw new Error('Unknown cleanup-step selector.');
  const envelope = parseCellCleanupEnvelope(input);
  let current = await store.read();
  if (!current) throw new CampaignFencedError();
  validateCampaignState(current.state);
  assertActor(current.state, envelope, selector);
  if (current.etag !== envelope.expected_etag || canonical_json(current.state.runInventory) !== canonical_json(envelope.inventory))
    throw new CampaignConflictError();
  if ((selector === 'evidence_capture' || selector === 'terminal_capture') && !current.state.runInventory!.evidence_destination)
    throw new Error('Persist the exact evidence destination before any capture effect.');
  let effectEtag: string | undefined;
  const assertCurrent = async () => {
    const observed = await store.read();
    if (!observed) throw new CampaignFencedError();
    assertActor(observed.state, envelope, selector);
    if (effectEtag !== undefined && observed.etag !== effectEtag) throw new CampaignConflictError();
  };
  const authority: CleanupMutationAuthority = { mode: envelope.actor.mode, campaignId: envelope.inventory.campaign_id,
    ownerId: envelope.actor.owner_id, fencingToken: envelope.actor.fencing_token, assertCurrent };
  const journal = journalFactory(assertCurrent), priorSelectors = selectors.slice(0, selectors.indexOf(selector));
  for (const prior of priorSelectors) {
    const saved = await journal.read(prior);
    if (saved) {
      assertRecord(saved.value, envelope, prior);
      if (saved.value.phase !== 'complete') throw new Error('Prior cell cleanup effect is unresolved.');
    } else {
      const step = CELL_CLEANUP_SELECTORS[prior];
      if (envelope.actor.mode !== 'controller' ||
          !current.state.cleanupSteps.some(value => value.step === step && value.status === 'complete'))
        throw new Error('Prior cell cleanup effect has no positive durable receipt.');
    }
  }
  let saved = await journal.read(selector), result: RunInventory;
  if (saved) {
    assertRecord(saved.value, envelope, selector);
    if (saved.value.inventory) validateCampaignState({ ...current.state, runInventory: saved.value.inventory });
    if (saved.value.phase === 'running') {
      const recover = providers[selector as Exclude<CellCleanupSelector, 'handoff'>]?.recover;
      if (selector !== 'handoff' && !recover) throw new Error('Cleanup effect outcome UNKNOWN; no recovery provider, never replay.');
      result = selector === 'handoff' ? current.state.runInventory! :
        await recover!(current.state.runInventory!, `${envelope.inventory.cell_run_id}:${selector}`, authority);
    } else result = saved.value.phase === 'complete' ? current.state.runInventory! : saved.value.inventory!;
  } else {
    const provider = providers[selector as Exclude<CellCleanupSelector, 'handoff'>];
    if (selector !== 'handoff' && !provider) throw new Error('No concrete cleanup provider is installed.');
    if (selector === 'handoff') terminal(current.state.runInventory!);
    current = await compareAndSwapRunInventory(store, current, mark(current.state.runInventory!, selector, 'running'), assertCurrent);
    effectEtag = current.etag;
    const value: CellStepRecord = { schema_version: 1, selector, origin: origin(envelope.inventory), phase: 'running', inventory: null, receipt: null };
    saved = { value, etag: await journal.write(value) };
    await assertCurrent();
    result = selector === 'handoff' ? current.state.runInventory! :
      await provider!.execute(current.state.runInventory!, `${envelope.inventory.cell_run_id}:${selector}`, authority);
  }
  effectEtag = undefined;
  await assertCurrent();
  if (selector === 'handoff') terminal(result);
  result = mark(result, selector, 'complete');
  if (saved.value.phase !== 'complete') {
    const value: CellStepRecord = { ...saved.value, phase: 'effect-confirmed', inventory: result, receipt: null };
    saved = { value, etag: await journal.write(value, saved.etag) };
  }
  current = (await store.read())!;
  const confirmed = await compareAndSwapRunInventory(store, current, result, assertCurrent), inventory = confirmed.state.runInventory!;
  const receipt: CellCleanupReceipt = { status: 'complete', step: selector, ...origin(inventory),
    namespace_uid: inventory.namespace.uid, actor: envelope.actor, completed_at_unix_ms: Date.now(), etag: confirmed.etag };
  if (saved.value.phase !== 'complete')
    await journal.write({ ...saved.value, phase: 'complete', receipt }, saved.etag);
  return receipt;
}

export function createCellCleanupProviders(actions: ExternalCleanupActions, recover: CellStepProviders = {}): CellStepProviders {
  const run = (selector: Exclude<CellCleanupSelector, 'handoff'>): CellStepProvider => ({
    recover: recover[selector]?.recover,
    execute: async (supplied, key, authority) => {
      let inventory = await actions.discoverExactInventory(supplied);
      if (canonical_json(inventory) !== canonical_json(supplied))
        throw new Error('Persist exact discovered inventory/UIDs before invoking a cleanup effect.');
      const complete = (result: string) => { if (result !== 'complete') throw new Error('Cleanup effect is pending or UNKNOWN; recover its journal, never replay.'); };
      switch (selector) {
        case 'stop_admission': complete(await actions.stopAdmission(inventory, inventory.cleanup.stop_admission.deadline_unix_ms!, key, authority)); break;
        case 'bounded_drain': complete(await actions.boundedDrain(inventory, inventory.cleanup.bounded_drain.deadline_unix_ms!, key, authority)); break;
        case 'evidence_capture': complete(await actions.preserveEvidence(inventory, key, authority)); break;
        case 'terminal_capture': complete(await actions.captureTerminalState(inventory, key, authority)); break;
        case 'stop_maintenance':
          if (inventory.resources.some(value => value.role === 'maintenance' &&
              (['planned', 'uncertain'].includes(value.state) || value.state === 'observed' && !value.uid)))
            throw new Error('Exact maintenance workload stop identity is UNKNOWN.');
          for (const resource of inventory.resources.filter(value => value.role === 'maintenance' && value.state === 'observed')) {
            if (!resource.uid) throw new Error('Maintenance UID is UNKNOWN.');
            complete(await actions.stopMaintenanceIfUidAndOwnerMatch(resource, inventory.namespace.owner_token, key + ':' + resource.name, authority));
          }
          break;
        case 'remove_namespace':
          if (!inventory.namespace.uid || inventory.resources.some(value => ['planned', 'uncertain'].includes(value.state)))
            throw new Error('Namespace UID or planned workload identity is UNKNOWN.');
          complete(await actions.deleteNamespaceIfUidAndOwnerMatch(inventory.namespace, key, authority));
          inventory = { ...inventory, resources: inventory.resources.map(resource => ({ ...resource, state: resource.uid ? 'removed' : resource.state })) };
          break;
        case 'remove_qap':
          if (inventory.qap && !['removed', 'absent'].includes(inventory.qap.state)) {
            if (inventory.qap.state !== 'observed') throw new Error('QAP original setup outcome is UNKNOWN.');
            for (const binding of inventory.qap.bindings)
              complete(await actions.deleteQapBindingIfOwned(inventory.qap.database, binding, inventory.campaign_id, key + ':' + binding.external_table, authority));
            inventory = { ...inventory, qap: { ...inventory.qap, state: 'removed' } };
          }
          break;
        case 'remove_fic':
          if (inventory.fic && !['removed', 'absent'].includes(inventory.fic.state)) {
            if (inventory.fic.state !== 'observed') throw new Error('FIC original setup outcome is UNKNOWN.');
            complete(await actions.deleteFicIfExact(inventory.fic, inventory.campaign_id, key, authority));
            inventory = { ...inventory, fic: { ...inventory.fic, state: 'removed' } };
          }
      }
      return inventory;
    },
  });
  return Object.fromEntries(selectors.filter((selector): selector is Exclude<CellCleanupSelector, 'handoff'> => selector !== 'handoff')
    .map(selector => [selector, run(selector)]));
}
