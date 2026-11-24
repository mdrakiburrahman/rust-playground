import fs from 'node:fs';
import path from 'node:path';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { cleanup_run_fic, recover_run_fic_record, type RunFicPlan } from '../../infra/azure/run_fic.js';
import { cleanup_oat_qap, delete_oat_qap_workload, recover_oat_qap_record, recover_oat_workload_stop,
  parse_oat_qap_reference, type OatQapReference, type RecordV1 } from '../../infra/azure/oat_qap.js';
import { DRAIN_UNSETTLED_REASON, hasVerifiedUnsettledDrain, publishCleanupRunInventory,
  type CampaignState, type CampaignStore, type InventoryStep, type RunInventory, type RunInventoryResource } from './aks-campaign.js';
import type { CleanupExecutor } from './aks-reconcile.js';
import { assertRunCleanupActor, createSetupRunCleanupActions } from './aks-setup-cleanup.js';
import { ADMISSION_STOP_BUDGET_MS, drainBudgetMs, TERMINAL_CAPTURE_BUDGET_MS, withCleanupDeadline } from './aks-cleanup-budget.js';
import { MAINTENANCE_STOP_TIMEOUT_CEILING_MS, type EffectiveStopBudgets } from './stop-budgets.js';

export type CleanupActionResult = 'pending' | 'complete' | 'uncertain';
export interface CleanupMutationAuthority {
  mode?: 'producer' | 'controller';
  campaignId: string;
  ownerId: string;
  fencingToken: number;
  assertCurrent(): Promise<void>;
  assertEvidenceCurrent?(): Promise<void>;
}
export interface ExternalCleanupActions {
  verifyCellPreservation?(inventory: RunInventory, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  recoverLifecycle?(field: Exclude<keyof RunInventory['cleanup'], 'handoff'>, inventory: RunInventory, deadlineUnixMs: number | null,
    idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  discoverExactInventory(inventory: RunInventory): Promise<RunInventory>;
  stopAdmission(inventory: RunInventory, deadlineUnixMs: number, idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  boundedDrain(inventory: RunInventory, deadlineUnixMs: number, idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  preserveEvidence(inventory: RunInventory, idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  stopMaintenanceIfUidAndOwnerMatch(resource: RunInventoryResource, ownerToken: string, idempotencyKey: string,
    authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  captureTerminalState(inventory: RunInventory, idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  deleteNamespaceIfUidAndOwnerMatch(namespace: RunInventory['namespace'], idempotencyKey: string,
    authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  deleteQapBindingIfOwned(database: string, binding: NonNullable<RunInventory['qap']>['bindings'][number], campaignId: string,
    idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  deleteFicIfExact(fic: NonNullable<RunInventory['fic']>, campaignId: string, idempotencyKey: string,
    authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  restoreSafeIdle(state: CampaignState, idempotencyKey: string, authority: CleanupMutationAuthority): Promise<CleanupActionResult>;
  verifySafeIdle(state: CampaignState, idempotencyKey: string): Promise<CleanupActionResult>;
}

function hasUnresolvedIdentity(inventory: RunInventory): boolean {
  return inventory.resources.some(resource => resource.state === 'planned' || resource.state === 'uncertain') ||
    inventory.fic?.state === 'uncertain' || inventory.qap?.state === 'uncertain';
}
function keyed(key: string, identity: string): string { return key + ':' + identity; }
function setStep(step: InventoryStep, state: InventoryStep['state'], budgetMs = 0, reasonCode: string | null = null): InventoryStep {
  return { state, deadline_unix_ms: step.deadline_unix_ms ?? (budgetMs > 0 ? Date.now() + budgetMs : null), reason_code: reasonCode };
}
function stepField(step: string): keyof RunInventory['cleanup'] | undefined {
  switch (step) {
    case 'stop-admission': return 'stop_admission';
    case 'bounded-drain': return 'bounded_drain';
    case 'preserve-evidence': return 'evidence_capture';
    case 'stop-maintenance': return 'stop_maintenance';
    case 'record-terminal-state': return 'terminal_capture';
    default: return undefined;
  }
}

export type OwnedRunCleanupActions = Pick<ExternalCleanupActions,
  'deleteNamespaceIfUidAndOwnerMatch' | 'deleteQapBindingIfOwned' | 'deleteFicIfExact'>;

export function createOwnedRunCleanupActions(store: CampaignStore, azure: Azure, inputReference: OatQapReference | null,
  workDirectory: string, setupPlan?: RunFicPlan): OwnedRunCleanupActions {
  if (inputReference === null) {
    if (!setupPlan) throw new Error('Cleanup without QAP requires the original durable FIC plan.');
    return createSetupRunCleanupActions(store, azure, setupPlan, workDirectory);
  }
  const reference = parse_oat_qap_reference(inputReference);
  const fresh = async (authority: CleanupMutationAuthority): Promise<RunInventory> => {
    await authority.assertCurrent();
    const inventory = await assertRunCleanupActor(store, authority);
    if (
        inventory.campaign_id !== authority.campaignId || inventory.cell_run_id !== reference.run_id ||
        inventory.namespace.uid !== reference.namespace_uid || inventory.namespace.owner_token !== reference.namespace_owner_token)
      throw new Error('Owned cleanup reference does not match the current cleaning actor and immutable run origin.');
    return inventory;
  };
  const run = async (authority: CleanupMutationAuthority,
    work: (inventory: RunInventory, record: RecordV1, directory: string, guarded: CleanupMutationAuthority) => Promise<void>): Promise<'complete'> => {
    await fresh(authority);
    const stat = fs.lstatSync(workDirectory);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
      throw new Error('Cleanup working directory must be a private directory, not a symlink.');
    const directory = fs.mkdtempSync(path.join(workDirectory, 'owned-cleanup-'));
    const guarded = { ...authority, assertCurrent: async () => { await fresh(authority); } };
    const record = await recover_oat_qap_record(azure, reference, path.join(directory, 'qap.json'), guarded);
    const inventory = await fresh(authority);
    const bindings = inventory.qap?.bindings.map(item => item.external_table + '|' + item.relative_path).sort();
    const expected = record.manifest.tables.map(item => item.external_table + '|' + item.relative_path).sort();
    if (inventory.namespace.name !== record.workload.namespace || inventory.qap?.database !== record.environment.database ||
        canonical_json(bindings) !== canonical_json(expected))
      throw new Error('Inventory bindings differ from the original full QAP ownership snapshot.');
    await work(inventory, record, directory, guarded);
    await guarded.assertCurrent();
    fs.rmSync(directory, { recursive: true });
    return 'complete';
  };
  return {
    deleteNamespaceIfUidAndOwnerMatch: (namespace, _key, authority) => run(authority, async (inventory, _record, directory, guarded) => {
      if (canonical_json(namespace) !== canonical_json(inventory.namespace)) throw new Error('Namespace cleanup identity changed.');
      await delete_oat_qap_workload(azure, reference, path.join(directory, 'qap.json'), path.join(directory, 'stop.json'), guarded);
    }),
    deleteQapBindingIfOwned: (database, binding, campaignId, _key, authority) =>
      run(authority, async (inventory, _record, directory, guarded) => {
        if (campaignId !== inventory.campaign_id || database !== inventory.qap?.database ||
            !inventory.qap.bindings.some(item => canonical_json(item) === canonical_json(binding)))
          throw new Error('QAP cleanup requested a binding outside the exact original run.');
        const stopPath = path.join(directory, 'stop.json');
        await recover_oat_workload_stop(azure, reference, stopPath, guarded);
        await cleanup_oat_qap(azure, path.join(directory, 'qap.json'), stopPath, guarded);
      }),
    deleteFicIfExact: (fic, campaignId, _key, authority) =>
      run(authority, async (inventory, record, directory, guarded) => {
        const resourceId = record.workload.identityId + '/federatedIdentityCredentials/' + record.workload.credentialName;
        if (campaignId !== inventory.campaign_id || canonical_json(fic) !== canonical_json(inventory.fic) ||
            fic.resource_id !== resourceId || fic.name !== record.workload.credentialName)
          throw new Error('FIC cleanup requested a different original trust identity.');
        await recover_oat_workload_stop(azure, reference, path.join(directory, 'stop.json'), guarded);
        const ficPath = path.join(directory, 'fic.json');
        await recover_run_fic_record(azure, record.workload, ficPath, guarded);
        await cleanup_run_fic(azure, reference.run_id, ficPath, guarded);
      }),
  };
}

export function createExternalCleanupExecutor(store: CampaignStore, actions: ExternalCleanupActions,
  stopBudgets: EffectiveStopBudgets & { budget_source: 'effective_stop_budgets' | 'legacy_ceiling' }): CleanupExecutor {
  if (!Number.isSafeInteger(stopBudgets.maintenanceGraceSeconds) || stopBudgets.maintenanceGraceSeconds <= 0 ||
      !Number.isSafeInteger(stopBudgets.maintenanceStopTimeoutMs) || stopBudgets.maintenanceStopTimeoutMs <= 0 ||
      stopBudgets.maintenanceStopTimeoutMs > MAINTENANCE_STOP_TIMEOUT_CEILING_MS ||
      stopBudgets.maintenanceStopTimeoutMs < stopBudgets.maintenanceGraceSeconds * 1_000 ||
      stopBudgets.budget_source === 'legacy_ceiling' && stopBudgets.maintenanceStopTimeoutMs !== MAINTENANCE_STOP_TIMEOUT_CEILING_MS)
    throw new Error('External cleanup requires valid persisted maintenance stop budgets.');
  return async (step, suppliedState, idempotencyKey) => {
    if (suppliedState.status !== 'cleaning' || !suppliedState.runInventory ||
        !suppliedState.cleanupSteps.some(receipt => receipt.step === step && receipt.idempotencyKey === idempotencyKey && receipt.status === 'running')) {
      throw new Error('External cleanup requires a running receipt and persisted RunInventory.');
    }
    const ownerId = suppliedState.ownerId;
    const campaignId = suppliedState.campaignId;
    const fencingToken = suppliedState.fencingToken;
    let effectEtag: string | undefined, effectState: CampaignState | undefined;
    const withoutScaleTrials = (state: CampaignState) => { const copy = { ...state }; delete copy.scaleTrials; return copy; };
    const assertCurrent = async () => {
      const current = await store.read();
      if (!current || current.state.status !== 'cleaning' || current.state.campaignId !== campaignId ||
          current.state.ownerId !== ownerId || current.state.fencingToken !== fencingToken || !current.state.runInventory)
        throw new Error('Cleanup controller fencing changed before the mutation gate.');
      if (effectEtag !== undefined && current.etag !== effectEtag) {
        if (step !== 'record-terminal-state' || !effectState ||
            canonical_json(current.state.scaleTrials ?? []) === canonical_json(effectState.scaleTrials ?? []) ||
            canonical_json(withoutScaleTrials(current.state)) !== canonical_json(withoutScaleTrials(effectState)))
          throw new Error('Cleanup controller fencing changed before the mutation gate.');
        effectEtag = current.etag;
        effectState = current.state;
      }
      if (current.state.runInventory.owner_id !== suppliedState.runInventory!.owner_id ||
          current.state.runInventory.fencing_token !== suppliedState.runInventory!.fencing_token ||
          current.state.runInventory.cell_run_id !== suppliedState.runInventory!.cell_run_id ||
          current.state.runInventory.orchestration_run_id !== suppliedState.runInventory!.orchestration_run_id ||
          !current.state.cleanupSteps.some(receipt => receipt.step === step && receipt.idempotencyKey === idempotencyKey && receipt.status === 'running')) {
        throw new Error('Cleanup controller fencing changed before the mutation gate.');
      }
    };
    const authority: CleanupMutationAuthority = { mode: 'controller', campaignId, ownerId, fencingToken, assertCurrent };
    const writeInventory = async (update: (value: RunInventory) => RunInventory) => {
      await assertCurrent();
      const current = (await store.read())!;
      return publishCleanupRunInventory(store, update(current.state.runInventory!), ownerId, fencingToken);
    };
    await assertCurrent();
    let inventory = (await store.read())!.state.runInventory!;
    const discovered = await actions.discoverExactInventory(inventory);
    inventory = (await publishCleanupRunInventory(store, discovered, ownerId, fencingToken)).state.runInventory!;
    const currentState = (await store.read())!.state;
    const runAction = async (field: keyof RunInventory['cleanup'], action: (deadline: number | null) => Promise<CleanupActionResult>, budgetMs = 0): Promise<'pending' | 'complete'> => {
      const prior = inventory.cleanup[field];
      if (prior.state === 'complete') return 'complete';
      if (prior.state === 'failed' || field === 'handoff') return 'pending';
      const recovering = ['running', 'uncertain'].includes(prior.state);
      if (recovering && !actions.recoverLifecycle) return 'pending';
      const running = recovering ? prior : setStep(prior, 'running', budgetMs);
      if (!recovering) inventory = (await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, [field]: running } }))).state.runInventory!;
      await authority.assertCurrent();
      const admitted = (await store.read())!;
      if (canonical_json(admitted.state.runInventory) !== canonical_json(inventory)) throw new Error('Cleanup inventory changed before effect admission.');
      effectEtag = admitted.etag;
      effectState = admitted.state;
      let result: CleanupActionResult;
      try {
        await authority.assertCurrent();
        result = recovering ? await actions.recoverLifecycle!(field, inventory, running.deadline_unix_ms, idempotencyKey, authority) :
          await action(running.deadline_unix_ms);
      } finally { effectEtag = undefined; effectState = undefined; }
      if (result === 'pending') {
        if (recovering) return 'pending';
        inventory = (await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, [field]: setStep(value.cleanup[field], 'pending', budgetMs) } }))).state.runInventory!;
        return 'pending';
      }
      const updated = setStep(inventory.cleanup[field], result === 'complete' ? 'complete' : 'uncertain', 0,
        result === 'uncertain' ? 'action_outcome_uncertain' : null);
      inventory = (await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, [field]: updated } }))).state.runInventory!;
      return result === 'complete' ? 'complete' : 'pending';
    };
    switch (step) {
      case 'stop-admission':
        return runAction('stop_admission', deadline => actions.stopAdmission(inventory, deadline!, idempotencyKey, authority), ADMISSION_STOP_BUDGET_MS);
      case 'bounded-drain':
        return runAction('bounded_drain', deadline => actions.boundedDrain(inventory, deadline!, idempotencyKey, authority), drainBudgetMs(inventory));
      case 'preserve-evidence':
        return runAction('evidence_capture', () => actions.preserveEvidence(inventory, idempotencyKey, authority));
      case 'stop-maintenance': {
        return runAction('stop_maintenance', async deadline => {
          if (hasUnresolvedIdentity(inventory)) return 'pending';
          for (const resource of inventory.resources.filter(item => item.role === 'maintenance')) {
            if (resource.state === 'observed' && resource.uid) {
              if (Date.now() >= deadline!) return 'uncertain';
              const bounded = withCleanupDeadline(authority, deadline);
              await bounded.assertCurrent();
              const result = await actions.stopMaintenanceIfUidAndOwnerMatch(resource, inventory.namespace.owner_token,
                keyed(idempotencyKey, resource.name), bounded);
              if (result !== 'complete') {
                if (result === 'uncertain') inventory = (await writeInventory(value => ({ ...value, resources: value.resources.map(item =>
                  item.name === resource.name && item.kind === resource.kind ? { ...item, state: 'uncertain' } : item) }))).state.runInventory!;
                return result;
              }
            }
          }
          return 'complete';
        }, stopBudgets.maintenanceStopTimeoutMs);
      }
      case 'record-terminal-state':
        return runAction('terminal_capture', () => actions.captureTerminalState(inventory, idempotencyKey, authority), TERMINAL_CAPTURE_BUDGET_MS);
      case 'remove-run-namespace': {
        if (hasUnresolvedIdentity(inventory)) return 'pending';
        if (inventory.namespace.uid) {
          await authority.assertCurrent();
          const result = await actions.deleteNamespaceIfUidAndOwnerMatch(inventory.namespace, idempotencyKey, authority);
          if (result !== 'complete') return 'pending';
        }
        await writeInventory(value => ({ ...value, resources: value.resources.map(resource => ({ ...resource,
          state: resource.uid ? 'removed' : resource.state })) }));
        return 'complete';
      }
      case 'remove-run-qap': {
        if (!inventory.qap || inventory.qap.state === 'absent' || inventory.qap.state === 'removed') return 'complete';
        if (inventory.qap.state === 'uncertain' || inventory.qap.state === 'planned') return 'pending';
        if (inventory.qap.state === 'observed') {
          const absentBindings = new Set(inventory.qap.absence.map(item => item.external_table + '|' + item.relative_path));
          for (const binding of inventory.qap.bindings.filter(item => !absentBindings.has(item.external_table + '|' + item.relative_path))) {
            await authority.assertCurrent();
            const result = await actions.deleteQapBindingIfOwned(inventory.qap.database, binding, inventory.campaign_id,
              keyed(idempotencyKey, binding.external_table + ':' + binding.relative_path), authority);
            if (result !== 'complete') {
              if (result === 'uncertain') inventory = (await writeInventory(value => ({ ...value,
                qap: value.qap ? { ...value.qap, state: 'uncertain' } : null }))).state.runInventory!;
              return 'pending';
            }
          }
        }
        await writeInventory(value => ({ ...value, qap: value.qap ? { ...value.qap, state: 'removed' } : null }));
        return 'complete';
      }
      case 'remove-run-fic': {
        if (!inventory.fic || inventory.fic.state === 'absent' || inventory.fic.state === 'removed') return 'complete';
        if (inventory.fic.state === 'uncertain' || inventory.fic.state === 'planned') return 'pending';
        if (inventory.fic.state === 'observed') {
          await authority.assertCurrent();
          const result = await actions.deleteFicIfExact(inventory.fic, inventory.campaign_id, idempotencyKey, authority);
          if (result !== 'complete') {
            if (result === 'uncertain') inventory = (await writeInventory(value => ({ ...value,
              fic: value.fic ? { ...value.fic, state: 'uncertain' } : null }))).state.runInventory!;
            return 'pending';
          }
        }
        await writeInventory(value => ({ ...value, fic: value.fic ? { ...value.fic, state: 'removed' } : null }));
        return 'complete';
      }
      case 'restore-safe-idle': {
        await authority.assertCurrent();
        const result = await actions.restoreSafeIdle(currentState, idempotencyKey, authority);
        return result === 'complete' ? 'complete' : 'pending';
      }
      case 'verify-safe-idle': {
        const result = await actions.verifySafeIdle(currentState, idempotencyKey);
        if (result !== 'complete') return 'pending';
        const unsettled = hasVerifiedUnsettledDrain(currentState);
        if (inventory.cleanup.handoff.state === 'failed' && inventory.cleanup.handoff.reason_code !== DRAIN_UNSETTLED_REASON)
          throw new Error('Unrelated failed cell handoff cannot be replaced with clean or expiry verification.');
        const handoff = setStep(inventory.cleanup.handoff, unsettled ? 'failed' : 'complete', 0,
          unsettled ? DRAIN_UNSETTLED_REASON : null);
        await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, handoff } }));
        return 'complete';
      }
    }
  };
}
