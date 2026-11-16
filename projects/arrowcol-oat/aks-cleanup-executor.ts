import { publishCleanupRunInventory, type CampaignState, type CampaignStore, type InventoryStep, type RunInventory, type RunInventoryResource } from './aks-campaign.js';
import type { CleanupExecutor } from './aks-reconcile.js';

export type CleanupActionResult = 'pending' | 'complete' | 'uncertain';
export interface CleanupMutationAuthority {
  campaignId: string;
  ownerId: string;
  fencingToken: number;
  assertCurrent(): Promise<void>;
}
export interface ExternalCleanupActions {
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

const ADMISSION_STOP_BUDGET_MS = 60_000;
const DRAIN_BUDGET_MS = 120_000;

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
    case 'record-terminal-state': return 'terminal_capture';
    default: return undefined;
  }
}

export function createExternalCleanupExecutor(store: CampaignStore, actions: ExternalCleanupActions): CleanupExecutor {
  return async (step, suppliedState, idempotencyKey) => {
    if (suppliedState.status !== 'cleaning' || !suppliedState.runInventory ||
        !suppliedState.cleanupSteps.some(receipt => receipt.step === step && receipt.idempotencyKey === idempotencyKey && receipt.status === 'running')) {
      throw new Error('External cleanup requires a running receipt and persisted RunInventory.');
    }
    const ownerId = suppliedState.ownerId;
    const campaignId = suppliedState.campaignId;
    const fencingToken = suppliedState.fencingToken;
    const assertCurrent = async () => {
      const current = await store.read();
      if (!current || current.state.status !== 'cleaning' || current.state.campaignId !== campaignId ||
          current.state.ownerId !== ownerId || current.state.fencingToken !== fencingToken || !current.state.runInventory ||
          current.state.runInventory.owner_id !== suppliedState.runInventory!.owner_id ||
          current.state.runInventory.fencing_token !== suppliedState.runInventory!.fencing_token) {
        throw new Error('Cleanup controller fencing changed before the mutation gate.');
      }
    };
    const authority: CleanupMutationAuthority = { campaignId, ownerId, fencingToken, assertCurrent };
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
      const running = setStep(prior, 'running', budgetMs);
      inventory = (await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, [field]: running } }))).state.runInventory!;
      await authority.assertCurrent();
      const result = await action(running.deadline_unix_ms);
      if (result === 'pending') {
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
        return runAction('bounded_drain', deadline => actions.boundedDrain(inventory, deadline!, idempotencyKey, authority), DRAIN_BUDGET_MS);
      case 'preserve-evidence':
        return runAction('evidence_capture', () => actions.preserveEvidence(inventory, idempotencyKey, authority));
      case 'stop-maintenance': {
        if (hasUnresolvedIdentity(inventory)) return 'pending';
        for (const resource of inventory.resources.filter(item => item.role === 'maintenance')) {
          if (resource.state === 'observed' && resource.uid) {
            await authority.assertCurrent();
            const result = await actions.stopMaintenanceIfUidAndOwnerMatch(resource, inventory.namespace.owner_token,
              keyed(idempotencyKey, resource.name), authority);
            if (result !== 'complete') {
              if (result === 'uncertain') inventory = (await writeInventory(value => ({ ...value, resources: value.resources.map(item =>
                item.name === resource.name && item.kind === resource.kind ? { ...item, state: 'uncertain' } : item) }))).state.runInventory!;
              return 'pending';
            }
          }
        }
        return 'complete';
      }
      case 'record-terminal-state':
        return runAction('terminal_capture', () => actions.captureTerminalState(inventory, idempotencyKey, authority));
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
        if (!inventory.qap) return 'complete';
        if (inventory.qap.state === 'uncertain' || inventory.qap.state === 'planned') return 'pending';
        if (inventory.qap.state === 'observed') {
          for (const binding of inventory.qap.bindings) {
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
        if (!inventory.fic) return 'complete';
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
        const handoff = setStep(inventory.cleanup.handoff, 'complete');
        await writeInventory(value => ({ ...value, cleanup: { ...value.cleanup, handoff } }));
        return 'complete';
      }
    }
  };
}
