import { validateCampaignState, type RunInventory } from '../../projects/arrowcol-oat/aks-campaign.js';
import { CELL_CLEANUP_SELECTORS, createBlobCellStepJournal, executeCellCleanupStep,
  type CellCleanupEnvelope, type CellCleanupSelector, type CellStepProvider } from '../../projects/arrowcol-oat/aks-cell-cleanup.js';
import { directEvidencePrefix } from './benchmark-evidence-custody.js';
import { requireEvidence } from './benchmark-evidence-json.js';
import { directProducerAuthority, type DirectCampaignScope } from './benchmark-run-authority.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import type { DirectTerminalArm, finishDirectTerminalCustody } from './benchmark-terminal-custody.js';
import { canonical_json, type Azure } from './bootstrap.js';
import type { RunFicRecord } from './run_fic.js';

const workload = [
  { api_version: 'apps/v1', kind: 'Deployment', name: 'arrowcol', role: 'collector' },
  { api_version: 'apps/v1', kind: 'Deployment', name: 'delta-maintenance', role: 'maintenance' },
  { api_version: 'batch/v1', kind: 'Job', name: 'direct-load-confirmwide', role: 'client' },
] as const;

export function assertDirectInventory(azure: Azure, inventory: RunInventory, fic: RunFicRecord, action: 'arm' | 'finish',
  arm?: DirectTerminalArm): void {
  const prefix = directEvidencePrefix({ runId: fic.runId, namespaceUid: fic.namespaceUid });
  requireEvidence(inventory.qap === null && !inventory.official_measurement &&
    canonical_json(inventory.evidence_destination) === canonical_json({
      account: azure.c.account, container: azure.c.checkpoints, blob_prefix: prefix,
    }), 'Direct inventory must bind the exact evidence prefix and must not own OAT measurement or QAP bindings');
  requireEvidence(inventory.fic?.resource_id === `${fic.identityId}/federatedIdentityCredentials/${fic.credentialName}` &&
    inventory.fic.name === fic.credentialName && (inventory.fic.state === 'observed' || action === 'finish' &&
      inventory.fic.state === 'removed'), 'Direct inventory does not own the original FIC');
  const uids = arm ? [arm.writer.deploymentUid, arm.maintenance.deploymentUid, arm.clients[0]?.jobUid] : undefined;
  requireEvidence(inventory.resources.length === workload.length && workload.every((expected, index) =>
    inventory.resources.some(resource => resource.api_version === expected.api_version && resource.kind === expected.kind &&
      resource.name === expected.name && resource.role === expected.role && resource.namespace === fic.namespace &&
      resource.uid && (!uids || resource.uid === uids[index]) && (resource.state === 'observed' ||
        action === 'finish' && resource.state === 'removed'))),
  'Direct inventory does not own the exact writer, maintenance and client Job UIDs');
  if (action === 'arm')
    requireEvidence(Object.values(inventory.cleanup).every(step => step.state === 'not_started'),
      'Cannot arm after direct cleanup has begun');
}

/** The terminal adapter returns only after positive seal, namespace and FIC observations.
 * Providers below reconcile those effects into the existing per-cell journal; none may redispatch them. */
export async function publishDirectHandoff(azure: Azure, campaign: DirectCampaignScope, origin: DirectStopOrigin,
  result: Awaited<ReturnType<typeof finishDirectTerminalCustody>>) {
  const authority = directProducerAuthority(campaign, origin, 'finish');
  const currentEnvelope = async (): Promise<CellCleanupEnvelope> => {
    await authority.assertCurrent();
    const current = await campaign.store.read();
    requireEvidence(current, 'Direct handoff has no current campaign');
    const state = validateCampaignState(current.state), inventory = state.runInventory;
    requireEvidence(state.status === 'active' && state.mode === 'armed' && state.ownerId === campaign.ownerId &&
      state.campaignId === campaign.campaignId && state.fencingToken === origin.fencingToken &&
      inventory && inventory.cell_run_id === origin.runId && inventory.namespace.uid === origin.namespaceUid,
    'Direct handoff campaign or original run changed');
    return { schema_version: 1, inventory, expected_etag: current.etag,
      actor: { mode: 'producer', owner_id: campaign.ownerId, fencing_token: origin.fencingToken } };
  };
  for (const selector of Object.keys(CELL_CLEANUP_SELECTORS) as CellCleanupSelector[]) {
    const envelope = await currentEnvelope();
    const journal = (assertCurrent: () => Promise<void>) => createBlobCellStepJournal(azure, envelope, assertCurrent);
    const recorded = await journal(authority.assertCurrent).read(selector);
    if (recorded?.value.phase === 'complete') continue;
    const prove: CellStepProvider['execute'] = async (inventory, _key, stepAuthority) => {
      await stepAuthority.assertCurrent();
      await result.assertSealCurrent();
      switch (selector) {
        case 'remove_namespace':
          requireEvidence(result.receipt.namespace_record.namespaceUid === inventory.namespace.uid &&
            result.receipt.namespace_record.namespace === inventory.namespace.name &&
            result.receipt.namespace_record.namespaceOwner === inventory.owner_id &&
            result.receipt.namespace_record.runId === inventory.cell_run_id,
          'Direct namespace receipt does not bind this inventory');
          return { ...inventory, resources: inventory.resources.map(resource => ({ ...resource, state: 'removed' })) };
        case 'remove_qap':
          requireEvidence(inventory.qap === null, 'Direct handoff cannot settle a QAP binding');
          break;
        case 'remove_fic':
          requireEvidence(inventory.fic, 'Direct handoff has no original FIC inventory');
          return { ...inventory, fic: { ...inventory.fic, state: 'removed' } };
        case 'handoff':
          await result.assertTerminalCurrent();
          break;
      }
      return inventory;
    };
    await executeCellCleanupStep(campaign.store, envelope, selector, { [selector]: { execute: prove, recover: prove } }, journal);
  }
  const final = await currentEnvelope();
  requireEvidence(final.inventory.cleanup.handoff.state === 'complete', 'Direct inventory handoff was not durably completed');
  await result.assertTerminalCurrent();
  return final.expected_etag;
}
