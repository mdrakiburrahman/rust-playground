import { assertRunCleanupActor } from '../../projects/arrowcol-oat/aks-setup-cleanup.js';
import type { CampaignStore } from '../../projects/arrowcol-oat/aks-campaign.js';
import { requireEvidence } from './benchmark-evidence-json.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import type { DirectRunAuthority } from './benchmark-terminal-custody.js';

export interface DirectCampaignScope { store: CampaignStore; campaignId: string; ownerId: string }

/** Reuse the active producer fence without calling the OAT measurement/cleanup engine. */
export function directProducerAuthority({ store, campaignId, ownerId }: DirectCampaignScope, origin: DirectStopOrigin,
  phase: 'arm' | 'finish' = 'arm'): DirectRunAuthority {
  const { runId, owner, fencingToken } = origin;
  return {
    runId, owner, fencingToken,
    async assertCurrent() {
      const current = await assertRunCleanupActor(store, {
        mode: 'producer', campaignId, ownerId, fencingToken, assertCurrent: async () => {},
      });
      requireEvidence(current.campaign_id === campaignId && current.owner_id === ownerId &&
        current.cell_run_id === runId && current.fencing_token === fencingToken && current.namespace.name === origin.namespace &&
        current.namespace.uid === origin.namespaceUid && current.namespace.owner_token === owner &&
        current.load_admission === 'authorized' && (current.fic?.state === 'observed' || phase === 'finish' && current.fic?.state === 'removed') &&
        (phase === 'finish' || current.cleanup.handoff.state !== 'complete'),
      'The active producer does not own this direct run, namespace and FIC');
    },
  };
}
