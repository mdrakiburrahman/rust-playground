import { assertDirectInventory } from './benchmark-direct-handoff.js';
import { loadDurableArm } from './benchmark-direct-cli.js';
import type { DirectRelease } from './benchmark-direct-arm.js';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { isDeepStrictEqual } from 'node:util';
import { create_namespace_deletion_journal } from './aks-namespace-custody.js';
import { readDirectEvidenceSeal, readSealedDirectTerminal } from './benchmark-evidence-custody.js';
import { evidenceJson } from './benchmark-audit.js';
import { captureFrozenDelta } from './benchmark-delta-evidence.js';
import { evidenceObject, parseEvidence, requireEvidence } from './benchmark-evidence-json.js';
import { canonical_json, type Azure } from './bootstrap.js';
import { FENCED, PublisherControl, READY } from './publisher_control.js';
import { assert_owned_namespace_absent, find_run_fic_record } from './run_fic.js';
import type { CampaignStore, StoredCampaign } from '../../projects/arrowcol-oat/aks-campaign.js';

export async function inspectDirectRecovery(azure: Azure, store: CampaignStore, selected: StoredCampaign,
  release: DirectRelease, signal: AbortSignal, workDirectory: string, interrupted = false) {
  const { state, etag } = selected;
  const inventory = state.runInventory;
  const preAdmission = interrupted && inventory?.load_admission === 'not_authorized';
  requireEvidence(inventory && /^"[^"\r\n]+"$/.test(etag) && state.mode === 'armed' &&
    (state.status === 'fenced' || state.status === 'cleaning') &&
    state.campaignId === inventory.campaign_id && state.ownerId === inventory.owner_id &&
    state.fencingToken === inventory.fencing_token + 1 && (inventory.load_admission === 'authorized' || preAdmission) &&
    inventory.qap === null && inventory.namespace.uid && inventory.namespace.owner_token === state.ownerId &&
    inventory.fic?.plan, 'Direct recovery requires the exact fenced authorized inventory and original FIC plan.');
  const assertCurrent = async () => {
    signal.throwIfAborted();
    const now = await store.read();
    signal.throwIfAborted();
    requireEvidence(now?.etag === etag && canonical_json(now.state) === canonical_json(state),
      'Original fenced campaign changed during direct recovery inspection.');
  };
  const authority = { assertCurrent };
  await assertCurrent();
  const fic = await find_run_fic_record(azure, inventory.fic.plan, authority);
  requireEvidence(fic && fic.runId === inventory.cell_run_id && fic.namespace === inventory.namespace.name &&
    fic.namespaceUid === inventory.namespace.uid && fic.namespaceOwner === inventory.namespace.owner_token,
  'Original off-node FIC ledger does not match the authorized direct inventory.');
  await assertCurrent();
  const control = new PublisherControl(azure);
  const [publisher, registry, publisherEtag] = await control.read();
  requireEvidence(publisher.occupancy === 'released' && (publisher.resetstate === READY &&
    Object.values(registry).every(entry => entry.state === 'active') ||
    publisher.resetstate === FENCED && Object.keys(registry).length === 0),
  'Original exclusive publisher generation is unresolved.');
  const arm = await loadDurableArm(azure, { runId: inventory.cell_run_id, namespace: inventory.namespace.name,
    owner: inventory.namespace.owner_token, namespaceUid: inventory.namespace.uid, sourceRevision: release.revision,
    cloudGeneration: publisher.resetgeneration, fencingToken: inventory.fencing_token }, release);
  requireEvidence(!preAdmission || arm.scheduleDisposition === 'held' && arm.heldBarrier,
    'Pre-admission recovery needs an original held arm with versioned barrier proof.');
  assertDirectInventory(azure, inventory, fic, 'finish', arm);
  const seal = await readDirectEvidenceSeal(azure, authority, arm.origin);
  requireEvidence(interrupted || fic.phase === 'ready' || !!seal, 'FIC deletion has no original terminal seal.');
  if (seal) {
    requireEvidence(publisher.resetstate === FENCED, 'Original terminal seal requires a fenced publisher generation.');
    const sealed = await readSealedDirectTerminal(azure, authority, arm.origin, seal);
    requireEvidence(canonical_json(sealed.terminal.arm) === canonical_json(arm),
      'Original terminal proof arm changed.');
    const directory = await mkdtemp(join(workDirectory, 'delta-inspection-'));
    try {
      const current = await captureFrozenDelta(azure, authority, arm.origin.runId, arm.baseline.table_root, arm.baseline, directory);
      const expected = evidenceObject(evidenceObject(parseEvidence(sealed.raw)).delta);
      const identity = (files: ReadonlyArray<{ key: unknown; bytes: unknown; sha256: unknown }>) =>
        files.map(({ key, bytes, sha256 }) => ({ key, bytes, sha256 })).sort((a, b) => String(a.key).localeCompare(String(b.key)));
      requireEvidence(isDeepStrictEqual(parseEvidence(evidenceJson(current.snapshot)), expected.snapshot) &&
        isDeepStrictEqual(parseEvidence(evidenceJson(current.physical)), expected.physical) &&
        canonical_json(identity(current.evidence)) === canonical_json(identity(sealed.delta.map(file =>
          ({ key: file.key, bytes: file.bytes, sha256: file.sha256 })))),
      'Original terminal Delta history or physical file versions changed.');
    } finally { await rm(directory, { recursive: true, force: true }); }
    await readDirectEvidenceSeal(azure, authority, arm.origin, seal);
  }
  if (fic.phase === 'deleted' && seal) {
    const identity = { version: 1 as const, runId: fic.runId, namespace: fic.namespace, namespaceUid: fic.namespaceUid,
      namespaceOwner: fic.namespaceOwner, aksId: fic.aksId, server: fic.server };
    await create_namespace_deletion_journal(azure, identity, authority).stopped();
    await assert_owned_namespace_absent(identity, authority);
  }
  const [after, afterRegistry, afterEtag] = await control.read();
  requireEvidence(afterEtag === publisherEtag && canonical_json(after) === canonical_json(publisher) &&
    canonical_json(afterRegistry) === canonical_json(registry), 'Publisher authority changed during direct recovery inspection.');
  await assertCurrent();
  return { arm, fic, seal, authority, preAdmission };
}
