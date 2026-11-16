import fs from 'node:fs';
import path from 'node:path';
import { canonical_json, runtime, type Azure } from '../../infra/azure/bootstrap.js';
import { create_namespace_deletion_journal } from '../../infra/azure/aks-namespace-custody.js';
import { cleanup_run_fic, delete_owned_namespace, find_run_fic_record, plan_run_fic, recover_run_fic_record,
  type RunFicPlan, type RunNamespaceIdentity } from '../../infra/azure/run_fic.js';
import type { CampaignStore, RunInventory, RunInventoryResource } from './aks-campaign.js';
import type { CleanupMutationAuthority, OwnedRunCleanupActions } from './aks-cleanup-executor.js';
import { cleanupBudget } from './aks-cleanup-budget.js';

export type CustodyCommandRunner = typeof runtime.run_tree;

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Exact workload observation is UNKNOWN.');
  return value as Record<string, unknown>;
}
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
function uid(value: unknown): string {
  if (typeof value !== 'string' || !uuid.test(value)) throw new Error('Exact workload UID is UNKNOWN.');
  return value;
}
export async function assertCleaningActor(store: CampaignStore, authority: CleanupMutationAuthority): Promise<RunInventory> {
  if (authority.mode === 'producer') throw new Error('Campaign-terminal cleanup requires controller authority.');
  return assertRunCleanupActor(store, authority);
}
export async function assertRunCleanupActor(store: CampaignStore, authority: CleanupMutationAuthority): Promise<RunInventory> {
  await authority.assertCurrent();
  const state = (await store.read())?.state;
  const producer = authority.mode === 'producer', now = Date.now();
  if (!state || state.status !== (producer ? 'active' : 'cleaning') || state.campaignId !== authority.campaignId || state.ownerId !== authority.ownerId ||
      producer && (state.runInventory?.fencing_token !== authority.fencingToken || now < state.heartbeatAtMs ||
        now - state.heartbeatAtMs >= 180_000 || now >= state.expiresAtMs) ||
      state.fencingToken !== authority.fencingToken || !state.runInventory)
    throw new Error(`The current ${producer ? 'producer' : 'cleaning'} actor is fenced.`);
  return state.runInventory;
}
export async function observeExactKubernetes(server: string, args: string[], authority: CleanupMutationAuthority,
  runCommand: CustodyCommandRunner = runtime.run_tree): Promise<Record<string, unknown> | undefined> {
  await authority.assertCurrent();
  const budget = cleanupBudget(authority), remaining = budget ? budget.deadlineUnixMs - Date.now() : 20_000;
  if (remaining <= 0) throw new Error('Exact Kubernetes observation budget expired before admission.');
  const response = await runCommand(['kubectl', '--server', server, '--insecure-skip-tls-verify=false',
    '--request-timeout', `${Math.max(1, Math.floor(Math.min(15_000, remaining)))}ms`,
    'get', ...args, '--ignore-not-found', '--output', 'json'], Math.min(20, remaining / 1000));
  if (response.returncode !== 0 || response.stdout.length > 4_194_304)
    throw new Error('Exact Kubernetes observation failed; permission/API errors are not absence.');
  if (!response.stdout.trim()) return undefined;
  return object(JSON.parse(response.stdout) as unknown);
}
function ownedMetadata(value: Record<string, unknown>, inventory: RunInventory, name: string, namespace?: string) {
  const metadata = object(value.metadata), labels = object(metadata.labels), annotations = object(metadata.annotations);
  if (metadata.name !== name || (namespace !== undefined && metadata.namespace !== namespace) ||
      labels['arrowcol-oat/run-id'] !== inventory.cell_run_id || annotations['arrowcol-oat/run-id'] !== inventory.cell_run_id ||
      labels['arrowcol-oat/owner-token'] !== inventory.namespace.owner_token ||
      annotations['arrowcol-oat/owner-token'] !== inventory.namespace.owner_token)
    throw new Error('Exact workload origin marker differs; no identity adoption or mutation.');
  return metadata;
}
async function validatePlan(azure: Azure, inventory: RunInventory): Promise<RunFicPlan> {
  const plan = inventory.fic?.plan;
  if (!plan || plan.runId !== inventory.cell_run_id || plan.namespace !== inventory.namespace.name ||
      plan.resourceId !== inventory.fic?.resource_id || plan.credentialName !== inventory.fic.name ||
      canonical_json(plan) !== canonical_json(await plan_run_fic(azure, plan.runId, plan.namespace, plan.serviceAccount)))
    throw new Error('Cleanup requires the exact persisted authoritative FIC plan, not a guessed identity.');
  return plan;
}
export async function discoverOwnedRunInventory(azure: Azure, supplied: RunInventory, authority: CleanupMutationAuthority,
  runCommand: CustodyCommandRunner = runtime.run_tree): Promise<RunInventory> {
  const inventory = structuredClone(supplied), plan = await validatePlan(azure, inventory);
  const originalFic = inventory.fic ? await find_run_fic_record(azure, plan, authority) : undefined;
  if (originalFic) {
    if (originalFic.namespaceOwner !== inventory.namespace.owner_token ||
        inventory.namespace.uid !== null && inventory.namespace.uid !== originalFic.namespaceUid)
      throw new Error('Original FIC creation proof belongs to another namespace UID/owner.');
    inventory.namespace.uid ??= originalFic.namespaceUid;
  }
  const namespace = await observeExactKubernetes(plan.server, ['namespace', inventory.namespace.name], authority, runCommand);
  if (namespace) {
    if (namespace.apiVersion !== 'v1' || namespace.kind !== 'Namespace') throw new Error('Exact namespace type differs.');
    const observedUid = uid(ownedMetadata(namespace, inventory, inventory.namespace.name).uid);
    if (inventory.namespace.uid !== null && inventory.namespace.uid !== observedUid) throw new Error('Original namespace UID changed.');
    inventory.namespace.uid = observedUid;
  }
  const originalNamespaceUid = inventory.namespace.uid;
  for (const resource of inventory.resources) {
    const type = resource.api_version + ':' + resource.kind;
    const names: Record<string, string> = { 'apps/v1:Deployment': 'deployment.apps', 'batch/v1:Job': 'job.batch', 'v1:Pod': 'pod' };
    if (!names[type]) throw new Error('Unsupported exact workload API/kind.');
    const value = await observeExactKubernetes(plan.server, [names[type], resource.name, '--namespace', resource.namespace], authority, runCommand);
    if (!value) {
      if (resource.uid === null) {
        resource.state = 'absent';
        resource.absence ??= { api_version: resource.api_version, kind: resource.kind, namespace: resource.namespace,
          name: resource.name, namespace_uid: originalNamespaceUid, observed_at_unix_ms: Date.now() };
      }
      continue;
    }
    if (!namespace || value.apiVersion !== resource.api_version || value.kind !== resource.kind ||
        resource.state === 'absent' || resource.state === 'removed')
      throw new Error('Exact workload appeared outside its original namespace or terminal receipt.');
    const metadata = object(value.metadata);
    if (metadata.name !== resource.name || metadata.namespace !== resource.namespace) throw new Error('Workload name/namespace differs.');
    const observedUid = uid(metadata.uid);
    if (resource.uid !== null && resource.uid !== observedUid) throw new Error('Original workload UID changed.');
    if (resource.uid === null) ownedMetadata(value, inventory, resource.name, resource.namespace);
    const currentNamespace = await observeExactKubernetes(plan.server, ['namespace', inventory.namespace.name], authority, runCommand);
    if (!currentNamespace || uid(ownedMetadata(currentNamespace, inventory, inventory.namespace.name).uid) !== originalNamespaceUid)
      throw new Error('Namespace identity changed during exact workload discovery.');
    resource.uid = observedUid;
    resource.state = 'observed';
  }
  if (inventory.fic) {
    const record = originalFic;
    if (!record) {
      if (inventory.fic.state === 'observed' || inventory.fic.state === 'removed')
        throw new Error('Previously observed FIC lost its original durable proof.');
      inventory.fic.state = 'absent';
      inventory.fic.absence ??= { resource_id: plan.resourceId, name: plan.credentialName, observed_at_unix_ms: Date.now() };
    } else {
      if (record.namespaceOwner !== inventory.namespace.owner_token || record.namespaceUid !== inventory.namespace.uid)
        throw new Error('Original FIC creation proof belongs to another namespace UID/owner.');
      if (inventory.fic.state === 'absent') throw new Error('A terminal absent FIC cannot be adopted.');
      inventory.fic.state = record.phase === 'deleted' ? 'removed' : 'observed';
    }
  }
  return inventory;
}

export function createSetupRunCleanupActions(store: CampaignStore, azure: Azure, plan: RunFicPlan,
  workDirectory: string): OwnedRunCleanupActions {
  const current = async (authority: CleanupMutationAuthority) => {
    const inventory = await assertRunCleanupActor(store, authority);
    if (inventory.qap !== null || canonical_json(inventory.fic?.plan) !== canonical_json(plan) ||
        plan.runId !== inventory.cell_run_id || plan.namespace !== inventory.namespace.name)
      throw new Error('Setup cleanup requires the exact durable plan and no planned QAP binding.');
    await validatePlan(azure, inventory);
    return inventory;
  };
  const withNamespace = async (authority: CleanupMutationAuthority,
    work: (inventory: RunInventory, identity: RunNamespaceIdentity, directory: string, guarded: CleanupMutationAuthority) => Promise<void>): Promise<'complete'> => {
    const inventory = await current(authority);
    if (!inventory.namespace.uid) throw new Error('Unknown namespace UID never authorizes delete.');
    const stat = fs.lstatSync(workDirectory);
    if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0) throw new Error('Setup cleanup needs a private working directory.');
    const identity: RunNamespaceIdentity = { version: 1, runId: inventory.cell_run_id, namespace: inventory.namespace.name,
      namespaceUid: inventory.namespace.uid, namespaceOwner: inventory.namespace.owner_token, aksId: plan.aksId, server: plan.server };
    const guarded = { ...authority, assertCurrent: async () => { await current(authority); } };
    const directory = fs.mkdtempSync(path.join(workDirectory, 'setup-cleanup-'));
    await work(inventory, identity, directory, guarded);
    await guarded.assertCurrent();
    fs.rmSync(directory, { recursive: true });
    return 'complete';
  };
  return {
    deleteNamespaceIfUidAndOwnerMatch: (requested, _key, authority) => withNamespace(authority, async (inventory, identity, directory, guarded) => {
      if (canonical_json(requested) !== canonical_json(inventory.namespace)) throw new Error('Original namespace identity changed.');
      const journal = create_namespace_deletion_journal(azure, identity, guarded);
      const receipt = await delete_owned_namespace(azure, identity, path.join(directory, 'stop.json'), guarded, journal);
      await journal.complete(receipt);
    }),
    deleteFicIfExact: (fic, campaignId, _key, authority) => withNamespace(authority, async (inventory, identity, directory, guarded) => {
      if (campaignId !== inventory.campaign_id || canonical_json(fic) !== canonical_json(inventory.fic) ||
          fic.resource_id !== plan.resourceId || fic.name !== plan.credentialName) throw new Error('Exact setup FIC identity changed.');
      await create_namespace_deletion_journal(azure, identity, guarded).stopped();
      const original = await find_run_fic_record(azure, plan, guarded);
      if (!original) throw new Error('Setup FIC has no original creation proof; discover a verified absence instead.');
      if (original.namespaceUid !== identity.namespaceUid || original.namespaceOwner !== identity.namespaceOwner)
        throw new Error('Setup FIC proof belongs to a different namespace.');
      const filename = path.join(directory, 'fic.json');
      await recover_run_fic_record(azure, original, filename, guarded);
      await cleanup_run_fic(azure, plan.runId, filename, guarded);
    }),
    deleteQapBindingIfOwned: async () => { throw new Error('No QAP binding exists in this setup cleanup authority.'); },
  };
}
