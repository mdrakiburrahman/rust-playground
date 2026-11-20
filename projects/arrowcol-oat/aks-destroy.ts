import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { stringify } from 'yaml';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { capturedManagedGroups, resetTimeoutMinutes } from '../../tools/scripts/src/qap-sandbox-reset.js';
import type { DestroyOptions, DestroyResult } from './aks-types.js';

const GROUP = 'arrow-collector-rg';
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const DERIVED = [
  '.env.azure', '.env.azure.tables.json',
  'out/arrowcol-oat/runtime.json', 'out/arrowcol-oat/bootstrap.env',
  'out/arrowcol-oat/aks.env', 'out/arrowcol-oat/oat-config.aks.json',
  'out/arrowcol-oat/secrets',
];

function outsideCheckout(filename: string): string {
  if (!path.isAbsolute(filename) || filename.split(/[\\/]/).includes('..'))
    throw new Error('--receipt must be an absolute path without traversal outside this checkout.');
  const root = fs.realpathSync(ROOT);
  const resolved = path.resolve(filename);
  const parent = fs.realpathSync(path.dirname(resolved));
  if (resolved === root || resolved.startsWith(root + path.sep) || parent === root || parent.startsWith(root + path.sep))
    throw new Error('--receipt must be outside this checkout, including through symlinks.');
  const receipt = path.join(parent, path.basename(resolved));
  if (fs.existsSync(receipt)) throw new Error('--receipt already exists; choose a new receipt path.');
  return receipt;
}

export function removeDerivedState(root: string): void {
  for (const relative of DERIVED) {
    let parent = root;
    for (const part of relative.split('/').slice(0, -1)) {
      parent = path.join(parent, part);
      if (fs.lstatSync(parent, { throwIfNoEntry: false })?.isSymbolicLink())
        throw new Error(`Refusing derived-state cleanup through symlink: ${parent}`);
    }
  }
  for (const relative of DERIVED)
    fs.rmSync(path.join(root, relative), { recursive: relative === 'out/arrowcol-oat/secrets', force: true });
}

export async function destroy(options: DestroyOptions, runner: typeof run_tree = run_tree): Promise<DestroyResult> {
  const resourceGroup = options.resourceGroup ?? GROUP;
  if (resourceGroup !== GROUP) throw new Error(`--resource-group must be ${GROUP}; other resource groups are not authorized.`);
  if (options.confirm !== undefined && options.confirm !== resourceGroup)
    throw new Error(`Destruction requires --confirm ${resourceGroup}.`);
  const timeoutMinutes = resetTimeoutMinutes(options.timeoutMinutes);
  const subscription = options.subscription ??
    loadStampConfig(path.resolve(ROOT, options.stampConfig ?? 'infra/azure/stamp.yaml')).subscription;
  if (!UUID.test(subscription)) throw new Error('--subscription must be a UUID.');
  const request = async (args: string[]): Promise<unknown> => {
    const result = await runner(['az', ...args, '--subscription', subscription, '--only-show-errors', '--output', 'json'], 120, ROOT);
    if (result.returncode) throw new Error(`Azure ${args.slice(0, 2).join(' ')} failed (exit ${result.returncode}); no local state was removed.`);
    return JSON.parse(result.stdout) as unknown;
  };
  const account = await request(['account', 'show']);
  if (!account || typeof account !== 'object' || !('id' in account) ||
      typeof account.id !== 'string' || account.id.toLowerCase() !== subscription.toLowerCase())
    throw new Error('Azure account does not match the authorized subscription.');
  const exists = async (name: string): Promise<boolean> => {
    const value = await request(['group', 'exists', '--name', name]);
    if (typeof value !== 'boolean') throw new Error('Azure returned an invalid resource-group existence response.');
    return value;
  };
  const existed = await exists(resourceGroup);
  const clusters = existed ? await request(['aks', 'list', '--resource-group', resourceGroup]) : [];
  const managedResourceGroups = capturedManagedGroups(clusters, subscription);
  const resources = existed ? await request(['resource', 'list', '--resource-group', resourceGroup]) : [];
  if (!Array.isArray(resources)) throw new Error('Azure returned an invalid resource inventory.');
  console.log(stringify({ resourceGroup, existed, managedResourceGroups, resources }));
  if (options.confirm === undefined) {
    console.log(`Read-only inventory; nothing changed. To delete: npx --no-install arrowcol-oat aks destroy --subscription ${subscription} --confirm ${resourceGroup}`);
    return { resourceGroup, existed, deleted: false, dryRun: true, managedResourceGroups };
  }
  const receiptPath = outsideCheckout(options.receiptPath ??
    path.join(os.tmpdir(), `arrowcol-oat-destroy-${randomUUID()}.json`));
  let reset: Awaited<ReturnType<typeof runner>>;
  try {
    reset = await runner(['npx', '--no-install', 'tsx', 'tools/scripts/src/qap-sandbox-reset.ts',
      '--subscription', subscription, '--confirm-reset', resourceGroup, '--receipt', receiptPath,
      '--timeout-minutes', String(timeoutMinutes)], timeoutMinutes * 60, ROOT);
  } catch (error) {
    throw new Error(`Sandbox reset outcome is unknown; inspect Azure before retrying. No local state was removed. Receipt: ${receiptPath}`, { cause: error });
  }
  if (reset.returncode) throw new Error(`Sandbox reset failed (exit ${reset.returncode}); no local state was removed. Receipt: ${receiptPath}`);
  const evidence: unknown = JSON.parse(fs.readFileSync(receiptPath, 'utf8'));
  if (!evidence || typeof evidence !== 'object' || !('stage' in evidence) || evidence.stage !== 'complete' ||
      !('exitCode' in evidence) || evidence.exitCode !== 0 ||
      !('subscription' in evidence) || typeof evidence.subscription !== 'string' ||
      evidence.subscription.toLowerCase() !== subscription.toLowerCase() ||
      !('group' in evidence) || evidence.group !== resourceGroup)
    throw new Error(`Reset did not produce a matching completion receipt; no local state was removed. Receipt: ${receiptPath}`);
  for (const group of [resourceGroup, ...managedResourceGroups])
    if (await exists(group)) throw new Error(`Resource group ${group} is still present; no local state was removed. Receipt: ${receiptPath}`);
  removeDerivedState(ROOT);
  console.log(`${existed ? 'Deleted' : 'Already absent:'} ${resourceGroup}; devbox-local derived state removed. Receipt: ${receiptPath}`);
  return { resourceGroup, existed, deleted: existed, dryRun: false, managedResourceGroups, receiptPath };
}

export async function main(args: string[], runner: typeof run_tree = run_tree): Promise<void> {
  const { values } = parseArgs({ args, options: {
    subscription: { type: 'string' }, 'resource-group': { type: 'string' }, confirm: { type: 'string' },
    receipt: { type: 'string' }, 'timeout-minutes': { type: 'string' }, 'stamp-config': { type: 'string' },
  } });
  await destroy({
    subscription: values.subscription, resourceGroup: values['resource-group'], confirm: values.confirm,
    receiptPath: values.receipt, timeoutMinutes: values['timeout-minutes'] === undefined ? undefined : Number(values['timeout-minutes']),
    stampConfig: values['stamp-config'],
  }, runner);
}
