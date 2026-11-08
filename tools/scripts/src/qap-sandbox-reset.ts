import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';

const GROUP = 'arrow-collector-rg';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function azCommand(args: string[], platform: NodeJS.Platform): { executable: string; args: string[] } {
  if (platform !== 'win32') return { executable: 'az', args };
  if (args.some(arg => !/^[A-Za-z0-9_.():/-]+$/.test(arg))) throw new Error('Unsafe Azure CLI argument.');
  return { executable: process.env.ComSpec || 'cmd.exe', args: ['/d', '/s', '/c', `az.cmd ${args.join(' ')}`] };
}

export function capturedManagedGroups(value: unknown, subscription: string): string[] {
  if (!Array.isArray(value) || value.length > 20) throw new Error('AKS inventory is invalid or too large.');
  const prefix = `/subscriptions/${subscription}/resourceGroups/${GROUP}/providers/Microsoft.ContainerService/managedClusters/`.toLowerCase();
  const groups = new Set<string>();
  for (const cluster of value) {
    if (!cluster || typeof cluster !== 'object') throw new Error('Invalid AKS inventory entry.');
    const { id, name, nodeResourceGroup } = cluster as Record<string, unknown>;
    if (typeof id !== 'string' || typeof name !== 'string' || !/^arrow-collector-[1-9][0-9]{0,5}$/.test(name) ||
        id.toLowerCase() !== prefix + name.toLowerCase() ||
        typeof nodeResourceGroup !== 'string' || !/^MC_[A-Za-z0-9_.()-]{1,80}$/i.test(nodeResourceGroup) ||
        nodeResourceGroup.toLowerCase() === GROUP.toLowerCase()) {
      throw new Error('AKS inventory contains an unexpected cluster or managed resource group.');
    }
    groups.add(nodeResourceGroup);
  }
  return [...groups].sort();
}

function az(args: string[]): string {
  const plan = azCommand(args, process.platform);
  const result = spawnSync(plan.executable, plan.args, { encoding: 'utf8', timeout: 120_000, maxBuffer: 8 * 1024 * 1024, windowsHide: true });
  if (result.error || result.status !== 0) {
    const code = (result.error as NodeJS.ErrnoException | undefined)?.code;
    throw new Error(`az ${args.slice(0, 2).join(' ')} failed (${result.status ?? code ?? result.signal ?? 'unknown'}).`);
  }
  return result.stdout.trim();
}

function exists(subscription: string, group: string): boolean {
  const result = az(['group', 'exists', '--subscription', subscription, '--name', group, '--output', 'json']);
  if (result !== 'true' && result !== 'false') throw new Error('Unexpected az group exists response.');
  return result === 'true';
}

async function main(): Promise<void> {
  const { values } = parseArgs({ options: {
    subscription: { type: 'string' },
    'confirm-reset': { type: 'string' },
    receipt: { type: 'string' },
  } });
  const subscription = values.subscription;
  if (!subscription || !UUID.test(subscription) || values['confirm-reset'] !== GROUP)
    throw new Error('Require --subscription <UUID> --confirm-reset arrow-collector-rg.');
  const receipt = values.receipt;
  if (!receipt || !path.isAbsolute(receipt) || receipt.split(/[\\/]/).includes('..'))
    throw new Error('Require an absolute --receipt path without traversal outside this checkout.');
  const root = fs.realpathSync('.');
  const resolved = path.resolve(receipt);
  if (resolved === root || resolved.startsWith(root + path.sep))
    throw new Error('Receipt must be outside this checkout.');
  const parent = fs.realpathSync(path.dirname(resolved));
  if (parent === root || parent.startsWith(root + path.sep))
    throw new Error('Receipt must be outside this checkout.');
  const file = path.join(parent, path.basename(resolved));
  const fd = fs.openSync(file, 'wx', 0o600);
  const evidence: Record<string, unknown> = {
    subscription, group: GROUP, stage: 'authorization', startedAt: new Date().toISOString(),
  };
  const save = () => {
    fs.ftruncateSync(fd, 0);
    fs.writeSync(fd, JSON.stringify(evidence, null, 2) + '\n', 0, 'utf8');
    fs.fsyncSync(fd);
  };
  try {
    save();
    const accountOutput = az(['account', 'show', '--subscription', subscription, '--output', 'json']);
    let account: unknown;
    try { account = JSON.parse(accountOutput) as unknown; }
    catch { throw new Error('Invalid Azure account JSON.'); }
    if (!account || typeof account !== 'object' || !('id' in account) ||
        typeof account.id !== 'string' || account.id.toLowerCase() !== subscription.toLowerCase())
      throw new Error('Azure account does not match the authorized subscription.');
    evidence.stage = 'inventory'; save();
    const groupExists = exists(subscription, GROUP);
    let managedGroups: string[] = [];
    if (groupExists) {
      const inventoryOutput = az(['aks', 'list', '--subscription', subscription, '--resource-group', GROUP, '--output', 'json']);
      let inventory: unknown;
      try { inventory = JSON.parse(inventoryOutput) as unknown; } catch { throw new Error('Invalid AKS inventory JSON.'); }
      managedGroups = capturedManagedGroups(inventory, subscription);
    }
    evidence.managedGroups = managedGroups;
    evidence.stage = 'deletion'; save();
    if (groupExists) {
      az(['group', 'delete', '--subscription', subscription, '--name', GROUP, '--yes', '--no-wait']);
    }
    evidence.stage = 'wait'; save();
    const deadline = Date.now() + 45 * 60_000;
    let remaining = groupExists ? [GROUP, ...managedGroups] : [];
    while (remaining.length) {
      if (Date.now() >= deadline) throw new Error('Timed out waiting for captured resource groups.');
      remaining = remaining.filter(group => exists(subscription, group));
      evidence.remaining = remaining; save();
      if (remaining.length) await new Promise(resolve => setTimeout(resolve, 15_000));
    }
    evidence.exitCode = 0;
    evidence.stage = 'complete';
    evidence.finishedAt = new Date().toISOString();
    save();
  } catch (error) {
    evidence.exitCode = 1;
    evidence.failedAt = new Date().toISOString();
    evidence.error = error instanceof Error ? error.message : String(error);
    save();
    throw error;
  } finally {
    fs.closeSync(fd);
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
