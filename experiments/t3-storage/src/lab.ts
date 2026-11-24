import { createHash, randomUUID } from 'node:crypto';
import { readFile, mkdir, writeFile, realpath, chmod } from 'node:fs/promises';
import path from 'node:path';
import { runStreamingTree } from '../../../tools/scripts/src/process.js';

export interface Config {
  subscription: string;
  resourceGroup: string;
  location: string;
  stagingLocation: string;
  prefix: string;
  owner: string;
  vmSize: string;
  sshPublicKeyPath: string;
  uploaderObjectId: string;
  uploaderIp: string;
  resourceTags: Record<string, string>;
  outboundIpTags: { ipTagType: string; tag: string }[];
  azureCli: { program: string; prefix: string[] };
  buildContainer: string;
  outDir: string;
}

export interface Inventory {
  resourceGroup: string;
  vmNames: string[];
  storageAccounts: string[];
  storageEndpoints: { blob?: string; file?: string }[];
  nfsSource: string;
  shareQuotaGiB: number;
  vmSize: string;
  imageVersion: string;
}

const configKeys: (keyof Config)[] = ['subscription', 'resourceGroup', 'location', 'stagingLocation', 'prefix', 'owner', 'vmSize', 'sshPublicKeyPath', 'uploaderObjectId', 'uploaderIp', 'resourceTags', 'outboundIpTags', 'azureCli', 'buildContainer', 'outDir'];

export function validateConfig(input: unknown): Config {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('Config must be an object.');
  const c = input as Config;
  if (Object.keys(c).some(k => !configKeys.includes(k as keyof Config)) || configKeys.some(k => c[k] === undefined)) throw new Error('Missing or unknown configuration field.');
  if (!/^[a-z0-9]{2,12}$/u.test(c.prefix) || !/^[a-z0-9-]+$/u.test(c.owner)) throw new Error('Invalid resource prefix or owner.');
  if (!/^rg-[a-z0-9-]+$/u.test(c.resourceGroup) || !c.resourceGroup.endsWith(`-${c.owner}`)) throw new Error('A dedicated resource group with the owner suffix is required.');
  if (!/^[0-9a-f-]{36}$/u.test(c.subscription) || !/^[0-9a-f-]{36}$/u.test(c.uploaderObjectId)) throw new Error('Invalid subscription or uploader identity.');
  if (!/^[a-z0-9]+$/u.test(c.location) || !/^[a-z0-9]+$/u.test(c.stagingLocation) || !/^Standard_[A-Za-z0-9_]+$/u.test(c.vmSize)) throw new Error('Invalid region or VM size.');
  if (!/^(?:\d{1,3}\.){3}\d{1,3}$/u.test(c.uploaderIp) || c.uploaderIp.split('.').some(p => Number(p) > 255)) throw new Error('A single uploader IPv4 address is required.');
  if (!c.azureCli.program || !Array.isArray(c.azureCli.prefix) || c.azureCli.prefix.some(p => typeof p !== 'string')) throw new Error('Invalid Azure CLI invocation.');
  if (!c.outDir || !c.buildContainer || !c.sshPublicKeyPath) throw new Error('Output, container and public key are required.');
  if (!c.resourceTags || typeof c.resourceTags !== 'object' || Object.values(c.resourceTags).some(v => typeof v !== 'string') || !Array.isArray(c.outboundIpTags)) throw new Error('Invalid resource tags.');
  return c;
}

export async function execute(program: string, args: string[], timeoutMs = 120_000): Promise<string> {
  return runStreamingTree(program, args, { timeoutMs, capture: true, failureDetail: stderr => stderr });
}

export function shellQuote(value: string): string {
  if (value.includes('\0') || /[\r\n]/u.test(value)) throw new Error('Invalid shell argument.');
  return `'${value.replaceAll("'", "'\\''")}'`;
}

export async function privateDirectory(directory: string): Promise<string> {
  const root = await realpath(process.cwd());
  const resolved = path.resolve(directory);
  if (!resolved.startsWith(`${root}${path.sep}`)) throw new Error('Private output must be inside this checkout.');
  await execute('git', ['check-ignore', '-q', '--', path.relative(root, resolved) + '/evidence.json']);
  await mkdir(resolved, { recursive: true, mode: 0o700 });
  if (!(await realpath(resolved)).startsWith(`${root}${path.sep}`)) throw new Error('Private output symlink escapes the checkout.');
  await chmod(resolved, 0o700);
  return resolved;
}

export class Lab {
  constructor(readonly config: Config) {}

  async az(args: string[], timeoutMs = 120_000): Promise<unknown> {
    const text = await execute(this.config.azureCli.program, [...this.config.azureCli.prefix, ...args, '--subscription', this.config.subscription, '--only-show-errors', '--output', 'json'], timeoutMs);
    return text.trim() ? JSON.parse(text) : null;
  }

  async save(name: string, value: unknown): Promise<void> {
    if (!/^[a-zA-Z0-9_.-]+$/u.test(name)) throw new Error('Invalid evidence filename.');
    const dir = await privateDirectory(this.config.outDir);
    await writeFile(path.join(dir, name), JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
  }

  async cliPath(file: string): Promise<string> {
    return this.config.azureCli.program.endsWith('.exe') ? (await execute('wslpath', ['-w', path.resolve(file)])).trim() : path.resolve(file);
  }

  async inventory(): Promise<Inventory> {
    const inventory = JSON.parse(await readFile(path.join(this.config.outDir, 'inventory.json'), 'utf8')) as Inventory;
    if (inventory.resourceGroup !== this.config.resourceGroup || inventory.vmNames.length !== 2 || inventory.vmNames.some(name => !name.startsWith(`${this.config.prefix}-vm-`))) throw new Error('Inventory does not belong to this lab.');
    return inventory;
  }

  async assertOwned(): Promise<void> {
    const group = await this.az(['group', 'show', '--name', this.config.resourceGroup]) as { tags?: Record<string, string> };
    if (group.tags?.purpose !== 'storage-spike' || group.tags?.owner !== this.config.owner) throw new Error('Resource group ownership does not match this experiment.');
  }

  async deploy(): Promise<Inventory> {
    const directory = await privateDirectory(this.config.outDir);
    const exists = await this.az(['group', 'exists', '--name', this.config.resourceGroup]);
    if (exists) await this.assertOwned();
    const previousInventory = exists ? await this.inventory().catch(() => null) : null;
    const image = previousInventory ? { name: previousInventory.imageVersion } : await this.az(['vm', 'image', 'show', '--location', this.config.location, '--urn', 'Canonical:ubuntu-24_04-lts:server:latest']) as { name: string };
    if (!/^\d+\.\d+\.\d+$/u.test(image.name)) throw new Error('Image did not resolve to an exact version.');
    const publicKey = (await readFile(this.config.sshPublicKeyPath, 'utf8')).trim();
    if (!publicKey.startsWith('ssh-rsa ') && !publicKey.startsWith('ssh-ed25519 ')) throw new Error('Invalid SSH public key.');
    const values = {
      resourceGroupName: this.config.resourceGroup, location: this.config.location, stagingLocation: this.config.stagingLocation,
      prefix: this.config.prefix, sshPublicKey: publicKey,
      uploaderObjectId: this.config.uploaderObjectId, uploaderIp: this.config.uploaderIp,
      resourceTags: { ...this.config.resourceTags, owner: this.config.owner },
      outboundIpTags: this.config.outboundIpTags, vmSize: this.config.vmSize,
      imageVersion: image.name, shareQuotaGiB: 8192,
    };
    const parameters = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, { value }]));
    const parameterFile = path.join(directory, 'parameters.json');
    await this.save('parameters.json', { '$schema': 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#', contentVersion: '1.0.0.0', parameters });
    const templateFile = path.join(directory, 'template.json');
    await execute('docker', ['exec', this.config.buildContainer, 'az', 'bicep', 'build', '--file', 'experiments/t3-storage/infra/main.bicep', '--outfile', path.posix.join('/workspaces/rust-playground', path.relative(process.cwd(), templateFile))]);
    const name = `storage-spike-${createHash('sha256').update(this.config.resourceGroup).digest('hex').slice(0, 12)}`;
    await this.save('deployment-receipt.json', { name, location: this.config.location, submittedAt: new Date().toISOString(), status: 'submission-pending' });
    const args = ['deployment', 'sub', 'create', '--name', name, '--location', this.config.location, '--template-file', await this.cliPath(templateFile), '--parameters', `@${await this.cliPath(parameterFile)}`];
    try {
      const deployment = await this.az(args, 30 * 60_000) as { properties: { outputs: { inventory: { value: Inventory } } } };
      await this.save('deployment.json', deployment);
      const inventory = deployment.properties.outputs.inventory.value;
      await this.save('inventory.json', inventory);
      return inventory;
    } catch (error) {
      const state = await this.az(['deployment', 'sub', 'show', '--name', name]).catch(() => null);
      await this.save('deployment-reconcile.json', { state, error: String(error) });
      throw new Error('Deployment did not complete successfully. Reconcile the saved deployment state before retrying.', { cause: error });
    }
  }

  async remote(vm: string, script: string, label: string): Promise<string> {
    const inventory = await this.inventory();
    if (!inventory.vmNames.includes(vm)) throw new Error('VM is outside the saved lab inventory.');
    await this.assertOwned();
    if (Buffer.byteLength(script) > 3_000_000) throw new Error('Run Command script is too large.');
    let source = `set -eu\n${script}`;
    if (Buffer.byteLength(source) > 12_000) {
      const directory = await privateDirectory(this.config.outDir);
      const file = path.join(directory, `command-${randomUUID()}.txt`);
      await writeFile(file, source, { mode: 0o600 });
      source = `@${await this.cliPath(file)}`;
    }
    const result = await this.az(['vm', 'run-command', 'invoke', '--resource-group', this.config.resourceGroup, '--name', vm, '--command-id', 'RunShellScript', '--scripts', source], 20 * 60_000) as { value: { code: string; message: string }[] };
    await this.save(`${label}-${randomUUID()}.json`, result);
    const messages = result.value.map(v => v.message).join('\n');
    if (!messages.split(/\r?\n/u).includes('STORAGE_SPIKE_OK')) throw new Error(`Remote operation ${label} did not return a success marker. See private evidence.`);
    return messages;
  }

  async stop(): Promise<void> {
    await this.assertOwned();
    const inventory = await this.inventory();
    const states = await Promise.all(inventory.vmNames.map(async name => {
      await this.az(['vm', 'deallocate', '--resource-group', this.config.resourceGroup, '--name', name], 15 * 60_000);
      return this.az(['vm', 'get-instance-view', '--resource-group', this.config.resourceGroup, '--name', name, '--query', '{name:name,statuses:instanceView.statuses[].code}']);
    }));
    await this.save('stopped-compute.json', states);
    if (states.some(s => !(s as { statuses: string[] }).statuses.includes('PowerState/deallocated'))) throw new Error('Compute deallocation was not verified.');
  }
}
