import fs from 'node:fs';
import path from 'node:path';
import { randomInt } from 'node:crypto';
import { parseArgs } from 'node:util';
import { parseDocument, stringify } from 'yaml';
import { Config, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig, type StampConfig } from '../../infra/azure/stamp.js';
import type { AksNetworkMode, DeployAttempt, DeployOptions, DeployResult } from './aks-types.js';
import { setup } from './aks-setup.js';
import { renderStampConfig } from './aks-stamp.js';

type Options = DeployOptions;
type Network = Exclude<AksNetworkMode, 'auto'>;
type Json = Record<string, unknown>;
const GROUP = 'arrow-collector-rg';
const STATE = path.join(ROOT, 'out', 'arrowcol-oat');
const READ_SECONDS = 180;
const PROVISION_SECONDS = 3600;
const DELETE_SECONDS = 1800;
const destroyCommand = (subscription: string) =>
  `npx --no-install arrowcol-oat aks destroy --subscription ${subscription} --confirm ${GROUP}`;
const object = (value: unknown, label: string): Json => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${label} response.`);
  return value as Json;
};
const list = (value: unknown, label: string): Json[] => {
  if (!Array.isArray(value)) throw new Error(`Invalid ${label} response.`);
  return value.map(item => object(item, label));
};

function safeError(value: string): string {
  return value.replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, '[redacted-token]')
    .replace(/(bearer\s+)\S+/gi, '$1[redacted]')
    .replace(/([?&](?:sig|token|key|secret)=)[^&\s"']+/gi, '$1[redacted]')
    .replace(/("(?:accessToken|refreshToken|clientSecret|password|token)"\s*:\s*")[^"]*/gi, '$1[redacted]')
    .slice(0, 16000);
}

export function pickStamp(): string {
  return String(randomInt(10000, 100000));
}

function validateOptions(options: Options): void {
  if (options.stamp !== undefined && !/^(00000|[1-9]\d{4})$/.test(String(options.stamp)))
    throw new Error('--stamp must be exactly five digits: 00000 or 10000..99999.');
  if (options.network !== undefined && !['auto', 'azure', 'cilium'].includes(options.network))
    throw new Error('--network must be auto, azure, or cilium.');
  if (options.subscription !== undefined && !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(options.subscription))
    throw new Error('--subscription must be a UUID.');
}

function assertCleanState(subscription: string): void {
  const stale = ['.env.azure', '.env.azure.tables.json'].map(name => path.join(ROOT, name))
    .concat(['runtime.json', 'bootstrap.env', 'aks.env', 'oat-config.aks.json', 'secrets'].map(name => path.join(STATE, name)))
    .filter(filename => fs.existsSync(filename));
  if (stale.length) throw new Error(`Generated state already exists (${stale.map(filename => path.relative(ROOT, filename)).join(', ')}). ` +
    `Refusing to overwrite it. Run: ${destroyCommand(subscription)}`);
}

function privateWrite(filename: string, contents: string): void {
  fs.mkdirSync(path.dirname(filename), { recursive: true, mode: 0o700 });
  fs.writeFileSync(filename, contents, { mode: 0o600 });
  fs.chmodSync(filename, 0o600);
}

export async function deployInfrastructure(options: Options, runner: typeof run_tree = run_tree): Promise<DeployResult> {
  validateOptions(options);
  const configPath = path.resolve(ROOT, options.stampConfig ?? 'infra/azure/stamp.yaml');
  const config = loadStampConfig(configPath);
  if (options.subscription && options.subscription.toLowerCase() !== config.subscription)
    throw new Error('--subscription must match the private stamp configuration; no implicit subscription override is allowed.');
  const subscription = config.subscription;
  const ignored = await runner(['git', 'check-ignore', '--quiet', '--', configPath], 30, ROOT);
  if (ignored.returncode) throw new Error('--stamp-config must be a gitignored private file; refusing to rewrite tracked configuration.');
  const network: Network = options.network === 'cilium' ? 'cilium' : 'azure';
  const next = `npx --no-install arrowcol-oat aks deploy --stamp-config ${JSON.stringify(configPath)}`;
  const commands: Array<{ step: string; seconds: number; returncode: number }> = [];
  let bootstrapEnvironment: NodeJS.ProcessEnv | undefined;
  const exec = async (command: string[], timeout = READ_SECONDS) => {
    const started = performance.now();
    try {
      const result = await runner(command, timeout, ROOT, command[0] === 'npx' ? bootstrapEnvironment : undefined);
      commands.push({ step: command.slice(0, command[0] === 'npx' ? 5 : 4).join(' '), seconds: (performance.now() - started) / 1000, returncode: result.returncode });
      return result;
    } catch (error) {
      commands.push({ step: command.slice(0, command[0] === 'npx' ? 5 : 4).join(' '), seconds: (performance.now() - started) / 1000, returncode: -1 });
      throw new Error(`Command outcome is unknown; automatic retry is unsafe. ${safeError(String(error))}. Inspect Azure, then run: ${destroyCommand(subscription)}`);
    }
  };
  const az = async (...args: string[]): Promise<unknown> => {
    const result = await exec(['az', ...args, '--subscription', subscription, '--only-show-errors', '--output', 'json']);
    if (result.returncode) throw new Error(safeError(result.stderr || result.stdout || `az ${args.slice(0, 2).join(' ')} failed.`));
    try { return JSON.parse(result.stdout); }
    catch { throw new Error(`Azure returned invalid JSON for ${args.slice(0, 3).join(' ')}.`); }
  };
  const account = await exec(['az', 'account', 'show', '--subscription', subscription, '--only-show-errors', '--output', 'json'], 60);
  if (account.returncode) throw new Error('Azure CLI is not signed in to the configured subscription. Run: az login --use-device-code');
  const accountId = object(JSON.parse(account.stdout), 'Azure account').id;
  if (typeof accountId !== 'string' || accountId.toLowerCase() !== subscription)
    throw new Error('Azure CLI account does not match the configured subscription. Run: az login --use-device-code');
  const exists = await az('group', 'exists', '--name', GROUP);
  if (typeof exists !== 'boolean') throw new Error('Azure did not report resource-group existence.');
  if (exists) {
    const clusters = list(await az('aks', 'list', '--resource-group', GROUP), 'AKS inventory');
    if (clusters.length) throw new Error(`A live stamp already exists (${clusters.map(cluster => cluster.name).join(', ')}). Run: ${destroyCommand(subscription)}`);
    const resources = list(await az('resource', 'list', '--resource-group', GROUP), 'resource inventory');
    if (resources.length) throw new Error(`Partial stamp resources already exist; refusing to reuse live names. Run: ${destroyCommand(subscription)}`);
  }
  if (!options.whatIf) assertCleanState(subscription);

  let stamp = String(options.stamp ?? pickStamp());
  let available = false;
  for (let candidate = 0; candidate < 10; candidate++) {
    const storage = object(await az('storage', 'account', 'check-name', '--name', `arrowcollector${stamp}`), 'storage name availability');
    const readLogs = config.storageReadLogDestination === 'storage'
      ? object(await az('storage', 'account', 'check-name', '--name', `arrowreadlogs${stamp}`), 'read-log storage name availability')
      : undefined;
    const registry = object(await az('acr', 'check-name', '--name', `arrowcollector${stamp}`), 'ACR name availability');
    const kusto = object(await az('rest', '--method', 'post', '--url',
      `https://management.azure.com/subscriptions/${subscription}/providers/Microsoft.Kusto/locations/${config.location}/checkNameAvailability?api-version=2024-04-13`,
      '--body', JSON.stringify({ name: `arrow-collector-${stamp}`, type: 'Microsoft.Kusto/clusters' })), 'Kusto name availability');
    const names = [storage, registry, kusto, ...(readLogs ? [readLogs] : [])];
    if (names.every(response => response.nameAvailable === true)) {
      available = true;
      break;
    }
    for (const response of names)
      if (typeof response.nameAvailable !== 'boolean') throw new Error('Azure did not report explicit name availability.');
    if (options.stamp !== undefined) throw new Error(`Stamp ${stamp} has a name collision; no resources were created. Choose another --stamp.`);
    console.log(`Stamp ${stamp} unavailable; choosing another five-digit stamp.`);
    stamp = pickStamp();
  }
  if (!available) throw new Error('Could not find an available stamp after 10 candidates; no resources were created.');

  const original = fs.readFileSync(configPath, 'utf8');
  const persisted = renderStampConfig(original, { stamp });
  const selected: StampConfig = { ...config, stamp, aks: { ...config.aks, networkDataplane: network } };
  bootstrapEnvironment = {
    ...Object.fromEntries(Object.entries(process.env).filter(([key]) =>
      !/^(AZURE_|ARROWCOL_)/.test(key) || ['AZURE_CONFIG_DIR', 'AZURE_EXTENSION_DIR'].includes(key))),
    ...new Config({ AZURE_SUBSCRIPTION_ID: subscription }, selected).generated(),
    AZURE_SUBSCRIPTION_ID: subscription, AZURE_UAMI_CLIENT_ID: '', ARROWCOL_RESET_GENERATION: '',
  };
  fs.mkdirSync(STATE, { recursive: true, mode: 0o700 });
  const work = fs.mkdtempSync(path.join(STATE, '.deploy-'));
  fs.chmodSync(work, 0o700);
  const candidatePath = path.join(work, 'stamp.yaml');
  privateWrite(candidatePath, renderStampConfig(original, { stamp, network }));
  const bootstrap = (command: string, filename: string) =>
    ['npx', '--no-install', 'tsx', 'infra/azure/bootstrap.ts', command, '--stamp-config', filename];
  const receiptPath = path.join(STATE, 'receipts', `deploy-${stamp}.yaml`);
  const attempts: DeployAttempt[] = [];
  const postconditions: Record<string, boolean> = {};
  const receipt: Json = { stamp, subscription, resourceGroup: GROUP, whatIf: options.whatIf ?? false,
    startedAt: new Date().toISOString(), networkPath: [], attempts, steps: commands, postconditions };
  const save = () => privateWrite(receiptPath, stringify(receipt));
  let current = network;
  try {
    console.log(`Selected stamp ${stamp}; network ${network}${options.network === undefined || options.network === 'auto' ? ' (auto: one Cilium fallback only for proven AKS creation failure)' : ' (pinned)'}.`);
    const preflight = await exec([...bootstrap('preflight', candidatePath), '--read-only'], 900);
    if (preflight.returncode) throw new Error(`Read-only quota/network preflight failed: ${safeError(preflight.stderr || preflight.stdout)}`);
    if (options.whatIf) {
      const preview = await exec(bootstrap('what-if', candidatePath), 900);
      if (preview.returncode) throw new Error(`ARM what-if failed: ${safeError(preview.stderr || preview.stdout)}`);
      console.log(preview.stdout);
      receipt.networkPath = [network];
      receipt.completedAt = new Date().toISOString();
      save();
      console.log(`Read-only ARM what-if complete; private stamp YAML unchanged. Receipt: ${receiptPath}`);
      return { stamp, network, attempts, receiptPath };
    }
    privateWrite(configPath, persisted);
    for (let pass = 1; pass <= 2; pass++) {
      console.log(`Provisioning pass ${pass}/2: stamp ${stamp}, network ${current} (timeout ${PROVISION_SECONDS}s).`);
      receipt.networkPath = [...attempts.map(attempt => attempt.network), current];
      save();
      let provision: Awaited<ReturnType<typeof runner>>;
      try {
        provision = await exec([...bootstrap('provision', candidatePath), '--wait-seconds', '600'], PROVISION_SECONDS);
      } catch (error) {
        attempts.push({ network: current, outcome: 'other-failure', error: safeError(String(error)) });
        throw error;
      }
      if (!provision.returncode) {
        attempts.push({ network: current, outcome: 'succeeded' });
        break;
      }
      const error = safeError(provision.stderr || provision.stdout || 'Provisioning failed without diagnostics.');
      console.error(`Provisioning pass ${pass} (${current}) failed: ${error}`);
      const attempt: DeployAttempt = { network: current, outcome: 'other-failure', error };
      attempts.push(attempt);
      save();
      let clusterFailure: boolean;
      try { clusterFailure = await failedClusterOnly(selected, az); }
      catch (diagnosticError) {
        throw new Error(`${error}\nCould not prove an AKS-only creation failure: ${safeError(String(diagnosticError))}`);
      }
      if (clusterFailure) attempt.outcome = 'cluster-create-failed';
      save();
      if (pass === 1 && clusterFailure && current === 'azure' && (!options.network || options.network === 'auto')) {
        console.log('Confirmed AKS-only creation failure; deleting only that failed cluster, then retrying once with Cilium.');
        const deletion = await exec(['az', 'aks', 'delete', '--subscription', subscription, '--resource-group', GROUP,
          '--name', `arrow-collector-${stamp}`, '--yes', '--only-show-errors'], DELETE_SECONDS);
        if (deletion.returncode && !/ResourceNotFound|ResourceGroupNotFound/.test(deletion.stderr))
          throw new Error(`Failed cluster deletion did not complete: ${safeError(deletion.stderr)}`);
        const remaining = list(await az('aks', 'list', '--resource-group', GROUP), 'AKS inventory');
        if (remaining.some(cluster => cluster.name === `arrow-collector-${stamp}`))
          throw new Error('Failed cluster is still present; refusing a dataplane change.');
        current = 'cilium';
        privateWrite(candidatePath, renderStampConfig(original, { stamp, network: current }));
        continue;
      }
      if (pass === 1 && provision.returncode === 3 && !clusterFailure) {
        console.log('Delta logs have not converged; allowing the final provision pass on the same network.');
        continue;
      }
      throw new Error(`Provisioning failed: ${attempts.map(attempt => `${attempt.network}: ${attempt.error}`).join('\n')}. ` +
        `No further network retry is permitted. Run: ${destroyCommand(subscription)}`);
    }
    if (attempts.at(-1)?.outcome !== 'succeeded') throw new Error('Provisioning did not converge within two passes.');
    await verifyPostconditions({ ...selected, aks: { ...selected.aks, networkDataplane: current } }, az, postconditions);
    receipt.completedAt = new Date().toISOString();
    save();
    console.log(`Identity receipt: exactly one UAMI arrow-collector; native and publisher federation plus run-FIC permission verified.`);
    console.log(`Deployed stamp ${stamp}; network path: ${attempts.map(attempt => attempt.network).join(' -> ')}. Receipt: ${receiptPath}`);
    if (current === 'azure') console.warn('WARNING: Azure dataplane has no enforcing network-policy provider. In-cluster workloads can reach the passwordless dataflow port; there is no public ingress.');
    return { stamp, network: current, attempts, receiptPath };
  } catch (error) {
    receipt.failedAt = new Date().toISOString();
    receipt.error = safeError(error instanceof Error ? error.message : String(error));
    save();
    console.error(`Deploy receipt: ${receiptPath}`);
    throw new Error(`${receipt.error}\nRe-run only after resolving the cause: ${next}\nOr destroy partial resources: ${destroyCommand(subscription)}`);
  } finally {
    fs.rmSync(work, { recursive: true, force: true });
  }
}

async function failedClusterOnly(config: StampConfig, az: (...args: string[]) => Promise<unknown>): Promise<boolean> {
  const parentName = `arrowcol-sandbox-stamp-${config.stamp}`;
  const parent = object(await az('deployment', 'sub', 'show', '--name', parentName), 'subscription deployment');
  if (parent.name !== parentName ||
      object(parent.properties, 'subscription deployment properties').provisioningState !== 'Failed') return false;
  const deployments = list(await az('deployment', 'group', 'list', '--resource-group', GROUP), 'group deployments');
  const names = [`arrowcol-sandbox-stamp-${config.stamp}`, `arrowcol-aks-stamp-${config.stamp}`];
  if (names.some(name => !deployments.some(item => item.name === name &&
    object(item.properties, 'deployment properties').provisioningState === 'Failed'))) return false;
  let clusterFailed = false;
  for (const name of names) {
    const operations = list(await az('deployment', 'operation', 'group', 'list', '--resource-group', GROUP, '--name', name), 'deployment operations');
    for (const operation of operations) {
      const properties = object(operation.properties, 'operation properties');
      if (properties.provisioningState !== 'Failed') continue;
      const target = object(properties.targetResource, 'failed operation target');
      if (target.resourceType === 'Microsoft.Resources/deployments') continue;
      if (target.resourceType !== 'Microsoft.ContainerService/managedClusters' || target.resourceName !== `arrow-collector-${config.stamp}`)
        return false;
      clusterFailed = true;
    }
  }
  const clusters = list(await az('aks', 'list', '--resource-group', GROUP), 'AKS inventory');
  const cluster = clusters.find(item => item.name === `arrow-collector-${config.stamp}`);
  return clusterFailed && (!cluster || (cluster.provisioningState === 'Failed' &&
    object(cluster.tags, 'AKS ownership tags').sandboxId === `stamp-${config.stamp}` &&
    object(cluster.tags, 'AKS ownership tags').project === 'rust-playground' &&
    object(cluster.tags, 'AKS ownership tags').purpose === 'disposable-arrowcol-sandbox'));
}

async function verifyPostconditions(config: StampConfig, az: (...args: string[]) => Promise<unknown>, receipt: Record<string, boolean>): Promise<void> {
  const name = `arrow-collector-${config.stamp}`;
  const account = `arrowcollector${config.stamp}`;
  const clusters = list(await az('aks', 'list', '--resource-group', GROUP), 'AKS inventory');
  const cluster = clusters.find(item => item.name === name);
  receipt.aks = clusters.length === 1 && cluster?.provisioningState === 'Succeeded' &&
    object(cluster.networkProfile, 'AKS network profile').networkDataplane === config.aks.networkDataplane;
  const resources = list(await az('resource', 'list', '--resource-group', GROUP), 'resource inventory');
  const identities = resources.filter(item => String(item.type).toLowerCase() === 'microsoft.managedidentity/userassignedidentities');
  receipt.identity = identities.length === 1 && identities[0]?.name === 'arrow-collector';
  const identity = object(await az('identity', 'show', '--resource-group', GROUP, '--name', 'arrow-collector'), 'shared identity');
  if (typeof identity.id !== 'string' || typeof identity.principalId !== 'string')
    throw new Error('Shared identity resource ID and principal ID are missing.');
  const assignments = list(await az('role', 'assignment', 'list', '--scope', identity.id, '--include-inherited'), 'identity role assignments');
  receipt.runFederationPermission = assignments.some(assignment =>
    assignment.principalId === identity.principalId &&
    typeof assignment.roleDefinitionId === 'string' &&
    assignment.roleDefinitionId.toLowerCase().endsWith('/7e559ce2-48d7-4b27-9128-fa1b247f1308'));
  for (const [key, type, resourceName] of [
    ['storage', 'Microsoft.Storage/storageAccounts', account], ['acr', 'Microsoft.ContainerRegistry/registries', account],
    ['kusto', 'Microsoft.Kusto/clusters', name],
  ]) receipt[key!] = resources.some(item => String(item.type).toLowerCase() === type!.toLowerCase() && item.name === resourceName);
  const databaseResponse = object(await az('rest', '--method', 'get', '--url',
    `https://management.azure.com/subscriptions/${config.subscription}/resourceGroups/${GROUP}/providers/Microsoft.Kusto/clusters/${name}/databases?api-version=2024-04-13`), 'Kusto database inventory');
  const databases = list(databaseResponse.value, 'Kusto databases');
  receipt.databases = ['arrowcol', 'experimentqap'].every(database => databases.some(item => item.name === database || item.name === `${name}/${database}`));
  const credentials = list(await az('identity', 'federated-credential', 'list', '--resource-group', GROUP,
    '--identity-name', 'arrow-collector'), 'federated credentials');
  const properties = (entry: Json) => entry.properties ? object(entry.properties, 'credential properties') : entry;
  const validAudience = (entry: Json) => {
    const audiences = properties(entry).audiences;
    return Array.isArray(audiences) && audiences.length === 1 && audiences[0] === 'api://AzureADTokenExchange';
  };
  receipt.nativeFederation = credentials.some(item => item.name === `azure-qap-${name}` &&
    properties(item).subject === `system:serviceaccount:${config.aks.namespace}:${config.aks.serviceAccount}` &&
    properties(item).issuer === object(cluster?.oidcIssuerProfile, 'AKS OIDC profile').issuerUrl && validAudience(item));
  receipt.publisherFederation = config.publisherFederatedCredentials.every(expected => credentials.some(item => item.name === expected.name &&
      properties(item).issuer === expected.issuer && properties(item).subject === expected.subject && validAudience(item)));
  const failed = Object.entries(receipt).filter(([, passed]) => !passed).map(([key]) => key);
  if (failed.length) throw new Error(`Deployment postconditions failed: ${failed.join(', ')}.`);
}

export async function deploy(options: Options, runner: typeof run_tree = run_tree): Promise<DeployResult> {
  const result = await deployInfrastructure(options, runner);
  if (!options.whatIf) {
    try {
      result.setup = await setup({ stamp: result.stamp, stampConfig: options.stampConfig }, runner);
      if (!result.setup.preflightReady) throw new Error('Setup did not reach preflight READY.');
    } catch (error) {
      const document = parseDocument(fs.readFileSync(result.receiptPath, 'utf8'));
      document.set('setup', { outcome: 'failed', error: safeError(String(error)) });
      privateWrite(result.receiptPath, document.toString());
      throw new Error(`Infrastructure is deployed, but setup failed: ${safeError(String(error))}. ` +
        `Run: npx --no-install arrowcol-oat aks setup --stamp ${result.stamp}` +
        (options.stampConfig ? ` --stamp-config ${JSON.stringify(options.stampConfig)}` : ''));
    }
    const document = parseDocument(fs.readFileSync(result.receiptPath, 'utf8'));
    document.set('setup', { outcome: 'succeeded', preflightReady: result.setup.preflightReady });
    privateWrite(result.receiptPath, document.toString());
    console.log('Next: npx --no-install arrowcol-oat --mode aks');
  }
  return result;
}

export async function main(args: string[], runner: typeof run_tree = run_tree): Promise<void> {
  const { values } = parseArgs({ args, options: {
    stamp: { type: 'string' }, network: { type: 'string', default: 'auto' }, 'what-if': { type: 'boolean' },
    'stamp-config': { type: 'string' }, subscription: { type: 'string' },
  } });
  const network = values.network;
  if (network !== 'auto' && network !== 'azure' && network !== 'cilium') throw new Error('--network must be auto, azure, or cilium.');
  await deploy({ stamp: values.stamp, network, whatIf: values['what-if'], stampConfig: values['stamp-config'],
    subscription: values.subscription }, runner);
}
