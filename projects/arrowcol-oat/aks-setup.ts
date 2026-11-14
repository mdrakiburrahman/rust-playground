import { randomUUID } from 'node:crypto';
import { chmod, lstat, mkdir, readFile, rm } from 'node:fs/promises';
import { relative, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { parseDocument } from 'yaml';
import { Config, load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { PublisherControl, READY } from '../../infra/azure/publisher_control.js';
import type { Metadata } from '../../infra/azure/authority.js';
import type { LiveStamp, SetupOptions, SetupResult } from './aks-types.js';
import { renderStampConfig } from './aks-stamp.js';
import { generatePrivateInputs, renderBootstrapEnv, validateRuntime, writePrivateFile, type RuntimeConfig } from './runtime.js';

const setupCommand = 'npx --no-install arrowcol-oat aks setup';
const destroyCommand = 'npx --no-install arrowcol-oat aks destroy --confirm arrow-collector-rg';
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

async function checked(runner: typeof run_tree, command: string[], timeout: number, label: string,
  environment?: NodeJS.ProcessEnv): Promise<string> {
  const result = await runner(command, timeout, ROOT, environment);
  if (result.returncode) {
    const detail = result.stderr.trim().replace(/[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, '[redacted]').slice(0, 1000);
    throw new Error(`${label} failed (${result.returncode})${detail ? `: ${detail}` : ''}. Run \`${setupCommand}\` after correcting the failure.`);
  }
  return result.stdout.trim();
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stampNumber(value: string): string {
  if (!/^(?:00000|[1-9]\d{4})$/.test(value)) throw new Error('--stamp must be 00000 or an integer in 10000..99999.');
  return value;
}

export async function discoverLiveStamp(options: SetupOptions = {}, runner: typeof run_tree = run_tree): Promise<LiveStamp> {
  const config = loadStampConfig(resolve(ROOT, options.stampConfig ?? 'infra/azure/stamp.yaml'));
  const result = await runner(['az', 'aks', 'list', '--subscription', config.subscription,
    '--resource-group', config.resourceGroup, '--output', 'json', '--only-show-errors'], 120, ROOT);
  if (result.returncode) {
    if (/ResourceGroupNotFound/.test(result.stderr)) throw new Error('No live stamp; run `npx --no-install arrowcol-oat aks deploy`.');
    throw new Error('Could not discover the live AKS stamp; run `az login --use-device-code`, then retry `npx --no-install arrowcol-oat aks setup`.');
  }
  let value: unknown;
  try { value = JSON.parse(result.stdout); }
  catch { throw new Error(`Azure returned invalid AKS inventory JSON; retry \`${setupCommand}\`.`); }
  if (!Array.isArray(value)) throw new Error(`Azure returned an invalid AKS inventory; retry \`${setupCommand}\`.`);
  const clusters = value.filter((item): item is { name: string } =>
    item !== null && typeof item === 'object' && typeof item.name === 'string' && item.name.startsWith('arrow-collector-'));
  if (!clusters.length) throw new Error('No live stamp; run `npx --no-install arrowcol-oat aks deploy`.');
  if (clusters.length !== 1) throw new Error(`Several live AKS stamps found: ${clusters.map(c => c.name).sort().join(', ')}. Select one live stamp before setup; inventory with \`npx --no-install arrowcol-oat aks destroy\`.`);
  const clusterName = clusters[0]!.name;
  const stamp = stampNumber(clusterName.slice('arrow-collector-'.length));
  if (options.stamp !== undefined && stampNumber(String(options.stamp)) !== stamp) {
    throw new Error(`Requested stamp ${options.stamp} disagrees with live cluster ${clusterName}; run \`${setupCommand} --stamp ${stamp}\`.`);
  }
  return { stamp, subscription: config.subscription, resourceGroup: config.resourceGroup, clusterName };
}

async function requireCurrentState(stamp: LiveStamp, directory: string): Promise<void> {
  for (const filename of ['runtime.json', 'oat-config.aks.json']) {
    try {
      const state = JSON.parse(await readFile(resolve(directory, filename), 'utf8')) as { stamp?: unknown; subscriptionId?: unknown };
      if (state.stamp !== stamp.stamp || (filename === 'runtime.json' &&
          (typeof state.subscriptionId !== 'string' || state.subscriptionId.toLowerCase() !== stamp.subscription.toLowerCase()))) {
        throw new Error(`Stale AKS private inputs belong to another stamp; run \`${destroyCommand}\` before deploying.`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
}

async function capturedGeneration(config: Config, runner: typeof run_tree): Promise<string> {
  const authority = new PublisherControl({
    c: { sandbox: config.sandbox },
    async head_control(): Promise<[Metadata, string]> {
      const value = JSON.parse(await checked(runner, ['az', 'storage', 'container', 'show',
        '--account-name', config.account, '--name', config.checkpoints, '--auth-mode', 'login',
        '--subscription', config.subscription, '--output', 'json', '--only-show-errors'], 120, 'Read publisher authority')) as {
          metadata?: Metadata; properties?: { etag?: string };
        };
      if (!value.metadata || !value.properties?.etag) throw new Error('Publisher authority metadata or ETag is missing.');
      return [value.metadata, value.properties.etag];
    },
    async set_control(): Promise<void> { throw new Error('Setup must never mutate publisher authority.'); },
  });
  const [metadata] = await authority.read();
  if (metadata.resetstate !== READY) {
    throw new Error('Publisher authority must be ready-v1 before setup; finish provisioning first.');
  }
  return metadata.resetgeneration;
}

export async function setup(
  options: SetupOptions = {}, runner: typeof run_tree = run_tree, directory = resolve(ROOT, 'out/arrowcol-oat'),
): Promise<SetupResult> {
  if (options.tag !== undefined && !/^[a-z0-9][a-z0-9._-]{0,127}$/.test(options.tag)) {
    throw new Error('--tag must be a valid lowercase container tag.');
  }
  const live = await discoverLiveStamp(options, runner);
  await requireCurrentState(live, directory);
  const stampConfigPath = resolve(ROOT, options.stampConfig ?? 'infra/azure/stamp.yaml');
  await checked(runner, ['git', 'check-ignore', '--quiet', '--', stampConfigPath], 15, 'Private stamp configuration ignore check');
  const original = await readFile(stampConfigPath, 'utf8');
  const selected = renderStampConfig(original, { stamp: live.stamp });
  if (selected !== original) {
    if (!(await lstat(stampConfigPath)).isFile()) throw new Error('Stamp YAML must be a regular file.');
    await chmod(stampConfigPath, 0o600);
    await writePrivateFile(stampConfigPath, selected);
  }
  const stampConfig = loadStampConfig(stampConfigPath);
  if (stampConfig.aks.namespace !== 'arrow-collector' || stampConfig.aks.serviceAccount !== 'arrow-collector') {
    throw new Error('The host publisher namespace and ServiceAccount must both be arrow-collector.');
  }
  const privatePaths = ['runtime.json', 'bootstrap.env', 'aks.env', 'oat-config.aks.json',
    'secrets/publisher-assertion.jwt'].map(filename => relative(ROOT, resolve(directory, filename)));
  const ignored = await checked(runner, ['git', 'check-ignore', ...privatePaths], 15, 'Private-state ignore check');
  if (ignored.split(/\r?\n/).sort().join('\n') !== [...privatePaths].sort().join('\n')) {
    throw new Error('All AKS private inputs must be ignored by git.');
  }
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const info = await lstat(directory);
  if (!info.isDirectory()) throw new Error('Private input directory must be a real directory.');
  await chmod(directory, 0o700);
  await checked(runner, ['az', 'aks', 'get-credentials', '--subscription', live.subscription,
    '--resource-group', live.resourceGroup, '--name', live.clusterName, '--context', live.clusterName,
    '--overwrite-existing', '--only-show-errors'], 180, 'Fetch AKS credentials');
  const kubeloginHelp = await checked(runner, ['kubelogin', 'get-token', '--help'], 30, 'Verify kubelogin environment override support');
  if (!kubeloginHelp.includes('--disable-environment-override')) throw new Error('Installed kubelogin does not support --disable-environment-override.');
  await checked(runner, ['kubelogin', 'convert-kubeconfig', '--context', live.clusterName, '-l', 'azurecli',
    '--subscription', live.subscription, '--disable-environment-override'], 60, 'Configure operator kube authentication');
  const kubeconfigText = await checked(runner, ['kubectl', 'config', 'view', '--minify', '--context', live.clusterName,
    '-o', 'json'], 30, 'Read operator kube authentication configuration');
  let kubeconfig: unknown;
  try { kubeconfig = JSON.parse(kubeconfigText); }
  catch { throw new Error('AKS returned invalid kubeconfig JSON after credential conversion.'); }
  if (!isRecord(kubeconfig) || !Array.isArray(kubeconfig.contexts) || !Array.isArray(kubeconfig.users)) {
    throw new Error('AKS returned an invalid kubeconfig structure after credential conversion.');
  }
  const activeContext = kubeconfig.contexts[0];
  const context = isRecord(activeContext) && isRecord(activeContext.context) ? activeContext.context : undefined;
  const kubeUser = context?.user;
  if (typeof kubeUser !== 'string' || !kubeUser) throw new Error('AKS kubeconfig has no active user after credential conversion.');
  const user = kubeconfig.users.find(entry => isRecord(entry) && entry.name === kubeUser);
  const userConfig = isRecord(user) && isRecord(user.user) ? user.user : undefined;
  const exec = userConfig && isRecord(userConfig.exec) ? userConfig.exec : undefined;
  const rawExecArgs = exec?.args;
  if (!exec || exec.command !== 'kubelogin' || !Array.isArray(rawExecArgs) ||
      !rawExecArgs.every((arg): arg is string => typeof arg === 'string')) {
    throw new Error('AKS kubeconfig has no valid kubelogin exec configuration after credential conversion.');
  }
  if (exec.apiVersion !== undefined && typeof exec.apiVersion !== 'string') {
    throw new Error('AKS kubeconfig has an invalid kubelogin API version.');
  }
  if (exec.interactiveMode !== undefined && typeof exec.interactiveMode !== 'string') {
    throw new Error('AKS kubeconfig has an invalid kubelogin interactive mode.');
  }
  if (exec.provideClusterInfo !== undefined && typeof exec.provideClusterInfo !== 'boolean') {
    throw new Error('AKS kubeconfig has an invalid kubelogin cluster-info setting.');
  }
  const rawExecEnv = exec.env;
  if (rawExecEnv !== undefined && rawExecEnv !== null && !Array.isArray(rawExecEnv)) {
    throw new Error('AKS kubeconfig has invalid kubelogin environment settings.');
  }
  const execEnv: Array<{ name: string; value: string }> = [];
  for (const value of (Array.isArray(rawExecEnv) ? rawExecEnv : [])) {
    if (!isRecord(value) || typeof value.name !== 'string' || typeof value.value !== 'string') {
      throw new Error('AKS kubeconfig has invalid kubelogin environment settings.');
    }
    execEnv.push({ name: value.name, value: value.value });
  }
  const originalArgs = rawExecArgs;
  const normalizedArgs: string[] = [];
  for (let index = 0; index < originalArgs.length; index++) {
    const arg = originalArgs[index]!;
    if (arg === '--subscription') {
      if (!originalArgs[index + 1] || originalArgs[index + 1]!.startsWith('--')) {
        throw new Error('AKS kubeconfig has a malformed kubelogin subscription argument.');
      }
      index++;
    } else if (arg.startsWith('--subscription=') ||
      arg === '--disable-environment-override' || arg.startsWith('--disable-environment-override=')) {
      continue;
    } else {
      normalizedArgs.push(arg);
    }
  }
  normalizedArgs.push('--subscription', live.subscription, '--disable-environment-override');
  if (normalizedArgs.length !== originalArgs.length || normalizedArgs.some((arg, index) => arg !== originalArgs[index])) {
    const setArgs = ['config', 'set-credentials', kubeUser, `--exec-command=${exec.command}`];
    if (typeof exec.apiVersion === 'string') setArgs.push(`--exec-api-version=${exec.apiVersion}`);
    setArgs.push(...normalizedArgs.map(arg => `--exec-arg=${arg}`));
    for (const entry of execEnv) {
      setArgs.push(`--exec-env=${entry.name}=${entry.value}`);
    }
    if (typeof exec.interactiveMode === 'string') setArgs.push(`--exec-interactive-mode=${exec.interactiveMode}`);
    if (typeof exec.provideClusterInfo === 'boolean') {
      setArgs.push(`--exec-provide-cluster-info=${exec.provideClusterInfo}`);
    }
    await checked(runner, ['kubectl', ...setArgs], 60, 'Harden kubelogin operator authentication');
  }
  const clientId = await checked(runner, ['az', 'identity', 'show', '--subscription', live.subscription,
    '--resource-group', live.resourceGroup, '--name', 'arrow-collector', '--query', 'clientId',
    '--output', 'tsv', '--only-show-errors'], 60, 'Read arrow-collector UAMI');
  const tenantId = await checked(runner, ['az', 'account', 'show', '--subscription', live.subscription,
    '--query', 'tenantId', '--output', 'tsv', '--only-show-errors'], 60, 'Read operator tenant');
  if (!uuid.test(clientId) || !uuid.test(tenantId)) throw new Error('Azure returned an invalid UAMI client ID or tenant UUID.');
  const config = new Config({ AZURE_SUBSCRIPTION_ID: live.subscription, AZURE_UAMI_CLIENT_ID: clientId }, stampConfig);
  const generation = await capturedGeneration(config, runner);
  try {
    if (await readFile(resolve(directory, 'secrets/generation'), 'utf8') !== generation) {
      throw new Error('Existing private secrets capture a different publisher generation; do not rotate secrets or publisher generation during a run.');
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const bootstrapValues = { ...config.generated(), AZURE_SUBSCRIPTION_ID: live.subscription };
  const bootstrapEnvPath = resolve(directory, 'bootstrap.env');
  await writePrivateFile(bootstrapEnvPath, renderBootstrapEnv(bootstrapValues, generation));

  const manifests = await Promise.all(['namespace.yaml', 'service-account.yaml'].map(async filename => {
    const template = parseDocument(await readFile(resolve(ROOT, 'infra/kubernetes/base', filename), 'utf8'));
    if (template.errors.length) throw new Error(`Invalid publisher identity template ${filename}.`);
    template.setIn(['metadata', 'name'], 'arrow-collector');
    if (filename === 'service-account.yaml') {
      template.setIn(['metadata', 'namespace'], 'arrow-collector');
      template.setIn(['metadata', 'annotations', 'azure.workload.identity/client-id'], clientId);
    }
    return template.toString();
  }));
  const identityPath = resolve(directory, 'publisher-identity.yaml');
  await writePrivateFile(identityPath, manifests.join('---\n'));
  await checked(runner, ['kubectl', '--context', live.clusterName, '--request-timeout=60s', 'apply',
    '--filename', identityPath], 120, 'Apply host publisher identity');

  const draft: RuntimeConfig = {
    namespace: 'arrow-collector', serviceAccount: 'arrow-collector', oatRunId: randomUUID(),
    workloadIdentityClientId: clientId, subscriptionId: live.subscription, stamp: live.stamp,
    deltaRoot: `${config.table_root}dataflow`,
    images: { sqlserver: 'unpublished', catalog: 'unpublished', router: 'unpublished', dataflow: 'unpublished',
      ...(options.maintenance ? { maintenance: 'unpublished' } : {}) },
  };
  const revision = await checked(runner, ['git', 'rev-parse', 'HEAD'], 15, 'Read source revision');
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error('Git returned an invalid full source revision.');
  const tag = options.tag ?? `oat-${revision}`;
  if (!/^[a-z0-9][a-z0-9._-]{0,127}$/.test(tag)) throw new Error('--tag must be a valid lowercase container tag.');
  const imageOptions = { env: bootstrapEnvPath, azureEnv: bootstrapEnvPath, stampConfig: stampConfigPath,
    registry: config.registry, federated: true, tag, runtimeConfig: resolve(directory, 'runtime.publish.json') };
  const imagesApi = await import('./publish-images.js');
  let runtime: RuntimeConfig;
  let clientImage: string;
  try {
    const published = await imagesApi.readPublishedImages({ ...imageOptions, revision, maintenance: options.maintenance }, runner);
    runtime = validateRuntime({ ...draft, images: published.images });
    clientImage = published.client;
    console.log(`Reusing ${options.maintenance ? 'six' : 'five'} revision-verified images for ${revision}.`);
  } catch (error) {
    if (!(error instanceof imagesApi.MissingPublishedImagesError)) throw error;
    if (options.skipImages) throw new Error(`Revision-pinned images are missing; run \`${setupCommand}\` without --skip-images.`, { cause: error });
    try {
      await writePrivateFile(imageOptions.runtimeConfig, `${JSON.stringify(validateRuntime(draft, true), null, 2)}\n`);
      await imagesApi.buildLocalImages({ revision, maintenance: options.maintenance }, runner);
      await imagesApi.publishImages(imageOptions, runner);
      await imagesApi.publishClientImage(imageOptions, runner);
      const verified = await imagesApi.readPublishedImages({ ...imageOptions, revision, maintenance: options.maintenance }, runner);
      runtime = validateRuntime({ ...draft, images: verified.images });
      clientImage = verified.client;
    } finally { await rm(imageOptions.runtimeConfig, { force: true }); }
  }
  await imagesApi.publishViewer(imageOptions, runner);
  const assertion = await checked(runner, ['kubectl', '--context', live.clusterName, '--request-timeout=60s',
    'create', 'token', 'arrow-collector', '--namespace', 'arrow-collector', '--audience',
    'api://AzureADTokenExchange', '--duration', '1h'], 90, 'Mint host federation assertion');
  const oatTemplate = JSON.parse(await readFile(resolve(ROOT,
    'docs/experiments/arrowcol-oat/oat-config.aks.quick.json'), 'utf8')) as Record<string, unknown>;
  if (options.maintenance) oatTemplate.tunables = {
    maintenance: { enabled: true, apply: false, operations: ['optimize'] },
  };
  const files = await generatePrivateInputs({ directory, runtime, bootstrapValues, resetGeneration: generation,
    tenantId, assertion, clientImage, qapEndpoint: config.uri, oatTemplate });
  let preflightReady = false;
  if (!options.skipPreflight) {
    for (let attempt = 1; attempt <= 3; attempt++) {
      console.log(`AKS preflight pass ${attempt}/3 for ${live.clusterName}`);
      const result = await runner(['npx', '--no-install', 'arrowcol-oat', '--mode', 'aks', '--preflight-only',
        '--stamp-config', relative(ROOT, stampConfigPath), '--config', files.aksConfigPath], 600, ROOT, { ...load_env(resolve(directory, 'aks.env')), ...process.env });
      if (result.stdout.trim()) console.log(result.stdout.trim());
      if (!result.returncode) { preflightReady = true; break; }
      if (result.stderr.trim()) console.error(result.stderr.trim());
    }
    if (!preflightReady) throw new Error(`AKS preflight did not become READY in three bounded passes; correct the reported gates and run \`${setupCommand}\`.`);
  }
  console.log(`AKS setup ${preflightReady ? 'READY' : 'prepared (preflight skipped)'}: stamp ${live.stamp}, ${live.clusterName}, ${options.maintenance ? 'six' : 'five'} digest-pinned images.`);
  console.log('Host assertion lasts one hour; rerun setup before it expires. Next: npx --no-install arrowcol-oat --mode aks --stamp-config ' + relative(ROOT, stampConfigPath));
  return { stamp: live.stamp, clusterName: live.clusterName, images: { ...runtime.images, client: clientImage },
    ...files, preflightReady };
}

export async function main(args: string[], runner: typeof run_tree = run_tree): Promise<void> {
  const { values } = parseArgs({ args, strict: true, allowPositionals: false, options: {
    stamp: { type: 'string' }, 'stamp-config': { type: 'string' }, tag: { type: 'string' },
    'skip-images': { type: 'boolean', default: false }, 'skip-preflight': { type: 'boolean', default: false },
    maintenance: { type: 'boolean', default: false },
  } });
  await setup({ stamp: values.stamp === undefined ? undefined : stampNumber(values.stamp),
    stampConfig: values['stamp-config'], tag: values.tag,
    skipImages: values['skip-images'], skipPreflight: values['skip-preflight'], maintenance: values.maintenance }, runner);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'AKS setup failed'); process.exitCode = 1;
  });
}
