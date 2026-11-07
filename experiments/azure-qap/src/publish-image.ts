import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { parseDocument } from 'yaml';
import { Failure, ROOT, load_env, run_tree } from '../../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../../infra/azure/stamp.js';

const IMAGE_NAME = 'azure-qap';

export interface ImageOptions { env: string; azureEnv: string; stampConfig: string; registry: string; tag?: string; federated?: boolean }

export function parseImageOptions(argv: readonly string[]): ImageOptions {
  const { values } = parseArgs({ args: [...argv], strict: true, allowPositionals: false, options: {
    env: { type: 'string', default: '.env' }, 'azure-env': { type: 'string', default: '.env.azure' },
    'stamp-config': { type: 'string', default: 'infra/azure/stamp.yaml' }, registry: { type: 'string' }, tag: { type: 'string' },
    federated: { type: 'boolean', default: false },
  } });
  if (!values.registry || !/^[a-z0-9]{5,50}$/.test(values.registry))
    throw new Failure('--registry must be the name of the Azure Container Registry.');
  if (values.tag !== undefined && !/^[a-z0-9][a-z0-9._-]{0,127}$/.test(values.tag))
    throw new Failure('--tag must be a valid lowercase container tag.');
  return { env: values.env!, azureEnv: values['azure-env']!, stampConfig: values['stamp-config']!, registry: values.registry,
    ...(values.tag ? { tag: values.tag } : {}), ...(values.federated ? { federated: true } : {}) };
}

function ensure(name: string, result: { returncode: number; stdout: string; stderr: string }, secrets: string[]): string {
  if (result.returncode) {
    let detail = result.stderr.trim().slice(0, 1200);
    for (const secret of secrets) detail = detail.replaceAll(secret, '******');
    throw new Failure(`${name} failed (${result.returncode}): ${detail || 'no diagnostic output'}`);
  }
  return result.stdout.trim();
}

export async function publishRuntimeImage(options: ImageOptions, runner: typeof run_tree = run_tree): Promise<string> {
  const environment = { ...load_env(path.resolve(ROOT, options.env)), ...process.env } as Record<string, string>;
  const generated = load_env(path.resolve(ROOT, options.azureEnv));
  const stamp = loadStampConfig(options.stampConfig);
  const subscription = stamp.subscription;
  const identityClientId = generated.AZURE_UAMI_CLIENT_ID;
  const expectedRegistry = generated.AZURE_CONTAINER_REGISTRY_NAME;
  const expectedLoginServer = generated.AZURE_CONTAINER_REGISTRY_SERVER;
  if (!identityClientId || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(identityClientId))
    throw new Failure('Generated Azure config must contain the deployed arrow-collector UAMI client ID.');
  if (options.registry !== expectedRegistry || expectedLoginServer !== `${options.registry}.azurecr.io`)
    throw new Failure('The requested registry must match the validated stamp and generated Azure config.');
  const secrets = [...Object.entries(environment), ...Object.entries(generated)].filter(([key, value]) =>
    /SECRET|TOKEN|PASSWORD|KEY/.test(key) && value.length >= 8).map(([, value]) => value);
  const clean = (name: string, result: { returncode: number; stdout: string; stderr: string }) => ensure(name, result, secrets);
  const revision = clean('git rev-parse', await runner(['git', 'rev-parse', '--short=12', 'HEAD'], 15));
  const dirty = clean('git status', await runner(['git', 'status', '--porcelain'], 15));
  if (dirty) throw new Failure('Commit the source changes before building a runtime image so its digest identifies a revision.');
  let temporary: string | undefined;
  let commandEnvironment: NodeJS.ProcessEnv | undefined;
  const run: typeof run_tree = (command, timeout) => runner(command, timeout, ROOT, commandEnvironment);
  try {
    if (options.federated) {
      const fqdn = clean('az aks show', await run(['az', 'aks', 'show', '--subscription', subscription,
        '--resource-group', stamp.resourceGroup, '--name', `arrow-collector-${stamp.stamp}`, '--query', 'fqdn', '--output', 'tsv'], 60));
      const server = clean('kubectl config view', await run(['kubectl', 'config', 'view', '--minify',
        '--output', 'jsonpath={.clusters[0].cluster.server}'], 30));
      let endpoint: URL;
      try { endpoint = new URL(server); } catch { throw new Failure('The active kubectl context has an invalid server URL.'); }
      if (!fqdn || endpoint.protocol !== 'https:' || endpoint.hostname.toLowerCase() !== fqdn.toLowerCase() ||
          (endpoint.port && endpoint.port !== '443') || endpoint.username || endpoint.password)
        throw new Failure('The active kubectl context must target the stamped AKS cluster before federated publishing.');
      const tenant = clean('az account tenant', await run(['az', 'account', 'show', '--subscription', subscription,
        '--query', 'tenantId', '--output', 'tsv'], 60));
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(tenant))
        throw new Failure('Azure CLI returned an invalid tenant ID.');
      temporary = fs.mkdtempSync(path.join(os.tmpdir(), 'azure-qap-publisher-'));
      const manifests = ['namespace.yaml', 'service-account.yaml'].map(filename => {
        const document = parseDocument(fs.readFileSync(path.join(ROOT, 'infra/kubernetes/base', filename), 'utf8'));
        if (document.errors.length) throw new Failure(`Invalid identity template ${filename}.`);
        document.setIn(['metadata', 'name'], filename === 'namespace.yaml' ? stamp.aks.namespace : stamp.aks.serviceAccount);
        if (filename === 'service-account.yaml') {
          document.setIn(['metadata', 'namespace'], stamp.aks.namespace);
          document.setIn(['metadata', 'annotations', 'azure.workload.identity/client-id'], identityClientId);
        }
        return document.toString();
      });
      const manifest = path.join(temporary, 'identity.yaml');
      fs.writeFileSync(manifest, manifests.join('---\n'), { mode: 0o600 });
      clean('kubectl apply publisher identity', await run(['kubectl', 'apply', '--filename', manifest], 120));
      const assertion = clean('kubectl create publisher token', await run(['kubectl', 'create', 'token',
        stamp.aks.serviceAccount, '--namespace', stamp.aks.namespace, '--audience', 'api://AzureADTokenExchange',
        '--duration', '1h'], 60));
      secrets.push(assertion);
      if (assertion.length > 16_384 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(assertion))
        throw new Failure('Kubernetes returned an invalid publisher federation assertion.');
      commandEnvironment = { ...environment, AZURE_CONFIG_DIR: path.join(temporary, 'azure') };
      clean('az federated publisher login', await run(['az', 'login', '--service-principal', '--username', identityClientId,
        '--tenant', tenant, '--federated-token', assertion, '--output', 'none'], 120));
    }
    const authenticatedClientId = clean('az account show', await run([
      'az', 'account', 'show', '--subscription', subscription, '--query', 'user.name', '--output', 'tsv',
    ], 60));
    if (authenticatedClientId.toLowerCase() !== identityClientId.toLowerCase())
      throw new Failure('Azure CLI must be authenticated as the single arrow-collector UAMI before publishing.');
    const tag = options.tag ?? `qap-${revision}`;
    const loginServer = clean('az acr show', await run([
      'az', 'acr', 'show', '--name', options.registry, '--subscription', subscription, '--query', 'loginServer', '--output', 'tsv',
    ], 60));
    if (loginServer !== expectedLoginServer || !/^[a-z0-9][a-z0-9.-]*\.azurecr\.io$/.test(loginServer))
      throw new Failure('Azure Container Registry does not match the generated stamp configuration.');
    clean('az acr login', await run([
      'az', 'acr', 'login', '--name', options.registry, '--subscription', subscription,
    ], 120));
    const tagRef = `${loginServer}/${IMAGE_NAME}:${tag}`;
    clean('docker buildx build', await run([
      'docker', 'buildx', 'build', '--pull', '--platform', 'linux/amd64', '--push', '--tag', tagRef,
      '--file', 'experiments/azure-qap/Dockerfile', '.',
    ], 3600));
    const digest = clean('az acr manifest show-metadata', await run([
      'az', 'acr', 'manifest', 'show-metadata', '--registry', options.registry, '--name', `${IMAGE_NAME}:${tag}`,
      '--subscription', subscription, '--query', 'digest', '--output', 'tsv',
    ], 120));
    if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Failure('Azure Container Registry returned an invalid image digest.');
    const image = `${loginServer}/${IMAGE_NAME}@${digest}`;
    console.log(`Digest-pinned runtime image: ${image}`);
    return image;
  } finally {
    if (temporary) fs.rmSync(temporary, { recursive: true, force: true });
  }
}

async function main(argv: readonly string[]): Promise<number> {
  try { await publishRuntimeImage(parseImageOptions(argv)); return 0; }
  catch (error) { console.error(error instanceof Error ? error.message : String(error)); return 1; }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main(process.argv.slice(2));