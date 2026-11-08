import { randomUUID } from 'node:crypto';
import { readFile, rename, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { parseImageOptions, withPublisherIdentity, type ImageOptions } from '../../experiments/azure-qap/src/publish-image.js';
import { validateRuntime, type RuntimeConfig } from './runtime.js';

const repositories = {
  sqlserver: 'arrowcol-sqlserver', catalog: 'arrowcol-catalog-rust',
  router: 'arrowcol-router-rust', dataflow: 'arrowcol-dataflow-rust',
} as const;
type Options = ImageOptions & { runtimeConfig: string };

export async function publishImages(options: Options, runner: typeof run_tree = run_tree): Promise<RuntimeConfig> {
  if (!options.federated) throw new Error('OAT publishing requires --federated; never use operator credentials for ACR publication.');
  const file = resolve(ROOT, options.runtimeConfig);
  const ignored = await runner(['git', 'check-ignore', '--quiet', file], 15);
  if (ignored.returncode) throw new Error('Runtime configuration must be an ignored workspace file.');
  const original = await readFile(file, 'utf8');
  const config = validateRuntime(JSON.parse(original), true);
  const stamp = loadStampConfig(options.stampConfig);
  const generated = load_env(resolve(ROOT, options.azureEnv));
  if (config.stamp !== String(stamp.stamp) || config.subscriptionId.toLowerCase() !== stamp.subscription.toLowerCase() ||
      config.workloadIdentityClientId.toLowerCase() !== generated.AZURE_UAMI_CLIENT_ID?.toLowerCase() ||
      config.namespace !== stamp.aks.namespace || (config.serviceAccount ?? 'arrowcol-oat') !== stamp.aks.serviceAccount) {
    throw new Error('Runtime configuration must reuse the selected stamp namespace, ServiceAccount and UAMI.');
  }
  for (const role of Object.keys(repositories)) {
    const result = await runner(['docker', 'image', 'inspect', '--format', '{{.Os}}/{{.Architecture}}', `arrowcol-oat/${role}:local`], 30);
    if (result.returncode || result.stdout.trim() !== 'linux/amd64') throw new Error(`Missing linux/amd64 local image for ${role}; build it before publishing.`);
  }
  const images = await withPublisherIdentity(options, async ({ run, clean, revision, loginServer, subscription }) => {
    const published = {} as RuntimeConfig['images'];
    const tag = options.tag ?? `oat-${revision}`;
    for (const [role, repository] of Object.entries(repositories) as Array<[keyof typeof repositories, string]>) {
      const tagged = `${loginServer}/${repository}:${tag}`;
      clean(`docker tag ${role}`, await run(['docker', 'tag', `arrowcol-oat/${role}:local`, tagged], 30));
      clean(`docker push ${role}`, await run(['docker', 'push', tagged], 3600));
      const digest = clean(`ACR manifest ${role}`, await run(['az', 'acr', 'manifest', 'show-metadata',
        '--registry', options.registry, '--name', `${repository}:${tag}`, '--subscription', subscription,
        '--query', 'digest', '--output', 'tsv'], 120));
      if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`ACR returned an invalid ${role} manifest digest.`);
      published[role] = `${loginServer}/${repository}@${digest}`;
    }
    return published;
  }, runner, true);
  const published = validateRuntime({ ...config, images });
  if (await readFile(file, 'utf8') !== original) throw new Error('Runtime configuration changed during publication; refusing to overwrite it.');
  const pending = `${file}.published-${randomUUID()}`;
  try {
    await writeFile(pending, `${JSON.stringify(published, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
    await rename(pending, file);
  } finally { await rm(pending, { force: true }); }
  return published;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [runtimeConfig, ...args] = process.argv.slice(2);
  Promise.resolve().then(async () => {
    if (!runtimeConfig || runtimeConfig.startsWith('--')) throw new Error('Usage: publish-images.ts <ignored-runtime.json> --federated --registry <acr> --azure-env <env> --stamp-config <stamp> [--env <env>] [--tag <tag>]');
    const result = await publishImages({ ...parseImageOptions(args), runtimeConfig });
    console.log(JSON.stringify({ images: result.images, runtimeConfig }, null, 2));
  }).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Image publication failed'); process.exitCode = 1; });
}
